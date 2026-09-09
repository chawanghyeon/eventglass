//! Single-flight checkpoint creation started from an Indexer seal boundary.

use anyhow::{Result, ensure};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tokio::{sync::Semaphore, task::JoinHandle};

use crate::db::worker::DbWorker;

use super::{
    archive,
    checkpoint::{PinnedSnapshot, SnapshotLimits},
    remote::{CheckpointDocument, LocalCheckpoint, ObjectReference, ShardReference, publish},
    s3::ObjectStore,
};

#[derive(Clone)]
pub struct BackupCoordinator {
    inner: Arc<Inner>,
}

struct Inner {
    db: DbWorker,
    data_dir: PathBuf,
    store: Arc<dyn ObjectStore>,
    disk: super::budget::DiskBudget,
    gate: Arc<Semaphore>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

pub struct BackupJob {
    coordinator: BackupCoordinator,
    snapshot: PinnedSnapshot,
    _disk: Arc<super::budget::Reservation>,
    _permit: Arc<tokio::sync::OwnedSemaphorePermit>,
}

impl BackupCoordinator {
    pub fn new(
        db: DbWorker,
        data_dir: &Path,
        store: Arc<dyn ObjectStore>,
        disk: super::budget::DiskBudget,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                db,
                data_dir: data_dir.to_owned(),
                store,
                disk,
                gate: Arc::new(Semaphore::new(1)),
                tasks: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Must be called after SQLite finalized a seal and before the next active
    /// shard is adopted. Returning is the snapshot handshake that releases Indexer.
    pub async fn begin_cut(&self) -> Result<Option<BackupJob>> {
        let permit = match Arc::clone(&self.inner.gate).try_acquire_owned() {
            Ok(permit) => permit,
            Err(tokio::sync::TryAcquireError::NoPermits) => return Ok(None),
            Err(tokio::sync::TryAcquireError::Closed) => {
                anyhow::bail!("backup coordinator is closed")
            }
        };
        let path = self.inner.data_dir.join("meta.db");
        let snapshot = tokio::task::spawn_blocking(move || PinnedSnapshot::open(&path)).await??;
        let reservation = self.inner.disk.reserve(snapshot.temporary_bytes()?)?;
        Ok(Some(BackupJob {
            _disk: Arc::new(reservation),
            coordinator: self.clone(),
            snapshot,
            _permit: Arc::new(permit),
        }))
    }

    pub async fn shutdown(&self) {
        let tasks = self
            .inner
            .tasks
            .lock()
            .map(|mut tasks| std::mem::take(&mut *tasks))
            .unwrap_or_default();
        for task in tasks {
            let _ = task.await;
        }
    }
}

impl BackupJob {
    pub fn launch(self) {
        let coordinator = self.coordinator.clone();
        let task = tokio::spawn(async move {
            if let Err(error) = self.complete().await {
                tracing::warn!(reason = %error, "checkpoint backup failed; local service remains available");
            }
        });
        match coordinator.inner.tasks.lock() {
            Ok(mut tasks) => {
                tasks.retain(|task| !task.is_finished());
                tasks.push(task);
            }
            Err(_) => task.abort(),
        }
    }

    async fn complete(self) -> Result<()> {
        let checkpoint_id = uuid::Uuid::new_v4().to_string();
        let created_at_us = crate::model::now_us()?;
        let sequence = u64::try_from(self.snapshot.cut().boundary.inbox_id)?;
        ensure!(sequence > 0, "checkpoint boundary must be nonzero");
        let root = self
            .coordinator
            .inner
            .data_dir
            .join(format!(".backup-{checkpoint_id}.tmp"));
        std::fs::create_dir(&root)?;
        let guard = Arc::new(RemoveDirectory(root.clone()));
        let snapshot_path = root.join("snapshot.db");
        let recovery_checkpoints = self.snapshot.recovery_checkpoints()?;
        let snapshot = self.snapshot;
        let snapshot_output = snapshot_path.clone();
        let resources = (
            Arc::clone(&guard),
            Arc::clone(&self._disk),
            Arc::clone(&self._permit),
        );
        let artifact = tokio::task::spawn_blocking(move || {
            let _resources = resources;
            snapshot.backup_to(&snapshot_output, SnapshotLimits::default(), || false)
        })
        .await??;

        let mut shards = Vec::with_capacity(artifact.cut.shards.len());
        let mut shard_archives = Vec::with_capacity(artifact.cut.shards.len());
        let mut previous = std::collections::BTreeMap::new();
        for shard in &artifact.cut.shards {
            if let Some(checkpoint_id) = recovery_checkpoints.get(&shard.id) {
                if !previous.contains_key(checkpoint_id) {
                    let document = super::remote::read_checkpoint(
                        self.coordinator.inner.store.as_ref(),
                        checkpoint_id,
                        &artifact.cut.installation_id,
                    )
                    .await?;
                    previous.insert(checkpoint_id.clone(), document);
                }
                let reference = previous[checkpoint_id]
                    .shards
                    .iter()
                    .find(|reference| reference.id == shard.id)
                    .ok_or_else(|| anyhow::anyhow!("recovery checkpoint omits catalog shard"))?;
                ensure!(
                    shard.remote_archive_key.as_ref() == Some(&reference.object.key)
                        && shard.archive_sha256.as_ref() == Some(&reference.object.sha256),
                    "recovery checkpoint differs from catalog archive"
                );
                shards.push(reference.clone());
                continue;
            }
            let source = self
                .coordinator
                .inner
                .data_dir
                .join("shards")
                .join(&shard.id);
            let path = root.join(format!("{}.tar.gz", shard.id));
            let installation = artifact.cut.installation_id.clone();
            let id = shard.id.clone();
            let source_path = source.clone();
            let output_path = path.clone();
            let resources = (
                Arc::clone(&guard),
                Arc::clone(&self._disk),
                Arc::clone(&self._permit),
            );
            let archived = tokio::task::spawn_blocking(move || {
                let _resources = resources;
                archive::create(&source_path, &output_path, &installation, &id, || false)
            })
            .await??;
            shards.push(ShardReference {
                id: shard.id.clone(),
                object: ObjectReference {
                    key: format!("shards/{}.tar.gz", shard.id),
                    size: archived.size,
                    sha256: archived.sha256,
                },
            });
            shard_archives.push((shard.id.clone(), path));
        }
        let document = CheckpointDocument {
            format_version: 1,
            checkpoint_id: checkpoint_id.clone(),
            sequence,
            created_at_us,
            cut: artifact.cut,
            snapshot: ObjectReference {
                key: format!("snapshots/{checkpoint_id}.db"),
                size: artifact.size,
                sha256: artifact.sha256,
            },
            shards,
        };
        let candidate = LocalCheckpoint {
            document: document.clone(),
            snapshot_path,
            shard_archives,
        };
        publish(self.coordinator.inner.store.as_ref(), &candidate).await?;
        let checkpoint = document.checkpoint_id;
        let verified = document.shards;
        self.coordinator
            .inner
            .db
            .call(move |connection| {
                let transaction = connection.transaction()?;
                for shard in verified {
                    ensure!(
                        transaction.execute(
                            "UPDATE shards
                             SET state=CASE WHEN state='remote_only' THEN 'remote_only' ELSE 'remote_verified' END,remote_archive_key=?1,
                                 archive_sha256=?2,recovery_checkpoint_id=?3
                             WHERE id=?4 AND state IN ('local','remote_verified','remote_only')",
                            rusqlite::params![
                                shard.object.key,
                                shard.object.sha256,
                                checkpoint,
                                shard.id
                            ],
                        )? == 1,
                        "checkpoint shard catalog changed before verification"
                    );
                }
                transaction.commit()?;
                Ok(())
            })
            .await?;
        drop(guard);
        Ok(())
    }
}

struct RemoveDirectory(PathBuf);

impl Drop for RemoveDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        model::Boundary,
        search::active::ActiveShard,
        storage::{
            manifest::{self, ShardStats},
            remote::sha256,
            s3::{ObjectMetadata, ObjectPage},
        },
    };
    use anyhow::Context;
    use async_trait::async_trait;
    use std::{collections::BTreeMap, fs::File, sync::Mutex};

    #[derive(Default)]
    struct MemoryStore(Mutex<BTreeMap<String, Vec<u8>>>);

    impl MemoryStore {
        fn put(&self, key: &str, bytes: Vec<u8>, absent: bool) -> Result<()> {
            let mut objects = self.0.lock().unwrap();
            ensure!(!absent || !objects.contains_key(key), "object exists");
            objects.insert(key.to_owned(), bytes);
            Ok(())
        }
    }

    #[async_trait]
    impl ObjectStore for MemoryStore {
        async fn list(&self, _continuation: Option<String>) -> Result<ObjectPage> {
            Ok(ObjectPage {
                objects: self
                    .0
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(key, bytes)| ObjectMetadata {
                        key: key.clone(),
                        size: bytes.len() as u64,
                        etag: None,
                    })
                    .collect(),
                continuation: None,
            })
        }

        async fn get_small(&self, relative: &str, max_bytes: u64) -> Result<Vec<u8>> {
            let bytes = self
                .0
                .lock()
                .unwrap()
                .get(relative)
                .context("missing object")?
                .clone();
            ensure!(bytes.len() as u64 <= max_bytes, "object too large");
            Ok(bytes)
        }

        async fn put_if_absent(&self, relative: &str, bytes: Vec<u8>) -> Result<()> {
            self.put(relative, bytes, true)
        }

        async fn put_bytes(&self, relative: &str, bytes: Vec<u8>) -> Result<()> {
            self.put(relative, bytes, false)
        }

        async fn put_file(
            &self,
            relative: &str,
            path: &Path,
            checksum_sha256_hex: &str,
        ) -> Result<()> {
            let bytes = std::fs::read(path)?;
            ensure!(sha256(&bytes) == checksum_sha256_hex, "checksum mismatch");
            self.put(relative, bytes, relative.starts_with("shards/"))
        }

        async fn download(
            &self,
            relative: &str,
            destination: &Path,
            max_bytes: u64,
        ) -> Result<ObjectMetadata> {
            let bytes = self.get_small(relative, max_bytes).await?;
            std::fs::write(destination, &bytes)?;
            Ok(ObjectMetadata {
                key: relative.to_owned(),
                size: bytes.len() as u64,
                etag: None,
            })
        }
    }

    #[tokio::test]
    async fn sealed_cut_publishes_once_and_marks_only_checkpointed_shards() -> Result<()> {
        let root = tempfile::tempdir()?;
        let data = root.path();
        std::fs::create_dir(data.join("shards"))?;
        let installation = uuid::Uuid::new_v4().to_string();
        let shard_id = uuid::Uuid::new_v4().to_string();
        let boundary = Boundary {
            inbox_id: 1,
            ingest_seq: 1,
        };
        let shard_path = data.join("shards").join(&shard_id);
        std::fs::create_dir(&shard_path)?;
        let mut active =
            ActiveShard::create(&shard_path, &installation, &shard_id, Boundary::default())?;
        active.publish(Boundary::default())?;
        active.commit(&[], boundary)?;
        active.publish(boundary)?;
        let manifest = active.seal(
            &shard_path,
            ShardStats {
                record_count: 0,
                min_timestamp_us: None,
                max_timestamp_us: None,
                min_received_at_us: None,
                max_received_at_us: None,
                min_ingest_seq: None,
                max_ingest_seq: None,
            },
            1,
        )?;
        let size = i64::try_from(manifest::local_size(&shard_path, &manifest)?)?;
        let lock = Arc::new(File::open(data)?);
        let db = DbWorker::start(&data.join("meta.db"), lock)?;
        let install = installation.clone();
        let id = shard_id.clone();
        db.call(move |connection| {
            connection.execute(
                "INSERT INTO runtime_state(singleton,installation_id,storage_generation,
                    next_ingest_seq,last_applied_inbox_id,last_applied_ingest_seq)
                 VALUES(1,?1,?2,2,1,1)",
                rusqlite::params![install, uuid::Uuid::new_v4().to_string()],
            )?;
            connection.execute(
                "INSERT INTO shards(id,schema_version,format_version,tokenizer_version,state,
                    last_applied_inbox_id,record_count,size_bytes,created_at_us,sealed_at_us)
                 VALUES(?1,1,?2,1,'local',1,0,?3,1,1)",
                rusqlite::params![id, crate::db::shards::FORMAT_VERSION, size],
            )?;
            Ok(())
        })
        .await?;
        let store = Arc::new(MemoryStore::default());
        store.put(
            "installation.json",
            serde_json::to_vec(&super::super::remote::InstallationDocument {
                format_version: 1,
                installation_id: installation.clone(),
                created_at_us: 1,
            })?,
            false,
        )?;
        let coordinator = BackupCoordinator::new(
            db.clone(),
            data,
            store.clone(),
            super::super::budget::DiskBudget::new(data),
        );
        let job = coordinator
            .begin_cut()
            .await?
            .context("missing backup job")?;
        assert!(coordinator.begin_cut().await?.is_none());
        assert!(coordinator.inner.disk.reserved_bytes()? > 0);
        job.complete().await?;
        assert_eq!(coordinator.inner.disk.reserved_bytes()?, 0);

        let keys = store.0.lock().unwrap().keys().cloned().collect::<Vec<_>>();
        assert!(keys.iter().any(|key| key == "latest.json"));
        assert!(keys.iter().any(|key| key.starts_with("checkpoints/")));
        assert!(keys.iter().any(|key| key.starts_with("snapshots/")));
        assert!(
            keys.iter()
                .any(|key| key == &format!("shards/{shard_id}.tar.gz"))
        );
        let verified_id = shard_id.clone();
        let verified = db
            .call(move |connection| {
                connection
                    .query_row(
                        "SELECT state,remote_archive_key,archive_sha256,recovery_checkpoint_id
                     FROM shards WHERE id=?1",
                        [verified_id],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, Option<String>>(1)?,
                                row.get::<_, Option<String>>(2)?,
                                row.get::<_, Option<String>>(3)?,
                            ))
                        },
                    )
                    .map_err(Into::into)
            })
            .await?;
        assert_eq!(verified.0, "remote_verified");
        assert!(verified.1.is_some() && verified.2.is_some() && verified.3.is_some());

        // Eviction must not prevent later cuts from covering all historical shards.
        db.call(|connection| {
            connection.execute("UPDATE shards SET state='remote_only'", [])?;
            Ok(())
        })
        .await?;
        std::fs::remove_dir_all(&shard_path)?;
        let archive_key = format!("shards/{shard_id}.tar.gz");
        let archive = store
            .0
            .lock()
            .unwrap()
            .remove(&archive_key)
            .context("archive missing")?;
        let before = store.get_small("latest.json", 4096).await?;
        assert!(
            coordinator
                .begin_cut()
                .await?
                .context("job")?
                .complete()
                .await
                .is_err()
        );
        assert_eq!(store.get_small("latest.json", 4096).await?, before);
        store.put(&archive_key, archive, true)?;
        coordinator
            .begin_cut()
            .await?
            .context("job")?
            .complete()
            .await?;
        assert!(
            !shard_path.exists(),
            "backup must not hydrate an evicted shard"
        );
        let state = db
            .call(|connection| {
                Ok(connection.query_row("SELECT state FROM shards", [], |row| {
                    row.get::<_, String>(0)
                })?)
            })
            .await?;
        assert_eq!(state, "remote_only");
        let latest: super::super::remote::LatestDocument =
            serde_json::from_slice(&store.get_small("latest.json", 4096).await?)?;
        let restored = super::super::remote::prepare_restore(
            store.as_ref(),
            &installation,
            &data.join("restored"),
            super::super::remote::RestoreLimits::default(),
            || false,
        )
        .await?;
        assert_eq!(restored.document.checkpoint_id, latest.checkpoint_id);
        let restored_db = crate::db::open_reader(&restored.directory.join("meta.db"))?;
        assert_eq!(
            restored_db.query_row("SELECT state FROM shards", [], |row| row
                .get::<_, String>(0))?,
            "remote_verified"
        );
        assert!(restored.directory.join("shards").join(shard_id).is_dir());
        Ok(())
    }
}
