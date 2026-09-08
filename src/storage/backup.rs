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
    gate: Arc<Semaphore>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

pub struct BackupJob {
    coordinator: BackupCoordinator,
    snapshot: PinnedSnapshot,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl BackupCoordinator {
    pub fn new(db: DbWorker, data_dir: &Path, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner: Arc::new(Inner {
                db,
                data_dir: data_dir.to_owned(),
                store,
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
        Ok(Some(BackupJob {
            coordinator: self.clone(),
            snapshot,
            _permit: permit,
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
        let guard = RemoveDirectory(root.clone());
        let snapshot_path = root.join("snapshot.db");
        let snapshot = self.snapshot;
        let snapshot_output = snapshot_path.clone();
        let artifact = tokio::task::spawn_blocking(move || {
            snapshot.backup_to(&snapshot_output, SnapshotLimits::default(), || false)
        })
        .await??;

        let mut shards = Vec::with_capacity(artifact.cut.shards.len());
        let mut shard_archives = Vec::with_capacity(artifact.cut.shards.len());
        for shard in &artifact.cut.shards {
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
            let archived = tokio::task::spawn_blocking(move || {
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
                             SET state='remote_verified',remote_archive_key=?1,
                                 archive_sha256=?2,recovery_checkpoint_id=?3
                             WHERE id=?4 AND state IN ('local','remote_verified')",
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
                objects: Vec::new(),
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
            self.put(relative, bytes, false)
        }

        async fn download(
            &self,
            _relative: &str,
            _destination: &Path,
            _max_bytes: u64,
        ) -> Result<ObjectMetadata> {
            anyhow::bail!("download is unused")
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
        let coordinator = BackupCoordinator::new(db.clone(), data, store.clone());
        let job = coordinator
            .begin_cut()
            .await?
            .context("missing backup job")?;
        assert!(coordinator.begin_cut().await?.is_none());
        job.complete().await?;

        let keys = store.0.lock().unwrap().keys().cloned().collect::<Vec<_>>();
        assert!(keys.iter().any(|key| key == "latest.json"));
        assert!(keys.iter().any(|key| key.starts_with("checkpoints/")));
        assert!(keys.iter().any(|key| key.starts_with("snapshots/")));
        assert!(
            keys.iter()
                .any(|key| key == &format!("shards/{shard_id}.tar.gz"))
        );
        let verified = db
            .call(move |connection| {
                connection
                    .query_row(
                        "SELECT state,remote_archive_key,archive_sha256,recovery_checkpoint_id
                     FROM shards WHERE id=?1",
                        [shard_id],
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
        Ok(())
    }
}
