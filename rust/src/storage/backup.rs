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
    #[cfg(test)]
    fail_next_cut: std::sync::atomic::AtomicBool,
}

pub struct BackupJob {
    coordinator: BackupCoordinator,
    snapshot: PinnedSnapshot,
    _disk: Arc<super::budget::Reservation>,
    _permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    sequence: u64,
}

impl BackupCoordinator {
    /// Exclude local Replay collection for the full pinned snapshot/upload lifetime.
    pub(crate) fn try_maintenance_permit(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        self.inner.gate.clone().try_acquire_owned().ok()
    }

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
                #[cfg(test)]
                fail_next_cut: std::sync::atomic::AtomicBool::new(false),
            }),
        }
    }

    /// Must be called after SQLite finalized a seal and before the next active
    /// shard is adopted. Returning is the snapshot handshake that releases Indexer.
    pub async fn begin_cut(&self) -> Result<Option<BackupJob>> {
        #[cfg(test)]
        if self
            .inner
            .fail_next_cut
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            anyhow::bail!("injected checkpoint cut failure");
        }
        let permit = match Arc::clone(&self.inner.gate).try_acquire_owned() {
            Ok(permit) => permit,
            Err(tokio::sync::TryAcquireError::NoPermits) => return Ok(None),
            Err(tokio::sync::TryAcquireError::Closed) => {
                anyhow::bail!("backup coordinator is closed")
            }
        };
        let sequence=self.inner.db.call(|db| {
            let now=crate::model::now_us()?;
            let tx=db.transaction()?;
            tx.execute("INSERT INTO settings(key,value_json,updated_at_us) VALUES('replay.backup_attempt','true',?1) ON CONFLICT(key) DO UPDATE SET updated_at_us=excluded.updated_at_us",[now])?;
            tx.execute("INSERT INTO settings(key,value_json,updated_at_us) VALUES('checkpoint.sequence',CAST(?1 AS TEXT),?1) ON CONFLICT(key) DO UPDATE SET value_json=CAST(max(CAST(value_json AS INTEGER)+1,?1) AS TEXT),updated_at_us=excluded.updated_at_us",[now])?;
            let sequence:i64=tx.query_row("SELECT CAST(value_json AS INTEGER) FROM settings WHERE key='checkpoint.sequence'",[],|r|r.get(0))?;
            tx.commit()?;
            Ok(u64::try_from(sequence)?)
        }).await?;
        let path = self.inner.data_dir.join("meta.db");
        let snapshot = tokio::task::spawn_blocking(move || PinnedSnapshot::open(&path)).await??;
        let reservation = self.inner.disk.reserve(snapshot.temporary_bytes()?)?;
        Ok(Some(BackupJob {
            sequence,
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

    #[cfg(test)]
    pub(crate) fn fail_next_cut(&self) {
        self.inner
            .fail_next_cut
            .store(true, std::sync::atomic::Ordering::SeqCst);
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
        let sequence = self.sequence;
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
        let checkpoint_ids = recovery_checkpoints
            .values()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        for checkpoint_id in checkpoint_ids {
            let document = super::remote::read_checkpoint(
                self.coordinator.inner.store.as_ref(),
                &checkpoint_id,
                &artifact.cut.installation_id,
            )
            .await?;
            previous.insert(checkpoint_id, document);
        }
        for shard in &artifact.cut.shards {
            if let Some(checkpoint_id) = recovery_checkpoints.get(&shard.id) {
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
        let replay_files = document
            .cut
            .replay_blobs
            .iter()
            .map(|r| (r.key.clone(), self.coordinator.inner.data_dir.join(&r.key)))
            .collect();
        let candidate = LocalCheckpoint {
            replay_files,
            document: document.clone(),
            snapshot_path,
            shard_archives,
        };
        publish(self.coordinator.inner.store.as_ref(), &candidate).await?;
        let replay_revision = document.cut.replay_revision;
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
                transaction.execute("INSERT INTO settings(key,value_json,updated_at_us) VALUES('replay.backup_revision',CAST(?1 AS TEXT),?2) ON CONFLICT(key) DO UPDATE SET value_json=CAST(max(CAST(value_json AS INTEGER),?1) AS TEXT),updated_at_us=excluded.updated_at_us",rusqlite::params![replay_revision,created_at_us])?;
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
pub(crate) mod tests {
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
    pub(crate) struct MemoryStore(Mutex<BTreeMap<String, Vec<u8>>>);

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
    async fn closed_coordinator_and_launched_failure_are_bounded() {
        let root = tempfile::tempdir().expect("backup failure directory");
        let app = crate::app::AppState::open(crate::config::Config {
            addr: "127.0.0.1:0".parse().expect("loopback address"),
            data_dir: root.path().to_owned(),
            base_url: "http://localhost:8080".parse().expect("base URL"),
            s3_url: None,
            s3_endpoint: None,
            s3_initialize: false,
        })
        .await
        .expect("backup application state");
        let closed = BackupCoordinator::new(
            app.db.clone(),
            root.path(),
            Arc::new(MemoryStore::default()),
            app.disk_budget.clone(),
        );
        closed.inner.gate.close();
        assert!(closed.begin_cut().await.is_err());

        let launched = BackupCoordinator::new(
            app.db.clone(),
            root.path(),
            Arc::new(MemoryStore::default()),
            app.disk_budget.clone(),
        );
        let job = launched
            .begin_cut()
            .await
            .expect("backup handshake")
            .expect("backup job");
        job.launch();
        launched.shutdown().await;
        assert_eq!(
            launched.inner.disk.reserved_bytes().expect("released disk"),
            0
        );

        let poisoned = BackupCoordinator::new(
            app.db.clone(),
            root.path(),
            Arc::new(MemoryStore::default()),
            app.disk_budget.clone(),
        );
        let job = poisoned
            .begin_cut()
            .await
            .expect("poisoned backup handshake")
            .expect("poisoned backup job");
        let inner = Arc::clone(&poisoned.inner);
        assert!(
            tokio::task::spawn_blocking(move || {
                let _guard = inner.tasks.lock().expect("lock task list before poisoning");
                panic!("poison backup task list");
            })
            .await
            .is_err()
        );
        job.launch();
        tokio::task::yield_now().await;
    }

    #[tokio::test]
    async fn replay_only_ingest_checkpoints_zero_index_boundary_and_doctor_checks_blobs()
    -> Result<()> {
        let root = tempfile::tempdir()?;
        let app = crate::app::AppState::open(crate::config::Config {
            addr: "127.0.0.1:0".parse()?,
            data_dir: root.path().to_owned(),
            base_url: "http://localhost:8080".parse()?,
            s3_url: None,
            s3_endpoint: None,
            s3_initialize: false,
        })
        .await?;
        let segment = crate::sentry::replay::decode_envelope(
            include_bytes!("../../tests/fixtures/replay/plain-0.envelope"),
            &[],
        )
        .expect("decode SDK recording")
        .expect("SDK recording pair");
        let blob = super::super::replay::write(root.path(), &segment.recording)?;
        let stored = blob.clone();
        let installation = app.db.call(move |db| {
            let tx = db.transaction()?;
            tx.execute("INSERT INTO projects(id,slug,name,created_at_us,updated_at_us) VALUES(1,'replay','Replay',0,0)", [])?;
            crate::db::replays::accept(&tx, 1, &crate::db::replays::PreparedReplay {
                metadata: segment.metadata, blob: stored, recording_bytes: segment.recording.len(), frustration: Default::default(),
            }, crate::model::now_us()?)?;
            tx.commit()?;
            db.query_row("SELECT installation_id FROM runtime_state", [], |r| r.get::<_,String>(0)).map_err(Into::into)
        }).await?;
        let store = Arc::new(MemoryStore::default());
        store
            .put(
                "installation.json",
                serde_json::to_vec(&super::super::remote::InstallationDocument {
                    format_version: 1,
                    installation_id: installation,
                    created_at_us: 1,
                })?,
                true,
            )
            .expect("installation document");
        let backup = BackupCoordinator::new(
            app.db.clone(),
            root.path(),
            store.clone(),
            app.disk_budget.clone(),
        );
        let indexer = crate::indexer::Indexer::start_with_backup(
            app.db.clone(),
            root.path(),
            Some(backup.clone()),
        )
        .await?;
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if !app
                    .db
                    .call(|db| crate::db::replays::backup_pending(db))
                    .await?
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            anyhow::Ok(())
        })
        .await??;
        indexer.shutdown().await?;
        backup.shutdown().await;
        let latest: super::super::remote::LatestDocument =
            serde_json::from_slice(&store.get_small("latest.json", 4096).await?)?;
        let checkpoint_bytes = store
            .get_small(
                &format!("checkpoints/{}.json", latest.checkpoint_id),
                1024 * 1024,
            )
            .await
            .expect("checkpoint document");
        let document: CheckpointDocument =
            serde_json::from_slice(&checkpoint_bytes).expect("decode checkpoint");
        assert_eq!(document.cut.boundary, Boundary::default());
        assert_eq!(document.cut.replay_blobs, vec![blob.clone()]);
        assert!(document.cut.replay_revision > 0);
        assert!(crate::operations::doctor(root.path())?.ok);
        let path = root.path().join(&blob.key);
        let original = std::fs::read(&path)?;
        std::fs::write(&path, b"corrupt")?;
        assert!(crate::operations::doctor(root.path()).is_err());
        assert_eq!(std::fs::read(path)?, b"corrupt", "doctor is read-only");
        assert!(!original.is_empty());
        Ok(())
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
        let manifest = active
            .seal(
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
            )
            .expect("seal backup fixture");
        let size = i64::try_from(manifest::local_size(&shard_path, &manifest)?)?;
        let lock = Arc::new(File::open(data)?);
        let db = DbWorker::start(&data.join("meta.db"), lock)?;
        let install = installation.clone();
        let id = shard_id.clone();
        db.call(move |connection| {
            connection
                .execute(
                    "INSERT INTO runtime_state(singleton,installation_id,storage_generation,
                    next_ingest_seq,last_applied_inbox_id,last_applied_ingest_seq)
                 VALUES(1,?1,?2,2,1,1)",
                    rusqlite::params![install, uuid::Uuid::new_v4().to_string()],
                )
                .expect("insert backup runtime state");
            connection
                .execute(
                    "INSERT INTO shards(id,schema_version,format_version,tokenizer_version,state,
                    last_applied_inbox_id,record_count,size_bytes,created_at_us,sealed_at_us)
                 VALUES(?1,1,?2,1,'local',1,0,?3,1,1)",
                    rusqlite::params![id, crate::db::shards::FORMAT_VERSION, size],
                )
                .expect("insert backup shard");
            Ok(())
        })
        .await?;
        let replay = crate::sentry::replay::decode_envelope(
            include_bytes!("../../tests/fixtures/replay/plain-0.envelope"),
            &[],
        )
        .expect("decode Replay fixture")
        .expect("Replay fixture pair");
        let replay_raw = replay.recording.clone();
        let replay_blob = super::super::replay::write(data, &replay.recording)?;
        let blob_for_db = replay_blob.clone();
        db.call(move |db| {
            let tx=db.transaction()?;
            tx.execute("INSERT INTO projects(id,slug,name,created_at_us,updated_at_us) VALUES(1,'replay','Replay',0,0)",[])?;
            crate::db::replays::accept(&tx,1,&crate::db::replays::PreparedReplay{metadata:replay.metadata,blob:blob_for_db,recording_bytes:replay.recording.len(),frustration:Default::default()},crate::model::now_us()?)?;
            tx.commit()?;Ok(())
        }).await?;
        let store = Arc::new(MemoryStore::default());
        store
            .put(
                "installation.json",
                serde_json::to_vec(&super::super::remote::InstallationDocument {
                    format_version: 1,
                    installation_id: installation.clone(),
                    created_at_us: 1,
                })?,
                false,
            )
            .expect("installation document");
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
        assert!(
            coordinator.try_maintenance_permit().is_none(),
            "pinned backup must exclude Replay collection"
        );
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

        let checkpoint_key = format!("checkpoints/{}.json", verified.3.as_deref().unwrap());
        let checkpoint = store
            .0
            .lock()
            .unwrap()
            .remove(&checkpoint_key)
            .expect("published checkpoint document");
        assert!(
            coordinator
                .begin_cut()
                .await?
                .context("job with missing recovery checkpoint")?
                .complete()
                .await
                .is_err()
        );
        store.put(&checkpoint_key, checkpoint, true)?;

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
        let replay_refs = crate::db::replays::blob_references(&restored_db)?;
        assert_eq!(replay_refs, vec![replay_blob.clone()]);
        assert_eq!(
            super::super::replay::read(&restored.directory, &replay_blob)?,
            replay_raw
        );
        // A SQLite snapshot without its referenced recording is not a valid checkpoint.
        store.0.lock().unwrap().remove(&replay_blob.key);
        assert!(
            super::super::remote::prepare_restore(
                store.as_ref(),
                &installation,
                &data.join("missing-replay-restore"),
                Default::default(),
                || false
            )
            .await
            .is_err()
        );

        Ok(())
    }
}
