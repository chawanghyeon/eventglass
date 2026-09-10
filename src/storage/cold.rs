//! On-demand, single-flight installation of checkpoint-verified remote shards.

use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::{Context, Result, ensure};
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;

use crate::db::worker::DbWorker;

use super::{archive, remote::RestoreLimits, s3::ObjectStore};

#[derive(Clone)]
pub struct ColdStorage {
    db: DbWorker,
    data_dir: PathBuf,
    installation_id: String,
    store: Arc<dyn ObjectStore>,
    registry: super::registry::Registry,
    disk: super::budget::DiskBudget,
    gate: Arc<Semaphore>,
}

struct RemoteShard {
    key: String,
    sha256: String,
    expanded_bytes: u64,
}

type CatalogRow = (String, Option<String>, Option<String>, Option<String>, i64);

fn remote_shard(row: CatalogRow) -> Result<Option<RemoteShard>> {
    match row.0.as_str() {
        "active" | "local" | "remote_verified" => Ok(None),
        "remote_only" => {
            ensure!(row.3.is_some(), "cold shard has no recovery checkpoint");
            Ok(Some(RemoteShard {
                key: row.1.context("cold shard has no archive key")?,
                sha256: row.2.context("cold shard has no archive checksum")?,
                expanded_bytes: u64::try_from(row.4)
                    .context("cold shard has invalid local size")?,
            }))
        }
        _ => anyhow::bail!("unsupported shard state"),
    }
}

impl ColdStorage {
    pub fn new(
        db: DbWorker,
        data_dir: &Path,
        installation_id: String,
        store: Arc<dyn ObjectStore>,
        registry: super::registry::Registry,
        disk: super::budget::DiskBudget,
    ) -> Self {
        Self {
            db,
            data_dir: data_dir.to_owned(),
            installation_id,
            store,
            registry,
            disk,
            gate: Arc::new(Semaphore::new(1)),
        }
    }

    pub async fn evict_remote_verified(&self, shard_id: &str) -> Result<bool> {
        let _permit = self
            .gate
            .acquire()
            .await
            .context("cold storage is closed")?;
        self.evict_one(shard_id).await
    }

    pub async fn reclaim_for_ingest(&self, disk: &super::budget::DiskBudget) -> Result<usize> {
        let _permit = self
            .gate
            .acquire()
            .await
            .context("cold storage is closed")?;
        let cutoff = crate::model::now_us()?.saturating_sub(30_000_000);
        let candidates = self
            .db
            .call(move |db| {
                let mut statement = db.prepare(
                    "SELECT id FROM shards
                     WHERE state='remote_verified' AND remote_archive_key IS NOT NULL
                       AND archive_sha256 IS NOT NULL AND recovery_checkpoint_id IS NOT NULL
                       AND (last_accessed_at_us IS NULL OR last_accessed_at_us<?1)
                     ORDER BY coalesce(last_accessed_at_us,sealed_at_us,created_at_us),id",
                )?;
                Ok(statement
                    .query_map([cutoff], |row| row.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?)
            })
            .await?;
        let mut reclaimed = 0usize;
        for shard_id in candidates {
            if disk.status()?.ingest_accepting {
                break;
            }
            reclaimed += usize::from(self.evict_one(&shard_id).await?);
        }
        Ok(reclaimed)
    }

    async fn evict_one(&self, shard_id: &str) -> Result<bool> {
        uuid::Uuid::parse_str(shard_id).context("invalid eviction shard ID")?;
        let id = shard_id.to_owned();
        let registry = self.registry.clone();
        self.db
            .call(move |db| {
                registry.evict_local(
                    &id,
                    || {
                        Ok(db.execute(
                            "UPDATE shards SET state='remote_only'
                             WHERE id=?1 AND state='remote_verified'
                               AND remote_archive_key IS NOT NULL
                               AND archive_sha256 IS NOT NULL
                               AND recovery_checkpoint_id IS NOT NULL",
                            [&id],
                        )? == 1)
                    },
                    || {
                        ensure!(
                            db.execute(
                                "UPDATE shards SET state='remote_verified'
                                 WHERE id=?1 AND state='remote_only'",
                                [&id],
                            )? == 1,
                            "failed to roll back shard eviction"
                        );
                        Ok(())
                    },
                )
            })
            .await
    }

    pub async fn ensure_local(&self, shard_ids: &[String]) -> Result<usize> {
        let permit = Arc::new(
            Arc::clone(&self.gate)
                .acquire_owned()
                .await
                .context("cold storage is closed")?,
        );
        let mut hydrated = 0usize;
        for shard_id in shard_ids {
            hydrated += usize::from(self.ensure_one(shard_id, &permit).await?);
        }
        Ok(hydrated)
    }

    async fn ensure_one(
        &self,
        shard_id: &str,
        permit: &Arc<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<bool> {
        uuid::Uuid::parse_str(shard_id).context("invalid cold shard ID")?;
        let id = shard_id.to_owned();
        let remote = self
            .db
            .call(move |db| {
                let row = db.query_row(
                    "SELECT state,remote_archive_key,archive_sha256,recovery_checkpoint_id,size_bytes
                     FROM shards WHERE id=?1",
                    [&id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, Option<String>>(1)?,
                            row.get::<_, Option<String>>(2)?,
                            row.get::<_, Option<String>>(3)?,
                            row.get::<_, i64>(4)?,
                        ))
                    },
                )?;
                remote_shard(row)
            })
            .await?;
        let Some(remote) = remote else {
            return Ok(false);
        };
        let expanded_limit = remote.expanded_bytes.max(1024 * 1024);
        let reservation_bytes = expanded_limit
            .checked_mul(3)
            .context("cold shard reservation overflow")?;
        let disk_reservation = Arc::new(self.disk.reserve(reservation_bytes)?);

        let root = self.data_dir.join("shards");
        std::fs::create_dir_all(&root)?;
        let existing = root.join(shard_id);
        if existing.exists() {
            let path = existing.clone();
            let installation = self.installation_id.clone();
            let id = shard_id.to_owned();
            let resources = (Arc::clone(permit), Arc::clone(&disk_reservation));
            let size = tokio::task::spawn_blocking(move || {
                let _resources = resources;
                let manifest = super::manifest::verify(&path, &installation, &id)?;
                super::manifest::local_size(&path, &manifest)
            })
            .await??;
            self.promote(shard_id, &remote, size).await?;
            return Ok(true);
        }
        let compressed = self.data_dir.join(format!(
            ".cold-{}-{}.tar.gz",
            shard_id,
            uuid::Uuid::new_v4()
        ));
        let cleanup = Arc::new(RemoveFile(compressed.clone()));
        let limits = RestoreLimits::default();
        let metadata = self
            .store
            .download(
                &remote.key,
                &compressed,
                limits.shard_archive_bytes.min(expanded_limit * 2),
            )
            .await?;
        ensure!(
            metadata.key == remote.key,
            "cold download returned a different object"
        );
        let archive_path = compressed.clone();
        let expected_hash = remote.sha256.clone();
        let resources = (
            Arc::clone(permit),
            Arc::clone(&disk_reservation),
            Arc::clone(&cleanup),
        );
        tokio::task::spawn_blocking(move || {
            let _resources = resources;
            verify_file(&archive_path, &expected_hash)
        })
        .await??;

        let archive_path = compressed.clone();
        let destination = root.join(shard_id);
        let installation = self.installation_id.clone();
        let id = shard_id.to_owned();
        let mut cancellation = Cancellation::new();
        let task_cancelled = Arc::clone(&cancellation.flag);
        let resources = (
            Arc::clone(permit),
            Arc::clone(&disk_reservation),
            Arc::clone(&cleanup),
        );
        let hydration = tokio::task::spawn_blocking(move || {
            let _resources = resources;
            archive::hydrate(
                &archive_path,
                &destination,
                &installation,
                &id,
                limits.shard_expanded_bytes.min(expanded_limit),
                || task_cancelled.load(Ordering::Relaxed),
            )
        });
        let manifest = hydration.await??;
        cancellation.disarm();
        let size = super::manifest::local_size(&root.join(shard_id), &manifest)?;
        self.promote(shard_id, &remote, size).await?;
        File::open(&root)?.sync_all()?;
        drop(cleanup);
        Ok(true)
    }

    async fn promote(&self, shard_id: &str, remote: &RemoteShard, size: u64) -> Result<()> {
        let id = shard_id.to_owned();
        let key = remote.key.clone();
        let hash = remote.sha256.clone();
        let changed = self
            .db
            .call(move |db| {
                Ok(db.execute(
                    "UPDATE shards SET state='remote_verified',size_bytes=?1,last_accessed_at_us=?2
                     WHERE id=?3 AND state='remote_only' AND remote_archive_key=?4
                       AND archive_sha256=?5 AND recovery_checkpoint_id IS NOT NULL",
                    rusqlite::params![i64::try_from(size)?, crate::model::now_us()?, id, key, hash],
                )?)
            })
            .await?;
        ensure!(
            changed == 1,
            "cold shard catalog changed during installation"
        );
        Ok(())
    }
}

fn verify_file(path: &Path, expected: &str) -> Result<()> {
    ensure!(expected.len() == 64, "invalid archive checksum");
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 32 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    ensure!(
        format!("{:x}", digest.finalize()) == expected,
        "archive checksum mismatch"
    );
    Ok(())
}

struct Cancellation {
    flag: Arc<AtomicBool>,
    active: bool,
}

impl Cancellation {
    fn new() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
            active: true,
        }
    }

    fn disarm(&mut self) {
        self.active = false;
    }
}

impl Drop for Cancellation {
    fn drop(&mut self) {
        if self.active {
            self.flag.store(true, Ordering::Relaxed);
        }
    }
}

struct RemoveFile(PathBuf);

impl Drop for RemoveFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(state: &str) -> CatalogRow {
        (
            state.into(),
            Some("archive".into()),
            Some("0".repeat(64)),
            Some("checkpoint".into()),
            10,
        )
    }

    #[test]
    fn catalog_rows_fail_closed_and_cancellation_is_armed_until_disarmed() {
        for state in ["active", "local", "remote_verified"] {
            assert!(remote_shard(row(state)).unwrap().is_none());
        }
        let remote = remote_shard(row("remote_only")).unwrap().unwrap();
        assert_eq!(
            (remote.key.as_str(), remote.expanded_bytes),
            ("archive", 10)
        );
        for damaged in [
            (
                "remote_only".into(),
                None,
                Some("0".repeat(64)),
                Some("c".into()),
                1,
            ),
            (
                "remote_only".into(),
                Some("a".into()),
                None,
                Some("c".into()),
                1,
            ),
            (
                "remote_only".into(),
                Some("a".into()),
                Some("0".repeat(64)),
                None,
                1,
            ),
            (
                "remote_only".into(),
                Some("a".into()),
                Some("0".repeat(64)),
                Some("c".into()),
                -1,
            ),
            row("unknown"),
        ] {
            assert!(remote_shard(damaged).is_err());
        }

        let armed = Cancellation::new();
        let flag = Arc::clone(&armed.flag);
        drop(armed);
        assert!(flag.load(Ordering::Relaxed));
        let mut disarmed = Cancellation::new();
        let flag = Arc::clone(&disarmed.flag);
        disarmed.disarm();
        drop(disarmed);
        assert!(!flag.load(Ordering::Relaxed));
    }

    #[test]
    fn archive_checksum_and_temporary_file_cleanup_are_exact() {
        let directory = tempfile::tempdir().expect("cold fixture directory");
        let path = directory.path().join("archive");
        std::fs::write(&path, b"archive").expect("archive fixture");
        assert!(verify_file(&path, &crate::storage::remote::sha256(b"archive")).is_ok());
        assert!(verify_file(&path, "short").is_err());
        assert!(verify_file(&path, &"0".repeat(64)).is_err());
        drop(RemoveFile(path.clone()));
        assert!(!path.exists());
    }
}
