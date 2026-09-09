//! A pinned SQLite cut and an atomically published backup candidate.

use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OpenFlags, backup::StepResult};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use crate::model::Boundary;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointShard {
    pub id: String,
    pub state: String,
    pub remote_archive_key: Option<String>,
    pub archive_sha256: Option<String>,
    pub last_applied_inbox_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointCut {
    pub format_version: u32,
    pub installation_id: String,
    pub storage_generation: String,
    pub boundary: Boundary,
    pub shards: Vec<CheckpointShard>,
}

#[derive(Debug, Clone, Copy)]
pub struct SnapshotLimits {
    pub deadline: Duration,
    pub wal_growth_bytes: u64,
    pub pages_per_step: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotArtifact {
    pub cut: CheckpointCut,
    pub size: u64,
    pub sha256: String,
}

impl Default for SnapshotLimits {
    fn default() -> Self {
        Self {
            deadline: Duration::from_secs(120),
            wal_growth_bytes: 128 * 1024 * 1024,
            pages_per_step: 64,
        }
    }
}

pub struct PinnedSnapshot {
    source: Connection,
    source_path: PathBuf,
    cut: CheckpointCut,
}

impl PinnedSnapshot {
    /// Returning from this function is the coordinator handshake: BEGIN and all
    /// cut-defining SELECTs have completed on one read snapshot.
    pub fn open(source_path: &Path) -> Result<Self> {
        let source = crate::db::open_reader(source_path)?;
        source.execute_batch("BEGIN")?;
        let (installation_id, storage_generation, inbox_id, ingest_seq, active): (
            String,
            String,
            i64,
            i64,
            Option<String>,
        ) = source.query_row(
            "SELECT installation_id,storage_generation,last_applied_inbox_id,
                    last_applied_ingest_seq,active_shard_id
             FROM runtime_state WHERE singleton=1",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )?;
        ensure!(active.is_none(), "checkpoint cut requires a completed seal");
        ensure!(
            inbox_id >= 0 && ingest_seq >= 0 && (inbox_id == 0) == (ingest_seq == 0),
            "invalid checkpoint boundary"
        );
        let mut statement = source.prepare(
            "SELECT id,state,remote_archive_key,archive_sha256,last_applied_inbox_id
             FROM shards ORDER BY id",
        )?;
        let shards = statement
            .query_map([], |row| {
                Ok(CheckpointShard {
                    id: row.get(0)?,
                    state: row.get(1)?,
                    remote_archive_key: row.get(2)?,
                    archive_sha256: row.get(3)?,
                    last_applied_inbox_id: row.get(4)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);
        ensure!(
            shards.iter().all(|shard| {
                matches!(
                    shard.state.as_str(),
                    "local" | "remote_verified" | "remote_only"
                ) && shard.last_applied_inbox_id <= inbox_id
            }),
            "checkpoint catalog contains an unavailable shard"
        );
        Ok(Self {
            source,
            source_path: source_path.to_owned(),
            cut: CheckpointCut {
                format_version: 1,
                installation_id,
                storage_generation,
                boundary: Boundary {
                    inbox_id,
                    ingest_seq,
                },
                shards,
            },
        })
    }

    pub fn cut(&self) -> &CheckpointCut {
        &self.cut
    }

    /// Reserve the snapshot and newly archived shards from this exact SQLite cut.
    pub fn temporary_bytes(&self) -> Result<u64> {
        let pages: i64 = self
            .source
            .query_row("PRAGMA page_count", [], |row| row.get(0))?;
        let page_size: i64 = self
            .source
            .query_row("PRAGMA page_size", [], |row| row.get(0))?;
        let shards: i64 = self.source.query_row(
            "SELECT coalesce(sum(size_bytes),0) FROM shards WHERE state='local'",
            [],
            |row| row.get(0),
        )?;
        let pages = u64::try_from(pages)?;
        let page_size = u64::try_from(page_size)?;
        let shards = u64::try_from(shards)?;
        pages
            .checked_mul(page_size)
            .and_then(|bytes| bytes.checked_add(shards))
            .and_then(|bytes| bytes.checked_mul(2))
            .and_then(|bytes| bytes.checked_add(1024 * 1024))
            .context("checkpoint disk reservation overflow")
    }

    /// Read recovery provenance from the same pinned snapshot as the catalog.
    pub fn recovery_checkpoints(&self) -> Result<std::collections::BTreeMap<String, String>> {
        let mut statement = self.source.prepare(
            "SELECT id,recovery_checkpoint_id FROM shards
             WHERE state IN ('remote_verified','remote_only')",
        )?;
        Ok(statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?)
    }

    pub fn backup_to(
        self,
        destination: &Path,
        limits: SnapshotLimits,
        mut cancelled: impl FnMut() -> bool,
    ) -> Result<SnapshotArtifact> {
        ensure!(
            limits.pages_per_step > 0,
            "backup page step must be positive"
        );
        ensure!(
            !destination.exists(),
            "checkpoint destination already exists"
        );
        let parent = destination
            .parent()
            .context("checkpoint destination parent")?;
        fs::create_dir_all(parent)?;
        let temporary = parent.join(format!(".checkpoint-{}.tmp", uuid::Uuid::new_v4()));
        let guard = PartialSnapshot(temporary.clone());
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;
        let mut output = Connection::open_with_flags(&temporary, flags)?;
        let started = Instant::now();
        let initial_wal = file_size(&wal_path(&self.source_path))?;
        {
            let backup = rusqlite::backup::Backup::new(&self.source, &mut output)?;
            loop {
                ensure!(!cancelled(), "checkpoint backup cancelled");
                ensure!(
                    started.elapsed() <= limits.deadline,
                    "checkpoint backup deadline exceeded"
                );
                let wal = file_size(&wal_path(&self.source_path))?;
                ensure!(
                    wal.saturating_sub(initial_wal) <= limits.wal_growth_bytes,
                    "checkpoint WAL growth budget exceeded"
                );
                match backup.step(limits.pages_per_step)? {
                    StepResult::Done => break,
                    StepResult::More => {}
                    StepResult::Busy | StepResult::Locked => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    _ => anyhow::bail!("unsupported SQLite backup result"),
                }
            }
        }
        let integrity: String = output.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
        ensure!(
            integrity == "ok",
            "checkpoint snapshot integrity check failed"
        );
        drop(output);
        let mut file = File::open(&temporary)?;
        file.sync_all()?;
        let size = file.metadata()?.len();
        let mut sha = Sha256::new();
        let mut buffer = [0u8; 32 * 1024];
        let mut hashed = 0u64;
        loop {
            ensure!(!cancelled(), "checkpoint backup cancelled");
            ensure!(
                started.elapsed() <= limits.deadline,
                "checkpoint backup deadline exceeded"
            );
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            sha.update(&buffer[..count]);
            hashed += count as u64;
        }
        ensure!(hashed == size, "checkpoint snapshot changed while hashing");
        self.source.execute_batch("ROLLBACK")?;
        fs::rename(&temporary, destination)?;
        File::open(parent)?.sync_all()?;
        drop(guard);
        Ok(SnapshotArtifact {
            cut: self.cut.clone(),
            size,
            sha256: format!("{:x}", sha.finalize()),
        })
    }
}

impl Drop for PinnedSnapshot {
    fn drop(&mut self) {
        let _ = self.source.execute_batch("ROLLBACK");
    }
}

struct PartialSnapshot(PathBuf);

impl Drop for PartialSnapshot {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn wal_path(database: &Path) -> PathBuf {
    let mut value = database.as_os_str().to_owned();
    value.push("-wal");
    value.into()
}

fn file_size(path: &Path) -> Result<u64> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn database(root: &Path) -> Result<(PathBuf, Connection)> {
        let path = root.join("meta.db");
        let writer = crate::db::open(&path)?;
        writer.execute(
            "INSERT INTO runtime_state(singleton,installation_id,storage_generation,next_ingest_seq)
             VALUES(1,?1,?2,1)",
            rusqlite::params![uuid::Uuid::new_v4().to_string(), uuid::Uuid::new_v4().to_string()],
        )?;
        writer.execute(
            "INSERT INTO settings(key,value_json,updated_at_us) VALUES('cut','10',0)",
            [],
        )?;
        Ok((path, writer))
    }

    #[test]
    fn snapshot_stays_at_handshake_cut_while_writes_continue() -> Result<()> {
        let root = tempfile::tempdir()?;
        let (path, writer) = database(root.path())?;
        let snapshot = PinnedSnapshot::open(&path)?;
        writer.execute("UPDATE settings SET value_json='20' WHERE key='cut'", [])?;
        let destination = root.path().join("snapshot.db");
        let artifact = snapshot.backup_to(&destination, SnapshotLimits::default(), || false)?;
        let restored = Connection::open(&destination)?;
        assert_eq!(
            restored.query_row(
                "SELECT value_json FROM settings WHERE key='cut'",
                [],
                |row| row.get::<_, String>(0)
            )?,
            "10"
        );
        assert_eq!(artifact.cut.boundary, Boundary::default());
        assert_eq!(artifact.size, fs::metadata(&destination)?.len());
        assert_eq!(artifact.sha256.len(), 64);
        assert_eq!(
            writer.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row
                .get::<_, i64>(0))?,
            0
        );
        Ok(())
    }

    #[test]
    fn cancelled_snapshot_never_publishes_a_partial_database() -> Result<()> {
        let root = tempfile::tempdir()?;
        let (path, writer) = database(root.path())?;
        writer.execute(
            "INSERT INTO settings(key,value_json,updated_at_us) VALUES('large',?1,0)",
            ["x".repeat(512 * 1024)],
        )?;
        let destination = root.path().join("snapshot.db");
        let error = PinnedSnapshot::open(&path)?
            .backup_to(&destination, SnapshotLimits::default(), || true)
            .unwrap_err();
        assert!(error.to_string().contains("cancelled"));
        assert!(!destination.exists());
        assert_eq!(
            writer.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row
                .get::<_, i64>(0))?,
            0
        );
        Ok(())
    }
}
