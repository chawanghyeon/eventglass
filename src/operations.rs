//! Read-only operational status and filesystem/database consistency checks.

use std::{collections::HashSet, path::Path};

use anyhow::{Context, Result, ensure};
use rusqlite::Connection;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Status {
    pub version: &'static str,
    pub ready: bool,
    pub ingest_accepting: bool,
    pub installation_id: String,
    pub storage_generation: String,
    pub applied_inbox_id: String,
    pub applied_ingest_seq: String,
    pub inbox_records: String,
    pub inbox_bytes: String,
    pub database_bytes: String,
    pub wal_bytes: String,
    pub disk: DiskView,
    pub shards: ShardStatus,
    pub backup: BackupStatus,
    pub alerts: AlertStatus,
}

#[derive(Debug, Serialize)]
pub struct DiskView {
    pub total_bytes: String,
    pub free_bytes: String,
    pub reserved_bytes: String,
    pub minimum_free_bytes: String,
    pub ingest_accepting: bool,
}

#[derive(Debug, Serialize)]
pub struct ShardStatus {
    pub active: String,
    pub local: String,
    pub remote_verified: String,
    pub remote_only: String,
    pub records: String,
    pub catalog_bytes: String,
    pub recoverable_records: String,
}

#[derive(Debug, Serialize)]
pub struct BackupStatus {
    pub configured: bool,
    pub state: &'static str,
    pub latest_checkpoint_id: Option<String>,
    pub recoverable_through_ingest_seq: Option<String>,
    pub lag_records: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct AlertStatus {
    pub pending_deliveries: String,
    pub failed_deliveries: String,
    pub evaluation_failures: String,
}

pub fn status(
    db: &Connection,
    data_dir: &Path,
    disk: crate::storage::budget::DiskStatus,
    ready: bool,
    s3_configured: bool,
) -> Result<Status> {
    let runtime: (String, String, i64, i64, i64, i64) = db.query_row(
        "SELECT installation_id,storage_generation,last_applied_inbox_id,
                last_applied_ingest_seq,inbox_records,inbox_bytes
         FROM runtime_state WHERE singleton=1",
        [],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        },
    )?;
    let shard_counts: (i64, i64, i64, i64, i64, i64, i64) = db.query_row(
        "SELECT
           coalesce(sum(CASE WHEN state='active' THEN 1 ELSE 0 END),0),
           coalesce(sum(CASE WHEN state='local' THEN 1 ELSE 0 END),0),
           coalesce(sum(CASE WHEN state='remote_verified' THEN 1 ELSE 0 END),0),
           coalesce(sum(CASE WHEN state='remote_only' THEN 1 ELSE 0 END),0),
           coalesce(sum(record_count),0),coalesce(sum(size_bytes),0),
           coalesce(sum(CASE WHEN state IN ('remote_verified','remote_only') THEN record_count ELSE 0 END),0)
         FROM shards",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?)),
    )?;
    let checkpoint: Option<(String, i64)> = db
        .query_row(
            "SELECT recovery_checkpoint_id,max(max_ingest_seq)
             FROM shards WHERE recovery_checkpoint_id IS NOT NULL
             GROUP BY recovery_checkpoint_id ORDER BY max(max_ingest_seq) DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let alert: (i64, i64, i64) = db.query_row(
        "SELECT
           (SELECT count(*) FROM alert_deliveries WHERE state='pending'),
           (SELECT count(*) FROM alert_deliveries WHERE state='failed'),
           (SELECT count(*) FROM alerts WHERE deleted_at_us IS NULL AND last_evaluation_error IS NOT NULL)",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    let database_bytes = file_size(&data_dir.join("meta.db"))?;
    let wal_bytes = optional_file_size(&data_dir.join("meta.db-wal"))?;
    let recoverable = checkpoint.as_ref().map(|(_, sequence)| *sequence);
    let backup_state = if !s3_configured {
        "disabled"
    } else if checkpoint.is_none() {
        "no_checkpoint"
    } else if recoverable == Some(runtime.3) && !crate::db::replays::backup_pending(db)? {
        "current"
    } else {
        "lagging"
    };
    Ok(Status {
        version: crate::VERSION,
        ready,
        ingest_accepting: ready && disk.ingest_accepting,
        installation_id: runtime.0,
        storage_generation: runtime.1,
        applied_inbox_id: runtime.2.to_string(),
        applied_ingest_seq: runtime.3.to_string(),
        inbox_records: runtime.4.to_string(),
        inbox_bytes: runtime.5.to_string(),
        database_bytes: database_bytes.to_string(),
        wal_bytes: wal_bytes.to_string(),
        disk: DiskView {
            total_bytes: disk.total_bytes.to_string(),
            free_bytes: disk.free_bytes.to_string(),
            reserved_bytes: disk.reserved_bytes.to_string(),
            minimum_free_bytes: disk.minimum_free_bytes.to_string(),
            ingest_accepting: disk.ingest_accepting,
        },
        shards: ShardStatus {
            active: shard_counts.0.to_string(),
            local: shard_counts.1.to_string(),
            remote_verified: shard_counts.2.to_string(),
            remote_only: shard_counts.3.to_string(),
            records: shard_counts.4.to_string(),
            catalog_bytes: shard_counts.5.to_string(),
            recoverable_records: shard_counts.6.to_string(),
        },
        backup: BackupStatus {
            configured: s3_configured,
            state: backup_state,
            latest_checkpoint_id: checkpoint.as_ref().map(|value| value.0.clone()),
            recoverable_through_ingest_seq: recoverable.map(|value| value.to_string()),
            lag_records: recoverable.map(|value| runtime.3.saturating_sub(value).to_string()),
        },
        alerts: AlertStatus {
            pending_deliveries: alert.0.to_string(),
            failed_deliveries: alert.1.to_string(),
            evaluation_failures: alert.2.to_string(),
        },
    })
}

#[derive(Debug, Serialize)]
pub struct DoctorReport {
    pub ok: bool,
    pub schema_version: String,
    pub installation_id: String,
    pub storage_generation: String,
    pub checked_local_shards: String,
    pub checked_remote_only_shards: String,
}

pub fn doctor(data_dir: &Path) -> Result<DoctorReport> {
    let db = crate::db::inspect(&data_dir.join("meta.db"))?;
    doctor_connection(&db, data_dir)
}

pub fn doctor_connection(db: &Connection, data_dir: &Path) -> Result<DoctorReport> {
    let (installation, generation): (String, String) = db.query_row(
        "SELECT installation_id,storage_generation FROM runtime_state WHERE singleton=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    uuid::Uuid::parse_str(&installation).context("invalid installation identity")?;
    uuid::Uuid::parse_str(&generation).context("invalid storage generation")?;
    crate::db::shards::startup(db)?;
    let local_catalog = crate::db::shards::local_catalog(db)?
        .into_iter()
        .map(|item| (item.id.clone(), item))
        .collect::<std::collections::HashMap<_, _>>();
    let mut local = 0u64;
    let mut remote_only = 0u64;
    let mut catalog_ids = HashSet::new();
    let mut statement = db.prepare("SELECT id,state FROM shards ORDER BY id")?;
    for row in statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })? {
        let (id, state) = row?;
        ensure!(catalog_ids.insert(id.clone()), "duplicate shard identity");
        let path = data_dir.join("shards").join(&id);
        match state.as_str() {
            "active" => {
                ensure!(path.is_dir(), "active shard directory is missing");
                ensure!(
                    !path.join(crate::storage::manifest::NAME).exists(),
                    "active shard has a sealed manifest"
                );
                local += 1;
            }
            "local" | "remote_verified" => {
                let manifest = crate::storage::manifest::verify(&path, &installation, &id)?;
                let size = crate::storage::manifest::local_size(&path, &manifest)?;
                let catalog = local_catalog
                    .get(&id)
                    .context("local shard is missing from catalog")?;
                ensure!(
                    catalog.matches(&manifest, size),
                    "local shard differs from catalog"
                );
                local += 1;
            }
            "remote_only" => {
                ensure!(
                    !path.exists(),
                    "remote-only shard still has a local directory"
                );
                remote_only += 1;
            }
            _ => anyhow::bail!("unsupported shard state"),
        }
    }
    drop(statement);
    let root = data_dir.join("shards");
    if root.is_dir() {
        for entry in std::fs::read_dir(root)? {
            let entry = entry?;
            let id = entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("non-UTF-8 shard entry"))?;
            ensure!(catalog_ids.contains(&id), "unregistered shard entry");
        }
    }
    // Verify each immutable recording sequentially, without retaining decoded sessions.
    for reference in crate::db::replays::blob_references(db)? {
        if let Err(error) = crate::storage::replay::read(data_dir, &reference) {
            // Standalone CLI doctor can overlap the server's live retention sweep.
            // A removed reference is no longer a consistency obligation.
            let referenced: bool = db.query_row(
                "SELECT EXISTS(SELECT 1 FROM replay_segments WHERE blob_sha256=?1)",
                [&reference.sha256],
                |row| row.get(0),
            )?;
            if referenced {
                return Err(error)
                    .with_context(|| format!("invalid Replay blob {}", reference.key));
            }
        }
    }
    Ok(DoctorReport {
        ok: true,
        schema_version: crate::db::SCHEMA_VERSION.to_string(),
        installation_id: installation,
        storage_generation: generation,
        checked_local_shards: local.to_string(),
        checked_remote_only_shards: remote_only.to_string(),
    })
}

fn file_size(path: &Path) -> Result<u64> {
    let metadata = std::fs::symlink_metadata(path)?;
    ensure!(
        metadata.file_type().is_file(),
        "operational file is not regular"
    );
    Ok(metadata.len())
}

fn optional_file_size(path: &Path) -> Result<u64> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            ensure!(
                metadata.file_type().is_file(),
                "operational file is not regular"
            );
            Ok(metadata.len())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
    }
}

use rusqlite::OptionalExtension;

/// Completed Sentry transport requests since this process started. No payload or identity labels.
#[derive(Default)]
pub struct IngestStats {
    counts: [std::sync::atomic::AtomicU64; 8],
}
impl IngestStats {
    pub fn record(&self, status: u16) {
        let bucket = match status {
            200..=299 => 0,
            400 | 415 | 422 => 1,
            401 | 403 => 2,
            409 => 3,
            413 => 4,
            429 => 5,
            500..=599 => 6,
            _ => 7,
        };
        self.counts[bucket].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    pub fn snapshot(&self) -> std::collections::BTreeMap<&'static str, String> {
        [
            "accepted",
            "invalid_payload",
            "unauthorized",
            "conflict",
            "too_large",
            "busy",
            "server_error",
            "other",
        ]
        .into_iter()
        .zip(
            self.counts
                .iter()
                .map(|v| v.load(std::sync::atomic::Ordering::Relaxed).to_string()),
        )
        .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operational_file_sizes_reject_non_regular_and_io_failures() -> Result<()> {
        let root = tempfile::tempdir()?;
        let file = root.path().join("file");
        std::fs::write(&file, b"abc")?;
        assert_eq!(file_size(&file)?, 3);
        assert_eq!(optional_file_size(&root.path().join("missing"))?, 0);
        assert!(file_size(root.path()).is_err());
        assert!(optional_file_size(root.path()).is_err());
        assert!(file_size(&root.path().join("missing")).is_err());
        Ok(())
    }

    #[test]
    fn ingest_stats_classify_every_public_status_family() {
        let stats = IngestStats::default();
        for status in [202, 400, 401, 409, 413, 429, 503, 302] {
            stats.record(status);
        }
        assert!(stats.snapshot().values().all(|count| count == "1"));
    }
}

#[derive(Serialize)]
pub struct ReplayStatus {
    pub active_replays: String,
    pub partial_replays: String,
    pub expired_replays: String,
    pub segments: String,
    /// Referenced compressed local bytes, deduplicated across sessions/projects.
    pub referenced_bytes: String,
    pub backup_pending: bool,
}
pub fn replay_status(db: &Connection, now: i64) -> Result<ReplayStatus> {
    let (active, partial, expired): (i64,i64,i64) = db.query_row(
        "SELECT coalesce(sum(expires_at_us>?1),0), coalesce(sum(expires_at_us>?1 AND segment_count<>max_segment_id+1),0), coalesce(sum(expires_at_us<=?1),0) FROM replays", [now],
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    let segments: i64 = db.query_row("SELECT count(*) FROM replay_segments", [], |r| r.get(0))?;
    let bytes: i64 = db.query_row("SELECT coalesce(sum(size),0) FROM (SELECT max(blob_size) AS size FROM replay_segments GROUP BY blob_sha256)", [], |r|r.get(0))?;
    Ok(ReplayStatus {
        active_replays: active.to_string(),
        partial_replays: partial.to_string(),
        expired_replays: expired.to_string(),
        segments: segments.to_string(),
        referenced_bytes: bytes.to_string(),
        backup_pending: crate::db::replays::backup_pending(db)?,
    })
}
