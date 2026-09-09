//! SQLite schema ownership and connection policy.

pub mod alerts;
pub mod auth;
pub mod indexer;
pub mod ingest;
pub mod issue_detail;
pub mod issues;
pub mod projects;
pub mod search;
pub mod shards;
pub mod worker;

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior};
use sha2::{Digest, Sha256};

const INITIAL_SCHEMA: &str = include_str!("../../migrations/0001_initial.sql");
// Compatibility bridge: this release still creates v1, but can safely read/write
// the unchanged v1 tables alongside the additive Replay extension after rollback.
const REPLAY_SCHEMA: &str = include_str!("../../migrations/0002_replays.sql");
pub const SCHEMA_VERSION: i64 = 1;
const MAX_COMPATIBLE_SCHEMA_VERSION: i64 = 2;

/// Open a database after the caller has acquired the exclusive data-directory lock.
/// A corrupt or newer database is returned as an error, never replaced.
pub fn open(path: &Path) -> Result<Connection> {
    let mut connection = Connection::open(path).context("open metadata database")?;
    configure(&connection)?;
    let integrity: String = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
    if integrity != "ok" {
        bail!("metadata integrity check failed");
    }
    migrate(&mut connection)?;
    Ok(connection)
}

pub fn open_reader(path: &Path) -> Result<Connection> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    connection.pragma_update(None, "cache_size", -4096)?;
    Ok(connection)
}

/// Opens and verifies an existing metadata database without creating or migrating it.
pub fn inspect(path: &Path) -> Result<Connection> {
    let metadata = std::fs::symlink_metadata(path).context("inspect metadata database")?;
    if !metadata.file_type().is_file() {
        bail!("metadata database is not a regular file");
    }
    let connection = open_reader(path)?;
    let integrity: String = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
    if integrity != "ok" {
        bail!("metadata integrity check failed");
    }
    let version: i64 = connection.query_row(
        "SELECT coalesce(max(version),0) FROM schema_migrations",
        [],
        |row| row.get(0),
    )?;
    if !(SCHEMA_VERSION..=MAX_COMPATIBLE_SCHEMA_VERSION).contains(&version) {
        bail!("unsupported metadata schema version {version}");
    }
    let checksum: String = connection.query_row(
        "SELECT checksum FROM schema_migrations WHERE version=?1",
        [SCHEMA_VERSION],
        |row| row.get(0),
    )?;
    if checksum != initial_schema_checksum() {
        bail!("applied migration checksum differs from this binary");
    }
    verify_replay_extension(&connection, version)?;
    let foreign_key_errors: i64 =
        connection.query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |row| {
            row.get(0)
        })?;
    if foreign_key_errors != 0 {
        bail!("metadata foreign key check failed");
    }
    Ok(connection)
}

fn configure(connection: &Connection) -> Result<()> {
    connection.busy_timeout(Duration::from_secs(5))?;
    // Must precede the first table, and is a no-op for an established database.
    connection.pragma_update(None, "auto_vacuum", "INCREMENTAL")?;
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    connection.pragma_update(None, "cache_size", -4096)?;
    Ok(())
}

fn migrate(connection: &mut Connection) -> Result<()> {
    let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let has_migrations: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations')",
        [],
        |row| row.get(0),
    )?;
    let expected = initial_schema_checksum();
    if has_migrations {
        let max: i64 = tx.query_row(
            "SELECT coalesce(max(version),0) FROM schema_migrations",
            [],
            |row| row.get(0),
        )?;
        if !(SCHEMA_VERSION..=MAX_COMPATIBLE_SCHEMA_VERSION).contains(&max) {
            bail!("unsupported metadata schema version {max}");
        }
        verify_replay_extension(&tx, max)?;
        let checksum: Option<String> = tx
            .query_row(
                "SELECT checksum FROM schema_migrations WHERE version=1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if checksum.as_deref() != Some(&expected) {
            bail!("applied migration checksum differs from this binary");
        }
    } else {
        let tables: i64 = tx.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            [],
            |row| row.get(0),
        )?;
        if tables != 0 {
            bail!("unrecognized existing metadata schema; refusing initialization");
        }
        tx.execute_batch(INITIAL_SCHEMA)?;
        tx.execute(
            "INSERT INTO schema_migrations(version,checksum,applied_at_us) VALUES(1,?1,?2)",
            rusqlite::params![expected, crate::model::now_us()?],
        )?;
    }
    tx.commit()?;
    Ok(())
}

fn initial_schema_checksum() -> String {
    format!("{:x}", Sha256::digest(INITIAL_SCHEMA.as_bytes()))
}

/// v2 only adds independent tables/indexes and never changes the v1 contracts.
/// Unknown or modified extensions remain fail-closed; rollback never drops data.
fn verify_replay_extension(connection: &Connection, version: i64) -> Result<()> {
    if version < 2 {
        return Ok(());
    }
    let checksum: Option<String> = connection
        .query_row(
            "SELECT checksum FROM schema_migrations WHERE version=2",
            [],
            |r| r.get(0),
        )
        .optional()?;
    let expected = format!("{:x}", Sha256::digest(REPLAY_SCHEMA.as_bytes()));
    if checksum.as_deref() != Some(&expected) {
        bail!("unsupported additive schema checksum");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_reopens_and_enforces_connection_policy() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("meta.db");
        drop(open(&path)?);
        let db = open(&path)?;
        for (pragma, expected) in [("synchronous", 2), ("foreign_keys", 1), ("auto_vacuum", 2)] {
            let value: i64 = db.query_row(&format!("PRAGMA {pragma}"), [], |row| row.get(0))?;
            assert_eq!(value, expected, "{pragma}");
        }
        assert_eq!(
            db.query_row("PRAGMA journal_mode", [], |r| r.get::<_, String>(0))?,
            "wal"
        );
        Ok(())
    }

    #[test]
    fn newer_or_changed_schema_is_never_initialized_over() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("meta.db");
        let db = open(&path)?;
        db.execute("UPDATE schema_migrations SET version=99", [])?;
        drop(db);
        assert!(open(&path).unwrap_err().to_string().contains("unsupported"));
        let db = Connection::open(&path)?;
        db.execute(
            "UPDATE schema_migrations SET version=1,checksum='changed'",
            [],
        )?;
        drop(db);
        assert!(open(&path).unwrap_err().to_string().contains("checksum"));
        Ok(())
    }

    #[test]
    fn additive_replay_schema_can_roll_back_without_dropping_accepted_data() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("meta.db");
        let mut db = open(&path)?;
        assert_eq!(
            db.query_row("SELECT max(version) FROM schema_migrations", [], |r| r
                .get::<_, i64>(0))?,
            1
        );
        let tx = db.transaction()?;
        tx.execute_batch(REPLAY_SCHEMA)?;
        tx.execute(
            "INSERT INTO schema_migrations(version,checksum,applied_at_us) VALUES(2,?1,0)",
            [format!("{:x}", Sha256::digest(REPLAY_SCHEMA.as_bytes()))],
        )?;
        tx.execute_batch("INSERT INTO projects(id,slug,name,created_at_us,updated_at_us) VALUES(1,'preserve','Preserve',0,0); INSERT INTO replays(project_id,replay_id,started_at_ms,finished_at_ms,expires_at_us,metadata) VALUES(1,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',0,1,2,'{}');")?;
        tx.commit()?;
        drop(db);
        let db = open(&path)?;
        assert_eq!(
            db.query_row("SELECT count(*) FROM replays", [], |r| r.get::<_, i64>(0))?,
            1
        );
        db.execute("UPDATE projects SET name='Still writable' WHERE id=1", [])?;
        drop(db);
        drop(inspect(&path)?);
        assert!(
            crate::storage::checkpoint::PinnedSnapshot::open(&path)
                .err()
                .unwrap()
                .to_string()
                .contains("feature-capable")
        );
        let db = Connection::open(&path)?;
        db.execute(
            "UPDATE schema_migrations SET checksum='unknown' WHERE version=2",
            [],
        )?;
        drop(db);
        assert!(open(&path).is_err());
        Ok(())
    }

    #[test]
    fn unrecognized_database_is_preserved() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("meta.db");
        let db = Connection::open(&path)?;
        db.execute_batch(
            "CREATE TABLE precious(value TEXT); INSERT INTO precious VALUES('keep');",
        )?;
        drop(db);
        assert!(open(&path).is_err());
        let db = Connection::open(&path)?;
        assert_eq!(
            db.query_row("SELECT value FROM precious", [], |r| r.get::<_, String>(0))?,
            "keep"
        );
        Ok(())
    }
}
