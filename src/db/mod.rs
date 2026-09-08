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
pub const SCHEMA_VERSION: i64 = 1;

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
    let expected = format!("{:x}", Sha256::digest(INITIAL_SCHEMA.as_bytes()));
    if has_migrations {
        let max: i64 = tx.query_row(
            "SELECT coalesce(max(version),0) FROM schema_migrations",
            [],
            |row| row.get(0),
        )?;
        if max != SCHEMA_VERSION {
            bail!("unsupported metadata schema version {max}");
        }
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
        db.execute("UPDATE schema_migrations SET version=2", [])?;
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
