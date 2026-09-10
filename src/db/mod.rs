//! SQLite schema ownership and connection policy.

pub mod alerts;
pub mod auth;
pub mod indexer;
pub mod ingest;
pub mod issue_detail;
pub mod issues;
pub mod projects;
pub mod replays;
pub mod search;
pub mod shards;
pub mod worker;

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior};
use sha2::{Digest, Sha256};

const INITIAL_SCHEMA: &str = include_str!("../../migrations/0001_initial.sql");
const REPLAY_SCHEMA: &str = include_str!("../../migrations/0002_replays.sql");
const MIGRATIONS: &[&str] = &[INITIAL_SCHEMA, REPLAY_SCHEMA];
pub const SCHEMA_VERSION: i64 = 2;

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
    if version != SCHEMA_VERSION {
        bail!("unsupported metadata schema version {version}");
    }
    verify_migrations(&connection, version)?;
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
    let version = if has_migrations {
        let version: i64 = tx.query_row(
            "SELECT coalesce(max(version),0) FROM schema_migrations",
            [],
            |r| r.get(0),
        )?;
        if !(1..=SCHEMA_VERSION).contains(&version) {
            bail!("unsupported metadata schema version {version}");
        }
        verify_migrations(&tx, version)?;
        version
    } else {
        let tables: i64 = tx.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            [],
            |r| r.get(0),
        )?;
        if tables != 0 {
            bail!("unrecognized existing metadata schema; refusing initialization");
        }
        0
    };
    for next in version + 1..=SCHEMA_VERSION {
        let sql = MIGRATIONS[(next - 1) as usize];
        tx.execute_batch(sql)?;
        tx.execute(
            "INSERT INTO schema_migrations(version,checksum,applied_at_us) VALUES(?1,?2,?3)",
            rusqlite::params![
                next,
                format!("{:x}", Sha256::digest(sql.as_bytes())),
                crate::model::now_us()?
            ],
        )?;
    }
    tx.commit()?;
    Ok(())
}

fn verify_migrations(connection: &Connection, version: i64) -> Result<()> {
    for applied in 1..=version {
        let checksum: Option<String> = connection
            .query_row(
                "SELECT checksum FROM schema_migrations WHERE version=?1",
                [applied],
                |r| r.get(0),
            )
            .optional()?;
        let expected = format!(
            "{:x}",
            Sha256::digest(MIGRATIONS[(applied - 1) as usize].as_bytes())
        );
        if checksum.as_deref() != Some(&expected) {
            bail!("applied migration checksum differs from this binary");
        }
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
        db.execute(
            "UPDATE schema_migrations SET version=99 WHERE version=2",
            [],
        )
        .expect("mark fixture schema as newer");
        drop(db);
        assert!(open(&path).unwrap_err().to_string().contains("unsupported"));
        let db = Connection::open(&path)?;
        db.execute(
            "UPDATE schema_migrations SET version=2 WHERE version=99",
            [],
        )
        .expect("restore fixture schema version");
        db.execute(
            "UPDATE schema_migrations SET checksum='changed' WHERE version=1",
            [],
        )
        .expect("corrupt fixture migration checksum");
        drop(db);
        assert!(open(&path).unwrap_err().to_string().contains("checksum"));
        Ok(())
    }

    #[test]
    fn v1_upgrade_preserves_original_checksum_and_existing_data() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("meta.db");
        let db = Connection::open(&path)?;
        db.execute_batch(INITIAL_SCHEMA)?;
        let original = format!("{:x}", Sha256::digest(INITIAL_SCHEMA.as_bytes()));
        db.execute(
            "INSERT INTO schema_migrations(version,checksum,applied_at_us) VALUES(1,?1,0)",
            [&original],
        )
        .expect("install v1 migration fixture");
        db.execute_batch("INSERT INTO projects(id,slug,name,created_at_us,updated_at_us) VALUES(1,'upgrade','Preserve',0,0);")?;
        drop(db);
        let db = open(&path)?;
        assert_eq!(
            db.query_row(
                "SELECT checksum FROM schema_migrations WHERE version=1",
                [],
                |r| r.get::<_, String>(0)
            )
            .expect("read original v1 checksum"),
            original
        );
        assert_eq!(
            db.query_row("SELECT name FROM projects WHERE id=1", [], |r| r
                .get::<_, String>(0))?,
            "Preserve"
        );
        assert_eq!(
            db.query_row("SELECT max(version) FROM schema_migrations", [], |r| r
                .get::<_, i64>(0))?,
            2
        );
        drop(db);
        drop(open(&path)?);
        drop(inspect(&path)?);
        Ok(())
    }

    #[test]
    fn unrecognized_database_is_preserved() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("meta.db");
        let db = Connection::open(&path)?;
        db.execute_batch("CREATE TABLE precious(value TEXT); INSERT INTO precious VALUES('keep');")
            .expect("create unknown schema fixture");
        drop(db);
        assert!(open(&path).is_err());
        let db = Connection::open(&path)?;
        assert_eq!(
            db.query_row("SELECT value FROM precious", [], |r| r.get::<_, String>(0))?,
            "keep"
        );
        Ok(())
    }

    #[test]
    fn inspection_rejects_non_files_newer_versions_and_foreign_key_damage() {
        let directory = tempfile::tempdir().expect("temporary inspection directory");
        assert!(inspect(directory.path()).is_err());

        let path = directory.path().join("meta.db");
        let db = open(&path).expect("open inspection fixture");
        db.execute(
            "UPDATE schema_migrations SET version=99 WHERE version=2",
            [],
        )
        .expect("mark inspected fixture as newer");
        drop(db);
        assert!(
            inspect(&path)
                .unwrap_err()
                .to_string()
                .contains("unsupported")
        );

        let db = Connection::open(&path).expect("open fixture without connection policy");
        db.pragma_update(None, "foreign_keys", "OFF")
            .expect("disable fixture foreign key enforcement");
        db.execute(
            "UPDATE schema_migrations SET version=2 WHERE version=99",
            [],
        )
        .expect("restore inspected schema version");
        db.execute(
            "INSERT INTO sessions(token_hash,user_id,expires_at_us,created_at_us,last_seen_at_us) VALUES('broken',999,1,1,1)",
            [],
        )
        .expect("insert foreign key violation with checks disabled");
        drop(db);
        assert!(
            inspect(&path)
                .unwrap_err()
                .to_string()
                .contains("foreign key")
        );
    }
}
