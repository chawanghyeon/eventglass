use std::time::Duration;

use anyhow::Result;
use rusqlite::{
    Connection,
    backup::{Backup, StepResult},
};

#[test]
fn pinned_read_snapshot_preserves_cut_and_pending_inbox() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("meta.db");
    let writer = eventglass::db::open(&path)?;
    writer.execute_batch(
        "CREATE TABLE cut(value INTEGER); INSERT INTO cut VALUES(10);
        CREATE TABLE pending(seq INTEGER); INSERT INTO pending VALUES(11);",
    )?;
    let source = eventglass::db::open_reader(&path)?;
    source.execute_batch("BEGIN")?;
    assert_eq!(
        source.query_row("SELECT value FROM cut", [], |r| r.get::<_, i64>(0))?,
        10
    );
    writer.execute_batch(
        "UPDATE cut SET value=20; DELETE FROM pending; INSERT INTO pending VALUES(21);",
    )?;
    let mut destination = Connection::open(dir.path().join("snapshot.db"))?;
    {
        let backup = Backup::new(&source, &mut destination)?;
        backup.run_to_completion(4, Duration::from_millis(1), None)?;
    }
    assert_eq!(
        destination.query_row("SELECT value FROM cut", [], |r| r.get::<_, i64>(0))?,
        10
    );
    assert_eq!(
        destination.query_row("SELECT seq FROM pending", [], |r| r.get::<_, i64>(0))?,
        11
    );
    source.execute_batch("ROLLBACK")?;
    assert_eq!(
        source.query_row("SELECT value FROM cut", [], |r| r.get::<_, i64>(0))?,
        20
    );
    let busy: i64 = writer.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| r.get(0))?;
    assert_eq!(busy, 0);
    Ok(())
}

#[test]
fn interrupted_backup_does_not_publish_partial_destination() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let source = Connection::open(dir.path().join("source.db"))?;
    source.execute_batch(
        "CREATE TABLE big(payload BLOB); INSERT INTO big VALUES(zeroblob(262144));",
    )?;
    let mut destination = Connection::open(dir.path().join("candidate.db"))?;
    {
        let backup = Backup::new(&source, &mut destination)?;
        assert_eq!(backup.step(1)?, StepResult::More);
        // Drop calls finish; this is cancellation, not successful completion.
    }
    let tables: i64 = destination.query_row(
        "SELECT count(*) FROM sqlite_master WHERE name='big'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(tables, 0);
    Ok(())
}
