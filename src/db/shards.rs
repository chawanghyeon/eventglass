//! Active-shard catalog transitions; filesystem creation precedes catalog adoption.

use anyhow::{Result, ensure};
use rusqlite::{Connection, params};

use crate::{model::Boundary, storage::manifest::Manifest};

pub const FORMAT_VERSION: &str = "tantivy-0.26.1/format-7";

pub struct StartupCatalog {
    pub installation_id: String,
    pub applied: Boundary,
    pub active_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct LocalCatalogShard {
    pub id: String,
    pub state: String,
    pub size_bytes: i64,
    pub record_count: i64,
    pub min_timestamp_us: Option<i64>,
    pub max_timestamp_us: Option<i64>,
    pub min_received_at_us: Option<i64>,
    pub max_received_at_us: Option<i64>,
    pub min_ingest_seq: Option<i64>,
    pub max_ingest_seq: Option<i64>,
    pub last_applied_inbox_id: i64,
    pub sealed_at_us: Option<i64>,
}

impl LocalCatalogShard {
    pub fn matches(&self, manifest: &Manifest, local_size: u64) -> bool {
        self.id == manifest.shard_id
            && matches!(self.state.as_str(), "local" | "remote_verified")
            && self.size_bytes >= 0
            && u64::try_from(self.size_bytes).ok() == Some(local_size)
            && u64::try_from(self.record_count).ok() == Some(manifest.stats.record_count)
            && self.min_timestamp_us == manifest.stats.min_timestamp_us
            && self.max_timestamp_us == manifest.stats.max_timestamp_us
            && self.min_received_at_us == manifest.stats.min_received_at_us
            && self.max_received_at_us == manifest.stats.max_received_at_us
            && self.min_ingest_seq == manifest.stats.min_ingest_seq
            && self.max_ingest_seq == manifest.stats.max_ingest_seq
            && self.last_applied_inbox_id == manifest.boundary.inbox_id
            && self.sealed_at_us == Some(manifest.sealed_at_us)
    }
}

pub fn local_catalog(db: &Connection) -> Result<Vec<LocalCatalogShard>> {
    let mut statement = db.prepare(
        "SELECT id,state,size_bytes,record_count,min_timestamp_us,max_timestamp_us,
                min_received_at_us,max_received_at_us,min_ingest_seq,max_ingest_seq,
                last_applied_inbox_id,sealed_at_us
         FROM shards WHERE state IN ('local','remote_verified') ORDER BY id",
    )?;
    Ok(statement
        .query_map([], |row| {
            Ok(LocalCatalogShard {
                id: row.get(0)?,
                state: row.get(1)?,
                size_bytes: row.get(2)?,
                record_count: row.get(3)?,
                min_timestamp_us: row.get(4)?,
                max_timestamp_us: row.get(5)?,
                min_received_at_us: row.get(6)?,
                max_received_at_us: row.get(7)?,
                min_ingest_seq: row.get(8)?,
                max_ingest_seq: row.get(9)?,
                last_applied_inbox_id: row.get(10)?,
                sealed_at_us: row.get(11)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn catalog_ids(db: &Connection) -> Result<std::collections::HashSet<String>> {
    let mut statement = db.prepare("SELECT id FROM shards")?;
    Ok(statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<_>>()?)
}

const ROLLOVER_BYTES: u64 = 256 * 1024 * 1024;
const ROLLOVER_AGE_US: i64 = 60 * 60 * 1_000_000;

pub fn rotation_plan(
    db: &Connection,
    active_id: &str,
    measured_size: Option<u64>,
    now_us: i64,
) -> Result<Option<crate::storage::manifest::ShardStats>> {
    rotation_plan_inner(db, active_id, measured_size, now_us, false)
}

pub fn replay_backup_rotation(
    db: &Connection,
    active_id: &str,
    now_us: i64,
) -> Result<Option<crate::storage::manifest::ShardStats>> {
    rotation_plan_inner(
        db,
        active_id,
        None,
        now_us,
        crate::db::replays::backup_due(db, now_us)?,
    )
}

fn rotation_plan_inner(
    db: &Connection,
    active_id: &str,
    measured_size: Option<u64>,
    now_us: i64,
    force: bool,
) -> Result<Option<crate::storage::manifest::ShardStats>> {
    let measured_size = measured_size.map(i64::try_from).transpose()?;
    if let Some(size) = measured_size {
        ensure!(
            db.execute(
                "UPDATE shards SET size_bytes=?1 WHERE id=?2 AND state='active'",
                params![size, active_id],
            )? == 1,
            "active shard disappeared while measuring rollover"
        );
    }
    let row: SealRow = db.query_row(
        "SELECT state,last_applied_inbox_id,record_count,min_timestamp_us,max_timestamp_us,
                min_received_at_us,max_received_at_us,min_ingest_seq,max_ingest_seq
         FROM shards WHERE id=?1",
        [active_id],
        |r| {
            Ok(SealRow {
                state: r.get(0)?,
                last_applied_inbox_id: r.get(1)?,
                record_count: r.get(2)?,
                min_timestamp_us: r.get(3)?,
                max_timestamp_us: r.get(4)?,
                min_received_at_us: r.get(5)?,
                max_received_at_us: r.get(6)?,
                min_ingest_seq: r.get(7)?,
                max_ingest_seq: r.get(8)?,
            })
        },
    )?;
    ensure!(row.state == "active", "rollover target is not active");
    let (applied, first_received, catalog_size): (Boundary, Option<i64>, i64) = db.query_row(
        "SELECT r.last_applied_inbox_id,r.last_applied_ingest_seq,
                s.first_record_received_at_us,s.size_bytes
         FROM runtime_state r JOIN shards s ON s.id=r.active_shard_id
         WHERE r.singleton=1 AND s.id=?1",
        [active_id],
        |r| {
            Ok((
                Boundary {
                    inbox_id: r.get(0)?,
                    ingest_seq: r.get(1)?,
                },
                r.get(2)?,
                r.get(3)?,
            ))
        },
    )?;
    ensure!(
        row.last_applied_inbox_id == applied.inbox_id,
        "active shard is not published through SQLite A"
    );
    let age_due = first_received
        .and_then(|first| first.checked_add(ROLLOVER_AGE_US))
        .is_some_and(|deadline| now_us >= deadline);
    let size_due = u64::try_from(catalog_size).is_ok_and(|size| size >= ROLLOVER_BYTES);
    if !force && (row.record_count == 0 || !(age_due || size_due)) {
        return Ok(None);
    }
    Ok(Some(crate::storage::manifest::ShardStats {
        record_count: u64::try_from(row.record_count)?,
        min_timestamp_us: row.min_timestamp_us,
        max_timestamp_us: row.max_timestamp_us,
        min_received_at_us: row.min_received_at_us,
        max_received_at_us: row.max_received_at_us,
        min_ingest_seq: row.min_ingest_seq,
        max_ingest_seq: row.max_ingest_seq,
    }))
}

struct SealRow {
    state: String,
    last_applied_inbox_id: i64,
    record_count: i64,
    min_timestamp_us: Option<i64>,
    max_timestamp_us: Option<i64>,
    min_received_at_us: Option<i64>,
    max_received_at_us: Option<i64>,
    min_ingest_seq: Option<i64>,
    max_ingest_seq: Option<i64>,
}

fn supported(schema: u32, format: &str, tokenizer: u32) -> bool {
    schema == crate::search::schema::APPLICATION_SCHEMA_VERSION
        && format == FORMAT_VERSION
        && tokenizer == crate::search::schema::TOKENIZER_VERSION
}

pub fn startup(db: &Connection) -> Result<StartupCatalog> {
    let catalog = db.query_row("SELECT installation_id,last_applied_inbox_id,last_applied_ingest_seq,active_shard_id FROM runtime_state WHERE singleton=1",[],|row|Ok(StartupCatalog {
        installation_id:row.get(0)?,applied:Boundary{inbox_id:row.get(1)?,ingest_seq:row.get(2)?},active_id:row.get(3)?
    }))?;
    ensure!(
        super::indexer::applied(db)? == catalog.applied,
        "runtime boundary changed during startup"
    );
    ensure!(
        (catalog.applied.inbox_id == 0) == (catalog.applied.ingest_seq == 0),
        "runtime zero boundary pair is inconsistent"
    );
    if let Some(id) = &catalog.active_id {
        uuid::Uuid::parse_str(id)?;
        let (state, schema, format, tokenizer): (String, u32, String, u32) = db.query_row(
            "SELECT state,schema_version,format_version,tokenizer_version FROM shards WHERE id=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )?;
        ensure!(
            state == "active" && supported(schema, &format, tokenizer),
            "unsupported active shard catalog metadata"
        );
    } else {
        let invalid: i64 = db.query_row(
            "SELECT count(*) FROM shards
             WHERE state='active' OR schema_version!=?1 OR format_version!=?2
                OR tokenizer_version!=?3",
            params![
                crate::search::schema::APPLICATION_SCHEMA_VERSION,
                FORMAT_VERSION,
                crate::search::schema::TOKENIZER_VERSION
            ],
            |r| r.get(0),
        )?;
        ensure!(invalid == 0, "unsupported sealed shard catalog metadata");
        let last_boundary: Option<i64> =
            db.query_row("SELECT max(last_applied_inbox_id) FROM shards", [], |r| {
                r.get(0)
            })?;
        ensure!(
            match last_boundary {
                None => catalog.applied == Boundary::default(),
                Some(inbox_id) => inbox_id == catalog.applied.inbox_id,
            },
            "missing active shard has no sealed boundary at SQLite A"
        );
    }
    Ok(catalog)
}

/// Completes the SQLite half of a seal after the durable manifest exists.
/// Repeating the transition after a crash is an idempotent no-op.
pub fn finalize_seal(db: &mut Connection, manifest: &Manifest, size_bytes: u64) -> Result<()> {
    let size_bytes = i64::try_from(size_bytes)?;
    let tx = db.transaction()?;
    let state = startup(&tx)?;
    ensure!(
        state.installation_id == manifest.installation_id && state.applied == manifest.boundary,
        "sealed manifest does not match SQLite boundary"
    );
    let row = tx.query_row(
        "SELECT state,last_applied_inbox_id,record_count,min_timestamp_us,max_timestamp_us,
                min_received_at_us,max_received_at_us,min_ingest_seq,max_ingest_seq
         FROM shards WHERE id=?1",
        [&manifest.shard_id],
        |r| {
            Ok(SealRow {
                state: r.get(0)?,
                last_applied_inbox_id: r.get(1)?,
                record_count: r.get(2)?,
                min_timestamp_us: r.get(3)?,
                max_timestamp_us: r.get(4)?,
                min_received_at_us: r.get(5)?,
                max_received_at_us: r.get(6)?,
                min_ingest_seq: r.get(7)?,
                max_ingest_seq: r.get(8)?,
            })
        },
    )?;
    ensure!(
        row.last_applied_inbox_id == manifest.boundary.inbox_id
            && u64::try_from(row.record_count)? == manifest.stats.record_count
            && row.min_timestamp_us == manifest.stats.min_timestamp_us
            && row.max_timestamp_us == manifest.stats.max_timestamp_us
            && row.min_received_at_us == manifest.stats.min_received_at_us
            && row.max_received_at_us == manifest.stats.max_received_at_us
            && row.min_ingest_seq == manifest.stats.min_ingest_seq
            && row.max_ingest_seq == manifest.stats.max_ingest_seq,
        "sealed manifest statistics do not match catalog"
    );
    match row.state.as_str() {
        "active" => {
            ensure!(
                state.active_id.as_deref() == Some(manifest.shard_id.as_str()),
                "sealed shard is not the catalog active"
            );
            ensure!(
                tx.execute(
                    "UPDATE shards SET state='local',size_bytes=?1,sealed_at_us=?2
                     WHERE id=?3 AND state='active'",
                    params![size_bytes, manifest.sealed_at_us, manifest.shard_id],
                )? == 1,
                "active shard seal CAS failed"
            );
            ensure!(
                tx.execute(
                    "UPDATE runtime_state SET active_shard_id=NULL
                     WHERE singleton=1 AND active_shard_id=?1",
                    [&manifest.shard_id],
                )? == 1,
                "runtime active seal CAS failed"
            );
        }
        "local" => {
            ensure!(
                state.active_id.as_deref() != Some(manifest.shard_id.as_str())
                    && row.last_applied_inbox_id == manifest.boundary.inbox_id,
                "inconsistent completed seal"
            );
        }
        _ => anyhow::bail!("seal can only finalize an active or local shard"),
    }
    tx.commit()?;
    Ok(())
}

pub fn adopt_initial(
    db: &mut Connection,
    id: &str,
    expected: Boundary,
    created_us: i64,
) -> Result<()> {
    uuid::Uuid::parse_str(id)?;
    let tx = db.transaction()?;
    let state = startup(&tx)?;
    ensure!(
        state.active_id.is_none() && state.applied == expected,
        "active catalog changed during creation"
    );
    tx.execute("INSERT INTO shards(id,schema_version,format_version,tokenizer_version,state,last_applied_inbox_id,created_at_us) VALUES(?1,?2,?3,?4,'active',?5,?6)",
        params![id,crate::search::schema::APPLICATION_SCHEMA_VERSION,FORMAT_VERSION,crate::search::schema::TOKENIZER_VERSION,expected.inbox_id,created_us])?;
    tx.execute(
        "UPDATE runtime_state SET active_shard_id=?1 WHERE singleton=1",
        [id],
    )?;
    tx.commit()?;
    Ok(())
}
