//! Active-shard catalog transitions; filesystem creation precedes catalog adoption.

use anyhow::{Result, ensure};
use rusqlite::{Connection, params};

use crate::model::Boundary;

pub const FORMAT_VERSION: &str = "tantivy-0.26.1/format-7";

pub struct StartupCatalog {
    pub installation_id: String,
    pub applied: Boundary,
    pub active_id: Option<String>,
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
            state == "active"
                && schema == crate::search::schema::APPLICATION_SCHEMA_VERSION
                && format == FORMAT_VERSION
                && tokenizer == crate::search::schema::TOKENIZER_VERSION,
            "unsupported active shard catalog metadata"
        );
    } else {
        let count: i64 = db.query_row("SELECT count(*) FROM shards", [], |r| r.get(0))?;
        ensure!(
            count == 0 && catalog.applied == Boundary::default(),
            "missing active shard requires explicit rotation/restore recovery"
        );
    }
    Ok(catalog)
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
