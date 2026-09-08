//! Atomic authorization capture for every search surface and durable token key ownership.

use anyhow::{Result, ensure};
use rusqlite::{Connection, OptionalExtension, params, params_from_iter};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone)]
pub struct Authorization {
    pub projects: Vec<i64>,
    pub storage_generation: String,
    pub epoch: i64,
    pub hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeError {
    Forbidden,
    TooLarge,
    Invalid,
}
impl std::fmt::Display for ScopeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ScopeError {}

pub fn token_key(db: &mut Connection) -> Result<[u8; 32]> {
    let tx = db.transaction()?;
    let existing: Option<String> = tx
        .query_row(
            "SELECT value_json FROM settings WHERE key='token_hmac_v1'",
            [],
            |r| r.get(0),
        )
        .optional()?;
    let key = if let Some(raw) = existing {
        serde_json::from_str(&raw)?
    } else {
        let key: [u8; 32] = rand::random();
        tx.execute(
            "INSERT INTO settings(key,value_json,updated_at_us) VALUES('token_hmac_v1',?1,?2)",
            params![serde_json::to_string(&key)?, crate::model::now_us()?],
        )?;
        key
    };
    tx.commit()?;
    Ok(key)
}

/// Empty requested means all active projects. A nonempty selection must be fully authorized.
pub fn capture(
    db: &mut Connection,
    principal: i64,
    mut requested: Vec<i64>,
) -> Result<Authorization> {
    if requested.len() > 1000 {
        return Err(ScopeError::TooLarge.into());
    }
    if requested.iter().any(|id| *id <= 0) {
        return Err(ScopeError::Invalid.into());
    }
    requested.sort_unstable();
    requested.dedup();
    let tx = db.transaction()?;
    let mut authorization = identity(&tx, principal)?;
    let projects = if requested.is_empty() {
        let mut query =
            tx.prepare("SELECT id FROM projects WHERE is_active=1 ORDER BY id LIMIT 1001")?;
        let ids = query
            .query_map([], |r| r.get::<_, i64>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if ids.len() > 1000 {
            return Err(ScopeError::TooLarge.into());
        }
        ids
    } else {
        let placeholders = std::iter::repeat_n("?", requested.len())
            .collect::<Vec<_>>()
            .join(",");
        let mut query = tx.prepare(&format!(
            "SELECT id FROM projects WHERE is_active=1 AND id IN ({placeholders}) ORDER BY id"
        ))?;
        let ids = query
            .query_map(params_from_iter(requested.iter()), |r| r.get::<_, i64>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if ids != requested {
            return Err(ScopeError::Forbidden.into());
        }
        ids
    };
    authorization.projects = projects;
    tx.commit()?;
    Ok(authorization)
}

/// Identity/epoch only, for verifying an opaque detail token before selecting its project.
pub fn identity(db: &Connection, principal: i64) -> Result<Authorization> {
    let role: Option<String> = db
        .query_row(
            "SELECT role FROM users WHERE id=?1 AND is_active=1",
            [principal],
            |r| r.get(0),
        )
        .optional()?;
    let role = role.ok_or(ScopeError::Forbidden)?;
    ensure!(
        matches!(role.as_str(), "admin" | "member"),
        ScopeError::Forbidden
    );
    let (storage_generation, epoch): (String, i64) = db.query_row(
        "SELECT storage_generation,authorization_epoch FROM runtime_state WHERE singleton=1",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    // v1 grants each active user all active projects. The global epoch captures
    // project/user state changes; the selected projects belong in request_hash.
    // This lets a row's detail token be checked without replaying its search form.
    let hash = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&(principal, role, epoch))?)
    );
    Ok(Authorization {
        projects: Vec::new(),
        storage_generation,
        epoch,
        hash,
    })
}

pub fn event_candidates(
    db: &Connection,
    start_us: i64,
    end_us: i64,
    watermark: i64,
) -> Result<Vec<String>> {
    ensure!(start_us < end_us && watermark >= 0, ScopeError::Invalid);
    let mut statement = db.prepare(
        "SELECT id,state FROM shards
         WHERE (min_ingest_seq IS NULL OR min_ingest_seq<=?1)
           AND (min_timestamp_us IS NULL OR max_timestamp_us>=?2)
           AND (max_timestamp_us IS NULL OR min_timestamp_us<?3)
         ORDER BY id",
    )?;
    let rows = statement
        .query_map(params![watermark, start_us, end_us], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(
        rows.iter().all(|(_, state)| matches!(
            state.as_str(),
            "active" | "local" | "remote_verified" | "remote_only"
        )),
        "candidate shard has an unsupported state"
    );
    Ok(rows.into_iter().map(|(id, _)| id).collect())
}

pub fn received_candidates(
    db: &Connection,
    start_us: i64,
    end_us: i64,
    watermark: i64,
) -> Result<Vec<String>> {
    ensure!(start_us < end_us && watermark >= 0, ScopeError::Invalid);
    let mut statement = db.prepare(
        "SELECT id,state FROM shards
         WHERE (min_ingest_seq IS NULL OR min_ingest_seq<=?1)
           AND (min_received_at_us IS NULL OR max_received_at_us>=?2)
           AND (max_received_at_us IS NULL OR min_received_at_us<?3)
         ORDER BY id",
    )?;
    let rows = statement
        .query_map(params![watermark, start_us, end_us], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(
        rows.iter().all(|(_, state)| matches!(
            state.as_str(),
            "active" | "local" | "remote_verified" | "remote_only"
        )),
        "candidate shard has an unsupported state"
    );
    Ok(rows.into_iter().map(|(id, _)| id).collect())
}

pub fn local_detail_shard(db: &Connection, shard_id: &str) -> Result<()> {
    let state: Option<String> = db
        .query_row("SELECT state FROM shards WHERE id=?1", [shard_id], |row| {
            row.get(0)
        })
        .optional()?;
    ensure!(
        state.is_some_and(|state| matches!(state.as_str(), "active" | "local" | "remote_verified")),
        "detail shard is not locally available"
    );
    Ok(())
}
