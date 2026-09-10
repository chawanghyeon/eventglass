//! Replay metadata and immutable segment references in the existing operational DB.
use crate::{
    sentry::replay::{Frustration, ReplayMetadata},
    storage::remote::ObjectReference,
};
use anyhow::{Result, ensure};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const RETENTION_US: i64 = 30 * 24 * 60 * 60 * 1_000_000;
pub const MAX_SESSION_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug)]
pub enum ReplayError {
    Conflict,
    TooLarge,
    Forbidden,
    NotFound,
}
impl std::fmt::Display for ReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ReplayError {}

#[derive(Debug)]
pub struct PreparedReplay {
    pub metadata: ReplayMetadata,
    pub blob: ObjectReference,
    pub recording_bytes: usize,
    pub frustration: Frustration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplaySummary {
    pub project_id: String,
    pub metadata: ReplayMetadata,
    pub segment_count: u32,
    pub max_segment_id: u32,
    pub recording_bytes: u64,
    pub partial: bool,
    pub frustration: Frustration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentReference {
    pub segment_id: u32,
    pub blob: ObjectReference,
}

/// Called only within the same transaction as any normal Event/Log items.
pub fn accept(
    tx: &Transaction<'_>,
    project_id: i64,
    prepared: &PreparedReplay,
    received_at_us: i64,
) -> Result<()> {
    let metadata = &prepared.metadata;
    let identity = crate::storage::remote::sha256(&serde_json::to_vec(metadata)?);
    let existing: Option<(String,String)> = tx.query_row("SELECT blob_sha256,metadata_sha256 FROM replay_segments WHERE project_id=?1 AND replay_id=?2 AND segment_id=?3",params![project_id,metadata.replay_id,metadata.segment_id as i64],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    if let Some((hash, old_identity)) = existing {
        if hash != prepared.blob.sha256 || old_identity != identity {
            return Err(ReplayError::Conflict.into());
        }
        return Ok(());
    }
    let previous: Option<(String, i64)> = tx
        .query_row(
            "SELECT metadata,recording_bytes FROM replays WHERE project_id=?1 AND replay_id=?2",
            params![project_id, metadata.replay_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let merged = if let Some((previous, size)) = previous {
        if size as usize + prepared.recording_bytes > MAX_SESSION_BYTES {
            return Err(ReplayError::TooLarge.into());
        }
        merge(serde_json::from_str(&previous)?, metadata.clone())?
    } else {
        metadata.clone()
    };
    let serialized = serde_json::to_string(&merged)?;
    if serialized.len() > 1024 * 1024 {
        return Err(ReplayError::TooLarge.into());
    }
    // Expiration is anchored to first acceptance: retries cannot keep recordings forever.
    tx.execute("INSERT INTO replays(project_id,replay_id,started_at_ms,finished_at_ms,expires_at_us,metadata,segment_count,max_segment_id,recording_bytes,slow_count,dead_count,rage_count,multi_count)
    VALUES(?1,?2,?3,?4,?5,?6,1,?7,?8,?9,?10,?11,?12)
    ON CONFLICT(project_id,replay_id) DO UPDATE SET started_at_ms=excluded.started_at_ms,finished_at_ms=excluded.finished_at_ms,metadata=excluded.metadata,segment_count=segment_count+1,max_segment_id=max(max_segment_id,excluded.max_segment_id),recording_bytes=recording_bytes+excluded.recording_bytes,slow_count=slow_count+excluded.slow_count,dead_count=dead_count+excluded.dead_count,rage_count=rage_count+excluded.rage_count,multi_count=multi_count+excluded.multi_count",params![project_id,metadata.replay_id,merged.started_at_ms,merged.finished_at_ms,received_at_us.checked_add(RETENTION_US).ok_or(ReplayError::TooLarge)?,serialized,metadata.segment_id as i64,prepared.recording_bytes as i64,prepared.frustration.slow,prepared.frustration.dead,prepared.frustration.rage,prepared.frustration.multi])?;
    tx.execute("INSERT INTO replay_segments(project_id,replay_id,segment_id,blob_sha256,blob_size,metadata_sha256) VALUES(?1,?2,?3,?4,?5,?6)",params![project_id,metadata.replay_id,metadata.segment_id as i64,prepared.blob.sha256,prepared.blob.size as i64,identity])?;
    mark_dirty(tx, received_at_us)?;
    Ok(())
}

pub fn mark_dirty(tx: &Transaction<'_>, now: i64) -> Result<()> {
    tx.execute("INSERT INTO settings(key,value_json,updated_at_us) VALUES('replay.revision','1',?1) ON CONFLICT(key) DO UPDATE SET value_json=CAST(CAST(value_json AS INTEGER)+1 AS TEXT),updated_at_us=excluded.updated_at_us",[now])?;
    Ok(())
}

pub fn revision(db: &Connection) -> Result<i64> {
    Ok(db
        .query_row(
            "SELECT CAST(value_json AS INTEGER) FROM settings WHERE key='replay.revision'",
            [],
            |r| r.get(0),
        )
        .optional()?
        .unwrap_or(0))
}

pub fn backup_pending(db: &Connection) -> Result<bool> {
    let current = revision(db)?;
    let published: i64 = db
        .query_row(
            "SELECT CAST(value_json AS INTEGER) FROM settings WHERE key='replay.backup_revision'",
            [],
            |r| r.get(0),
        )
        .optional()?
        .unwrap_or(0);
    Ok(current > published)
}

pub fn backup_due(db: &Connection, now: i64) -> Result<bool> {
    let attempted: i64 = db
        .query_row(
            "SELECT updated_at_us FROM settings WHERE key='replay.backup_attempt'",
            [],
            |r| r.get(0),
        )
        .optional()?
        .unwrap_or(0);
    Ok(backup_pending(db)? && now.saturating_sub(attempted) >= 5 * 60 * 1_000_000)
}

fn merge(a: ReplayMetadata, b: ReplayMetadata) -> Result<ReplayMetadata> {
    let mut result = if a.segment_id > b.segment_id {
        a.clone()
    } else {
        b.clone()
    };
    result.started_at_ms = a.started_at_ms.min(b.started_at_ms);
    result.finished_at_ms = a.finished_at_ms.max(b.finished_at_ms);
    let union = |a: Vec<String>, b: Vec<String>| -> Result<Vec<String>> {
        let values: BTreeSet<_> = a.into_iter().chain(b).collect();
        if values.len() > 10_000 {
            return Err(ReplayError::TooLarge.into());
        }
        Ok(values.into_iter().collect())
    };
    result.urls = union(a.urls, b.urls)?;
    result.error_ids = union(a.error_ids, b.error_ids)?;
    result.trace_ids = union(a.trace_ids, b.trace_ids)?;
    Ok(result)
}

/// Also supports pre-Replay checkpoints when restoring an earlier release.
pub fn blob_references(db: &Connection) -> Result<Vec<ObjectReference>> {
    let exists = replay_segments_exist(db)?;
    if !exists {
        return Ok(Vec::new());
    }
    let mut statement = db.prepare(
        "SELECT DISTINCT blob_sha256,blob_size FROM replay_segments ORDER BY blob_sha256",
    )?;
    let values = statement
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    values
        .into_iter()
        .map(|(hash, size)| {
            Ok(ObjectReference {
                key: crate::storage::replay::key(&hash)?,
                sha256: hash,
                size: u64::try_from(size)?,
            })
        })
        .collect()
}

fn replay_segments_exist(db: &Connection) -> Result<bool> {
    db.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='replay_segments' AND type='table')",
        [],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

fn first_string(row: &rusqlite::Row<'_>) -> rusqlite::Result<String> {
    row.get(0)
}

/// Up to 16 small sessions / 256 segments, or one larger session (SDK max 10,001).
/// Caller must exclude ingest, recording readers, and pinned backup snapshots.
pub(crate) fn expire_batch(db: &mut Connection, now: i64) -> Result<()> {
    let tx = db.transaction()?;
    let candidates = tx.prepare("SELECT rowid,segment_count FROM replays WHERE expires_at_us<=?1 ORDER BY expires_at_us LIMIT 16")?
        .query_map([now], |row| Ok((row.get::<_,i64>(0)?, row.get::<_,u32>(1)?)))?
        .collect::<std::result::Result<Vec<_>,_>>()?;
    let (mut removed, mut segments) = (0, 0);
    for (id, count) in candidates {
        if removed > 0 && segments + count > 256 {
            break;
        }
        removed += tx.execute("DELETE FROM replays WHERE rowid=?1", [id])?;
        segments += count;
    }
    removed += tx.execute("DELETE FROM feedback WHERE rowid IN (SELECT rowid FROM feedback WHERE expires_at_us<=?1 LIMIT 256)", [now])?;
    if removed > 0 {
        mark_dirty(&tx, now)?;
    }
    tx.commit()?;
    Ok(())
}

/// Startup-only physical cleanup: no reader or in-flight backup can lose a pinned blob.
/// Expired replays are hidden immediately by API queries even before the next restart.
pub fn expire_at_startup(db: &mut Connection, root: &std::path::Path, now: i64) -> Result<()> {
    let tx = db.transaction()?;
    let removed = tx.execute("DELETE FROM replays WHERE expires_at_us<=?1", [now])?
        + tx.execute("DELETE FROM feedback WHERE expires_at_us<=?1", [now])?;
    if removed > 0 {
        mark_dirty(&tx, now)?;
    }
    tx.commit()?;
    let referenced: BTreeSet<_> = blob_references(db)?.into_iter().map(|r| r.key).collect();
    let directory = root.join("replay-blobs");
    if !directory.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(&directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        ensure!(entry.file_type()?.is_file(), "unexpected replay blob entry");
        let key = format!("replay-blobs/{name}");
        let owned = name
            .strip_suffix(".zlib")
            .is_some_and(|s| crate::storage::replay::key(s).is_ok())
            || name
                .strip_prefix('.')
                .and_then(|s| s.strip_suffix(".tmp"))
                .is_some_and(|s| uuid::Uuid::parse_str(s).is_ok());
        if owned && !referenced.contains(&key) {
            std::fs::remove_file(entry.path())?;
        }
    }
    std::fs::File::open(directory)?.sync_all()?;
    Ok(())
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayFilter {
    pub viewport: Option<crate::replay::ViewportClass>,
    pub project_id: i64,
    pub environment: Option<String>,
    pub release: Option<String>,
    pub url: Option<String>,
    pub user: Option<String>,
    pub has_error: Option<bool>,
    pub rage_click: Option<bool>,
    pub dead_click: Option<bool>,
    pub min_duration_ms: Option<i64>,
    pub max_duration_ms: Option<i64>,
    pub started_after_ms: Option<i64>,
    pub started_before_ms: Option<i64>,
    pub before_started_ms: Option<i64>,
    pub before_id: Option<String>,
}

fn authorize(db: &mut Connection, actor: i64, project: i64) -> Result<()> {
    super::search::capture(db, actor, vec![project]).map_err(|_| ReplayError::Forbidden)?;
    Ok(())
}

fn summary(row: &rusqlite::Row<'_>) -> rusqlite::Result<ReplaySummary> {
    let metadata: String = row.get(1)?;
    let metadata: ReplayMetadata = serde_json::from_str(&metadata).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(e))
    })?;
    let segment_count: u32 = row.get(2)?;
    let max_segment_id: u32 = row.get(3)?;
    Ok(ReplaySummary {
        project_id: row.get::<_, i64>(0)?.to_string(),
        metadata,
        segment_count,
        max_segment_id,
        recording_bytes: row.get::<_, i64>(4)? as u64,
        partial: segment_count != max_segment_id + 1,
        frustration: Frustration {
            slow: row.get(5)?,
            dead: row.get(6)?,
            rage: row.get(7)?,
            multi: row.get(8)?,
        },
    })
}
const COLUMNS: &str = "project_id,metadata,segment_count,max_segment_id,recording_bytes,slow_count,dead_count,rage_count,multi_count";

pub fn list(
    db: &mut Connection,
    actor: i64,
    filter: &ReplayFilter,
    now: i64,
) -> Result<Vec<ReplaySummary>> {
    authorize(db, actor, filter.project_id)?;
    let mut query=db.prepare(&format!("SELECT {COLUMNS} FROM replays WHERE project_id=?1 AND expires_at_us>?2
    AND (?3 IS NULL OR json_extract(metadata,'$.environment')=?3)
    AND (?4 IS NULL OR json_extract(metadata,'$.release')=?4)
    AND (?5 IS NULL OR EXISTS(SELECT 1 FROM json_each(metadata,'$.urls') WHERE instr(value,?5)>0))
    AND (?6 IS NULL OR json_extract(metadata,'$.user.id')=?6 OR json_extract(metadata,'$.user.email')=?6 OR json_extract(metadata,'$.user.username')=?6)
    AND (?7 IS NULL OR (json_array_length(metadata,'$.error_ids')>0)=?7)
    AND (?8 IS NULL OR (rage_count>0)=?8) AND (?9 IS NULL OR (dead_count>0)=?9)
    AND (?10 IS NULL OR finished_at_ms-started_at_ms>=?10) AND (?11 IS NULL OR finished_at_ms-started_at_ms<=?11)
    AND (?12 IS NULL OR (started_at_ms,replay_id)<(?12,?13))
    AND (?14 IS NULL OR started_at_ms>=?14) AND (?15 IS NULL OR started_at_ms<?15)
    ORDER BY started_at_ms DESC,replay_id DESC LIMIT 51"))?;
    let parameters = params![
        filter.project_id,
        now,
        filter.environment,
        filter.release,
        filter.url,
        filter.user,
        filter.has_error,
        filter.rage_click,
        filter.dead_click,
        filter.min_duration_ms,
        filter.max_duration_ms,
        filter.before_started_ms,
        filter.before_id,
        filter.started_after_ms,
        filter.started_before_ms
    ];
    let rows = query.query_map(parameters, summary)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn get(
    db: &mut Connection,
    actor: i64,
    project: i64,
    replay_id: &str,
    now: i64,
) -> Result<ReplaySummary> {
    authorize(db, actor, project)?;
    db.query_row(&format!("SELECT {COLUMNS} FROM replays WHERE project_id=?1 AND replay_id=?2 AND expires_at_us>?3"),params![project,replay_id,now],summary).optional()?.ok_or_else(||ReplayError::NotFound.into())
}

pub fn segments(db: &Connection, project: i64, replay_id: &str) -> Result<Vec<SegmentReference>> {
    let mut statement=db.prepare("SELECT segment_id,blob_sha256,blob_size FROM replay_segments WHERE project_id=?1 AND replay_id=?2 ORDER BY segment_id")?;
    let values = statement
        .query_map(params![project, replay_id], |r| {
            Ok((
                r.get::<_, u32>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    values
        .into_iter()
        .map(|(segment_id, hash, size)| {
            Ok(SegmentReference {
                segment_id,
                blob: ObjectReference {
                    key: crate::storage::replay::key(&hash)?,
                    sha256: hash,
                    size: u64::try_from(size)?,
                },
            })
        })
        .collect()
}

pub fn associations(
    db: &Connection,
    project: i64,
    replay: &ReplaySummary,
    now: i64,
) -> Result<serde_json::Value> {
    let ids = serde_json::to_string(&replay.metadata.error_ids)?;
    let mut statement=db.prepare("SELECT o.source_event_id,o.record_id,o.issue_id FROM issue_occurrences o JOIN issues i ON i.id=o.issue_id WHERE i.project_id=?1 AND o.source_event_id IN (SELECT value FROM json_each(?2)) LIMIT 1000")?;
    let errors=statement.query_map(params![project,ids],|r|Ok(serde_json::json!({"event_id":r.get::<_,String>(0)?,"record_id":r.get::<_,String>(1)?,"issue_id":r.get::<_,String>(2)?})))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let raw = feedback_rows(db, project, &replay.metadata.replay_id, now)?;
    let feedback = raw
        .iter()
        .map(|r| serde_json::from_str::<serde_json::Value>(r))
        .collect::<serde_json::Result<Vec<_>>>()?;
    Ok(serde_json::json!({"errors":errors,"feedback":feedback}))
}

fn feedback_rows(db: &Connection, project: i64, replay_id: &str, now: i64) -> Result<Vec<String>> {
    let mut statement=db.prepare("SELECT payload FROM feedback WHERE project_id=?1 AND replay_id=?2 AND expires_at_us>?3 ORDER BY timestamp_ms LIMIT 100")?;
    Ok(statement
        .query_map(params![project, replay_id, now], first_string)?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn feedback_list(
    db: &mut Connection,
    actor: i64,
    project: i64,
    now: i64,
) -> Result<Vec<serde_json::Value>> {
    authorize(db, actor, project)?;
    let mut statement=db.prepare("SELECT payload FROM feedback WHERE project_id=?1 AND expires_at_us>?2 ORDER BY timestamp_ms DESC,event_id DESC LIMIT 20")?;
    let raw = statement
        .query_map(params![project, now], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(raw
        .into_iter()
        .map(|value| serde_json::from_str(&value))
        .collect::<serde_json::Result<Vec<_>>>()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn database() -> (tempfile::TempDir, Connection) {
        let directory = tempfile::tempdir().expect("replay database directory");
        let database =
            crate::db::open(&directory.path().join("meta.db")).expect("open replay database");
        database
            .execute(
                "INSERT INTO projects(id,slug,name,created_at_us,updated_at_us) VALUES(1,'p','P',0,0)",
                [],
            )
            .expect("insert project");
        (directory, database)
    }

    fn metadata(segment_id: u64) -> ReplayMetadata {
        ReplayMetadata {
            replay_id: "a".repeat(32),
            segment_id,
            started_at_ms: 20,
            finished_at_ms: 30,
            user: None,
            environment: None,
            release: None,
            browser: None,
            os: None,
            device: None,
            urls: Vec::new(),
            error_ids: Vec::new(),
            trace_ids: Vec::new(),
            sdk_version: None,
            replay_type: None,
        }
    }

    #[test]
    fn errors_merge_limits_and_pre_replay_backups_are_explicit() {
        for error in [
            ReplayError::Conflict,
            ReplayError::TooLarge,
            ReplayError::Forbidden,
            ReplayError::NotFound,
        ] {
            assert!(!error.to_string().is_empty());
        }

        let mut first = metadata(2);
        first.started_at_ms = 10;
        first.urls = vec!["first".into()];
        let mut second = metadata(1);
        second.finished_at_ms = 40;
        second.urls = vec!["second".into()];
        let merged = merge(first.clone(), second.clone()).expect("merge later segment");
        assert_eq!(merged.segment_id, 2);
        assert_eq!(merged.started_at_ms, 10);
        assert_eq!(merged.finished_at_ms, 40);
        assert_eq!(merged.urls, ["first", "second"]);
        let merged = merge(second, first).expect("merge earlier segment");
        assert_eq!(merged.segment_id, 2);

        let mut oversized = metadata(3);
        oversized.urls = (0..=10_000).map(|index| index.to_string()).collect();
        assert!(matches!(
            merge(metadata(0), oversized)
                .expect_err("union cardinality must remain bounded")
                .downcast_ref::<ReplayError>(),
            Some(ReplayError::TooLarge)
        ));

        let database = Connection::open_in_memory().expect("open legacy database");
        assert!(
            blob_references(&database)
                .expect("read legacy references")
                .is_empty()
        );
        assert_eq!(
            database
                .query_row("SELECT 'value'", [], first_string)
                .expect("first string"),
            "value"
        );
        let damaged = Connection::open_in_memory().expect("damaged reference database");
        damaged
            .execute("CREATE TABLE replay_segments(other TEXT)", [])
            .expect("damaged reference schema");
        assert!(blob_references(&damaged).is_err());
    }

    #[test]
    fn acceptance_enforces_session_and_metadata_byte_limits() {
        let (_directory, mut database) = database();
        let prior = metadata(0);
        database
            .execute(
                "INSERT INTO replays(project_id,replay_id,started_at_ms,finished_at_ms,expires_at_us,metadata,segment_count,max_segment_id,recording_bytes) VALUES(1,?1,0,1,1,?2,1,0,?3)",
                params![prior.replay_id, serde_json::to_string(&prior).expect("metadata"), MAX_SESSION_BYTES as i64],
            )
            .expect("insert replay");
        let prepared = PreparedReplay {
            metadata: metadata(1),
            blob: ObjectReference {
                key: "replay-blobs/unused.zlib".into(),
                size: 1,
                sha256: "0".repeat(64),
            },
            recording_bytes: 1,
            frustration: Frustration::default(),
        };
        let transaction = database.transaction().expect("transaction");
        assert!(matches!(
            accept(&transaction, 1, &prepared, 0)
                .expect_err("session cap")
                .downcast_ref::<ReplayError>(),
            Some(ReplayError::TooLarge)
        ));
        drop(transaction);

        database
            .execute("DELETE FROM replays", [])
            .expect("clear replay");
        let mut oversized = metadata(0);
        oversized.urls.push("x".repeat(1024 * 1024 + 1));
        let prepared = PreparedReplay {
            metadata: oversized,
            ..prepared
        };
        let transaction = database.transaction().expect("transaction");
        assert!(matches!(
            accept(&transaction, 1, &prepared, 0)
                .expect_err("metadata cap")
                .downcast_ref::<ReplayError>(),
            Some(ReplayError::TooLarge)
        ));
    }

    #[test]
    fn expiration_batches_bound_segments_and_remove_owned_orphans() {
        let (directory, mut database) = database();
        let value = serde_json::to_string(&metadata(0)).expect("metadata");
        for (id, count) in [("a".repeat(32), 200), ("b".repeat(32), 200)] {
            database
                .execute(
                    "INSERT INTO replays(project_id,replay_id,started_at_ms,finished_at_ms,expires_at_us,metadata,segment_count,max_segment_id,recording_bytes) VALUES(1,?1,0,1,0,?2,?3,0,0)",
                    params![id, value, count],
                )
                .expect("insert expiring replay");
        }
        expire_batch(&mut database, 0).expect("bounded expiration");
        assert_eq!(
            database
                .query_row("SELECT count(*) FROM replays", [], |row| row
                    .get::<_, i64>(0))
                .expect("remaining replays"),
            1
        );

        let blobs = directory.path().join("replay-blobs");
        std::fs::create_dir(&blobs).expect("blob directory");
        let blob = format!("{}.zlib", "0".repeat(64));
        let temporary = format!(".{}.tmp", uuid::Uuid::new_v4());
        std::fs::write(blobs.join(&blob), b"orphan").expect("orphan blob");
        std::fs::write(blobs.join(&temporary), b"temporary").expect("temporary blob");
        std::fs::write(blobs.join("unowned"), b"keep").expect("unowned file");
        expire_at_startup(&mut database, directory.path(), 0).expect("startup cleanup");
        assert!(!blobs.join(blob).exists());
        assert!(!blobs.join(temporary).exists());
        assert!(blobs.join("unowned").exists());
    }

    #[test]
    fn summary_rejects_invalid_metadata_and_marks_segment_gaps() {
        let database = Connection::open_in_memory().expect("summary database");
        let invalid = database.query_row("SELECT 1,'not-json',1,0,0,0,0,0,0", [], summary);
        assert!(invalid.is_err());

        let value = serde_json::to_string(&metadata(2)).expect("metadata");
        let replay = database
            .query_row("SELECT 1,?1,2,2,10,1,2,3,4", [value], summary)
            .expect("valid summary");
        assert!(replay.partial);
        assert_eq!(replay.frustration.rage, 3);
    }

    #[test]
    fn associations_reject_a_non_text_feedback_payload() {
        use rusqlite::hooks::{AuthAction, AuthContext, Authorization};

        let (_directory, database) = database();
        let replay = ReplaySummary {
            project_id: "1".into(),
            metadata: metadata(0),
            segment_count: 1,
            max_segment_id: 0,
            recording_bytes: 1,
            partial: false,
            frustration: Frustration::default(),
        };
        let event_id = "f".repeat(32);
        database
            .execute(
                "INSERT INTO feedback(project_id,event_id,replay_id,timestamp_ms,expires_at_us,payload)
                 VALUES(1,?1,?2,0,10,x'80')",
                params![event_id, replay.metadata.replay_id],
            )
            .expect("insert damaged feedback payload");
        assert!(associations(&database, 1, &replay, 0).is_err());

        database
            .authorizer(Some(|context: AuthContext<'_>| match context.action {
                AuthAction::Read {
                    table_name: "feedback",
                    ..
                } => Authorization::Deny,
                _ => Authorization::Allow,
            }))
            .expect("install feedback query authorizer");
        assert!(associations(&database, 1, &replay, 0).is_err());

        let interrupted = Connection::open_in_memory().expect("interrupted feedback database");
        interrupted
            .execute_batch(
                "CREATE TABLE feedback(
                    project_id INTEGER,replay_id TEXT,expires_at_us INTEGER,
                    timestamp_ms INTEGER,payload TEXT)",
            )
            .expect("interrupted feedback schema");
        interrupted
            .progress_handler(1, Some(|| true))
            .expect("install feedback interrupt handler");
        assert!(feedback_rows(&interrupted, 1, &replay.metadata.replay_id, 0).is_err());
    }
}
