//! SQLite half of the native commit/finalize boundary and Issue lifecycle.

use std::collections::HashSet;

use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params, params_from_iter};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::{
    config::Limits,
    db::ingest::{InboxChunk, InboxPayload},
    model::{Boundary, Record, RecordKind},
};

const INBOX_VERSION: u32 = 1;
const NORMALIZER_VERSION: u32 = 1;
const EVENT_KEY_QUERY_CHUNK: usize = 300;
const MAX_EVENT_DELIVERIES_PER_BATCH: usize = 1_000;

#[derive(Debug)]
pub struct PreparedBatch {
    pub chunks: Vec<InboxChunk>,
    /// Records to add to the native index. Logs are retained; repeated Errors are removed.
    pub records: Vec<Record>,
    pub boundary: Boundary,
}

struct StoredChunkRow {
    project_id: i64,
    acceptance_id: String,
    chunk_no: i64,
    first_seq: i64,
    last_seq: i64,
    record_count: i64,
    received_at_us: i64,
    normalizer_version: i64,
    raw: Vec<u8>,
}

pub fn applied(db: &Connection) -> Result<Boundary> {
    let (inbox_id, ingest_seq, next_ingest_seq): (i64, i64, i64) = db.query_row(
        "SELECT last_applied_inbox_id,last_applied_ingest_seq,next_ingest_seq
         FROM runtime_state WHERE singleton=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    ensure!(
        inbox_id >= 0 && ingest_seq >= 0 && next_ingest_seq > ingest_seq,
        "invalid SQLite applied boundary"
    );
    Ok(Boundary {
        inbox_id,
        ingest_seq,
    })
}

pub fn prepare(db: &Connection, limits: &Limits) -> Result<Option<PreparedBatch>> {
    let start = applied(db)?;
    let chunks = load_chunks(db, start, None, limits)?;
    if chunks.is_empty() {
        return Ok(None);
    }
    Ok(Some(build_batch(db, chunks)?))
}

/// Rebuild exactly the native-committed `(A, C]` batch without adding native documents again.
pub fn recover_batch(
    db: &Connection,
    committed: Boundary,
    limits: &Limits,
) -> Result<PreparedBatch> {
    let start = applied(db)?;
    ensure!(
        committed.inbox_id > start.inbox_id && committed.ingest_seq > start.ingest_seq,
        "native committed boundary is not ahead of SQLite"
    );
    let chunks = load_chunks(db, start, Some(committed), limits)?;
    ensure!(!chunks.is_empty(), "committed Inbox batch is missing");
    let batch = build_batch(db, chunks)?;
    ensure!(
        batch.boundary == committed,
        "native and recovered Inbox boundaries differ"
    );
    Ok(batch)
}

/// Finalize the already committed native batch. No network or native work belongs in this call.
pub fn finalize(db: &mut Connection, shard_id: &str, batch: PreparedBatch) -> Result<Boundary> {
    let expected = expected_boundary(&batch)?;
    let tx = db.transaction()?;
    let current = applied(&tx)?;
    if current == batch.boundary {
        return Ok(current);
    }
    ensure!(current == expected, "SQLite applied-boundary CAS failed");

    validate_stored_chunks(&tx, &batch)?;
    let recomputed = select_unique_records(&tx, &batch.chunks)?;
    ensure!(
        serde_json::to_vec(&recomputed)? == serde_json::to_vec(&batch.records)?,
        "prepared native records changed before finalize"
    );

    let mut event_deliveries = 0usize;
    for record in &batch.records {
        if record.kind == RecordKind::Error {
            finalize_error(&tx, shard_id, record, &mut event_deliveries)?;
        }
    }

    update_shard(&tx, shard_id, expected, &batch)?;
    let inbox_bytes = checked_usize_to_i64(
        batch
            .chunks
            .iter()
            .try_fold(0usize, |total, chunk| total.checked_add(chunk.bytes))
            .context("Inbox byte total overflow")?,
        "Inbox byte total",
    )?;
    let inbox_records = checked_usize_to_i64(
        batch
            .chunks
            .iter()
            .try_fold(0usize, |total, chunk| {
                total.checked_add(chunk.payload.records.len())
            })
            .context("Inbox record total overflow")?,
        "Inbox record total",
    )?;
    ensure!(
        tx.execute(
            "UPDATE runtime_state
             SET last_applied_inbox_id=?1,last_applied_ingest_seq=?2,
                 inbox_bytes=inbox_bytes-?3,inbox_records=inbox_records-?4
             WHERE singleton=1 AND last_applied_inbox_id=?5 AND last_applied_ingest_seq=?6
               AND inbox_bytes>=?3 AND inbox_records>=?4",
            params![
                batch.boundary.inbox_id,
                batch.boundary.ingest_seq,
                inbox_bytes,
                inbox_records,
                expected.inbox_id,
                expected.ingest_seq
            ],
        )? == 1,
        "runtime boundary or Inbox counters changed before finalize"
    );
    let deleted = tx.execute(
        "DELETE FROM inbox WHERE id>?1 AND id<=?2",
        params![expected.inbox_id, batch.boundary.inbox_id],
    )?;
    ensure!(
        deleted == batch.chunks.len(),
        "finalize did not delete the exact committed chunks"
    );
    tx.commit()?;
    Ok(batch.boundary)
}

fn load_chunks(
    db: &Connection,
    start: Boundary,
    end: Option<Boundary>,
    limits: &Limits,
) -> Result<Vec<InboxChunk>> {
    ensure!(
        limits.batch_records > 0
            && limits.batch_bytes > 0
            && limits.chunk_records > 0
            && limits.chunk_bytes > 0
            && limits.record_bytes > 0,
        "Indexer batch budget must be positive"
    );
    if let Some(end) = end {
        ensure!(
            end.inbox_id > start.inbox_id && end.ingest_seq > start.ingest_seq,
            "invalid recovery range"
        );
    }
    let row_limit = checked_usize_to_i64(
        limits
            .batch_records
            .checked_add(1)
            .context("Indexer row limit overflow")?,
        "Indexer row limit",
    )?;
    let end_id = end.map(|boundary| boundary.inbox_id);
    let mut statement = db.prepare(
        "SELECT id,project_id,acceptance_id,chunk_no,first_ingest_seq,last_ingest_seq,
                record_count,received_at_us,normalizer_version,length(payload),payload
         FROM inbox
         WHERE id>?1 AND (?2 IS NULL OR id<=?2)
         ORDER BY id LIMIT ?3",
    )?;
    let mut rows = statement.query(params![start.inbox_id, end_id, row_limit])?;
    let mut chunks = Vec::new();
    let mut record_count = 0usize;
    let mut byte_count = 0usize;
    let mut expected_id = start
        .inbox_id
        .checked_add(1)
        .context("Inbox ID exhausted")?;
    let mut expected_seq = start
        .ingest_seq
        .checked_add(1)
        .context("ingest sequence exhausted")?;
    let mut previous_acceptance: Option<(String, i64)> = None;
    let maximum_stored_chunk = limits
        .chunk_bytes
        .max(limits.record_bytes.saturating_add(1024));

    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let project_id: i64 = row.get(1)?;
        let acceptance_id: String = row.get(2)?;
        let chunk_no: i64 = row.get(3)?;
        let first_seq: i64 = row.get(4)?;
        let last_seq: i64 = row.get(5)?;
        let stored_count: i64 = row.get(6)?;
        let received_at_us: i64 = row.get(7)?;
        let normalizer_version: i64 = row.get(8)?;
        let stored_bytes: i64 = row.get(9)?;
        ensure!(
            id == expected_id && first_seq == expected_seq,
            "missing or noncontiguous Inbox chunk"
        );
        ensure!(
            project_id > 0
                && !acceptance_id.is_empty()
                && chunk_no >= 0
                && stored_count > 0
                && normalizer_version == i64::from(NORMALIZER_VERSION),
            "invalid Inbox chunk metadata"
        );
        let stored_bytes = usize::try_from(stored_bytes).context("invalid Inbox payload size")?;
        ensure!(
            stored_bytes <= maximum_stored_chunk
                && (stored_bytes <= limits.chunk_bytes || stored_count == 1),
            "Inbox payload exceeds its storage bound"
        );
        let raw: Vec<u8> = row.get(10)?;
        ensure!(raw.len() == stored_bytes, "Inbox payload length changed");
        let payload: InboxPayload = serde_json::from_slice(&raw)?;
        ensure!(
            serde_json::to_vec(&payload)? == raw,
            "Inbox payload is not canonical serialized Record data"
        );
        validate_chunk(
            &payload,
            project_id,
            first_seq,
            last_seq,
            stored_count,
            received_at_us,
            limits,
        )?;
        if let Some((previous_id, previous_no)) = &previous_acceptance {
            if previous_id == &acceptance_id {
                ensure!(
                    chunk_no == previous_no + 1,
                    "Inbox acceptance chunk ordinal is not contiguous"
                );
            } else {
                ensure!(
                    chunk_no == 0,
                    "new Inbox acceptance does not start at chunk zero"
                );
            }
        }

        let next_records = record_count
            .checked_add(payload.records.len())
            .context("Indexer record count overflow")?;
        let next_bytes = byte_count
            .checked_add(raw.len())
            .context("Indexer byte count overflow")?;
        if next_records > limits.batch_records || next_bytes > limits.batch_bytes {
            ensure!(
                end.is_none() && !chunks.is_empty(),
                "committed or first Inbox chunk exceeds Indexer batch budget"
            );
            break;
        }

        expected_id = id.checked_add(1).context("Inbox ID exhausted")?;
        expected_seq = last_seq
            .checked_add(1)
            .context("ingest sequence exhausted")?;
        record_count = next_records;
        byte_count = next_bytes;
        previous_acceptance = Some((acceptance_id, chunk_no));
        chunks.push(InboxChunk {
            id,
            first_seq,
            last_seq,
            payload,
            bytes: raw.len(),
        });
    }

    if let Some(end) = end {
        let last = chunks.last().context("committed Inbox batch is missing")?;
        ensure!(
            last.id == end.inbox_id && last.last_seq == end.ingest_seq,
            "recovered Inbox does not reach the native boundary"
        );
    }
    Ok(chunks)
}

fn validate_chunk(
    payload: &InboxPayload,
    project_id: i64,
    first_seq: i64,
    last_seq: i64,
    stored_count: i64,
    received_at_us: i64,
    limits: &Limits,
) -> Result<()> {
    ensure!(
        payload.version == INBOX_VERSION && !payload.records.is_empty(),
        "unsupported or empty Inbox payload"
    );
    ensure!(
        payload.records.len() as i64 == stored_count
            && payload.records.first().map(|record| record.ingest_seq) == Some(first_seq)
            && payload.records.last().map(|record| record.ingest_seq) == Some(last_seq)
            && payload
                .records
                .windows(2)
                .all(|pair| pair[0].ingest_seq.checked_add(1) == Some(pair[1].ingest_seq)),
        "Inbox Record boundary does not match its row"
    );
    ensure!(
        payload.records.len() <= limits.chunk_records,
        "Inbox chunk Record count exceeds its bound"
    );
    for record in &payload.records {
        validate_record(record)?;
        ensure!(
            record.project_id == project_id
                && record.received_at_us == received_at_us
                && record.normalizer_version == NORMALIZER_VERSION,
            "Inbox Record metadata does not match its row"
        );
        ensure!(
            serde_json::to_vec(record)?.len() <= limits.record_bytes,
            "Inbox Record exceeds its hard bound"
        );
    }
    Ok(())
}

fn validate_record(record: &Record) -> Result<()> {
    ensure!(
        record.project_id > 0 && record.ingest_seq > 0 && is_lower_hex_digest(&record.record_id),
        "invalid normalized Record identity"
    );
    match record.kind {
        RecordKind::Error => {
            let issue_id = record
                .issue_id
                .as_deref()
                .context("Error missing issue ID")?;
            let fingerprint = record
                .fingerprint
                .as_deref()
                .context("Error missing fingerprint")?;
            ensure!(
                record.fingerprint_version.is_some()
                    && is_lower_hex_digest(issue_id)
                    && is_lower_hex_digest(fingerprint)
                    && record.source_event_id.as_deref().is_none_or(|event_id| {
                        event_id.len() == 32
                            && event_id
                                .bytes()
                                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    }),
                "invalid normalized Error identity"
            );
        }
        RecordKind::Log => ensure!(
            record.source_event_id.is_none()
                && record.issue_id.is_none()
                && record.fingerprint_version.is_none()
                && record.fingerprint.is_none(),
            "Log contains Error-only identity fields"
        ),
    }
    Ok(())
}

fn is_lower_hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn build_batch(db: &Connection, chunks: Vec<InboxChunk>) -> Result<PreparedBatch> {
    let boundary = chunks
        .last()
        .map(|chunk| Boundary {
            inbox_id: chunk.id,
            ingest_seq: chunk.last_seq,
        })
        .context("cannot build an empty Indexer batch")?;
    let records = select_unique_records(db, &chunks)?;
    Ok(PreparedBatch {
        chunks,
        records,
        boundary,
    })
}

fn select_unique_records(db: &Connection, chunks: &[InboxChunk]) -> Result<Vec<Record>> {
    let mut candidate_keys = Vec::new();
    let mut candidates_seen = HashSet::new();
    for record in chunks
        .iter()
        .flat_map(|chunk| chunk.payload.records.iter())
        .filter(|record| record.kind == RecordKind::Error)
    {
        if candidates_seen.insert(record.record_id.clone()) {
            candidate_keys.push(record.record_id.clone());
        }
    }

    let mut persisted = HashSet::new();
    for keys in candidate_keys.chunks(EVENT_KEY_QUERY_CHUNK) {
        let placeholders = std::iter::repeat_n("?", keys.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT event_key,record_id FROM issue_occurrences
             WHERE event_key IN ({placeholders}) OR record_id IN ({placeholders})"
        );
        let mut parameters = Vec::with_capacity(keys.len() * 2);
        parameters.extend(keys.iter());
        parameters.extend(keys.iter());
        let mut statement = db.prepare(&sql)?;
        let mut rows = statement.query(params_from_iter(parameters))?;
        while let Some(row) = rows.next()? {
            persisted.insert(row.get::<_, String>(0)?);
            persisted.insert(row.get::<_, String>(1)?);
        }
    }

    let mut records = Vec::new();
    let mut batch_errors = HashSet::new();
    for record in chunks.iter().flat_map(|chunk| chunk.payload.records.iter()) {
        if record.kind == RecordKind::Log
            || (!persisted.contains(&record.record_id)
                && batch_errors.insert(record.record_id.clone()))
        {
            records.push(record.clone());
        }
    }
    Ok(records)
}

fn expected_boundary(batch: &PreparedBatch) -> Result<Boundary> {
    let first = batch
        .chunks
        .first()
        .context("cannot finalize an empty batch")?;
    let last = batch
        .chunks
        .last()
        .context("cannot finalize an empty batch")?;
    let expected = Boundary {
        inbox_id: first.id.checked_sub(1).context("invalid first Inbox ID")?,
        ingest_seq: first
            .first_seq
            .checked_sub(1)
            .context("invalid first ingest sequence")?,
    };
    let mut expected_id = first.id;
    let mut expected_seq = first.first_seq;
    for chunk in &batch.chunks {
        ensure!(
            chunk.id == expected_id && chunk.first_seq == expected_seq,
            "prepared chunks are not contiguous"
        );
        ensure!(
            chunk
                .payload
                .records
                .first()
                .map(|record| record.ingest_seq)
                == Some(chunk.first_seq)
                && chunk.payload.records.last().map(|record| record.ingest_seq)
                    == Some(chunk.last_seq),
            "prepared chunk boundary changed"
        );
        expected_id = chunk.id.checked_add(1).context("Inbox ID exhausted")?;
        expected_seq = chunk
            .last_seq
            .checked_add(1)
            .context("ingest sequence exhausted")?;
    }
    ensure!(
        batch.boundary.inbox_id == last.id && batch.boundary.ingest_seq == last.last_seq,
        "prepared final boundary changed"
    );
    Ok(expected)
}

fn validate_stored_chunks(db: &Connection, batch: &PreparedBatch) -> Result<()> {
    for chunk in &batch.chunks {
        let stored: Option<StoredChunkRow> = db
            .query_row(
                "SELECT project_id,acceptance_id,chunk_no,first_ingest_seq,last_ingest_seq,
                        record_count,received_at_us,normalizer_version,payload
                 FROM inbox WHERE id=?1",
                [chunk.id],
                |row| {
                    Ok(StoredChunkRow {
                        project_id: row.get(0)?,
                        acceptance_id: row.get(1)?,
                        chunk_no: row.get(2)?,
                        first_seq: row.get(3)?,
                        last_seq: row.get(4)?,
                        record_count: row.get(5)?,
                        received_at_us: row.get(6)?,
                        normalizer_version: row.get(7)?,
                        raw: row.get(8)?,
                    })
                },
            )
            .optional()?;
        let stored = stored.context("prepared Inbox chunk disappeared before finalize")?;
        ensure!(
            !stored.acceptance_id.is_empty()
                && stored.chunk_no >= 0
                && stored.normalizer_version == i64::from(NORMALIZER_VERSION)
                && stored.first_seq == chunk.first_seq
                && stored.last_seq == chunk.last_seq
                && stored.record_count == chunk.payload.records.len() as i64
                && stored.raw.len() == chunk.bytes
                && stored.raw == serde_json::to_vec(&chunk.payload)?,
            "prepared Inbox chunk changed before finalize"
        );
        ensure!(
            chunk.payload.records.iter().all(|record| {
                record.project_id == stored.project_id
                    && record.received_at_us == stored.received_at_us
            }),
            "prepared Inbox row metadata changed before finalize"
        );
    }
    Ok(())
}

fn finalize_error(
    db: &Connection,
    shard_id: &str,
    record: &Record,
    event_deliveries: &mut usize,
) -> Result<()> {
    let issue_id = record
        .issue_id
        .as_deref()
        .context("Error missing issue ID")?;
    let fingerprint = record
        .fingerprint
        .as_deref()
        .context("Error missing fingerprint")?;
    let fingerprint_version = i64::from(
        record
            .fingerprint_version
            .context("Error missing fingerprint version")?,
    );
    let new_issue = db.execute(
        "INSERT INTO issues(
             id,project_id,fingerprint,fingerprint_version,title,culprit,level,status,
             first_seen_us,last_seen_us,occurrence_count,first_seen_ingest_seq,
             last_seen_ingest_seq,first_release,last_release,created_at_us,updated_at_us)
         VALUES(?1,?2,?3,?4,?5,NULL,?6,'unresolved',?7,?7,0,?8,?8,?9,?9,?10,?10)
         ON CONFLICT(id) DO NOTHING",
        params![
            issue_id,
            record.project_id,
            fingerprint,
            fingerprint_version,
            record.message,
            record.level,
            record.timestamp_us,
            record.ingest_seq,
            record.release,
            record.received_at_us
        ],
    )? == 1;
    let (stored_project, stored_version, stored_fingerprint, status, resolved_through): (
        i64,
        i64,
        String,
        String,
        Option<i64>,
    ) = db.query_row(
        "SELECT project_id,fingerprint_version,fingerprint,status,resolved_through_ingest_seq
         FROM issues WHERE id=?1",
        [issue_id],
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
    ensure!(
        stored_project == record.project_id
            && stored_version == fingerprint_version
            && stored_fingerprint == fingerprint,
        "Issue identity collision or corruption"
    );
    ensure!(
        status != "resolved" || resolved_through.is_some(),
        "resolved Issue is missing its receive watermark"
    );

    // The immutable normalized record_id is the event_key. For source IDs, normalization has
    // already derived both from the domain-separated project/source identity hash.
    let inserted = db.execute(
        "INSERT INTO issue_occurrences(
             event_key,project_id,issue_id,record_id,shard_id,source_event_id,ingest_seq,occurred_at_us)
         VALUES(?1,?2,?3,?1,?4,?5,?6,?7)
         ON CONFLICT(event_key) DO NOTHING",
        params![
            record.record_id,
            record.project_id,
            issue_id,
            shard_id,
            record.source_event_id,
            record.ingest_seq,
            record.timestamp_us
        ],
    )? == 1;
    if !inserted {
        ensure!(
            !new_issue,
            "new Issue occurrence unexpectedly already exists"
        );
        return Ok(());
    }

    let regression = status == "resolved"
        && resolved_through.is_some_and(|watermark| record.ingest_seq > watermark);
    ensure!(
        db.execute(
            "UPDATE issues SET
                 occurrence_count=occurrence_count+1,
                 first_seen_us=CASE
                   WHEN ?2<first_seen_us OR (?2=first_seen_us AND ?1<first_seen_ingest_seq)
                   THEN ?2 ELSE first_seen_us END,
                 first_seen_ingest_seq=CASE
                   WHEN ?2<first_seen_us OR (?2=first_seen_us AND ?1<first_seen_ingest_seq)
                   THEN ?1 ELSE first_seen_ingest_seq END,
                 first_release=CASE
                   WHEN ?2<first_seen_us OR (?2=first_seen_us AND ?1<first_seen_ingest_seq)
                   THEN ?3 ELSE first_release END,
                 last_seen_us=CASE
                   WHEN ?2>last_seen_us OR (?2=last_seen_us AND ?1>last_seen_ingest_seq)
                   THEN ?2 ELSE last_seen_us END,
                 last_seen_ingest_seq=CASE
                   WHEN ?2>last_seen_us OR (?2=last_seen_us AND ?1>last_seen_ingest_seq)
                   THEN ?1 ELSE last_seen_ingest_seq END,
                 last_release=CASE
                   WHEN ?2>last_seen_us OR (?2=last_seen_us AND ?1>last_seen_ingest_seq)
                   THEN ?3 ELSE last_release END,
                 title=CASE
                   WHEN ?2>last_seen_us OR (?2=last_seen_us AND ?1>last_seen_ingest_seq)
                   THEN ?4 ELSE title END,
                 level=CASE
                   WHEN ?2>last_seen_us OR (?2=last_seen_us AND ?1>last_seen_ingest_seq)
                   THEN ?5 ELSE level END,
                 status=CASE WHEN ?6 THEN 'unresolved' ELSE status END,
                 revision=revision+CASE WHEN ?6 THEN 1 ELSE 0 END,
                 resolved_at_us=CASE WHEN ?6 THEN NULL ELSE resolved_at_us END,
                 resolved_through_ingest_seq=CASE
                   WHEN ?6 THEN NULL ELSE resolved_through_ingest_seq END,
                 updated_at_us=?7
             WHERE id=?8",
            params![
                record.ingest_seq,
                record.timestamp_us,
                record.release,
                record.message,
                record.level,
                regression,
                record.received_at_us,
                issue_id
            ],
        )? == 1,
        "Issue disappeared during finalize"
    );
    if new_issue {
        enqueue_event_alerts(db, "new_issue", record, event_deliveries)?;
    } else if regression {
        enqueue_event_alerts(db, "regression", record, event_deliveries)?;
    }
    Ok(())
}

fn enqueue_event_alerts(
    db: &Connection,
    kind: &str,
    record: &Record,
    event_deliveries: &mut usize,
) -> Result<()> {
    let issue_id = record
        .issue_id
        .as_deref()
        .context("Error missing issue ID")?;
    let mut statement = db.prepare(
        "SELECT id,revision,destination_json FROM alerts
         WHERE condition_type=?1 AND enabled=1 AND deleted_at_us IS NULL
           AND (project_id IS NULL OR project_id=?2)
         ORDER BY id LIMIT 1001",
    )?;
    let alert_ids = statement
        .query_map(params![kind, record.project_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(statement);
    let next_delivery_count = event_deliveries
        .checked_add(alert_ids.len())
        .context("event alert count overflow")?;
    ensure!(
        next_delivery_count <= MAX_EVENT_DELIVERIES_PER_BATCH,
        "enabled event alert count exceeds finalize bound"
    );
    *event_deliveries = next_delivery_count;
    for (alert_id, alert_revision, destination_json) in alert_ids {
        let dedupe_key = format!(
            "eventglass:{alert_id}:{kind}:{issue_id}:{}",
            record.ingest_seq
        );
        let delivery_id = format!(
            "{:x}",
            Sha256::digest(format!("eventglass.alert.delivery.v1:{dedupe_key}").as_bytes())
        );
        let destination: serde_json::Value = serde_json::from_str(&destination_json)?;
        let payload = json!({
            "version": 1,
            "kind": kind,
            "alert_id": alert_id.to_string(),
            "alert_revision": alert_revision,
            "issue_id": issue_id,
            "project_id": record.project_id.to_string(),
            "trigger_ingest_seq": record.ingest_seq.to_string(),
            "destination": destination
        })
        .to_string();
        let inserted = db.execute(
            "INSERT INTO alert_deliveries(
                 id,alert_id,dedupe_key,payload_json,state,attempts,next_retry_at_us,created_at_us)
             VALUES(?1,?2,?3,?4,'pending',0,?5,?5)
             ON CONFLICT(dedupe_key) DO NOTHING",
            params![
                delivery_id,
                alert_id,
                dedupe_key,
                payload,
                record.received_at_us
            ],
        )?;
        if inserted == 1 {
            db.execute(
                "UPDATE alerts SET last_triggered_at_us=?1 WHERE id=?2",
                params![record.received_at_us, alert_id],
            )?;
        }
    }
    Ok(())
}

fn update_shard(
    db: &Connection,
    shard_id: &str,
    expected: Boundary,
    batch: &PreparedBatch,
) -> Result<()> {
    let record_count = checked_usize_to_i64(batch.records.len(), "native Record count")?;
    let min_timestamp = batch.records.iter().map(|record| record.timestamp_us).min();
    let max_timestamp = batch.records.iter().map(|record| record.timestamp_us).max();
    let min_received = batch
        .records
        .iter()
        .map(|record| record.received_at_us)
        .min();
    let max_received = batch
        .records
        .iter()
        .map(|record| record.received_at_us)
        .max();
    let min_ingest = batch.records.iter().map(|record| record.ingest_seq).min();
    let max_ingest = batch.records.iter().map(|record| record.ingest_seq).max();
    let first_received = batch.records.first().map(|record| record.received_at_us);
    ensure!(
        db.execute(
            "UPDATE shards SET
                 last_applied_inbox_id=?1,
                 record_count=record_count+?2,
                 min_timestamp_us=CASE WHEN ?3 IS NULL THEN min_timestamp_us
                   WHEN min_timestamp_us IS NULL OR ?3<min_timestamp_us THEN ?3
                   ELSE min_timestamp_us END,
                 max_timestamp_us=CASE WHEN ?4 IS NULL THEN max_timestamp_us
                   WHEN max_timestamp_us IS NULL OR ?4>max_timestamp_us THEN ?4
                   ELSE max_timestamp_us END,
                 min_received_at_us=CASE WHEN ?5 IS NULL THEN min_received_at_us
                   WHEN min_received_at_us IS NULL OR ?5<min_received_at_us THEN ?5
                   ELSE min_received_at_us END,
                 max_received_at_us=CASE WHEN ?6 IS NULL THEN max_received_at_us
                   WHEN max_received_at_us IS NULL OR ?6>max_received_at_us THEN ?6
                   ELSE max_received_at_us END,
                 first_record_received_at_us=coalesce(first_record_received_at_us,?9),
                 min_ingest_seq=CASE WHEN ?7 IS NULL THEN min_ingest_seq
                   WHEN min_ingest_seq IS NULL OR ?7<min_ingest_seq THEN ?7
                   ELSE min_ingest_seq END,
                 max_ingest_seq=CASE WHEN ?8 IS NULL THEN max_ingest_seq
                   WHEN max_ingest_seq IS NULL OR ?8>max_ingest_seq THEN ?8
                   ELSE max_ingest_seq END
             WHERE id=?10 AND state='active' AND last_applied_inbox_id=?11",
            params![
                batch.boundary.inbox_id,
                record_count,
                min_timestamp,
                max_timestamp,
                min_received,
                max_received,
                min_ingest,
                max_ingest,
                first_received,
                shard_id,
                expected.inbox_id
            ],
        )? == 1,
        "active shard boundary CAS failed"
    );
    Ok(())
}

fn checked_usize_to_i64(value: usize, name: &str) -> Result<i64> {
    i64::try_from(value).with_context(|| format!("{name} exceeds SQLite integer range"))
}
