//! Durable atomic acceptance, complete Inbox chunks, and sequence allocation.

use anyhow::{Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

use crate::{config::Limits, model::Record};

#[derive(Debug, Clone)]
pub struct IngestProject {
    pub id: i64,
    pub slug: String,
    pub public_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InboxPayload {
    pub version: u32,
    pub records: Vec<Record>,
}

#[derive(Debug)]
pub struct InboxChunk {
    pub id: i64,
    pub first_seq: i64,
    pub last_seq: i64,
    pub payload: InboxPayload,
    pub bytes: usize,
}

#[derive(Debug, Serialize)]
pub struct Acceptance {
    pub accepted: usize,
    pub accepted_replay_segments: usize,
    pub accepted_feedback: usize,
    pub first_ingest_seq: Option<String>,
    pub last_ingest_seq: Option<String>,
}

struct StagedChunk {
    first_seq: i64,
    last_seq: i64,
    received_at_us: i64,
    count: usize,
    bytes: Vec<u8>,
}

fn required_i64(db: &Connection, sql: &str) -> Result<i64> {
    Ok(db.query_row(sql, [], |row| row.get(0))?)
}

fn inbox_bytes(db: &Connection) -> Result<i64> {
    required_i64(
        db,
        "SELECT inbox_bytes FROM runtime_state WHERE singleton=1",
    )
}

fn next_ingest_seq(db: &Connection) -> Result<i64> {
    required_i64(
        db,
        "SELECT next_ingest_seq FROM runtime_state WHERE singleton=1",
    )
}

impl StagedChunk {
    fn serialize(payload: &InboxPayload) -> Result<Self> {
        let first = payload.records.first().expect("nonempty staged chunk");
        let last = payload.records.last().expect("nonempty staged chunk");
        Ok(Self {
            first_seq: first.ingest_seq,
            last_seq: last.ingest_seq,
            received_at_us: first.received_at_us,
            count: payload.records.len(),
            bytes: serde_json::to_vec(payload)?,
        })
    }
}

#[derive(Debug)]
pub enum IngestError {
    Unauthorized,
    TooLarge,
    InvalidRecord,
    InboxFull,
}
impl std::fmt::Display for IngestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for IngestError {}

pub fn lookup_project(db: &Connection, id: i64, key: &str) -> Result<Option<IngestProject>> {
    Ok(db.query_row("SELECT p.id,p.slug,k.public_key FROM projects p JOIN project_keys k ON p.id=k.project_id WHERE p.id=?1 AND k.public_key=?2 AND p.is_active=1 AND k.revoked_at_us IS NULL",params![id,key],|r|Ok(IngestProject{id:r.get(0)?,slug:r.get(1)?,public_key:r.get(2)?})).optional()?)
}

/// Called on DbWorker after normalization. ACK may be sent only after this returns.
pub fn accept(
    db: &mut Connection,
    project: IngestProject,
    acceptance_id: &str,
    records: Vec<Record>,
    limits: &Limits,
) -> Result<Acceptance> {
    accept_with_replay(
        db,
        project,
        acceptance_id,
        records,
        limits,
        None,
        Vec::new(),
        crate::model::now_us()?,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn accept_with_replay(
    db: &mut Connection,
    project: IngestProject,
    acceptance_id: &str,
    records: Vec<Record>,
    limits: &Limits,
    replay: Option<super::replays::PreparedReplay>,
    feedback: Vec<serde_json::Value>,
    received_at_us: i64,
) -> Result<Acceptance> {
    if records.len() > limits.request_records {
        return Err(IngestError::TooLarge.into());
    }
    let tx = db.transaction()?;
    if lookup_project(&tx, project.id, &project.public_key)?.is_none() {
        return Err(IngestError::Unauthorized.into());
    }
    // Prevent an indefinitely stalled Indexer from filling the volume; disk reservation
    // admission is an additional independent requirement at the HTTP boundary.
    let pending = inbox_bytes(&tx)?;
    if pending >= 256 * 1024 * 1024 {
        return Err(IngestError::InboxFull.into());
    }
    let mut next = next_ingest_seq(&tx)?;
    let first = next;
    let accepted = records.len();
    let mut current = InboxPayload {
        version: 1,
        records: Vec::new(),
    };
    let mut chunks = Vec::<StagedChunk>::new();
    let mut approximate_bytes = 0usize;
    let mut total_bytes = 0usize;
    for mut record in records {
        if record.project_id != project.id
            || record.ingest_seq != 0
            || record.normalizer_version != 1
        {
            return Err(IngestError::InvalidRecord.into());
        }
        record.ingest_seq = next;
        next = next
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("ingest sequence exhausted"))?;
        let size = serde_json::to_vec(&record)?.len();
        if size > limits.record_bytes {
            return Err(IngestError::TooLarge.into());
        }
        if !current.records.is_empty()
            && (current.records.len() >= limits.chunk_records
                || approximate_bytes + size + 32 > limits.chunk_bytes)
        {
            let chunk = StagedChunk::serialize(&current)?;
            total_bytes += chunk.bytes.len();
            chunks.push(chunk);
            current.records.clear();
            approximate_bytes = 0;
        }
        approximate_bytes += size + 1;
        current.records.push(record);
    }
    if !current.records.is_empty() {
        let chunk = StagedChunk::serialize(&current)?;
        total_bytes += chunk.bytes.len();
        chunks.push(chunk);
    }
    if total_bytes > limits.decoded_bytes {
        return Err(IngestError::TooLarge.into());
    }
    if pending + total_bytes as i64 > 256 * 1024 * 1024 {
        return Err(IngestError::InboxFull.into());
    }
    for (ordinal, chunk) in chunks.iter().enumerate() {
        tx.execute("INSERT INTO inbox(project_id,acceptance_id,chunk_no,first_ingest_seq,last_ingest_seq,record_count,received_at_us,normalizer_version,payload) VALUES(?1,?2,?3,?4,?5,?6,?7,1,?8)",params![project.id,acceptance_id,ordinal as i64,chunk.first_seq,chunk.last_seq,chunk.count as i64,chunk.received_at_us,chunk.bytes])?;
    }
    tx.execute(
        "UPDATE runtime_state SET next_ingest_seq=?1,inbox_bytes=inbox_bytes+?2,inbox_records=inbox_records+?3 WHERE singleton=1",
        params![next,total_bytes as i64,accepted as i64],
    )?;
    if let Some(prepared) = &replay {
        super::replays::accept(&tx, project.id, prepared, received_at_us)?;
    }
    for item in &feedback {
        let id = crate::sentry::replay::canonical_id(&item["event_id"])?;
        let replay_id = item
            .pointer("/contexts/feedback/replay_id")
            .map(crate::sentry::replay::canonical_id)
            .transpose()?;
        let timestamp_ms = item["timestamp"]
            .as_f64()
            .filter(|t| t.is_finite() && *t >= 0.0 && *t < 253402300799.0)
            .map(|t| (t * 1000.0) as i64)
            .unwrap_or(received_at_us / 1000);
        let payload = serde_json::to_string(item)?;
        if payload.len() > limits.record_bytes {
            return Err(IngestError::TooLarge.into());
        }
        if tx.execute("INSERT INTO feedback(project_id,event_id,replay_id,timestamp_ms,expires_at_us,payload) VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(project_id,event_id) DO NOTHING",params![project.id,id,replay_id,timestamp_ms,received_at_us.checked_add(super::replays::RETENTION_US).ok_or(IngestError::TooLarge)?,payload])?>0 {super::replays::mark_dirty(&tx,received_at_us)?;}
    }
    tx.commit()?;
    Ok(Acceptance {
        accepted,
        accepted_replay_segments: usize::from(replay.is_some()),
        accepted_feedback: feedback.len(),
        first_ingest_seq: (accepted > 0).then(|| first.to_string()),
        last_ingest_seq: (accepted > 0).then(|| (next - 1).to_string()),
    })
}

/// Only complete chunks are returned. A single oversized record is still bounded
/// by the ingestion Record hard limit and fits the Indexer batch limit.
pub fn next_batch(db: &Connection, limits: &Limits) -> Result<Vec<InboxChunk>> {
    let applied: i64 = db.query_row(
        "SELECT last_applied_inbox_id FROM runtime_state WHERE singleton=1",
        [],
        |r| r.get(0),
    )?;
    let mut statement=db.prepare("SELECT id,first_ingest_seq,last_ingest_seq,record_count,payload FROM inbox WHERE id>?1 ORDER BY id LIMIT 1000")?;
    let mut rows = statement.query([applied])?;
    let mut batch = Vec::new();
    let mut count = 0;
    let mut bytes_total = 0;
    while let Some(row) = rows.next()? {
        let raw: Vec<u8> = row.get(4)?;
        let payload: InboxPayload = serde_json::from_slice(&raw)?;
        let first: i64 = row.get(1)?;
        let last: i64 = row.get(2)?;
        let stored_count: i64 = row.get(3)?;
        ensure!(
            payload.version == 1 && !payload.records.is_empty(),
            "unsupported or empty Inbox chunk"
        );
        ensure!(
            payload.records.len() as i64 == stored_count
                && payload.records.first().unwrap().ingest_seq == first
                && payload.records.last().unwrap().ingest_seq == last,
            "Inbox boundary mismatch"
        );
        ensure!(
            payload
                .records
                .windows(2)
                .all(|p| p[1].ingest_seq == p[0].ingest_seq + 1),
            "Inbox sequence ordering mismatch"
        );
        if count + payload.records.len() > limits.batch_records
            || bytes_total + raw.len() > limits.batch_bytes
        {
            ensure!(
                !batch.is_empty(),
                "Inbox chunk exceeds Indexer batch budget"
            );
            break;
        }
        count += payload.records.len();
        bytes_total += raw.len();
        batch.push(InboxChunk {
            id: row.get(0)?,
            first_seq: first,
            last_seq: last,
            payload,
            bytes: raw.len(),
        });
    }
    Ok(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::RecordKind;
    use serde_json::json;

    fn database() -> (tempfile::TempDir, Connection, IngestProject) {
        let directory = tempfile::tempdir().expect("temporary ingest database");
        let database =
            crate::db::open(&directory.path().join("meta.db")).expect("open ingest database");
        database
            .execute_batch(
                "INSERT INTO projects(id,slug,name,created_at_us,updated_at_us) VALUES(1,'p','Project',0,0);
                 INSERT INTO project_keys(id,project_id,public_key,created_at_us) VALUES(1,1,'public',0);
                 INSERT INTO runtime_state(singleton,installation_id,storage_generation,next_ingest_seq)
                 VALUES(1,'installation','generation',1);",
            )
            .expect("seed ingest authorization and runtime");
        (
            directory,
            database,
            IngestProject {
                id: 1,
                slug: "p".into(),
                public_key: "public".into(),
            },
        )
    }

    fn record() -> Record {
        Record {
            record_id: "r".into(),
            kind: RecordKind::Log,
            project_id: 1,
            source_event_id: None,
            ingest_seq: 0,
            received_at_us: 1,
            timestamp_us: 1,
            service: "service".into(),
            environment: None,
            release: None,
            level: "info".into(),
            logger: None,
            message: "message".into(),
            trace_id: None,
            span_id: None,
            request_id: None,
            user_id: None,
            user_email: None,
            issue_id: None,
            fingerprint_version: None,
            fingerprint: None,
            attributes: json!({}),
            search_text: "message".into(),
            raw_json: json!({}),
            normalizer_version: 1,
            indexing_warnings: Vec::new(),
        }
    }

    #[test]
    fn acceptance_limits_fail_before_any_durable_ack() {
        for error in [
            IngestError::Unauthorized,
            IngestError::TooLarge,
            IngestError::InvalidRecord,
            IngestError::InboxFull,
        ] {
            assert!(!error.to_string().is_empty());
        }

        let (_directory, mut database, project) = database();
        let limits = Limits {
            request_records: 0,
            ..Limits::default()
        };
        assert!(accept(&mut database, project.clone(), "a", vec![record()], &limits).is_err());

        database
            .execute(
                "UPDATE runtime_state SET inbox_bytes=?1",
                [256 * 1024 * 1024],
            )
            .expect("fill fixture Inbox");
        assert!(
            accept(
                &mut database,
                project.clone(),
                "b",
                Vec::new(),
                &Limits::default()
            )
            .is_err()
        );

        database
            .execute("UPDATE runtime_state SET inbox_bytes=0", [])
            .expect("empty fixture Inbox");
        let limits = Limits {
            decoded_bytes: 0,
            ..Limits::default()
        };
        assert!(accept(&mut database, project.clone(), "c", vec![record()], &limits).is_err());

        database
            .execute(
                "UPDATE runtime_state SET inbox_bytes=?1",
                [256 * 1024 * 1024 - 1],
            )
            .expect("nearly fill fixture Inbox");
        assert!(
            accept(
                &mut database,
                project.clone(),
                "d",
                vec![record()],
                &Limits::default()
            )
            .is_err()
        );

        database
            .execute("UPDATE runtime_state SET inbox_bytes=0", [])
            .expect("empty fixture Inbox again");
        let limits = Limits {
            record_bytes: 1,
            ..Limits::default()
        };
        let feedback = json!({
            "event_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "contexts":{"feedback":{"message":"safe"}}
        });
        assert!(
            accept_with_replay(
                &mut database,
                project,
                "e",
                Vec::new(),
                &limits,
                None,
                vec![feedback],
                1,
            )
            .is_err()
        );
    }

    #[test]
    fn corrupt_runtime_counters_never_allocate_or_ack() {
        let project = IngestProject {
            id: 1,
            slug: "p".into(),
            public_key: "public".into(),
        };
        for runtime_row in ["x'80',1", "0,x'80'"] {
            let mut database =
                Connection::open_in_memory().expect("open malformed ingest database");
            database
                .execute_batch(&format!(
                    "CREATE TABLE projects(id INTEGER,is_active INTEGER);
                     CREATE TABLE project_keys(project_id INTEGER,public_key TEXT,revoked_at_us INTEGER);
                     CREATE TABLE runtime_state(singleton INTEGER,inbox_bytes BLOB,next_ingest_seq BLOB);
                     INSERT INTO projects VALUES(1,1);
                     INSERT INTO project_keys VALUES(1,'public',NULL);
                     INSERT INTO runtime_state VALUES(1,{runtime_row});"
                ))
                .expect("seed malformed ingest runtime");
            assert!(
                accept(
                    &mut database,
                    project.clone(),
                    "acceptance",
                    Vec::new(),
                    &Limits::default()
                )
                .is_err()
            );
        }

        let database = Connection::open_in_memory().expect("open malformed Indexer cursor");
        database
            .execute_batch(
                "CREATE TABLE runtime_state(singleton INTEGER,last_applied_inbox_id BLOB);
                 INSERT INTO runtime_state VALUES(1,x'80');",
            )
            .expect("seed malformed Indexer cursor");
        assert!(next_batch(&database, &Limits::default()).is_err());
    }
}
