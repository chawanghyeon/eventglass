use anyhow::Result;
use eventglass::{
    config::Limits,
    db::{
        self,
        ingest::{self, IngestProject},
    },
    model::{Record, RecordKind},
};
use serde_json::json;

fn project() -> IngestProject {
    IngestProject {
        id: 1,
        slug: "test".into(),
        public_key: "public-test-key".into(),
    }
}

fn record(ordinal: usize) -> Record {
    Record {
        record_id: format!("record-{ordinal}"),
        kind: RecordKind::Log,
        project_id: 1,
        source_event_id: None,
        ingest_seq: 0,
        received_at_us: 1,
        timestamp_us: 1,
        service: "test".into(),
        environment: None,
        release: None,
        level: "info".into(),
        logger: None,
        message: "hello".into(),
        trace_id: None,
        span_id: None,
        request_id: None,
        user_id: None,
        user_email: None,
        issue_id: None,
        fingerprint_version: None,
        fingerprint: None,
        attributes: json!({}),
        search_text: "hello".into(),
        raw_json: json!({"body":"hello"}),
        normalizer_version: 1,
        indexing_warnings: vec![],
    }
}

fn database() -> Result<(tempfile::TempDir, rusqlite::Connection)> {
    let dir = tempfile::tempdir()?;
    let db = db::open(&dir.path().join("meta.db"))?;
    db.execute_batch("INSERT INTO runtime_state(singleton,installation_id,storage_generation,next_ingest_seq) VALUES(1,'test-install','test-generation',1);
        INSERT INTO projects(id,slug,name,created_at_us,updated_at_us) VALUES(1,'test','Test',0,0);
        INSERT INTO project_keys(project_id,public_key,created_at_us) VALUES(1,'public-test-key',0);")?;
    Ok((dir, db))
}

#[test]
fn atomic_acceptance_chunks_sequences_and_reopen() -> Result<()> {
    let (dir, mut db) = database()?;
    let limits = Limits::default();
    let receipt = ingest::accept(
        &mut db,
        project(),
        "request-1",
        (0..300).map(record).collect(),
        &limits,
    )?;
    assert_eq!(receipt.accepted, 300);
    assert_eq!(receipt.first_ingest_seq.as_deref(), Some("1"));
    assert_eq!(receipt.last_ingest_seq.as_deref(), Some("300"));
    let count: i64 = db.query_row("SELECT count(*) FROM inbox", [], |r| r.get(0))?;
    assert_eq!(count, 3);
    drop(db);
    let db = db::open(&dir.path().join("meta.db"))?;
    let batch = ingest::next_batch(
        &db,
        &Limits {
            batch_records: 128,
            ..limits
        },
    )?;
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].payload.records.len(), 128);
    let next: i64 = db.query_row("SELECT next_ingest_seq FROM runtime_state", [], |r| {
        r.get(0)
    })?;
    assert_eq!(next, 301);
    let bytes_match: bool = db.query_row(
        "SELECT inbox_bytes=(SELECT sum(length(payload)) FROM inbox) FROM runtime_state",
        [],
        |r| r.get(0),
    )?;
    assert!(bytes_match);
    Ok(())
}

#[test]
fn invalid_request_or_revoked_key_does_not_allocate_or_store() -> Result<()> {
    let (_dir, mut db) = database()?;
    let mut wrong = record(1);
    wrong.project_id = 2;
    assert!(
        ingest::accept(
            &mut db,
            project(),
            "invalid",
            vec![record(0), wrong],
            &Limits::default()
        )
        .is_err()
    );
    db.execute("UPDATE project_keys SET revoked_at_us=1", [])?;
    assert!(
        ingest::accept(
            &mut db,
            project(),
            "revoked",
            vec![record(0)],
            &Limits::default()
        )
        .is_err()
    );
    let state: (i64, i64, i64) = db.query_row(
        "SELECT next_ingest_seq,inbox_bytes,inbox_records FROM runtime_state",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    assert_eq!(state, (1, 0, 0));
    assert_eq!(
        db.query_row("SELECT count(*) FROM inbox", [], |r| r.get::<_, i64>(0))?,
        0
    );
    Ok(())
}

#[test]
fn oversized_late_record_rolls_back_entire_request() -> Result<()> {
    let (_dir, mut db) = database()?;
    let mut records: Vec<_> = (0..130).map(record).collect();
    records.last_mut().unwrap().raw_json = json!({"message":"x".repeat(1024*1024)});
    assert!(ingest::accept(&mut db, project(), "too-large", records, &Limits::default()).is_err());
    assert_eq!(
        db.query_row("SELECT count(*) FROM inbox", [], |r| r.get::<_, i64>(0))?,
        0
    );
    assert_eq!(
        db.query_row("SELECT next_ingest_seq FROM runtime_state", [], |r| r
            .get::<_, i64>(0))?,
        1
    );
    Ok(())
}
