use anyhow::Result;
use eventglass::{
    config::Limits,
    db::{self, indexer, ingest},
    model::{Boundary, Record, RecordKind},
};
use rusqlite::{Connection, params};
use serde_json::json;
use sha2::{Digest, Sha256};

fn database() -> Result<(tempfile::TempDir, Connection)> {
    let directory = tempfile::tempdir()?;
    let db = db::open(&directory.path().join("meta.db"))?;
    db.execute(
        "INSERT INTO runtime_state(
             singleton,installation_id,storage_generation,next_ingest_seq)
         VALUES(1,'installation','generation',1)",
        [],
    )?;
    db.execute(
        "INSERT INTO projects(id,slug,name,created_at_us,updated_at_us)
         VALUES(1,'project','Project',1,1)",
        [],
    )?;
    db.execute(
        "INSERT INTO project_keys(id,project_id,public_key,created_at_us)
         VALUES(1,1,'public-key',1)",
        [],
    )?;
    db.execute(
        "INSERT INTO shards(
             id,schema_version,format_version,tokenizer_version,state,
             last_applied_inbox_id,record_count,size_bytes,created_at_us)
         VALUES('active',1,'7',1,'active',0,0,0,1)",
        [],
    )?;
    db.execute(
        "UPDATE runtime_state SET active_shard_id='active' WHERE singleton=1",
        [],
    )?;
    Ok((directory, db))
}

fn project() -> ingest::IngestProject {
    ingest::IngestProject {
        id: 1,
        slug: "project".into(),
        public_key: "public-key".into(),
    }
}

fn digest(label: &str) -> String {
    format!("{:x}", Sha256::digest(label.as_bytes()))
}

fn error(source: u128, timestamp_us: i64, release: Option<&str>, message: &str) -> Record {
    let source_event_id = format!("{source:032x}");
    Record {
        record_id: digest(&format!("eventglass.error.1.{source_event_id}")),
        kind: RecordKind::Error,
        project_id: 1,
        source_event_id: Some(source_event_id),
        ingest_seq: 0,
        received_at_us: 2_000_000,
        timestamp_us,
        service: "service".into(),
        environment: Some("test".into()),
        release: release.map(str::to_owned),
        level: "error".into(),
        logger: None,
        message: message.into(),
        trace_id: None,
        span_id: None,
        request_id: None,
        user_id: None,
        user_email: None,
        issue_id: Some(digest("eventglass.issue.shared")),
        fingerprint_version: Some(1),
        fingerprint: Some(digest("eventglass.fingerprint.shared")),
        attributes: json!({}),
        search_text: message.into(),
        raw_json: json!({"event_id":format!("{source:032x}"),"message":message}),
        normalizer_version: 1,
        indexing_warnings: Vec::new(),
    }
}

fn log(identity: &str, timestamp_us: i64) -> Record {
    Record {
        record_id: digest(&format!("eventglass.log.{identity}")),
        kind: RecordKind::Log,
        project_id: 1,
        source_event_id: None,
        ingest_seq: 0,
        received_at_us: 2_000_000,
        timestamp_us,
        service: "service".into(),
        environment: Some("test".into()),
        release: None,
        level: "info".into(),
        logger: None,
        message: "repeatable log".into(),
        trace_id: None,
        span_id: None,
        request_id: None,
        user_id: None,
        user_email: None,
        issue_id: None,
        fingerprint_version: None,
        fingerprint: None,
        attributes: json!({}),
        search_text: "repeatable log".into(),
        raw_json: json!({"body":"repeatable log"}),
        normalizer_version: 1,
        indexing_warnings: Vec::new(),
    }
}

fn accept(
    db: &mut Connection,
    acceptance: &str,
    records: Vec<Record>,
    limits: &Limits,
) -> Result<()> {
    ingest::accept(db, project(), acceptance, records, limits)?;
    Ok(())
}

fn clone_batch(batch: &indexer::PreparedBatch) -> indexer::PreparedBatch {
    indexer::PreparedBatch {
        chunks: batch
            .chunks
            .iter()
            .map(|chunk| ingest::InboxChunk {
                id: chunk.id,
                first_seq: chunk.first_seq,
                last_seq: chunk.last_seq,
                payload: chunk.payload.clone(),
                bytes: chunk.bytes,
            })
            .collect(),
        records: batch.records.clone(),
        boundary: batch.boundary,
    }
}

#[test]
fn prepare_deduplicates_errors_but_never_logs_and_finalize_replays_once() -> Result<()> {
    let (_directory, mut db) = database()?;
    let duplicate_error = error(1, 200, Some("release-2"), "shared error");
    let duplicate_log = log("same", 210);
    accept(
        &mut db,
        "first",
        vec![
            duplicate_error.clone(),
            duplicate_error.clone(),
            duplicate_log.clone(),
            duplicate_log,
        ],
        &Limits::default(),
    )?;
    let batch = indexer::prepare(&db, &Limits::default())?.expect("batch");
    assert_eq!(
        batch.boundary,
        Boundary {
            inbox_id: 1,
            ingest_seq: 4
        }
    );
    assert_eq!(
        batch
            .records
            .iter()
            .map(|record| record.ingest_seq)
            .collect::<Vec<_>>(),
        [1, 3, 4]
    );
    let replay = clone_batch(&batch);
    assert_eq!(
        indexer::finalize(&mut db, "active", batch)?,
        replay.boundary
    );
    assert_eq!(
        indexer::finalize(&mut db, "active", replay)?,
        Boundary {
            inbox_id: 1,
            ingest_seq: 4
        }
    );
    let issue: (i64, String) =
        db.query_row("SELECT occurrence_count,status FROM issues", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?;
    assert_eq!(issue, (1, "unresolved".into()));
    assert!(db.query_row(
        "SELECT event_key=record_id FROM issue_occurrences",
        [],
        |row| { row.get::<_, bool>(0) }
    )?);
    let shard: (i64, i64, i64, i64) = db.query_row(
        "SELECT last_applied_inbox_id,record_count,min_ingest_seq,max_ingest_seq
         FROM shards WHERE id='active'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    assert_eq!(shard, (1, 3, 1, 4));
    let runtime: (i64, i64, i64, i64) = db.query_row(
        "SELECT last_applied_inbox_id,last_applied_ingest_seq,inbox_bytes,inbox_records
         FROM runtime_state",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    assert_eq!(runtime, (1, 4, 0, 0));

    accept(&mut db, "retry", vec![duplicate_error], &Limits::default())?;
    let all_duplicate = indexer::prepare(&db, &Limits::default())?.expect("empty native batch");
    assert!(all_duplicate.records.is_empty());
    assert_eq!(
        all_duplicate.boundary,
        Boundary {
            inbox_id: 2,
            ingest_seq: 5
        }
    );
    indexer::finalize(&mut db, "active", all_duplicate)?;
    assert_eq!(
        db.query_row("SELECT occurrence_count FROM issues", [], |row| {
            row.get::<_, i64>(0)
        })?,
        1
    );
    assert_eq!(
        db.query_row("SELECT record_count FROM shards", [], |row| {
            row.get::<_, i64>(0)
        })?,
        3
    );
    assert_eq!(
        indexer::applied(&db)?,
        Boundary {
            inbox_id: 2,
            ingest_seq: 5
        }
    );
    Ok(())
}

#[test]
fn recovery_requires_every_exact_bounded_chunk_and_committed_pair() -> Result<()> {
    let limits = Limits {
        chunk_records: 1,
        batch_records: 2,
        ..Limits::default()
    };
    let (_directory, mut db) = database()?;
    accept(
        &mut db,
        "recovery",
        vec![error(1, 100, None, "error"), log("one", 110)],
        &limits,
    )?;
    let committed = indexer::prepare(&db, &limits)?.expect("batch").boundary;
    assert_eq!(
        committed,
        Boundary {
            inbox_id: 2,
            ingest_seq: 2
        }
    );
    assert!(
        indexer::recover_batch(
            &db,
            Boundary {
                inbox_id: 2,
                ingest_seq: 3
            },
            &limits,
        )
        .is_err()
    );
    let mut too_small = limits.clone();
    too_small.batch_records = 1;
    assert!(indexer::recover_batch(&db, committed, &too_small).is_err());
    let recovered = indexer::recover_batch(&db, committed, &limits)?;
    assert_eq!(recovered.records.len(), 2);
    indexer::finalize(&mut db, "active", recovered)?;
    assert_eq!(indexer::applied(&db)?, committed);

    let (_missing_directory, mut missing) = database()?;
    accept(
        &mut missing,
        "missing",
        vec![error(1, 100, None, "error"), log("one", 110)],
        &limits,
    )?;
    let missing_boundary = indexer::prepare(&missing, &limits)?
        .expect("batch")
        .boundary;
    missing.execute("DELETE FROM inbox WHERE id=1", [])?;
    assert!(indexer::recover_batch(&missing, missing_boundary, &limits).is_err());
    assert_eq!(indexer::applied(&missing)?, Boundary::default());
    Ok(())
}

#[test]
fn lifecycle_uses_finalize_time_state_tuple_order_and_deterministic_outbox() -> Result<()> {
    let (_directory, mut db) = database()?;
    for (id, condition) in [(1, "new_issue"), (2, "regression")] {
        db.execute(
            r#"INSERT INTO alerts(
                 id,project_id,name,condition_type,condition_json,destination_type,
                 destination_json,enabled,created_at_us,updated_at_us)
             VALUES(?1,1,?2,?3,'{}','webhook',
                 '{"type":"webhook","url":"https://hooks.example.test"}',1,1,1)"#,
            params![id, condition, condition],
        )?;
    }

    accept(
        &mut db,
        "initial",
        vec![error(1, 200, Some("release-2"), "middle")],
        &Limits::default(),
    )?;
    let initial = indexer::prepare(&db, &Limits::default())?.expect("initial");
    indexer::finalize(&mut db, "active", initial)?;
    assert_eq!(
        db.query_row("SELECT count(*) FROM alert_deliveries", [], |row| {
            row.get::<_, i64>(0)
        })?,
        1
    );

    accept(
        &mut db,
        "backlog",
        vec![error(2, 300, Some("release-3"), "backlog")],
        &Limits::default(),
    )?;
    let backlog = indexer::prepare(&db, &Limits::default())?.expect("backlog");
    db.execute(
        "UPDATE issues SET status='resolved',resolved_at_us=10,
             resolved_through_ingest_seq=2,revision=revision+1",
        [],
    )?;
    indexer::finalize(&mut db, "active", backlog)?;
    let resolved: (String, i64, i64, Option<i64>) = db.query_row(
        "SELECT status,occurrence_count,revision,resolved_through_ingest_seq FROM issues",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    assert_eq!(resolved, ("resolved".into(), 2, 1, Some(2)));
    assert_eq!(
        db.query_row("SELECT count(*) FROM alert_deliveries", [], |row| {
            row.get::<_, i64>(0)
        })?,
        1,
        "pre-resolve backlog is not a regression"
    );

    accept(
        &mut db,
        "regression",
        vec![error(3, 400, Some("release-4"), "regression")],
        &Limits::default(),
    )?;
    let regression = indexer::prepare(&db, &Limits::default())?.expect("regression");
    indexer::finalize(&mut db, "active", regression)?;
    let reopened: (String, i64, i64, Option<i64>, Option<i64>) = db.query_row(
        "SELECT status,occurrence_count,revision,resolved_at_us,resolved_through_ingest_seq
         FROM issues",
        [],
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
    assert_eq!(reopened, ("unresolved".into(), 3, 2, None, None));
    let deliveries: Vec<(String, String)> = {
        let mut statement = db.prepare(
            "SELECT json_extract(payload_json,'$.kind'),dedupe_key
             FROM alert_deliveries ORDER BY created_at_us",
        )?;
        statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<std::result::Result<_, _>>()?
    };
    assert_eq!(deliveries.len(), 2);
    assert_eq!(deliveries[0].0, "new_issue");
    assert_eq!(deliveries[1].0, "regression");
    assert!(deliveries[1].1.ends_with(":3"));

    db.execute("UPDATE issues SET status='ignored',revision=revision+1", [])?;
    accept(
        &mut db,
        "late",
        vec![error(4, 100, Some("release-1"), "earliest")],
        &Limits::default(),
    )?;
    let late = indexer::prepare(&db, &Limits::default())?.expect("late");
    indexer::finalize(&mut db, "active", late)?;
    accept(
        &mut db,
        "latest-null-release",
        vec![error(5, 500, None, "latest")],
        &Limits::default(),
    )?;
    let latest = indexer::prepare(&db, &Limits::default())?.expect("latest");
    indexer::finalize(&mut db, "active", latest)?;

    let lifecycle: (
        String,
        i64,
        i64,
        i64,
        i64,
        i64,
        String,
        Option<String>,
        String,
        i64,
    ) = db.query_row(
        "SELECT status,occurrence_count,first_seen_us,first_seen_ingest_seq,
                last_seen_us,last_seen_ingest_seq,first_release,last_release,title,revision
         FROM issues",
        [],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get(8)?,
                row.get(9)?,
            ))
        },
    )?;
    assert_eq!(
        lifecycle,
        (
            "ignored".into(),
            5,
            100,
            4,
            500,
            5,
            "release-1".into(),
            None,
            "latest".into(),
            3,
        )
    );
    assert_eq!(
        db.query_row("SELECT count(*) FROM alert_deliveries", [], |row| {
            row.get::<_, i64>(0)
        })?,
        2,
        "ignored occurrences do not create regressions"
    );
    Ok(())
}

#[test]
fn changed_inbox_and_alert_overflow_roll_back_the_whole_finalize() -> Result<()> {
    let (_directory, mut db) = database()?;
    accept(
        &mut db,
        "rollback",
        vec![error(1, 100, None, "rollback")],
        &Limits::default(),
    )?;
    let batch = indexer::prepare(&db, &Limits::default())?.expect("batch");
    db.execute("UPDATE inbox SET record_count=2", [])?;
    assert!(
        indexer::finalize(&mut db, "active", clone_batch(&batch)).is_err(),
        "finalize must recheck the stored chunk"
    );
    db.execute("UPDATE inbox SET record_count=1", [])?;

    let tx = db.transaction()?;
    {
        let mut insert = tx.prepare(
            r#"INSERT INTO alerts(
                 id,project_id,name,condition_type,condition_json,destination_type,
                 destination_json,enabled,created_at_us,updated_at_us)
             VALUES(?1,1,?2,'new_issue','{}','webhook',
                 '{"type":"webhook","url":"https://hooks.example.test"}',1,1,1)"#,
        )?;
        for id in 1..=1_001i64 {
            insert.execute(params![id, format!("alert-{id}")])?;
        }
    }
    tx.commit()?;
    assert!(
        indexer::finalize(&mut db, "active", clone_batch(&batch)).is_err(),
        "unbounded event alert fanout must fail closed"
    );
    let unchanged: (i64, i64, i64, i64) = db.query_row(
        "SELECT
           (SELECT count(*) FROM issues),
           (SELECT count(*) FROM issue_occurrences),
           (SELECT count(*) FROM alert_deliveries),
           (SELECT record_count FROM shards WHERE id='active')",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    assert_eq!(unchanged, (0, 0, 0, 0));
    assert_eq!(indexer::applied(&db)?, Boundary::default());
    assert_eq!(
        db.query_row("SELECT count(*) FROM inbox", [], |row| row.get::<_, i64>(0))?,
        1
    );

    db.execute("UPDATE alerts SET enabled=0", [])?;
    assert_eq!(
        indexer::finalize(&mut db, "active", batch)?,
        Boundary {
            inbox_id: 1,
            ingest_seq: 1
        }
    );
    Ok(())
}
