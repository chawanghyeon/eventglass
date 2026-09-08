use anyhow::{Result, ensure};
use eventglass::{
    app::AppState,
    config::Config,
    db::ingest::{self, IngestProject},
    model::{Boundary, RecordKind},
    sentry::{self, ProjectContext},
};

async fn app() -> Result<(tempfile::TempDir, AppState)> {
    let dir = tempfile::tempdir()?;
    let app = AppState::open(Config {
        addr: "127.0.0.1:0".parse()?,
        data_dir: dir.path().to_owned(),
        base_url: "http://localhost:8080".parse()?,
        s3_url: None,
    })
    .await?;
    app.db.call(|db|{
        db.execute_batch("INSERT INTO projects(id,slug,name,created_at_us,updated_at_us) VALUES(1,'test','Test',0,0);
          INSERT INTO project_keys(id,project_id,public_key,created_at_us) VALUES(1,1,'public',0)")?;
        Ok(())
    }).await?;
    Ok((dir, app))
}

#[tokio::test]
async fn accepted_error_replay_is_deduped_and_logs_remain_distinct() -> Result<()> {
    let (_dir, app) = app().await?;
    let context = ProjectContext {
        project_id: 1,
        slug: "test".into(),
        public_key: "public".into(),
        scrub_keys: vec![],
    };
    let error = sentry::normalize_store(
        b"{\"event_id\":\"0123456789abcdef0123456789abcdef\",\"message\":\"coordinator error\"}",
        &context,
        uuid::Uuid::new_v4(),
        1788860000000000,
        &Default::default(),
    )?
    .records
    .remove(0);
    let mut first_log = error.clone();
    first_log.kind = RecordKind::Log;
    first_log.source_event_id = None;
    first_log.record_id = "a".repeat(64);
    first_log.issue_id = None;
    first_log.fingerprint = None;
    first_log.fingerprint_version = None;
    let mut second_log = first_log.clone();
    second_log.record_id = "b".repeat(64);
    app.db
        .call(move |db| {
            ingest::accept(
                db,
                IngestProject {
                    id: 1,
                    slug: "test".into(),
                    public_key: "public".into(),
                },
                "coordinator-acceptance",
                vec![error.clone(), error, first_log, second_log],
                &Default::default(),
            )
        })
        .await?;
    let app = app.start_core().await?;
    let indexer = app.indexer.as_ref().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let snapshot = indexer.snapshot()?;
            if snapshot.boundary.ingest_seq == 4 {
                ensure!(snapshot.searcher.num_docs() == 3, "duplicate Error indexed");
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await??;
    let (a, inbox, occurrences, issues) = app
        .db
        .call(|db| {
            Ok((
                eventglass::db::indexer::applied(db)?,
                db.query_row("SELECT count(*) FROM inbox", [], |r| r.get::<_, i64>(0))?,
                db.query_row("SELECT count(*) FROM issue_occurrences", [], |r| {
                    r.get::<_, i64>(0)
                })?,
                db.query_row("SELECT sum(occurrence_count) FROM issues", [], |r| {
                    r.get::<_, i64>(0)
                })?,
            ))
        })
        .await?;
    assert_eq!(
        a,
        Boundary {
            inbox_id: 1,
            ingest_seq: 4
        }
    );
    assert_eq!((inbox, occurrences, issues), (0, 1, 1));
    indexer.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn cataloged_missing_native_directory_never_reinitializes() -> Result<()> {
    let (_dir, app) = app().await?;
    let id = uuid::Uuid::new_v4().to_string();
    app.db
        .call(move |db| eventglass::db::shards::adopt_initial(db, &id, Boundary::default(), 0))
        .await?;
    assert!(app.start_core().await.is_err());
    Ok(())
}
