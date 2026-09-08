use anyhow::{Result, ensure};
use eventglass::{
    app::AppState,
    config::Config,
    db::ingest::{self, IngestProject},
    model::{Boundary, RecordKind},
    search::active::ActiveShard,
    sentry::{self, ProjectContext},
    storage::manifest::ShardStats,
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
        eventglass::model::now_us()?,
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
async fn first_record_age_rolls_over_only_after_publish_and_keeps_the_old_reader() -> Result<()> {
    let (dir, app) = app().await?;
    let app = app.start_core().await?;
    let indexer = app.indexer.as_ref().unwrap();
    let initial = indexer.snapshot()?;
    let received_at = eventglass::model::now_us()? - 2 * 60 * 60 * 1_000_000;
    let mut records = sentry::normalize_store(
        b"{\"event_id\":\"fedcba9876543210fedcba9876543210\",\"message\":\"aged shard\"}",
        &ProjectContext {
            project_id: 1,
            slug: "test".into(),
            public_key: "public".into(),
            scrub_keys: vec![],
        },
        uuid::Uuid::new_v4(),
        received_at,
        &Default::default(),
    )?
    .records;
    let record_id = records[0].record_id.clone();
    app.db
        .call(move |db| {
            ingest::accept(
                db,
                IngestProject {
                    id: 1,
                    slug: "test".into(),
                    public_key: "public".into(),
                },
                "aged-rollover",
                std::mem::take(&mut records),
                &Default::default(),
            )
        })
        .await?;
    indexer.wake();
    let next = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let snapshot = indexer.snapshot()?;
            if snapshot.shard_id != initial.shard_id && snapshot.boundary.ingest_seq == 1 {
                return Ok::<_, anyhow::Error>(snapshot);
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await??;
    assert_eq!(next.searcher.num_docs(), 0);
    let initial_id = initial.shard_id.clone();
    let states = app
        .db
        .call(move |db| {
            Ok((
                db.query_row(
                    "SELECT state FROM shards WHERE id=?1",
                    [&initial_id],
                    |row| row.get::<_, String>(0),
                )?,
                db.query_row(
                    "SELECT count(*) FROM shards WHERE state='active'",
                    [],
                    |row| row.get::<_, i64>(0),
                )?,
            ))
        })
        .await?;
    assert_eq!(states, ("local".into(), 1));
    assert!(
        dir.path()
            .join("shards")
            .join(&initial.shard_id)
            .join(eventglass::storage::manifest::NAME)
            .is_file()
    );
    let pins = indexer.pin_shards(std::slice::from_ref(&initial.shard_id))?;
    assert_eq!(pins[0].published().searcher.num_docs(), 1);
    let detail = eventglass::search::detail::load(pins[0].published(), 1, &record_id, 1)?;
    assert!(detail.is_some());
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

#[tokio::test]
async fn startup_finishes_manifested_seal_before_creating_the_next_active() -> Result<()> {
    let (dir, app) = app().await?;
    let installation = app
        .db
        .call(|db| {
            Ok(db.query_row(
                "SELECT installation_id FROM runtime_state WHERE singleton=1",
                [],
                |row| row.get::<_, String>(0),
            )?)
        })
        .await?;
    let sealed_id = uuid::Uuid::new_v4().to_string();
    let catalog_id = sealed_id.clone();
    app.db
        .call(move |db| {
            eventglass::db::shards::adopt_initial(db, &catalog_id, Boundary::default(), 1)
        })
        .await?;
    let path = dir.path().join("shards").join(&sealed_id);
    std::fs::create_dir_all(&path)?;
    let mut active = ActiveShard::create(&path, &installation, &sealed_id, Boundary::default())?;
    active.publish(Boundary::default())?;
    active.seal(
        &path,
        ShardStats {
            record_count: 0,
            min_timestamp_us: None,
            max_timestamp_us: None,
            min_received_at_us: None,
            max_received_at_us: None,
            min_ingest_seq: None,
            max_ingest_seq: None,
        },
        2,
    )?;

    let app = app.start_core().await?;
    let next = app.indexer.as_ref().unwrap().snapshot()?;
    assert_ne!(next.shard_id, sealed_id);
    assert_eq!(next.boundary, Boundary::default());
    let sealed_id_for_db = sealed_id.clone();
    let states = app
        .db
        .call(move |db| {
            Ok((
                db.query_row(
                    "SELECT state FROM shards WHERE id=?1",
                    [&sealed_id_for_db],
                    |row| row.get::<_, String>(0),
                )?,
                db.query_row(
                    "SELECT count(*) FROM shards WHERE state='active'",
                    [],
                    |row| row.get::<_, i64>(0),
                )?,
            ))
        })
        .await?;
    assert_eq!(states, ("local".into(), 1));
    app.indexer.as_ref().unwrap().shutdown().await?;
    Ok(())
}
