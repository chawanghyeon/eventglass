use eventglass::{
    alerts::{Condition, Configuration, Destination, TimeBasis},
    app::AppState,
    config::Config,
    db::alerts,
    db::ingest::{self, IngestProject},
    sentry::{self, ProjectContext},
};

fn database() -> anyhow::Result<(tempfile::TempDir, rusqlite::Connection)> {
    let directory = tempfile::tempdir()?;
    let db = eventglass::db::open(&directory.path().join("meta.db"))?;
    db.execute_batch(
        "INSERT INTO users(id,email,password_hash,role,is_active,created_at_us,updated_at_us)
         VALUES(1,'admin@example.test','x','admin',1,0,0),
               (2,'member@example.test','x','member',1,0,0);
         INSERT INTO projects(id,slug,name,is_active,created_at_us,updated_at_us)
         VALUES(1,'one','One',1,0,0)",
    )?;
    Ok((directory, db))
}

fn configuration(name: &str) -> Configuration {
    Configuration {
        name: name.into(),
        project_id: Some(1),
        condition: Condition::ErrorCount {
            query: "service:api".into(),
            window_seconds: 300,
            threshold: 10,
            cooldown_seconds: 600,
            time_basis: TimeBasis::ReceivedAt,
        },
        destination: Destination::Webhook {
            url: "https://hooks.example.test/eventglass".into(),
        },
        enabled: true,
    }
}

#[test]
fn alert_crud_is_admin_only_revisioned_and_soft_delete_cancels_pending() -> anyhow::Result<()> {
    let (_directory, mut db) = database()?;
    assert!(alerts::create(&mut db, 2, &configuration("Denied"), 1).is_err());
    let id = alerts::create(&mut db, 1, &configuration("Latency"), 2)?;
    let listed = alerts::list(&db, 1)?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].condition, configuration("Latency").condition);

    let mut changed = configuration("Latency production");
    changed.enabled = false;
    let updated = alerts::update(&mut db, 1, id, 0, &changed, 3)?;
    assert_eq!(updated.revision, 1);
    assert!(!updated.enabled);
    assert!(alerts::update(&mut db, 1, id, 0, &changed, 4).is_err());

    db.execute(
        "INSERT INTO alert_deliveries(id,alert_id,dedupe_key,payload_json,state,
             attempts,next_retry_at_us,created_at_us)
         VALUES(?1,?2,'dedupe','{}','pending',0,0,0)",
        ["a".repeat(64), id.to_string()],
    )?;
    alerts::delete(&mut db, 1, id, 1, 5)?;
    assert!(alerts::list(&db, 1)?.is_empty());
    assert_eq!(
        db.query_row(
            "SELECT state FROM alert_deliveries WHERE alert_id=?1",
            [id],
            |row| row.get::<_, String>(0),
        )?,
        "cancelled"
    );
    Ok(())
}

#[test]
fn failed_delivery_manual_retry_reuses_the_same_identity() -> anyhow::Result<()> {
    let (_directory, mut db) = database()?;
    let id = alerts::create(&mut db, 1, &configuration("Retry"), 1)?;
    let delivery = "b".repeat(64);
    db.execute(
        "INSERT INTO alert_deliveries(id,alert_id,dedupe_key,payload_json,state,
             attempts,next_retry_at_us,created_at_us,last_error)
         VALUES(?1,?2,'retry-dedupe','{}','failed',12,1,1,'timeout')",
        rusqlite::params![delivery, id],
    )?;
    alerts::retry(&mut db, 1, &delivery, 99)?;
    let row = alerts::deliveries(&db, 1, 10)?.pop().unwrap();
    assert_eq!(row.id, delivery);
    assert_eq!(row.state, "pending");
    assert_eq!(row.attempts, 0);
    assert_eq!(row.next_retry_at_us, "99");
    Ok(())
}

#[test]
fn threshold_revision_and_cooldown_are_rechecked_in_the_completion_transaction()
-> anyhow::Result<()> {
    let (_directory, mut db) = database()?;
    db.execute(
        "INSERT INTO runtime_state(singleton,installation_id,storage_generation,next_ingest_seq)
         VALUES(1,'installation','generation',1)",
        [],
    )?;
    let id = alerts::create(&mut db, 1, &configuration("Cooldown"), 0)?;
    let first = alerts::reserve_evaluation(&mut db, 60_000_000)?.unwrap();
    assert_eq!(first.alert_id, id);
    assert!(alerts::finish_evaluation(&mut db, &first, 10, 60_000_001)?);
    let second = alerts::reserve_evaluation(&mut db, 120_000_000)?.unwrap();
    assert!(alerts::finish_evaluation(
        &mut db,
        &second,
        10,
        120_000_001
    )?);
    assert_eq!(
        db.query_row("SELECT count(*) FROM alert_deliveries", [], |row| {
            row.get::<_, i64>(0)
        })?,
        1,
        "the second matching window is inside the configured cooldown"
    );

    let pending = alerts::reserve_evaluation(&mut db, 180_000_000)?.unwrap();
    let mut changed = configuration("Changed");
    changed.enabled = true;
    alerts::update(&mut db, 1, id, 0, &changed, 180_000_001)?;
    assert!(!alerts::finish_evaluation(
        &mut db,
        &pending,
        10,
        180_000_002
    )?);
    Ok(())
}

#[tokio::test]
async fn threshold_evaluation_waits_for_cut_uses_native_count_and_advances_once()
-> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let app = AppState::open(Config {
        addr: "127.0.0.1:0".parse()?,
        data_dir: directory.path().into(),
        base_url: "http://127.0.0.1:8080".parse()?,
        s3_url: None,
        s3_endpoint: None,
        s3_initialize: false,
    })
    .await?;
    app.db
        .call(|db| {
            db.execute_batch(
                "INSERT INTO users(id,email,password_hash,role,is_active,created_at_us,updated_at_us)
                 VALUES(1,'admin@example.test','x','admin',1,0,0);
                 INSERT INTO projects(id,slug,name,is_active,created_at_us,updated_at_us)
                 VALUES(1,'one','One',1,0,0);
                 INSERT INTO project_keys(id,project_id,public_key,created_at_us)
                 VALUES(1,1,'public',0)",
            )?;
            Ok(())
        })
        .await?;
    let minute = eventglass::model::now_us()? / 60_000_000 * 60_000_000;
    let received_at = minute - 1;
    let records = sentry::normalize_store(
        &serde_json::to_vec(&serde_json::json!({
            "event_id":"11111111111111111111111111111111",
            "timestamp":"2020-01-01T00:00:00Z",
            "message":"threshold match"
        }))?,
        &ProjectContext {
            project_id: 1,
            slug: "one".into(),
            public_key: "public".into(),
            scrub_keys: Vec::new(),
        },
        uuid::Uuid::new_v4(),
        received_at,
        &Default::default(),
    )?
    .records;
    app.db
        .call(move |db| {
            ingest::accept(
                db,
                IngestProject {
                    id: 1,
                    slug: "one".into(),
                    public_key: "public".into(),
                },
                "threshold-record",
                records,
                &Default::default(),
            )
        })
        .await?;
    let configuration = Configuration {
        name: "Errors".into(),
        project_id: Some(1),
        condition: Condition::ErrorCount {
            query: "\"threshold match\"".into(),
            window_seconds: 60,
            threshold: 1,
            cooldown_seconds: 0,
            time_basis: TimeBasis::ReceivedAt,
        },
        destination: Destination::Webhook {
            url: "https://hooks.example.test/eventglass".into(),
        },
        enabled: true,
    };
    app.db
        .call(move |db| alerts::create(db, 1, &configuration, minute - 60_000_000))
        .await?;
    let indexer = eventglass::indexer::Indexer::start(app.db.clone(), directory.path()).await?;
    indexer.wake();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if indexer.snapshot().unwrap().boundary.ingest_seq == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert!(eventglass::alerts::evaluate_once(&app.db, &indexer, &app.query_permit, None).await?);
    assert!(!eventglass::alerts::evaluate_once(&app.db, &indexer, &app.query_permit, None).await?);
    let state = app
        .db
        .call(|db| {
            Ok((
                db.query_row("SELECT count(*) FROM alert_deliveries", [], |row| {
                    row.get::<_, i64>(0)
                })?,
                db.query_row("SELECT last_evaluated_at_us FROM alerts", [], |row| {
                    row.get::<_, i64>(0)
                })?,
                db.query_row("SELECT payload_json FROM alert_deliveries", [], |row| {
                    row.get::<_, String>(0)
                })?,
            ))
        })
        .await?;
    assert_eq!(state.0, 1);
    assert_eq!(state.1, minute);
    let payload: serde_json::Value = serde_json::from_str(&state.2)?;
    assert_eq!(payload["count"], "1");
    assert_eq!(payload["cut_ingest_seq"], "1");
    indexer.shutdown().await?;
    Ok(())
}
