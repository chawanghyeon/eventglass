#![cfg(feature = "s3")]

use std::{env, path::Path, time::Duration};

use anyhow::{Context, Result, ensure};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use eventglass::{
    app::AppState,
    config::Config,
    db::{
        self,
        ingest::{self, IngestProject},
        issues::IssueStatus,
    },
    model::{Boundary, RecordKind},
    search::active::ActiveShard,
    sentry::{self, ProjectContext},
    storage::{
        archive,
        checkpoint::{PinnedSnapshot, SnapshotLimits},
        remote::{
            CheckpointDocument, InstallationDocument, LatestDocument, LocalCheckpoint,
            ObjectReference, RestoreLimits, ShardReference, ensure_installation, install_prepared,
            list_all, prepare_restore, publish, sha256,
        },
        s3::{AwsObjectStore, ObjectStore, S3Location},
    },
};
use serde_json::{Value, json};
use tower::ServiceExt;
use url::Url;

const TEST_PASSWORD: &str = "restore-password-123"; // pragma: allowlist secret -- test only

#[tokio::test]
#[ignore = "explicit compatible S3 gate: requires an isolated localhost MinIO server"]
async fn minio_checkpoint_and_object_contract() -> Result<()> {
    let endpoint = Url::parse(&required_env("EVENTGLASS_S3_TEST_ENDPOINT")?)?;
    ensure!(
        endpoint.scheme() == "http" && endpoint.host_str() == Some("127.0.0.1"),
        "compatible S3 test endpoint must be loopback HTTP"
    );
    let bucket = required_env("EVENTGLASS_S3_TEST_BUCKET")?;
    let prefix = required_env("EVENTGLASS_S3_TEST_PREFIX")?;
    let store = AwsObjectStore::load(
        S3Location::parse(&format!("s3://{bucket}/{prefix}"))?,
        Some(&endpoint),
    )
    .await?;

    let root = tempfile::tempdir()?;
    let fixture = restorable_candidate(root.path()).await?;
    let candidate = &fixture.candidate;
    let installation_id = candidate.document.cut.installation_id.clone();
    let installation = InstallationDocument {
        format_version: 1,
        installation_id: installation_id.clone(),
        created_at_us: 1,
    };
    assert!(
        ensure_installation(&store, &installation, false)
            .await
            .is_err()
    );
    assert_eq!(
        ensure_installation(&store, &installation, true).await?,
        installation
    );
    assert!(
        prepare_restore(
            &store,
            &installation_id,
            &tempfile::tempdir()?.path().join("no-checkpoint"),
            RestoreLimits::default(),
            || false,
        )
        .await
        .is_err(),
        "an existing installation without a checkpoint must not become a fresh restore"
    );
    assert!(
        store
            .put_if_absent("installation.json", serde_json::to_vec(&installation)?)
            .await
            .is_err(),
        "conditional create must reject an existing key"
    );

    for ordinal in 0..1_005u32 {
        store
            .put_bytes(
                &format!("pagination/{ordinal:04}.json"),
                format!("{{\"ordinal\":{ordinal}}}").into_bytes(),
            )
            .await?;
    }
    let objects = list_all(&store).await?;
    ensure!(objects.len() == 1_006, "S3 pagination lost objects");

    store.put_bytes("bounded.bin", vec![7; 32]).await?;
    assert!(store.get_small("bounded.bin", 31).await.is_err());
    ensure!(store.get_small("bounded.bin", 32).await? == vec![7; 32]);

    let multipart_path = root.path().join("multipart.bin");
    let multipart_bytes = (0..17 * 1024 * 1024)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    std::fs::write(&multipart_path, &multipart_bytes)?;
    let multipart_sha = sha256(&multipart_bytes);
    store
        .put_file("multipart.bin", &multipart_path, &multipart_sha)
        .await?;
    let multipart_download = root.path().join("multipart-download.bin");
    let downloaded = store
        .download(
            "multipart.bin",
            &multipart_download,
            multipart_bytes.len() as u64,
        )
        .await?;
    ensure!(downloaded.size == multipart_bytes.len() as u64);
    ensure!(std::fs::read(&multipart_download)? == multipart_bytes);
    assert!(
        store
            .put_file("multipart-aborted.bin", &multipart_path, &"0".repeat(64))
            .await
            .is_err()
    );
    assert!(
        store
            .get_small("multipart-aborted.bin", multipart_bytes.len() as u64)
            .await
            .is_err(),
        "failed multipart upload must not publish an object"
    );

    let latest = publish(&store, candidate).await?;
    ensure!(latest.sequence == candidate.document.sequence);

    let cancelled_destination = root.path().join("cancelled-data").join(".restore");
    assert!(
        prepare_restore(
            &store,
            &installation_id,
            &cancelled_destination,
            RestoreLimits::default(),
            || true,
        )
        .await
        .is_err()
    );
    ensure!(!cancelled_destination.exists());

    let mut newer = candidate.document.clone();
    newer.checkpoint_id = uuid::Uuid::new_v4().to_string();
    newer.sequence = candidate.document.sequence + 1;
    newer.created_at_us = 2;
    newer.snapshot.key = format!("snapshots/{}.db", newer.checkpoint_id);
    store
        .put_bytes(&newer.snapshot.key, b"corrupt snapshot".to_vec())
        .await?;
    let newer_bytes = serde_json::to_vec(&newer)?;
    store
        .put_if_absent(
            &format!("checkpoints/{}.json", newer.checkpoint_id),
            newer_bytes.clone(),
        )
        .await?;
    let mut missing = candidate.document.clone();
    missing.checkpoint_id = uuid::Uuid::new_v4().to_string();
    missing.sequence = candidate.document.sequence + 2;
    missing.created_at_us = 3;
    missing.snapshot.key = format!("snapshots/{}.db", missing.checkpoint_id);
    let missing_bytes = serde_json::to_vec(&missing)?;
    store
        .put_if_absent(
            &format!("checkpoints/{}.json", missing.checkpoint_id),
            missing_bytes.clone(),
        )
        .await?;
    store
        .put_bytes(
            "latest.json",
            serde_json::to_vec(&LatestDocument {
                format_version: 1,
                installation_id: installation_id.clone(),
                checkpoint_id: missing.checkpoint_id.clone(),
                sequence: missing.sequence,
                checkpoint_sha256: sha256(&missing_bytes),
            })?,
        )
        .await?;
    store
        .put_bytes("shards/unreferenced.tar.gz", b"unreferenced".to_vec())
        .await?;

    let data_dir = root.path().join("lost-local-data");
    std::fs::create_dir(&data_dir)?;
    std::fs::write(data_dir.join(".eventglass-s3-test-data"), b"owned by test")?;
    std::fs::write(data_dir.join("destroyed-with-local-disk"), b"local only")?;
    reset_test_data_dir(root.path(), &data_dir)?;
    let destination = data_dir.join(".restore");
    let restored = prepare_restore(
        &store,
        &installation_id,
        &destination,
        RestoreLimits::default(),
        || false,
    )
    .await?;
    ensure!(restored.document.checkpoint_id == candidate.document.checkpoint_id);
    ensure!(restored.document.sequence == candidate.document.sequence);
    ensure!(
        !destination.join("shards").join("unreferenced").exists(),
        "unreferenced archive was mixed into recovery"
    );

    install_prepared(&data_dir, restored)?;
    let database = db::open(&data_dir.join("meta.db"))?;
    let users: i64 = database.query_row("SELECT count(*) FROM users", [], |row| row.get(0))?;
    let sessions: i64 =
        database.query_row("SELECT count(*) FROM sessions", [], |row| row.get(0))?;
    let hmac_keys: i64 = database.query_row(
        "SELECT count(*) FROM settings WHERE key='token_hmac_v1'",
        [],
        |row| row.get(0),
    )?;
    let projects: i64 =
        database.query_row("SELECT count(*) FROM projects", [], |row| row.get(0))?;
    let keys: i64 = database.query_row(
        "SELECT count(*) FROM project_keys WHERE revoked_at_us IS NULL",
        [],
        |row| row.get(0),
    )?;
    let (issues, resolved): (i64, i64) = database.query_row(
        "SELECT count(*),sum(status='resolved') FROM issues",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let pending: i64 = database.query_row("SELECT count(*) FROM inbox", [], |row| row.get(0))?;
    let (
        generation,
        applied_inbox,
        shard_inbox,
        state,
        archive_key,
        archive_sha,
        recovery_checkpoint,
    ): (
        String,
        i64,
        i64,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
    ) = database.query_row(
        "SELECT r.storage_generation,r.last_applied_inbox_id,s.last_applied_inbox_id,
                s.state,s.remote_archive_key,s.archive_sha256,
                s.recovery_checkpoint_id
         FROM runtime_state r JOIN shards s ON s.id=?1 WHERE r.singleton=1",
        [&fixture.sealed_shard_id],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
            ))
        },
    )?;
    ensure!(
        users == 1
            && sessions == 0
            && hmac_keys == 0
            && projects == 1
            && keys == 1
            && issues == 1
            && resolved == 1
            && pending == 1,
        "restored checkpoint metadata differs from its application cut"
    );
    ensure!(generation != fixture.checkpoint_generation);
    ensure!(
        applied_inbox == 2 && shard_inbox == applied_inbox,
        "restored cut boundary mismatch: applied={applied_inbox}, shard={shard_inbox}"
    );
    ensure!(
        state == "remote_verified"
            && archive_key.as_deref() == Some(candidate.document.shards[0].object.key.as_str())
            && archive_sha.as_deref() == Some(candidate.document.shards[0].object.sha256.as_str())
            && recovery_checkpoint.as_deref() == Some(candidate.document.checkpoint_id.as_str()),
        "restore did not bind the local shard to its verified remote archive"
    );
    ensure!(
        data_dir
            .join("shards")
            .join(&candidate.document.shards[0].id)
            .is_dir()
    );
    ensure!(
        database.execute(
            "UPDATE shards SET state='remote_only' WHERE id=?1 AND state='remote_verified'",
            [&fixture.sealed_shard_id],
        )? == 1
    );
    drop(database);
    std::fs::remove_dir_all(data_dir.join("shards").join(&fixture.sealed_shard_id))?;

    let restored_app = AppState::open(Config {
        addr: "127.0.0.1:0".parse()?,
        data_dir: data_dir.clone(),
        base_url: "http://localhost:8080".parse()?,
        s3_url: Some(format!("s3://{bucket}/{prefix}")),
        s3_endpoint: Some(endpoint.clone()),
        s3_initialize: false,
    })
    .await?
    .start_core()
    .await
    .context("start restored core")?;
    wait_for_boundary(&restored_app, 3).await?;
    let restored_router = eventglass::http::router(restored_app.clone());
    let stale = Session {
        cookie: fixture.old_session.clone(),
        csrf: String::new(),
    };
    ensure!(
        restored_router
            .clone()
            .oneshot(request("GET", "/api/auth/me", Value::Null, Some(&stale)))
            .await?
            .status()
            == StatusCode::UNAUTHORIZED,
        "pre-restore session remained valid"
    );
    let session = login(&restored_router).await?;
    let detail = restored_router
        .clone()
        .oneshot(request(
            "GET",
            &format!(
                "/api/issues/{}/events/{}",
                fixture.issue_id, fixture.first_record_id
            ),
            Value::Null,
            Some(&session),
        ))
        .await?;
    ensure!(
        detail.status() == StatusCode::OK,
        "cold occurrence detail failed"
    );
    let detail_body = response_json(detail).await?;
    ensure!(detail_body["raw"]["message"] == "checkpoint-marker database unavailable");

    let search = restored_router
        .clone()
        .oneshot(request(
            "POST",
            "/api/explore/search",
            json!({
                "projects": [fixture.project_id.to_string()],
                "start": "2026-09-07T00:00:00Z",
                "end": "2026-09-09T00:00:00Z",
                "query": "checkpoint-marker",
                "limit": 10
            }),
            Some(&session),
        ))
        .await?;
    ensure!(search.status() == StatusCode::OK, "restored search failed");
    let search_body = response_json(search).await?;
    ensure!(
        search_body["rows"]
            .as_array()
            .is_some_and(|rows| rows.len() == 3)
    );

    let semantics = restored_app
        .db
        .call({
            let issue_id = fixture.issue_id.clone();
            let public_key = fixture.public_key.clone();
            let sealed_shard_id = fixture.sealed_shard_id.clone();
            move |db| {
                Ok((
                    db.query_row(
                        "SELECT status,occurrence_count FROM issues WHERE id=?1",
                        [&issue_id],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
                    )?,
                    db.query_row("SELECT count(*) FROM inbox", [], |row| row.get::<_, i64>(0))?,
                    db.query_row(
                        "SELECT count(*) FROM issue_occurrences WHERE issue_id=?1",
                        [&issue_id],
                        |row| row.get::<_, i64>(0),
                    )?,
                    db.query_row(
                        "SELECT count(*) FROM project_keys WHERE project_id=?1 AND public_key=?2
                         AND revoked_at_us IS NULL",
                        rusqlite::params![fixture.project_id, public_key],
                        |row| row.get::<_, i64>(0),
                    )?,
                    db.query_row(
                        "SELECT state FROM shards WHERE id=?1",
                        [&sealed_shard_id],
                        |row| row.get::<_, String>(0),
                    )?,
                ))
            }
        })
        .await?;
    ensure!(semantics.0 == ("unresolved".to_owned(), 2));
    ensure!(semantics.1 == 0 && semantics.2 == 2 && semantics.3 == 1);
    ensure!(semantics.4 == "remote_verified");
    restored_app
        .indexer
        .as_ref()
        .context("missing restored Indexer")?
        .shutdown()
        .await?;

    store.put_bytes("latest.json", b"not json".to_vec()).await?;
    let fallback = root.path().join("fallback-data").join(".restore");
    let restored = prepare_restore(
        &store,
        &installation_id,
        &fallback,
        RestoreLimits::default(),
        || false,
    )
    .await?;
    ensure!(
        restored.document.sequence == candidate.document.sequence,
        "corrupt latest did not fall back"
    );

    let wrong = AwsObjectStore::load(
        S3Location::parse(&format!("s3://{bucket}/{prefix}-wrong"))?,
        Some(&endpoint),
    )
    .await?;
    wrong
        .put_bytes("foreign-object", b"occupied".to_vec())
        .await?;
    assert!(
        ensure_installation(&wrong, &installation, false)
            .await
            .is_err()
    );

    println!(
        "{}",
        json!({
            "result": "pass",
            "server": "MinIO RELEASE.2025-09-07T16-13-09Z",
            "listed_objects_before_checkpoint": objects.len(),
            "multipart_bytes": multipart_bytes.len(),
            "checkpoint_sequence": candidate.document.sequence,
            "fallback_from_missing_sequence": missing.sequence,
            "fallback_past_corrupt_sequence": newer.sequence,
            "full_loss_restore": true,
            "sessions_invalidated": true,
            "projects_and_keys_restored": true,
            "issue_state_and_pending_inbox_reconciled": true,
            "search_and_cold_detail_verified": true,
            "unreferenced_archive_ignored": true,
        })
    );
    Ok(())
}

fn required_env(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("{name} is required"))
}

fn reset_test_data_dir(root: &Path, data_dir: &Path) -> Result<()> {
    ensure!(
        data_dir.parent() == Some(root),
        "test data directory escaped its root"
    );
    ensure!(
        data_dir.join(".eventglass-s3-test-data").is_file(),
        "refusing to remove an unmarked data directory"
    );
    std::fs::remove_dir_all(data_dir)?;
    std::fs::create_dir(data_dir)?;
    Ok(())
}

struct RestoreFixture {
    candidate: LocalCheckpoint,
    old_session: String,
    project_id: i64,
    public_key: String,
    issue_id: String,
    first_record_id: String,
    sealed_shard_id: String,
    checkpoint_generation: String,
}

#[derive(Clone)]
struct Session {
    cookie: String,
    csrf: String,
}

fn request(method: &str, path: &str, input: Value, session: Option<&Session>) -> Request<Body> {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header(header::ORIGIN, "http://localhost:8080")
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(session) = session {
        request = request
            .header(header::COOKIE, &session.cookie)
            .header("x-csrf-token", &session.csrf);
    }
    request.body(Body::from(input.to_string())).unwrap()
}

async fn response_json(response: axum::response::Response) -> Result<Value> {
    Ok(serde_json::from_slice(
        &to_bytes(response.into_body(), 1024 * 1024).await?,
    )?)
}

async fn login(router: &axum::Router) -> Result<Session> {
    let response = router
        .clone()
        .oneshot(request(
            "POST",
            "/api/auth/login",
            json!({"email":"restore@example.test","password":TEST_PASSWORD}),
            None,
        ))
        .await?;
    ensure!(response.status() == StatusCode::OK, "fixture login failed");
    let cookie = response.headers()[header::SET_COOKIE]
        .to_str()?
        .split(';')
        .next()
        .context("missing session cookie")?
        .to_owned();
    let body = response_json(response).await?;
    Ok(Session {
        cookie,
        csrf: body["csrf_token"]
            .as_str()
            .context("missing CSRF token")?
            .to_owned(),
    })
}

fn normalized_error(
    event_id: &str,
    received_at_us: i64,
    public_key: &str,
) -> Result<eventglass::model::Record> {
    Ok(sentry::normalize_store(
        &serde_json::to_vec(&json!({
            "event_id": event_id,
            "timestamp": "2026-09-08T00:00:00.000001Z",
            "message": "checkpoint-marker database unavailable",
            "environment": "production",
            "release": "restore-contract@1.0.0"
        }))?,
        &ProjectContext {
            project_id: 1,
            slug: "restore-contract".into(),
            public_key: public_key.into(),
            scrub_keys: vec![],
        },
        uuid::Uuid::new_v4(),
        received_at_us,
        &Default::default(),
    )?
    .records
    .remove(0))
}

async fn wait_for_boundary(app: &AppState, expected: i64) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if app
                .indexer
                .as_ref()
                .context("missing Indexer")?
                .snapshot()?
                .boundary
                .ingest_seq
                == expected
            {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    Ok(())
}

async fn restorable_candidate(root: &Path) -> Result<RestoreFixture> {
    let data_dir = root.join("application-source");
    let app = AppState::open(Config {
        addr: "127.0.0.1:0".parse()?,
        data_dir: data_dir.clone(),
        base_url: "http://localhost:8080".parse()?,
        s3_url: None,
        s3_endpoint: None,
        s3_initialize: false,
    })
    .await?;
    let router = eventglass::http::router(app.clone());
    let setup_token = eventglass::http::issue_setup_token(&app).await?;
    let setup = router
        .clone()
        .oneshot(request(
            "POST",
            "/api/setup",
            json!({
                "token": setup_token,
                "email": "restore@example.test",
                "password": TEST_PASSWORD
            }),
            None,
        ))
        .await?;
    ensure!(
        setup.status() == StatusCode::CREATED,
        "fixture setup failed"
    );
    let session = login(&router).await?;

    let project = router
        .clone()
        .oneshot(request(
            "POST",
            "/api/projects",
            json!({"slug":"restore-contract","name":"Restore contract"}),
            Some(&session),
        ))
        .await?;
    ensure!(
        project.status() == StatusCode::CREATED,
        "project creation failed"
    );
    let project_id = response_json(project).await?["id"]
        .as_str()
        .context("missing project ID")?
        .parse()?;
    let key = router
        .clone()
        .oneshot(request(
            "POST",
            &format!("/api/projects/{project_id}/keys"),
            Value::Null,
            Some(&session),
        ))
        .await?;
    ensure!(key.status() == StatusCode::CREATED, "key creation failed");
    let public_key = response_json(key).await?["public_key"]
        .as_str()
        .context("missing project key")?
        .to_owned();

    let installation_id = app
        .db
        .call(|db| {
            db.query_row(
                "SELECT installation_id FROM runtime_state WHERE singleton=1",
                [],
                |row| row.get::<_, String>(0),
            )
            .map_err(Into::into)
        })
        .await?;
    let app = app.start_core().await?;
    let received_at_us = eventglass::model::now_us()?;
    let first = normalized_error(
        "11111111111111111111111111111111",
        received_at_us,
        &public_key,
    )?;
    let first_record_id = first.record_id.clone();
    let first_public_key = public_key.clone();
    app.db
        .call(move |db| {
            ingest::accept(
                db,
                IngestProject {
                    id: 1,
                    slug: "restore-contract".into(),
                    public_key: first_public_key,
                },
                "restore-checkpoint-first",
                vec![first],
                &Default::default(),
            )
        })
        .await?;
    app.indexer.as_ref().context("missing Indexer")?.wake();
    wait_for_boundary(&app, 1).await?;

    let issue_id = app
        .db
        .call(|db| {
            db.query_row("SELECT id FROM issues", [], |row| row.get::<_, String>(0))
                .map_err(Into::into)
        })
        .await?;
    let status_issue = issue_id.clone();
    app.db
        .call(move |db| {
            eventglass::db::issues::set_status(db, 1, &status_issue, IssueStatus::Resolved, 0)
        })
        .await?;

    let mut log = normalized_error(
        "22222222222222222222222222222222",
        received_at_us + 1,
        &public_key,
    )?;
    log.kind = RecordKind::Log;
    log.source_event_id = None;
    log.issue_id = None;
    log.fingerprint = None;
    log.fingerprint_version = None;
    log.record_id = "b".repeat(64);
    let log_public_key = public_key.clone();
    app.db
        .call(move |db| {
            ingest::accept(
                db,
                IngestProject {
                    id: 1,
                    slug: "restore-contract".into(),
                    public_key: log_public_key,
                },
                "restore-checkpoint-log",
                vec![log],
                &Default::default(),
            )
        })
        .await?;
    app.indexer.as_ref().context("missing Indexer")?.wake();
    wait_for_boundary(&app, 2).await?;
    app.indexer
        .as_ref()
        .context("missing Indexer")?
        .shutdown()
        .await?;

    let active_shard_id = app
        .db
        .call(|db| {
            db.query_row("SELECT id FROM shards WHERE state='active'", [], |row| {
                row.get::<_, String>(0)
            })
            .map_err(Into::into)
        })
        .await?;
    let old_received_at = received_at_us - 2 * 60 * 60 * 1_000_000;
    let rotation_id = active_shard_id.clone();
    let stats = app
        .db
        .call(move |db| {
            ensure!(
                db.execute(
                    "UPDATE shards SET first_record_received_at_us=?1 WHERE id=?2 AND state='active'",
                    rusqlite::params![old_received_at, rotation_id],
                )? == 1,
                "fixture active shard missing"
            );
            eventglass::db::shards::rotation_plan(
                db,
                &rotation_id,
                None,
                eventglass::model::now_us()?,
            )?
            .context("fixture shard did not become sealable")
        })
        .await?;
    let shard_path = data_dir.join("shards").join(&active_shard_id);
    let mut active = ActiveShard::open(&shard_path, &installation_id, &active_shard_id)?;
    active.publish(Boundary {
        inbox_id: 2,
        ingest_seq: 2,
    })?;
    let manifest = active.seal(&shard_path, stats, eventglass::model::now_us()?)?;
    let sealed_size = eventglass::storage::manifest::local_size(&shard_path, &manifest)?;
    app.db
        .call(move |db| eventglass::db::shards::finalize_seal(db, &manifest, sealed_size))
        .await?;
    let sealed_shard_id = active_shard_id;

    let pending = normalized_error(
        "33333333333333333333333333333333",
        received_at_us + 2,
        &public_key,
    )?;
    let pending_public_key = public_key.clone();
    app.db
        .call(move |db| {
            ingest::accept(
                db,
                IngestProject {
                    id: 1,
                    slug: "restore-contract".into(),
                    public_key: pending_public_key,
                },
                "restore-pending-inbox",
                vec![pending],
                &Default::default(),
            )
        })
        .await?;

    let database_path = data_dir.join("meta.db");
    let checkpoint_id = uuid::Uuid::new_v4().to_string();
    let snapshot_path = root.join("snapshot.db");
    let snapshot = PinnedSnapshot::open(&database_path)?.backup_to(
        &snapshot_path,
        SnapshotLimits::default(),
        || false,
    )?;
    ensure!(snapshot.cut.boundary.ingest_seq == 2);
    let pending_count: i64 =
        db::inspect(&snapshot_path)?
            .query_row("SELECT count(*) FROM inbox", [], |row| row.get(0))?;
    ensure!(
        pending_count == 1,
        "checkpoint did not retain pending Inbox B"
    );

    let shard_source = data_dir.join("shards").join(&sealed_shard_id);
    let archive_path = root.join("shard.tar.gz");
    let archived = archive::create(
        &shard_source,
        &archive_path,
        &installation_id,
        &sealed_shard_id,
        || false,
    )?;
    let checkpoint_generation = snapshot.cut.storage_generation.clone();
    let candidate = LocalCheckpoint {
        document: CheckpointDocument {
            format_version: 1,
            checkpoint_id: checkpoint_id.clone(),
            sequence: u64::try_from(snapshot.cut.boundary.inbox_id)?,
            created_at_us: eventglass::model::now_us()?,
            cut: snapshot.cut,
            snapshot: ObjectReference {
                key: format!("snapshots/{checkpoint_id}.db"),
                size: snapshot.size,
                sha256: snapshot.sha256,
            },
            shards: vec![ShardReference {
                id: sealed_shard_id.clone(),
                object: ObjectReference {
                    key: format!("shards/{sealed_shard_id}.tar.gz"),
                    size: archived.size,
                    sha256: archived.sha256,
                },
            }],
        },
        snapshot_path,
        shard_archives: vec![(sealed_shard_id.clone(), archive_path)],
    };
    Ok(RestoreFixture {
        candidate,
        old_session: session.cookie,
        project_id,
        public_key,
        issue_id,
        first_record_id,
        sealed_shard_id,
        checkpoint_generation,
    })
}
