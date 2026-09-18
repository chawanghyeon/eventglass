use async_trait::async_trait;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use eventglass::{
    app::AppState,
    config::Config,
    storage::{
        cold::ColdStorage,
        s3::{ObjectMetadata, ObjectPage, ObjectStore},
    },
};
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;

struct UnusedStore;

#[async_trait]
impl ObjectStore for UnusedStore {
    async fn list(&self, _continuation: Option<String>) -> anyhow::Result<ObjectPage> {
        unreachable!()
    }

    async fn get_small(&self, _relative: &str, _max_bytes: u64) -> anyhow::Result<Vec<u8>> {
        unreachable!()
    }

    async fn put_if_absent(&self, _relative: &str, _bytes: Vec<u8>) -> anyhow::Result<()> {
        unreachable!()
    }

    async fn put_bytes(&self, _relative: &str, _bytes: Vec<u8>) -> anyhow::Result<()> {
        unreachable!()
    }

    async fn put_file(
        &self,
        _relative: &str,
        _path: &std::path::Path,
        _sha: &str,
    ) -> anyhow::Result<()> {
        unreachable!()
    }

    async fn download(
        &self,
        _relative: &str,
        _destination: &std::path::Path,
        _max_bytes: u64,
    ) -> anyhow::Result<ObjectMetadata> {
        unreachable!()
    }
}

async fn app() -> anyhow::Result<(tempfile::TempDir, AppState, axum::Router)> {
    let dir = tempfile::tempdir()?;
    let state = AppState::open(Config {
        addr: "127.0.0.1:0".parse()?,
        data_dir: dir.path().to_owned(),
        base_url: "http://localhost:8080".parse()?,
        s3_url: None,
        s3_endpoint: None,
        s3_initialize: false,
    })
    .await?;
    state.db.call(|db| {
        db.execute_batch("INSERT INTO projects(id,slug,name,created_at_us,updated_at_us) VALUES(1,'test','Test',0,0);
          INSERT INTO project_keys(id,project_id,public_key,created_at_us) VALUES(1,1,'public-test-key',0)")?;
        Ok(())
    }).await?;
    let router = eventglass::http::router(state.clone());
    Ok((dir, state, router))
}

fn envelope(key: &str) -> Vec<u8> {
    let event = json!({"event_id":"0123456789abcdef0123456789abcdef", "message":"durable error",
        "timestamp":1788860000.0,"request":{"headers":{"Authorization":"sentinel-credential"}}});
    format!(
        "{}\n{}\n{}\n",
        json!({"dsn":format!("http://{key}@localhost/1")}),
        json!({"type":"event","length":event.to_string().len()}),
        event
    )
    .into_bytes()
}

#[tokio::test]
async fn dsn_only_ack_is_durable_sanitized_and_cors_is_scoped() -> anyhow::Result<()> {
    let (_dir, state, router) = app().await?;
    let response = router
        .clone()
        .oneshot(
            Request::post("/api/1/envelope/")
                .header("origin", "https://sdk.example.test")
                .body(Body::from(envelope("public-test-key")))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(response.headers()["access-control-allow-origin"], "*");
    assert!(
        response
            .headers()
            .get("access-control-allow-credentials")
            .is_none()
    );
    let receipt: Value = serde_json::from_slice(&to_bytes(response.into_body(), 8192).await?)?;
    assert_eq!(receipt["accepted"], 1);
    assert_eq!(receipt["first_ingest_seq"], "1");
    let path = state.config.data_dir.join("meta.db");
    let persisted = tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<u8>> {
        let db = eventglass::db::open_reader(&path)?;
        Ok(db.query_row("SELECT payload FROM inbox", [], |r| r.get(0))?)
    })
    .await??;
    assert!(!String::from_utf8_lossy(&persisted).contains("sentinel-credential"));
    let preflight = router
        .clone()
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/api/1/envelope/")
                .header("origin", "https://sdk.example.test")
                .header("access-control-request-method", "POST")
                .header(
                    "access-control-request-headers",
                    "x-sentry-auth,content-type",
                )
                .body(Body::empty())?,
        )
        .await?;
    assert!(preflight.status().is_success());
    assert_eq!(preflight.headers()["access-control-allow-origin"], "*");
    let admin = router
        .oneshot(
            Request::get("/api/projects")
                .header("origin", "https://sdk.example.test")
                .body(Body::empty())?,
        )
        .await?;
    assert!(admin.headers().get("access-control-allow-origin").is_none());
    Ok(())
}

#[tokio::test]
async fn conflict_revoke_pressure_and_malformed_never_ack() -> anyhow::Result<()> {
    let (_dir, state, router) = app().await?;
    let conflict = router
        .clone()
        .oneshot(
            Request::post("/api/1/envelope/?sentry_key=other")
                .body(Body::from(envelope("public-test-key")))?,
        )
        .await?;
    assert_eq!(conflict.status(), StatusCode::UNAUTHORIZED);
    let permit = state.ingress_permit.clone().acquire_owned().await?;
    let busy = router
        .clone()
        .oneshot(Request::post("/api/1/envelope/").body(Body::from(envelope("public-test-key")))?)
        .await?;
    assert_eq!(busy.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(busy.headers().contains_key("retry-after"));
    drop(permit);
    let malformed = router
        .clone()
        .oneshot(
            Request::post("/api/1/envelope/?sentry_key=public-test-key")
                .body(Body::from("{}\n{\"type\":\"event\",\"length\":10}\n{}"))?,
        )
        .await?;
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
    state
        .db
        .call(|db| {
            db.execute("UPDATE project_keys SET revoked_at_us=1", [])?;
            Ok(())
        })
        .await?;
    let revoked = router
        .oneshot(Request::post("/api/1/envelope/").body(Body::from(envelope("public-test-key")))?)
        .await?;
    assert_eq!(revoked.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        state
            .db
            .call(|db| Ok(db.query_row("SELECT count(*) FROM inbox", [], |r| r.get::<_, i64>(0))?))
            .await?,
        0
    );
    Ok(())
}

#[tokio::test]
async fn sdk_transaction_ack_reaches_index_without_creating_an_issue() -> anyhow::Result<()> {
    let (_dir, state, router) = app().await?;
    let response = router
        .oneshot(
            Request::post("/api/1/envelope/?sentry_key=public-test-key").body(Body::from(
                include_bytes!("fixtures/sentry/rust-http-transaction/transaction.envelope")
                    .as_slice(),
            ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let receipt: Value = serde_json::from_slice(&to_bytes(response.into_body(), 8192).await?)?;
    assert_eq!(receipt["accepted"], 1);
    let state = state.start_core().await?;
    let indexer = state.indexer.as_ref().unwrap();
    let running_router = eventglass::http::router(state.clone());
    let second = running_router
        .oneshot(
            Request::post("/api/1/envelope/?sentry_key=public-test-key").body(Body::from(
                include_bytes!("fixtures/sentry/rust-http-transaction/transaction.envelope")
                    .as_slice(),
            ))?,
        )
        .await?;
    assert_eq!(second.status(), StatusCode::ACCEPTED);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while indexer.snapshot()?.boundary.ingest_seq < 2 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    assert_eq!(indexer.snapshot()?.searcher.num_docs(), 2);
    assert_eq!(
        state
            .db
            .call(
                |db| Ok(db.query_row("SELECT count(*) FROM issues", [], |row| row
                    .get::<_, i64>(0))?)
            )
            .await?,
        0
    );
    indexer.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn disk_reclaim_failure_is_reported_before_acceptance() -> anyhow::Result<()> {
    let (directory, mut state, _router) = app().await?;
    let installation = state
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
    state.cold = Some(ColdStorage::new(
        state.db.clone(),
        directory.path(),
        installation.clone(),
        Arc::new(UnusedStore),
        eventglass::storage::registry::Registry::new(directory.path(), installation),
        state.disk_budget.clone(),
    ));
    state
        .db
        .call(|db| {
            db.execute_batch("PRAGMA foreign_keys=OFF; DROP TABLE shards")?;
            Ok(())
        })
        .await?;
    let disk = state.disk_budget.status()?;
    let held = state.disk_budget.reserve(
        disk.free_bytes
            .saturating_sub(disk.minimum_free_bytes)
            .saturating_sub(8 * 1024 * 1024),
    )?;
    assert!(!state.disk_budget.status()?.ingest_accepting);

    let response = eventglass::http::router(state)
        .oneshot(Request::post("/api/1/envelope/").body(Body::from(envelope("public-test-key")))?)
        .await?;
    drop(held);
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body: Value = serde_json::from_slice(&to_bytes(response.into_body(), 8192).await?)?;
    assert_eq!(body["error"]["code"], "disk_reclaim_failed");
    Ok(())
}

#[tokio::test]
async fn store_and_transport_metadata_fail_closed_at_every_public_boundary() -> anyhow::Result<()> {
    let (_dir, _state, router) = app().await?;
    let event = json!({
        "event_id": "fedcba9876543210fedcba9876543210", // pragma: allowlist secret -- event ID fixture
        "message": "store event",
        "timestamp": 1788860000.0
    })
    .to_string();
    let accepted = router
        .clone()
        .oneshot(
            Request::post("/api/1/store/?sentry_key=public-test-key")
                .body(Body::from(event.clone()))?,
        )
        .await?;
    assert_eq!(accepted.status(), StatusCode::ACCEPTED);
    let receipt: Value = serde_json::from_slice(&to_bytes(accepted.into_body(), 8192).await?)?;
    assert_eq!(receipt["id"], "fedcba9876543210fedcba9876543210"); // pragma: allowlist secret -- event ID fixture

    for (request, status) in [
        (
            Request::post("/api/0/store/?sentry_key=public-test-key")
                .body(Body::from(event.clone()))?,
            StatusCode::UNAUTHORIZED,
        ),
        (
            Request::post("/api/1/store/?sentry_key=wrong").body(Body::from(event.clone()))?,
            StatusCode::UNAUTHORIZED,
        ),
        (
            Request::post("/api/1/store/")
                .header("content-encoding", "br")
                .body(Body::from(event.clone()))?,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
        ),
        (
            Request::post("/api/1/store/")
                .header("content-length", "invalid")
                .body(Body::from(event.clone()))?,
            StatusCode::BAD_REQUEST,
        ),
        (
            Request::post("/api/1/store/")
                .header("content-length", (21 * 1024 * 1024).to_string())
                .body(Body::from(event.clone()))?,
            StatusCode::PAYLOAD_TOO_LARGE,
        ),
        (
            Request::post("/api/1/store/?sentry_key=public-test-key").body(Body::from("[]"))?,
            StatusCode::BAD_REQUEST,
        ),
    ] {
        assert_eq!(router.clone().oneshot(request).await?.status(), status);
    }

    let missing_key = router
        .clone()
        .oneshot(Request::post("/api/1/store/").body(Body::from(event))?)
        .await?;
    assert_eq!(missing_key.status(), StatusCode::UNAUTHORIZED);
    let mismatched_project = router
        .oneshot(Request::post("/api/2/envelope/").body(Body::from(envelope("public-test-key")))?)
        .await?;
    assert_eq!(mismatched_project.status(), StatusCode::UNAUTHORIZED);
    Ok(())
}
