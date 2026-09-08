use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use eventglass::{app::AppState, config::Config};
use serde_json::{Value, json};
use tower::ServiceExt;

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
