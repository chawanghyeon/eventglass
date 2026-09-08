use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use eventglass::{
    app::AppState,
    config::Config,
    db::{
        ingest::{self, IngestProject},
        search as authorization,
    },
    model::RecordKind,
    search::tokens::{Position, TokenContext},
    sentry::{self, ProjectContext},
};
use serde_json::{Value, json};
use tower::ServiceExt;

const SENTINEL: &str = "detail-secret-sentinel";

async fn request(
    router: &Router,
    method: &str,
    path: &str,
    cookie: &str,
    input: Option<Value>,
) -> anyhow::Result<(StatusCode, Value)> {
    let mut builder = Request::builder().method(method).uri(path);
    if !cookie.is_empty() {
        builder = builder.header(header::COOKIE, cookie);
    }
    let body = if let Some(input) = input {
        builder = builder
            .header(header::ORIGIN, "http://localhost:8080")
            .header(header::CONTENT_TYPE, "application/json");
        Body::from(input.to_string())
    } else {
        Body::empty()
    };
    let response = router.clone().oneshot(builder.body(body)?).await?;
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await?;
    Ok((
        status,
        if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes)?
        },
    ))
}

async fn fixture() -> anyhow::Result<(tempfile::TempDir, AppState, Router, String)> {
    let dir = tempfile::tempdir()?;
    let app = AppState::open(Config {
        addr: "127.0.0.1:0".parse()?,
        data_dir: dir.path().into(),
        base_url: "http://localhost:8080".parse()?,
        s3_url: None,
    })
    .await?;
    let router = eventglass::http::router(app.clone());
    let setup = eventglass::http::issue_setup_token(&app).await?;
    let password = "record-detail-password-123"; // pragma: allowlist secret -- disposable test
    assert_eq!(
        request(
            &router,
            "POST",
            "/api/setup",
            "",
            Some(json!({
                "token": setup,
                "email": "detail@example.test",
                "password": password
            })),
        )
        .await?
        .0,
        StatusCode::CREATED
    );
    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header(header::ORIGIN, "http://localhost:8080")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"email":"detail@example.test","password":password}).to_string(),
                ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let cookie = response.headers()[header::SET_COOKIE]
        .to_str()?
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    app.db
        .call(|db| {
            db.execute_batch(
                "INSERT INTO projects(id,slug,name,created_at_us,updated_at_us)
                 VALUES(1,'detail','Detail',0,0);
                 INSERT INTO project_keys(id,project_id,public_key,created_at_us)
                 VALUES(1,1,'detail-public',0)",
            )?;
            Ok(())
        })
        .await?;
    let app = app.start_core().await?;
    Ok((dir, app.clone(), eventglass::http::router(app), cookie))
}

async fn ingest_record(app: &AppState) -> anyhow::Result<()> {
    let raw = serde_json::to_vec(&json!({
        "event_id": "01010101010101010101010101010101",
        "timestamp": "2026-09-08T00:00:00.000001Z",
        "message": "stored detail",
        "extra": {"password": SENTINEL, "safe": "retained"}
    }))?;
    let records = sentry::normalize_store(
        &raw,
        &ProjectContext {
            project_id: 1,
            slug: "detail".into(),
            public_key: "detail-public".into(),
            scrub_keys: vec![],
        },
        uuid::Uuid::new_v4(),
        eventglass::model::now_us()?,
        &Default::default(),
    )?
    .records;
    app.db
        .call(move |db| {
            ingest::accept(
                db,
                IngestProject {
                    id: 1,
                    slug: "detail".into(),
                    public_key: "detail-public".into(),
                },
                "record-detail-acceptance",
                records,
                &Default::default(),
            )
        })
        .await?;
    app.indexer.as_ref().unwrap().wake();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if app
                .indexer
                .as_ref()
                .unwrap()
                .snapshot()
                .unwrap()
                .boundary
                .ingest_seq
                == 1
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}

async fn search(router: &Router, cookie: &str) -> anyhow::Result<Value> {
    let (status, body) = request(
        router,
        "POST",
        "/api/explore/search",
        cookie,
        Some(json!({
            "projects": ["1"],
            "start": "2026-09-07T00:00:00Z",
            "end": "2026-09-09T00:00:00Z",
            "query": "",
            "limit": 10
        })),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    Ok(body)
}

async fn issue_detail_token(app: &AppState, record_id: String) -> anyhow::Result<String> {
    let published = app.indexer.as_ref().unwrap().snapshot()?;
    issue_detail_token_at(
        app,
        record_id,
        published.boundary.ingest_seq,
        published.shard_id,
    )
    .await
}

async fn issue_detail_token_at(
    app: &AppState,
    record_id: String,
    watermark: i64,
    shard_id: String,
) -> anyhow::Result<String> {
    let identity = app.db.call(|db| authorization::identity(db, 1)).await?;
    Ok(app.tokens.issue(
        TokenContext {
            storage_generation: identity.storage_generation,
            authorization_epoch: identity.epoch,
            authorization_hash: identity.hash,
            request_hash: "record-detail-v1".into(),
        },
        watermark,
        Position::Detail {
            project_id: 1,
            shard_id,
            record_id,
        },
        eventglass::model::now_us()?,
    )?)
}

async fn ingest_duplicate_logs(app: &AppState) -> anyhow::Result<String> {
    let mut record = sentry::normalize_store(
        &serde_json::to_vec(&json!({
            "event_id": "02020202020202020202020202020202",
            "timestamp": "2026-09-08T00:00:01Z",
            "message": "duplicate corruption probe"
        }))?,
        &ProjectContext {
            project_id: 1,
            slug: "detail".into(),
            public_key: "detail-public".into(),
            scrub_keys: vec![],
        },
        uuid::Uuid::new_v4(),
        eventglass::model::now_us()?,
        &Default::default(),
    )?
    .records
    .remove(0);
    record.kind = RecordKind::Log;
    record.source_event_id = None;
    record.issue_id = None;
    record.fingerprint = None;
    record.fingerprint_version = None;
    let record_id = record.record_id.clone();
    app.db
        .call(move |db| {
            ingest::accept(
                db,
                IngestProject {
                    id: 1,
                    slug: "detail".into(),
                    public_key: "detail-public".into(),
                },
                "record-detail-duplicate-acceptance",
                vec![record.clone(), record],
                &Default::default(),
            )
        })
        .await?;
    app.indexer.as_ref().unwrap().wake();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if app
                .indexer
                .as_ref()
                .unwrap()
                .snapshot()
                .unwrap()
                .boundary
                .ingest_seq
                == 3
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(record_id)
}

#[tokio::test]
async fn detail_is_exact_scrubbed_and_fails_closed() -> anyhow::Result<()> {
    let (_dir, app, router, cookie) = fixture().await?;
    ingest_record(&app).await?;
    let page = search(&router, &cookie).await?;
    assert_eq!(page["rows"].as_array().unwrap().len(), 1);
    let record_id = page["rows"][0]["record_id"].as_str().unwrap();
    let detail_token = page["rows"][0]["detail_token"].as_str().unwrap();

    let (status, detail) = request(
        &router,
        "GET",
        &format!("/api/records/{detail_token}"),
        &cookie,
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{detail}");
    assert_eq!(detail.as_object().unwrap().len(), 2);
    assert_eq!(detail["record_id"], record_id);
    assert_eq!(detail["raw"]["extra"]["password"], "[Filtered]");
    assert_eq!(detail["raw"]["extra"]["safe"], "retained");
    assert!(!serde_json::to_string(&detail)?.contains(SENTINEL));

    let shard_id = app.indexer.as_ref().unwrap().snapshot()?.shard_id;
    let before_record = issue_detail_token_at(&app, record_id.to_owned(), 0, shard_id).await?;
    assert_eq!(
        request(
            &router,
            "GET",
            &format!("/api/records/{before_record}"),
            &cookie,
            None,
        )
        .await?
        .0,
        StatusCode::NOT_FOUND
    );
    let wrong_shard = issue_detail_token_at(
        &app,
        record_id.to_owned(),
        1,
        uuid::Uuid::new_v4().to_string(),
    )
    .await?;
    assert_eq!(
        request(
            &router,
            "GET",
            &format!("/api/records/{wrong_shard}"),
            &cookie,
            None,
        )
        .await?
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );

    let wrong_domain = page["read_token"].as_str().unwrap();
    assert_eq!(
        request(
            &router,
            "GET",
            &format!("/api/records/{wrong_domain}"),
            &cookie,
            None,
        )
        .await?
        .0,
        StatusCode::BAD_REQUEST
    );
    let mut tampered = detail_token.as_bytes().to_vec();
    let last = tampered.last_mut().unwrap();
    *last = if *last == b'a' { b'b' } else { b'a' };
    let tampered = String::from_utf8(tampered)?;
    assert_eq!(
        request(
            &router,
            "GET",
            &format!("/api/records/{tampered}"),
            &cookie,
            None,
        )
        .await?
        .0,
        StatusCode::BAD_REQUEST
    );

    let absent = issue_detail_token(&app, "f".repeat(64)).await?;
    assert_eq!(
        request(
            &router,
            "GET",
            &format!("/api/records/{absent}"),
            &cookie,
            None,
        )
        .await?
        .0,
        StatusCode::NOT_FOUND
    );

    let duplicate_id = ingest_duplicate_logs(&app).await?;
    let duplicate = issue_detail_token(&app, duplicate_id).await?;
    assert_eq!(
        request(
            &router,
            "GET",
            &format!("/api/records/{duplicate}"),
            &cookie,
            None,
        )
        .await?
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    let permit = app.query_permit.clone().acquire_owned().await?;
    assert_eq!(
        request(
            &router,
            "GET",
            &format!("/api/records/{detail_token}"),
            &cookie,
            None,
        )
        .await?
        .0,
        StatusCode::TOO_MANY_REQUESTS
    );
    drop(permit);
    assert_eq!(
        request(
            &router,
            "GET",
            &format!("/api/records/{detail_token}"),
            "",
            None,
        )
        .await?
        .0,
        StatusCode::UNAUTHORIZED
    );

    app.db
        .call(|db| {
            db.execute(
                "UPDATE runtime_state SET authorization_epoch=authorization_epoch+1",
                [],
            )?;
            Ok(())
        })
        .await?;
    assert_eq!(
        request(
            &router,
            "GET",
            &format!("/api/records/{detail_token}"),
            &cookie,
            None,
        )
        .await?
        .0,
        StatusCode::FORBIDDEN
    );

    app.indexer.as_ref().unwrap().shutdown().await?;
    Ok(())
}
