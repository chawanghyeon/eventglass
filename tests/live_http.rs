use axum::{
    Router,
    body::{Body, BodyDataStream, to_bytes},
    http::{Request, StatusCode, header},
};
use eventglass::{
    app::AppState,
    config::Config,
    db::ingest::{self, IngestProject},
    sentry::{self, ProjectContext},
};
use serde_json::{Value, json};
use tokio_stream::StreamExt;
use tower::ServiceExt;

struct SseEvent {
    event: String,
    id: Option<String>,
    data: Value,
}

async fn json_call(
    router: &Router,
    path: &str,
    cookie: &str,
    input: Value,
) -> anyhow::Result<(StatusCode, Value)> {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header(header::ORIGIN, "http://localhost:8080")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::COOKIE, cookie)
                .body(Body::from(input.to_string()))?,
        )
        .await?;
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await?;
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)?
    };
    Ok((status, value))
}

async fn fixture() -> anyhow::Result<(tempfile::TempDir, AppState, Router, String)> {
    let directory = tempfile::tempdir()?;
    let app = AppState::open(Config {
        addr: "127.0.0.1:0".parse()?,
        data_dir: directory.path().into(),
        base_url: "http://localhost:8080".parse()?,
        s3_url: None,
        s3_endpoint: None,
        s3_initialize: false,
    })
    .await?;
    let router = eventglass::http::router(app.clone());
    let setup = eventglass::http::issue_setup_token(&app).await?;
    let password = "live-test-password-123"; // pragma: allowlist secret -- disposable test
    assert_eq!(
        json_call(
            &router,
            "/api/setup",
            "",
            json!({"token":setup,"email":"live@example.test","password":password}),
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
                    json!({"email":"live@example.test","password":password}).to_string(),
                ))?,
        )
        .await?;
    let cookie = response.headers()[header::SET_COOKIE]
        .to_str()?
        .split(';')
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing session cookie"))?
        .to_owned();
    app.db
        .call(|db| {
            db.execute_batch(
                "INSERT INTO projects(id,slug,name,created_at_us,updated_at_us)
                 VALUES(1,'live','Live',0,0);
                 INSERT INTO project_keys(id,project_id,public_key,created_at_us)
                 VALUES(1,1,'public',0)",
            )?;
            Ok(())
        })
        .await?;
    let app = app.start_core().await?;
    let router = eventglass::http::router(app.clone());
    Ok((directory, app, router, cookie))
}

async fn ingest(app: &AppState, id: u32, message: &str) -> anyhow::Result<i64> {
    let records = sentry::normalize_store(
        &serde_json::to_vec(&json!({
            "event_id":format!("{id:032x}"),
            "timestamp":"2020-01-01T00:00:00Z",
            "message":message
        }))?,
        &ProjectContext {
            project_id: 1,
            slug: "live".into(),
            public_key: "public".into(),
            scrub_keys: Vec::new(),
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
                    slug: "live".into(),
                    public_key: "public".into(),
                },
                &format!("live-test-{id}"),
                records,
                &Default::default(),
            )
        })
        .await?;
    let target = app
        .db
        .call(|db| {
            Ok(db.query_row(
                "SELECT next_ingest_seq-1 FROM runtime_state WHERE singleton=1",
                [],
                |row| row.get::<_, i64>(0),
            )?)
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
                >= target
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(target)
}

async fn subscribe(
    router: &Router,
    cookie: &str,
    resume: Option<&str>,
) -> anyhow::Result<BodyDataStream> {
    let path =
        "/api/logs/live?projects=1&start=2026-09-01T00%3A00%3A00Z&end=2026-10-01T00%3A00%3A00Z";
    let mut request = Request::builder().uri(path).header(header::COOKIE, cookie);
    if let Some(resume) = resume {
        request = request.header("last-event-id", resume);
    }
    let response = router.clone().oneshot(request.body(Body::empty())?).await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "text/event-stream"
    );
    Ok(response.into_body().into_data_stream())
}

async fn next_event(stream: &mut BodyDataStream, pending: &mut String) -> anyhow::Result<SseEvent> {
    loop {
        if let Some(end) = pending.find("\n\n") {
            let raw = pending[..end].to_owned();
            pending.drain(..end + 2);
            if raw.starts_with(':') {
                continue;
            }
            let mut event = None;
            let mut id = None;
            let mut data = None;
            for line in raw.lines() {
                if let Some(value) = line.strip_prefix("event: ") {
                    event = Some(value.to_owned());
                } else if let Some(value) = line.strip_prefix("id: ") {
                    id = Some(value.to_owned());
                } else if let Some(value) = line.strip_prefix("data: ") {
                    data = Some(serde_json::from_str(value)?);
                }
            }
            return Ok(SseEvent {
                event: event.ok_or_else(|| anyhow::anyhow!("SSE event name missing: {raw}"))?,
                id,
                data: data.ok_or_else(|| anyhow::anyhow!("SSE data missing: {raw}"))?,
            });
        }
        let chunk = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
            .await?
            .ok_or_else(|| anyhow::anyhow!("SSE stream ended"))??;
        pending.push_str(std::str::from_utf8(&chunk)?);
    }
}

#[tokio::test]
async fn live_checkpoints_zero_matches_resumes_and_closes_after_revoke() -> anyhow::Result<()> {
    let (_directory, app, router, cookie) = fixture().await?;
    let mut stream = subscribe(&router, &cookie, None).await?;
    let mut pending = String::new();
    let empty = next_event(&mut stream, &mut pending).await?;
    assert_eq!(empty.event, "checkpoint");
    assert_eq!(empty.data["scan_seq"], "0");

    ingest(&app, 1, "first live record").await?;
    ingest(&app, 2, "second live record").await?;
    let mut sequences = Vec::new();
    let resume = loop {
        let event = next_event(&mut stream, &mut pending).await?;
        match event.event.as_str() {
            "record" => sequences.push(event.data["ingest_seq"].as_str().unwrap().to_owned()),
            "checkpoint" if event.data["scan_seq"] == "2" => {
                break event
                    .id
                    .ok_or_else(|| anyhow::anyhow!("missing checkpoint id"))?;
            }
            "checkpoint" => {}
            unexpected => return Err(anyhow::anyhow!("unexpected live event: {unexpected}")),
        }
    };
    assert_eq!(sequences, ["1", "2"]);
    drop(stream);

    ingest(&app, 3, "third live record").await?;
    let mut resumed = subscribe(&router, &cookie, Some(&resume)).await?;
    let mut pending = String::new();
    let third = next_event(&mut resumed, &mut pending).await?;
    assert_eq!(third.event, "record");
    assert_eq!(third.data["ingest_seq"], "3");
    assert_eq!(
        next_event(&mut resumed, &mut pending).await?.event,
        "checkpoint"
    );

    app.db
        .call(|db| {
            db.execute(
                "UPDATE users SET is_active=0 WHERE email='live@example.test'",
                [],
            )?;
            Ok(())
        })
        .await?;
    ingest(&app, 4, "must not be delivered after revoke").await?;
    let error = next_event(&mut resumed, &mut pending).await?;
    assert_eq!(error.event, "error");
    assert_eq!(error.data["code"], "live_authorization_changed");
    app.indexer.as_ref().unwrap().shutdown().await?;
    Ok(())
}
