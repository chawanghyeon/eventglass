use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, Response, StatusCode, header},
};
use eventglass::{
    app::AppState,
    config::Config,
    db::ingest::{self, IngestProject},
    sentry::{self, ProjectContext},
};
use serde_json::{Value, json};
use tower::ServiceExt;

const ORIGIN: &str = "http://localhost:8080";
const PASSWORD: &str = "occurrence-detail-password-123"; // pragma: allowlist secret -- test only
const SENTINEL: &str = "occurrence-detail-secret";
const HOSTILE: &str = "<script>window.evil = true</script>";

#[derive(Clone)]
struct Session {
    cookie: String,
    csrf: String,
}

async fn send(
    router: &Router,
    method: &str,
    path: &str,
    input: Option<Value>,
    session: Option<&Session>,
) -> anyhow::Result<Response<Body>> {
    let mut request = Request::builder().method(method).uri(path);
    if let Some(session) = session {
        request = request
            .header(header::COOKIE, &session.cookie)
            .header("x-csrf-token", &session.csrf);
    }
    let body = if let Some(input) = input {
        request = request
            .header(header::ORIGIN, ORIGIN)
            .header(header::CONTENT_TYPE, "application/json");
        Body::from(input.to_string())
    } else {
        Body::empty()
    };
    Ok(router.clone().oneshot(request.body(body)?).await?)
}

async fn json_body(response: Response<Body>) -> anyhow::Result<Value> {
    Ok(serde_json::from_slice(
        &to_bytes(response.into_body(), 1024 * 1024).await?,
    )?)
}

async fn login(router: &Router, email: &str) -> anyhow::Result<Session> {
    let response = send(
        router,
        "POST",
        "/api/auth/login",
        Some(json!({"email":email,"password":PASSWORD})),
        None,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let cookie = response.headers()[header::SET_COOKIE]
        .to_str()?
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let body = json_body(response).await?;
    Ok(Session {
        cookie,
        csrf: body["csrf_token"].as_str().unwrap().to_owned(),
    })
}

async fn fixture() -> anyhow::Result<(tempfile::TempDir, AppState, Router, Session, Session)> {
    let directory = tempfile::tempdir()?;
    let app = AppState::open(Config {
        addr: "127.0.0.1:0".parse()?,
        data_dir: directory.path().into(),
        base_url: ORIGIN.parse()?,
        s3_url: None,
    })
    .await?;
    let router = eventglass::http::router(app.clone());
    let setup_token = eventglass::http::issue_setup_token(&app).await?;
    let response = send(
        &router,
        "POST",
        "/api/setup",
        Some(json!({
            "token": setup_token,
            "email": "owner-detail@example.test",
            "password": PASSWORD
        })),
        None,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    let admin = login(&router, "owner-detail@example.test").await?;
    let response = send(
        &router,
        "POST",
        "/api/users",
        Some(json!({
            "email": "member-detail@example.test",
            "password": PASSWORD,
            "role": "member"
        })),
        Some(&admin),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    let member = login(&router, "member-detail@example.test").await?;
    app.db
        .call(|db| {
            db.execute_batch(
                "INSERT INTO projects(id,slug,name,created_at_us,updated_at_us)
                 VALUES(1,'first-detail','First detail',0,0),
                       (2,'second-detail','Second detail',0,0);
                 INSERT INTO project_keys(id,project_id,public_key,created_at_us)
                 VALUES(1,1,'first-public',0),(2,2,'second-public',0)",
            )?;
            Ok(())
        })
        .await?;
    let app = app.start_core().await?;
    Ok((
        directory,
        app.clone(),
        eventglass::http::router(app),
        admin,
        member,
    ))
}

async fn ingest_error(
    app: &AppState,
    project_id: i64,
    event_id: &str,
    message: &str,
) -> anyhow::Result<()> {
    let (slug, public_key) = if project_id == 1 {
        ("first-detail", "first-public")
    } else {
        ("second-detail", "second-public")
    };
    let records = sentry::normalize_store(
        &serde_json::to_vec(&json!({
            "event_id": event_id,
            "timestamp": "2026-09-08T00:00:00.000001Z",
            "message": message,
            "extra": {"password": SENTINEL, "nested": {"safe": "kept"}},
            "breadcrumbs": [{"message": HOSTILE}]
        }))?,
        &ProjectContext {
            project_id,
            slug: slug.into(),
            public_key: public_key.into(),
            scrub_keys: vec![],
        },
        uuid::Uuid::new_v4(),
        1_788_825_600_000_001,
        &Default::default(),
    )?
    .records;
    app.db
        .call(move |db| {
            ingest::accept(
                db,
                IngestProject {
                    id: project_id,
                    slug: slug.into(),
                    public_key: public_key.into(),
                },
                &format!("issue-detail-{project_id}"),
                records,
                &Default::default(),
            )
        })
        .await?;
    app.indexer.as_ref().unwrap().wake();
    Ok(())
}

async fn wait_for_cut(app: &AppState, expected: i64) -> anyhow::Result<()> {
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
                == expected
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}

async fn identities(app: &AppState, project_id: i64) -> anyhow::Result<(String, String)> {
    app.db
        .call(move |db| {
            Ok(db.query_row(
                "SELECT i.id,o.record_id
                 FROM issues i JOIN issue_occurrences o ON o.issue_id=i.id
                 WHERE i.project_id=?1",
                [project_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?)
        })
        .await
}

fn detail_path(issue_id: &str, record_id: &str) -> String {
    format!("/api/issues/{issue_id}/events/{record_id}")
}

#[tokio::test]
async fn occurrence_detail_uses_durable_identity_and_current_authorization() -> anyhow::Result<()> {
    let (_directory, app, router, admin, member) = fixture().await?;
    ingest_error(&app, 1, "11111111111111111111111111111111", HOSTILE).await?;
    ingest_error(&app, 2, "22222222222222222222222222222222", "other project").await?;
    wait_for_cut(&app, 2).await?;
    let (issue_one, record_one) = identities(&app, 1).await?;
    let (issue_two, record_two) = identities(&app, 2).await?;

    for session in [&admin, &member] {
        let response = send(
            &router,
            "GET",
            &detail_path(&issue_one, &record_one),
            None,
            Some(session),
        )
        .await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        let body = json_body(response).await?;
        assert_eq!(body.as_object().unwrap().len(), 2);
        assert_eq!(body["record_id"], record_one);
        assert_eq!(body["raw"]["message"], HOSTILE);
        assert_eq!(body["raw"]["breadcrumbs"][0]["message"], HOSTILE);
        assert_eq!(body["raw"]["extra"]["password"], "[Filtered]");
        assert_eq!(body["raw"]["extra"]["nested"]["safe"], "kept");
        assert!(!serde_json::to_string(&body)?.contains(SENTINEL));
    }

    for path in [
        detail_path(&issue_one, &record_two),
        detail_path(&issue_two, &record_one),
    ] {
        assert_eq!(
            send(&router, "GET", &path, None, Some(&admin))
                .await?
                .status(),
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        send(
            &router,
            "GET",
            &detail_path(&"f".repeat(64), &record_one),
            None,
            Some(&admin),
        )
        .await?
        .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(
            &router,
            "GET",
            &detail_path("not-a-digest", &record_one),
            None,
            Some(&admin),
        )
        .await?
        .status(),
        StatusCode::BAD_REQUEST
    );

    let permit = app.query_permit.clone().acquire_owned().await?;
    assert_eq!(
        send(
            &router,
            "GET",
            &detail_path(&issue_one, &record_one),
            None,
            Some(&admin),
        )
        .await?
        .status(),
        StatusCode::TOO_MANY_REQUESTS
    );
    drop(permit);

    // A durable occurrence sequence that points at a different native record is corruption, not
    // permission to return another document that happens to share a lower watermark.
    let mismatched_record = record_one.clone();
    let removed_record = record_two.clone();
    app.db
        .call(move |db| {
            db.execute(
                "DELETE FROM issue_occurrences WHERE record_id=?1",
                [removed_record],
            )?;
            db.execute(
                "UPDATE issue_occurrences SET ingest_seq=2 WHERE record_id=?1",
                [mismatched_record],
            )?;
            Ok(())
        })
        .await?;
    assert_eq!(
        send(
            &router,
            "GET",
            &detail_path(&issue_one, &record_one),
            None,
            Some(&admin),
        )
        .await?
        .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );

    app.db
        .call(|db| {
            db.execute("UPDATE projects SET is_active=0 WHERE id=1", [])?;
            db.execute(
                "UPDATE runtime_state SET authorization_epoch=authorization_epoch+1",
                [],
            )?;
            Ok(())
        })
        .await?;
    assert_eq!(
        send(
            &router,
            "GET",
            &detail_path(&issue_one, &record_one),
            None,
            Some(&admin),
        )
        .await?
        .status(),
        StatusCode::NOT_FOUND
    );

    app.db
        .call(|db| {
            db.execute(
                "UPDATE users SET is_active=0 WHERE email='member-detail@example.test'",
                [],
            )?;
            db.execute(
                "DELETE FROM sessions WHERE user_id=(SELECT id FROM users WHERE email='member-detail@example.test')",
                [],
            )?;
            db.execute(
                "UPDATE runtime_state SET authorization_epoch=authorization_epoch+1",
                [],
            )?;
            Ok(())
        })
        .await?;
    assert_eq!(
        send(
            &router,
            "GET",
            &detail_path(&issue_two, &record_two),
            None,
            Some(&member),
        )
        .await?
        .status(),
        StatusCode::UNAUTHORIZED
    );

    app.indexer.as_ref().unwrap().shutdown().await?;
    Ok(())
}
