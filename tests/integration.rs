use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use eventglass::{app::AppState, config::Config};
use serde_json::{Value, json};
use tower::ServiceExt;

const TEST_PASSWORD: &str = "test-password-123"; // pragma: allowlist secret -- local fixture only

async fn app() -> anyhow::Result<(tempfile::TempDir, AppState, Router)> {
    let dir = tempfile::tempdir()?;
    let state = AppState::open(Config {
        addr: "127.0.0.1:0".parse()?,
        data_dir: dir.path().to_path_buf(),
        base_url: "http://localhost:8080".parse()?,
        s3_url: None,
    })
    .await?;
    let router = eventglass::http::router(state.clone());
    Ok((dir, state, router))
}

fn request(
    method: &str,
    path: &str,
    body: Value,
    cookie: Option<&str>,
    csrf: Option<&str>,
) -> Request<Body> {
    let mut req = Request::builder()
        .method(method)
        .uri(path)
        .header("origin", "http://localhost:8080")
        .header("content-type", "application/json");
    if let Some(cookie) = cookie {
        req = req.header("cookie", cookie);
    }
    if let Some(csrf) = csrf {
        req = req.header("x-csrf-token", csrf);
    }
    req.body(Body::from(body.to_string())).unwrap()
}

#[tokio::test]
async fn setup_session_dsn_csrf_revoke_logout() -> anyhow::Result<()> {
    let (_dir, state, router) = app().await?;
    let token = eventglass::http::issue_setup_token(&state).await?;
    let setup = request(
        "POST",
        "/api/setup",
        json!({"token":token,"email":"owner@example.test","password":TEST_PASSWORD}), // pragma: allowlist secret -- local test password
        None,
        None,
    );
    assert_eq!(
        router.clone().oneshot(setup).await?.status(),
        StatusCode::CREATED
    );
    assert!(eventglass::http::issue_setup_token(&state).await.is_err());
    let login = router
        .clone()
        .oneshot(request(
            "POST",
            "/api/auth/login",
            json!({"email":"owner@example.test","password":TEST_PASSWORD}),
            None,
            None,
        ))
        .await?;
    assert_eq!(login.status(), StatusCode::OK);
    let cookie = login.headers()["set-cookie"]
        .to_str()?
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let data: Value = serde_json::from_slice(&to_bytes(login.into_body(), 8192).await?)?;
    let csrf = data["csrf_token"].as_str().unwrap();
    let no_csrf = router
        .clone()
        .oneshot(request(
            "POST",
            "/api/projects",
            json!({"slug":"test","name":"Test"}),
            Some(&cookie),
            None,
        ))
        .await?;
    assert_eq!(no_csrf.status(), StatusCode::FORBIDDEN);
    let created = router
        .clone()
        .oneshot(request(
            "POST",
            "/api/projects",
            json!({"slug":"test","name":"Test"}),
            Some(&cookie),
            Some(csrf),
        ))
        .await?;
    assert_eq!(created.status(), StatusCode::CREATED);
    let key = router
        .clone()
        .oneshot(request(
            "POST",
            "/api/projects/1/keys",
            json!({}),
            Some(&cookie),
            Some(csrf),
        ))
        .await?;
    assert_eq!(key.status(), StatusCode::CREATED);
    let key: Value = serde_json::from_slice(&to_bytes(key.into_body(), 8192).await?)?;
    assert!(key["dsn"].as_str().unwrap().ends_with("@localhost:8080/1"));
    let forbidden = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/projects")
                .header(
                    "x-sentry-auth",
                    format!("Sentry sentry_key={}", key["public_key"].as_str().unwrap()),
                )
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(forbidden.status(), StatusCode::UNAUTHORIZED);
    let revoked = router
        .clone()
        .oneshot(request(
            "DELETE",
            "/api/projects/1/keys/1",
            Value::Null,
            Some(&cookie),
            Some(csrf),
        ))
        .await?;
    assert_eq!(revoked.status(), StatusCode::NO_CONTENT);
    assert!(
        state
            .db
            .call(|db| Ok(db.query_row(
                "SELECT revoked_at_us IS NOT NULL FROM project_keys",
                [],
                |r| r.get::<_, bool>(0)
            )?))
            .await?
    );
    let logout = router
        .clone()
        .oneshot(request(
            "POST",
            "/api/auth/logout",
            Value::Null,
            Some(&cookie),
            Some(csrf),
        ))
        .await?;
    assert_eq!(logout.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        router
            .oneshot(request(
                "GET",
                "/api/auth/me",
                Value::Null,
                Some(&cookie),
                None
            ))
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    Ok(())
}

#[tokio::test]
async fn concurrent_setup_is_atomic_and_tokens_are_not_stored() -> anyhow::Result<()> {
    let (_dir, state, router) = app().await?;
    let token = eventglass::http::issue_setup_token(&state).await?;
    let make = |email| {
        request(
            "POST",
            "/api/setup",
            json!({"token":token,"email":email,"password":TEST_PASSWORD}),
            None,
            None,
        )
    };
    let (a, b) = tokio::join!(
        router.clone().oneshot(make("a@example.test")),
        router.clone().oneshot(make("b@example.test"))
    );
    let mut statuses = [a?.status().as_u16(), b?.status().as_u16()];
    statuses.sort();
    assert_eq!(statuses, [201, 403]);
    let (users, settings) = state
        .db
        .call(|db| {
            Ok((
                db.query_row("SELECT count(*) FROM users", [], |r| r.get::<_, i64>(0))?,
                db.query_row("SELECT count(*) FROM settings WHERE key='setup'", [], |r| {
                    r.get::<_, i64>(0)
                })?,
            ))
        })
        .await?;
    assert_eq!((users, settings), (1, 0));
    Ok(())
}

#[tokio::test]
async fn directory_lock_excludes_second_instance() -> anyhow::Result<()> {
    let (_dir, state, _router) = app().await?;
    assert!(AppState::open((*state.config).clone()).await.is_err());
    Ok(())
}
