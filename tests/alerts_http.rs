use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode, header},
};
use eventglass::{app::AppState, config::Config};
use serde_json::{Value, json};
use tower::ServiceExt;

async fn call(
    router: &Router,
    method: Method,
    path: &str,
    cookie: &str,
    csrf: Option<&str>,
    input: Option<Value>,
) -> anyhow::Result<(StatusCode, Value)> {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header(header::ORIGIN, "http://localhost:8080")
        .header(header::COOKIE, cookie);
    if let Some(csrf) = csrf {
        request = request.header("x-csrf-token", csrf);
    }
    let body = if let Some(value) = input {
        request = request.header(header::CONTENT_TYPE, "application/json");
        Body::from(value.to_string())
    } else {
        Body::empty()
    };
    let response = router.clone().oneshot(request.body(body)?).await?;
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await?;
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)?
    };
    Ok((status, value))
}

fn alert(name: &str, enabled: bool) -> Value {
    json!({
        "name":name,
        "project_id":"1",
        "condition":{
            "type":"log_count",
            "query":"service:api",
            "window_seconds":300,
            "threshold":5,
            "cooldown_seconds":600,
            "time_basis":"received_at"
        },
        "destination":{"type":"webhook","url":"https://hooks.example.test/eventglass"},
        "enabled":enabled
    })
}

#[tokio::test]
async fn alert_http_contract_enforces_csrf_revision_and_soft_delete() -> anyhow::Result<()> {
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
    let password = "alerts-http-password-123"; // pragma: allowlist secret
    assert_eq!(
        call(
            &router,
            Method::POST,
            "/api/setup",
            "",
            None,
            Some(json!({"token":setup,"email":"admin@example.test","password":password})),
        )
        .await?
        .0,
        StatusCode::CREATED
    );
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/auth/login")
                .header(header::ORIGIN, "http://localhost:8080")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"email":"admin@example.test","password":password}).to_string(),
                ))?,
        )
        .await?;
    let cookie = response.headers()[header::SET_COOKIE]
        .to_str()?
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let login: Value = serde_json::from_slice(&to_bytes(response.into_body(), 64 * 1024).await?)?;
    let csrf = login["csrf_token"].as_str().unwrap();
    app.db
        .call(|db| {
            db.execute(
                "INSERT INTO projects(id,slug,name,created_at_us,updated_at_us)
                 VALUES(1,'api','API',0,0)",
                [],
            )?;
            Ok(())
        })
        .await?;

    assert_eq!(
        call(
            &router,
            Method::POST,
            "/api/alerts",
            &cookie,
            None,
            Some(alert("No CSRF", true)),
        )
        .await?
        .0,
        StatusCode::FORBIDDEN
    );
    let (status, created) = call(
        &router,
        Method::POST,
        "/api/alerts",
        &cookie,
        Some(csrf),
        Some(alert("API errors", true)),
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let id = created["id"].as_str().unwrap();
    let (_, listed) = call(&router, Method::GET, "/api/alerts", &cookie, None, None).await?;
    assert_eq!(listed["items"][0]["name"], "API errors");

    let mut update = alert("API logs", false);
    update["revision"] = json!(0);
    assert_eq!(
        call(
            &router,
            Method::PATCH,
            &format!("/api/alerts/{id}"),
            &cookie,
            Some(csrf),
            Some(update.clone()),
        )
        .await?
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &router,
            Method::PATCH,
            &format!("/api/alerts/{id}"),
            &cookie,
            Some(csrf),
            Some(update),
        )
        .await?
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        call(
            &router,
            Method::DELETE,
            &format!("/api/alerts/{id}?revision=1"),
            &cookie,
            Some(csrf),
            None,
        )
        .await?
        .0,
        StatusCode::NO_CONTENT
    );
    Ok(())
}
