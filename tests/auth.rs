use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, Response, StatusCode, header},
};
use eventglass::{app::AppState, config::Config};
use serde_json::{Value, json};
use tower::ServiceExt;

const PASSWORD: &str = "test-password-123"; // pragma: allowlist secret -- test fixture
const SECOND_PASSWORD: &str = "second-password-123"; // pragma: allowlist secret -- test fixture
const ORIGIN: &str = "http://localhost:8080";

struct Fixture {
    _directory: tempfile::TempDir,
    state: AppState,
    router: Router,
}

#[derive(Clone)]
struct Session {
    cookie: String,
    csrf: String,
}

async fn fixture() -> anyhow::Result<Fixture> {
    let directory = tempfile::tempdir()?;
    let state = AppState::open(Config {
        addr: "127.0.0.1:0".parse()?,
        data_dir: directory.path().to_path_buf(),
        base_url: ORIGIN.parse()?,
        s3_url: None,
        s3_endpoint: None,
        s3_initialize: false,
    })
    .await?;
    let router = eventglass::http::router(state.clone());
    Ok(Fixture {
        _directory: directory,
        state,
        router,
    })
}

fn request(method: &str, path: &str, body: Value, session: Option<&Session>) -> Request<Body> {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header(header::ORIGIN, ORIGIN)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(session) = session {
        request = request
            .header(header::COOKIE, &session.cookie)
            .header("x-csrf-token", &session.csrf);
    }
    request.body(Body::from(body.to_string())).unwrap()
}

async fn json_body(response: Response<Body>) -> anyhow::Result<Value> {
    Ok(serde_json::from_slice(
        &to_bytes(response.into_body(), 64 * 1024).await?,
    )?)
}

async fn setup(fixture: &Fixture, email: &str, password: &str) -> anyhow::Result<()> {
    let token = eventglass::http::issue_setup_token(&fixture.state).await?;
    let response = fixture
        .router
        .clone()
        .oneshot(request(
            "POST",
            "/api/setup",
            json!({"token":token,"email":email,"password":password}),
            None,
        ))
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    Ok(())
}

async fn login(fixture: &Fixture, email: &str, password: &str) -> anyhow::Result<Session> {
    let response = fixture
        .router
        .clone()
        .oneshot(request(
            "POST",
            "/api/auth/login",
            json!({"email":email,"password":password}),
            None,
        ))
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    let cookie = response.headers()[header::SET_COOKIE]
        .to_str()?
        .split(';')
        .next()
        .expect("session cookie must have a value")
        .to_owned();
    let body = json_body(response).await?;
    Ok(Session {
        cookie,
        csrf: body["csrf_token"]
            .as_str()
            .expect("login must return a CSRF token")
            .to_owned(),
    })
}

async fn authorization_epoch(fixture: &Fixture) -> anyhow::Result<i64> {
    fixture
        .state
        .db
        .call(|db| {
            Ok(db.query_row(
                "SELECT authorization_epoch FROM runtime_state WHERE singleton=1",
                [],
                |row| row.get(0),
            )?)
        })
        .await
}

#[tokio::test]
async fn setup_token_expiry_tampering_and_password_contract_fail_closed() -> anyhow::Result<()> {
    let fixture = fixture().await?;
    let expired_token = eventglass::http::issue_setup_token(&fixture.state).await?;
    let stored_setting = fixture
        .state
        .db
        .call(|db| {
            Ok(db.query_row(
                "SELECT value_json FROM settings WHERE key='setup'",
                [],
                |row| row.get::<_, String>(0),
            )?)
        })
        .await?;
    assert!(!stored_setting.contains(&expired_token));
    assert!(stored_setting.contains("expires_at_us"));
    fixture
        .state
        .db
        .call(|db| {
            let raw: String = db.query_row(
                "SELECT value_json FROM settings WHERE key='setup'",
                [],
                |row| row.get(0),
            )?;
            let mut setting: Value = serde_json::from_str(&raw)?;
            setting["expires_at_us"] = json!(eventglass::model::now_us()? - 1);
            db.execute(
                "UPDATE settings SET value_json=?1 WHERE key='setup'",
                [setting.to_string()],
            )?;
            Ok(())
        })
        .await?;
    let expired = fixture
        .router
        .clone()
        .oneshot(request(
            "POST",
            "/api/setup",
            json!({"token":expired_token,"email":"owner@example.test","password":PASSWORD}),
            None,
        ))
        .await?;
    assert_eq!(expired.status(), StatusCode::FORBIDDEN);

    let token = eventglass::http::issue_setup_token(&fixture.state).await?;
    let mut tampered_bytes = token.clone().into_bytes();
    tampered_bytes[0] = if tampered_bytes[0] == b'0' {
        b'1'
    } else {
        b'0'
    };
    let tampered_token = String::from_utf8(tampered_bytes)?;
    let tampered = fixture
        .router
        .clone()
        .oneshot(request(
            "POST",
            "/api/setup",
            json!({"token":tampered_token,"email":"owner@example.test","password":PASSWORD}),
            None,
        ))
        .await?;
    assert_eq!(tampered.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        fixture
            .router
            .clone()
            .oneshot(request(
                "POST",
                "/api/setup",
                json!({"token":token,"email":"OWNER@example.test","password":PASSWORD}),
                None,
            ))
            .await?
            .status(),
        StatusCode::CREATED
    );
    let password_hash = fixture
        .state
        .db
        .call(|db| {
            Ok(db.query_row("SELECT password_hash FROM users", [], |row| {
                row.get::<_, String>(0)
            })?)
        })
        .await?;
    assert!(password_hash.starts_with("$argon2id$v=19$m=32768,t=3,p=1$"));
    assert!(
        eventglass::http::issue_setup_token(&fixture.state)
            .await
            .is_err()
    );

    for email in ["missing@example.test", "owner@example.test"] {
        let response = fixture
            .router
            .clone()
            .oneshot(request(
                "POST",
                "/api/auth/login",
                json!({"email":email,"password":"incorrect-password-123"}), // pragma: allowlist secret -- synthetic test fixture or golden identity
                None,
            ))
            .await?;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            json_body(response).await?["error"]["code"],
            "invalid_credentials"
        );
    }
    Ok(())
}

#[tokio::test]
async fn sessions_reject_tampering_duplicate_cookies_expiry_and_revocation() -> anyhow::Result<()> {
    let fixture = fixture().await?;
    setup(&fixture, "owner@example.test", PASSWORD).await?;
    let session = login(&fixture, "owner@example.test", PASSWORD).await?;

    let mut tampered = session.clone();
    let mut bytes = tampered.cookie.into_bytes();
    let last = bytes.last_mut().expect("cookie cannot be empty");
    *last = if *last == b'0' { b'1' } else { b'0' };
    tampered.cookie = String::from_utf8(bytes)?;
    assert_eq!(
        fixture
            .router
            .clone()
            .oneshot(request("GET", "/api/auth/me", Value::Null, Some(&tampered)))
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );

    let duplicate_cookie = Request::builder()
        .uri("/api/auth/me")
        .header(header::COOKIE, &session.cookie)
        .header(header::COOKIE, &session.cookie)
        .body(Body::empty())?;
    assert_eq!(
        fixture
            .router
            .clone()
            .oneshot(duplicate_cookie)
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );

    let mut bad_csrf = session.clone();
    bad_csrf.csrf.replace_range(..1, "0");
    if bad_csrf.csrf == session.csrf {
        bad_csrf.csrf.replace_range(..1, "1");
    }
    assert_eq!(
        fixture
            .router
            .clone()
            .oneshot(request(
                "POST",
                "/api/users",
                json!({"email":"blocked@example.test","password":SECOND_PASSWORD,"role":"member"}),
                Some(&bad_csrf),
            ))
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );

    fixture
        .state
        .db
        .call(|db| {
            db.execute("UPDATE sessions SET expires_at_us=0", [])?;
            Ok(())
        })
        .await?;
    assert_eq!(
        fixture
            .router
            .clone()
            .oneshot(request("GET", "/api/auth/me", Value::Null, Some(&session)))
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );

    let renewed = login(&fixture, "owner@example.test", PASSWORD).await?;
    let logout = fixture
        .router
        .clone()
        .oneshot(request(
            "POST",
            "/api/auth/logout",
            Value::Null,
            Some(&renewed),
        ))
        .await?;
    assert_eq!(logout.status(), StatusCode::NO_CONTENT);
    assert!(
        logout.headers()[header::SET_COOKIE]
            .to_str()?
            .contains("Max-Age=0")
    );
    assert_eq!(
        fixture
            .router
            .clone()
            .oneshot(request("GET", "/api/auth/me", Value::Null, Some(&renewed)))
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    Ok(())
}

#[tokio::test]
async fn member_permissions_and_user_state_changes_revoke_sessions() -> anyhow::Result<()> {
    let fixture = fixture().await?;
    setup(&fixture, "owner@example.test", PASSWORD).await?;
    let owner = login(&fixture, "owner@example.test", PASSWORD).await?;
    let create = fixture
        .router
        .clone()
        .oneshot(request(
            "POST",
            "/api/users",
            json!({"email":"member@example.test","password":SECOND_PASSWORD,"role":"member"}),
            Some(&owner),
        ))
        .await?;
    assert_eq!(create.status(), StatusCode::CREATED);
    let member_id: i64 = json_body(create).await?["id"]
        .as_str()
        .expect("created user id")
        .parse()?;

    let users = fixture
        .router
        .clone()
        .oneshot(request("GET", "/api/users", Value::Null, Some(&owner)))
        .await?;
    assert_eq!(users.status(), StatusCode::OK);
    assert_eq!(
        json_body(users).await?["items"].as_array().unwrap().len(),
        2
    );

    let member = login(&fixture, "member@example.test", SECOND_PASSWORD).await?;
    assert_eq!(
        fixture
            .router
            .clone()
            .oneshot(request("GET", "/api/users", Value::Null, Some(&member)))
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        fixture
            .router
            .clone()
            .oneshot(request("GET", "/api/projects", Value::Null, Some(&member),))
            .await?
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        fixture
            .router
            .clone()
            .oneshot(request(
                "POST",
                "/api/projects",
                json!({"slug":"member-project","name":"Member Project"}),
                Some(&member),
            ))
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );

    let before_role_change = authorization_epoch(&fixture).await?;
    assert_eq!(
        fixture
            .router
            .clone()
            .oneshot(request(
                "PATCH",
                &format!("/api/users/{member_id}"),
                json!({"role":"admin"}),
                Some(&owner),
            ))
            .await?
            .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(authorization_epoch(&fixture).await?, before_role_change + 1);
    assert_eq!(
        fixture
            .router
            .clone()
            .oneshot(request("GET", "/api/auth/me", Value::Null, Some(&member)))
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );

    let promoted = login(&fixture, "member@example.test", SECOND_PASSWORD).await?;
    assert_eq!(
        fixture
            .router
            .clone()
            .oneshot(request("GET", "/api/users", Value::Null, Some(&promoted)))
            .await?
            .status(),
        StatusCode::OK
    );
    let before_deactivate = authorization_epoch(&fixture).await?;
    assert_eq!(
        fixture
            .router
            .clone()
            .oneshot(request(
                "PATCH",
                &format!("/api/users/{member_id}"),
                json!({"is_active":false}),
                Some(&owner),
            ))
            .await?
            .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(authorization_epoch(&fixture).await?, before_deactivate + 1);
    assert_eq!(
        fixture
            .router
            .clone()
            .oneshot(request("GET", "/api/auth/me", Value::Null, Some(&promoted)))
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        fixture
            .router
            .clone()
            .oneshot(request(
                "POST",
                "/api/auth/login",
                json!({"email":"member@example.test","password":SECOND_PASSWORD}),
                None,
            ))
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    Ok(())
}

#[tokio::test]
async fn last_active_admin_guard_is_atomic_and_db_rechecks_the_actor() -> anyhow::Result<()> {
    let fixture = fixture().await?;
    setup(&fixture, "owner@example.test", PASSWORD).await?;
    let owner = login(&fixture, "owner@example.test", PASSWORD).await?;
    let before = authorization_epoch(&fixture).await?;
    let rejected = fixture
        .router
        .clone()
        .oneshot(request(
            "PATCH",
            "/api/users/1",
            json!({"role":"member"}),
            Some(&owner),
        ))
        .await?;
    assert_eq!(rejected.status(), StatusCode::CONFLICT);
    assert_eq!(
        json_body(rejected).await?["error"]["code"],
        "last_admin_required"
    );
    assert_eq!(authorization_epoch(&fixture).await?, before);
    assert_eq!(
        fixture
            .router
            .clone()
            .oneshot(request("GET", "/api/auth/me", Value::Null, Some(&owner)))
            .await?
            .status(),
        StatusCode::OK
    );

    let created = fixture
        .router
        .clone()
        .oneshot(request(
            "POST",
            "/api/users",
            json!({"email":"backup@example.test","password":SECOND_PASSWORD,"role":"admin"}),
            Some(&owner),
        ))
        .await?;
    assert_eq!(created.status(), StatusCode::CREATED);
    let backup_id: i64 = json_body(created).await?["id"]
        .as_str()
        .expect("created admin id")
        .parse()?;
    assert_eq!(
        fixture
            .router
            .clone()
            .oneshot(request(
                "PATCH",
                "/api/users/1",
                json!({"role":"member"}),
                Some(&owner),
            ))
            .await?
            .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        fixture
            .router
            .clone()
            .oneshot(request("GET", "/api/auth/me", Value::Null, Some(&owner)))
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );

    let stale_actor_error = fixture
        .state
        .db
        .call(|db| {
            eventglass::db::auth::create_user(
                db,
                1,
                "must-not-exist@example.test",
                "unused-password-hash",
                "member",
                eventglass::model::now_us()?,
            )
        })
        .await
        .expect_err("the database must recheck the stale actor");
    assert!(matches!(
        stale_actor_error.downcast_ref::<eventglass::db::auth::AuthDbError>(),
        Some(eventglass::db::auth::AuthDbError::Forbidden)
    ));

    let backup = login(&fixture, "backup@example.test", SECOND_PASSWORD).await?;
    let rejected = fixture
        .router
        .clone()
        .oneshot(request(
            "PATCH",
            &format!("/api/users/{backup_id}"),
            json!({"is_active":false}),
            Some(&backup),
        ))
        .await?;
    assert_eq!(rejected.status(), StatusCode::CONFLICT);
    assert_eq!(
        fixture
            .router
            .clone()
            .oneshot(request("GET", "/api/auth/me", Value::Null, Some(&backup)))
            .await?
            .status(),
        StatusCode::OK
    );
    Ok(())
}

#[tokio::test]
async fn login_and_setup_have_separate_bounded_attempt_queues() -> anyhow::Result<()> {
    let fixture = fixture().await?;
    for attempt in 0..21 {
        let response = fixture
            .router
            .clone()
            .oneshot(request(
                "POST",
                "/api/auth/login",
                json!({"email":"missing@example.test","password":"x".repeat(129)}),
                None,
            ))
            .await?;
        let expected = if attempt < 20 {
            StatusCode::UNAUTHORIZED
        } else {
            StatusCode::TOO_MANY_REQUESTS
        };
        assert_eq!(response.status(), expected);
        if attempt == 20 {
            assert_eq!(response.headers()[header::RETRY_AFTER], "60");
        }
    }
    let setup_response = fixture
        .router
        .clone()
        .oneshot(request(
            "POST",
            "/api/setup",
            json!({"token":"0".repeat(64),"email":"owner@example.test","password":"short"}), // pragma: allowlist secret -- synthetic test fixture or golden identity
            None,
        ))
        .await?;
    assert_eq!(setup_response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}
