use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, Response, StatusCode, header},
};
use eventglass::{app::AppState, config::Config};
use rusqlite::params;
use serde_json::{Value, json};
use tower::ServiceExt;

const PASSWORD: &str = "test-password-123"; // pragma: allowlist secret -- test fixture
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

async fn body(response: Response<Body>) -> anyhow::Result<Value> {
    Ok(serde_json::from_slice(
        &to_bytes(response.into_body(), 128 * 1024).await?,
    )?)
}

async fn setup(fixture: &Fixture) -> anyhow::Result<Session> {
    let token = eventglass::http::issue_setup_token(&fixture.state).await?;
    let response = fixture
        .router
        .clone()
        .oneshot(request(
            "POST",
            "/api/setup",
            json!({"token":token,"email":"owner@example.test","password":PASSWORD}),
            None,
        ))
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    login(fixture, "owner@example.test").await
}

async fn login(fixture: &Fixture, email: &str) -> anyhow::Result<Session> {
    let response = fixture
        .router
        .clone()
        .oneshot(request(
            "POST",
            "/api/auth/login",
            json!({"email":email,"password":PASSWORD}),
            None,
        ))
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let cookie = response.headers()[header::SET_COOKIE]
        .to_str()?
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let data = body(response).await?;
    Ok(Session {
        cookie,
        csrf: data["csrf_token"].as_str().unwrap().to_owned(),
    })
}

async fn create_project(fixture: &Fixture, admin: &Session) -> anyhow::Result<i64> {
    let response = fixture
        .router
        .clone()
        .oneshot(request(
            "POST",
            "/api/projects",
            json!({"slug":"issues","name":"Issues"}),
            Some(admin),
        ))
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    Ok(body(response).await?["id"].as_str().unwrap().parse()?)
}

async fn create_member(fixture: &Fixture, admin: &Session) -> anyhow::Result<Session> {
    let response = fixture
        .router
        .clone()
        .oneshot(request(
            "POST",
            "/api/users",
            json!({"email":"member@example.test","password":PASSWORD,"role":"member"}),
            Some(admin),
        ))
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    login(fixture, "member@example.test").await
}

#[derive(Clone)]
struct SeedIssue {
    id: String,
    fingerprint: String,
    title: String,
    last_seen_us: i64,
    occurrence_count: i64,
}

async fn seed_issues(
    fixture: &Fixture,
    project_id: i64,
    issues: Vec<SeedIssue>,
) -> anyhow::Result<()> {
    fixture
        .state
        .db
        .call(move |db| {
            let tx = db.transaction()?;
            for issue in issues {
                tx.execute(
                    "INSERT INTO issues(
                         id,project_id,fingerprint,fingerprint_version,title,culprit,level,status,
                         first_seen_us,last_seen_us,occurrence_count,first_seen_ingest_seq,
                         last_seen_ingest_seq,revision,first_release,last_release,created_at_us,
                         updated_at_us)
                     VALUES(?1,?2,?3,1,?4,NULL,'error','unresolved',100,?5,?6,1,1,0,
                            'v1','v1',100,100)",
                    params![
                        issue.id,
                        project_id,
                        issue.fingerprint,
                        issue.title,
                        issue.last_seen_us,
                        issue.occurrence_count
                    ],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
}

fn digest(character: char) -> String {
    std::iter::repeat_n(character, 64).collect()
}

#[tokio::test]
async fn member_and_admin_access_active_issues_but_disabled_projects_are_hidden()
-> anyhow::Result<()> {
    let fixture = fixture().await?;
    let admin = setup(&fixture).await?;
    let member = create_member(&fixture, &admin).await?;
    let project_id = create_project(&fixture, &admin).await?;
    let issue_id = digest('1');
    seed_issues(
        &fixture,
        project_id,
        vec![SeedIssue {
            id: issue_id.clone(),
            fingerprint: digest('a'),
            title: "active issue".into(),
            last_seen_us: 200,
            occurrence_count: 1,
        }],
    )
    .await?;

    assert_eq!(
        fixture
            .router
            .clone()
            .oneshot(request(
                "GET",
                &format!("/api/issues/{issue_id}"),
                Value::Null,
                None,
            ))
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );

    for session in [&admin, &member] {
        let response = fixture
            .router
            .clone()
            .oneshot(request(
                "GET",
                &format!("/api/issues/{issue_id}"),
                Value::Null,
                Some(session),
            ))
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let issue = body(response).await?;
        assert_eq!(issue["project_id"], project_id.to_string());
        assert!(issue.get("raw_json").is_none());
    }
    let member_update = fixture
        .router
        .clone()
        .oneshot(request(
            "PATCH",
            &format!("/api/issues/{issue_id}"),
            json!({"status":"ignored","expected_revision":"0"}),
            Some(&member),
        ))
        .await?;
    assert_eq!(member_update.status(), StatusCode::OK);
    assert_eq!(body(member_update).await?["status"], "ignored");

    assert_eq!(
        fixture
            .router
            .clone()
            .oneshot(request(
                "PATCH",
                &format!("/api/projects/{project_id}"),
                json!({"is_active":false}),
                Some(&admin),
            ))
            .await?
            .status(),
        StatusCode::NO_CONTENT
    );
    for session in [&admin, &member] {
        let response = fixture
            .router
            .clone()
            .oneshot(request(
                "GET",
                &format!("/api/issues/{issue_id}"),
                Value::Null,
                Some(session),
            ))
            .await?;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(body(response).await?["error"]["code"], "issue_not_found");
    }
    Ok(())
}

#[tokio::test]
async fn resolve_uses_receive_watermark_and_revision_cas_then_ignore_clears_it()
-> anyhow::Result<()> {
    let fixture = fixture().await?;
    let admin = setup(&fixture).await?;
    let project_id = create_project(&fixture, &admin).await?;
    let issue_id = digest('2');
    seed_issues(
        &fixture,
        project_id,
        vec![SeedIssue {
            id: issue_id.clone(),
            fingerprint: digest('b'),
            title: "watermark".into(),
            last_seen_us: 200,
            occurrence_count: 1,
        }],
    )
    .await?;
    fixture
        .state
        .db
        .call(|db| {
            db.execute(
                "UPDATE runtime_state SET next_ingest_seq=41 WHERE singleton=1",
                [],
            )?;
            Ok(())
        })
        .await?;

    let without_csrf = Request::builder()
        .method("PATCH")
        .uri(format!("/api/issues/{issue_id}"))
        .header(header::ORIGIN, ORIGIN)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::COOKIE, &admin.cookie)
        .body(Body::from(
            json!({"status":"resolved","expected_revision":"0"}).to_string(),
        ))?;
    assert_eq!(
        fixture.router.clone().oneshot(without_csrf).await?.status(),
        StatusCode::FORBIDDEN
    );

    let resolved = fixture
        .router
        .clone()
        .oneshot(request(
            "PATCH",
            &format!("/api/issues/{issue_id}"),
            json!({"status":"resolved","expected_revision":"0"}),
            Some(&admin),
        ))
        .await?;
    assert_eq!(resolved.status(), StatusCode::OK);
    let resolved = body(resolved).await?;
    assert_eq!(resolved["status"], "resolved");
    assert_eq!(resolved["revision"], "1");
    assert_eq!(resolved["resolved_through_ingest_seq"], "40");
    assert!(resolved["resolved_at_us"].as_str().is_some());

    let stale = fixture
        .router
        .clone()
        .oneshot(request(
            "PATCH",
            &format!("/api/issues/{issue_id}"),
            json!({"status":"ignored","expected_revision":"0"}),
            Some(&admin),
        ))
        .await?;
    assert_eq!(stale.status(), StatusCode::CONFLICT);
    assert_eq!(
        body(stale).await?["error"]["code"],
        "issue_revision_conflict"
    );

    let ignored = fixture
        .router
        .clone()
        .oneshot(request(
            "PATCH",
            &format!("/api/issues/{issue_id}"),
            json!({"status":"ignored","expected_revision":"1"}),
            Some(&admin),
        ))
        .await?;
    assert_eq!(ignored.status(), StatusCode::OK);
    let ignored = body(ignored).await?;
    assert_eq!(ignored["status"], "ignored");
    assert_eq!(ignored["revision"], "2");
    assert!(ignored["resolved_at_us"].is_null());
    assert!(ignored["resolved_through_ingest_seq"].is_null());
    Ok(())
}

#[tokio::test]
async fn issue_and_occurrence_pages_keep_stable_ties_and_late_event_order() -> anyhow::Result<()> {
    let fixture = fixture().await?;
    let admin = setup(&fixture).await?;
    let project_id = create_project(&fixture, &admin).await?;
    let first = digest('1');
    let second = digest('2');
    let third = digest('3');
    seed_issues(
        &fixture,
        project_id,
        vec![
            SeedIssue {
                id: first.clone(),
                fingerprint: digest('a'),
                title: "<script>literal title</script>".into(),
                last_seen_us: 200,
                occurrence_count: 3,
            },
            SeedIssue {
                id: second.clone(),
                fingerprint: digest('b'),
                title: "same time".into(),
                last_seen_us: 200,
                occurrence_count: 1,
            },
            SeedIssue {
                id: third.clone(),
                fingerprint: digest('c'),
                title: "older".into(),
                last_seen_us: 100,
                occurrence_count: 1,
            },
        ],
    )
    .await?;
    let first_for_db = first.clone();
    fixture
        .state
        .db
        .call(move |db| {
            db.execute(
                "INSERT INTO shards(id,schema_version,format_version,state,created_at_us)
                 VALUES('issue-test-shard',1,'1','active',100)",
                [],
            )?;
            for (record, seq, occurred) in [
                (digest('a'), 10_i64, 300_i64),
                (digest('b'), 9, 300),
                // Received later, but the occurrence happened earlier.
                (digest('c'), 11, 100),
            ] {
                db.execute(
                    "INSERT INTO issue_occurrences(
                         event_key,project_id,issue_id,record_id,shard_id,source_event_id,
                         ingest_seq,occurred_at_us)
                     VALUES(?1,?2,?3,?1,'issue-test-shard',NULL,?4,?5)",
                    params![record, project_id, first_for_db, seq, occurred],
                )?;
            }
            Ok(())
        })
        .await?;

    let oversized_page = fixture
        .router
        .clone()
        .oneshot(request(
            "GET",
            &format!("/api/issues?project_id={project_id}&limit=101"),
            Value::Null,
            Some(&admin),
        ))
        .await?;
    assert_eq!(oversized_page.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        body(oversized_page).await?["error"]["code"],
        "invalid_issue_query"
    );

    let page_one = fixture
        .router
        .clone()
        .oneshot(request(
            "GET",
            &format!("/api/issues?project_id={project_id}&limit=2"),
            Value::Null,
            Some(&admin),
        ))
        .await?;
    assert_eq!(page_one.status(), StatusCode::OK);
    let page_one = body(page_one).await?;
    assert_eq!(page_one["items"][0]["id"], first);
    assert_eq!(page_one["items"][1]["id"], second);
    assert_eq!(page_one["items"][0]["last_seen_us"], "200");
    assert_eq!(page_one["items"][0]["occurrence_count"], "3");
    assert_eq!(
        page_one["items"][0]["title"],
        "<script>literal title</script>"
    );
    let cursor_time = page_one["next_cursor"]["last_seen_us"].as_str().unwrap();
    let cursor_id = page_one["next_cursor"]["id"].as_str().unwrap();
    let page_two = fixture
        .router
        .clone()
        .oneshot(request(
            "GET",
            &format!(
                "/api/issues?project_id={project_id}&limit=2&cursor_last_seen_us={cursor_time}&cursor_id={cursor_id}"
            ),
            Value::Null,
            Some(&admin),
        ))
        .await?;
    assert_eq!(body(page_two).await?["items"][0]["id"], third);

    let occurrence_one = fixture
        .router
        .clone()
        .oneshot(request(
            "GET",
            &format!("/api/issues/{first}/events?limit=2"),
            Value::Null,
            Some(&admin),
        ))
        .await?;
    assert_eq!(occurrence_one.status(), StatusCode::OK);
    let occurrence_one = body(occurrence_one).await?;
    assert_eq!(occurrence_one["items"][0]["ingest_seq"], "10");
    assert_eq!(occurrence_one["items"][1]["ingest_seq"], "9");
    assert!(occurrence_one["items"][0].get("raw_json").is_none());
    assert!(occurrence_one["items"][0].get("shard_id").is_none());
    let cursor_time = occurrence_one["next_cursor"]["occurred_at_us"]
        .as_str()
        .unwrap();
    let cursor_seq = occurrence_one["next_cursor"]["ingest_seq"]
        .as_str()
        .unwrap();
    let occurrence_two = fixture
        .router
        .clone()
        .oneshot(request(
            "GET",
            &format!(
                "/api/issues/{first}/events?limit=2&cursor_occurred_at_us={cursor_time}&cursor_ingest_seq={cursor_seq}"
            ),
            Value::Null,
            Some(&admin),
        ))
        .await?;
    let occurrence_two = body(occurrence_two).await?;
    assert_eq!(occurrence_two["items"][0]["ingest_seq"], "11");
    assert_eq!(occurrence_two["items"][0]["occurred_at_us"], "100");
    assert!(occurrence_two["next_cursor"].is_null());
    Ok(())
}

#[tokio::test]
async fn title_search_is_literal_scoped_and_keeps_pagination() -> anyhow::Result<()> {
    let fixture = fixture().await?;
    let admin = setup(&fixture).await?;
    let project_id = create_project(&fixture, &admin).await?;
    seed_issues(
        &fixture,
        project_id,
        vec![
            SeedIssue {
                id: digest('1'),
                fingerprint: digest('a'),
                title: "Checkout 50%_ failed".into(),
                last_seen_us: 300,
                occurrence_count: 1,
            },
            SeedIssue {
                id: digest('2'),
                fingerprint: digest('b'),
                title: "checkout timeout".into(),
                last_seen_us: 200,
                occurrence_count: 1,
            },
            SeedIssue {
                id: digest('3'),
                fingerprint: digest('c'),
                title: "Login failed".into(),
                last_seen_us: 100,
                occurrence_count: 1,
            },
        ],
    )
    .await?;
    let base = format!("/api/issues?project_id={project_id}&status=unresolved");
    for (query, expected) in [("CHECKOUT", 2), ("%25_", 1), ("missing", 0)] {
        let response = fixture
            .router
            .clone()
            .oneshot(request(
                "GET",
                &format!("{base}&query={query}"),
                Value::Null,
                Some(&admin),
            ))
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            body(response).await?["items"].as_array().unwrap().len(),
            expected
        );
    }
    let first = body(
        fixture
            .router
            .clone()
            .oneshot(request(
                "GET",
                &format!("{base}&query=checkout&limit=1"),
                Value::Null,
                Some(&admin),
            ))
            .await?,
    )
    .await?;
    let cursor = &first["next_cursor"];
    let second = body(
        fixture
            .router
            .clone()
            .oneshot(request(
                "GET",
                &format!(
                    "{base}&query=checkout&limit=1&cursor_last_seen_us={}&cursor_id={}",
                    cursor["last_seen_us"].as_str().unwrap(),
                    cursor["id"].as_str().unwrap()
                ),
                Value::Null,
                Some(&admin),
            ))
            .await?,
    )
    .await?;
    assert_eq!(second["items"][0]["id"], digest('2'));
    assert!(second["next_cursor"].is_null());
    let response = fixture
        .router
        .clone()
        .oneshot(request(
            "GET",
            &format!("{base}&query={}", "a".repeat(257)),
            Value::Null,
            Some(&admin),
        ))
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let response = fixture
        .router
        .clone()
        .oneshot(request(
            "GET",
            &format!("{base}&query=checkout"),
            Value::Null,
            None,
        ))
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    Ok(())
}
