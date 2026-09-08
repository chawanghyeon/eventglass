use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use eventglass::{
    app::AppState,
    config::Config,
    db::ingest::{self, IngestProject},
    model::Boundary,
    search::active::ActiveShard,
    sentry::{self, ProjectContext},
    storage::manifest::{self, ShardStats},
};
use rusqlite::params;
use serde_json::{Value, json};
use tower::ServiceExt;

async fn call(
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
        s3_endpoint: None,
        s3_initialize: false,
    })
    .await?;
    let router = eventglass::http::router(app.clone());
    let setup = eventglass::http::issue_setup_token(&app).await?;
    let password = "search-test-password-123"; // pragma: allowlist secret -- disposable test
    assert_eq!(
        call(
            &router,
            "/api/setup",
            "",
            json!({"token":setup,"email":"search@example.test","password":password})
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
                    json!({"email":"search@example.test","password":password}).to_string(),
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
    app.db.call(|db| { db.execute_batch("INSERT INTO projects(id,slug,name,created_at_us,updated_at_us) VALUES(1,'test','Test',0,0),(2,'other','Other',0,0); INSERT INTO project_keys(id,project_id,public_key,created_at_us) VALUES(1,1,'public',0),(2,2,'other-public',0)")?; Ok(()) }).await?;
    let app = app.start_core().await?;
    let router = eventglass::http::router(app.clone());
    Ok((dir, app, router, cookie))
}
async fn ingest(app: &AppState, project: i64, id: u32, message: &str) -> anyhow::Result<i64> {
    let slug = if project == 1 { "test" } else { "other" };
    let key = if project == 1 {
        "public"
    } else {
        "other-public"
    };
    let records = sentry::normalize_store(&serde_json::to_vec(&json!({"event_id":format!("{id:032x}"),"timestamp":"2026-09-08T00:00:00.000001Z","message":message,"extra":{"password":"sensitive-value"}}))?,
        &ProjectContext {project_id:project,slug:slug.into(),public_key:key.into(),scrub_keys:vec![]}, uuid::Uuid::new_v4(),eventglass::model::now_us()?,&Default::default())?.records;
    app.db
        .call(move |db| {
            ingest::accept(
                db,
                IngestProject {
                    id: project,
                    slug: slug.into(),
                    public_key: key.into(),
                },
                &format!("search-test-{id}"),
                records,
                &Default::default(),
            )
        })
        .await?;
    app.indexer.as_ref().unwrap().wake();
    let target = app
        .db
        .call(|db| {
            Ok(
                db.query_row("SELECT next_ingest_seq-1 FROM runtime_state", [], |r| {
                    r.get::<_, i64>(0)
                })?,
            )
        })
        .await?;
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

async fn add_matching_local_shard(
    dir: &tempfile::TempDir,
    app: &AppState,
    message: &str,
) -> anyhow::Result<String> {
    let (installation, boundary) = app
        .db
        .call(|db| {
            Ok((
                db.query_row(
                    "SELECT installation_id FROM runtime_state WHERE singleton=1",
                    [],
                    |row| row.get::<_, String>(0),
                )?,
                eventglass::db::indexer::applied(db)?,
            ))
        })
        .await?;
    let shard_id = uuid::Uuid::new_v4().to_string();
    let path = dir.path().join("shards").join(&shard_id);
    std::fs::create_dir(&path)?;
    let received_at = 1_788_825_600_000_001;
    let mut records = sentry::normalize_store(
        &serde_json::to_vec(&json!({
            "event_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "timestamp":"2026-09-08T00:00:00.000001Z",
            "message":message
        }))?,
        &ProjectContext {
            project_id: 1,
            slug: "test".into(),
            public_key: "public".into(),
            scrub_keys: vec![],
        },
        uuid::Uuid::new_v4(),
        received_at,
        &Default::default(),
    )?
    .records;
    records[0].ingest_seq = boundary.ingest_seq;
    let mut native = ActiveShard::create(&path, &installation, &shard_id, Boundary::default())?;
    native.publish(Boundary::default())?;
    native.commit(&records, boundary)?;
    native.publish(boundary)?;
    let manifest = native.seal(
        &path,
        ShardStats {
            record_count: 1,
            min_timestamp_us: Some(received_at),
            max_timestamp_us: Some(received_at),
            min_received_at_us: Some(received_at),
            max_received_at_us: Some(received_at),
            min_ingest_seq: Some(boundary.ingest_seq),
            max_ingest_seq: Some(boundary.ingest_seq),
        },
        received_at,
    )?;
    let size = i64::try_from(manifest::local_size(&path, &manifest)?)?;
    let catalog_id = shard_id.clone();
    app.db
        .call(move |db| {
            db.execute(
                "INSERT INTO shards(id,schema_version,format_version,tokenizer_version,state,
                    min_timestamp_us,max_timestamp_us,min_received_at_us,max_received_at_us,
                    first_record_received_at_us,min_ingest_seq,max_ingest_seq,last_applied_inbox_id,
                    record_count,size_bytes,created_at_us,sealed_at_us)
                 VALUES(?1,1,?2,1,'local',?3,?3,?3,?3,?3,?4,?4,?5,1,?6,?3,?3)",
                params![
                    catalog_id,
                    eventglass::db::shards::FORMAT_VERSION,
                    received_at,
                    boundary.ingest_seq,
                    boundary.inbox_id,
                    size
                ],
            )?;
            Ok(())
        })
        .await?;
    Ok(shard_id)
}
fn query() -> Value {
    json!({"projects":["1"],"start":"2026-09-07T00:00:00Z","end":"2026-09-09T00:00:00Z","query":"","limit":1})
}

#[tokio::test]
async fn rows_aggregate_and_detail_read_a_verified_local_sealed_shard() -> anyhow::Result<()> {
    let (dir, app, router, cookie) = fixture().await?;
    ingest(&app, 1, 91, "active-only").await?;
    add_matching_local_shard(&dir, &app, "sealed-only").await?;
    let mut input = query();
    input["query"] = json!("\"sealed-only\"");
    input["limit"] = json!(10);
    let (status, rows) = call(&router, "/api/explore/search", &cookie, input.clone()).await?;
    assert_eq!(status, StatusCode::OK, "{rows}");
    assert_eq!(rows["rows"].as_array().unwrap().len(), 1);
    assert_eq!(rows["rows"][0]["message"], "sealed-only");
    assert_eq!(rows["searched_shards"], "2");

    let detail = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/api/records/{}",
                    rows["rows"][0]["detail_token"].as_str().unwrap()
                ))
                .header(header::COOKIE, &cookie)
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(detail.status(), StatusCode::OK);

    let aggregate = json!({
        "projects":["1"],
        "start":"2026-09-07T00:00:00Z",
        "end":"2026-09-09T00:00:00Z",
        "query":"\"sealed-only\"",
        "metrics":[{"op":"count"}],
        "read_token":rows["read_token"]
    });
    let (status, result) = call(&router, "/api/explore/aggregate", &cookie, aggregate).await?;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["record_count"], "1");
    assert_eq!(result["searched_shards"], "2");
    app.indexer.as_ref().unwrap().shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn stable_http_pages_hold_watermark_and_do_not_project_raw() -> anyhow::Result<()> {
    let (_dir, app, router, cookie) = fixture().await?;
    ingest(&app, 1, 1, "first").await?;
    ingest(&app, 1, 2, "second").await?;
    ingest(&app, 2, 3, "hidden").await?;
    let (status, page) = call(&router, "/api/explore/search", &cookie, query()).await?;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(page["watermark"], "3");
    assert_eq!(page["rows"][0]["ingest_seq"], "2");
    let row = &page["rows"][0];
    for absent in [
        "raw_json",
        "attributes",
        "shard_id",
        "search_text",
        "timestamp_us",
    ] {
        assert!(row.get(absent).is_none());
    }
    assert_eq!(row["timestamp"], "2026-09-08T00:00:00.000001Z");
    assert!(row["detail_token"].as_str().is_some());
    ingest(&app, 1, 4, "late arrival").await?;
    let mut next = query();
    next["cursor"] = page["next_cursor"].clone();
    next["read_token"] = page["read_token"].clone();
    let (status, page2) = call(&router, "/api/explore/search", &cookie, next.clone()).await?;
    assert_eq!(status, StatusCode::OK, "{page2}");
    assert_eq!(page2["rows"][0]["ingest_seq"], "1");
    assert!(page2["next_cursor"].is_null());
    assert_eq!(page2["watermark"], "3");
    next["limit"] = json!(2);
    assert_eq!(
        call(&router, "/api/explore/search", &cookie, next).await?.0,
        StatusCode::BAD_REQUEST
    );
    let mut same = query();
    same["read_token"] = page["read_token"].clone();
    same["limit"] = json!(10);
    let (_, all) = call(&router, "/api/explore/search", &cookie, same).await?;
    assert_eq!(all["rows"].as_array().unwrap().len(), 2);
    app.indexer.as_ref().unwrap().shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn query_scope_tokens_revocation_and_busy_are_fail_closed() -> anyhow::Result<()> {
    let (_dir, app, router, cookie) = fixture().await?;
    ingest(&app, 1, 1, "visible").await?;
    ingest(&app, 2, 2, "hidden").await?;
    let mut input = query();
    input["query"] = json!("* OR project_id:2");
    input["limit"] = json!(10);
    let (status, page) = call(&router, "/api/explore/search", &cookie, input.clone()).await?;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(page["rows"].as_array().unwrap().len(), 1);
    assert_eq!(page["rows"][0]["project_id"], "1");
    input["query"] = json!("message:\"");
    assert_eq!(
        call(&router, "/api/explore/search", &cookie, input)
            .await?
            .0,
        StatusCode::BAD_REQUEST
    );
    let mut wrong = query();
    wrong["read_token"] = page["rows"][0]["detail_token"].clone();
    assert_eq!(
        call(&router, "/api/explore/search", &cookie, wrong)
            .await?
            .0,
        StatusCode::BAD_REQUEST
    );
    let permit = app.query_permit.clone().acquire_owned().await?;
    assert_eq!(
        call(&router, "/api/explore/search", &cookie, query())
            .await?
            .0,
        StatusCode::TOO_MANY_REQUESTS
    );
    drop(permit);
    assert_eq!(
        call(&router, "/api/explore/search", "", query()).await?.0,
        StatusCode::UNAUTHORIZED
    );
    let (_, page) = call(&router, "/api/explore/search", &cookie, query()).await?;
    app.db
        .call(|db| {
            db.execute(
                "UPDATE runtime_state SET authorization_epoch=authorization_epoch+1",
                [],
            )?;
            Ok(())
        })
        .await?;
    let mut revoked = query();
    revoked["read_token"] = page["read_token"].clone();
    assert_eq!(
        call(&router, "/api/explore/search", &cookie, revoked)
            .await?
            .0,
        StatusCode::FORBIDDEN
    );
    app.indexer.as_ref().unwrap().shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn get_wrapper_matches_post_and_unavailable_catalog_is_not_empty_success()
-> anyhow::Result<()> {
    let (_dir, app, router, cookie) = fixture().await?;
    ingest(&app, 1, 1, "visible").await?;
    let url = url::Url::parse_with_params(
        "http://localhost/api/logs",
        [
            ("projects", "1"),
            ("start", "2026-09-07T00:00:00Z"),
            ("end", "2026-09-09T00:00:00Z"),
            ("query", "visible"),
            ("limit", "1"),
        ],
    )?;
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("{}?{}", url.path(), url.query().unwrap()))
                .header(header::COOKIE, &cookie)
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    let page: Value = serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await?)?;
    assert_eq!(page["rows"][0]["message"], "visible");
    let mut tampered = query();
    let mut token = page["read_token"].as_str().unwrap().to_owned();
    token.replace_range(0..1, "!");
    tampered["read_token"] = json!(token);
    assert_eq!(
        call(&router, "/api/explore/search", &cookie, tampered)
            .await?
            .0,
        StatusCode::BAD_REQUEST
    );
    app.db.call(|db| {
        db.execute("INSERT INTO shards(id,schema_version,format_version,state,created_at_us) VALUES(?1,1,'tantivy-0.26.1/format-7','remote_only',0)",[uuid::Uuid::new_v4().to_string()])?; Ok(())
    }).await?;
    assert_eq!(
        call(&router, "/api/explore/search", &cookie, query())
            .await?
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    app.indexer.as_ref().unwrap().shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn aggregation_reuses_row_snapshot_and_auto_histogram_keeps_exact_count() -> anyhow::Result<()>
{
    let (_dir, app, router, cookie) = fixture().await?;
    ingest(&app, 1, 11, "first").await?;
    ingest(&app, 1, 12, "second").await?;
    let (_, page) = call(&router, "/api/explore/search", &cookie, query()).await?;
    ingest(&app, 1, 13, "late").await?;
    let mut aggregate = query();
    aggregate.as_object_mut().unwrap().remove("limit");
    aggregate["read_token"] = page["read_token"].clone();
    aggregate["metrics"] = json!([{"op":"count","name":"total"}]);
    aggregate["histogram"] = json!({"field":"timestamp","interval":"auto"});
    let (status, result) = call(
        &router,
        "/api/explore/aggregate",
        &cookie,
        aggregate.clone(),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["record_count"], "2");
    assert_eq!(result["metrics"][0]["value"], "2");
    assert_eq!(result["watermark"], "2");
    assert_eq!(result["complete"], true);
    let buckets = result["buckets"]["buckets"].as_array().unwrap();
    let count: u64 = buckets
        .iter()
        .map(|bucket| {
            bucket["doc_count"]
                .as_str()
                .unwrap()
                .parse::<u64>()
                .unwrap()
        })
        .sum();
    assert_eq!(count, 2);
    assert_eq!(
        result["buckets"]["dimension"]["histogram"]["interval_ms"],
        3_600_000
    );
    // Aggregate-issued read token is valid for rows with the same canonical source scope.
    let mut rows = query();
    rows["read_token"] = result["read_token"].clone();
    rows["limit"] = json!(100);
    let (status, rows) = call(&router, "/api/explore/search", &cookie, rows).await?;
    assert_eq!(status, StatusCode::OK, "{rows}");
    assert_eq!(rows["rows"].as_array().unwrap().len(), 2);
    aggregate["histogram"]["field"] = json!("received_at");
    assert_eq!(
        call(&router, "/api/explore/aggregate", &cookie, aggregate)
            .await?
            .0,
        StatusCode::BAD_REQUEST
    );
    app.indexer.as_ref().unwrap().shutdown().await?;
    Ok(())
}
