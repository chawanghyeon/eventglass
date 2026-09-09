use std::io::Write;

use anyhow::{Context, ensure};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use eventglass::{app::AppState, config::Config};
use serde_json::{Value, json};
use tower::ServiceExt;

const LINUX_ENOSPC: i32 = 28;

fn envelope() -> Vec<u8> {
    let event = json!({
        "event_id": "0123456789abcdef0123456789abcdef",
        "message": "must not be acknowledged under disk pressure",
        "timestamp": 1788860000.0
    });
    format!(
        "{}\n{}\n{}\n",
        json!({"dsn":"http://public-test-key@localhost/1"}),
        json!({"type":"event","length":event.to_string().len()}),
        event
    )
    .into_bytes()
}

#[tokio::test]
async fn isolated_enospc_rejects_ingest_with_429_while_ready() -> anyhow::Result<()> {
    let root = std::env::var_os("EVENTGLASS_DISK_PRESSURE_ROOT")
        .context("EVENTGLASS_DISK_PRESSURE_ROOT must name the isolated test filesystem")?;
    let root = std::path::PathBuf::from(root);
    let total = fs4::total_space(&root)?;
    ensure!(
        total <= 805_306_368,
        "refusing to fill an unbounded filesystem: {total} bytes"
    );

    let data_dir = root.join(format!("eventglass-{}", uuid::Uuid::new_v4()));
    let app = AppState::open(Config {
        addr: "127.0.0.1:0".parse()?,
        data_dir: data_dir.clone(),
        base_url: "http://localhost:8080".parse()?,
        s3_url: None,
        s3_endpoint: None,
        s3_initialize: false,
    })
    .await?;
    app.db
        .call(|db| {
            db.execute_batch(
                "INSERT INTO projects(id,slug,name,created_at_us,updated_at_us)
                 VALUES(1,'test','Test',0,0);
                 INSERT INTO project_keys(id,project_id,public_key,created_at_us)
                 VALUES(1,1,'public-test-key',0)",
            )?;
            Ok(())
        })
        .await?;
    let app = app.start_core().await?;
    ensure!(app.disk_budget.status()?.ingest_accepting);
    let router = eventglass::http::router(app.clone());

    let filler_path = root.join("enospc-filler");
    let mut filler = std::fs::File::create(&filler_path)?;
    let block = vec![0xa5; 1024 * 1024];
    let enospc = loop {
        match filler.write_all(&block) {
            Ok(()) => {}
            Err(error) => break error,
        }
    };
    ensure!(
        enospc.raw_os_error() == Some(LINUX_ENOSPC),
        "isolated filesystem did not return ENOSPC: {enospc}"
    );
    ensure!(!app.disk_budget.status()?.ingest_accepting);

    let rejected = router
        .clone()
        .oneshot(Request::post("/api/1/envelope/").body(Body::from(envelope()))?)
        .await?;
    ensure!(rejected.status() == StatusCode::TOO_MANY_REQUESTS);
    ensure!(rejected.headers()["retry-after"] == "60");
    let body: Value = serde_json::from_slice(&to_bytes(rejected.into_body(), 8192).await?)?;
    ensure!(body["error"]["code"] == "disk_reserve");
    let pending = app
        .db
        .call(|db| Ok(db.query_row("SELECT count(*) FROM inbox", [], |row| row.get::<_, i64>(0))?))
        .await?;
    ensure!(
        pending == 0,
        "disk-pressure request was durably acknowledged"
    );

    let ready = router
        .oneshot(Request::get("/readyz").body(Body::empty())?)
        .await?;
    ensure!(ready.status() == StatusCode::OK);

    drop(filler);
    std::fs::remove_file(filler_path)?;
    if let Some(alerts) = &app.alerts {
        alerts.shutdown().await?;
    }
    app.indexer
        .as_ref()
        .context("missing Indexer")?
        .shutdown()
        .await?;
    std::fs::remove_dir_all(data_dir)?;
    Ok(())
}
