use eventglass::{app::AppState, config::Config};

fn config(path: &std::path::Path) -> Config {
    Config {
        addr: "127.0.0.1:0".parse().unwrap(),
        data_dir: path.to_owned(),
        base_url: "http://localhost:8080".parse().unwrap(),
        s3_url: None,
        s3_endpoint: None,
        s3_initialize: false,
    }
}

#[tokio::test]
async fn doctor_is_read_only_and_status_distinguishes_recovery_state() -> anyhow::Result<()> {
    let missing = tempfile::tempdir()?;
    let database = missing.path().join("meta.db");
    assert!(eventglass::operations::doctor(missing.path()).is_err());
    assert!(!database.exists(), "doctor must not initialize metadata");

    let root = tempfile::tempdir()?;
    let app = AppState::open(config(root.path()))
        .await?
        .start_core()
        .await?;
    let report = eventglass::operations::doctor(root.path())?;
    assert!(report.ok);
    assert_eq!(report.checked_local_shards, "1");
    assert_eq!(report.checked_remote_only_shards, "0");
    let cli = std::process::Command::new(env!("CARGO_BIN_EXE_eventglass"))
        .arg("doctor")
        .env("EVENTGLASS_DATA_DIR", root.path())
        .env("EVENTGLASS_BASE_URL", "http://localhost:8080")
        .output()?;
    assert!(
        cli.status.success(),
        "{}",
        String::from_utf8_lossy(&cli.stderr)
    );
    let cli_report: serde_json::Value = serde_json::from_slice(&cli.stdout)?;
    assert_eq!(cli_report["ok"], true);

    let disk = app.disk_budget.status()?;
    let data_dir = root.path().to_owned();
    let status = app
        .db
        .call(move |db| eventglass::operations::status(db, &data_dir, disk, true, false))
        .await?;
    assert!(status.ready);
    assert!(status.ingest_accepting);
    assert_eq!(status.shards.active, "1");
    assert_eq!(status.backup.state, "disabled");
    assert_eq!(status.inbox_records, "0");

    std::fs::create_dir(root.path().join("shards").join("unregistered"))?;
    assert!(eventglass::operations::doctor(root.path()).is_err());
    assert!(root.path().join("shards").join("unregistered").is_dir());
    std::fs::remove_dir(root.path().join("shards").join("unregistered"))?;
    if let Some(alerts) = &app.alerts {
        alerts.shutdown().await?;
    }
    app.indexer.as_ref().unwrap().shutdown().await?;
    Ok(())
}
