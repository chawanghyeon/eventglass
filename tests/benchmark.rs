use std::{
    env, fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use eventglass::{
    app::AppState,
    config::{Config, Limits},
    db::ingest::{self, IngestProject},
    model::{Record, RecordKind},
    search::{
        aggregate::{self, AggregateRequest, HistogramSpec, MetricSpec},
        query::{JsonScalar, QueryScope, SearchRequest, SearchShard, TimeField, TypedFilter},
    },
};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const BASE_US: i64 = 1_788_825_600_000_000;
const BATCH_RECORDS: usize = 1_000;

#[derive(Serialize)]
struct Distribution {
    samples: usize,
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
}

#[derive(Serialize)]
struct Report {
    format_version: u32,
    duration_unit: &'static str,
    size_unit: &'static str,
    profile: String,
    records: usize,
    logs: usize,
    errors: usize,
    late_records: usize,
    attributes_per_record: usize,
    high_cardinality_fields: Vec<&'static str>,
    record_bytes: Distribution,
    acceptance_batch_records: usize,
    acceptance_latency: Distribution,
    seed_elapsed_ms: u128,
    seed_records_per_second: f64,
    visibility_lag_ms: u128,
    structured_search_latency: Distribution,
    text_search_latency: Distribution,
    histogram_latency: Distribution,
    searched_shards: usize,
    sqlite_bytes: u64,
    wal_bytes: u64,
    index_bytes: u64,
    inbox_records_after_drain: i64,
    inbox_bytes_after_drain: i64,
    issue_count: i64,
    cgroup: Value,
    process: Value,
    revision: String,
    cargo_lock_sha256: String,
    rust_version: String,
    native_format: &'static str,
    limitations: Vec<&'static str>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit resource benchmark; run through scripts/check-benchmark"]
async fn seeded_dataset_capacity() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_target(false)
        .try_init();
    let profile = required_env("EVENTGLASS_BENCH_PROFILE")?;
    let records = parse_record_count(&required_env("EVENTGLASS_BENCH_RECORDS")?)?;
    let report_path = PathBuf::from(required_env("EVENTGLASS_BENCH_REPORT")?);
    let data_root = tempfile::tempdir()?;
    let data_dir = data_root.path().join("data");
    let app = AppState::open(Config {
        addr: "127.0.0.1:0".parse()?,
        data_dir: data_dir.clone(),
        base_url: "http://127.0.0.1:8080".parse()?,
        s3_url: None,
        s3_endpoint: None,
        s3_initialize: false,
    })
    .await?;
    app.db
        .call(|database| {
            database.execute_batch(
                "INSERT INTO users(id,email,password_hash,role,is_active,created_at_us,updated_at_us)
                 VALUES(1,'benchmark@example.test','disabled','admin',1,1,1);
                 INSERT INTO projects(id,slug,name,created_at_us,updated_at_us)
                 VALUES(1,'benchmark','Benchmark',1,1);
                 INSERT INTO project_keys(id,project_id,public_key,created_at_us)
                 VALUES(1,1,'benchmark-public-key',1);",
            )?;
            Ok(())
        })
        .await?;
    let app = app.start_core().await?;
    let project = IngestProject {
        id: 1,
        slug: "benchmark".into(),
        public_key: "benchmark-public-key".into(),
    };
    let mut acceptance_us = Vec::with_capacity(records.div_ceil(BATCH_RECORDS));
    let mut record_sizes = Vec::with_capacity(records.min(100_000));
    let mut accepted = 0usize;
    let received_base_us = eventglass::model::now_us()?;
    let seed_started = Instant::now();
    while accepted < records {
        let end = (accepted + BATCH_RECORDS).min(records);
        let batch = (accepted..end)
            .map(|index| record(index, received_base_us))
            .collect::<Vec<_>>();
        for value in &batch {
            if record_sizes.len() < record_sizes.capacity() {
                record_sizes.push(serde_json::to_vec(value)?.len() as u64);
            }
        }
        let acceptance_id = format!("benchmark-{}", accepted / BATCH_RECORDS);
        let input = project.clone();
        let started = Instant::now();
        app.db
            .call(move |database| {
                ingest::accept(database, input, &acceptance_id, batch, &Limits::default())
            })
            .await?;
        acceptance_us.push(duration_us(started.elapsed()));
        accepted = end;
        app.indexer.as_ref().context("missing Indexer")?.wake();
        if accepted.is_multiple_of(10_000) {
            wait_for_inbox_budget(&app, 128 * 1024 * 1024).await?;
        }
    }
    let seed_elapsed = seed_started.elapsed();
    let visibility_started = Instant::now();
    wait_for_visibility(&app, i64::try_from(records)?).await?;
    let visibility_lag = visibility_started.elapsed();

    let (shards, issue_count, inbox_records, inbox_bytes) = snapshot(&app).await?;
    let scope = QueryScope {
        project_ids: vec![1],
        start_us: BASE_US - 24 * 60 * 60 * 1_000_000,
        end_us: BASE_US + i64::try_from(records)? * 10_000 + 60 * 60 * 1_000_000,
        watermark: i64::try_from(records)?,
        time_field: TimeField::Timestamp,
    };
    let structured = SearchRequest {
        query: String::new(),
        scope: scope.clone(),
        filters: vec![TypedFilter::JsonEq {
            path: "region".into(),
            value: JsonScalar::String("ap-northeast-2".into()),
        }],
        cursor: None,
        limit: 100,
    };
    let text = SearchRequest {
        query: "checkout latency".into(),
        scope: scope.clone(),
        filters: Vec::new(),
        cursor: None,
        limit: 100,
    };
    let histogram = AggregateRequest {
        query: String::new(),
        scope,
        filters: Vec::new(),
        metrics: vec![MetricSpec::Count {
            name: "records".into(),
        }],
        group_by: Vec::new(),
        histogram: Some(HistogramSpec {
            interval_ms: 60_000,
        }),
    };
    for _ in 0..5 {
        ensure!(
            !eventglass::search::query::search(&shards, &structured)?
                .rows
                .is_empty()
        );
        ensure!(
            !eventglass::search::query::search(&shards, &text)?
                .rows
                .is_empty()
        );
        ensure!(aggregate::aggregate(&shards, &histogram)?.record_count == records as u64);
    }
    let structured_us = measure(30, || {
        eventglass::search::query::search(&shards, &structured)?;
        Ok(())
    })?;
    let text_us = measure(30, || {
        eventglass::search::query::search(&shards, &text)?;
        Ok(())
    })?;
    let histogram_us = measure(30, || {
        let page = aggregate::aggregate(&shards, &histogram)?;
        ensure!(
            page.record_count == records as u64,
            "histogram lost records"
        );
        Ok(())
    })?;

    let report = Report {
        format_version: 1,
        duration_unit: "microseconds",
        size_unit: "bytes",
        profile,
        records,
        logs: records - records.div_ceil(21),
        errors: records.div_ceil(21),
        late_records: records.div_ceil(97),
        attributes_per_record: 15,
        high_cardinality_fields: vec!["host", "customer_id", "request_id"],
        record_bytes: distribution(record_sizes),
        acceptance_batch_records: BATCH_RECORDS,
        acceptance_latency: distribution(acceptance_us),
        seed_elapsed_ms: seed_elapsed.as_millis(),
        seed_records_per_second: records as f64 / seed_elapsed.as_secs_f64(),
        visibility_lag_ms: visibility_lag.as_millis(),
        structured_search_latency: distribution(structured_us),
        text_search_latency: distribution(text_us),
        histogram_latency: distribution(histogram_us),
        searched_shards: shards.len(),
        sqlite_bytes: size(&data_dir.join("meta.db"))?,
        wal_bytes: size(&data_dir.join("meta.db-wal"))?,
        index_bytes: tree_size(&data_dir.join("shards"))?,
        inbox_records_after_drain: inbox_records,
        inbox_bytes_after_drain: inbox_bytes,
        issue_count,
        cgroup: cgroup_evidence(),
        process: process_evidence(),
        revision: required_env("EVENTGLASS_BENCH_REVISION")?,
        cargo_lock_sha256: sha256_file(Path::new("Cargo.lock"))?,
        rust_version: required_env("EVENTGLASS_BENCH_RUST_VERSION")?,
        native_format: eventglass::db::shards::FORMAT_VERSION,
        limitations: vec![
            "seed phase calls the production durable acceptance operation in 1000-record batches; it does not measure HTTP wire ACK latency",
            "development profile omits the required 5-minute warmup and 30-minute sustained mixed workload",
            "S3 transfer and checkpoint lag are measured by the separate compatible storage gate",
        ],
    };
    ensure!(report.inbox_records_after_drain == 0 && report.inbox_bytes_after_drain == 0);
    ensure!(report.errors == usize::try_from(issue_occurrences(&app).await?)?);
    if let Some(parent) = report_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&report_path, serde_json::to_vec_pretty(&report)?)?;
    println!("{}", serde_json::to_string(&report)?);
    app.indexer
        .as_ref()
        .context("missing Indexer")?
        .shutdown()
        .await?;
    Ok(())
}

fn parse_record_count(value: &str) -> Result<usize> {
    let count: usize = value.parse()?;
    ensure!(matches!(count, 100_000 | 1_000_000 | 10_000_000));
    Ok(count)
}

fn required_env(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("{name} is required"))
}

fn record(index: usize, received_base_us: i64) -> Record {
    let error = index.is_multiple_of(21);
    let received_at_us = received_base_us + i64::try_from(index / BATCH_RECORDS).unwrap() * 10_000;
    let event_timestamp_us = BASE_US + i64::try_from(index).unwrap() * 10_000;
    let timestamp_us = if index.is_multiple_of(97) {
        event_timestamp_us - 6 * 60 * 60 * 1_000_000
    } else {
        event_timestamp_us
    };
    let record_id = digest(format!("eventglass-benchmark-record-{index}"));
    let issue_id = error.then(|| digest(format!("issue-{}", index % 100)));
    let level = if error {
        "error"
    } else {
        ["trace", "debug", "info", "warn", "error", "fatal"][index % 6]
    };
    let region = ["ap-northeast-2", "us-east-1", "eu-west-1", "ap-south-1"][index % 4];
    let route = ["/checkout", "/catalog", "/profile", "/payments"][index % 4];
    let message = if error {
        format!("checkout latency exceeded while charging order {index:08}")
    } else {
        format!("checkout latency observation for request {index:08} on {route}")
    };
    let request_id = format!("req-{index:016x}");
    let method = ["GET", "POST", "PUT", "DELETE"][index % 4];
    let payload_class = ["small", "medium", "large"][index % 3];
    let attributes = json!({
        "region": region, "host": format!("node-{:08}", index % 50_000),
        "customer_id": format!("customer-{index:012}"), "route": route,
        "status_code": if error { 503 } else { 200 + index % 5 },
        "duration_ms": 5.0 + (index % 20_000) as f64 / 10.0,
        "retry": index.is_multiple_of(13), "response_bytes": 512 + index % 65_536,
        "method": method,
        "build": format!("2026.09.{:02}", index % 30 + 1),
        "feature": format!("experiment-{}", index % 32),
        "tenant": format!("tenant-{}", index % 10_000), "shard_hint": index % 128,
        "attempt": index % 4, "payload_class": payload_class,
    });
    let raw_json = if error {
        json!({
            "event_id": format!("{index:032x}"), "message": message.clone(),
            "exception": {"values": [{"type": "CheckoutTimeout", "value": "payment provider exceeded deadline", "stacktrace": {"frames": [
                {"filename": "src/http/checkout.rs", "function": "charge", "lineno": 87, "in_app": true},
                {"filename": "src/services/payment.rs", "function": "request", "lineno": 214, "in_app": true}
            ]}}]},
            "breadcrumbs": [{"category": "db", "message": "loaded cart"}, {"category": "http", "message": "called payment provider"}],
            "contexts": {"runtime": {"name": "rust", "version": "1.97.1"}},
            "tags": {"region": region, "route": route}
        })
    } else {
        json!({
            "message": message.clone(),
            "attributes": attributes.clone(),
            "request_id": request_id.clone()
        })
    };
    Record {
        record_id,
        kind: if error {
            RecordKind::Error
        } else {
            RecordKind::Log
        },
        project_id: 1,
        source_event_id: error.then(|| format!("{index:032x}")),
        ingest_seq: 0,
        received_at_us,
        timestamp_us,
        service: format!("checkout-{}", index % 8),
        environment: Some(["production", "staging"][index % 2].into()),
        release: Some(format!("2026.09.{}", index % 30 + 1)),
        level: level.into(),
        logger: Some(format!("eventglass.benchmark.{}", index % 16)),
        message: message.clone(),
        trace_id: Some(format!("{:032x}", index / 4)),
        span_id: Some(format!("{:016x}", index)),
        request_id: Some(request_id),
        user_id: Some(format!("user-{}", index % 200_000)),
        user_email: None,
        issue_id,
        fingerprint_version: error.then_some(1),
        fingerprint: error.then(|| digest(format!("checkout-timeout-{}", index % 100))),
        attributes,
        search_text: format!("{message} {region} {route}"),
        raw_json,
        normalizer_version: 1,
        indexing_warnings: Vec::new(),
    }
}

async fn wait_for_inbox_budget(app: &AppState, maximum: i64) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let pending = app
                .db
                .call(|database| {
                    Ok(database.query_row(
                        "SELECT inbox_bytes FROM runtime_state WHERE singleton=1",
                        [],
                        |row| row.get::<_, i64>(0),
                    )?)
                })
                .await?;
            if pending <= maximum {
                return Ok::<_, anyhow::Error>(());
            }
            ensure!(app.indexer.as_ref().context("missing Indexer")?.ready());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    Ok(())
}

async fn wait_for_visibility(app: &AppState, target: i64) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(3_600), async {
        loop {
            let indexer = app.indexer.as_ref().context("missing Indexer")?;
            if indexer.snapshot()?.boundary.ingest_seq >= target {
                return Ok::<_, anyhow::Error>(());
            }
            ensure!(indexer.ready());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    Ok(())
}

async fn snapshot(app: &AppState) -> Result<(Vec<SearchShard>, i64, i64, i64)> {
    let (mut ids, issues, inbox_records, inbox_bytes) = app
        .db
        .call(|database| {
            let mut statement = database.prepare(
                "SELECT id FROM shards WHERE state IN ('active','local','remote_verified') ORDER BY id",
            )?;
            let ids = statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok((
                ids,
                database.query_row("SELECT count(*) FROM issues", [], |row| row.get(0))?,
                database.query_row(
                    "SELECT inbox_records FROM runtime_state WHERE singleton=1",
                    [],
                    |row| row.get(0),
                )?,
                database.query_row(
                    "SELECT inbox_bytes FROM runtime_state WHERE singleton=1",
                    [],
                    |row| row.get(0),
                )?,
            ))
        })
        .await?;
    ids.sort();
    let pins = app
        .indexer
        .as_ref()
        .context("missing Indexer")?
        .pin_shards(&ids)?;
    let shards = pins
        .iter()
        .map(|pin| SearchShard {
            id: pin.published().shard_id.clone(),
            searcher: pin.published().searcher.clone(),
        })
        .collect();
    Ok((shards, issues, inbox_records, inbox_bytes))
}

async fn issue_occurrences(app: &AppState) -> Result<i64> {
    app.db
        .call(|database| {
            Ok(
                database.query_row("SELECT count(*) FROM issue_occurrences", [], |row| {
                    row.get(0)
                })?,
            )
        })
        .await
}

fn measure(samples: usize, mut operation: impl FnMut() -> Result<()>) -> Result<Vec<u64>> {
    let mut values = Vec::with_capacity(samples);
    for _ in 0..samples {
        let started = Instant::now();
        operation()?;
        values.push(duration_us(started.elapsed()));
    }
    Ok(values)
}

fn duration_us(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn distribution(mut values: Vec<u64>) -> Distribution {
    assert!(!values.is_empty());
    values.sort_unstable();
    let at = |percent: usize| values[(values.len() * percent).div_ceil(100).saturating_sub(1)];
    Distribution {
        samples: values.len(),
        p50: at(50),
        p95: at(95),
        p99: at(99),
        max: *values.last().unwrap(),
    }
}

fn digest(value: String) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

fn sha256_file(path: &Path) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(fs::read(path)?)))
}

fn size(path: &Path) -> Result<u64> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
    }
}

fn tree_size(path: &Path) -> Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let mut total = 0u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        total = total
            .checked_add(if entry.file_type()?.is_dir() {
                tree_size(&entry.path())?
            } else {
                entry.metadata()?.len()
            })
            .context("tree size overflow")?;
    }
    Ok(total)
}

fn cgroup_evidence() -> Value {
    let read = |name: &str| {
        fs::read_to_string(format!("/sys/fs/cgroup/{name}"))
            .ok()
            .map(|value| value.trim().to_owned())
    };
    json!({
        "cpu.max": read("cpu.max"), "memory.max": read("memory.max"),
        "memory.swap.max": read("memory.swap.max"), "memory.peak": read("memory.peak"),
        "memory.events": read("memory.events"),
    })
}

fn process_evidence() -> Value {
    let status = fs::read_to_string("/proc/self/status").unwrap_or_default();
    let field = |name: &str| {
        status
            .lines()
            .find_map(|line| line.strip_prefix(name).map(str::trim).map(str::to_owned))
    };
    json!({"vm_rss": field("VmRSS:"), "vm_hwm": field("VmHWM:")})
}
