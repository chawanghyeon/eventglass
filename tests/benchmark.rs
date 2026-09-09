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
        aggregate::{AggregateRequest, HistogramSpec, MetricSpec},
        query::{JsonScalar, QueryScope, SearchRequest, TimeField, TypedFilter},
    },
};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const BASE_US: i64 = 1_788_825_600_000_000;
const BATCH_RECORDS: usize = 1_000;

struct PhaseProfile {
    warmup: Duration,
    sustained: Duration,
    drain: Duration,
    target_records_per_second: usize,
}

impl PhaseProfile {
    fn from_env() -> Result<Self> {
        let profile = Self {
            warmup: Duration::from_secs(optional_seconds("EVENTGLASS_BENCH_WARMUP_SECONDS")?),
            sustained: Duration::from_secs(optional_seconds("EVENTGLASS_BENCH_SUSTAINED_SECONDS")?),
            drain: Duration::from_secs(optional_seconds("EVENTGLASS_BENCH_DRAIN_SECONDS")?),
            target_records_per_second: env::var("EVENTGLASS_BENCH_RECORDS_PER_SECOND")
                .map_or(Ok(0), |value| {
                    value.parse::<usize>().map_err(anyhow::Error::from)
                })?,
        };
        ensure!(
            (!profile.enabled() && profile.target_records_per_second == 0)
                || (profile.warmup == Duration::from_secs(300)
                    && profile.sustained == Duration::from_secs(1_800)
                    && profile.drain == Duration::from_secs(600)
                    && profile.target_records_per_second == 105),
            "sustained benchmark requires 300s warmup, 1800s workload, 600s drain, and 105 records/s"
        );
        Ok(profile)
    }

    fn enabled(&self) -> bool {
        !self.warmup.is_zero() || !self.sustained.is_zero() || !self.drain.is_zero()
    }

    fn maximum_sustained_records(&self) -> usize {
        usize::try_from(self.sustained.as_secs())
            .unwrap_or(usize::MAX)
            .saturating_mul(self.target_records_per_second)
    }
}

#[derive(Serialize)]
struct Distribution {
    samples: usize,
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
}

#[derive(Serialize)]
struct InboxSample {
    elapsed_ms: u128,
    bytes: i64,
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
    warmup_duration_ms: u128,
    sustained_duration_ms: u128,
    drain_duration_ms: u128,
    sustained_target_records_per_second: usize,
    sustained_records: usize,
    sustained_acceptance_latency: Distribution,
    sustained_structured_search_latency: Distribution,
    sustained_text_search_latency: Distribution,
    sustained_histogram_latency: Distribution,
    inbox_bytes_at_sustained_start: i64,
    inbox_bytes_peak: i64,
    inbox_bytes_timeline: Vec<InboxSample>,
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
    let phases = PhaseProfile::from_env()?;
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

    let warmup_started = Instant::now();
    while warmup_started.elapsed() < phases.warmup {
        measure_query_mix(&app, records).await?;
        sleep_until_next_second(warmup_started).await;
    }
    let warmup_elapsed = phase_elapsed(warmup_started, phases.warmup);

    let inbox_bytes_at_sustained_start = inbox_bytes(&app).await?;
    let mut inbox_bytes_peak = inbox_bytes_at_sustained_start;
    let mut inbox_bytes_timeline = vec![InboxSample {
        elapsed_ms: 0,
        bytes: inbox_bytes_at_sustained_start,
    }];
    let mut sustained_acceptance_us = Vec::new();
    let mut sustained_structured_us = Vec::new();
    let mut sustained_text_us = Vec::new();
    let mut sustained_histogram_us = Vec::new();
    let mut sustained_records = 0usize;
    let sustained_started = Instant::now();
    while sustained_started.elapsed() < phases.sustained {
        let scheduled = ((sustained_started.elapsed().as_secs_f64()
            * phases.target_records_per_second as f64) as usize)
            .min(phases.maximum_sustained_records());
        if scheduled > sustained_records {
            let start = records + sustained_records;
            let end = records + scheduled;
            let batch = (start..end)
                .map(|index| record(index, received_base_us))
                .collect::<Vec<_>>();
            let input = project.clone();
            let acceptance_id = format!("benchmark-sustained-{sustained_records}");
            let started = Instant::now();
            app.db
                .call(move |database| {
                    ingest::accept(database, input, &acceptance_id, batch, &Limits::default())
                })
                .await?;
            sustained_acceptance_us.push(duration_us(started.elapsed()));
            sustained_records = scheduled;
            app.indexer.as_ref().context("missing Indexer")?.wake();
        }
        let measured = measure_query_mix(&app, records + sustained_records).await?;
        sustained_structured_us.push(measured[0]);
        sustained_text_us.push(measured[1]);
        sustained_histogram_us.push(measured[2]);
        let pending_bytes = inbox_bytes(&app).await?;
        inbox_bytes_peak = inbox_bytes_peak.max(pending_bytes);
        inbox_bytes_timeline.push(InboxSample {
            elapsed_ms: sustained_started.elapsed().as_millis(),
            bytes: pending_bytes,
        });
        sleep_until_next_second(sustained_started).await;
    }
    if sustained_records < phases.maximum_sustained_records() {
        let start = records + sustained_records;
        let end = records + phases.maximum_sustained_records();
        ensure!(
            end - start <= BATCH_RECORDS,
            "sustained workload fell more than one batch behind its target"
        );
        let batch = (start..end)
            .map(|index| record(index, received_base_us))
            .collect::<Vec<_>>();
        let input = project.clone();
        let acceptance_id = format!("benchmark-sustained-{sustained_records}");
        let started = Instant::now();
        app.db
            .call(move |database| {
                ingest::accept(database, input, &acceptance_id, batch, &Limits::default())
            })
            .await?;
        sustained_acceptance_us.push(duration_us(started.elapsed()));
        sustained_records = phases.maximum_sustained_records();
        app.indexer.as_ref().context("missing Indexer")?.wake();
    }
    let sustained_elapsed = phase_elapsed(sustained_started, phases.sustained);
    let total_records = records + sustained_records;

    let drain_started = Instant::now();
    while drain_started.elapsed() < phases.drain {
        app.indexer.as_ref().context("missing Indexer")?.wake();
        let pending_bytes = inbox_bytes(&app).await?;
        inbox_bytes_peak = inbox_bytes_peak.max(pending_bytes);
        inbox_bytes_timeline.push(InboxSample {
            elapsed_ms: phases.sustained.as_millis() + drain_started.elapsed().as_millis(),
            bytes: pending_bytes,
        });
        sleep_until_next_second(drain_started).await;
    }
    let drain_elapsed = phase_elapsed(drain_started, phases.drain);
    wait_for_visibility(&app, i64::try_from(total_records)?).await?;

    let (shards, issue_count, inbox_records, inbox_bytes) = snapshot(&app).await?;
    let scope = QueryScope {
        project_ids: vec![1],
        start_us: BASE_US - 24 * 60 * 60 * 1_000_000,
        end_us: BASE_US + i64::try_from(total_records)? * 10_000 + 60 * 60 * 1_000_000,
        watermark: i64::try_from(total_records)?,
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
            !app.indexer
                .as_ref()
                .context("missing Indexer")?
                .search(&shards, &structured)?
                .rows
                .is_empty()
        );
        ensure!(
            !app.indexer
                .as_ref()
                .context("missing Indexer")?
                .search(&shards, &text)?
                .rows
                .is_empty()
        );
        ensure!(
            app.indexer
                .as_ref()
                .context("missing Indexer")?
                .aggregate(&shards, &histogram)?
                .record_count
                == total_records as u64
        );
    }
    let structured_us = measure(30, || {
        app.indexer
            .as_ref()
            .context("missing Indexer")?
            .search(&shards, &structured)?;
        Ok(())
    })?;
    let text_us = measure(30, || {
        app.indexer
            .as_ref()
            .context("missing Indexer")?
            .search(&shards, &text)?;
        Ok(())
    })?;
    let histogram_us = measure(30, || {
        let page = app
            .indexer
            .as_ref()
            .context("missing Indexer")?
            .aggregate(&shards, &histogram)?;
        ensure!(
            page.record_count == total_records as u64,
            "histogram lost records"
        );
        Ok(())
    })?;

    acceptance_us.extend_from_slice(&sustained_acceptance_us);
    let limitations = if phases.enabled() {
        vec![
            "acceptance calls the production durable operation directly; HTTP wire ACK latency is covered separately",
            "S3 transfer and checkpoint lag are measured by the separate compatible storage gate",
        ]
    } else {
        vec![
            "seed phase calls the production durable acceptance operation in 1000-record batches; it does not measure HTTP wire ACK latency",
            "capacity profile omits the separate 5-minute warmup and 30-minute sustained mixed workload",
            "S3 transfer and checkpoint lag are measured by the separate compatible storage gate",
        ]
    };
    let report = Report {
        format_version: 2,
        duration_unit: "microseconds",
        size_unit: "bytes",
        profile,
        records: total_records,
        logs: total_records - total_records.div_ceil(21),
        errors: total_records.div_ceil(21),
        late_records: total_records.div_ceil(97),
        attributes_per_record: 15,
        high_cardinality_fields: vec!["host", "customer_id", "request_id"],
        record_bytes: distribution(record_sizes),
        acceptance_batch_records: BATCH_RECORDS,
        acceptance_latency: distribution(acceptance_us),
        seed_elapsed_ms: seed_elapsed.as_millis(),
        seed_records_per_second: records as f64 / seed_elapsed.as_secs_f64(),
        visibility_lag_ms: visibility_lag.as_millis(),
        warmup_duration_ms: warmup_elapsed.as_millis(),
        sustained_duration_ms: sustained_elapsed.as_millis(),
        drain_duration_ms: drain_elapsed.as_millis(),
        sustained_target_records_per_second: phases.target_records_per_second,
        sustained_records,
        sustained_acceptance_latency: distribution(sustained_acceptance_us),
        sustained_structured_search_latency: distribution(sustained_structured_us),
        sustained_text_search_latency: distribution(sustained_text_us),
        sustained_histogram_latency: distribution(sustained_histogram_us),
        inbox_bytes_at_sustained_start,
        inbox_bytes_peak,
        inbox_bytes_timeline,
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
        limitations,
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

fn optional_seconds(name: &str) -> Result<u64> {
    env::var(name).map_or(Ok(0), |value| {
        value.parse::<u64>().map_err(anyhow::Error::from)
    })
}

fn phase_elapsed(started: Instant, configured: Duration) -> Duration {
    if configured.is_zero() {
        Duration::ZERO
    } else {
        started.elapsed()
    }
}

async fn inbox_bytes(app: &AppState) -> Result<i64> {
    app.db
        .call(|database| {
            Ok(database.query_row(
                "SELECT inbox_bytes FROM runtime_state WHERE singleton=1",
                [],
                |row| row.get(0),
            )?)
        })
        .await
}

async fn sleep_until_next_second(started: Instant) {
    let elapsed = started.elapsed();
    let next = Duration::from_secs(elapsed.as_secs().saturating_add(1));
    if next > elapsed {
        tokio::time::sleep(next - elapsed).await;
    }
}

async fn measure_query_mix(app: &AppState, records: usize) -> Result<[u64; 3]> {
    let (shards, _, _, _) = snapshot(app).await?;
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
    let structured_started = Instant::now();
    ensure!(
        !app.indexer
            .as_ref()
            .context("missing Indexer")?
            .search(&shards, &structured)?
            .rows
            .is_empty()
    );
    let structured_us = duration_us(structured_started.elapsed());
    let text_started = Instant::now();
    ensure!(
        !app.indexer
            .as_ref()
            .context("missing Indexer")?
            .search(&shards, &text)?
            .rows
            .is_empty()
    );
    let text_us = duration_us(text_started.elapsed());
    let histogram_started = Instant::now();
    ensure!(
        app.indexer
            .as_ref()
            .context("missing Indexer")?
            .aggregate(&shards, &histogram)?
            .record_count
            > 0
    );
    Ok([
        structured_us,
        text_us,
        duration_us(histogram_started.elapsed()),
    ])
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

async fn snapshot(app: &AppState) -> Result<(Vec<String>, i64, i64, i64)> {
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
    Ok((ids, issues, inbox_records, inbox_bytes))
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
    if values.is_empty() {
        return Distribution {
            samples: 0,
            p50: 0,
            p95: 0,
            p99: 0,
            max: 0,
        };
    }
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
