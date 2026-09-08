use std::{
    collections::BTreeMap,
    env,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result, ensure};
use rusqlite::Connection;
use serde_json::{Value, json};
use tantivy::{
    Index, Order, TantivyDocument,
    collector::{Count, TopDocs},
    query::AllQuery,
    schema::Value as TantivyValue,
};

const SENTINEL: &str = "eventglass-live-scrub-sentinel";
const CASES: &[&str] = &[
    "python-events",
    "python-logging-default",
    "python-logging-debug",
    "node-events-and-logs",
];

#[test]
#[ignore = "explicit live gate: requires already-bootstrapped pinned Python and Node SDKs"]
fn real_sdks_reach_running_eventglass() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let directory = tempfile::tempdir()?;
    let data_dir = directory.path().join("data");
    std::fs::create_dir(&data_dir)?;
    std::fs::write(
        data_dir.join(".sdk-live-owned"),
        "Eventglass SDK live fixture\n",
    )?;
    let report_path = directory.path().join("result.json");
    let source_binary = env::var_os("EVENTGLASS_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_eventglass")));
    let binary = directory.path().join("eventglass-live-bin");
    std::fs::copy(&source_binary, &binary).with_context(|| {
        format!(
            "snapshot Eventglass binary from {}",
            source_binary.display()
        )
    })?;
    let output = Command::new("python3")
        .arg(root.join("tools/sdk-fixtures/live.py"))
        .arg("--binary")
        .arg(&binary)
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--report")
        .arg(&report_path)
        .current_dir(&root)
        .output()
        .context("start SDK live fixture runner")?;
    ensure!(
        output.status.success(),
        "SDK live runner failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let report: Value = serde_json::from_slice(&std::fs::read(&report_path)?)?;
    ensure!(report["result"] == "pass");
    ensure!(report["localhost_only"] == true);
    ensure!(report["acknowledged_records"] == 17);
    ensure!(report["durable_cut_before_restart"] == 17);
    ensure!(report["durable_cut_after_restart"] == 17);
    ensure!(report["gzip_exercised"] == true);
    ensure!(report["chunked_transfer_exercised"] == true);
    ensure!(report["event_and_log_envelope_items_exercised"] == true);
    ensure!(report["sdk_mode"] == "live-sequential");

    let records = native_records(&data_dir, 17)?;
    ensure!(records.len() == 17, "expected 17 published native records");
    compare_expected(&root, &records, CASES)?;
    verify_native_storage(&records, 17)?;

    let direct_data_dir = directory.path().join("direct-data");
    std::fs::create_dir(&direct_data_dir)?;
    std::fs::write(
        direct_data_dir.join(".sdk-live-owned"),
        "Eventglass SDK live fixture\n",
    )?;
    let direct_report_path = directory.path().join("direct-result.json");
    let direct_output = Command::new("python3")
        .arg(root.join("tools/sdk-fixtures/live.py"))
        .arg("--binary")
        .arg(&binary)
        .arg("--mode")
        .arg("direct-node")
        .arg("--data-dir")
        .arg(&direct_data_dir)
        .arg("--report")
        .arg(&direct_report_path)
        .current_dir(&root)
        .output()
        .context("start direct Node SDK fixture runner")?;
    ensure!(
        direct_output.status.success(),
        "direct Node SDK runner failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&direct_output.stdout),
        String::from_utf8_lossy(&direct_output.stderr)
    );
    let direct_report: Value = serde_json::from_slice(&std::fs::read(&direct_report_path)?)?;
    ensure!(direct_report["result"] == "pass");
    ensure!(direct_report["mode"] == "direct_node");
    ensure!(direct_report["transport_observer"] == false);
    ensure!(direct_report["durable_cut"] == 8);
    ensure!(direct_report["sdk_mode"] == "live-sequential");
    let direct_records = native_records(&direct_data_dir, 8)?;
    compare_expected(&root, &direct_records, &["node-events-and-logs"])?;
    verify_native_storage(&direct_records, 8)?;

    let requests = report["requests"]
        .as_array()
        .context("live report requests must be an array")?;
    let wire_bytes = requests
        .iter()
        .map(|request| request["wire_bytes"].as_u64().context("request wire_bytes"))
        .sum::<Result<u64>>()?;
    let decoded_bytes = requests
        .iter()
        .map(|request| {
            request["decoded_bytes"]
                .as_u64()
                .context("request decoded_bytes")
        })
        .sum::<Result<u64>>()?;
    println!(
        "SDK live PASS: {} observed HTTP requests, 17 ACKed/native records, {wire_bytes} wire \
         bytes, {decoded_bytes} decoded bytes, restart preserved cut; direct Node added 8 exact \
         native records without observer; scrub sentinel absent",
        requests.len()
    );
    Ok(())
}

fn verify_native_storage(records: &[Value], expected: i64) -> Result<()> {
    let all_native_json = serde_json::to_string(records)?;
    ensure!(
        !all_native_json.contains(SENTINEL),
        "scrub sentinel reached the native store"
    );
    ensure!(
        records
            .iter()
            .filter_map(|record| record.get("raw_json").and_then(Value::as_str))
            .any(|raw| raw.contains("[Filtered]")),
        "scrub replacement is absent from native raw JSON"
    );
    let mut sequences = records
        .iter()
        .map(|record| record["ingest_seq"].as_i64().context("native ingest_seq"))
        .collect::<Result<Vec<_>>>()?;
    sequences.sort_unstable();
    ensure!(sequences == (1..=expected).collect::<Vec<_>>());
    Ok(())
}

fn native_records(data_dir: &Path, expected: i64) -> Result<Vec<Value>> {
    let database = Connection::open_with_flags(
        data_dir.join("meta.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let (shard_id, applied, pending): (String, i64, i64) = database.query_row(
        "SELECT active_shard_id,last_applied_ingest_seq,inbox_records
         FROM runtime_state WHERE singleton=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    ensure!(
        applied == expected && pending == 0,
        "native cut was not finalized"
    );
    drop(database);

    let index = Index::open_in_dir(data_dir.join("shards").join(shard_id))?;
    let schema = index.schema();
    let reader = index.reader()?;
    let searcher = reader.searcher();
    ensure!(searcher.search(&AllQuery, &Count)? == expected as usize);
    let collector =
        TopDocs::with_limit(expected as usize).order_by_fast_field::<i64>("ingest_seq", Order::Asc);
    let addresses = searcher.search(&AllQuery, &collector)?;
    addresses
        .into_iter()
        .map(|(_score, address)| {
            let document: TantivyDocument = searcher.doc(address)?;
            let mut record = json!({
                "kind": required_text(&document, &schema, "kind")?,
                "project_id": required_i64(&document, &schema, "project_id")?,
                "ingest_seq": required_i64(&document, &schema, "ingest_seq")?,
                "environment": optional_text(&document, &schema, "environment")?,
                "release": optional_text(&document, &schema, "release")?,
                "level": required_text(&document, &schema, "level")?,
                "message": required_text(&document, &schema, "message")?,
                "raw_json": required_text(&document, &schema, "raw_json")?,
            });
            if let Some(logger) = optional_text(&document, &schema, "logger")? {
                record["logger"] = json!(logger);
            }
            Ok(record)
        })
        .collect()
}

fn required_text(
    document: &TantivyDocument,
    schema: &tantivy::schema::Schema,
    name: &str,
) -> Result<String> {
    optional_text(document, schema, name)?.with_context(|| format!("missing native {name}"))
}

fn optional_text(
    document: &TantivyDocument,
    schema: &tantivy::schema::Schema,
    name: &str,
) -> Result<Option<String>> {
    let field = schema.get_field(name)?;
    Ok(document
        .get_first(field)
        .and_then(|value| value.as_str())
        .map(str::to_owned))
}

fn required_i64(
    document: &TantivyDocument,
    schema: &tantivy::schema::Schema,
    name: &str,
) -> Result<i64> {
    let field = schema.get_field(name)?;
    document
        .get_first(field)
        .and_then(|value| value.as_i64())
        .with_context(|| format!("missing native {name}"))
}

fn compare_expected(root: &Path, native: &[Value], cases: &[&str]) -> Result<()> {
    let mut expected = Vec::new();
    let mut forbidden = Vec::new();
    for case in cases {
        let fixture: Value = serde_json::from_slice(&std::fs::read(
            root.join("tests/fixtures/sentry")
                .join(case)
                .join("expected.normalized.json"),
        )?)?;
        expected.extend(
            fixture["records"]
                .as_array()
                .context("fixture records must be an array")?
                .iter()
                .cloned(),
        );
        forbidden.extend(
            fixture["must_not_contain"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_owned),
        );
    }
    let actual = native
        .iter()
        .map(|record| {
            let mut subset = BTreeMap::from([
                ("kind", record["kind"].clone()),
                ("project_id", record["project_id"].clone()),
                ("environment", record["environment"].clone()),
                ("release", record["release"].clone()),
                ("level", record["level"].clone()),
                ("message", record["message"].clone()),
            ]);
            if !record["logger"].is_null() {
                subset.insert("logger", record["logger"].clone());
            }
            serde_json::to_string(&subset)
        })
        .collect::<serde_json::Result<Vec<_>>>()?;
    let mut expected = expected
        .into_iter()
        .map(|record| serde_json::to_string(&record))
        .collect::<serde_json::Result<Vec<_>>>()?;
    let mut actual = actual;
    expected.sort();
    actual.sort();
    ensure!(
        actual == expected,
        "native normalized subsets differ from G07 mappings"
    );

    let native_json = serde_json::to_string(native)?;
    ensure!(
        forbidden
            .iter()
            .all(|message| !native_json.contains(message)),
        "default-threshold DEBUG message was published"
    );
    Ok(())
}
