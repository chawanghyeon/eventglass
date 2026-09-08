use std::{fs, io::Write, path::PathBuf};

use eventglass::{
    config::Limits,
    model::RecordKind,
    sentry::{
        ContentEncoding, ProjectContext, SentryError, decode_body, envelope_auth,
        normalize_envelope, normalize_store,
    },
};
use flate2::{
    Compression,
    write::{GzEncoder, ZlibEncoder},
};
use serde_json::{Value, json};
use uuid::Uuid;

const RECEIVED_AT_US: i64 = 1_767_323_045_000_000;

fn project() -> ProjectContext {
    ProjectContext {
        project_id: 1,
        slug: "fixture-project".to_owned(),
        public_key: "fixturePublicKey".to_owned(),
        scrub_keys: vec!["tenant_secret".to_owned()],
    }
}

fn fixture_dir(case: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/sentry")
        .join(case)
}

fn fixture_records(case: &str) -> Vec<eventglass::model::Record> {
    let root = fixture_dir(case);
    let metadata: Value =
        serde_json::from_slice(&fs::read(root.join("metadata.json")).unwrap()).unwrap();
    let mut records = Vec::new();
    for (ordinal, request) in metadata["requests"].as_array().unwrap().iter().enumerate() {
        let wire = fs::read(root.join(request["file"].as_str().unwrap())).unwrap();
        let encoding =
            ContentEncoding::parse(Some(request["content_encoding"].as_str().unwrap())).unwrap();
        let decoded = decode_body(&wire, encoding, &Limits::default()).unwrap();
        let normalized = normalize_envelope(
            &decoded,
            &project(),
            Uuid::from_u128((ordinal + 1) as u128),
            RECEIVED_AT_US,
            &Limits::default(),
        )
        .unwrap();
        let expected_unsupported = request["item_types"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|kind| !matches!(kind.as_str(), Some("event" | "log")))
            .count();
        assert_eq!(normalized.unsupported_items, expected_unsupported);
        if let Some(auth) = normalized.envelope_auth {
            assert_eq!(auth.project_id, 1);
            assert_eq!(auth.public_key, "fixturePublicKey");
        }
        records.extend(normalized.records);
    }
    assert_fixture_mapping(&root, &records);
    records
}

fn assert_fixture_mapping(root: &std::path::Path, records: &[eventglass::model::Record]) {
    let expected: Value =
        serde_json::from_slice(&fs::read(root.join("expected.normalized.json")).unwrap()).unwrap();
    assert_eq!(expected["server_normalization_executed"], false);
    assert_eq!(expected["assertion"], "unordered_record_subsets");
    let mut actual: Vec<Value> = records
        .iter()
        .map(|record| serde_json::to_value(record).unwrap())
        .collect();
    for subset in expected["records"].as_array().unwrap() {
        let subset = subset.as_object().unwrap();
        let index = actual
            .iter()
            .position(|record| {
                subset
                    .iter()
                    .all(|(key, value)| record.get(key) == Some(value))
            })
            .unwrap_or_else(|| panic!("no normalized Record matched fixture subset {subset:?}"));
        actual.swap_remove(index);
    }
    assert!(
        actual.is_empty(),
        "fixture produced unmapped records: {actual:?}"
    );
}

#[test]
fn every_supported_sdk_fixture_normalizes() {
    let python_events = fixture_records("python-events");
    assert_eq!(python_events.len(), 2);
    assert!(
        python_events
            .iter()
            .all(|record| record.kind == RecordKind::Error)
    );
    assert_eq!(
        messages(&python_events),
        ["python fixture exception", "python fixture message"]
    );

    let default_logs = fixture_records("python-logging-default");
    assert_eq!(default_logs.len(), 6);
    assert!(
        !default_logs
            .iter()
            .any(|record| record.message.contains("debug omitted"))
    );
    assert_eq!(
        default_logs
            .iter()
            .filter(|record| record.kind == RecordKind::Log)
            .count(),
        4
    );
    assert_eq!(
        default_logs
            .iter()
            .filter(|record| record.kind == RecordKind::Error)
            .count(),
        2
    );
    for message in ["python default error", "python default critical"] {
        let kinds: Vec<RecordKind> = default_logs
            .iter()
            .filter(|record| record.message == message)
            .map(|record| record.kind)
            .collect();
        assert_eq!(kinds.len(), 2);
        assert!(kinds.contains(&RecordKind::Log));
        assert!(kinds.contains(&RecordKind::Error));
    }

    let debug_logs = fixture_records("python-logging-debug");
    assert_eq!(debug_logs.len(), 1);
    assert_eq!(debug_logs[0].level, "debug");
    assert_eq!(debug_logs[0].message, "python opt-in debug");

    let node = fixture_records("node-events-and-logs");
    assert_eq!(node.len(), 8);
    assert_eq!(
        node.iter()
            .filter(|record| record.kind == RecordKind::Error)
            .count(),
        2
    );
    assert_eq!(
        node.iter()
            .filter(|record| record.kind == RecordKind::Log)
            .map(|record| record.level.as_str())
            .collect::<Vec<_>>(),
        ["trace", "debug", "info", "warning", "error", "fatal"]
    );

    for (case, message) in [
        ("python-fastapi", "fastapi fixture exception"),
        ("python-celery-fork", "celery fixture exception"),
    ] {
        let records = fixture_records(case);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].kind, RecordKind::Error);
        assert_eq!(records[0].message, message);
    }

    let browser = fixture_records("browser-events-and-console");
    assert_eq!(browser.len(), 3);
    assert_eq!(
        browser
            .iter()
            .filter(|record| record.kind == RecordKind::Error)
            .count(),
        2
    );
    assert_eq!(
        browser
            .iter()
            .filter(|record| record.kind == RecordKind::Log)
            .map(|record| record.message.as_str())
            .collect::<Vec<_>>(),
        ["browser fixture console info"]
    );

    let go = fixture_records("go-events");
    assert_eq!(go.len(), 2);
    assert!(go.iter().all(|record| record.kind == RecordKind::Error));
}

#[test]
fn envelope_uses_byte_lengths_for_unicode_and_binary_unsupported_items() {
    let event = serde_json::to_vec(&json!({
        "event_id": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "timestamp": "2026-01-02T03:04:05Z",
        "message": "안녕하세요 👋",
        "level": "warning"
    }))
    .unwrap();
    let binary = b"\0\xff\nnot-an-item-header\n";
    let body = envelope(
        &json!({}),
        &[
            ("attachment", binary.as_slice()),
            ("event", event.as_slice()),
        ],
        false,
    );
    let result = normalize_envelope(
        &body,
        &project(),
        Uuid::from_u128(1),
        RECEIVED_AT_US,
        &Limits::default(),
    )
    .unwrap();
    assert_eq!(result.unsupported_items, 1);
    assert_eq!(result.records.len(), 1);
    assert_eq!(result.records[0].message, "안녕하세요 👋");
    assert_eq!(
        result.records[0].source_event_id.as_deref(),
        Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    );
}

#[test]
fn malformed_supported_item_rejects_the_whole_request() {
    let valid = serde_json::to_vec(&json!({"message":"valid","level":"error"})).unwrap();
    let body = envelope(
        &json!({}),
        &[("event", valid.as_slice()), ("log", b"{not json")],
        true,
    );
    assert!(matches!(
        normalize_envelope(
            &body,
            &project(),
            Uuid::from_u128(1),
            RECEIVED_AT_US,
            &Limits::default()
        ),
        Err(SentryError::Malformed(_))
    ));

    let bad_length = b"{}\n{\"type\":\"event\",\"length\":99}\n{}".to_vec();
    assert!(
        normalize_envelope(
            &bad_length,
            &project(),
            Uuid::from_u128(1),
            RECEIVED_AT_US,
            &Limits::default()
        )
        .is_err()
    );
}

#[test]
fn envelope_dsn_is_exposed_and_conflicts_are_rejected() {
    let event = serde_json::to_vec(&json!({"message":"dsn","level":"error"})).unwrap();
    let body = envelope(
        &json!({"dsn":"http://fixturePublicKey@localhost:8123/1"}),
        &[("event", event.as_slice())],
        true,
    );
    let normalized = normalize_envelope(
        &body,
        &project(),
        Uuid::from_u128(1),
        RECEIVED_AT_US,
        &Limits::default(),
    )
    .unwrap();
    let auth = normalized.envelope_auth.unwrap();
    assert_eq!(auth.project_id, 1);
    assert_eq!(auth.public_key, "fixturePublicKey");
    assert_eq!(envelope_auth(&body).unwrap().unwrap(), auth);

    let conflict = envelope(
        &json!({"dsn":"http://different-key@localhost:8123/1"}),
        &[("event", event.as_slice())],
        true,
    );
    assert!(matches!(
        normalize_envelope(
            &conflict,
            &project(),
            Uuid::from_u128(1),
            RECEIVED_AT_US,
            &Limits::default()
        ),
        Err(SentryError::Malformed(_))
    ));

    for invalid in [
        "ftp://fixturePublicKey@localhost/1",
        "http://fixturePublicKey:password@localhost/1", // pragma: allowlist secret -- synthetic test fixture or golden identity
        "http://fixturePublicKey@localhost/0",
        "http://fixturePublicKey@localhost/1?key=value",
        "http://fixturePublicKey@localhost/1#fragment",
    ] {
        let body = envelope(
            &json!({"dsn":invalid}),
            &[("event", event.as_slice())],
            true,
        );
        assert!(matches!(
            envelope_auth(&body),
            Err(SentryError::Malformed(_))
        ));
    }
}

#[test]
fn store_event_and_record_ids_are_stable_golden_vectors() {
    let body = serde_json::to_vec(&json!({
        "event_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "message":"store event",
        "level":"error"
    }))
    .unwrap();
    let record = normalize_store(
        &body,
        &project(),
        Uuid::from_u128(1),
        RECEIVED_AT_US,
        &Limits::default(),
    )
    .unwrap()
    .records
    .remove(0);
    assert_eq!(
        record.record_id,
        "06dac01611efb2ec25740b3b8cb6a6da21fea507e8d48e665402da78759281fc" // pragma: allowlist secret -- synthetic test fixture or golden identity
    );

    let no_id = serde_json::to_vec(&json!({"message":"accepted","level":"error"})).unwrap();
    let record = normalize_store(
        &no_id,
        &project(),
        Uuid::from_u128(1),
        RECEIVED_AT_US,
        &Limits::default(),
    )
    .unwrap()
    .records
    .remove(0);
    assert_eq!(
        record.record_id,
        "cef18a16bbabb76e5e31b45453ca73bb900c27016a853087297082caedab3fca" // pragma: allowlist secret -- synthetic test fixture or golden identity
    );
}

#[test]
fn scrub_happens_before_raw_attributes_search_and_fingerprint() {
    let secret = "TOP_SECRET_SENTINEL"; // pragma: allowlist secret -- synthetic test fixture or golden identity
    let body = serde_json::to_vec(&json!({
        "message":"safe message",
        "level":"error",
        "request": {
            "url": format!("https://example.invalid/pay?access_token={secret}&safe=ok"),
            "headers": [["Authorization", secret], ["X-Safe", "visible"]]
        },
        "extra": {"nested":{"password":secret}, "tenant_secret":secret},
        "tags": {"api_key":secret},
        "exception": {"values":[{
            "type":"FixtureError",
            "value":"safe message",
            "stacktrace":{"frames":[{"in_app":true,"function":"safe_function","filename":"fixture.rs"}]}
        }]}
    }))
    .unwrap();
    let first = normalize_store(
        &body,
        &project(),
        Uuid::from_u128(1),
        RECEIVED_AT_US,
        &Limits::default(),
    )
    .unwrap()
    .records
    .remove(0);
    let serialized = serde_json::to_string(&first).unwrap();
    assert!(!serialized.contains(secret));
    assert!(serialized.contains("[Filtered]"));

    let log_payload = serde_json::to_vec(&json!({"version":2,"items":[{
        "timestamp":1767323045.0,
        "level":"error",
        "body":"safe log",
        "attributes":{
            "api_key":{"value":secret,"type":"string"},
            "http.request.url":{"value":format!("https://example.invalid/?password={secret}"),"type":"string"}
        }
    }]}))
    .unwrap();
    let log_envelope = envelope(&json!({}), &[("log", log_payload.as_slice())], true);
    let log = normalize_envelope(
        &log_envelope,
        &project(),
        Uuid::from_u128(3),
        RECEIVED_AT_US,
        &Limits::default(),
    )
    .unwrap()
    .records
    .remove(0);
    let serialized_log = serde_json::to_string(&log).unwrap();
    assert!(!serialized_log.contains(secret));
    assert_eq!(log.attributes["api_key"], "[Filtered]");

    let other = serde_json::to_vec(&json!({
        "message":"safe message",
        "level":"error",
        "exception": {"values":[{
            "type":"FixtureError",
            "value":"safe message",
            "stacktrace":{"frames":[{"in_app":true,"function":"ANOTHER_SECRET","filename":"fixture.rs"}]}
        }]}
    }))
    .unwrap();
    let mut custom_project = project();
    custom_project.scrub_keys.push("function".to_owned());
    let first_with_custom = normalize_store(
        &body,
        &custom_project,
        Uuid::from_u128(1),
        RECEIVED_AT_US,
        &Limits::default(),
    )
    .unwrap()
    .records
    .remove(0);
    let second_with_custom = normalize_store(
        &other,
        &custom_project,
        Uuid::from_u128(2),
        RECEIVED_AT_US,
        &Limits::default(),
    )
    .unwrap()
    .records
    .remove(0);
    assert_eq!(
        first_with_custom.fingerprint,
        second_with_custom.fingerprint
    );
}

#[test]
fn fingerprint_uses_in_app_frames_and_normalizes_dynamic_tokens() {
    let make_event = |status: u16, dynamic: &str, outer: &str, inner: &str| {
        serde_json::to_vec(&json!({
            "message":format!("HTTP {status} request {dynamic}"),
            "level":"error",
            "exception":{"values":[{
                "type":"HttpError",
                "value":format!("HTTP {status} request {dynamic}"),
                "stacktrace":{"frames":[
                    {"in_app":false,"function":outer,"filename":"runtime.rs","lineno":10},
                    {"in_app":true,"function":inner,"filename":"app.rs","lineno":20}
                ]}
            }]}
        }))
        .unwrap()
    };
    let normalize = |body: Vec<u8>| {
        normalize_store(
            &body,
            &project(),
            Uuid::from_u128(1),
            RECEIVED_AT_US,
            &Limits::default(),
        )
        .unwrap()
        .records
        .remove(0)
        .fingerprint
        .unwrap()
    };
    let first = normalize(make_event(
        404,
        "550e8400-e29b-41d4-a716-446655440000",
        "outer-a",
        "handler",
    ));
    let same = normalize(make_event(
        404,
        "123e4567-e89b-12d3-a456-426614174000",
        "outer-b",
        "handler",
    ));
    let different_status = normalize(make_event(
        500,
        "123e4567-e89b-12d3-a456-426614174000",
        "outer-a",
        "handler",
    ));
    let different_frame = normalize(make_event(
        404,
        "123e4567-e89b-12d3-a456-426614174000",
        "outer-a",
        "other_handler",
    ));
    assert_eq!(first, same);
    assert_ne!(first, different_status);
    assert_ne!(first, different_frame);
}

#[test]
fn explicit_fingerprint_expands_default_and_rejects_invalid_tokens() {
    let event = |fingerprint: Value| {
        serde_json::to_vec(&json!({
            "message":"fixture 123456",
            "level":"error",
            "fingerprint":fingerprint,
            "exception":{"values":[{"type":"FixtureError","value":"fixture 123456"}]}
        }))
        .unwrap()
    };
    let normalize = |body: Vec<u8>| {
        normalize_store(
            &body,
            &project(),
            Uuid::from_u128(1),
            RECEIVED_AT_US,
            &Limits::default(),
        )
    };
    let default = normalize(event(json!([])))
        .unwrap()
        .records
        .remove(0)
        .fingerprint;
    let expanded = normalize(event(json!(["tenant", "{{ default }}"])))
        .unwrap()
        .records
        .remove(0)
        .fingerprint;
    let explicit_only = normalize(event(json!(["tenant"])))
        .unwrap()
        .records
        .remove(0)
        .fingerprint;
    assert_ne!(default, expanded);
    assert_ne!(expanded, explicit_only);
    assert!(matches!(
        normalize(event(json!(["valid", 1]))),
        Err(SentryError::Malformed(_))
    ));
}

#[test]
fn json_projection_record_count_and_timestamp_limits_are_enforced() {
    let mut deep = json!("leaf");
    for _ in 0..64 {
        deep = json!([deep]);
    }
    let deep_body = serde_json::to_vec(&json!({"message":"deep","extra":deep})).unwrap();
    assert!(matches!(
        normalize_store(
            &deep_body,
            &project(),
            Uuid::from_u128(1),
            RECEIVED_AT_US,
            &Limits::default()
        ),
        Err(SentryError::TooLarge(_))
    ));

    let mut projected_deep = json!("deep searchable value");
    for _ in 0..17 {
        projected_deep = json!([projected_deep]);
    }
    let projection_body =
        serde_json::to_vec(&json!({"message":"projection","extra":projected_deep})).unwrap();
    let record = normalize_store(
        &projection_body,
        &project(),
        Uuid::from_u128(1),
        RECEIVED_AT_US,
        &Limits::default(),
    )
    .unwrap()
    .records
    .remove(0);
    assert!(
        record
            .indexing_warnings
            .contains(&"search_depth_truncated".to_owned())
    );
    assert!(!record.search_text.contains("deep searchable value"));

    let scalar_body =
        serde_json::to_vec(&json!({"message":"scalars","extra":vec![1; 1_001]})).unwrap();
    let record = normalize_store(
        &scalar_body,
        &project(),
        Uuid::from_u128(1),
        RECEIVED_AT_US,
        &Limits::default(),
    )
    .unwrap()
    .records
    .remove(0);
    assert!(
        record
            .indexing_warnings
            .contains(&"search_scalar_truncated".to_owned())
    );

    let nodes_body =
        serde_json::to_vec(&json!({"message":"nodes","extra":vec![0; 20_000]})).unwrap();
    assert!(matches!(
        normalize_store(
            &nodes_body,
            &project(),
            Uuid::from_u128(1),
            RECEIVED_AT_US,
            &Limits::default()
        ),
        Err(SentryError::TooLarge(_))
    ));

    let long = "가".repeat(30_000);
    let long_body = serde_json::to_vec(&json!({"message":long})).unwrap();
    let record = normalize_store(
        &long_body,
        &project(),
        Uuid::from_u128(1),
        RECEIVED_AT_US,
        &Limits::default(),
    )
    .unwrap()
    .records
    .remove(0);
    assert!(record.search_text.len() <= 64 * 1024);
    assert!(
        record
            .search_text
            .is_char_boundary(record.search_text.len())
    );
    assert!(
        record
            .indexing_warnings
            .contains(&"search_text_truncated".to_owned())
    );

    let invalid_time = serde_json::to_vec(&json!({"message":"time","timestamp":9.3e9})).unwrap();
    let record = normalize_store(
        &invalid_time,
        &project(),
        Uuid::from_u128(1),
        RECEIVED_AT_US,
        &Limits::default(),
    )
    .unwrap()
    .records
    .remove(0);
    assert_eq!(record.timestamp_us, RECEIVED_AT_US);
    assert!(
        record
            .indexing_warnings
            .contains(&"timestamp_fallback".to_owned())
    );
    assert!(
        normalize_store(
            &invalid_time,
            &project(),
            Uuid::from_u128(1),
            i64::MAX,
            &Limits::default()
        )
        .is_err()
    );

    let malformed_id =
        serde_json::to_vec(&json!({"event_id":"not-an-id","message":"bad"})).unwrap();
    assert!(matches!(
        normalize_store(
            &malformed_id,
            &project(),
            Uuid::from_u128(1),
            RECEIVED_AT_US,
            &Limits::default()
        ),
        Err(SentryError::Malformed(_))
    ));

    let logs = serde_json::to_vec(&json!({"version":2,"items":[
        {"timestamp":1767323045.0,"level":"info","body":"one"},
        {"timestamp":1767323045.0,"level":"info","body":"two"}
    ]}))
    .unwrap();
    let body = envelope(&json!({}), &[("log", logs.as_slice())], true);
    let limits = Limits {
        request_records: 1,
        ..Limits::default()
    };
    assert!(matches!(
        normalize_envelope(
            &body,
            &project(),
            Uuid::from_u128(1),
            RECEIVED_AT_US,
            &limits
        ),
        Err(SentryError::TooLarge(_))
    ));
}

#[test]
fn wire_and_normalized_size_limits_are_enforced() {
    let raw = b"repeated decoded bytes repeated decoded bytes".repeat(1_000);
    let mut gzip = GzEncoder::new(Vec::new(), Compression::default());
    gzip.write_all(&raw).unwrap();
    let gzip = gzip.finish().unwrap();
    let mut deflate = ZlibEncoder::new(Vec::new(), Compression::default());
    deflate.write_all(&raw).unwrap();
    let deflate = deflate.finish().unwrap();

    let mut limits = Limits {
        decoded_bytes: raw.len(),
        ..Limits::default()
    };
    assert_eq!(
        decode_body(&gzip, ContentEncoding::Gzip, &limits).unwrap(),
        raw
    );
    assert_eq!(
        decode_body(&deflate, ContentEncoding::Deflate, &limits).unwrap(),
        raw
    );
    limits.decoded_bytes -= 1;
    assert!(matches!(
        decode_body(&gzip, ContentEncoding::Gzip, &limits),
        Err(SentryError::TooLarge(_))
    ));
    assert!(decode_body(b"not gzip", ContentEncoding::Gzip, &Limits::default()).is_err());

    let event = serde_json::to_vec(&json!({"message":"output duplicates this text","extra":{"visible":"output duplicates this text"}})).unwrap();
    let tight = Limits {
        decoded_bytes: event.len() + 10,
        ..Limits::default()
    };
    assert!(event.len() <= tight.decoded_bytes);
    assert!(matches!(
        normalize_store(
            &event,
            &project(),
            Uuid::from_u128(1),
            RECEIVED_AT_US,
            &tight
        ),
        Err(SentryError::TooLarge(_))
    ));
    let record_limit = Limits {
        record_bytes: 100,
        ..Limits::default()
    };
    assert!(matches!(
        normalize_store(
            &event,
            &project(),
            Uuid::from_u128(1),
            RECEIVED_AT_US,
            &record_limit
        ),
        Err(SentryError::TooLarge(_))
    ));
}

fn messages(records: &[eventglass::model::Record]) -> Vec<&str> {
    let mut messages: Vec<&str> = records
        .iter()
        .map(|record| record.message.as_str())
        .collect();
    messages.sort_unstable();
    messages
}

fn envelope(header: &Value, items: &[(&str, &[u8])], final_newline: bool) -> Vec<u8> {
    let mut output = serde_json::to_vec(header).unwrap();
    output.push(b'\n');
    for (index, (kind, payload)) in items.iter().enumerate() {
        output.extend_from_slice(
            serde_json::to_string(&json!({"type":kind,"length":payload.len()}))
                .unwrap()
                .as_bytes(),
        );
        output.push(b'\n');
        output.extend_from_slice(payload);
        if index + 1 < items.len() || final_newline {
            output.push(b'\n');
        }
    }
    output
}
