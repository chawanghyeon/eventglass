use eventglass::sentry::{
    SentryError,
    replay::{self, ReplaySegment},
};
use serde_json::{Value, json};

const PLAIN: &[u8] = include_bytes!("fixtures/replay/plain-0.envelope");
const COMPRESSED: &[u8] = include_bytes!("fixtures/replay/compressed-0.envelope");
fn decode(bytes: &[u8]) -> ReplaySegment {
    replay::decode_envelope(bytes, &[]).unwrap().unwrap()
}
fn pair(metadata: &Value, recording: &[u8]) -> Vec<u8> {
    let mut bytes = format!("{{}}\n{{\"type\":\"replay_event\"}}\n{metadata}\n{{\"type\":\"replay_recording\",\"length\":{}}}\n",recording.len()).into_bytes();
    bytes.extend(recording);
    bytes
}
fn metadata() -> Value {
    json!({"replay_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","segment_id":0,"replay_start_timestamp":1700000000,"timestamp":1700000010})
}

#[test]
fn official_sdk_plain_and_zlib_recordings_preserve_privacy() {
    for bytes in [PLAIN, COMPRESSED] {
        let segment = decode(bytes);
        assert_eq!(segment.metadata.sdk_version.as_deref(), Some("10.73.0"));
        assert_eq!(segment.metadata.segment_id, 0);
        assert_eq!(segment.metadata.user.unwrap()["id"], "replay-fixture-user");
        let text = String::from_utf8(segment.recording.clone()).unwrap();
        for secret in [
            "PRIVATE_TEXT_SENTINEL",
            "PRIVATE_PASSWORD_SENTINEL",
            "BLOCKED_SENTINEL",
        ] {
            assert!(!text.contains(secret), "{secret}");
        }
        assert!(text.contains("****"));
        let events = replay::recording_events(&segment.recording).unwrap();
        assert!(events.iter().any(|e| e["type"] == 2), "DOM snapshot");
        assert!(
            events
                .iter()
                .any(|e| e["type"] == 4 && e["data"]["width"] == 1000)
        );
        assert!(
            events
                .iter()
                .any(|e| e["type"] == 3 && e["data"]["source"] == 1),
            "movement"
        );
        assert!(
            events
                .iter()
                .any(|e| e["type"] == 3 && e["data"]["source"] == 2 && e["data"]["type"] == 2),
            "click"
        );
        assert!(!segment.metadata.trace_ids.is_empty());
    }
}

#[test]
fn official_sdk_later_segments_include_scroll_navigation_errors_network_and_dead_click() {
    let dead = decode(include_bytes!("fixtures/replay/plain-1.envelope"));
    let events = replay::recording_events(&dead.recording).unwrap();
    assert!(events.iter().any(|e| replay::frustration(e).dead == 1));
    let last = decode(include_bytes!("fixtures/replay/plain-2.envelope"));
    assert_eq!(dead.metadata.replay_id, last.metadata.replay_id);
    assert_eq!(last.metadata.segment_id, 2);
    assert_eq!(last.metadata.error_ids.len(), 1);
    assert!(!last.metadata.trace_ids.is_empty());
    assert!(last.metadata.urls.iter().any(|u| u.ends_with("/checkout")));
    let events = replay::recording_events(&last.recording).unwrap();
    assert!(
        events
            .iter()
            .any(|e| e["type"] == 3 && e["data"]["source"] == 3 && e["data"]["y"] == 1200)
    );
    assert!(
        events
            .iter()
            .any(|e| e.pointer("/data/payload/category") == Some(&json!("navigation")))
    );
    let network = events
        .iter()
        .find(|e| e.pointer("/data/payload/op") == Some(&json!("resource.fetch")))
        .unwrap();
    let ms = replay::event_timestamp_ms(network).unwrap();
    assert!(ms >= last.metadata.started_at_ms && ms <= last.metadata.finished_at_ms);
}

#[test]
fn segment_header_id_and_compression_are_validated() {
    for bytes in [
        b"{\"segment_id\":1}\n[]".as_slice(),
        b"{\"segment_id\":0}\nnot-zlib",
    ] {
        assert!(replay::decode_envelope(&pair(&metadata(), bytes), &[]).is_err());
    }
    assert!(replay::decode_envelope(b"{}\n{\"type\":\"replay_event\"}\n{}", &[]).is_err());
    assert!(
        replay::decode_envelope(b"{}\n{\"type\":\"transaction\"}\n{}", &[])
            .unwrap()
            .is_none()
    );
}

#[test]
fn compressed_bomb_is_rejected_before_json_allocation() {
    use std::io::Write;
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder
        .write_all(&vec![b' '; replay::MAX_RECORDING_BYTES + 1])
        .unwrap();
    let mut body = b"{\"segment_id\":0}\n".to_vec();
    body.extend(encoder.finish().unwrap());
    assert!(matches!(
        replay::decode_envelope(&pair(&metadata(), &body), &[]),
        Err(SentryError::TooLarge(_))
    ));
}

#[test]
fn known_secrets_scrubbed_without_unmasking_recording() {
    let event = json!({"type":5,"timestamp":1700000000000u64,"data":{"tag":"breadcrumb","payload":{"message":"****","data":{"password":"secret-value","custom_secret":"custom-value"}}}}); // pragma: allowlist secret (synthetic privacy sentinels)
    let body = format!("{{\"segment_id\":0}}\n[{event}]");
    let segment = replay::decode_envelope(
        &pair(&metadata(), body.as_bytes()),
        &["custom_secret".into()],
    )
    .unwrap()
    .unwrap();
    let text = String::from_utf8(segment.recording).unwrap();
    assert!(!text.contains("secret-value"));
    assert!(!text.contains("custom-value"));
    assert!(text.contains("****"));
}

#[test]
fn frustration_matches_sentry_predicates_without_guessing() {
    let mut event = json!({"type":5,"timestamp":1700000000000u64,"data":{"tag":"breadcrumb","payload":{"category":"ui.slowClickDetected","data":{"endReason":"timeout","node":{"tagName":"BUTTON"},"clickCount":5}}}});
    assert_eq!(
        replay::frustration(&event),
        replay::Frustration {
            slow: 1,
            dead: 1,
            rage: 1,
            multi: 0
        }
    );
    event["data"]["payload"]["data"]["endReason"] = json!("mutation");
    assert_eq!(replay::frustration(&event).rage, 0);
    event["data"]["payload"]["category"] = json!("ui.multiClick");
    assert_eq!(
        replay::frustration(&event),
        replay::Frustration {
            multi: 1,
            rage: 1,
            ..Default::default()
        }
    );
}
