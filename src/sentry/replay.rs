//! Official Sentry Replay v10 envelope/rrweb decoding; no client instrumentation.
//! Wire evidence and compatibility boundaries: docs/observe/replay.md.

use std::io::Read;

use flate2::read::ZlibDecoder;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{SentryError, envelope, json, scrub};

pub const MAX_RECORDING_BYTES: usize = 20 * 1024 * 1024;
const MAX_RECORDING_NODES: usize = 200_000;
pub const MAX_SEGMENT_ID: u64 = 10_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplayMetadata {
    pub replay_id: String,
    pub segment_id: u64,
    pub started_at_ms: i64,
    pub finished_at_ms: i64,
    pub user: Option<Value>,
    pub environment: Option<String>,
    pub release: Option<String>,
    pub browser: Option<Value>,
    pub os: Option<Value>,
    pub device: Option<Value>,
    pub urls: Vec<String>,
    pub error_ids: Vec<String>,
    pub trace_ids: Vec<String>,
    pub sdk_version: Option<String>,
    pub replay_type: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ReplaySegment {
    pub metadata: ReplayMetadata,
    /// Canonical, scrubbed rrweb JSON array. No second copy of raw envelope is stored.
    pub recording: Vec<u8>,
}

/// The official browser SDK sends one replay_event and one replay_recording per envelope.
/// Unknown item types remain the existing normalizer's responsibility.
pub fn decode_envelope(
    input: &[u8],
    scrub_keys: &[String],
) -> Result<Option<ReplaySegment>, SentryError> {
    let envelope = envelope::parse(input, 10_000)?;
    let mut metadata = None;
    let mut recording = None;
    for item in envelope.items {
        let target = match item.kind.as_str() {
            "replay_event" => &mut metadata,
            "replay_recording" => &mut recording,
            _ => continue,
        };
        if target.replace(item.payload).is_some() {
            return Err(SentryError::Malformed(
                "multiple replay pairs in one envelope",
            ));
        }
    }
    match (metadata, recording) {
        (None, None) => Ok(None),
        (Some(metadata), Some(recording)) => {
            let mut raw = json::parse(metadata)?;
            scrub::scrub(&mut raw, scrub_keys);
            let metadata = metadata_from_value(&raw)?;
            if let Some(id) = envelope.header.get("event_id")
                && canonical_id(id)? != metadata.replay_id
            {
                return Err(SentryError::Malformed("envelope replay id mismatch"));
            }
            let newline = recording
                .iter()
                .position(|b| *b == b'\n')
                .filter(|n| *n <= 64 * 1024)
                .ok_or(SentryError::Malformed("missing recording header"))?;
            let header = json::parse(&recording[..newline])?;
            if header.get("segment_id").and_then(Value::as_u64) != Some(metadata.segment_id) {
                return Err(SentryError::Malformed("recording segment id mismatch"));
            }
            let bytes = &recording[newline + 1..];
            let decoded;
            let bytes = if bytes.iter().find(|b| !b.is_ascii_whitespace()) == Some(&b'[') {
                bytes
            } else {
                let mut decoder = ZlibDecoder::new(bytes);
                let mut result = Vec::new();
                (&mut decoder)
                    .take((MAX_RECORDING_BYTES + 1) as u64)
                    .read_to_end(&mut result)
                    .map_err(|_| SentryError::Malformed("invalid zlib recording"))?;
                if result.len() > MAX_RECORDING_BYTES {
                    return Err(SentryError::TooLarge(
                        "recording decoded bytes exceed limit",
                    ));
                }
                if decoder.total_in() != bytes.len() as u64 {
                    return Err(SentryError::Malformed(
                        "trailing recording compression data",
                    ));
                }
                decoded = result;
                &decoded
            };
            let mut events = recording_events(bytes)?;
            for event in &mut events {
                // Retains SDK masking/blocking and applies the project's existing secret policy.
                scrub::scrub(event, scrub_keys);
            }
            let recording = serde_json::to_vec(&events)
                .map_err(|_| SentryError::Malformed("invalid recording"))?;
            if recording.len() > MAX_RECORDING_BYTES {
                return Err(SentryError::TooLarge("normalized recording exceeds limit"));
            }
            Ok(Some(ReplaySegment {
                metadata,
                recording,
            }))
        }
        _ => Err(SentryError::Malformed(
            "replay requires metadata and recording",
        )),
    }
}

pub fn recording_events(bytes: &[u8]) -> Result<Vec<Value>, SentryError> {
    if bytes.len() > MAX_RECORDING_BYTES {
        return Err(SentryError::TooLarge("recording exceeds limit"));
    }
    let value = json::parse_with_limit(bytes, MAX_RECORDING_NODES)?;
    let Value::Array(events) = value else {
        return Err(SentryError::Malformed("recording must be an event array"));
    };
    for event in &events {
        if !event.is_object()
            || event.get("type").and_then(Value::as_u64).is_none()
            || event_timestamp_ms(event).is_none()
            || !event.get("data").is_some_and(Value::is_object)
        {
            return Err(SentryError::Malformed("invalid rrweb event"));
        }
    }
    Ok(events)
}

fn metadata_from_value(raw: &Value) -> Result<ReplayMetadata, SentryError> {
    let replay_id = canonical_id(&raw["replay_id"])?;
    if let Some(id) = raw.get("event_id")
        && canonical_id(id)? != replay_id
    {
        return Err(SentryError::Malformed("replay event id mismatch"));
    }
    let segment_id = raw["segment_id"]
        .as_u64()
        .filter(|n| *n <= MAX_SEGMENT_ID)
        .ok_or(SentryError::Malformed("invalid replay segment id"))?;
    let started_at_ms = seconds_ms(&raw["replay_start_timestamp"])
        .ok_or(SentryError::Malformed("invalid replay start timestamp"))?;
    let finished_at_ms =
        seconds_ms(&raw["timestamp"]).ok_or(SentryError::Malformed("invalid replay timestamp"))?;
    if finished_at_ms < started_at_ms || finished_at_ms - started_at_ms > 24 * 60 * 60 * 1000 {
        return Err(SentryError::Malformed("invalid replay time interval"));
    }
    let ids = |key: &str| -> Result<Vec<String>, SentryError> {
        match raw.get(key) {
            None => Ok(Vec::new()),
            Some(Value::Array(values)) if values.len() <= 1000 => {
                values.iter().map(canonical_id).collect()
            }
            _ => Err(SentryError::Malformed("invalid replay association ids")),
        }
    };
    let urls = match raw.get("urls") {
        None => Vec::new(),
        Some(Value::Array(values)) if values.len() <= 1000 => values
            .iter()
            .map(|v| {
                v.as_str()
                    .filter(|v| v.len() <= 4096)
                    .map(str::to_owned)
                    .ok_or(SentryError::Malformed("invalid replay URL"))
            })
            .collect::<Result<Vec<_>, _>>()?,
        _ => return Err(SentryError::Malformed("invalid replay URLs")),
    };
    Ok(ReplayMetadata {
        replay_id,
        segment_id,
        started_at_ms,
        finished_at_ms,
        user: raw.get("user").filter(|v| v.is_object()).cloned(),
        environment: string(raw.get("environment")),
        release: string(raw.get("release")),
        browser: raw
            .pointer("/contexts/browser")
            .filter(|v| v.is_object())
            .cloned(),
        os: raw
            .pointer("/contexts/os")
            .filter(|v| v.is_object())
            .cloned(),
        device: raw
            .pointer("/contexts/device")
            .filter(|v| v.is_object())
            .cloned(),
        urls,
        error_ids: ids("error_ids")?,
        trace_ids: ids("trace_ids")?,
        sdk_version: string(raw.pointer("/sdk/version")),
        replay_type: string(raw.get("replay_type")),
    })
}

pub fn canonical_id(value: &Value) -> Result<String, SentryError> {
    value
        .as_str()
        .filter(|s| s.len() == 32 && s.bytes().all(|c| c.is_ascii_hexdigit()))
        .map(str::to_ascii_lowercase)
        .ok_or(SentryError::Malformed("invalid Sentry id"))
}

fn string(value: Option<&Value>) -> Option<String> {
    value.and_then(Value::as_str).map(str::to_owned)
}

fn seconds_ms(value: &Value) -> Option<i64> {
    if let Some(seconds) = value.as_f64() {
        return finite_ms(seconds * 1000.0);
    }
    let timestamp = time::OffsetDateTime::parse(
        value.as_str()?,
        &time::format_description::well_known::Rfc3339,
    )
    .ok()?;
    i64::try_from(timestamp.unix_timestamp_nanos() / 1_000_000)
        .ok()
        .filter(|v| *v >= 0)
}

fn finite_ms(value: f64) -> Option<i64> {
    (value.is_finite() && (0.0..=253_402_300_799_000.0).contains(&value)).then_some(value as i64)
}

/// rrweb timestamps are milliseconds; SDK performanceSpan custom events use seconds.
/// Do not apply a heuristic to regular rrweb timestamps.
pub fn event_timestamp_ms(event: &Value) -> Option<i64> {
    if event.pointer("/data/tag").and_then(Value::as_str) == Some("performanceSpan") {
        seconds_ms(event.pointer("/data/payload/startTimestamp")?)
    } else {
        finite_ms(event.get("timestamp")?.as_f64()?)
    }
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct Frustration {
    pub slow: u32,
    pub dead: u32,
    pub rage: u32,
    pub multi: u32,
}

/// Matches Sentry's isDeadClick/isDeadRageClick/isRageClick predicates, not a new UX score.
pub fn frustration(event: &Value) -> Frustration {
    let mut result = Frustration::default();
    if event.pointer("/data/tag").and_then(Value::as_str) != Some("breadcrumb") {
        return result;
    }
    let payload = &event["data"]["payload"];
    let data = &payload["data"];
    let count = data["clickCount"].as_u64().unwrap_or(0);
    match payload["category"].as_str() {
        Some("ui.slowClickDetected") => {
            result.slow = 1;
            if data["endReason"] == "timeout"
                && data["node"]["tagName"].as_str().is_some_and(|tag| {
                    matches!(tag.to_ascii_lowercase().as_str(), "a" | "button" | "input")
                })
            {
                result.dead = 1;
                result.rage = u32::from(count >= 5);
            }
        }
        Some("ui.multiClick") => {
            result.multi = 1;
            result.rage = u32::from(count >= 5);
        }
        _ => {}
    }
    result
}
