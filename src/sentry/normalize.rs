use std::collections::BTreeMap;

use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Value};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::{
    config::Limits,
    model::{Record, RecordKind},
};

use super::{EnvelopeAuth, NormalizedRequest, ProjectContext, SentryError, identity, scrub};

const MAX_SEARCH_DEPTH: usize = 16;
const MAX_SEARCH_SCALARS: usize = 1_000;
const MAX_SEARCH_BYTES: usize = 64 * 1024;
const NORMALIZER_VERSION: u32 = 1;
const FINGERPRINT_VERSION: u32 = 1;

pub(super) struct RequestNormalizer<'a> {
    project: &'a ProjectContext,
    acceptance_id: Uuid,
    received_at_us: i64,
    limits: &'a Limits,
    records: Vec<Record>,
    normalized_bytes: usize,
    unsupported_items: usize,
}

impl<'a> RequestNormalizer<'a> {
    pub(super) fn new(
        project: &'a ProjectContext,
        acceptance_id: Uuid,
        received_at_us: i64,
        limits: &'a Limits,
    ) -> Self {
        Self {
            project,
            acceptance_id,
            received_at_us,
            limits,
            records: Vec::new(),
            normalized_bytes: 2,
            unsupported_items: 0,
        }
    }

    pub(super) fn event(
        &mut self,
        payload: &[u8],
        item_ordinal: usize,
        record_ordinal: usize,
    ) -> Result<(), SentryError> {
        let mut raw = super::json::parse(payload)?;
        if !raw.is_object() {
            return Err(SentryError::Malformed("event payload must be an object"));
        }
        scrub::scrub(&mut raw, &self.project.scrub_keys);
        let record = normalize_event(
            raw,
            self.project,
            self.acceptance_id,
            self.received_at_us,
            item_ordinal,
            record_ordinal,
        )?;
        self.push(record)
    }

    // Transactions use the existing append-only log path. Keep nested spans in
    // the scrubbed raw transaction once; never create Issues for performance data.
    pub(super) fn transaction(
        &mut self,
        payload: &[u8],
        item_ordinal: usize,
    ) -> Result<(), SentryError> {
        let mut raw = super::json::parse(payload)?;
        if !raw.is_object() {
            return Err(SentryError::Malformed(
                "transaction payload must be an object",
            ));
        }
        scrub::scrub(&mut raw, &self.project.scrub_keys);
        let mut record = normalize_event(
            raw,
            self.project,
            self.acceptance_id,
            self.received_at_us,
            item_ordinal,
            0,
        )?;
        record.kind = RecordKind::Log;
        record.record_id = identity::accepted_record_id(
            self.project.project_id,
            self.acceptance_id,
            item_ordinal,
            0,
        );
        record.source_event_id = None;
        record.issue_id = None;
        record.fingerprint = None;
        record.fingerprint_version = None;
        if !record.raw_json.get("level").is_some_and(Value::is_string) {
            record.level = "info".to_owned();
        }
        record.message = record
            .raw_json
            .get("transaction")
            .and_then(Value::as_str)
            .unwrap_or("transaction")
            .to_owned();
        record.search_text = event_search_projection(
            &record.message,
            &record.raw_json,
            &mut record.indexing_warnings,
        );
        record.attributes["sentry_type"] = Value::String("transaction".to_owned());
        if let Some(event_id) = record.raw_json.get("event_id") {
            record.attributes["event_id"] = event_id.clone();
        }
        if let (Some(start), Some(end)) = (
            record
                .raw_json
                .get("start_timestamp")
                .and_then(parse_timestamp_us),
            record
                .raw_json
                .get("timestamp")
                .and_then(parse_timestamp_us),
        ) && let Some(duration) = end.checked_sub(start).filter(|duration| *duration >= 0)
        {
            record.attributes["duration_us"] = Value::from(duration);
        }
        self.push(record)
    }

    pub(super) fn logs(&mut self, payload: &[u8], item_ordinal: usize) -> Result<(), SentryError> {
        let mut deserializer = serde_json::Deserializer::from_slice(payload);
        let mut captured_error = None;
        let result = LogBatchSeed {
            normalizer: self,
            item_ordinal,
            captured_error: &mut captured_error,
        }
        .deserialize(&mut deserializer);
        if let Some(error) = captured_error {
            return Err(error);
        }
        result.map_err(|_| SentryError::Malformed("invalid structured log payload"))?;
        deserializer
            .end()
            .map_err(|_| SentryError::Malformed("invalid structured log payload"))?;
        Ok(())
    }

    pub(super) fn unsupported_item(&mut self) -> Result<(), SentryError> {
        self.unsupported_items = self
            .unsupported_items
            .checked_add(1)
            .ok_or(SentryError::TooLarge("unsupported item count overflow"))?;
        Ok(())
    }

    pub(super) fn finish(
        self,
        envelope_auth: Option<EnvelopeAuth>,
    ) -> Result<NormalizedRequest, SentryError> {
        Ok(NormalizedRequest {
            records: self.records,
            replay: None,
            feedback: Vec::new(),
            unsupported_items: self.unsupported_items,
            envelope_auth,
        })
    }

    fn log(
        &mut self,
        mut raw: Value,
        item_ordinal: usize,
        record_ordinal: usize,
    ) -> Result<(), SentryError> {
        if !raw.is_object() {
            return Err(SentryError::Malformed(
                "structured log record must be an object",
            ));
        }
        scrub::scrub(&mut raw, &self.project.scrub_keys);
        let record = normalize_log(
            raw,
            self.project,
            self.acceptance_id,
            self.received_at_us,
            item_ordinal,
            record_ordinal,
        )?;
        self.push(record)
    }

    fn push(&mut self, record: Record) -> Result<(), SentryError> {
        if self.records.len() >= self.limits.request_records {
            return Err(SentryError::TooLarge("record count exceeds limit"));
        }
        let bytes = serde_json::to_vec(&record)
            .map_err(|_| SentryError::Malformed("normalized record serialization failed"))?
            .len();
        if bytes > self.limits.record_bytes {
            return Err(SentryError::TooLarge("normalized record exceeds limit"));
        }
        let separator = usize::from(!self.records.is_empty());
        self.normalized_bytes = self
            .normalized_bytes
            .checked_add(separator)
            .and_then(|value| value.checked_add(bytes))
            .ok_or(SentryError::TooLarge("normalized request size overflow"))?;
        if self.normalized_bytes > self.limits.decoded_bytes {
            return Err(SentryError::TooLarge("normalized request exceeds limit"));
        }
        self.records.push(record);
        Ok(())
    }
}

struct LogBatchSeed<'a, 'project> {
    normalizer: &'a mut RequestNormalizer<'project>,
    item_ordinal: usize,
    captured_error: &'a mut Option<SentryError>,
}

impl<'de> DeserializeSeed<'de> for LogBatchSeed<'_, '_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(LogBatchVisitor {
            normalizer: self.normalizer,
            item_ordinal: self.item_ordinal,
            captured_error: self.captured_error,
        })
    }
}

struct LogBatchVisitor<'a, 'project> {
    normalizer: &'a mut RequestNormalizer<'project>,
    item_ordinal: usize,
    captured_error: &'a mut Option<SentryError>,
}

impl<'de> Visitor<'de> for LogBatchVisitor<'_, '_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a Sentry structured log batch object")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut version = None;
        let mut saw_items = false;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "version" => {
                    if version.is_some() {
                        return Err(de::Error::duplicate_field("version"));
                    }
                    version = Some(map.next_value::<u64>()?);
                }
                "items" => {
                    if saw_items {
                        return Err(de::Error::duplicate_field("items"));
                    }
                    saw_items = true;
                    map.next_value_seed(LogItemsSeed {
                        normalizer: self.normalizer,
                        item_ordinal: self.item_ordinal,
                        captured_error: self.captured_error,
                    })?;
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        if version != Some(2) {
            return Err(de::Error::custom(
                "unsupported structured log payload version",
            ));
        }
        if !saw_items {
            return Err(de::Error::missing_field("items"));
        }
        Ok(())
    }
}

struct LogItemsSeed<'a, 'project> {
    normalizer: &'a mut RequestNormalizer<'project>,
    item_ordinal: usize,
    captured_error: &'a mut Option<SentryError>,
}

impl<'de> DeserializeSeed<'de> for LogItemsSeed<'_, '_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(LogItemsVisitor {
            normalizer: self.normalizer,
            item_ordinal: self.item_ordinal,
            captured_error: self.captured_error,
        })
    }
}

struct LogItemsVisitor<'a, 'project> {
    normalizer: &'a mut RequestNormalizer<'project>,
    item_ordinal: usize,
    captured_error: &'a mut Option<SentryError>,
}

impl<'de> Visitor<'de> for LogItemsVisitor<'_, '_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("an array of Sentry structured log records")
    }

    fn visit_seq<S>(self, mut sequence: S) -> Result<Self::Value, S::Error>
    where
        S: SeqAccess<'de>,
    {
        let mut ordinal = 0;
        loop {
            let mut nodes = 0;
            let Some(value) = sequence.next_element_seed(super::json::BoundedValue::new(
                &mut nodes,
                self.captured_error,
            ))?
            else {
                break;
            };
            if let Err(error) = self.normalizer.log(value, self.item_ordinal, ordinal) {
                *self.captured_error = Some(error);
                return Err(de::Error::custom("structured log normalization failed"));
            }
            ordinal += 1;
        }
        Ok(())
    }
}

fn normalize_event(
    raw: Value,
    project: &ProjectContext,
    acceptance_id: Uuid,
    received_at_us: i64,
    item_ordinal: usize,
    record_ordinal: usize,
) -> Result<Record, SentryError> {
    let source_event_id = canonical_event_id(raw.get("event_id"))?;
    let record_id = source_event_id.as_deref().map_or_else(
        || {
            identity::accepted_record_id(
                project.project_id,
                acceptance_id,
                item_ordinal,
                record_ordinal,
            )
        },
        |event_id| identity::error_record_id(project.project_id, event_id),
    );
    let mut indexing_warnings = Vec::new();
    let timestamp_us = timestamp_us(raw.get("timestamp"), received_at_us, &mut indexing_warnings)?;
    let message = event_message(&raw);
    let level = canonical_level(raw.get("level").and_then(Value::as_str).unwrap_or("error"))?;
    let logger = string_at(&raw, &["logger"]);
    let environment = string_at(&raw, &["environment"]);
    let release = string_at(&raw, &["release"]);
    let service = string_at(&raw, &["service"])
        .or_else(|| string_at(&raw, &["tags", "service.name"]))
        .unwrap_or_else(|| project.slug.clone());
    let trace_id = string_at(&raw, &["contexts", "trace", "trace_id"]);
    let span_id = string_at(&raw, &["contexts", "trace", "span_id"]);
    let request_id = first_string(
        &raw,
        &[
            &["request_id"],
            &["tags", "request_id"],
            &["tags", "request.id"],
            &["contexts", "request", "id"],
        ],
    );
    let user_id = string_at(&raw, &["user", "id"]);
    let user_email = string_at(&raw, &["user", "email"]);
    let attributes = event_attributes(&raw);
    let default_parts = default_fingerprint_parts(&raw, &message, logger.as_deref());
    let fingerprint_parts = explicit_fingerprint_parts(raw.get("fingerprint"), &default_parts)?;
    let fingerprint = identity::fingerprint(&fingerprint_parts);
    let issue_id = identity::issue_id(project.project_id, FINGERPRINT_VERSION, &fingerprint);
    let search_text = event_search_projection(&message, &raw, &mut indexing_warnings);

    Ok(Record {
        record_id,
        kind: RecordKind::Error,
        project_id: project.project_id,
        source_event_id,
        ingest_seq: 0,
        received_at_us,
        timestamp_us,
        service,
        environment,
        release,
        level,
        logger,
        message,
        trace_id,
        span_id,
        request_id,
        user_id,
        user_email,
        issue_id: Some(issue_id),
        fingerprint_version: Some(FINGERPRINT_VERSION),
        fingerprint: Some(fingerprint),
        attributes,
        search_text,
        raw_json: raw,
        normalizer_version: NORMALIZER_VERSION,
        indexing_warnings,
    })
}

fn normalize_log(
    raw: Value,
    project: &ProjectContext,
    acceptance_id: Uuid,
    received_at_us: i64,
    item_ordinal: usize,
    record_ordinal: usize,
) -> Result<Record, SentryError> {
    let mut indexing_warnings = Vec::new();
    let timestamp_us = timestamp_us(raw.get("timestamp"), received_at_us, &mut indexing_warnings)?;
    let message = raw
        .get("body")
        .and_then(Value::as_str)
        .ok_or(SentryError::Malformed(
            "structured log body must be a string",
        ))?
        .to_owned();
    let level = canonical_level(raw.get("level").and_then(Value::as_str).ok_or(
        SentryError::Malformed("structured log level must be a string"),
    )?)?;
    let attributes = flattened_log_attributes(&raw)?;
    let service = string_at(&attributes, &["service.name"]).unwrap_or_else(|| project.slug.clone());
    let environment = string_at(&attributes, &["sentry.environment"]);
    let release = string_at(&attributes, &["sentry.release"]);
    let logger = string_at(&attributes, &["logger.name"]);
    let trace_id = string_at(&raw, &["trace_id"]);
    let span_id = first_string(
        &raw,
        &[&["span_id"], &["attributes", "sentry.span_id", "value"]],
    );
    let request_id = first_string(
        &attributes,
        &[&["request_id"], &["request.id"], &["http.request.id"]],
    );
    let user_id = string_at(&attributes, &["user.id"]);
    let user_email = string_at(&attributes, &["user.email"]);
    let search_text = log_search_projection(&message, &attributes, &mut indexing_warnings);

    Ok(Record {
        record_id: identity::accepted_record_id(
            project.project_id,
            acceptance_id,
            item_ordinal,
            record_ordinal,
        ),
        kind: RecordKind::Log,
        project_id: project.project_id,
        source_event_id: None,
        ingest_seq: 0,
        received_at_us,
        timestamp_us,
        service,
        environment,
        release,
        level,
        logger,
        message,
        trace_id,
        span_id,
        request_id,
        user_id,
        user_email,
        issue_id: None,
        fingerprint_version: None,
        fingerprint: None,
        attributes,
        search_text,
        raw_json: raw,
        normalizer_version: NORMALIZER_VERSION,
        indexing_warnings,
    })
}

fn canonical_event_id(value: Option<&Value>) -> Result<Option<String>, SentryError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let event_id = value
        .as_str()
        .ok_or(SentryError::Malformed("event_id must be a string"))?;
    if event_id.len() != 32 || !event_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(SentryError::Malformed(
            "event_id must be 32 hexadecimal characters",
        ));
    }
    Ok(Some(event_id.to_ascii_lowercase()))
}

fn canonical_level(level: &str) -> Result<String, SentryError> {
    match level.to_ascii_lowercase().as_str() {
        "trace" => Ok("trace".to_owned()),
        "debug" => Ok("debug".to_owned()),
        "info" | "log" => Ok("info".to_owned()),
        "warn" | "warning" => Ok("warning".to_owned()),
        "error" => Ok("error".to_owned()),
        "fatal" | "critical" => Ok("fatal".to_owned()),
        _ => Err(SentryError::Malformed("unsupported event or log level")),
    }
}

fn timestamp_us(
    value: Option<&Value>,
    received_at_us: i64,
    warnings: &mut Vec<String>,
) -> Result<i64, SentryError> {
    if received_at_us.checked_mul(1_000).is_none() {
        return Err(SentryError::Malformed(
            "received timestamp is outside native index range",
        ));
    }
    let parsed = value
        .and_then(parse_timestamp_us)
        .filter(|timestamp| timestamp.checked_mul(1_000).is_some());
    match parsed {
        Some(timestamp) => Ok(timestamp),
        None => {
            warnings.push("timestamp_fallback".to_owned());
            Ok(received_at_us)
        }
    }
}

fn parse_timestamp_us(value: &Value) -> Option<i64> {
    if let Some(seconds) = value.as_f64() {
        let micros = seconds * 1_000_000.0;
        if micros.is_finite() && micros >= i64::MIN as f64 && micros <= i64::MAX as f64 {
            return Some(micros.round() as i64);
        }
        return None;
    }
    let text = value.as_str()?;
    if let Ok(seconds) = text.parse::<f64>() {
        let micros = seconds * 1_000_000.0;
        if micros.is_finite() && micros >= i64::MIN as f64 && micros <= i64::MAX as f64 {
            return Some(micros.round() as i64);
        }
    }
    let timestamp = OffsetDateTime::parse(text, &Rfc3339).ok()?;
    let nanos = timestamp.unix_timestamp_nanos();
    i64::try_from(nanos.div_euclid(1_000)).ok()
}

fn event_message(raw: &Value) -> String {
    if let Some(message) = string_at(raw, &["message"]) {
        return message;
    }
    if let Some(message) = string_at(raw, &["logentry", "formatted"])
        .or_else(|| string_at(raw, &["logentry", "message"]))
    {
        return message;
    }
    last_exception(raw)
        .and_then(|exception| exception.get("value"))
        .and_then(Value::as_str)
        .unwrap_or("event")
        .to_owned()
}

fn last_exception(raw: &Value) -> Option<&Value> {
    let exception = raw.get("exception")?;
    exception
        .get("values")
        .and_then(Value::as_array)
        .or_else(|| exception.as_array())
        .and_then(|values| values.last())
}

fn event_attributes(raw: &Value) -> Value {
    let mut attributes = Map::new();
    for key in ["extra", "tags", "contexts", "request", "user"] {
        if let Some(value) = raw.get(key) {
            attributes.insert(key.to_owned(), value.clone());
        }
    }
    Value::Object(attributes)
}

fn flattened_log_attributes(raw: &Value) -> Result<Value, SentryError> {
    let Some(attributes) = raw.get("attributes") else {
        return Ok(Value::Object(Map::new()));
    };
    let object = attributes.as_object().ok_or(SentryError::Malformed(
        "structured log attributes must be an object",
    ))?;
    let mut flattened = Map::new();
    for (key, wrapper) in object {
        let value = wrapper
            .as_object()
            .and_then(|wrapper| wrapper.get("value"))
            .ok_or(SentryError::Malformed(
                "structured log attribute must contain value",
            ))?;
        flattened.insert(key.clone(), value.clone());
    }
    Ok(Value::Object(flattened))
}

fn default_fingerprint_parts(raw: &Value, message: &str, logger: Option<&str>) -> Vec<String> {
    let mut parts = Vec::new();
    let exception = last_exception(raw);
    if let Some(exception) = exception {
        if let Some(kind) = exception.get("type").and_then(Value::as_str) {
            parts.push(format!("type:{}", normalize_fingerprint_text(kind)));
        }
        parts.push(format!("message:{}", normalize_fingerprint_text(message)));
        let frames = exception
            .get("stacktrace")
            .and_then(|value| value.get("frames"))
            .or_else(|| raw.get("stacktrace").and_then(|value| value.get("frames")))
            .and_then(Value::as_array);
        if let Some(frames) = frames {
            let in_app: Vec<&Value> = frames
                .iter()
                .filter(|frame| frame.get("in_app").and_then(Value::as_bool) == Some(true))
                .collect();
            let selected: Vec<&Value> = if in_app.is_empty() {
                frames.iter().collect()
            } else {
                in_app
            };
            let start = selected.len().saturating_sub(5);
            for frame in &selected[start..] {
                for key in ["module", "function", "filename"] {
                    let value = frame.get(key).and_then(Value::as_str).unwrap_or("");
                    parts.push(format!("frame.{key}:{}", normalize_fingerprint_text(value)));
                }
            }
        }
    } else {
        parts.push(format!(
            "logger:{}",
            normalize_fingerprint_text(logger.unwrap_or(""))
        ));
        parts.push(format!("message:{}", normalize_fingerprint_text(message)));
    }
    parts
}

fn explicit_fingerprint_parts(
    fingerprint: Option<&Value>,
    default: &[String],
) -> Result<Vec<String>, SentryError> {
    let Some(fingerprint) = fingerprint else {
        return Ok(default.to_vec());
    };
    let values = fingerprint.as_array().ok_or(SentryError::Malformed(
        "fingerprint must be an array of strings",
    ))?;
    if values.is_empty() {
        return Ok(default.to_vec());
    }
    let mut parts = Vec::new();
    for value in values {
        let value = value.as_str().ok_or(SentryError::Malformed(
            "fingerprint must be an array of strings",
        ))?;
        if value == "{{ default }}" {
            parts.extend_from_slice(default);
        } else {
            parts.push(normalize_fingerprint_text(value));
        }
    }
    Ok(parts)
}

fn normalize_fingerprint_text(input: &str) -> String {
    input
        .split_whitespace()
        .map(normalize_fingerprint_token)
        .collect::<Vec<_>>()
        .join(" ")
}

fn normalize_fingerprint_token(token: &str) -> String {
    let token =
        if (token.starts_with("http://") || token.starts_with("https://")) && token.contains('?') {
            format!(
                "{}?<query>",
                token.split_once('?').expect("checked above").0
            )
        } else {
            token.to_owned()
        };
    let chars: Vec<char> = token.chars().collect();
    let mut output = String::new();
    let mut index = 0;
    while index < chars.len() {
        if is_uuid_at(&chars, index) {
            output.push_str("<uuid>");
            index += 36;
            continue;
        }
        if chars[index] == '0'
            && chars
                .get(index + 1)
                .is_some_and(|character| *character == 'x' || *character == 'X')
        {
            let end = ascii_run_end(&chars, index + 2, |character| character.is_ascii_hexdigit());
            if end.saturating_sub(index + 2) >= 8 {
                output.push_str("<addr>");
                index = end;
                continue;
            }
        }
        if chars[index].is_ascii_hexdigit() {
            let end = ascii_run_end(&chars, index, |character| character.is_ascii_hexdigit());
            if end - index >= 16 {
                output.push_str("<hex>");
                index = end;
                continue;
            }
        }
        if chars[index].is_ascii_digit() {
            let end = ascii_run_end(&chars, index, |character| character.is_ascii_digit());
            if end - index >= 6 {
                output.push_str("<id>");
                index = end;
                continue;
            }
        }
        output.push(chars[index]);
        index += 1;
    }
    output
}

fn ascii_run_end(chars: &[char], start: usize, predicate: impl Fn(char) -> bool) -> usize {
    let mut end = start;
    while chars.get(end).copied().is_some_and(&predicate) {
        end += 1;
    }
    end
}

fn is_uuid_at(chars: &[char], start: usize) -> bool {
    if start + 36 > chars.len() {
        return false;
    }
    (0..36).all(|offset| match offset {
        8 | 13 | 18 | 23 => chars[start + offset] == '-',
        _ => chars[start + offset].is_ascii_hexdigit(),
    })
}

fn event_search_projection(message: &str, raw: &Value, warnings: &mut Vec<String>) -> String {
    let mut projection = Projection::default();
    projection.push_text(message);
    if let Some(exception) = raw.get("exception") {
        projection.walk(exception, 1);
    }
    if let Some(values) = raw
        .get("breadcrumbs")
        .and_then(|breadcrumbs| breadcrumbs.get("values"))
        .and_then(Value::as_array)
    {
        for breadcrumb in values {
            if let Some(message) = breadcrumb.get("message") {
                projection.walk(message, 1);
            }
        }
    }
    for key in ["extra", "contexts", "tags", "user"] {
        if let Some(value) = raw.get(key) {
            projection.walk(value, 1);
        }
    }
    if let Some(url) = raw.get("request").and_then(|request| request.get("url")) {
        projection.walk(url, 1);
    }
    warnings.extend(projection.warnings());
    projection.text
}

fn log_search_projection(message: &str, attributes: &Value, warnings: &mut Vec<String>) -> String {
    let mut projection = Projection::default();
    projection.push_text(message);
    projection.walk(attributes, 1);
    warnings.extend(projection.warnings());
    projection.text
}

#[derive(Default)]
struct Projection {
    text: String,
    scalars: usize,
    depth_truncated: bool,
    scalar_truncated: bool,
    text_truncated: bool,
}

impl Projection {
    fn walk(&mut self, value: &Value, depth: usize) {
        if self.scalar_truncated || self.text_truncated {
            return;
        }
        if depth > MAX_SEARCH_DEPTH {
            self.depth_truncated = true;
            return;
        }
        match value {
            Value::Array(values) => {
                for value in values {
                    self.walk(value, depth + 1);
                }
            }
            Value::Object(object) => {
                for value in object.values() {
                    self.walk(value, depth + 1);
                }
            }
            Value::String(value) => self.push_scalar(value),
            Value::Number(value) => self.push_scalar(&value.to_string()),
            Value::Bool(value) => self.push_scalar(if *value { "true" } else { "false" }),
            Value::Null => {}
        }
    }

    fn push_scalar(&mut self, value: &str) {
        if self.scalars >= MAX_SEARCH_SCALARS {
            self.scalar_truncated = true;
            return;
        }
        self.scalars += 1;
        self.push_text(value);
    }

    fn push_text(&mut self, value: &str) {
        if self.text_truncated || value.is_empty() {
            return;
        }
        let separator = usize::from(!self.text.is_empty());
        let available = MAX_SEARCH_BYTES.saturating_sub(self.text.len() + separator);
        if available == 0 {
            self.text_truncated = true;
            return;
        }
        if separator == 1 {
            self.text.push(' ');
        }
        if value.len() <= available {
            self.text.push_str(value);
            return;
        }
        let mut boundary = available;
        while boundary > 0 && !value.is_char_boundary(boundary) {
            boundary -= 1;
        }
        self.text.push_str(&value[..boundary]);
        self.text_truncated = true;
    }

    fn warnings(&self) -> Vec<String> {
        let flags = BTreeMap::from([
            ("search_depth_truncated", self.depth_truncated),
            ("search_scalar_truncated", self.scalar_truncated),
            ("search_text_truncated", self.text_truncated),
        ]);
        flags
            .into_iter()
            .filter(|(_, present)| *present)
            .map(|(warning, _)| warning.to_owned())
            .collect()
    }
}

fn first_string(value: &Value, paths: &[&[&str]]) -> Option<String> {
    paths.iter().find_map(|path| string_at(value, path))
}

fn string_at(value: &Value, path: &[&str]) -> Option<String> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_str().map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use std::fmt;

    use serde::de::Visitor;
    use serde_json::json;

    use super::*;

    const RECEIVED_AT_US: i64 = 1_767_323_045_000_000;

    fn project() -> ProjectContext {
        ProjectContext {
            project_id: 7,
            slug: "fixture".to_owned(),
            public_key: "public".to_owned(),
            scrub_keys: Vec::new(),
        }
    }

    fn normalizer<'a>(project: &'a ProjectContext, limits: &'a Limits) -> RequestNormalizer<'a> {
        RequestNormalizer::new(project, Uuid::from_u128(1), RECEIVED_AT_US, limits)
    }

    #[test]
    fn request_entry_points_reject_non_objects_and_invalid_normalized_fields() {
        let project = project();
        let limits = Limits::default();
        let mut state = normalizer(&project, &limits);
        assert_eq!(
            state.event(b"[]", 0, 0),
            Err(SentryError::Malformed("event payload must be an object"))
        );
        assert_eq!(
            state.transaction(b"null", 0),
            Err(SentryError::Malformed(
                "transaction payload must be an object"
            ))
        );
        assert!(matches!(
            state.event(br#"{"event_id":1}"#, 0, 0),
            Err(SentryError::Malformed("event_id must be a string"))
        ));
        assert!(matches!(
            state.event(br#"{"level":"notice"}"#, 0, 0),
            Err(SentryError::Malformed("unsupported event or log level"))
        ));
    }

    #[test]
    fn structured_log_shape_validation_is_closed_by_default() {
        let project = project();
        let limits = Limits::default();
        let cases: &[&[u8]] = &[
            b"[]",
            br#"{"version":2}"#,
            br#"{"version":1,"items":[]}"#,
            br#"{"version":2,"version":2,"items":[]}"#,
            br#"{"version":2,"items":[],"items":[]}"#,
            br#"{"version":2,"items":{}}"#,
            br#"{"version":2,"items":[null]}"#,
            br#"{"version":2,"items":[{"body":"x","level":"notice"}]}"#,
            br#"{"version":2,"items":[{"body":"x","level":"info","attributes":[]}] }"#,
            br#"{"version":2,"items":[{"body":"x","level":"info","attributes":{"bad":{}}}]}"#,
        ];
        for payload in cases {
            let mut state = normalizer(&project, &limits);
            assert!(
                matches!(state.logs(payload, 0), Err(SentryError::Malformed(_))),
                "payload was accepted: {}",
                String::from_utf8_lossy(payload)
            );
        }
    }

    struct Expecting<'a, V>(&'a V);

    impl<V> fmt::Display for Expecting<'_, V>
    where
        V: for<'de> Visitor<'de>,
    {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.0.expecting(formatter)
        }
    }

    #[test]
    fn structured_log_visitors_describe_their_required_shapes() {
        let project = project();
        let limits = Limits::default();
        let mut state = normalizer(&project, &limits);
        let mut error = None;
        let batch = LogBatchVisitor {
            normalizer: &mut state,
            item_ordinal: 0,
            captured_error: &mut error,
        };
        assert_eq!(
            Expecting(&batch).to_string(),
            "a Sentry structured log batch object"
        );

        let mut state = normalizer(&project, &limits);
        let mut error = None;
        let items = LogItemsVisitor {
            normalizer: &mut state,
            item_ordinal: 0,
            captured_error: &mut error,
        };
        assert_eq!(
            Expecting(&items).to_string(),
            "an array of Sentry structured log records"
        );
    }

    #[test]
    fn canonical_fields_and_timestamps_cover_valid_and_hostile_forms() {
        assert_eq!(canonical_event_id(None).unwrap(), None);
        assert_eq!(canonical_event_id(Some(&Value::Null)).unwrap(), None);
        assert_eq!(
            canonical_event_id(Some(&json!("ABCDEFABCDEFABCDEFABCDEFABCDEFAB"))).unwrap(),
            Some("abcdefabcdefabcdefabcdefabcdefab".to_owned())
        );
        assert!(canonical_event_id(Some(&json!("abc"))).is_err());

        for (input, expected) in [
            ("TRACE", "trace"),
            ("debug", "debug"),
            ("log", "info"),
            ("warn", "warning"),
            ("critical", "fatal"),
        ] {
            assert_eq!(canonical_level(input).unwrap(), expected);
        }
        assert!(canonical_level("notice").is_err());

        assert_eq!(parse_timestamp_us(&json!(1.25)), Some(1_250_000));
        assert_eq!(parse_timestamp_us(&json!("1.25")), Some(1_250_000));
        assert_eq!(
            parse_timestamp_us(&json!("2026-01-02T03:04:05.000006Z")),
            Some(1_767_323_045_000_006)
        );
        assert_eq!(parse_timestamp_us(&json!("not-a-time")), None);
        assert_eq!(parse_timestamp_us(&json!(1e300)), None);
        assert_eq!(parse_timestamp_us(&json!("1e300")), None);
        assert_eq!(parse_timestamp_us(&Value::Null), None);
    }

    #[test]
    fn fingerprint_normalization_removes_unbounded_dynamic_identifiers() {
        assert_eq!(
            normalize_fingerprint_token("https://example.test/path?secret=yes"),
            "https://example.test/path?<query>"
        );
        assert_eq!(normalize_fingerprint_token("0x12345678"), "<addr>");
        assert_eq!(normalize_fingerprint_token("abcdef0123456789"), "<hex>");
        assert_eq!(normalize_fingerprint_token("123456"), "<id>");
        assert_eq!(
            normalize_fingerprint_token("550e8400-e29b-41d4-a716-446655440000"),
            "<uuid>"
        );

        let raw = json!({
            "exception": {"values": [{
                "type": "Failure",
                "stacktrace": {"frames": [{"function": "outer"}]}
            }]}
        });
        let parts = default_fingerprint_parts(&raw, "failed", None);
        assert!(parts.iter().any(|part| part == "frame.function:outer"));
    }

    #[test]
    fn projection_stops_after_each_bound_and_preserves_utf8() {
        let mut projection = Projection {
            scalar_truncated: true,
            ..Projection::default()
        };
        projection.walk(&json!("ignored"), 1);
        assert!(projection.text.is_empty());

        let mut projection = Projection {
            scalars: MAX_SEARCH_SCALARS,
            ..Projection::default()
        };
        projection.push_scalar("ignored");
        assert!(projection.scalar_truncated);

        let mut projection = Projection {
            text: "x".repeat(MAX_SEARCH_BYTES),
            ..Projection::default()
        };
        projection.push_text("ignored");
        assert!(projection.text_truncated);

        let mut projection = Projection {
            text: "x".repeat(MAX_SEARCH_BYTES - 2),
            ..Projection::default()
        };
        projection.push_text("가");
        assert!(projection.text.is_char_boundary(projection.text.len()));
        assert!(projection.text_truncated);
        assert_eq!(
            projection.warnings(),
            vec!["search_text_truncated".to_owned()]
        );
    }
}
