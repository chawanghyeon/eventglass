use anyhow::{Result, ensure};
use tantivy::{
    DateTime, TantivyDocument,
    schema::{
        DateOptions, DateTimePrecision, FAST, INDEXED, IndexRecordOption, JsonObjectOptions,
        STORED, STRING, Schema, TEXT, TextFieldIndexing, TextOptions,
    },
};

use crate::model::{Record, RecordKind};

pub const APPLICATION_SCHEMA_VERSION: u32 = 1;
pub const TOKENIZER_VERSION: u32 = 1;

pub fn build() -> Schema {
    let mut schema = Schema::builder();
    schema.add_text_field("record_id", STRING | STORED);
    for name in [
        "kind",
        "service",
        "level",
        "environment",
        "release",
        "logger",
        "issue_id",
        "fingerprint",
    ] {
        schema.add_text_field(name, STRING | FAST | STORED);
    }
    for name in ["trace_id", "span_id", "request_id", "user_id", "user_email"] {
        schema.add_text_field(name, STRING | STORED);
    }
    for name in ["project_id", "ingest_seq"] {
        schema.add_i64_field(name, INDEXED | FAST | STORED);
    }
    for name in ["timestamp", "received_at"] {
        schema.add_date_field(
            name,
            DateOptions::default()
                .set_indexed()
                .set_fast()
                .set_stored()
                .set_precision(DateTimePrecision::Microseconds),
        );
    }
    schema.add_text_field("message", TEXT | STORED);
    schema.add_text_field("search_text", TEXT);
    let json_text = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer("raw")
            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
    );
    schema.add_json_field(
        "attributes",
        JsonObjectOptions::from(json_text).set_fast(Some("raw")),
    );
    schema.add_text_field("raw_json", STORED);
    schema.add_u64_field("normalizer_version", STORED);
    schema.add_text_field("indexing_warnings", STORED);
    schema.build()
}

pub fn document(schema: &Schema, record: &Record) -> Result<TantivyDocument> {
    ensure!(
        record.ingest_seq > 0 && record.project_id > 0,
        "unassigned record identity"
    );
    let timestamp = record
        .timestamp_us
        .checked_mul(1000)
        .ok_or_else(|| anyhow::anyhow!("timestamp exceeds native DateTime range"))?;
    let received = record
        .received_at_us
        .checked_mul(1000)
        .ok_or_else(|| anyhow::anyhow!("received timestamp exceeds native DateTime range"))?;
    let mut doc = TantivyDocument::default();
    for (name, value) in [
        ("record_id", record.record_id.as_str()),
        (
            "kind",
            match record.kind {
                RecordKind::Log => "log",
                RecordKind::Error => "error",
            },
        ),
        ("service", record.service.as_str()),
        ("level", record.level.as_str()),
        ("message", record.message.as_str()),
        ("search_text", record.search_text.as_str()),
    ] {
        doc.add_text(schema.get_field(name)?, value);
    }
    for (name, value) in [
        ("environment", &record.environment),
        ("release", &record.release),
        ("logger", &record.logger),
        ("issue_id", &record.issue_id),
        ("fingerprint", &record.fingerprint),
        ("trace_id", &record.trace_id),
        ("span_id", &record.span_id),
        ("request_id", &record.request_id),
        ("user_id", &record.user_id),
        ("user_email", &record.user_email),
    ] {
        if let Some(value) = value {
            doc.add_text(schema.get_field(name)?, value);
        }
    }
    doc.add_i64(schema.get_field("project_id")?, record.project_id);
    doc.add_i64(schema.get_field("ingest_seq")?, record.ingest_seq);
    doc.add_date(
        schema.get_field("timestamp")?,
        DateTime::from_timestamp_nanos(timestamp),
    );
    doc.add_date(
        schema.get_field("received_at")?,
        DateTime::from_timestamp_nanos(received),
    );
    // The native document conversion supports serde_json values without creating
    // per-attribute schema fields. Attributes must be an object by normalization.
    let attributes = record
        .attributes
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("attributes must be an object"))?;
    doc.add_object(
        schema.get_field("attributes")?,
        attributes
            .iter()
            .map(|(k, v)| (k.clone(), v.clone().into()))
            .collect(),
    );
    doc.add_text(
        schema.get_field("raw_json")?,
        serde_json::to_string(&record.raw_json)?,
    );
    doc.add_u64(
        schema.get_field("normalizer_version")?,
        u64::from(record.normalizer_version),
    );
    doc.add_text(
        schema.get_field("indexing_warnings")?,
        serde_json::to_string(&record.indexing_warnings)?,
    );
    Ok(doc)
}
