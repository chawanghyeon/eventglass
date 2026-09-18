use anyhow::{Result, ensure};
use tantivy::{
    DateTime, TantivyDocument,
    schema::{
        DateOptions, DateTimePrecision, FAST, Field, INDEXED, IndexRecordOption, JsonObjectOptions,
        STORED, STRING, Schema, TEXT, TextFieldIndexing, TextOptions,
    },
};

use crate::model::{Record, RecordKind};

pub const APPLICATION_SCHEMA_VERSION: u32 = 1;
pub const TOKENIZER_VERSION: u32 = 1;

pub struct DocumentFields {
    record_id: Field,
    kind: Field,
    service: Field,
    level: Field,
    message: Field,
    search_text: Field,
    environment: Field,
    release: Field,
    logger: Field,
    issue_id: Field,
    fingerprint: Field,
    trace_id: Field,
    span_id: Field,
    request_id: Field,
    user_id: Field,
    user_email: Field,
    project_id: Field,
    ingest_seq: Field,
    timestamp: Field,
    received_at: Field,
    attributes: Field,
    raw_json: Field,
    normalizer_version: Field,
    indexing_warnings: Field,
}

impl DocumentFields {
    pub fn new(schema: &Schema) -> Result<Self> {
        Ok(Self {
            record_id: schema.get_field("record_id")?,
            kind: schema.get_field("kind")?,
            service: schema.get_field("service")?,
            level: schema.get_field("level")?,
            message: schema.get_field("message")?,
            search_text: schema.get_field("search_text")?,
            environment: schema.get_field("environment")?,
            release: schema.get_field("release")?,
            logger: schema.get_field("logger")?,
            issue_id: schema.get_field("issue_id")?,
            fingerprint: schema.get_field("fingerprint")?,
            trace_id: schema.get_field("trace_id")?,
            span_id: schema.get_field("span_id")?,
            request_id: schema.get_field("request_id")?,
            user_id: schema.get_field("user_id")?,
            user_email: schema.get_field("user_email")?,
            project_id: schema.get_field("project_id")?,
            ingest_seq: schema.get_field("ingest_seq")?,
            timestamp: schema.get_field("timestamp")?,
            received_at: schema.get_field("received_at")?,
            attributes: schema.get_field("attributes")?,
            raw_json: schema.get_field("raw_json")?,
            normalizer_version: schema.get_field("normalizer_version")?,
            indexing_warnings: schema.get_field("indexing_warnings")?,
        })
    }
}

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
    document_with_fields(&DocumentFields::new(schema)?, record)
}

pub fn document_with_fields(fields: &DocumentFields, record: &Record) -> Result<TantivyDocument> {
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
    for (field, value) in [
        (fields.record_id, record.record_id.as_str()),
        (
            fields.kind,
            match record.kind {
                RecordKind::Log => "log",
                RecordKind::Error => "error",
            },
        ),
        (fields.service, record.service.as_str()),
        (fields.level, record.level.as_str()),
        (fields.message, record.message.as_str()),
        (fields.search_text, record.search_text.as_str()),
    ] {
        doc.add_text(field, value);
    }
    for (field, value) in [
        (fields.environment, &record.environment),
        (fields.release, &record.release),
        (fields.logger, &record.logger),
        (fields.issue_id, &record.issue_id),
        (fields.fingerprint, &record.fingerprint),
        (fields.trace_id, &record.trace_id),
        (fields.span_id, &record.span_id),
        (fields.request_id, &record.request_id),
        (fields.user_id, &record.user_id),
        (fields.user_email, &record.user_email),
    ] {
        if let Some(value) = value {
            doc.add_text(field, value);
        }
    }
    doc.add_i64(fields.project_id, record.project_id);
    doc.add_i64(fields.ingest_seq, record.ingest_seq);
    doc.add_date(fields.timestamp, DateTime::from_timestamp_nanos(timestamp));
    doc.add_date(fields.received_at, DateTime::from_timestamp_nanos(received));
    // The native document conversion supports serde_json values without creating
    // per-attribute schema fields. Attributes must be an object by normalization.
    let attributes = record
        .attributes
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("attributes must be an object"))?;
    doc.add_object(
        fields.attributes,
        attributes
            .iter()
            .map(|(k, v)| (k.clone(), v.clone().into()))
            .collect(),
    );
    doc.add_text(fields.raw_json, serde_json::to_string(&record.raw_json)?);
    doc.add_u64(
        fields.normalizer_version,
        u64::from(record.normalizer_version),
    );
    doc.add_text(
        fields.indexing_warnings,
        serde_json::to_string(&record.indexing_warnings)?,
    );
    Ok(doc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::Document;

    fn record() -> Record {
        Record {
            record_id: "record".into(),
            kind: RecordKind::Error,
            project_id: 1,
            source_event_id: None,
            ingest_seq: 1,
            received_at_us: 2,
            timestamp_us: 1,
            service: "api".into(),
            environment: None,
            release: None,
            level: "error".into(),
            logger: None,
            message: "failed".into(),
            trace_id: None,
            span_id: None,
            request_id: None,
            user_id: None,
            user_email: None,
            issue_id: None,
            fingerprint_version: None,
            fingerprint: None,
            attributes: serde_json::json!({}),
            search_text: "failed".into(),
            raw_json: serde_json::json!({}),
            normalizer_version: 1,
            indexing_warnings: vec![],
        }
    }

    #[test]
    fn document_rejects_invalid_identity_time_and_attribute_shapes() {
        let schema = build();
        assert!(document(&schema, &record()).is_ok());

        let mut value = record();
        value.project_id = 0;
        assert!(document(&schema, &value).is_err());
        let mut value = record();
        value.timestamp_us = i64::MAX;
        assert!(document(&schema, &value).is_err());
        let mut value = record();
        value.received_at_us = i64::MAX;
        assert!(document(&schema, &value).is_err());
        let mut value = record();
        value.attributes = serde_json::json!([]);
        assert!(document(&schema, &value).is_err());
    }

    #[test]
    fn cached_fields_preserve_every_document_value() -> Result<()> {
        let schema = build();
        let fields = DocumentFields::new(&schema)?;
        let mut value = record();
        value.environment = Some("production".into());
        value.release = Some("v2".into());
        value.logger = Some("api".into());
        value.issue_id = Some("issue".into());
        value.fingerprint = Some("fingerprint".into());
        value.trace_id = Some("trace".into());
        value.span_id = Some("span".into());
        value.request_id = Some("request".into());
        value.user_id = Some("user".into());
        value.user_email = Some("user@example.test".into());
        value.attributes = serde_json::json!({"region":"ap-northeast-2", "attempt":2});
        value.raw_json = serde_json::json!({"message":"failed", "context":{"code":500}});
        value.indexing_warnings = vec!["example".into()];
        assert_eq!(
            document(&schema, &value)?.to_named_doc(&schema).0,
            document_with_fields(&fields, &value)?
                .to_named_doc(&schema)
                .0
        );
        Ok(())
    }
}
