//! Durable normalized records. Inbox replay never normalizes these a second time.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Boundary {
    pub inbox_id: i64,
    pub ingest_seq: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordKind {
    Log,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub record_id: String,
    pub kind: RecordKind,
    pub project_id: i64,
    pub source_event_id: Option<String>,
    pub ingest_seq: i64,
    pub received_at_us: i64,
    pub timestamp_us: i64,
    pub service: String,
    pub environment: Option<String>,
    pub release: Option<String>,
    pub level: String,
    pub logger: Option<String>,
    pub message: String,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub request_id: Option<String>,
    pub user_id: Option<String>,
    pub user_email: Option<String>,
    pub issue_id: Option<String>,
    pub fingerprint_version: Option<u32>,
    pub fingerprint: Option<String>,
    pub attributes: Value,
    pub search_text: String,
    pub raw_json: Value,
    pub normalizer_version: u32,
    pub indexing_warnings: Vec<String>,
}

pub fn now_us() -> Result<i64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_micros()
        .try_into()
        .context("system clock exceeds timestamp range")
}
