//! Pure Sentry wire decoding, normalization, and scrubbing.
//!
//! This module has no HTTP or database dependency. Callers authenticate first, then pass the
//! accepted project context and a request-scoped UUID into these functions.

pub mod replay;

mod envelope;
mod identity;
mod json;
mod normalize;
mod scrub;

use std::io::Read;

use flate2::read::{GzDecoder, ZlibDecoder};
use uuid::Uuid;

use crate::{config::Limits, model::Record};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentEncoding {
    Identity,
    Gzip,
    Deflate,
}

impl ContentEncoding {
    pub fn parse(value: Option<&str>) -> Result<Self, SentryError> {
        match value.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
            None | Some("") | Some("identity") => Ok(Self::Identity),
            Some("gzip") => Ok(Self::Gzip),
            Some("deflate") => Ok(Self::Deflate),
            Some(_) => Err(SentryError::UnsupportedEncoding),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectContext {
    pub project_id: i64,
    pub slug: String,
    pub public_key: String,
    pub scrub_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvelopeAuth {
    pub project_id: i64,
    pub public_key: String,
}

#[derive(Debug, Clone)]
pub struct NormalizedRequest {
    pub records: Vec<Record>,
    pub replay: Option<replay::ReplaySegment>,
    pub feedback: Vec<serde_json::Value>,
    pub unsupported_items: usize,
    pub envelope_auth: Option<EnvelopeAuth>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SentryError {
    Malformed(&'static str),
    TooLarge(&'static str),
    UnsupportedEncoding,
}

impl SentryError {
    pub fn is_too_large(self) -> bool {
        matches!(self, Self::TooLarge(_))
    }
}

impl std::fmt::Display for SentryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Malformed(message) | Self::TooLarge(message) => formatter.write_str(message),
            Self::UnsupportedEncoding => formatter.write_str("unsupported content encoding"),
        }
    }
}

impl std::error::Error for SentryError {}

pub fn decode_body(
    wire: &[u8],
    content_encoding: ContentEncoding,
    limits: &Limits,
) -> Result<Vec<u8>, SentryError> {
    if wire.len() > limits.wire_bytes {
        return Err(SentryError::TooLarge(
            "compressed request body exceeds limit",
        ));
    }
    if content_encoding == ContentEncoding::Identity {
        if wire.len() > limits.decoded_bytes {
            return Err(SentryError::TooLarge("decoded request body exceeds limit"));
        }
        return Ok(wire.to_vec());
    }

    let reader: Box<dyn Read> = match content_encoding {
        ContentEncoding::Gzip => Box::new(GzDecoder::new(wire)),
        ContentEncoding::Deflate => Box::new(ZlibDecoder::new(wire)),
        ContentEncoding::Identity => unreachable!(),
    };
    let mut output = Vec::with_capacity(wire.len().min(limits.decoded_bytes));
    reader
        .take(limits.decoded_bytes.saturating_add(1) as u64)
        .read_to_end(&mut output)
        .map_err(|_| SentryError::Malformed("invalid compressed request body"))?;
    if output.len() > limits.decoded_bytes {
        return Err(SentryError::TooLarge("decoded request body exceeds limit"));
    }
    Ok(output)
}

pub fn normalize_envelope(
    decoded: &[u8],
    project: &ProjectContext,
    acceptance_id: Uuid,
    received_at_us: i64,
    limits: &Limits,
) -> Result<NormalizedRequest, SentryError> {
    if decoded.len() > limits.decoded_bytes {
        return Err(SentryError::TooLarge("decoded request body exceeds limit"));
    }
    let parsed = envelope::parse(decoded, limits.request_records)?;
    let envelope_auth = parse_auth_value(&parsed.header)?;
    if envelope_auth.as_ref().is_some_and(|auth| {
        auth.project_id != project.project_id || auth.public_key != project.public_key
    }) {
        return Err(SentryError::Malformed(
            "envelope DSN conflicts with authenticated project",
        ));
    }
    let mut state =
        normalize::RequestNormalizer::new(project, acceptance_id, received_at_us, limits);
    let replay = replay::decode_envelope(decoded, &project.scrub_keys)?;
    let mut feedback = Vec::new();
    for item in parsed.items {
        match item.kind.as_str() {
            "event" => state.event(item.payload, item.ordinal, 0)?,
            "transaction" => state.transaction(item.payload, item.ordinal)?,
            "log" => state.logs(item.payload, item.ordinal)?,
            "replay_event" | "replay_recording" => {}
            "feedback" => {
                let mut value = json::parse(item.payload)?;
                replay::canonical_id(&value["event_id"])?;
                if !value
                    .pointer("/contexts/feedback/message")
                    .is_some_and(serde_json::Value::is_string)
                {
                    return Err(SentryError::Malformed("invalid feedback"));
                }
                if let Some(id) = value.pointer("/contexts/feedback/replay_id") {
                    replay::canonical_id(id)?;
                }
                scrub::scrub(&mut value, &project.scrub_keys);
                feedback.push(value);
            }
            _ => state.unsupported_item()?,
        }
    }
    let mut normalized = state.finish(envelope_auth)?;
    normalized.replay = replay;
    normalized.feedback = feedback;
    Ok(normalized)
}

pub fn normalize_store(
    body: &[u8],
    project: &ProjectContext,
    acceptance_id: Uuid,
    received_at_us: i64,
    limits: &Limits,
) -> Result<NormalizedRequest, SentryError> {
    if body.len() > limits.decoded_bytes {
        return Err(SentryError::TooLarge("decoded request body exceeds limit"));
    }
    let mut state =
        normalize::RequestNormalizer::new(project, acceptance_id, received_at_us, limits);
    state.event(body, 0, 0)?;
    state.finish(None)
}

pub fn envelope_auth(decoded: &[u8]) -> Result<Option<EnvelopeAuth>, SentryError> {
    let (header, _) = envelope::parse_header(decoded)?;
    parse_auth_value(&header)
}

fn parse_auth_value(header: &serde_json::Value) -> Result<Option<EnvelopeAuth>, SentryError> {
    let Some(value) = header.get("dsn") else {
        return Ok(None);
    };
    let dsn = value
        .as_str()
        .ok_or(SentryError::Malformed("envelope DSN must be a string"))?;
    let parsed =
        url::Url::parse(dsn).map_err(|_| SentryError::Malformed("invalid envelope DSN"))?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || parsed.password().is_some()
        || parsed.username().is_empty()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(SentryError::Malformed("invalid envelope DSN credentials"));
    }
    let path = parsed.path().trim_matches('/');
    if path.is_empty() || path.contains('/') {
        return Err(SentryError::Malformed("invalid envelope DSN project"));
    }
    let project_id = path
        .parse::<i64>()
        .map_err(|_| SentryError::Malformed("invalid envelope DSN project"))?;
    if project_id <= 0 {
        return Err(SentryError::Malformed("invalid envelope DSN project"));
    }
    let auth = EnvelopeAuth {
        project_id,
        public_key: parsed.username().to_owned(),
    };
    Ok(Some(auth))
}
