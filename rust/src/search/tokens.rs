//! Opaque, domain-separated read/row/detail/live tokens. Never log their contents.

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenContext {
    pub storage_generation: String,
    pub authorization_epoch: i64,
    pub authorization_hash: String,
    pub request_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenKind {
    Read,
    Rows,
    Detail,
    Live,
}

impl TokenKind {
    fn domain(self) -> &'static [u8] {
        match self {
            Self::Read => b"eventglass.read.v1\0",
            Self::Rows => b"eventglass.rows.v1\0",
            Self::Detail => b"eventglass.detail.v1\0",
            Self::Live => b"eventglass.live.v1\0",
        }
    }
    fn ttl_us(self) -> i64 {
        if self == Self::Detail {
            3_600_000_000
        } else {
            900_000_000
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Position {
    Read,
    Rows {
        timestamp_us: i64,
        ingest_seq: i64,
        record_id: String,
    },
    Detail {
        project_id: i64,
        shard_id: String,
        record_id: String,
    },
    Live {
        scan_seq: i64,
    },
}

impl Position {
    fn kind(&self) -> TokenKind {
        match self {
            Self::Read => TokenKind::Read,
            Self::Rows { .. } => TokenKind::Rows,
            Self::Detail { .. } => TokenKind::Detail,
            Self::Live { .. } => TokenKind::Live,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    version: u32,
    context: TokenContext,
    watermark: i64,
    issued_at_us: i64,
    expires_at_us: i64,
    position: Position,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verified {
    pub watermark: i64,
    pub position: Position,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenError {
    Invalid,
    Expired,
    GenerationChanged,
    AuthorizationChanged,
}

impl std::fmt::Display for TokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for TokenError {}

pub struct TokenCodec {
    key: [u8; 32],
}

impl TokenCodec {
    pub fn new(key: [u8; 32]) -> Self {
        Self { key }
    }

    pub fn issue(
        &self,
        context: TokenContext,
        watermark: i64,
        position: Position,
        now_us: i64,
    ) -> Result<String, TokenError> {
        if watermark < 0 || now_us < 0 || context.authorization_epoch < 0 {
            return Err(TokenError::Invalid);
        }
        validate_position(&position, watermark)?;
        let kind = position.kind();
        let expires_at_us = now_us
            .checked_add(kind.ttl_us())
            .ok_or(TokenError::Invalid)?;
        let payload = Payload {
            version: 1,
            context,
            watermark,
            issued_at_us: now_us,
            expires_at_us,
            position,
        };
        let bytes = serde_json::to_vec(&payload).map_err(|_| TokenError::Invalid)?;
        if bytes.len() > 8 * 1024 {
            return Err(TokenError::Invalid);
        }
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.key).expect("HMAC accepts a 32-byte key");
        mac.update(kind.domain());
        mac.update(&bytes);
        Ok(format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(bytes),
            URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
        ))
    }

    pub fn verify(
        &self,
        token: &str,
        kind: TokenKind,
        expected: &TokenContext,
        now_us: i64,
    ) -> Result<Verified, TokenError> {
        if token.len() > 12 * 1024 || now_us < 0 {
            return Err(TokenError::Invalid);
        }
        let (payload, signature) = token.split_once('.').ok_or(TokenError::Invalid)?;
        let bytes = URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| TokenError::Invalid)?;
        if bytes.len() > 8 * 1024 {
            return Err(TokenError::Invalid);
        }
        let signature = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| TokenError::Invalid)?;
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.key).expect("HMAC accepts a 32-byte key");
        mac.update(kind.domain());
        mac.update(&bytes);
        mac.verify_slice(&signature)
            .map_err(|_| TokenError::Invalid)?;
        let payload: Payload = serde_json::from_slice(&bytes).map_err(|_| TokenError::Invalid)?;
        if payload.version != 1
            || payload.position.kind() != kind
            || payload.watermark < 0
            || payload.issued_at_us < 0
            || payload.expires_at_us.checked_sub(payload.issued_at_us) != Some(kind.ttl_us())
            || payload.issued_at_us > now_us.saturating_add(30_000_000)
        {
            return Err(TokenError::Invalid);
        }
        validate_position(&payload.position, payload.watermark)?;
        if payload.context.storage_generation != expected.storage_generation {
            return Err(TokenError::GenerationChanged);
        }
        if payload.context.authorization_epoch != expected.authorization_epoch
            || payload.context.authorization_hash != expected.authorization_hash
        {
            return Err(TokenError::AuthorizationChanged);
        }
        if payload.context.request_hash != expected.request_hash {
            return Err(TokenError::Invalid);
        }
        if now_us >= payload.expires_at_us {
            return Err(TokenError::Expired);
        }
        Ok(Verified {
            watermark: payload.watermark,
            position: payload.position,
        })
    }
}

fn validate_position(position: &Position, watermark: i64) -> Result<(), TokenError> {
    let digest = |value: &str| {
        value.len() == 64
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    };
    let valid = match position {
        Position::Read => true,
        Position::Rows {
            timestamp_us,
            ingest_seq,
            record_id,
        } => {
            timestamp_us.checked_mul(1000).is_some()
                && *ingest_seq > 0
                && *ingest_seq <= watermark
                && digest(record_id)
        }
        Position::Detail {
            project_id,
            shard_id,
            record_id,
        } => *project_id > 0 && uuid::Uuid::parse_str(shard_id).is_ok() && digest(record_id),
        Position::Live { scan_seq } => *scan_seq >= 0 && *scan_seq <= watermark,
    };
    if valid {
        Ok(())
    } else {
        Err(TokenError::Invalid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context() -> TokenContext {
        TokenContext {
            storage_generation: "generation".into(),
            authorization_epoch: 1,
            authorization_hash: "authorization".into(),
            request_hash: "request".into(),
        }
    }

    fn signed(codec: &TokenCodec, kind: TokenKind, bytes: &[u8]) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(&codec.key).unwrap();
        mac.update(kind.domain());
        mac.update(bytes);
        format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(bytes),
            URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
        )
    }

    #[test]
    fn token_errors_and_issue_limits_are_stable() {
        assert_eq!(TokenError::Invalid.to_string(), "Invalid");
        let codec = TokenCodec::new([1; 32]);
        assert_eq!(
            codec.issue(context(), -1, Position::Read, 0),
            Err(TokenError::Invalid)
        );
        let mut invalid_context = context();
        invalid_context.authorization_epoch = -1;
        assert_eq!(
            codec.issue(invalid_context, 0, Position::Read, 0),
            Err(TokenError::Invalid)
        );
        assert_eq!(
            codec.issue(context(), 0, Position::Read, i64::MAX),
            Err(TokenError::Invalid)
        );
        let mut huge = context();
        huge.request_hash = "x".repeat(8192);
        assert_eq!(
            codec.issue(huge, 0, Position::Read, 0),
            Err(TokenError::Invalid)
        );
    }

    #[test]
    fn signed_but_invalid_payloads_are_rejected_after_authentication() {
        let codec = TokenCodec::new([2; 32]);
        let oversized = vec![b' '; 8193];
        assert_eq!(
            codec.verify(
                &signed(&codec, TokenKind::Read, &oversized),
                TokenKind::Read,
                &context(),
                1
            ),
            Err(TokenError::Invalid)
        );
        let base = Payload {
            version: 2,
            context: context(),
            watermark: 0,
            issued_at_us: 0,
            expires_at_us: TokenKind::Read.ttl_us(),
            position: Position::Read,
        };
        let bytes = serde_json::to_vec(&base).unwrap();
        assert_eq!(
            codec.verify(
                &signed(&codec, TokenKind::Read, &bytes),
                TokenKind::Read,
                &context(),
                1
            ),
            Err(TokenError::Invalid)
        );
    }
}
