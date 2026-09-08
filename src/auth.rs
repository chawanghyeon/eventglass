//! Authentication policy and bounded password work, independent of HTTP.

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use argon2::{
    Algorithm, Argon2, Params, PasswordHash, PasswordHasher, PasswordVerifier, Version,
    password_hash::SaltString,
};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::sync::Semaphore;

use crate::app::AppState;

pub const PASSWORD_MEMORY_KIB: u32 = 32 * 1024;
const PASSWORD_TIME_COST: u32 = 3;
const PASSWORD_PARALLELISM: u32 = 1;
const AUTH_ATTEMPTS_PER_MINUTE: usize = 20;
const SETUP_TOKEN_TTL: Duration = Duration::from_secs(30 * 60);
const DUMMY_PASSWORD_HASH: &str = "$argon2id$v=19$m=32768,t=3,p=1$b2JzZXJ2ZS1kdW1teS1zYWx0$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

#[derive(Clone, Copy)]
pub(crate) enum AttemptKind {
    Login,
    Setup,
}

#[derive(Debug)]
pub(crate) enum AttemptError {
    Limited,
    Unavailable,
}

pub(crate) struct AttemptLimiter {
    login: Mutex<VecDeque<Instant>>,
    setup: Mutex<VecDeque<Instant>>,
    limit: usize,
    window: Duration,
}

impl Default for AttemptLimiter {
    fn default() -> Self {
        Self {
            login: Mutex::new(VecDeque::new()),
            setup: Mutex::new(VecDeque::new()),
            limit: AUTH_ATTEMPTS_PER_MINUTE,
            window: Duration::from_secs(60),
        }
    }
}

impl AttemptLimiter {
    pub(crate) fn record(&self, kind: AttemptKind) -> std::result::Result<(), AttemptError> {
        let attempts = match kind {
            AttemptKind::Login => &self.login,
            AttemptKind::Setup => &self.setup,
        };
        let mut attempts = attempts.lock().map_err(|_| AttemptError::Unavailable)?;
        let now = Instant::now();
        while attempts
            .front()
            .is_some_and(|then| now.duration_since(*then) >= self.window)
        {
            attempts.pop_front();
        }
        if attempts.len() >= self.limit {
            return Err(AttemptError::Limited);
        }
        attempts.push_back(now);
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct PasswordWorkError;

impl std::fmt::Display for PasswordWorkError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("password work failed")
    }
}

impl std::error::Error for PasswordWorkError {}

fn argon2id() -> std::result::Result<Argon2<'static>, PasswordWorkError> {
    let params = Params::new(
        PASSWORD_MEMORY_KIB,
        PASSWORD_TIME_COST,
        PASSWORD_PARALLELISM,
        None,
    )
    .map_err(|_| PasswordWorkError)?;
    Ok(Argon2::new(Algorithm::Argon2id, Version::V0x13, params))
}

pub(crate) async fn hash_password(
    permit: Arc<Semaphore>,
    password: String,
) -> std::result::Result<String, PasswordWorkError> {
    let permit = permit
        .acquire_owned()
        .await
        .map_err(|_| PasswordWorkError)?;
    tokio::task::spawn_blocking(move || {
        // The owned permit stays with the blocking task even if its HTTP future is cancelled.
        let _permit = permit;
        let salt =
            SaltString::encode_b64(&rand::random::<[u8; 16]>()).map_err(|_| PasswordWorkError)?;
        argon2id()?
            .hash_password(password.as_bytes(), &salt)
            .map(|hash| hash.to_string())
            .map_err(|_| PasswordWorkError)
    })
    .await
    .map_err(|_| PasswordWorkError)?
}

pub(crate) async fn verify_password(
    permit: Arc<Semaphore>,
    password: String,
    stored_hash: Option<String>,
) -> std::result::Result<bool, PasswordWorkError> {
    let user_exists = stored_hash.is_some();
    let encoded = stored_hash.unwrap_or_else(|| DUMMY_PASSWORD_HASH.to_owned());
    let permit = permit
        .acquire_owned()
        .await
        .map_err(|_| PasswordWorkError)?;
    let verified = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let parsed = PasswordHash::new(&encoded).map_err(|_| PasswordWorkError)?;
        Ok::<_, PasswordWorkError>(
            argon2id()?
                .verify_password(password.as_bytes(), &parsed)
                .is_ok(),
        )
    })
    .await
    .map_err(|_| PasswordWorkError)??;
    Ok(user_exists && verified)
}

pub(crate) fn normalize_credentials(email: &str, password: &str) -> Option<String> {
    if email.is_empty()
        || email.len() > 254
        || !email.contains('@')
        || email.chars().any(char::is_whitespace)
        || !(12..=128).contains(&password.len())
    {
        return None;
    }
    let normalized = email.to_lowercase();
    (normalized.len() <= 254).then_some(normalized)
}

pub(crate) fn valid_login_lengths(email: &str, password: &str) -> bool {
    !email.is_empty() && email.len() <= 254 && password.len() <= 128
}

pub(crate) fn random_token() -> String {
    let bytes = rand::random::<[u8; 32]>();
    let mut token = String::with_capacity(64);
    for byte in bytes {
        write!(token, "{byte:02x}").expect("writing to a String cannot fail");
    }
    token
}

pub(crate) fn hash_token(token: &str) -> String {
    format!("{:x}", Sha256::digest(token.as_bytes()))
}

pub(crate) fn csrf_token(session_token: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"eventglass-csrf-v1:");
    digest.update(session_token.as_bytes());
    format!("{:x}", digest.finalize())
}

pub(crate) fn secure_eq(left: &str, right: &str) -> bool {
    left.len() == right.len() && left.as_bytes().ct_eq(right.as_bytes()).into()
}

pub async fn issue_setup_token(app: &AppState) -> anyhow::Result<String> {
    let token = random_token();
    let token_hash = hash_token(&token);
    let expires_at_us = crate::model::now_us()?
        .checked_add(i64::try_from(SETUP_TOKEN_TTL.as_micros())?)
        .ok_or_else(|| anyhow::anyhow!("setup token expiry overflow"))?;
    app.db
        .call(move |db| {
            crate::db::auth::issue_setup_token(
                db,
                &token_hash,
                expires_at_us,
                crate::model::now_us()?,
            )
        })
        .await?;
    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attempt_kinds_have_separate_bounded_queues() {
        let limiter = AttemptLimiter {
            login: Mutex::new(VecDeque::new()),
            setup: Mutex::new(VecDeque::new()),
            limit: 1,
            window: Duration::from_secs(60),
        };
        assert!(limiter.record(AttemptKind::Login).is_ok());
        assert!(matches!(
            limiter.record(AttemptKind::Login),
            Err(AttemptError::Limited)
        ));
        assert!(limiter.record(AttemptKind::Setup).is_ok());
    }

    #[test]
    fn token_equality_checks_length_and_content() {
        assert!(secure_eq("same", "same"));
        assert!(!secure_eq("same", "different"));
        assert!(!secure_eq("short", "shorter"));
    }
}
