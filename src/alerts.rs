//! Alert configuration contracts shared by HTTP, SQLite evaluation, and delivery.

use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::{Duration, SystemTime},
};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use url::Url;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeBasis {
    ReceivedAt,
    Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Condition {
    NewIssue,
    Regression,
    ErrorCount {
        #[serde(default)]
        query: String,
        window_seconds: u32,
        threshold: u64,
        cooldown_seconds: u32,
        #[serde(default = "received_time")]
        time_basis: TimeBasis,
    },
    LogCount {
        #[serde(default)]
        query: String,
        window_seconds: u32,
        threshold: u64,
        cooldown_seconds: u32,
        #[serde(default = "received_time")]
        time_basis: TimeBasis,
    },
}

fn received_time() -> TimeBasis {
    TimeBasis::ReceivedAt
}

impl Condition {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::NewIssue => "new_issue",
            Self::Regression => "regression",
            Self::ErrorCount { .. } => "error_count",
            Self::LogCount { .. } => "log_count",
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Self::NewIssue | Self::Regression => Ok(()),
            Self::ErrorCount {
                query,
                window_seconds,
                threshold,
                cooldown_seconds,
                ..
            }
            | Self::LogCount {
                query,
                window_seconds,
                threshold,
                cooldown_seconds,
                ..
            } => {
                ensure!(query.len() <= 8 * 1024, "alert query is too long");
                crate::search::query::validate_query_text(query)
                    .map_err(|_| anyhow::anyhow!("alert query is invalid"))?;
                ensure!(
                    (60..=30 * 86_400).contains(window_seconds),
                    "alert window is outside the supported range"
                );
                ensure!(*threshold > 0, "alert threshold must be positive");
                ensure!(
                    *cooldown_seconds <= 30 * 86_400,
                    "alert cooldown is outside the supported range"
                );
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Destination {
    Webhook { url: String },
}

impl Destination {
    pub fn validate_syntax(&self) -> Result<()> {
        match self {
            Self::Webhook { url } => {
                let parsed = Url::parse(url)?;
                ensure!(parsed.scheme() == "https", "webhook URL must use HTTPS");
                ensure!(parsed.host_str().is_some(), "webhook URL must have a host");
                ensure!(
                    parsed.username().is_empty() && parsed.password().is_none(),
                    "webhook URL credentials are forbidden"
                );
                ensure!(
                    parsed.fragment().is_none(),
                    "webhook URL fragments are forbidden"
                );
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    pub name: String,
    pub project_id: Option<i64>,
    pub condition: Condition,
    pub destination: Destination,
    pub enabled: bool,
}

impl Configuration {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.name.trim().is_empty() && self.name.len() <= 200,
            "alert name is invalid"
        );
        ensure!(
            self.project_id.is_none_or(|id| id > 0),
            "project ID is invalid"
        );
        self.condition.validate()?;
        self.destination.validate_syntax()?;
        Ok(())
    }
}

const RESPONSE_LIMIT: usize = 64 * 1024;
const MAX_ATTEMPTS: u32 = 12;

#[derive(Clone)]
pub struct WebhookPolicy {
    allowed_private_hosts: HashSet<String>,
    allow_http_loopback: bool,
}

impl WebhookPolicy {
    pub fn from_env() -> Result<Self> {
        let value = std::env::var("EVENTGLASS_WEBHOOK_ALLOW_PRIVATE_HOSTS").ok();
        let allowed_private_hosts = parse_allowed_private_hosts(value.as_deref())?;
        Ok(Self {
            allowed_private_hosts,
            allow_http_loopback: false,
        })
    }

    #[cfg(test)]
    fn local_test() -> Self {
        Self {
            allowed_private_hosts: HashSet::from(["127.0.0.1".into()]),
            allow_http_loopback: true,
        }
    }

    async fn resolve(&self, raw_url: &str) -> Result<(Url, String, Vec<SocketAddr>)> {
        let url = Url::parse(raw_url)?;
        ensure!(
            url.scheme() == "https" || (self.allow_http_loopback && url.scheme() == "http"),
            "webhook transport is forbidden"
        );
        ensure!(
            url.username().is_empty() && url.password().is_none() && url.fragment().is_none(),
            "webhook URL contains forbidden components"
        );
        let host = url
            .host_str()
            .context("webhook URL is missing a host")?
            .to_ascii_lowercase();
        let port = url
            .port_or_known_default()
            .context("webhook URL has no usable port")?;
        let addresses = tokio::net::lookup_host((host.as_str(), port))
            .await?
            .collect::<Vec<_>>();
        ensure!(!addresses.is_empty(), "webhook DNS returned no addresses");
        let allow_private = self.allowed_private_hosts.contains(&host);
        ensure!(
            addresses
                .iter()
                .all(|address| allow_private || is_public(address.ip())),
            "webhook DNS resolved to a private or reserved address"
        );
        Ok((url, host, addresses))
    }
}

fn parse_allowed_private_hosts(value: Option<&str>) -> Result<HashSet<String>> {
    let mut hosts = HashSet::new();
    for host in value
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|host| !host.is_empty())
    {
        let normalized = host.to_ascii_lowercase();
        ensure!(
            normalized.len() <= 253
                && normalized.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b'.' | b'-' | b':')
                }),
            "EVENTGLASS_WEBHOOK_ALLOW_PRIVATE_HOSTS contains an invalid host"
        );
        hosts.insert(normalized);
    }
    Ok(hosts)
}

fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => public_v4(ip),
        IpAddr::V6(ip) => public_v6(ip),
    }
}

fn public_v4(ip: Ipv4Addr) -> bool {
    let [a, b, c, d] = ip.octets();
    !(a == 0
        || a == 10
        || a == 127
        || (a == 100 && (64..=127).contains(&b))
        || (a == 169 && b == 254)
        || (a == 172 && (16..=31).contains(&b))
        || (a == 192 && b == 0 && c == 0)
        || (a == 192 && b == 0 && c == 2)
        || (a == 192 && b == 88 && c == 99)
        || (a == 192 && b == 168)
        || (a == 198 && (b == 18 || b == 19))
        || (a == 198 && b == 51 && c == 100)
        || (a == 203 && b == 0 && c == 113)
        || a >= 224
        || (a == 255 && b == 255 && c == 255 && d == 255))
}

fn public_v6(ip: Ipv6Addr) -> bool {
    let segments = ip.segments();
    if let Some(mapped) = ip.to_ipv4_mapped() {
        return public_v4(mapped);
    }
    !ip.is_unspecified()
        && !ip.is_loopback()
        && segments[..6] != [0, 0, 0, 0, 0, 0]
        && !(segments[0] == 0x0064 && segments[1] == 0xff9b)
        && !(segments[0] == 0x0100 && segments[1] == 0 && segments[2] == 0 && segments[3] == 0)
        && !(segments[0] == 0x2001 && segments[1] <= 0x01ff)
        && (segments[0] & 0xfe00) != 0xfc00
        && (segments[0] & 0xffc0) != 0xfe80
        && (segments[0] & 0xffc0) != 0xfec0
        && (segments[0] & 0xff00) != 0xff00
        && !(segments[0] == 0x2001 && segments[1] == 0x0db8)
        && segments[0] != 0x2002
        && (segments[0] & 0xfff0) != 0x3ff0
        && segments[0] != 0x5f00
}

struct WebhookResponse {
    status: u16,
    retry_after: Option<Duration>,
}

#[derive(Clone)]
pub struct WebhookSender {
    policy: WebhookPolicy,
}

impl WebhookSender {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            policy: WebhookPolicy::from_env()?,
        })
    }

    #[cfg(test)]
    fn local_test() -> Self {
        Self {
            policy: WebhookPolicy::local_test(),
        }
    }

    pub async fn deliver_once(&self, db: &crate::db::worker::DbWorker) -> Result<bool> {
        let now_us = crate::model::now_us()?;
        let Some(delivery) = db
            .call(move |database| crate::db::alerts::due_delivery(database, now_us))
            .await?
        else {
            return Ok(false);
        };
        let (destination, outgoing_payload) = match delivery_payload(&delivery.payload_json) {
            Ok(payload) => payload,
            Err(_) => {
                let invalid = crate::db::alerts::DeliveryResult::Failed {
                    status: None,
                    error: "delivery_payload_invalid",
                };
                return db
                    .call(move |database| {
                        crate::db::alerts::finish_delivery(database, &delivery, invalid, now_us)
                    })
                    .await;
            }
        };
        let result = match self
            .send(&delivery.id, &outgoing_payload, &destination)
            .await
        {
            Ok(response) if (200..300).contains(&response.status) => {
                crash_point("alert.after_send");
                crate::db::alerts::DeliveryResult::Sent {
                    status: response.status,
                }
            }
            Ok(response) if retryable_status(response.status) => retry_result(
                &delivery.id,
                delivery.attempts,
                Some(response.status),
                response.retry_after,
                now_us,
                "webhook_retryable_status",
            ),
            Ok(response) => crate::db::alerts::DeliveryResult::Failed {
                status: Some(response.status),
                error: "webhook_permanent_status",
            },
            Err(_) => retry_result(
                &delivery.id,
                delivery.attempts,
                None,
                None,
                now_us,
                "webhook_transport_error",
            ),
        };
        let completed = db
            .call(move |database| {
                crate::db::alerts::finish_delivery(database, &delivery, result, now_us)
            })
            .await?;
        Ok(completed)
    }

    async fn send(
        &self,
        delivery_id: &str,
        payload: &str,
        destination: &Destination,
    ) -> Result<WebhookResponse> {
        let Destination::Webhook { url } = destination;
        let (url, host, addresses) = self.policy.resolve(url).await?;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .resolve_to_addrs(&host, &addresses)
            .build()?;
        let mut response = client
            .post(url)
            .header("content-type", "application/json")
            .header("x-eventglass-delivery", delivery_id)
            .body(payload.to_owned())
            .send()
            .await?;
        let remote = response
            .remote_addr()
            .context("webhook response did not expose its peer address")?;
        ensure!(
            addresses.iter().any(|address| address.ip() == remote.ip()),
            "webhook peer differs from the validated DNS addresses"
        );
        if response
            .content_length()
            .is_some_and(|length| length > RESPONSE_LIMIT as u64)
        {
            anyhow::bail!("webhook response exceeds the body limit");
        }
        let status = response.status().as_u16();
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(parse_retry_after);
        let mut received = 0usize;
        while let Some(chunk) = response.chunk().await? {
            received = received
                .checked_add(chunk.len())
                .context("webhook response size overflow")?;
            ensure!(
                received <= RESPONSE_LIMIT,
                "webhook response exceeds the body limit"
            );
        }
        Ok(WebhookResponse {
            status,
            retry_after,
        })
    }
}

fn delivery_payload(raw: &str) -> Result<(Destination, String)> {
    let mut value: serde_json::Value = serde_json::from_str(raw)?;
    let object = value
        .as_object_mut()
        .context("alert delivery payload must be an object")?;
    let destination = serde_json::from_value(
        object
            .remove("destination")
            .context("alert delivery destination is missing")?,
    )?;
    Ok((destination, serde_json::to_string(&value)?))
}

#[inline]
fn crash_point(name: &str) {
    #[cfg(feature = "failpoints")]
    if std::env::var("EVENTGLASS_FAILPOINT").as_deref() == Ok(name) {
        std::process::exit(86);
    }
    #[cfg(not(feature = "failpoints"))]
    let _ = name;
}

fn retryable_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 500..=599)
}

fn parse_retry_after(value: &str) -> Option<Duration> {
    let duration = value
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
        .or_else(|| {
            httpdate::parse_http_date(value)
                .ok()?
                .duration_since(SystemTime::now())
                .ok()
        })?;
    Some(duration.min(Duration::from_secs(3_600)))
}

fn retry_result(
    delivery_id: &str,
    previous_attempts: u32,
    status: Option<u16>,
    retry_after: Option<Duration>,
    now_us: i64,
    error: &'static str,
) -> crate::db::alerts::DeliveryResult {
    let attempts = previous_attempts.saturating_add(1);
    if attempts >= MAX_ATTEMPTS {
        return crate::db::alerts::DeliveryResult::Failed { status, error };
    }
    let exponent = previous_attempts.min(10);
    let base_seconds = 5u64.saturating_mul(1u64 << exponent).min(3_600);
    let digest = sha2::Sha256::digest(format!("{delivery_id}:{attempts}").as_bytes());
    let jitter = 750u64 + u64::from(u16::from_be_bytes([digest[0], digest[1]])) % 501;
    let delay = retry_after.unwrap_or_else(|| {
        Duration::from_millis(base_seconds.saturating_mul(jitter).min(3_600_000))
    });
    let next_retry_at_us = now_us.saturating_add(
        i64::try_from(delay.as_micros())
            .unwrap_or(i64::MAX)
            .min(3_600_000_000),
    );
    crate::db::alerts::DeliveryResult::Retry {
        status,
        next_retry_at_us,
        error,
    }
}

struct CoordinatorControl {
    stop: tokio::sync::watch::Sender<bool>,
    joins: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl Drop for CoordinatorControl {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        if let Ok(joins) = self.joins.get_mut() {
            for join in joins.drain(..) {
                join.abort();
            }
        }
    }
}

#[derive(Clone)]
pub struct AlertCoordinator {
    control: std::sync::Arc<CoordinatorControl>,
}

impl AlertCoordinator {
    pub fn start(
        db: crate::db::worker::DbWorker,
        indexer: crate::indexer::Indexer,
        query_permit: std::sync::Arc<tokio::sync::Semaphore>,
        cold: Option<crate::storage::cold::ColdStorage>,
    ) -> Result<Self> {
        let sender = WebhookSender::from_env()?;
        let (stop, sender_stop) = tokio::sync::watch::channel(false);
        let evaluation_stop = sender_stop.clone();
        let sender_db = db.clone();
        let sender_join = tokio::spawn(async move {
            sender_loop(sender_db, sender, sender_stop).await;
        });
        let evaluation_join = tokio::spawn(async move {
            evaluation_loop(db, indexer, query_permit, cold, evaluation_stop).await;
        });
        Ok(Self {
            control: std::sync::Arc::new(CoordinatorControl {
                stop,
                joins: std::sync::Mutex::new(vec![sender_join, evaluation_join]),
            }),
        })
    }

    pub async fn shutdown(&self) -> Result<()> {
        let _ = self.control.stop.send(true);
        let joins = self
            .control
            .joins
            .lock()
            .map_err(|_| anyhow::anyhow!("alert coordinator lock poisoned"))?
            .drain(..)
            .collect::<Vec<_>>();
        for join in joins {
            join.await.context("alert coordinator task failed")?;
        }
        Ok(())
    }
}

async fn sender_loop(
    db: crate::db::worker::DbWorker,
    sender: WebhookSender,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        if *stop.borrow() {
            return;
        }
        if let Err(error) = sender.deliver_once(&db).await {
            tracing::warn!(reason = %error, "alert delivery attempt failed before durable classification");
        }
        tokio::select! {
            changed = stop.changed() => if changed.is_err() || *stop.borrow() { return; },
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
}

async fn evaluation_loop(
    db: crate::db::worker::DbWorker,
    indexer: crate::indexer::Indexer,
    query_permit: std::sync::Arc<tokio::sync::Semaphore>,
    cold: Option<crate::storage::cold::ColdStorage>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        if *stop.borrow() {
            return;
        }
        if let Err(error) = evaluate_once(&db, &indexer, &query_permit, cold.as_ref()).await {
            tracing::warn!(reason = %error, "alert threshold evaluation pass failed");
        }
        tokio::select! {
            changed = stop.changed() => if changed.is_err() || *stop.borrow() { return; },
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
}

pub async fn evaluate_once(
    db: &crate::db::worker::DbWorker,
    indexer: &crate::indexer::Indexer,
    query_permit: &std::sync::Arc<tokio::sync::Semaphore>,
    cold: Option<&crate::storage::cold::ColdStorage>,
) -> Result<bool> {
    let now_us = crate::model::now_us()?;
    let Some(pending) = db
        .call(move |database| crate::db::alerts::reserve_evaluation(database, now_us))
        .await?
    else {
        return Ok(false);
    };
    if !cut_is_ready(indexer.snapshot()?.boundary.ingest_seq, pending.cut_seq) {
        return Ok(false);
    }
    let result = evaluate_pending(db, indexer, query_permit, cold, &pending).await;
    match result {
        Ok(count) => {
            let completed = db
                .call(move |database| {
                    crate::db::alerts::finish_evaluation(
                        database,
                        &pending,
                        count,
                        crate::model::now_us()?,
                    )
                })
                .await?;
            Ok(completed)
        }
        Err(error) => {
            tracing::warn!(alert_id = pending.alert_id, reason = %error, "alert query remains pending");
            db.call(move |database| {
                crate::db::alerts::fail_evaluation(database, &pending, "query_incomplete")
                    .map(|_| ())
            })
            .await?;
            Ok(false)
        }
    }
}

async fn evaluate_pending(
    db: &crate::db::worker::DbWorker,
    indexer: &crate::indexer::Indexer,
    query_permit: &std::sync::Arc<tokio::sync::Semaphore>,
    cold: Option<&crate::storage::cold::ColdStorage>,
    pending: &crate::db::alerts::PendingEvaluation,
) -> Result<u64> {
    use crate::search::{
        aggregate::{AggregateRequest, MetricSpec},
        query::{KeywordField, QueryScope, TimeField, TypedFilter},
    };
    let (query, window_seconds, kind, time_basis) = threshold_parts(&pending.condition)?;
    ensure!(
        pending.project_id.is_none() || !pending.project_ids.is_empty(),
        "configured alert project is inactive"
    );
    let start_us = pending
        .evaluation_end_us
        .checked_sub(i64::from(window_seconds).saturating_mul(1_000_000))
        .context("alert window overflow")?;
    let time_field = match time_basis {
        TimeBasis::ReceivedAt => TimeField::ReceivedAt,
        TimeBasis::Timestamp => TimeField::Timestamp,
    };
    let candidate_start = start_us;
    let candidate_end = pending.evaluation_end_us;
    let candidate_cut = pending.cut_seq;
    let candidate_ids = db
        .call(move |database| {
            alert_candidates(
                database,
                candidate_start,
                candidate_end,
                candidate_cut,
                time_field,
            )
        })
        .await?;
    if let Some(cold) = cold {
        tokio::time::timeout(Duration::from_secs(30), cold.ensure_local(&candidate_ids))
            .await
            .context("alert cold hydration timed out")??;
    }
    let request = AggregateRequest {
        query,
        scope: QueryScope {
            project_ids: pending.project_ids.clone(),
            start_us,
            end_us: pending.evaluation_end_us,
            watermark: pending.cut_seq,
            time_field,
        },
        filters: vec![TypedFilter::KeywordAny {
            field: KeywordField::Kind,
            values: vec![kind.into()],
        }],
        metrics: vec![MetricSpec::Count {
            name: "records".into(),
        }],
        group_by: Vec::new(),
        histogram: None,
    };
    let permit = query_permit.clone().acquire_owned().await?;
    let indexer = indexer.clone();
    let task = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        indexer
            .aggregate(&candidate_ids, &request)
            .map(|page| page.record_count)
    });
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .context("alert query timed out")?
        .context("alert query task failed")?
}

fn cut_is_ready(published: i64, required: i64) -> bool {
    published >= required
}

fn threshold_parts(condition: &Condition) -> Result<(String, u32, &'static str, TimeBasis)> {
    match condition {
        Condition::ErrorCount {
            query,
            window_seconds,
            time_basis,
            ..
        } => Ok((query.clone(), *window_seconds, "error", *time_basis)),
        Condition::LogCount {
            query,
            window_seconds,
            time_basis,
            ..
        } => Ok((query.clone(), *window_seconds, "log", *time_basis)),
        _ => anyhow::bail!("non-threshold alert reached evaluator"),
    }
}

fn alert_candidates(
    database: &rusqlite::Connection,
    start: i64,
    end: i64,
    cut: i64,
    time_field: crate::search::query::TimeField,
) -> Result<Vec<String>> {
    match time_field {
        crate::search::query::TimeField::ReceivedAt => {
            crate::db::search::received_candidates(database, start, end, cut)
        }
        crate::search::query::TimeField::Timestamp => {
            crate::db::search::event_candidates(database, start, end, cut)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn send_raw_response(response: Vec<u8>) -> Result<WebhookResponse> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0u8; 16 * 1024];
            let _ = socket.read(&mut request).await.unwrap();
            let _ = socket.write_all(&response).await;
        });
        let result = WebhookSender::local_test()
            .send(
                "bounded-response",
                "{}",
                &Destination::Webhook {
                    url: format!("http://{address}/hook"),
                },
            )
            .await;
        server.await?;
        result
    }

    fn threshold_condition(error: bool) -> Condition {
        let fields = (String::new(), 60, 1, 0, TimeBasis::ReceivedAt);
        if error {
            Condition::ErrorCount {
                query: fields.0,
                window_seconds: fields.1,
                threshold: fields.2,
                cooldown_seconds: fields.3,
                time_basis: fields.4,
            }
        } else {
            Condition::LogCount {
                query: fields.0,
                window_seconds: fields.1,
                threshold: fields.2,
                cooldown_seconds: fields.3,
                time_basis: fields.4,
            }
        }
    }

    fn config(path: &std::path::Path) -> crate::config::Config {
        crate::config::Config {
            addr: "127.0.0.1:0".parse().expect("loopback address"),
            data_dir: path.to_owned(),
            base_url: "http://localhost:8080".parse().expect("base URL"),
            s3_url: None,
            s3_endpoint: None,
            s3_initialize: false,
        }
    }

    #[test]
    fn alert_configuration_validation_covers_every_condition_and_destination_boundary() {
        assert_eq!(received_time(), TimeBasis::ReceivedAt);
        for (condition, kind) in [
            (Condition::NewIssue, "new_issue"),
            (Condition::Regression, "regression"),
            (threshold_condition(true), "error_count"),
            (threshold_condition(false), "log_count"),
        ] {
            assert_eq!(condition.kind(), kind);
            assert!(condition.validate().is_ok());
        }

        for condition in [
            Condition::ErrorCount {
                query: "(".into(),
                window_seconds: 60,
                threshold: 1,
                cooldown_seconds: 0,
                time_basis: TimeBasis::ReceivedAt,
            },
            Condition::ErrorCount {
                query: "x".repeat(8 * 1024 + 1),
                window_seconds: 60,
                threshold: 1,
                cooldown_seconds: 0,
                time_basis: TimeBasis::ReceivedAt,
            },
            Condition::LogCount {
                query: String::new(),
                window_seconds: 59,
                threshold: 1,
                cooldown_seconds: 0,
                time_basis: TimeBasis::Timestamp,
            },
            Condition::LogCount {
                query: String::new(),
                window_seconds: 60,
                threshold: 0,
                cooldown_seconds: 0,
                time_basis: TimeBasis::Timestamp,
            },
            Condition::LogCount {
                query: String::new(),
                window_seconds: 60,
                threshold: 1,
                cooldown_seconds: 30 * 86_400 + 1,
                time_basis: TimeBasis::Timestamp,
            },
        ] {
            assert!(condition.validate().is_err());
        }

        let valid = Destination::Webhook {
            url: "https://example.com/hook?tenant=one".into(),
        };
        assert!(valid.validate_syntax().is_ok());
        for url in [
            "http://example.com/hook",
            "https://",
            "https://user@example.com/hook",
            "https://example.com/hook#fragment",
        ] {
            assert!(
                Destination::Webhook { url: url.into() }
                    .validate_syntax()
                    .is_err(),
                "{url}"
            );
        }

        let mut configuration = Configuration {
            name: "production errors".into(),
            project_id: Some(1),
            condition: threshold_condition(true),
            destination: valid,
            enabled: true,
        };
        assert!(configuration.validate().is_ok());
        configuration.name = " ".into();
        assert!(configuration.validate().is_err());
        configuration.name = "x".repeat(201);
        assert!(configuration.validate().is_err());
        configuration.name = "valid".into();
        configuration.project_id = Some(0);
        assert!(configuration.validate().is_err());

        assert!(!cut_is_ready(1, 2));
        assert!(cut_is_ready(2, 2));
        assert_eq!(
            threshold_parts(&threshold_condition(true)).unwrap().2,
            "error"
        );
        let mut log = threshold_condition(false);
        if let Condition::LogCount { time_basis, .. } = &mut log {
            *time_basis = TimeBasis::Timestamp;
        }
        assert_eq!(threshold_parts(&log).unwrap().2, "log");
        assert!(threshold_parts(&Condition::NewIssue).is_err());

        let directory = tempfile::tempdir().expect("candidate database directory");
        let database =
            crate::db::open(&directory.path().join("meta.db")).expect("candidate database");
        assert!(
            alert_candidates(
                &database,
                0,
                1,
                0,
                crate::search::query::TimeField::ReceivedAt,
            )
            .unwrap()
            .is_empty()
        );
        assert!(
            alert_candidates(
                &database,
                0,
                1,
                0,
                crate::search::query::TimeField::Timestamp,
            )
            .unwrap()
            .is_empty()
        );
    }

    #[tokio::test]
    async fn coordinator_loops_stop_cleanly_and_surface_database_failures() {
        let directory = tempfile::tempdir().expect("alert loop directory");
        let app = crate::app::AppState::open(config(directory.path()))
            .await
            .expect("alert application state");
        let indexer = crate::indexer::Indexer::start(app.db.clone(), directory.path())
            .await
            .expect("alert Indexer");

        let (_stop, stopped) = tokio::sync::watch::channel(true);
        sender_loop(app.db.clone(), WebhookSender::local_test(), stopped).await;
        let (_stop, stopped) = tokio::sync::watch::channel(true);
        evaluation_loop(
            app.db.clone(),
            indexer.clone(),
            app.query_permit.clone(),
            None,
            stopped,
        )
        .await;

        app.db
            .call(|db| {
                db.execute_batch(
                    "PRAGMA foreign_keys=OFF; DROP TABLE alert_deliveries; DROP TABLE alerts",
                )
                .expect("drop alert loop tables");
                anyhow::Ok(())
            })
            .await
            .expect("damage alert loop tables");

        let (stop, receiver) = tokio::sync::watch::channel(false);
        let stop_task = tokio::spawn(async move {
            tokio::task::yield_now().await;
            let _ = stop.send(true);
        });
        sender_loop(app.db.clone(), WebhookSender::local_test(), receiver).await;
        stop_task.await.expect("sender stop task");

        let (stop, receiver) = tokio::sync::watch::channel(false);
        let stop_task = tokio::spawn(async move {
            tokio::task::yield_now().await;
            let _ = stop.send(true);
        });
        evaluation_loop(
            app.db.clone(),
            indexer.clone(),
            app.query_permit.clone(),
            None,
            receiver,
        )
        .await;
        stop_task.await.expect("evaluation stop task");

        let coordinator = AlertCoordinator::start(
            app.db.clone(),
            indexer.clone(),
            app.query_permit.clone(),
            None,
        )
        .expect("alert coordinator");
        drop(coordinator);
        indexer.shutdown().await.expect("Indexer shutdown");
    }

    #[test]
    fn private_host_allowlist_is_normalized_deduplicated_and_strict() {
        assert!(parse_allowed_private_hosts(None).unwrap().is_empty());
        let hosts = parse_allowed_private_hosts(Some(" LOCALHOST,localhost,::1 ")).unwrap();
        assert_eq!(hosts, HashSet::from(["localhost".into(), "::1".into()]));
        for invalid in ["bad host", "slash/name", &"x".repeat(254)] {
            assert!(parse_allowed_private_hosts(Some(invalid)).is_err());
        }
    }

    #[test]
    fn delivery_payload_retry_status_and_retry_after_are_strict() {
        let (destination, payload) = delivery_payload(
            r#"{"destination":{"type":"webhook","url":"https://example.com/hook"},"message":"test"}"#,
        )
        .unwrap();
        assert!(matches!(destination, Destination::Webhook { .. }));
        assert_eq!(payload, r#"{"message":"test"}"#);
        for invalid in [
            "[]",
            r#"{"message":"missing"}"#,
            r#"{"destination":{"type":"email"}}"#,
        ] {
            assert!(delivery_payload(invalid).is_err());
        }

        for status in [408, 429, 500, 599] {
            assert!(retryable_status(status));
        }
        for status in [200, 400, 499, 600] {
            assert!(!retryable_status(status));
        }
        assert_eq!(parse_retry_after("2"), Some(Duration::from_secs(2)));
        assert_eq!(parse_retry_after("9999"), Some(Duration::from_secs(3600)));
        let future = httpdate::fmt_http_date(SystemTime::now() + Duration::from_secs(120));
        assert!(parse_retry_after(&future).is_some());
        let past = httpdate::fmt_http_date(SystemTime::UNIX_EPOCH);
        assert_eq!(parse_retry_after(&past), None);
        assert_eq!(parse_retry_after("not-a-date"), None);
    }

    #[tokio::test]
    async fn webhook_resolution_rejects_forbidden_components_before_network_access() {
        let policy = WebhookPolicy::local_test();
        for url in [
            "ftp://127.0.0.1/hook",
            "http://user@127.0.0.1/hook",
            "http://127.0.0.1/hook#fragment",
            "http:///hook",
        ] {
            assert!(policy.resolve(url).await.is_err(), "{url}");
        }
        let (url, host, addresses) = policy.resolve("http://127.0.0.1:80/hook").await.unwrap();
        assert_eq!(url.path(), "/hook");
        assert_eq!(host, "127.0.0.1");
        assert_eq!(addresses, vec!["127.0.0.1:80".parse().unwrap()]);
    }

    #[test]
    fn reserved_networks_are_blocked_without_broad_standard_library_assumptions() {
        for address in [
            "0.0.0.0",
            "10.0.0.1",
            "100.64.0.1",
            "127.0.0.1",
            "169.254.169.254",
            "172.16.0.1",
            "192.0.0.1",
            "192.0.2.1",
            "192.88.99.1",
            "192.168.0.1",
            "198.18.0.1",
            "198.51.100.1",
            "203.0.113.1",
            "224.0.0.1",
            "255.255.255.255",
            "::",
            "::1",
            "::2",
            "64:ff9b::1",
            "100::1",
            "2001::1",
            "fc00::1",
            "fe80::1",
            "fec0::1",
            "ff00::1",
            "2001:db8::1",
            "2002::1",
            "3ff0::1",
            "5f00::1",
            "::ffff:127.0.0.1",
        ] {
            assert!(!is_public(address.parse().unwrap()), "{address}");
        }
        for address in ["1.1.1.1", "8.8.8.8", "2606:4700:4700::1111"] {
            assert!(is_public(address.parse().unwrap()), "{address}");
        }
    }

    #[tokio::test]
    async fn local_receiver_observes_stable_id_and_bounded_response() -> Result<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0u8; 16 * 1024];
            let read = socket.read(&mut request).await.unwrap();
            let request = std::str::from_utf8(&request[..read]).unwrap();
            assert!(request.contains("x-eventglass-delivery: stable-delivery"));
            assert!(request.contains("{\"message\":\"test\"}"));
            socket
                .write_all(
                    b"HTTP/1.1 202 Accepted\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                )
                .await
                .unwrap();
        });
        let sender = WebhookSender::local_test();
        let response = sender
            .send(
                "stable-delivery",
                "{\"message\":\"test\"}",
                &Destination::Webhook {
                    url: format!("http://{address}/hook"),
                },
            )
            .await?;
        assert_eq!(response.status, 202);
        server.await?;
        Ok(())
    }

    #[tokio::test]
    async fn webhook_response_limits_and_retry_header_are_enforced() {
        let response = send_raw_response(
            b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 2\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                .to_vec(),
        )
        .await
        .unwrap();
        assert_eq!(response.status, 429);
        assert_eq!(response.retry_after, Some(Duration::from_secs(2)));

        assert!(
            send_raw_response(
                b"HTTP/1.1 200 OK\r\nContent-Length: 65537\r\nConnection: close\r\n\r\n".to_vec()
            )
            .await
            .is_err()
        );

        let mut chunked =
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n10001\r\n"
                .to_vec();
        chunked.extend(std::iter::repeat_n(b'x', RESPONSE_LIMIT + 1));
        chunked.extend_from_slice(b"\r\n0\r\n\r\n");
        assert!(send_raw_response(chunked).await.is_err());
    }

    #[tokio::test]
    async fn dns_validation_rejects_private_destinations_without_opt_in() {
        let policy = WebhookPolicy {
            allowed_private_hosts: HashSet::new(),
            allow_http_loopback: false,
        };
        assert!(policy.resolve("https://127.0.0.1/hook").await.is_err());
    }

    #[test]
    fn retry_budget_is_bounded_and_retry_after_is_clamped() {
        let retry = retry_result(
            "delivery",
            0,
            Some(429),
            Some(Duration::from_secs(9_999)),
            1,
            "retry",
        );
        assert!(matches!(
            retry,
            crate::db::alerts::DeliveryResult::Retry {
                next_retry_at_us: 3_600_000_001,
                ..
            }
        ));
        assert!(matches!(
            retry_result("delivery", 11, None, None, 1, "retry"),
            crate::db::alerts::DeliveryResult::Failed { .. }
        ));
    }

    async fn delivery_worker(
        payload: String,
    ) -> Result<(tempfile::TempDir, crate::db::worker::DbWorker)> {
        let directory = tempfile::tempdir()?;
        let state = crate::app::AppState::open(crate::config::Config {
            addr: "127.0.0.1:0".parse()?,
            data_dir: directory.path().to_owned(),
            base_url: "http://localhost:8080".parse()?,
            s3_url: None,
            s3_endpoint: None,
            s3_initialize: false,
        })
        .await?;
        state
            .db
            .call(move |db| {
                db.execute_batch(
                    "INSERT INTO users(id,email,password_hash,role,is_active,created_at_us,updated_at_us)
                     VALUES(1,'admin@example.test','x','admin',1,0,0)",
                )
                .expect("seed alert test user");
                let configuration = Configuration {
                    name: "Delivery".into(),
                    project_id: None,
                    condition: Condition::NewIssue,
                    destination: Destination::Webhook {
                        url: "https://example.test/hook".into(),
                    },
                    enabled: true,
                };
                let alert = crate::db::alerts::create(db, 1, &configuration, 0)?;
                db.execute(
                    "INSERT INTO alert_deliveries(id,alert_id,dedupe_key,payload_json,state,attempts,
                         next_retry_at_us,created_at_us)
                     VALUES('delivery',?1,'delivery',?2,'pending',0,0,0)",
                    rusqlite::params![alert, payload],
                )
                .expect("seed alert delivery");
                Ok(())
            })
            .await?;
        Ok((directory, state.db))
    }

    async fn status_worker(
        response: Option<Vec<u8>>,
    ) -> Result<(tempfile::TempDir, crate::db::worker::DbWorker)> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        if let Some(response) = response {
            tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0; 4096];
                let _ = socket.read(&mut request).await.unwrap();
                socket.write_all(&response).await.unwrap();
            });
        } else {
            drop(listener);
        }
        delivery_worker(format!(
            r#"{{"destination":{{"type":"webhook","url":"http://{address}/hook"}},"message":"test"}}"#
        ))
        .await
    }

    async fn delivery_state(db: &crate::db::worker::DbWorker) -> Result<(String, Option<i64>)> {
        db.call(|database| {
            Ok(database
                .query_row(
                    "SELECT state,last_status_code FROM alert_deliveries WHERE id='delivery'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .expect("read delivery state"))
        })
        .await
    }

    #[tokio::test]
    async fn delivery_once_durably_classifies_payload_status_and_transport_outcomes() -> Result<()>
    {
        let sender = WebhookSender::local_test();

        let (_directory, db) = delivery_worker("{".into()).await?;
        assert!(sender.deliver_once(&db).await?);
        assert_eq!(delivery_state(&db).await?.0, "failed");
        assert!(!sender.deliver_once(&db).await?);

        for (response, state, status) in [
            (
                b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
                "sent",
                Some(204),
            ),
            (
                b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
                "failed",
                Some(400),
            ),
            (
                b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
                "pending",
                Some(429),
            ),
        ] {
            let (_directory, db) = status_worker(Some(response)).await?;
            assert!(sender.deliver_once(&db).await?);
            assert_eq!(delivery_state(&db).await?, (state.into(), status));
        }

        let (_directory, db) = status_worker(None).await?;
        assert!(sender.deliver_once(&db).await?);
        assert_eq!(delivery_state(&db).await?.0, "pending");
        Ok(())
    }
}
