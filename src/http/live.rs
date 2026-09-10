//! Bounded received-time SSE catch-up driven by Indexer publication notifications.

use super::{
    ApiError, ApiResult, HttpState,
    auth::authenticate,
    search::{
        Filters, ProjectId, context, format_timestamp, hash, scope_error, timestamp, token_error,
    },
};
use crate::{
    db::search as authorization,
    search::{
        query::{self, QueryScope, SearchRequest, TimeField},
        tokens::{Position, TokenContext, TokenKind},
    },
};
use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{convert::Infallible, time::Duration};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

const PAGE_SIZE: usize = 1_000;
const MAX_CATCH_UP_RECORDS: usize = 10_000;
const MAX_EVENT_BYTES: usize = 256 * 1024;

enum CatchUp {
    Complete,
    ResyncRequired,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LiveInput {
    projects: Option<String>,
    start: String,
    end: String,
    query: Option<String>,
    filters: Option<String>,
    resume: Option<String>,
}

struct LiveSession {
    auth: authorization::Authorization,
    context: TokenContext,
    start_us: i64,
    end_us: i64,
    query: String,
    filters: Filters,
    scan_seq: i64,
}

pub(super) async fn live(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Query(input): Query<LiveInput>,
) -> ApiResult<Response> {
    let permit = state
        .app
        .live_permit
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError(StatusCode::TOO_MANY_REQUESTS, "live_connection_limit"))?;
    let indexer = state.app.indexer.clone().ok_or(ApiError(
        StatusCode::SERVICE_UNAVAILABLE,
        "live_unavailable",
    ))?;
    // Subscribe before reading W so a publish racing setup is either in catch-up or queued.
    let updates = indexer.subscribe();
    let session = prepare(&state, &headers, input).await?;
    let (sender, receiver) = mpsc::channel(1);
    tokio::spawn(async move {
        let _permit = permit;
        if let Err(code) = produce(state, headers, indexer, updates, session, &sender).await {
            let data = json!({"code":code}).to_string();
            let _ = send_json(&sender, "error", None, &data).await;
        }
    });
    Ok(Sse::new(ReceiverStream::new(receiver))
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("eventglass-live"),
        )
        .into_response())
}

async fn prepare(
    state: &HttpState,
    headers: &HeaderMap,
    input: LiveInput,
) -> ApiResult<LiveSession> {
    let principal = authenticate(state, headers, false, false).await?;
    let start_us = timestamp(&input.start)?;
    let end_us = timestamp(&input.end)?;
    valid_live_window(start_us, end_us)?;
    let projects = input
        .projects
        .map(|projects| {
            projects
                .split(',')
                .map(|project| ProjectId::Decimal(project.to_owned()).parse())
                .collect::<ApiResult<Vec<_>>>()
        })
        .transpose()?
        .unwrap_or_default();
    let mut filters: Filters = input
        .filters
        .map(|filters| parse_live_filters(&filters))
        .transpose()?
        .unwrap_or_default();
    filters.canonicalize()?;
    let query = input.query.unwrap_or_default();
    let auth = state
        .app
        .db
        .call(move |database| authorization::capture(database, principal.id, projects))
        .await
        .map_err(scope_error)?;
    let live_context = context(
        &auth,
        hash(&(
            &query,
            &auth.projects,
            start_us,
            end_us,
            &filters,
            "received_at_asc",
        ))?,
    );
    let header_resume = headers
        .get("last-event-id")
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid_live_request"))
        })
        .transpose()?;
    matching_resume(input.resume.as_deref(), header_resume.as_deref())?;
    let resume = input.resume.or(header_resume);
    let scan_seq = if let Some(resume) = resume {
        let verified = state
            .app
            .tokens
            .verify(
                &resume,
                TokenKind::Live,
                &live_context,
                crate::model::now_us()?,
            )
            .map_err(token_error)?;
        live_scan_seq(verified.position)?
    } else {
        0
    };
    Ok(LiveSession {
        auth,
        context: live_context,
        start_us,
        end_us,
        query,
        filters,
        scan_seq,
    })
}

fn invalid_live() -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, "invalid_live_request")
}

fn valid_live_window(start: i64, end: i64) -> ApiResult<()> {
    (start < end).then_some(()).ok_or_else(invalid_live)
}

fn parse_live_filters(value: &str) -> ApiResult<Filters> {
    serde_json::from_str(value).map_err(|_| invalid_live())
}

fn matching_resume(query: Option<&str>, header: Option<&str>) -> ApiResult<()> {
    if query.is_some() && header.is_some() && query != header {
        Err(invalid_live())
    } else {
        Ok(())
    }
}

fn live_scan_seq(position: Position) -> ApiResult<i64> {
    match position {
        Position::Live { scan_seq } => Ok(scan_seq),
        _ => Err(invalid_live()),
    }
}

async fn produce(
    state: HttpState,
    headers: HeaderMap,
    indexer: crate::indexer::Indexer,
    mut updates: tokio::sync::broadcast::Receiver<i64>,
    mut session: LiveSession,
    sender: &mpsc::Sender<Result<Event, Infallible>>,
) -> Result<(), &'static str> {
    loop {
        super::search::revalidate_read(&state, &headers, &session.auth)
            .await
            .map_err(|_| "live_authorization_changed")?;
        let target = indexer
            .snapshot()
            .map_err(|_| "live_unavailable")?
            .boundary
            .ingest_seq;
        if target < session.scan_seq {
            send_json(sender, "resync_required", None, "{}").await?;
            return Ok(());
        }
        if target > session.scan_seq {
            if matches!(
                catch_up(&state, &headers, &indexer, &mut session, target, sender).await?,
                CatchUp::ResyncRequired
            ) {
                return Ok(());
            }
        } else if session.scan_seq == 0 {
            checkpoint(&state, &session, target, sender).await?;
        }
        tokio::select! {
            _ = sender.closed() => return Ok(()),
            update = updates.recv() => match update {
                Ok(_) => {},
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    send_json(sender, "resync_required", None, "{}").await?;
                    return Ok(());
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
            },
            _ = tokio::time::sleep(Duration::from_secs(15)) => {}
        }
    }
}

async fn catch_up(
    state: &HttpState,
    headers: &HeaderMap,
    indexer: &crate::indexer::Indexer,
    session: &mut LiveSession,
    target: i64,
    sender: &mpsc::Sender<Result<Event, Infallible>>,
) -> Result<CatchUp, &'static str> {
    let started = std::time::Instant::now();
    let mut delivered = 0usize;
    loop {
        let start_us = session.start_us;
        let end_us = session.end_us;
        let candidate_ids = state
            .app
            .db
            .call(move |database| {
                authorization::received_candidates(database, start_us, end_us, target)
            })
            .await
            .map_err(|_| "live_unavailable")?;
        super::search::hydrate_candidates(state, &candidate_ids)
            .await
            .map_err(|_| "live_cold_unavailable")?;
        let request = SearchRequest {
            query: session.query.clone(),
            scope: QueryScope {
                project_ids: session.auth.projects.clone(),
                start_us,
                end_us,
                watermark: target,
                time_field: TimeField::ReceivedAt,
            },
            filters: session.filters.native(),
            cursor: None,
            limit: PAGE_SIZE,
        };
        let after = session.scan_seq;
        let indexer = indexer.clone();
        let permit = state
            .app
            .query_permit
            .clone()
            .try_acquire_owned()
            .map_err(|_| "live_query_busy")?;
        let page = super::run_native(permit, Duration::from_secs(10), move || {
            indexer.search_live(&candidate_ids, &request, after)
        })
        .await
        .map_err(live_task_failure)?
        .map_err(|_| "live_unavailable")?;
        super::search::revalidate_read(state, headers, &session.auth)
            .await
            .map_err(|_| "live_authorization_changed")?;
        for row in page.rows {
            if delivered >= MAX_CATCH_UP_RECORDS || started.elapsed() > Duration::from_secs(10) {
                send_json(sender, "resync_required", None, "{}").await?;
                return Ok(CatchUp::ResyncRequired);
            }
            session.scan_seq = row.ingest_seq;
            delivered += 1;
            let token = live_token(state, session, target, row.ingest_seq)?;
            let value = live_row(state, session, target, row)?;
            let data = serde_json::to_string(&value).map_err(|_| "live_unavailable")?;
            send_json(sender, "record", Some(token), &data).await?;
        }
        if !page.has_more {
            session.scan_seq = target;
            checkpoint(state, session, target, sender).await?;
            return Ok(CatchUp::Complete);
        }
    }
}

fn live_task_failure(failure: super::NativeTaskFailure) -> &'static str {
    match failure {
        super::NativeTaskFailure::Timeout => "live_query_timeout",
        super::NativeTaskFailure::Join => "live_unavailable",
    }
}

fn live_row(
    state: &HttpState,
    session: &LiveSession,
    watermark: i64,
    row: query::LogRow,
) -> Result<Value, &'static str> {
    let detail_token = state
        .app
        .tokens
        .issue(
            context(&session.auth, "record-detail-v1".into()),
            watermark,
            Position::Detail {
                project_id: row.project_id,
                shard_id: row.shard_id.clone(),
                record_id: row.record_id.clone(),
            },
            crate::model::now_us().map_err(|_| "live_unavailable")?,
        )
        .map_err(|_| "live_unavailable")?;
    let mut value = serde_json::to_value(&row).map_err(|_| "live_unavailable")?;
    let object = value.as_object_mut().ok_or("live_unavailable")?;
    object.remove("shard_id");
    object.remove("timestamp_us");
    object.remove("received_at_us");
    object.insert("project_id".into(), json!(row.project_id.to_string()));
    object.insert("ingest_seq".into(), json!(row.ingest_seq.to_string()));
    object.insert(
        "timestamp".into(),
        json!(format_timestamp(row.timestamp_us).map_err(|_| "live_unavailable")?),
    );
    object.insert(
        "received_at".into(),
        json!(format_timestamp(row.received_at_us).map_err(|_| "live_unavailable")?),
    );
    object.insert("detail_token".into(), json!(detail_token));
    Ok(value)
}

async fn checkpoint(
    state: &HttpState,
    session: &LiveSession,
    watermark: i64,
    sender: &mpsc::Sender<Result<Event, Infallible>>,
) -> Result<(), &'static str> {
    let token = live_token(state, session, watermark, watermark)?;
    let data = json!({"scan_seq":watermark.to_string()}).to_string();
    send_json(sender, "checkpoint", Some(token), &data).await
}

fn live_token(
    state: &HttpState,
    session: &LiveSession,
    watermark: i64,
    scan_seq: i64,
) -> Result<String, &'static str> {
    state
        .app
        .tokens
        .issue(
            session.context.clone(),
            watermark,
            Position::Live { scan_seq },
            crate::model::now_us().map_err(|_| "live_unavailable")?,
        )
        .map_err(|_| "live_unavailable")
}

async fn send_json(
    sender: &mpsc::Sender<Result<Event, Infallible>>,
    event_name: &'static str,
    id: Option<String>,
    data: &str,
) -> Result<(), &'static str> {
    send_json_with_timeout(sender, event_name, id, data, Duration::from_secs(10)).await
}

async fn send_json_with_timeout(
    sender: &mpsc::Sender<Result<Event, Infallible>>,
    event_name: &'static str,
    id: Option<String>,
    data: &str,
    max_wait: Duration,
) -> Result<(), &'static str> {
    let encoded_bytes = event_name
        .len()
        .checked_add(id.as_ref().map_or(0, String::len))
        .and_then(|size| size.checked_add(data.len()))
        .and_then(|size| size.checked_add(32))
        .ok_or("live_record_too_large")?;
    if encoded_bytes > MAX_EVENT_BYTES {
        return Err("live_record_too_large");
    }
    let mut event = Event::default().event(event_name).data(data);
    if let Some(id) = id {
        event = event.id(id);
    }
    tokio::time::timeout(max_wait, sender.send(Ok(event)))
        .await
        .map_err(|_| "live_client_too_slow")?
        .map_err(|_| "live_client_closed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn slow_client_is_bounded_when_the_sse_queue_stays_full() {
        assert!(valid_live_window(0, 1).is_ok());
        assert!(valid_live_window(1, 1).is_err());
        assert!(parse_live_filters("{}").is_ok());
        assert!(parse_live_filters("{").is_err());
        assert!(matching_resume(None, Some("a")).is_ok());
        assert!(matching_resume(Some("a"), Some("a")).is_ok());
        assert!(matching_resume(Some("a"), Some("b")).is_err());
        assert_eq!(live_scan_seq(Position::Live { scan_seq: 7 }).unwrap(), 7);
        assert!(live_scan_seq(Position::Read).is_err());
        let (sender, _receiver) = mpsc::channel::<Result<Event, Infallible>>(1);
        sender.send(Ok(Event::default())).await.unwrap();

        assert_eq!(
            send_json_with_timeout(&sender, "record", None, "{}", Duration::from_millis(20),).await,
            Err("live_client_too_slow")
        );

        assert_eq!(
            send_json_with_timeout(
                &sender,
                "record",
                None,
                &"x".repeat(MAX_EVENT_BYTES),
                Duration::from_millis(20),
            )
            .await,
            Err("live_record_too_large")
        );
        assert_eq!(
            live_task_failure(super::super::NativeTaskFailure::Timeout),
            "live_query_timeout"
        );
        assert_eq!(
            live_task_failure(super::super::NativeTaskFailure::Join),
            "live_unavailable"
        );
    }
}
