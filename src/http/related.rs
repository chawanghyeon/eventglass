//! Correlated records derived from an authenticated stored record identity.

use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
};
use serde::Deserialize;
use serde_json::{Value, json};

use super::{
    ApiError, ApiResult, HttpState,
    auth::authenticate,
    records::{NativeDetail, compare_authorization, load_native},
    search::{context, format_timestamp, scope_error, token_error},
};
use crate::{
    db::search as authorization,
    search::{
        query::{KeywordField, QueryScope, SearchRequest, TimeField, TypedFilter},
        tokens::{Position, TokenError, TokenKind},
    },
};

const DEFAULT_WINDOW_SECONDS: u32 = 3_600;
const MAX_WINDOW_SECONDS: u32 = 86_400;
const RESULT_LIMIT: usize = 50;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RelatedInput {
    window_seconds: Option<u32>,
}

#[derive(Debug, Clone, Copy)]
enum Strategy {
    Trace,
    Request,
    UserAndService,
    ServiceErrorProximity,
}

impl Strategy {
    fn name(self) -> &'static str {
        match self {
            Self::Trace => "trace_id",
            Self::Request => "request_id",
            Self::UserAndService => "project_service_user_time",
            Self::ServiceErrorProximity => "project_service_error_time",
        }
    }

    fn exact(self) -> bool {
        matches!(self, Self::Trace | Self::Request)
    }
}

pub(super) async fn related(
    State(state): State<HttpState>,
    Path(detail_token): Path<String>,
    Query(input): Query<RelatedInput>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    let window_seconds = input.window_seconds.unwrap_or(DEFAULT_WINDOW_SECONDS);
    if !(30..=MAX_WINDOW_SECONDS).contains(&window_seconds) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_related_window"));
    }
    let principal = authenticate(&state, &headers, false, false).await?;
    let principal_id = principal.id;
    let identity = state
        .app
        .db
        .call(move |db| authorization::identity(db, principal_id))
        .await
        .map_err(scope_error)?;
    let verified = state
        .app
        .tokens
        .verify(
            &detail_token,
            TokenKind::Detail,
            &context(&identity, "record-detail-v1".into()),
            crate::model::now_us()?,
        )
        .map_err(token_error)?;
    let Position::Detail {
        project_id,
        shard_id,
        record_id,
    } = verified.position
    else {
        return Err(token_error(TokenError::Invalid));
    };
    let selected_project = project_id;
    let project_auth = state
        .app
        .db
        .call(move |db| authorization::capture(db, principal_id, vec![selected_project]))
        .await
        .map_err(scope_error)?;
    let reference = load_native(
        &state,
        shard_id,
        NativeDetail::Watermark {
            project_id,
            record_id: record_id.clone(),
            watermark: verified.watermark,
        },
    )
    .await?
    .ok_or(ApiError(StatusCode::NOT_FOUND, "record_not_found"))?;
    let seed = reference.correlation;
    let (strategy, mut project_ids, filters, effective_window) =
        correlation_strategy(&seed, project_id, window_seconds);
    let delta_us = i64::from(effective_window).saturating_mul(1_000_000);
    let start_us = seed.timestamp_us.saturating_sub(delta_us);
    let end_us = seed.timestamp_us.saturating_add(delta_us).saturating_add(1);
    if matches!(strategy, Strategy::Trace) {
        let all = state
            .app
            .db
            .call(move |db| authorization::capture(db, principal_id, Vec::new()))
            .await
            .map_err(scope_error)?;
        project_ids = all.projects;
    }
    let candidate_ids = state
        .app
        .db
        .call(move |db| authorization::event_candidates(db, start_us, end_us, verified.watermark))
        .await
        .map_err(scope_error)?;
    let permit = state
        .app
        .query_permit
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError(StatusCode::TOO_MANY_REQUESTS, "query_busy"))?;
    let request = SearchRequest {
        query: String::new(),
        scope: QueryScope {
            project_ids: project_ids.clone(),
            start_us,
            end_us,
            watermark: verified.watermark,
            time_field: TimeField::Timestamp,
        },
        filters,
        cursor: None,
        limit: RESULT_LIMIT + 1,
    };
    let indexer = state.app.indexer.clone().ok_or(ApiError(
        StatusCode::SERVICE_UNAVAILABLE,
        "search_unavailable",
    ))?;
    let page = super::run_native(permit, std::time::Duration::from_secs(10), move || {
        indexer.search(&candidate_ids, &request)
    })
    .await
    .map_err(|failure| match failure {
        super::NativeTaskFailure::Timeout => ApiError(StatusCode::GATEWAY_TIMEOUT, "query_timeout"),
        super::NativeTaskFailure::Join => {
            ApiError(StatusCode::SERVICE_UNAVAILABLE, "search_unavailable")
        }
    })?
    .map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "search_unavailable"))?;
    let selected_auth = state
        .app
        .db
        .call(move |db| authorization::capture(db, principal_id, project_ids))
        .await
        .map_err(scope_error)?;
    compare_authorization(&identity, &selected_auth)?;
    compare_authorization(&project_auth, &selected_auth)?;
    let current = authenticate(&state, &headers, false, false).await?;
    if current.id != principal_id {
        return Err(token_error(TokenError::AuthorizationChanged));
    }

    let now_us = crate::model::now_us()?;
    let search_has_more = page.has_more;
    let matched = page
        .rows
        .into_iter()
        .filter(|row| row.record_id != record_id)
        .collect::<Vec<_>>();
    let truncated = search_has_more || matched.len() > RESULT_LIMIT;
    let mut rows = Vec::new();
    for row in matched.into_iter().take(RESULT_LIMIT) {
        let detail_token = state
            .app
            .tokens
            .issue(
                context(&selected_auth, "record-detail-v1".into()),
                verified.watermark,
                Position::Detail {
                    project_id: row.project_id,
                    shard_id: row.shard_id.clone(),
                    record_id: row.record_id.clone(),
                },
                now_us,
            )
            .map_err(token_error)?;
        rows.push(json!({
            "record_id":row.record_id,
            "kind":row.kind,
            "project_id":row.project_id.to_string(),
            "ingest_seq":row.ingest_seq.to_string(),
            "timestamp":format_timestamp(row.timestamp_us)?,
            "received_at":format_timestamp(row.received_at_us)?,
            "service":row.service,
            "level":row.level,
            "message":row.message,
            "environment":row.environment,
            "release":row.release,
            "logger":row.logger,
            "trace_id":row.trace_id,
            "span_id":row.span_id,
            "request_id":row.request_id,
            "issue_id":row.issue_id,
            "fingerprint":row.fingerprint,
            "user_id":row.user_id,
            "user_email":row.user_email,
            "detail_token":detail_token,
        }));
    }
    Ok(Json(json!({
        "reference_record_id":record_id,
        "strategy":strategy.name(),
        "exact":strategy.exact(),
        "window_seconds":effective_window,
        "watermark":verified.watermark.to_string(),
        "complete":true,
        "truncated":truncated,
        "rows":rows,
    })))
}

fn keyword(field: KeywordField, value: String) -> TypedFilter {
    TypedFilter::KeywordAny {
        field,
        values: vec![value],
    }
}

fn correlation_strategy(
    seed: &crate::search::detail::CorrelationSeed,
    project_id: i64,
    window_seconds: u32,
) -> (Strategy, Vec<i64>, Vec<TypedFilter>, u32) {
    if let Some(trace_id) = &seed.trace_id {
        (
            Strategy::Trace,
            Vec::new(),
            vec![keyword(KeywordField::TraceId, trace_id.clone())],
            window_seconds,
        )
    } else if let Some(request_id) = &seed.request_id {
        (
            Strategy::Request,
            vec![project_id],
            vec![keyword(KeywordField::RequestId, request_id.clone())],
            window_seconds,
        )
    } else if let Some(user_id) = &seed.user_id {
        (
            Strategy::UserAndService,
            vec![project_id],
            vec![
                keyword(KeywordField::Service, seed.service.clone()),
                keyword(KeywordField::UserId, user_id.clone()),
            ],
            window_seconds,
        )
    } else {
        (
            Strategy::ServiceErrorProximity,
            vec![project_id],
            vec![
                keyword(KeywordField::Service, seed.service.clone()),
                keyword(KeywordField::Kind, "error".into()),
            ],
            30,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::detail::CorrelationSeed;

    fn seed() -> CorrelationSeed {
        CorrelationSeed {
            project_id: 7,
            ingest_seq: 9,
            timestamp_us: 11,
            service: "api".into(),
            trace_id: None,
            request_id: None,
            user_id: None,
        }
    }

    #[test]
    fn correlation_strategy_prefers_exact_ids_and_bounds_fallback_scope() {
        let mut value = seed();
        value.trace_id = Some("trace".into());
        let (strategy, projects, filters, window) = correlation_strategy(&value, 7, 600);
        assert_eq!(strategy.name(), "trace_id");
        assert!(strategy.exact());
        assert!(projects.is_empty());
        assert_eq!(filters.len(), 1);
        assert_eq!(window, 600);

        value.trace_id = None;
        value.request_id = Some("request".into());
        let (strategy, projects, filters, window) = correlation_strategy(&value, 7, 600);
        assert_eq!(strategy.name(), "request_id");
        assert!(strategy.exact());
        assert_eq!(projects, [7]);
        assert_eq!(filters.len(), 1);
        assert_eq!(window, 600);

        value.request_id = None;
        value.user_id = Some("user".into());
        let (strategy, projects, filters, window) = correlation_strategy(&value, 7, 600);
        assert_eq!(strategy.name(), "project_service_user_time");
        assert!(!strategy.exact());
        assert_eq!(projects, [7]);
        assert_eq!(filters.len(), 2);
        assert_eq!(window, 600);

        value.user_id = None;
        let (strategy, projects, filters, window) = correlation_strategy(&value, 7, 600);
        assert_eq!(strategy.name(), "project_service_error_time");
        assert!(!strategy.exact());
        assert_eq!(projects, [7]);
        assert_eq!(filters.len(), 2);
        assert_eq!(window, 30);
    }
}
