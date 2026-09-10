//! Search HTTP contract: authentication, canonical scope, opaque paging, safe projection.
use super::ApiJson;

use super::{ApiError, ApiResult, HttpState, auth::authenticate};
use crate::{
    db::search::{self as authorization, Authorization, ScopeError},
    search::{
        query::{self, KeywordField, QueryScope, RowCursor, SearchRequest, TimeField, TypedFilter},
        tokens::{Position, TokenContext, TokenError, TokenKind},
    },
};
use axum::{
    Json,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

#[derive(Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub(super) enum ProjectId {
    Decimal(String),
    Number(i64),
}
impl ProjectId {
    pub(super) fn parse(self) -> ApiResult<i64> {
        let value = match self {
            Self::Number(value) => value,
            Self::Decimal(value)
                if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) =>
            {
                value.parse().map_err(|_| invalid())?
            }
            _ => return Err(invalid()),
        };
        if value <= 0 {
            return Err(invalid());
        }
        Ok(value)
    }
}
#[derive(Default, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct Filters {
    kinds: Vec<String>,
    services: Vec<String>,
    levels: Vec<String>,
    environments: Vec<String>,
    releases: Vec<String>,
    loggers: Vec<String>,
}
impl Filters {
    pub(super) fn canonicalize(&mut self) -> ApiResult<()> {
        for values in [
            &mut self.kinds,
            &mut self.services,
            &mut self.levels,
            &mut self.environments,
            &mut self.releases,
            &mut self.loggers,
        ] {
            if values.len() > 64 || values.iter().any(|v| v.len() > 8192) {
                return Err(invalid());
            }
            values.sort();
            values.dedup();
        }
        if self
            .kinds
            .iter()
            .any(|v| !matches!(v.as_str(), "log" | "error"))
        {
            return Err(invalid());
        }
        Ok(())
    }
    pub(super) fn native(&self) -> Vec<TypedFilter> {
        [
            (KeywordField::Kind, &self.kinds),
            (KeywordField::Service, &self.services),
            (KeywordField::Level, &self.levels),
            (KeywordField::Environment, &self.environments),
            (KeywordField::Release, &self.releases),
            (KeywordField::Logger, &self.loggers),
        ]
        .into_iter()
        .filter(|(_, values)| !values.is_empty())
        .map(|(field, values)| TypedFilter::KeywordAny {
            field,
            values: values.clone(),
        })
        .collect()
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SearchInput {
    #[serde(default)]
    projects: Vec<ProjectId>,
    start: String,
    end: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    filters: Filters,
    limit: Option<usize>,
    cursor: Option<String>,
    read_token: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LogsInput {
    /// Comma-separated decimal project IDs; omitted means all active projects.
    projects: Option<String>,
    start: String,
    end: String,
    query: Option<String>,
    /// JSON-encoded fixed filter object, identical to POST.
    filters: Option<String>,
    limit: Option<usize>,
    cursor: Option<String>,
    read_token: Option<String>,
}
fn invalid() -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, "invalid_search_request")
}
fn unavailable() -> ApiError {
    ApiError(StatusCode::SERVICE_UNAVAILABLE, "search_unavailable")
}
pub(super) fn token_error(error: TokenError) -> ApiError {
    match error {
        TokenError::Invalid => ApiError(StatusCode::BAD_REQUEST, "invalid_search_token"),
        TokenError::Expired => ApiError(StatusCode::GONE, "search_token_expired"),
        TokenError::GenerationChanged => {
            ApiError(StatusCode::CONFLICT, "storage_generation_changed")
        }
        TokenError::AuthorizationChanged => {
            ApiError(StatusCode::FORBIDDEN, "search_authorization_changed")
        }
    }
}
pub(super) fn scope_error(error: anyhow::Error) -> ApiError {
    match error.downcast_ref::<ScopeError>() {
        Some(ScopeError::Forbidden) => ApiError(StatusCode::FORBIDDEN, "search_access_denied"),
        Some(ScopeError::Invalid | ScopeError::TooLarge) => invalid(),
        None => unavailable(),
    }
}
pub(super) fn context(auth: &Authorization, request_hash: String) -> TokenContext {
    TokenContext {
        storage_generation: auth.storage_generation.clone(),
        authorization_epoch: auth.epoch,
        authorization_hash: auth.hash.clone(),
        request_hash,
    }
}
pub(super) fn timestamp(value: &str) -> ApiResult<i64> {
    let date = time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
        .map_err(|_| invalid())?;
    let us = i64::try_from(date.unix_timestamp_nanos() / 1000).map_err(|_| invalid())?;
    us.checked_mul(1000).ok_or_else(invalid)?;
    Ok(us)
}
pub(super) fn hash(value: &impl Serialize) -> ApiResult<String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(value).map_err(|_| invalid())?)
    ))
}
pub(super) fn format_timestamp(value: i64) -> ApiResult<String> {
    time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(value) * 1000)
        .map_err(|_| unavailable())?
        .format(&time::format_description::well_known::Rfc3339)
        .map_err(|_| unavailable())
}
pub(super) async fn post_search(
    State(state): State<HttpState>,
    headers: HeaderMap,
    ApiJson(input): ApiJson<SearchInput>,
) -> ApiResult<Json<Value>> {
    execute(state, headers, input).await
}
pub(super) async fn get_logs(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Query(input): Query<LogsInput>,
) -> ApiResult<Json<Value>> {
    let projects = input
        .projects
        .map(|s| {
            s.split(',')
                .map(|v| ProjectId::Decimal(v.to_owned()))
                .collect()
        })
        .unwrap_or_default();
    let filters = input
        .filters
        .map(|s| serde_json::from_str(&s).map_err(|_| invalid()))
        .transpose()?
        .unwrap_or_default();
    execute(
        state,
        headers,
        SearchInput {
            projects,
            start: input.start,
            end: input.end,
            query: input.query.unwrap_or_default(),
            filters,
            limit: input.limit,
            cursor: input.cursor,
            read_token: input.read_token,
        },
    )
    .await
}
async fn execute(
    state: HttpState,
    headers: HeaderMap,
    input: SearchInput,
) -> ApiResult<Json<Value>> {
    let started = std::time::Instant::now();
    let limit = valid_limit(input.limit.unwrap_or(100))?;
    let has_read_token = input.read_token.is_some();
    let prepared = capture_read(
        &state,
        &headers,
        ReadInput {
            projects: input.projects,
            start: input.start,
            end: input.end,
            query: input.query,
            filters: input.filters,
            read_token: input.read_token,
        },
    )
    .await?;
    let ReadView {
        auth,
        active_boundary,
        candidate_ids,
        read_context,
        mut scope,
        query: query_text,
        filters,
    } = prepared;
    let rows_context = context(
        &auth,
        hash(&(&read_context.request_hash, limit, "timestamp_desc_seq_desc"))?,
    );
    let now = crate::model::now_us()?;
    let mut watermark = scope.watermark;
    let cursor = if let Some(token) = input.cursor {
        let verified = state
            .app
            .tokens
            .verify(&token, TokenKind::Rows, &rows_context, now)
            .map_err(token_error)?;
        matching_watermark(has_read_token, verified.watermark, watermark)?;
        watermark = verified.watermark;
        Some(row_cursor(verified.position)?)
    } else {
        None
    };
    published_watermark(watermark, active_boundary.ingest_seq)?;
    let hydrated_shards = hydrate_candidates(&state, &candidate_ids).await?;
    let permit = state
        .app
        .query_permit
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError(StatusCode::TOO_MANY_REQUESTS, "query_busy"))?;
    scope.watermark = watermark;
    let request = SearchRequest {
        query: query_text,
        scope,
        filters,
        cursor,
        limit,
    };
    let indexer = state.app.indexer.clone().ok_or_else(unavailable)?;
    let searched_shards = candidate_ids.len();
    let page = super::run_native(permit, std::time::Duration::from_secs(10), move || {
        indexer.search(&candidate_ids, &request)
    })
    .await
    .map_err(search_task_failure)?
    .map_err(search_error)?;
    revalidate_read(&state, &headers, &auth).await?;
    let now = crate::model::now_us()?;
    let next_cursor = if page.has_more {
        let last = page.rows.last().ok_or_else(unavailable)?;
        Some(
            state
                .app
                .tokens
                .issue(
                    rows_context,
                    watermark,
                    Position::Rows {
                        timestamp_us: last.timestamp_us,
                        ingest_seq: last.ingest_seq,
                        record_id: last.record_id.clone(),
                    },
                    now,
                )
                .map_err(token_error)?,
        )
    } else {
        None
    };
    let mut rows = Vec::with_capacity(page.rows.len());
    for row in page.rows {
        let detail_token = state
            .app
            .tokens
            .issue(
                context(&auth, "record-detail-v1".into()),
                watermark,
                Position::Detail {
                    project_id: row.project_id,
                    shard_id: row.shard_id.clone(),
                    record_id: row.record_id.clone(),
                },
                now,
            )
            .map_err(token_error)?;
        let timestamp = format_timestamp(row.timestamp_us)?;
        let received_at = format_timestamp(row.received_at_us)?;
        let mut value = serde_json::to_value(&row).map_err(|_| unavailable())?;
        let object = value.as_object_mut().ok_or_else(unavailable)?;
        object.remove("shard_id");
        object.remove("timestamp_us");
        object.remove("received_at_us");
        object.insert("project_id".into(), json!(row.project_id.to_string()));
        object.insert("ingest_seq".into(), json!(row.ingest_seq.to_string()));
        object.insert("timestamp".into(), json!(timestamp));
        object.insert("received_at".into(), json!(received_at));
        object.insert("detail_token".into(), json!(detail_token));
        rows.push(value);
    }
    let read_token = state
        .app
        .tokens
        .issue(read_context, watermark, Position::Read, now)
        .map_err(token_error)?;
    Ok(Json(
        json!({"rows":rows,"next_cursor":next_cursor,"read_token":read_token,"watermark":watermark.to_string(),
        "complete":true,"took_ms":started.elapsed().as_millis().to_string(),"searched_shards":searched_shards.to_string(),"hydrated_shards":hydrated_shards.to_string()}),
    ))
}

fn valid_limit(limit: usize) -> ApiResult<usize> {
    (1..=1000)
        .contains(&limit)
        .then_some(limit)
        .ok_or_else(invalid)
}

fn matching_watermark(has_read_token: bool, cursor: i64, read: i64) -> ApiResult<()> {
    if has_read_token && cursor != read {
        Err(invalid())
    } else {
        Ok(())
    }
}

fn row_cursor(position: Position) -> ApiResult<RowCursor> {
    match position {
        Position::Rows {
            timestamp_us,
            ingest_seq,
            record_id,
        } => Ok(RowCursor {
            timestamp_us,
            ingest_seq,
            record_id,
        }),
        _ => Err(invalid()),
    }
}

fn published_watermark(requested: i64, published: i64) -> ApiResult<()> {
    if requested > published {
        Err(unavailable())
    } else {
        Ok(())
    }
}

fn search_task_failure(failure: super::NativeTaskFailure) -> ApiError {
    match failure {
        super::NativeTaskFailure::Timeout => ApiError(StatusCode::GATEWAY_TIMEOUT, "query_timeout"),
        super::NativeTaskFailure::Join => unavailable(),
    }
}

fn search_error(error: anyhow::Error) -> ApiError {
    if error
        .downcast_ref::<query::SearchError>()
        .is_some_and(query::SearchError::is_bad_request)
    {
        invalid()
    } else if error
        .downcast_ref::<query::SearchError>()
        .is_some_and(query::SearchError::is_unprocessable)
    {
        ApiError(StatusCode::UNPROCESSABLE_ENTITY, "search_result_too_large")
    } else {
        unavailable()
    }
}

pub(super) async fn hydrate_candidates(
    state: &HttpState,
    shard_ids: &[String],
) -> ApiResult<usize> {
    let Some(cold) = &state.app.cold else {
        return Ok(0);
    };
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        cold.ensure_local(shard_ids),
    )
    .await
    .map_err(|_| ApiError(StatusCode::GATEWAY_TIMEOUT, "cold_storage_timeout"))?
    .map_err(|_| unavailable())
}

/// Common read scope for rows and aggregation. Operation-specific parameters do not enter read hash.
pub(super) struct ReadInput {
    pub projects: Vec<ProjectId>,
    pub start: String,
    pub end: String,
    pub query: String,
    pub filters: Filters,
    pub read_token: Option<String>,
}
pub(super) struct ReadView {
    pub auth: Authorization,
    pub active_boundary: crate::model::Boundary,
    pub candidate_ids: Vec<String>,
    pub read_context: TokenContext,
    pub scope: QueryScope,
    pub query: String,
    pub filters: Vec<TypedFilter>,
}
pub(super) async fn capture_read(
    state: &HttpState,
    headers: &HeaderMap,
    mut input: ReadInput,
) -> ApiResult<ReadView> {
    let principal = authenticate(state, headers, false, false).await?;
    let start_us = timestamp(&input.start)?;
    let end_us = timestamp(&input.end)?;
    valid_read_bounds(start_us, end_us, input.projects.len())?;
    input.filters.canonicalize()?;
    let requested = input
        .projects
        .into_iter()
        .map(ProjectId::parse)
        .collect::<ApiResult<Vec<_>>>()?;
    let auth = state
        .app
        .db
        .call(move |db| authorization::capture(db, principal.id, requested))
        .await
        .map_err(scope_error)?;
    let published = state
        .app
        .indexer
        .as_ref()
        .ok_or_else(unavailable)?
        .snapshot()
        .map_err(|_| unavailable())?;
    let read_context = context(
        &auth,
        hash(&(
            &input.query,
            &auth.projects,
            start_us,
            end_us,
            &input.filters,
            "timestamp",
        ))?,
    );
    let watermark = if let Some(token) = input.read_token {
        state
            .app
            .tokens
            .verify(
                &token,
                TokenKind::Read,
                &read_context,
                crate::model::now_us()?,
            )
            .map_err(token_error)?
            .watermark
    } else {
        published.boundary.ingest_seq
    };
    published_watermark(watermark, published.boundary.ingest_seq)?;
    let candidate_ids = state
        .app
        .db
        .call(move |db| authorization::event_candidates(db, start_us, end_us, watermark))
        .await
        .map_err(scope_error)?;
    let scope = QueryScope {
        project_ids: auth.projects.clone(),
        start_us,
        end_us,
        watermark,
        time_field: TimeField::Timestamp,
    };
    Ok(ReadView {
        auth,
        active_boundary: published.boundary,
        candidate_ids,
        read_context,
        scope,
        query: input.query,
        filters: input.filters.native(),
    })
}
pub(super) async fn revalidate_read(
    state: &HttpState,
    headers: &HeaderMap,
    auth: &Authorization,
) -> ApiResult<()> {
    // Session revocation as well as global permission changes invalidate in-flight reads.
    let current_principal = authenticate(state, headers, false, false).await?;
    let selected = auth.projects.clone();
    let current = state
        .app
        .db
        .call(move |db| authorization::capture(db, current_principal.id, selected))
        .await
        .map_err(scope_error)?;
    compare_read_authorization(auth, &current)
}

fn valid_read_bounds(start: i64, end: i64, projects: usize) -> ApiResult<()> {
    if start >= end || projects > 1000 {
        Err(invalid())
    } else {
        Ok(())
    }
}

fn compare_read_authorization(before: &Authorization, after: &Authorization) -> ApiResult<()> {
    if after.storage_generation != before.storage_generation {
        Err(token_error(TokenError::GenerationChanged))
    } else if after.hash != before.hash
        || after.epoch != before.epoch
        || after.projects != before.projects
    {
        Err(token_error(TokenError::AuthorizationChanged))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_dto_helpers_reject_ambiguous_values_and_map_all_errors() {
        assert_eq!(ProjectId::Number(7).parse().unwrap(), 7);
        assert_eq!(ProjectId::Decimal("8".into()).parse().unwrap(), 8);
        for project in [
            ProjectId::Number(0),
            ProjectId::Number(-1),
            ProjectId::Decimal(String::new()),
            ProjectId::Decimal("+1".into()),
            ProjectId::Decimal("999999999999999999999999".into()),
        ] {
            assert_eq!(project.parse().unwrap_err().1, "invalid_search_request");
        }

        let mut filters = Filters {
            kinds: vec!["log".into(), "error".into(), "log".into()],
            services: vec!["service".into()],
            levels: vec!["warning".into()],
            environments: vec!["production".into()],
            releases: vec!["v1".into()],
            loggers: vec!["app".into()],
        };
        filters.canonicalize().unwrap();
        assert_eq!(filters.kinds, ["error", "log"]);
        assert_eq!(filters.native().len(), 6);
        filters.kinds = vec!["transaction".into()];
        assert!(filters.canonicalize().is_err());
        filters.kinds.clear();
        filters.services = vec!["x".into(); 65];
        assert!(filters.canonicalize().is_err());
        filters.services = vec!["x".repeat(8193)];
        assert!(filters.canonicalize().is_err());

        for (error, status, code) in [
            (
                TokenError::Invalid,
                StatusCode::BAD_REQUEST,
                "invalid_search_token",
            ),
            (
                TokenError::Expired,
                StatusCode::GONE,
                "search_token_expired",
            ),
            (
                TokenError::GenerationChanged,
                StatusCode::CONFLICT,
                "storage_generation_changed",
            ),
            (
                TokenError::AuthorizationChanged,
                StatusCode::FORBIDDEN,
                "search_authorization_changed",
            ),
        ] {
            let mapped = token_error(error);
            assert_eq!((mapped.0, mapped.1), (status, code));
        }
        for (error, status, code) in [
            (
                anyhow::Error::from(ScopeError::Forbidden),
                StatusCode::FORBIDDEN,
                "search_access_denied",
            ),
            (
                anyhow::Error::from(ScopeError::Invalid),
                StatusCode::BAD_REQUEST,
                "invalid_search_request",
            ),
            (
                anyhow::Error::from(ScopeError::TooLarge),
                StatusCode::BAD_REQUEST,
                "invalid_search_request",
            ),
            (
                anyhow::anyhow!("private"),
                StatusCode::SERVICE_UNAVAILABLE,
                "search_unavailable",
            ),
        ] {
            let mapped = scope_error(error);
            assert_eq!((mapped.0, mapped.1), (status, code));
        }
    }

    #[test]
    fn timestamp_hash_and_token_context_helpers_preserve_exact_values() {
        assert_eq!(timestamp("1970-01-01T00:00:01.123456Z").unwrap(), 1_123_456);
        assert!(timestamp("not-a-time").is_err());
        assert!(format_timestamp(i64::MAX).is_err());
        assert_eq!(format_timestamp(0).unwrap(), "1970-01-01T00:00:00Z");
        assert_eq!(hash(&serde_json::json!({"a": 1})).unwrap().len(), 64);

        struct Failing;
        impl Serialize for Failing {
            fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                Err(serde::ser::Error::custom("fixture"))
            }
        }
        assert!(hash(&Failing).is_err());
        let authorization = Authorization {
            projects: vec![1],
            storage_generation: "generation".into(),
            epoch: 3,
            hash: "authorization".into(),
        };
        let context = context(&authorization, "request".into());
        assert_eq!(context.storage_generation, "generation");
        assert_eq!(context.authorization_epoch, 3);
        assert_eq!(context.authorization_hash, "authorization");
        assert_eq!(context.request_hash, "request");

        assert_eq!(valid_limit(1).unwrap(), 1);
        assert!(valid_limit(0).is_err());
        assert!(valid_limit(1001).is_err());
        assert!(matching_watermark(false, 2, 1).is_ok());
        assert!(matching_watermark(true, 2, 1).is_err());
        assert!(published_watermark(1, 1).is_ok());
        assert!(published_watermark(2, 1).is_err());
        assert!(valid_read_bounds(0, 1, 1000).is_ok());
        assert!(valid_read_bounds(1, 1, 0).is_err());
        assert!(valid_read_bounds(0, 1, 1001).is_err());
        assert!(row_cursor(Position::Read).is_err());
        let cursor = row_cursor(Position::Rows {
            timestamp_us: 1,
            ingest_seq: 2,
            record_id: "r".into(),
        })
        .unwrap();
        assert_eq!((cursor.timestamp_us, cursor.ingest_seq), (1, 2));
        assert_eq!(
            search_task_failure(super::super::NativeTaskFailure::Timeout).0,
            StatusCode::GATEWAY_TIMEOUT
        );
        assert_eq!(
            search_task_failure(super::super::NativeTaskFailure::Join).0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            search_error(anyhow::Error::from(query::SearchError::InvalidQuery)).0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            search_error(anyhow::Error::from(query::SearchError::ResultTooLarge {
                limit_bytes: 1
            }))
            .0,
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(
            search_error(anyhow::anyhow!("private")).0,
            StatusCode::SERVICE_UNAVAILABLE
        );

        assert!(compare_read_authorization(&authorization, &authorization).is_ok());
        let mut changed = authorization.clone();
        changed.storage_generation.push('x');
        assert!(compare_read_authorization(&authorization, &changed).is_err());
        let mut changed = authorization.clone();
        changed.epoch += 1;
        assert!(compare_read_authorization(&authorization, &changed).is_err());
    }
}
