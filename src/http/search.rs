//! Search HTTP contract: authentication, canonical scope, opaque paging, safe projection.
use super::{ApiError, ApiResult, HttpState, auth::authenticate};
use crate::{
    db::search::{self as authorization, Authorization, ScopeError},
    search::{
        query::{
            self, KeywordField, QueryScope, RowCursor, SearchRequest, SearchShard, TimeField,
            TypedFilter,
        },
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
    fn parse(self) -> ApiResult<i64> {
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
    fn canonicalize(&mut self) -> ApiResult<()> {
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
    fn native(&self) -> Vec<TypedFilter> {
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
fn timestamp(value: &str) -> ApiResult<i64> {
    let date = time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
        .map_err(|_| invalid())?;
    let us = i64::try_from(date.unix_timestamp_nanos() / 1000).map_err(|_| invalid())?;
    us.checked_mul(1000).ok_or_else(invalid)?;
    Ok(us)
}
fn hash(value: &impl Serialize) -> ApiResult<String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(value).map_err(|_| invalid())?)
    ))
}
fn format_timestamp(value: i64) -> ApiResult<String> {
    time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(value) * 1000)
        .map_err(|_| unavailable())?
        .format(&time::format_description::well_known::Rfc3339)
        .map_err(|_| unavailable())
}
pub(super) async fn post_search(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(input): Json<SearchInput>,
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
    let limit = input.limit.unwrap_or(100);
    if !(1..=1000).contains(&limit) {
        return Err(invalid());
    }
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
        published,
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
        if has_read_token && verified.watermark != watermark {
            return Err(invalid());
        }
        watermark = verified.watermark;
        match verified.position {
            Position::Rows {
                timestamp_us,
                ingest_seq,
                record_id,
            } => Some(RowCursor {
                timestamp_us,
                ingest_seq,
                record_id,
            }),
            _ => return Err(invalid()),
        }
    } else {
        None
    };
    if watermark > published.boundary.ingest_seq {
        return Err(unavailable());
    }
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
    let task = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        query::search(
            &[SearchShard {
                id: published.shard_id,
                searcher: published.searcher,
            }],
            &request,
        )
    });
    let page = tokio::time::timeout(std::time::Duration::from_secs(10), task)
        .await
        .map_err(|_| ApiError(StatusCode::GATEWAY_TIMEOUT, "query_timeout"))?
        .map_err(|_| unavailable())?
        .map_err(|error| {
            if error.is_bad_request() {
                invalid()
            } else if error.is_unprocessable() {
                ApiError(StatusCode::UNPROCESSABLE_ENTITY, "search_result_too_large")
            } else {
                unavailable()
            }
        })?;
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
        "complete":true,"took_ms":started.elapsed().as_millis().to_string(),"searched_shards":"1","hydrated_shards":"0"}),
    ))
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
    pub published: crate::search::active::Published,
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
    if start_us >= end_us || input.projects.len() > 1000 {
        return Err(invalid());
    }
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
    let shard_id = published.shard_id.clone();
    state
        .app
        .db
        .call(move |db| authorization::require_only_active(db, &shard_id))
        .await
        .map_err(scope_error)?;
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
    if watermark > published.boundary.ingest_seq {
        return Err(unavailable());
    }
    let scope = QueryScope {
        project_ids: auth.projects.clone(),
        start_us,
        end_us,
        watermark,
        time_field: TimeField::Timestamp,
    };
    Ok(ReadView {
        auth,
        published,
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
    if current.storage_generation != auth.storage_generation {
        return Err(token_error(TokenError::GenerationChanged));
    }
    if current.hash != auth.hash || current.epoch != auth.epoch || current.projects != auth.projects
    {
        return Err(token_error(TokenError::AuthorizationChanged));
    }
    Ok(())
}
