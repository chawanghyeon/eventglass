//! Issue HTTP validation, authentication, and response conversion.

use super::ApiJson;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
};
use serde::Deserialize;

use super::{ApiError, ApiResult, HttpState, auth::authenticate};
use crate::db::issues::{IssueDbError, IssueStatus};

const DEFAULT_PAGE_LIMIT: usize = 50;
const MAX_PAGE_LIMIT: usize = 100;

fn map_issue_error(error: anyhow::Error) -> ApiError {
    if let Some(error) = error.downcast_ref::<IssueDbError>() {
        return match error {
            IssueDbError::Forbidden => ApiError(StatusCode::FORBIDDEN, "issue_access_denied"),
            IssueDbError::NotFound => ApiError(StatusCode::NOT_FOUND, "issue_not_found"),
            IssueDbError::StaleRevision => {
                ApiError(StatusCode::CONFLICT, "issue_revision_conflict")
            }
        };
    }
    error.into()
}

fn decimal_i64(value: &str) -> Option<i64> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

fn signed_decimal_i64(value: &str) -> Option<i64> {
    let digits = value.strip_prefix('-').unwrap_or(value);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

fn project_id(value: &str) -> ApiResult<i64> {
    decimal_i64(value)
        .filter(|value| *value > 0)
        .ok_or(ApiError(StatusCode::BAD_REQUEST, "invalid_issue_query"))
}

fn issue_id(value: &str) -> ApiResult<()> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Ok(());
    }
    Err(ApiError(StatusCode::BAD_REQUEST, "invalid_issue_id"))
}

fn page_limit(limit: Option<usize>) -> ApiResult<usize> {
    let limit = limit.unwrap_or(DEFAULT_PAGE_LIMIT);
    if (1..=MAX_PAGE_LIMIT).contains(&limit) {
        Ok(limit)
    } else {
        Err(ApiError(StatusCode::BAD_REQUEST, "invalid_issue_query"))
    }
}

fn status(value: Option<&str>) -> ApiResult<Option<&str>> {
    if value.is_none_or(|value| matches!(value, "unresolved" | "resolved" | "ignored")) {
        Ok(value)
    } else {
        Err(ApiError(StatusCode::BAD_REQUEST, "invalid_issue_query"))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct IssueListQuery {
    project_id: String,
    status: Option<String>,
    limit: Option<usize>,
    cursor_last_seen_us: Option<String>,
    cursor_id: Option<String>,
}

pub(super) async fn list_issues(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Query(query): Query<IssueListQuery>,
) -> ApiResult<Json<crate::db::issues::IssuePage>> {
    let principal = authenticate(&state, &headers, false, false).await?;
    let project_id = project_id(&query.project_id)?;
    let limit = page_limit(query.limit)?;
    let status = status(query.status.as_deref())?.map(str::to_owned);
    let cursor = match (query.cursor_last_seen_us, query.cursor_id) {
        (None, None) => None,
        (Some(time), Some(id)) => {
            issue_id(&id)?;
            Some((
                signed_decimal_i64(&time)
                    .ok_or(ApiError(StatusCode::BAD_REQUEST, "invalid_issue_query"))?,
                id,
            ))
        }
        _ => return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_issue_query")),
    };
    let page = state
        .app
        .db
        .call(move |db| {
            crate::db::issues::list(
                db,
                principal.id,
                project_id,
                status.as_deref(),
                limit,
                cursor.as_ref().map(|(time, id)| (*time, id.as_str())),
            )
        })
        .await
        .map_err(map_issue_error)?;
    Ok(Json(page))
}

pub(super) async fn get_issue(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ApiResult<Json<crate::db::issues::Issue>> {
    issue_id(&id)?;
    let principal = authenticate(&state, &headers, false, false).await?;
    let issue = state
        .app
        .db
        .call(move |db| crate::db::issues::get(db, principal.id, &id))
        .await
        .map_err(map_issue_error)?;
    Ok(Json(issue))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OccurrenceListQuery {
    limit: Option<usize>,
    cursor_occurred_at_us: Option<String>,
    cursor_ingest_seq: Option<String>,
}

pub(super) async fn list_occurrences(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<OccurrenceListQuery>,
) -> ApiResult<Json<crate::db::issues::OccurrencePage>> {
    issue_id(&id)?;
    let principal = authenticate(&state, &headers, false, false).await?;
    let limit = page_limit(query.limit)?;
    let cursor = match (query.cursor_occurred_at_us, query.cursor_ingest_seq) {
        (None, None) => None,
        (Some(time), Some(seq)) => Some((
            signed_decimal_i64(&time)
                .ok_or(ApiError(StatusCode::BAD_REQUEST, "invalid_issue_query"))?,
            decimal_i64(&seq)
                .filter(|seq| *seq > 0)
                .ok_or(ApiError(StatusCode::BAD_REQUEST, "invalid_issue_query"))?,
        )),
        _ => return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_issue_query")),
    };
    let page = state
        .app
        .db
        .call(move |db| crate::db::issues::occurrences(db, principal.id, &id, limit, cursor))
        .await
        .map_err(map_issue_error)?;
    Ok(Json(page))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct IssueUpdate {
    status: String,
    expected_revision: String,
}

pub(super) async fn update_issue(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    ApiJson(input): ApiJson<IssueUpdate>,
) -> ApiResult<Json<crate::db::issues::Issue>> {
    issue_id(&id)?;
    let principal = authenticate(&state, &headers, true, false).await?;
    let expected_revision = decimal_i64(&input.expected_revision)
        .ok_or(ApiError(StatusCode::BAD_REQUEST, "invalid_issue_revision"))?;
    let status = match input.status.as_str() {
        "unresolved" => IssueStatus::Unresolved,
        "resolved" => IssueStatus::Resolved,
        "ignored" => IssueStatus::Ignored,
        _ => return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_issue_status")),
    };
    let issue = state
        .app
        .db
        .call(move |db| {
            crate::db::issues::set_status(db, principal.id, &id, status, expected_revision)
        })
        .await
        .map_err(map_issue_error)?;
    Ok(Json(issue))
}
