//! Token-free Issue occurrence detail backed by the durable occurrence identity.

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::Response,
};

use super::{
    ApiError, ApiResult, HttpState,
    auth::authenticate,
    records::{NativeDetail, compare_authorization, detail_response, load_native, unavailable},
    search::{scope_error, token_error},
};
use crate::db::{
    issue_detail::{self as metadata, AuthorizedOccurrence, IssueDetailError},
    search as authorization,
};
use crate::search::tokens::TokenError;

fn invalid_id(code: &'static str) -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, code)
}

fn not_found() -> ApiError {
    ApiError(StatusCode::NOT_FOUND, "issue_occurrence_not_found")
}

fn map_lookup_error(error: anyhow::Error) -> ApiError {
    match error.downcast_ref::<IssueDetailError>() {
        Some(IssueDetailError::NotFound) => not_found(),
        Some(IssueDetailError::Corrupt) => unavailable(),
        None => scope_error(error),
    }
}

pub(super) async fn get_occurrence_detail(
    State(state): State<HttpState>,
    Path((issue_id, record_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    if !canonical_digest(&issue_id) {
        return Err(invalid_id("invalid_issue_id"));
    }
    if !canonical_digest(&record_id) {
        return Err(invalid_id("invalid_record_id"));
    }
    let principal = authenticate(&state, &headers, false, false).await?;
    let principal_id = principal.id;
    let initial = lookup(&state, principal_id, issue_id, record_id).await?;

    let published = state
        .app
        .indexer
        .as_ref()
        .ok_or_else(unavailable)?
        .snapshot()
        .map_err(|_| unavailable())?;
    if published.boundary.ingest_seq < initial.location.ingest_seq {
        return Err(unavailable());
    }
    let detail_shard_id = initial.location.shard_id.clone();
    state
        .app
        .db
        .call(move |db| authorization::local_detail_shard(db, &detail_shard_id))
        .await
        .map_err(scope_error)?;
    let detail = load_native(
        &state,
        initial.location.shard_id.clone(),
        NativeDetail::Occurrence {
            project_id: initial.location.project_id,
            record_id: initial.location.record_id.clone(),
            ingest_seq: initial.location.ingest_seq,
        },
    )
    .await?
    .ok_or_else(unavailable)?;

    let current_principal = authenticate(&state, &headers, false, false).await?;
    if current_principal.id != principal_id {
        return Err(token_error(TokenError::AuthorizationChanged));
    }
    let current = lookup(
        &state,
        current_principal.id,
        initial.location.issue_id.clone(),
        initial.location.record_id.clone(),
    )
    .await?;
    compare_authorization(&initial.authorization, &current.authorization)?;
    if current.location != initial.location {
        return Err(unavailable());
    }
    Ok(detail_response(detail))
}

async fn lookup(
    state: &HttpState,
    actor: i64,
    issue_id: String,
    record_id: String,
) -> ApiResult<AuthorizedOccurrence> {
    state
        .app
        .db
        .call(move |db| metadata::lookup(db, actor, &issue_id, &record_id))
        .await
        .map_err(map_lookup_error)
}

fn canonical_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
