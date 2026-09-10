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
use crate::db::issue_detail::{self as metadata, AuthorizedOccurrence, IssueDetailError};
use crate::search::tokens::{Position, TokenError};

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
    require_digest(&issue_id, "invalid_issue_id")?;
    require_digest(&record_id, "invalid_record_id")?;
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
    require_published(published.boundary.ingest_seq, initial.location.ingest_seq)?;
    let mut detail = load_native(
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
    require_same_principal(principal_id, current_principal.id)?;
    let current = lookup(
        &state,
        current_principal.id,
        initial.location.issue_id.clone(),
        initial.location.record_id.clone(),
    )
    .await?;
    compare_authorization(&initial.authorization, &current.authorization)?;
    require_same_location(&initial.location, &current.location)?;
    detail.detail_token = Some(
        state
            .app
            .tokens
            .issue(
                super::search::context(&initial.authorization, "record-detail-v1".into()),
                published.boundary.ingest_seq,
                Position::Detail {
                    project_id: initial.location.project_id,
                    shard_id: initial.location.shard_id,
                    record_id: initial.location.record_id,
                },
                crate::model::now_us()?,
            )
            .map_err(token_error)?,
    );
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

fn require_digest(value: &str, code: &'static str) -> ApiResult<()> {
    canonical_digest(value)
        .then_some(())
        .ok_or_else(|| invalid_id(code))
}

fn require_published(published: i64, required: i64) -> ApiResult<()> {
    (published >= required)
        .then_some(())
        .ok_or_else(unavailable)
}

fn require_same_principal(before: i64, after: i64) -> ApiResult<()> {
    (before == after)
        .then_some(())
        .ok_or_else(|| token_error(TokenError::AuthorizationChanged))
}

fn require_same_location(
    before: &metadata::OccurrenceLocation,
    after: &metadata::OccurrenceLocation,
) -> ApiResult<()> {
    (before == after).then_some(()).ok_or_else(unavailable)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn occurrence_lookup_errors_and_digest_validation_fail_closed() {
        let missing = map_lookup_error(anyhow::Error::from(IssueDetailError::NotFound));
        assert_eq!(missing.0, StatusCode::NOT_FOUND);
        assert_eq!(missing.1, "issue_occurrence_not_found");
        let corrupt = map_lookup_error(anyhow::Error::from(IssueDetailError::Corrupt));
        assert_eq!(corrupt.0, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(corrupt.1, "search_unavailable");
        let internal = map_lookup_error(anyhow::anyhow!("private detail"));
        assert_eq!(internal.0, StatusCode::SERVICE_UNAVAILABLE);

        assert!(canonical_digest(&"0".repeat(64)));
        assert!(canonical_digest(&"f".repeat(64)));
        assert!(!canonical_digest(&"F".repeat(64)));
        assert!(!canonical_digest("short"));
        assert!(require_digest(&"0".repeat(64), "invalid").is_ok());
        assert_eq!(
            require_digest("bad", "invalid_record_id").unwrap_err().1,
            "invalid_record_id"
        );
        assert!(require_published(2, 2).is_ok());
        assert!(require_published(1, 2).is_err());
        assert!(require_same_principal(1, 1).is_ok());
        assert!(require_same_principal(1, 2).is_err());
        let location = metadata::OccurrenceLocation {
            issue_id: "issue".into(),
            project_id: 1,
            shard_id: "shard".into(),
            record_id: "record".into(),
            ingest_seq: 1,
        };
        assert!(require_same_location(&location, &location).is_ok());
        let mut changed = location.clone();
        changed.ingest_seq = 2;
        assert!(require_same_location(&location, &changed).is_err());
    }
}
