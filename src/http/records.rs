//! Authenticated stored record detail addressed only by an opaque signed token.

use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};

use super::{
    ApiError, ApiResult, HttpState,
    auth::authenticate,
    search::{context, scope_error, token_error},
};
use crate::{
    db::search::{self as authorization, Authorization},
    search::{
        detail::{self, RecordDetail},
        tokens::{Position, TokenError, TokenKind},
    },
};

pub(super) fn unavailable() -> ApiError {
    ApiError(StatusCode::SERVICE_UNAVAILABLE, "search_unavailable")
}

pub(super) async fn get_record(
    State(state): State<HttpState>,
    Path(detail_token): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let principal = authenticate(&state, &headers, false, false).await?;
    let principal_id = principal.id;
    let identity = state
        .app
        .db
        .call(move |db| authorization::identity(db, principal_id))
        .await
        .map_err(scope_error)?;
    let token_context = context(&identity, "record-detail-v1".to_owned());
    let verified = state
        .app
        .tokens
        .verify(
            &detail_token,
            TokenKind::Detail,
            &token_context,
            crate::model::now_us()?,
        )
        .map_err(token_error)?;
    let (project_id, shard_id, record_id) = detail_position(verified.position)?;

    // Verify current access only after the opaque token has authenticated its project identity.
    let selected_project = project_id;
    let scope = state
        .app
        .db
        .call(move |db| authorization::capture(db, principal_id, vec![selected_project]))
        .await
        .map_err(scope_error)?;
    let published = state
        .app
        .indexer
        .as_ref()
        .ok_or_else(unavailable)?
        .snapshot()
        .map_err(|_| unavailable())?;
    require_published(verified.watermark, published.boundary.ingest_seq)?;
    let mut detail = load_native(
        &state,
        shard_id,
        NativeDetail::Watermark {
            project_id,
            record_id,
            watermark: verified.watermark,
        },
    )
    .await?
    .ok_or(ApiError(StatusCode::NOT_FOUND, "record_not_found"))?;

    // A read that crossed a session, permission epoch or restore generation is never returned.
    let current_principal = authenticate(&state, &headers, false, false).await?;
    require_principal(principal_id, current_principal.id)?;
    let current_id = current_principal.id;
    let current_identity = state
        .app
        .db
        .call(move |db| authorization::identity(db, current_id))
        .await
        .map_err(scope_error)?;
    compare_authorization(&identity, &current_identity)?;
    let current_id = current_principal.id;
    let current_scope = state
        .app
        .db
        .call(move |db| authorization::capture(db, current_id, vec![project_id]))
        .await
        .map_err(scope_error)?;
    compare_authorization(&scope, &current_scope)?;
    require_projects(&scope.projects, &current_scope.projects)?;
    detail.detail_token = Some(detail_token);

    Ok(detail_response(detail))
}

pub(super) enum NativeDetail {
    Watermark {
        project_id: i64,
        record_id: String,
        watermark: i64,
    },
    Occurrence {
        project_id: i64,
        record_id: String,
        ingest_seq: i64,
    },
}

pub(super) fn detail_position(position: Position) -> ApiResult<(i64, String, String)> {
    match position {
        Position::Detail {
            project_id,
            shard_id,
            record_id,
        } => Ok((project_id, shard_id, record_id)),
        _ => Err(token_error(TokenError::Invalid)),
    }
}

fn require_published(watermark: i64, published: i64) -> ApiResult<()> {
    (watermark <= published)
        .then_some(())
        .ok_or_else(unavailable)
}

pub(super) fn require_principal(before: i64, after: i64) -> ApiResult<()> {
    (before == after)
        .then_some(())
        .ok_or_else(|| token_error(TokenError::AuthorizationChanged))
}

fn require_projects(before: &[i64], after: &[i64]) -> ApiResult<()> {
    (before == after)
        .then_some(())
        .ok_or_else(|| token_error(TokenError::AuthorizationChanged))
}

pub(super) async fn load_native(
    state: &HttpState,
    shard_id: String,
    lookup: NativeDetail,
) -> ApiResult<Option<RecordDetail>> {
    super::search::hydrate_candidates(state, std::slice::from_ref(&shard_id)).await?;
    let detail_shard_id = shard_id.clone();
    state
        .app
        .db
        .call(move |db| authorization::local_detail_shard(db, &detail_shard_id))
        .await
        .map_err(scope_error)?;
    let permit = state
        .app
        .query_permit
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError(StatusCode::TOO_MANY_REQUESTS, "query_busy"))?;
    let indexer = state.app.indexer.clone().ok_or_else(unavailable)?;
    super::run_native(permit, std::time::Duration::from_secs(10), move || {
        let pins = indexer.pin_shards(&[shard_id])?;
        let published = pins
            .first()
            .ok_or_else(|| anyhow::anyhow!("missing shard pin"))?
            .published();
        match lookup {
            NativeDetail::Watermark {
                project_id,
                record_id,
                watermark,
            } => detail::load(published, project_id, &record_id, watermark),
            NativeDetail::Occurrence {
                project_id,
                record_id,
                ingest_seq,
            } => detail::load_occurrence(published, project_id, &record_id, ingest_seq),
        }
    })
    .await
    .map_err(native_detail_failure)?
    .map_err(|_| unavailable())
}

fn native_detail_failure(failure: super::NativeTaskFailure) -> ApiError {
    match failure {
        super::NativeTaskFailure::Timeout => ApiError(StatusCode::GATEWAY_TIMEOUT, "query_timeout"),
        super::NativeTaskFailure::Join => unavailable(),
    }
}

pub(super) fn detail_response(detail: RecordDetail) -> Response {
    let mut response = Json(detail).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    response
}

pub(super) fn compare_authorization(
    before: &Authorization,
    after: &Authorization,
) -> ApiResult<()> {
    if after.storage_generation != before.storage_generation {
        return Err(token_error(TokenError::GenerationChanged));
    }
    if after.epoch != before.epoch || after.hash != before.hash {
        return Err(token_error(TokenError::AuthorizationChanged));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn authorization() -> Authorization {
        Authorization {
            projects: vec![1],
            storage_generation: "generation".into(),
            epoch: 1,
            hash: "hash".into(),
        }
    }

    #[test]
    fn detail_worker_and_authorization_changes_fail_closed() {
        let timeout = native_detail_failure(super::super::NativeTaskFailure::Timeout);
        assert_eq!(timeout.0, StatusCode::GATEWAY_TIMEOUT);
        let join = native_detail_failure(super::super::NativeTaskFailure::Join);
        assert_eq!(join.0, StatusCode::SERVICE_UNAVAILABLE);

        let before = authorization();
        assert!(compare_authorization(&before, &before).is_ok());
        let mut generation = before.clone();
        generation.storage_generation = "restored".into();
        assert!(compare_authorization(&before, &generation).is_err());
        let mut epoch = before.clone();
        epoch.epoch += 1;
        assert!(compare_authorization(&before, &epoch).is_err());
        let mut hash = before.clone();
        hash.hash = "changed".into();
        assert!(compare_authorization(&before, &hash).is_err());

        assert!(detail_position(Position::Read).is_err());
        let position = detail_position(Position::Detail {
            project_id: 1,
            shard_id: "s".into(),
            record_id: "r".into(),
        })
        .unwrap();
        assert_eq!(position.0, 1);
        assert!(require_published(1, 1).is_ok());
        assert!(require_published(2, 1).is_err());
        assert!(require_principal(1, 1).is_ok());
        assert!(require_principal(1, 2).is_err());
        assert!(require_projects(&[1], &[1]).is_ok());
        assert!(require_projects(&[1], &[2]).is_err());
    }
}
