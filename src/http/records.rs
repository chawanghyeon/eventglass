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
    let Position::Detail {
        project_id,
        shard_id,
        record_id,
    } = verified.position
    else {
        return Err(token_error(TokenError::Invalid));
    };

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
    if verified.watermark > published.boundary.ingest_seq {
        return Err(unavailable());
    }
    let detail_shard_id = shard_id.clone();
    state
        .app
        .db
        .call(move |db| authorization::local_detail_shard(db, &detail_shard_id))
        .await
        .map_err(scope_error)?;

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
    if current_principal.id != principal_id {
        return Err(token_error(TokenError::AuthorizationChanged));
    }
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
    if current_scope.projects != scope.projects {
        return Err(token_error(TokenError::AuthorizationChanged));
    }
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

pub(super) async fn load_native(
    state: &HttpState,
    shard_id: String,
    lookup: NativeDetail,
) -> ApiResult<Option<RecordDetail>> {
    let permit = state
        .app
        .query_permit
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError(StatusCode::TOO_MANY_REQUESTS, "query_busy"))?;
    let indexer = state.app.indexer.clone().ok_or_else(unavailable)?;
    let task = tokio::task::spawn_blocking(move || {
        let _permit = permit;
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
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), task)
        .await
        .map_err(|_| ApiError(StatusCode::GATEWAY_TIMEOUT, "query_timeout"))?
        .map_err(|_| unavailable())?
        .map_err(|_| unavailable())
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
