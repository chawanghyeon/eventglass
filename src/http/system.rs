//! Administrator operational status and read-only consistency inspection.

use axum::{Json, extract::State, http::HeaderMap};
use serde_json::{Value, json};

use super::{ApiResult, HttpState, auth::authenticate};

pub(super) async fn status(
    State(state): State<HttpState>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    authenticate(&state, &headers, false, true).await?;
    let disk = state.app.disk_budget.status()?;
    let ready = state
        .app
        .indexer
        .as_ref()
        .is_some_and(|indexer| indexer.ready());
    let data_dir = state.app.config.data_dir.clone();
    let s3 = state.app.config.s3_url.is_some();
    let status = state
        .app
        .db
        .call(move |db| crate::operations::status(db, &data_dir, disk, ready, s3))
        .await?;
    Ok(Json(json!(status)))
}

pub(super) async fn doctor(
    State(state): State<HttpState>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    authenticate(&state, &headers, false, true).await?;
    let data_dir = state.app.config.data_dir.clone();
    let report = state
        .app
        .db
        .call(move |db| crate::operations::doctor_connection(db, &data_dir))
        .await?;
    Ok(Json(json!(report)))
}
