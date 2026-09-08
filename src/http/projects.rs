//! Project HTTP DTOs and response conversion. Transactions live in db::projects.

use super::{ApiError, ApiResult, HttpState, auth::authenticate};
use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
};
use serde::Deserialize;
use serde_json::{Value, json};

pub(super) async fn projects(
    State(state): State<HttpState>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    let p = authenticate(&state, &headers, false, false).await?;
    let items = state
        .app
        .db
        .call(move |db| crate::db::projects::list(db, p.id))
        .await?;
    Ok(Json(json!({"items": items})))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ProjectInput {
    slug: String,
    name: String,
}

pub(super) async fn create_project(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(input): Json<ProjectInput>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let principal = authenticate(&state, &headers, true, true).await?;
    if input.slug.is_empty()
        || input.slug.len() > 64
        || !input
            .slug
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        || input.name.is_empty()
        || input.name.len() > 200
    {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_project"));
    }
    let id = state
        .app
        .db
        .call(move |db| crate::db::projects::create(db, principal.id, &input.slug, &input.name))
        .await?;
    Ok((StatusCode::CREATED, Json(json!({"id":id.to_string()}))))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ProjectUpdate {
    is_active: bool,
}

pub(super) async fn update_project(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Json(input): Json<ProjectUpdate>,
) -> ApiResult<StatusCode> {
    let p = authenticate(&state, &headers, true, true).await?;
    state
        .app
        .db
        .call(move |db| crate::db::projects::set_active(db, p.id, id, input.is_active))
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn create_key(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let p = authenticate(&state, &headers, true, true).await?;
    let key = crate::auth::random_token();
    let stored = key.clone();
    let key_id = state
        .app
        .db
        .call(move |db| crate::db::projects::create_key(db, p.id, id, &stored))
        .await?;
    let mut dsn = state.app.config.base_url.clone();
    dsn.set_username(&key)
        .map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "invalid_base_url"))?;
    dsn.set_path(&id.to_string());
    Ok((
        StatusCode::CREATED,
        Json(json!({"id":key_id.to_string(),"public_key":key,"dsn":dsn.to_string()})),
    ))
}

pub(super) async fn revoke_key(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((id, key_id)): Path<(i64, i64)>,
) -> ApiResult<StatusCode> {
    let p = authenticate(&state, &headers, true, true).await?;
    state
        .app
        .db
        .call(move |db| crate::db::projects::revoke_key(db, p.id, id, key_id))
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
