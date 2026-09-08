//! Administrator alert configuration and delivery inspection endpoints.

use super::{ApiError, ApiResult, HttpState, auth::authenticate};
use crate::alerts::{Condition, Configuration, Destination};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AlertInput {
    name: String,
    project_id: Option<String>,
    condition: Condition,
    destination: Destination,
    enabled: bool,
}

impl AlertInput {
    fn configuration(self) -> ApiResult<Configuration> {
        let project_id = self
            .project_id
            .map(|value| {
                if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err(invalid());
                }
                value
                    .parse::<i64>()
                    .ok()
                    .filter(|id| *id > 0)
                    .ok_or_else(invalid)
            })
            .transpose()?;
        let configuration = Configuration {
            name: self.name,
            project_id,
            condition: self.condition,
            destination: self.destination,
            enabled: self.enabled,
        };
        configuration.validate().map_err(|_| invalid())?;
        Ok(configuration)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AlertUpdate {
    revision: i64,
    #[serde(flatten)]
    input: AlertInput,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Revision {
    revision: i64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DeliveryQuery {
    limit: Option<usize>,
}

fn invalid() -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, "invalid_alert")
}

pub(super) async fn list(
    State(state): State<HttpState>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    let principal = authenticate(&state, &headers, false, true).await?;
    let items = state
        .app
        .db
        .call(move |db| crate::db::alerts::list(db, principal.id))
        .await?;
    Ok(Json(json!({"items":items})))
}

pub(super) async fn create(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(input): Json<AlertInput>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let principal = authenticate(&state, &headers, true, true).await?;
    let configuration = input.configuration()?;
    let id = state
        .app
        .db
        .call(move |db| {
            crate::db::alerts::create(db, principal.id, &configuration, crate::model::now_us()?)
        })
        .await?;
    Ok((StatusCode::CREATED, Json(json!({"id":id.to_string()}))))
}

pub(super) async fn update(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Json(input): Json<AlertUpdate>,
) -> ApiResult<Json<Value>> {
    let principal = authenticate(&state, &headers, true, true).await?;
    let revision = input.revision;
    let configuration = input.input.configuration()?;
    let alert = state
        .app
        .db
        .call(move |db| {
            crate::db::alerts::update(
                db,
                principal.id,
                id,
                revision,
                &configuration,
                crate::model::now_us()?,
            )
        })
        .await?;
    Ok(Json(json!(alert)))
}

pub(super) async fn delete_alert(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Query(input): Query<Revision>,
) -> ApiResult<StatusCode> {
    let principal = authenticate(&state, &headers, true, true).await?;
    state
        .app
        .db
        .call(move |db| {
            crate::db::alerts::delete(
                db,
                principal.id,
                id,
                input.revision,
                crate::model::now_us()?,
            )
        })
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn deliveries(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Query(input): Query<DeliveryQuery>,
) -> ApiResult<Json<Value>> {
    let principal = authenticate(&state, &headers, false, true).await?;
    let limit = input.limit.unwrap_or(100);
    let items = state
        .app
        .db
        .call(move |db| crate::db::alerts::deliveries(db, principal.id, limit))
        .await?;
    Ok(Json(json!({"items":items})))
}

pub(super) async fn retry_delivery(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ApiResult<StatusCode> {
    let principal = authenticate(&state, &headers, true, true).await?;
    if id.len() != 64
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid());
    }
    state
        .app
        .db
        .call(move |db| crate::db::alerts::retry(db, principal.id, &id, crate::model::now_us()?))
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
