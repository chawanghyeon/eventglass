//! Replay routes use the same session and project authorization as observability queries.
use super::{ApiError, ApiResult, HttpState, auth::authenticate};
use crate::db::replays::{self, ReplayFilter};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
};
use serde_json::{Value, json};

pub(super) async fn list(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Query(filter): Query<ReplayFilter>,
) -> ApiResult<Json<Value>> {
    let actor = authenticate(&state, &headers, false, false).await?;
    validate_filter(&filter)?;
    let mut items = state
        .app
        .db
        .call(move |db| replays::list(db, actor.id, &filter, crate::model::now_us()?))
        .await?;
    let has_more = items.len() > 50;
    items.truncate(50);
    let cursor=has_more.then(|| items.last().map(|last|json!({"before_started_ms":last.metadata.started_at_ms,"before_id":last.metadata.replay_id}))).flatten();
    Ok(Json(json!({"items":items,"next_cursor":cursor})))
}
fn validate_filter(filter: &ReplayFilter) -> ApiResult<()> {
    if filter.project_id <= 0
        || filter.before_id.is_some() != filter.before_started_ms.is_some()
        || [
            &filter.environment,
            &filter.release,
            &filter.url,
            &filter.user,
        ]
        .iter()
        .any(|s| s.as_ref().is_some_and(|s| s.len() > 4096))
        || filter
            .started_after_ms
            .zip(filter.started_before_ms)
            .is_some_and(|(a, b)| a >= b)
        || filter.min_duration_ms.is_some_and(|v| v < 0)
        || filter.max_duration_ms.is_some_and(|v| v < 0)
        || filter
            .min_duration_ms
            .zip(filter.max_duration_ms)
            .is_some_and(|(a, b)| a > b)
    {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_replay_filter"));
    }
    if let Some(id) = &filter.before_id {
        validate_id(id)?;
    }
    Ok(())
}
fn validate_id(id: &str) -> ApiResult<()> {
    crate::sentry::replay::canonical_id(&json!(id))
        .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid_replay_id"))?;
    Ok(())
}

pub(super) async fn detail(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, id)): Path<(i64, String)>,
) -> ApiResult<Json<Value>> {
    validate_id(&id)?;
    let actor = authenticate(&state, &headers, false, false).await?;
    let result=state.app.db.call(move |db| {
        let now=crate::model::now_us()?;
        let replay=replays::get(db,actor.id,project,&id,now)?;
        let segments=replays::segments(db,project,&id)?;
        let associations=replays::associations(db,project,&replay,now)?;
        Ok(json!({"replay":replay,"segments":segments.iter().map(|s|json!({"segment_id":s.segment_id,"compressed_bytes":s.blob.size})).collect::<Vec<_>>(),"associations":associations}))
    }).await?;
    Ok(Json(result))
}

pub(super) async fn recording(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, id, segment)): Path<(i64, String, u32)>,
) -> ApiResult<Json<Value>> {
    validate_id(&id)?;
    let actor = authenticate(&state, &headers, false, false).await?;
    let permit = state
        .app
        .query_permit
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError(StatusCode::TOO_MANY_REQUESTS, "query_busy"))?;
    let reference = state
        .app
        .db
        .call(move |db| {
            replays::get(db, actor.id, project, &id, crate::model::now_us()?)?;
            replays::segments(db, project, &id)?
                .into_iter()
                .find(|s| s.segment_id == segment)
                .map(|s| s.blob)
                .ok_or_else(|| anyhow::Error::from(replays::ReplayError::NotFound))
        })
        .await?;
    let root = state.app.config.data_dir.clone();
    let events = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
        let _permit = permit;
        let bytes = crate::storage::replay::read(&root, &reference)?;
        Ok(crate::sentry::replay::recording_events(&bytes)?)
    })
    .await
    .map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "replay_worker_failed"))??;
    Ok(Json(json!({"events":events})))
}

pub(super) async fn analysis(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((project, id)): Path<(i64, String)>,
) -> ApiResult<Json<crate::replay::Analysis>> {
    validate_id(&id)?;
    let actor = authenticate(&state, &headers, false, false).await?;
    let permit = state
        .app
        .query_permit
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError(StatusCode::TOO_MANY_REQUESTS, "query_busy"))?;
    let (replay, segments) = state
        .app
        .db
        .call(move |db| {
            let replay = replays::get(db, actor.id, project, &id, crate::model::now_us()?)?;
            Ok((replay, replays::segments(db, project, &id)?))
        })
        .await?;
    let root = state.app.config.data_dir.clone();
    let analysis = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
        let _permit = permit;
        crate::replay::analyze(
            &root,
            segments,
            replay.metadata.finished_at_ms,
            &mut crate::replay::ReadBudget::default(),
        )
    })
    .await
    .map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "replay_worker_failed"))??;
    Ok(Json(analysis))
}

pub(super) async fn pages(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Query(filter): Query<ReplayFilter>,
) -> ApiResult<Json<crate::replay::PageMaps>> {
    let actor = authenticate(&state, &headers, false, false).await?;
    validate_filter(&filter)?;
    let permit = state
        .app
        .query_permit
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError(StatusCode::TOO_MANY_REQUESTS, "query_busy"))?;
    let (items, more) = state
        .app
        .db
        .call(move |db| {
            let replays = replays::list(db, actor.id, &filter, crate::model::now_us()?)?;
            let more = replays.len() > 20;
            let items = replays
                .into_iter()
                .take(20)
                .map(|replay| {
                    Ok((
                        replay.metadata.finished_at_ms,
                        replay.metadata.replay_id.clone(),
                        replays::segments(db, filter.project_id, &replay.metadata.replay_id)?,
                    ))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok((items, more))
        })
        .await?;
    let root = state.app.config.data_dir.clone();
    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
        let _permit = permit;
        let mut maps = crate::replay::PageMaps {
            truncated: more,
            ..Default::default()
        };
        let mut budget = crate::replay::ReadBudget::default();
        for (end, id, segments) in items {
            let mut analysis = crate::replay::analyze(&root, segments, end, &mut budget)?;
            for page in analysis.pages.values_mut() {
                page.replay_ids.push(id.clone());
            }
            let stop = analysis.truncated;
            maps.include(analysis);
            if stop {
                break;
            }
        }
        Ok(maps)
    })
    .await
    .map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "replay_worker_failed"))??;
    Ok(Json(result))
}

pub(super) async fn feedback(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Query(filter): Query<ReplayFilter>,
) -> ApiResult<Json<Value>> {
    let actor = authenticate(&state, &headers, false, false).await?;
    validate_filter(&filter)?;
    let items = state
        .app
        .db
        .call(move |db| {
            replays::feedback_list(db, actor.id, filter.project_id, crate::model::now_us()?)
        })
        .await?;
    Ok(Json(json!({"items":items})))
}
