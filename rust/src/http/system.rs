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
    let mut status = state
        .app
        .db
        .call(move |db| {
            let mut status = json!(crate::operations::status(db, &data_dir, disk, ready, s3)?);
            status["replay"] = json!(crate::operations::replay_status(
                db,
                crate::model::now_us()?
            )?);
            Ok(status)
        })
        .await?;
    status["sentry_ingest_since_start"] = json!(state.app.ingest_stats.snapshot());
    status["replay_maintenance"] = json!(state.app.replay_maintenance.as_ref().map(|m| m.status()));
    Ok(Json(status))
}

pub(super) async fn efficiency(
    State(state): State<HttpState>,
    headers: HeaderMap,
) -> ApiResult<Json<crate::efficiency::Snapshot>> {
    authenticate(&state, &headers, false, true).await?;
    Ok(Json(state.app.efficiency.snapshot()))
}

pub(super) async fn doctor(
    State(state): State<HttpState>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    authenticate(&state, &headers, false, true).await?;
    let data_dir = state.app.config.data_dir.clone();
    // Hashing shard files must never occupy the single SQLite mutation worker.
    let permit = state
        .app
        .query_permit
        .clone()
        .try_acquire_owned()
        .map_err(|_| super::ApiError(axum::http::StatusCode::TOO_MANY_REQUESTS, "query_busy"))?;
    let report = super::run_native(permit, std::time::Duration::from_secs(30), move || {
        crate::operations::doctor(&data_dir)
    })
    .await
    .map_err(doctor_failure)??;
    authenticate(&state, &headers, false, true).await?;
    Ok(Json(json!(report)))
}

fn doctor_failure(error: super::NativeTaskFailure) -> super::ApiError {
    match error {
        super::NativeTaskFailure::Timeout => {
            super::ApiError(axum::http::StatusCode::GATEWAY_TIMEOUT, "doctor_timeout")
        }
        super::NativeTaskFailure::Join => super::ApiError(
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "doctor_unavailable",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    #[test]
    fn doctor_worker_failures_have_stable_public_statuses() {
        let timeout = doctor_failure(super::super::NativeTaskFailure::Timeout);
        assert_eq!(timeout.0, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(timeout.1, "doctor_timeout");
        let unavailable = doctor_failure(super::super::NativeTaskFailure::Join);
        assert_eq!(unavailable.0, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(unavailable.1, "doctor_unavailable");
    }
}
