//! Public DSN transport. Cookie authentication and administration CORS do not apply.

use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Path, State},
    http::{HeaderMap, Method, StatusCode, Uri, header},
    routing::post,
};
use serde_json::json;
use tower_http::cors::{Any, CorsLayer};

use super::{ApiError, ApiResult};
use crate::{app::AppState, config::Limits, db::ingest, sentry};

pub(super) fn router(app: AppState) -> Router {
    Router::new()
        .route("/api/{project_id}/envelope/", post(envelope))
        .route("/api/{project_id}/store/", post(store))
        .layer(axum::extract::DefaultBodyLimit::disable())
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([Method::POST])
                .allow_headers([
                    header::CONTENT_TYPE,
                    header::CONTENT_ENCODING,
                    axum::http::HeaderName::from_static("x-sentry-auth"),
                ]),
        )
        .with_state(app)
}

async fn envelope(
    State(app): State<AppState>,
    Path(project): Path<i64>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> ApiResult<(StatusCode, Json<serde_json::Value>)> {
    receive(app, project, uri, headers, body, true).await
}

async fn store(
    State(app): State<AppState>,
    Path(project): Path<i64>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> ApiResult<(StatusCode, Json<serde_json::Value>)> {
    receive(app, project, uri, headers, body, false).await
}

fn wire_error(error: sentry::SentryError) -> ApiError {
    match error {
        sentry::SentryError::TooLarge(_) => {
            ApiError(StatusCode::PAYLOAD_TOO_LARGE, "ingest_too_large")
        }
        sentry::SentryError::UnsupportedEncoding => {
            ApiError(StatusCode::UNSUPPORTED_MEDIA_TYPE, "unsupported_encoding")
        }
        sentry::SentryError::Malformed(_) => {
            ApiError(StatusCode::BAD_REQUEST, "invalid_sentry_payload")
        }
    }
}

fn key_from_transport(uri: &Uri, headers: &HeaderMap) -> ApiResult<Option<String>> {
    let mut key = None;
    let mut add = |candidate: &str| -> ApiResult<()> {
        if candidate.is_empty() || candidate.len() > 256 {
            return Err(ApiError(StatusCode::UNAUTHORIZED, "invalid_ingest_key"));
        }
        if key.as_deref().is_some_and(|previous| previous != candidate) {
            return Err(ApiError(StatusCode::UNAUTHORIZED, "conflicting_ingest_key"));
        }
        key = Some(candidate.to_owned());
        Ok(())
    };
    for (name, value) in url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()) {
        if name == "sentry_key" {
            add(&value)?;
        }
    }
    for value in headers.get_all("x-sentry-auth") {
        let value = value
            .to_str()
            .map_err(|_| ApiError(StatusCode::UNAUTHORIZED, "invalid_ingest_key"))?;
        let value = value
            .strip_prefix("Sentry ")
            .ok_or(ApiError(StatusCode::UNAUTHORIZED, "invalid_ingest_key"))?;
        for item in value.split(',') {
            if let Some((name, value)) = item.trim().split_once('=')
                && name == "sentry_key"
            {
                add(value.trim())?;
            }
        }
    }
    Ok(key)
}

async fn receive(
    app: AppState,
    project_id: i64,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
    is_envelope: bool,
) -> ApiResult<(StatusCode, Json<serde_json::Value>)> {
    let stats = app.ingest_stats.clone();
    let result = receive_inner(app, project_id, uri, headers, body, is_envelope).await;
    stats.record(match &result {
        Ok((status, _)) => status.as_u16(),
        Err(error) => error.0.as_u16(),
    });
    result
}

async fn receive_inner(
    app: AppState,
    project_id: i64,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
    is_envelope: bool,
) -> ApiResult<(StatusCode, Json<serde_json::Value>)> {
    if app.indexer.as_ref().is_some_and(|indexer| !indexer.ready()) {
        return Err(ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "indexer_unavailable",
        ));
    }
    if project_id <= 0 {
        return Err(ApiError(StatusCode::UNAUTHORIZED, "invalid_ingest_key"));
    }
    let transport_key = key_from_transport(&uri, &headers)?;
    let encoding = sentry::ContentEncoding::parse(
        headers
            .get(header::CONTENT_ENCODING)
            .map(|v| v.to_str())
            .transpose()
            .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid_encoding"))?,
    )
    .map_err(wire_error)?;
    let limits = Limits::default();
    if let Some(length) = headers.get(header::CONTENT_LENGTH) {
        let length = length
            .to_str()
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .ok_or(ApiError(StatusCode::BAD_REQUEST, "invalid_content_length"))?;
        if length > limits.wire_bytes {
            return Err(ApiError(StatusCode::PAYLOAD_TOO_LARGE, "ingest_too_large"));
        }
    }
    if !app.disk_budget.status()?.ingest_accepting
        && let Some(cold) = &app.cold
    {
        cold.reclaim_for_ingest(&app.disk_budget)
            .await
            .map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "disk_reclaim_failed"))?;
    }
    let permit = app
        .ingress_permit
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError(StatusCode::TOO_MANY_REQUESTS, "ingest_busy"))?;
    let wire = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        to_bytes(body, limits.wire_bytes),
    )
    .await
    .map_err(|_| ApiError(StatusCode::REQUEST_TIMEOUT, "ingest_body_timeout"))?
    .map_err(|_| ApiError(StatusCode::PAYLOAD_TOO_LARGE, "ingest_body_unreadable"))?;
    let disk_budget = app.disk_budget.clone();
    let (decoded, auth, permit, disk_reservation) =
        tokio::task::spawn_blocking(move || -> ApiResult<_> {
            let _held = &permit;
            let disk_reservation = disk_budget
                .reserve(2 * limits.decoded_bytes as u64)
                .map_err(|error| {
                    match error.downcast_ref::<crate::storage::budget::ReserveError>() {
                        Some(crate::storage::budget::ReserveError::Exhausted) => {
                            ApiError(StatusCode::TOO_MANY_REQUESTS, "disk_reserve")
                        }
                        _ => ApiError(StatusCode::SERVICE_UNAVAILABLE, "disk_unavailable"),
                    }
                })?;
            let decoded = sentry::decode_body(&wire, encoding, &limits).map_err(wire_error)?;
            let auth = if is_envelope {
                sentry::envelope_auth(&decoded).map_err(wire_error)?
            } else {
                None
            };
            Ok((decoded, auth, permit, disk_reservation))
        })
        .await
        .map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "ingest_worker_failed"))??;
    let key = match (transport_key, auth) {
        (Some(key), Some(auth)) if auth.project_id == project_id && auth.public_key == key => key,
        (None, Some(auth)) if auth.project_id == project_id => auth.public_key,
        (Some(key), None) => key,
        _ => return Err(ApiError(StatusCode::UNAUTHORIZED, "invalid_ingest_key")),
    };
    let project = app
        .db
        .call(move |db| ingest::lookup_project(db, project_id, &key))
        .await?
        .ok_or(ApiError(StatusCode::UNAUTHORIZED, "invalid_ingest_key"))?;
    let context = sentry::ProjectContext {
        project_id,
        slug: project.slug.clone(),
        public_key: project.public_key.clone(),
        scrub_keys: Vec::new(),
    };
    let acceptance_id = uuid::Uuid::new_v4();
    let received_at = crate::model::now_us()?;
    let replay_root = app.config.data_dir.clone();
    let (normalized, prepared_replay, permit, disk_reservation) =
        tokio::task::spawn_blocking(move || -> ApiResult<_> {
            let _held = &permit;
            let mut result = if is_envelope {
                sentry::normalize_envelope(
                    &decoded,
                    &context,
                    acceptance_id,
                    received_at,
                    &Limits::default(),
                )
            } else {
                sentry::normalize_store(
                    &decoded,
                    &context,
                    acceptance_id,
                    received_at,
                    &Limits::default(),
                )
            }
            .map_err(wire_error)?;
            let prepared_replay = result
                .replay
                .take()
                .map(|segment| -> ApiResult<_> {
                    let events =
                        sentry::replay::recording_events(&segment.recording).map_err(wire_error)?;
                    let mut frustration = sentry::replay::Frustration::default();
                    for event in &events {
                        let f = sentry::replay::frustration(event);
                        frustration.slow += f.slow;
                        frustration.dead += f.dead;
                        frustration.rage += f.rage;
                        frustration.multi += f.multi;
                    }
                    drop(events);
                    let blob = crate::storage::replay::write(&replay_root, &segment.recording)?;
                    Ok(crate::db::replays::PreparedReplay {
                        metadata: segment.metadata.clone(),
                        blob,
                        recording_bytes: segment.recording.len(),
                        frustration,
                    })
                })
                .transpose()?;
            Ok((result, prepared_replay, permit, disk_reservation))
        })
        .await
        .map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "ingest_worker_failed"))??;
    let event_id = normalized
        .records
        .first()
        .and_then(|record| record.source_event_id.clone());
    let accepted = app
        .db
        .call(move |db| {
            // The worker owns this guard even when the HTTP request is cancelled.
            let _permit = permit;
            let _disk_reservation = disk_reservation;
            ingest::accept_with_replay(
                db,
                project,
                &acceptance_id.to_string(),
                normalized.records,
                &Limits::default(),
                prepared_replay,
                normalized.feedback,
                received_at,
            )
        })
        .await
        .map_err(|error| match error.downcast_ref::<ingest::IngestError>() {
            Some(ingest::IngestError::Unauthorized) => {
                ApiError(StatusCode::UNAUTHORIZED, "invalid_ingest_key")
            }
            Some(ingest::IngestError::TooLarge) => {
                ApiError(StatusCode::PAYLOAD_TOO_LARGE, "ingest_too_large")
            }
            Some(ingest::IngestError::InvalidRecord) => {
                ApiError(StatusCode::BAD_REQUEST, "invalid_record")
            }
            Some(ingest::IngestError::InboxFull) => {
                ApiError(StatusCode::TOO_MANY_REQUESTS, "inbox_full")
            }
            None => ApiError::from(error),
        })?;
    if let Some(indexer) = &app.indexer {
        indexer.wake();
    }
    let mut response = serde_json::to_value(accepted).expect("acceptance serializes");
    if !is_envelope {
        response["id"] = json!(event_id);
    }
    Ok((StatusCode::ACCEPTED, Json(response)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn wire_errors_have_stable_public_status_and_codes() {
        for (error, status, code) in [
            (
                sentry::SentryError::TooLarge("private"),
                StatusCode::PAYLOAD_TOO_LARGE,
                "ingest_too_large",
            ),
            (
                sentry::SentryError::UnsupportedEncoding,
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_encoding",
            ),
            (
                sentry::SentryError::Malformed("private"),
                StatusCode::BAD_REQUEST,
                "invalid_sentry_payload",
            ),
        ] {
            let error = wire_error(error);
            assert_eq!(error.0, status);
            assert_eq!(error.1, code);
        }
    }

    #[test]
    fn transport_key_accepts_matching_sources_and_rejects_ambiguity() {
        let uri: Uri = "/api/1/envelope/?sentry_key=abc".parse().unwrap();
        assert_eq!(
            key_from_transport(&uri, &HeaderMap::new())
                .unwrap()
                .as_deref(),
            Some("abc")
        );

        let mut headers = HeaderMap::new();
        headers.append(
            "x-sentry-auth",
            HeaderValue::from_static("Sentry sentry_version=7, sentry_key=abc"),
        );
        assert_eq!(
            key_from_transport(&uri, &headers).unwrap().as_deref(),
            Some("abc")
        );
        headers.append(
            "x-sentry-auth",
            HeaderValue::from_static("Sentry sentry_key=other"),
        );
        assert_eq!(
            key_from_transport(&uri, &headers).unwrap_err().1,
            "conflicting_ingest_key"
        );

        for uri in [
            "/api/1/envelope/?sentry_key=",
            "/api/1/envelope/?sentry_key=one&sentry_key=two",
        ] {
            assert!(key_from_transport(&uri.parse().unwrap(), &HeaderMap::new()).is_err());
        }
        let long = format!("/api/1/envelope/?sentry_key={}", "a".repeat(257));
        assert!(key_from_transport(&long.parse().unwrap(), &HeaderMap::new()).is_err());

        let mut headers = HeaderMap::new();
        headers.insert("x-sentry-auth", HeaderValue::from_static("invalid"));
        assert!(key_from_transport(&"/".parse().unwrap(), &headers).is_err());
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-sentry-auth",
            HeaderValue::from_static("Sentry sentry_version=7"),
        );
        assert_eq!(
            key_from_transport(&"/".parse().unwrap(), &headers).unwrap(),
            None
        );
        let mut headers = HeaderMap::new();
        headers.insert("x-sentry-auth", HeaderValue::from_bytes(b"\xff").unwrap());
        assert!(key_from_transport(&"/".parse().unwrap(), &headers).is_err());
    }
}
