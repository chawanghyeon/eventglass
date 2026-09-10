//! Same-origin administration API. Public ingestion authentication is separate.

mod aggregate;
mod alerts;
#[cfg(feature = "embed-ui")]
mod assets;
mod auth;
mod ingest;
mod issue_detail;
mod issues;
mod live;
mod projects;
mod records;
mod related;
mod replays;
mod search;
mod system;

use std::sync::Arc;
use std::time::Duration;

use alerts::{
    create as create_alert, delete_alert, deliveries as alert_deliveries, list as alerts,
    retry_delivery, update as update_alert,
};
use auth::{create_user, list_users, login, logout, me, setup, update_user};
use axum::{
    Json, Router,
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{delete, get, patch, post},
};
use issues::{get_issue, list_issues, list_occurrences, update_issue};
use projects::{create_key, create_project, projects, revoke_key, update_project};
use serde_json::json;

use crate::app::AppState;

pub async fn issue_setup_token(app: &crate::app::AppState) -> anyhow::Result<String> {
    crate::auth::issue_setup_token(&app.db).await
}

#[derive(Clone)]
pub(super) struct HttpState {
    pub(super) app: AppState,
    pub(super) auth_attempts: Arc<crate::auth::AttemptLimiter>,
}

/// Keep extractor failures in the same public envelope as handler failures.
pub(super) struct ApiJson<T>(pub T);

impl<S, T> axum::extract::FromRequest<S> for ApiJson<T>
where
    S: Send + Sync,
    T: serde::de::DeserializeOwned,
{
    type Rejection = ApiError;

    async fn from_request(request: axum::extract::Request, state: &S) -> Result<Self, ApiError> {
        Json::<T>::from_request(request, state)
            .await
            .map(|Json(value)| Self(value))
            .map_err(|error| {
                let status = match error.status() {
                    StatusCode::PAYLOAD_TOO_LARGE => StatusCode::PAYLOAD_TOO_LARGE,
                    StatusCode::UNSUPPORTED_MEDIA_TYPE => StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    _ => StatusCode::BAD_REQUEST,
                };
                ApiError(status, "invalid_json_request")
            })
    }
}

#[derive(Debug)]
pub struct ApiError(pub StatusCode, pub &'static str);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = (
            self.0,
            Json(json!({"error": {
                "code": self.1,
                "message": self.1,
                "request_id": uuid::Uuid::new_v4().to_string(),
                "retryable": self.0.is_server_error() || self.0 == StatusCode::TOO_MANY_REQUESTS
            }})),
        )
            .into_response();
        if self.0 == StatusCode::TOO_MANY_REQUESTS {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("60"));
        }
        response
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        if let Some(error) = error.downcast_ref::<crate::db::auth::AuthDbError>() {
            use crate::db::auth::AuthDbError;
            return match error {
                AuthDbError::Forbidden => Self(StatusCode::FORBIDDEN, "admin_required"),
                AuthDbError::SetupCompleted => Self(StatusCode::CONFLICT, "setup_completed"),
                AuthDbError::SetupUnauthorized => {
                    Self(StatusCode::FORBIDDEN, "setup_not_authorized")
                }
                AuthDbError::Conflict => Self(StatusCode::CONFLICT, "user_exists"),
                AuthDbError::NotFound => Self(StatusCode::NOT_FOUND, "user_not_found"),
                AuthDbError::LastAdmin => Self(StatusCode::CONFLICT, "last_admin_required"),
                AuthDbError::InvalidRole => Self(StatusCode::BAD_REQUEST, "invalid_role"),
            };
        }
        if let Some(error) = error.downcast_ref::<crate::db::projects::ManagementError>() {
            use crate::db::projects::ManagementError;
            return match error {
                ManagementError::Forbidden => Self(StatusCode::FORBIDDEN, "admin_required"),
                ManagementError::Conflict => Self(StatusCode::CONFLICT, "project_exists"),
                ManagementError::NotFound => {
                    Self(StatusCode::NOT_FOUND, "project_or_key_not_found")
                }
            };
        }
        if let Some(error) = error.downcast_ref::<crate::db::alerts::AlertError>() {
            use crate::db::alerts::AlertError;
            return match error {
                AlertError::Forbidden => Self(StatusCode::FORBIDDEN, "admin_required"),
                AlertError::Invalid => Self(StatusCode::BAD_REQUEST, "invalid_alert"),
                AlertError::NotFound => Self(StatusCode::NOT_FOUND, "alert_not_found"),
                AlertError::RevisionConflict => {
                    Self(StatusCode::CONFLICT, "alert_revision_conflict")
                }
            };
        }
        if let Some(error) = error.downcast_ref::<crate::db::replays::ReplayError>() {
            use crate::db::replays::ReplayError;
            return match error {
                ReplayError::Conflict => Self(StatusCode::CONFLICT, "replay_segment_conflict"),
                ReplayError::TooLarge => Self(StatusCode::PAYLOAD_TOO_LARGE, "replay_too_large"),
                ReplayError::Forbidden => Self(StatusCode::FORBIDDEN, "replay_access_denied"),
                ReplayError::NotFound => Self(StatusCode::NOT_FOUND, "replay_not_found"),
            };
        }
        // Database/native error strings can contain sensitive input or paths.
        Self(StatusCode::SERVICE_UNAVAILABLE, "storage_unavailable")
    }
}

pub(super) type ApiResult<T> = Result<T, ApiError>;

#[derive(Debug, Eq, PartialEq)]
pub(super) enum NativeTaskFailure {
    Timeout,
    Join,
}

pub(super) async fn run_native<T, E, F>(
    permit: tokio::sync::OwnedSemaphorePermit,
    timeout: Duration,
    operation: F,
) -> Result<Result<T, E>, NativeTaskFailure>
where
    T: Send + 'static,
    E: Send + 'static,
    F: FnOnce() -> Result<T, E> + Send + 'static,
{
    let task = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        operation()
    });
    tokio::time::timeout(timeout, task)
        .await
        .map_err(|_| NativeTaskFailure::Timeout)?
        .map_err(|_| NativeTaskFailure::Join)
}

async fn no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn security_headers(mut response: Response) -> Response {
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'self'; base-uri 'none'; object-src 'none'; frame-ancestors 'none'; form-action 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data: blob:; font-src 'self' data:; frame-src 'self' blob:",
        ),
    );
    headers.insert(
        header::STRICT_TRANSPORT_SECURITY,
        HeaderValue::from_static("max-age=31536000"),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert("x-frame-options", HeaderValue::from_static("DENY"));
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(
        "permissions-policy",
        HeaderValue::from_static("camera=(), microphone=(), geolocation=(), payment=(), usb=()"),
    );
    response
}

pub fn router(app: AppState) -> Router {
    let ingest = ingest::router(app.clone());
    let router = Router::new()
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route(
            "/readyz",
            get(
                |axum::extract::State(state): axum::extract::State<HttpState>| async move {
                    let ready = state
                        .app
                        .indexer
                        .as_ref()
                        .is_some_and(|indexer| indexer.ready());
                    (
                        if ready {
                            StatusCode::OK
                        } else {
                            StatusCode::SERVICE_UNAVAILABLE
                        },
                        Json(json!({"ready":ready})),
                    )
                },
            ),
        )
        .route("/api/setup", post(setup))
        .route("/api/auth/login", post(login))
        .route("/api/auth/me", get(me))
        .route("/api/auth/logout", post(logout))
        .route("/api/users", get(list_users).post(create_user))
        .route("/api/users/{id}", patch(update_user))
        .route("/api/explore/search", post(search::post_search))
        .route("/api/explore/aggregate", post(aggregate::post_aggregate))
        .route("/api/logs", get(search::get_logs))
        .route("/api/logs/live", get(live::live))
        .route("/api/alerts", get(alerts).post(create_alert))
        .route("/api/alerts/{id}", patch(update_alert).delete(delete_alert))
        .route("/api/alert-deliveries", get(alert_deliveries))
        .route("/api/alert-deliveries/{id}/retry", post(retry_delivery))
        .route("/api/records/{detail_token}", get(records::get_record))
        .route("/api/records/{detail_token}/related", get(related::related))
        .route("/api/issues", get(list_issues))
        .route("/api/issues/{id}", get(get_issue).patch(update_issue))
        .route("/api/issues/{id}/events", get(list_occurrences))
        .route(
            "/api/issues/{id}/events/{record_id}",
            get(issue_detail::get_occurrence_detail),
        )
        .route("/api/replays", get(replays::list))
        .route("/api/replay-pages", get(replays::pages))
        .route("/api/feedback", get(replays::feedback))
        .route("/api/replays/{project}/{id}", get(replays::detail))
        .route(
            "/api/replays/{project}/{id}/analysis",
            get(replays::analysis),
        )
        .route(
            "/api/replays/{project}/{id}/segments/{segment}",
            get(replays::recording),
        )
        .route("/api/projects", get(projects).post(create_project))
        .route("/api/projects/{id}", patch(update_project))
        .route(
            "/api/projects/{id}/keys",
            post(create_key).get(projects::list_keys),
        )
        .route("/api/projects/{id}/keys/{key_id}", delete(revoke_key))
        .route("/api/system/status", get(system::status))
        .route("/api/system/doctor", get(system::doctor))
        .layer(axum::extract::DefaultBodyLimit::max(64 * 1024))
        .layer(axum::middleware::map_response(no_store))
        .with_state(HttpState {
            app,
            auth_attempts: Arc::new(crate::auth::AttemptLimiter::default()),
        })
        .merge(ingest);
    #[cfg(feature = "embed-ui")]
    let router = router.fallback(assets::serve);
    router.layer(axum::middleware::map_response(security_headers))
}

#[cfg(test)]
mod tests {
    use super::{ApiError, NativeTaskFailure, no_store, run_native, security_headers};
    use axum::{body::to_bytes, http::StatusCode, response::IntoResponse};
    use std::{sync::Arc, time::Duration};
    use tokio::sync::Semaphore;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_timeout_keeps_permit_until_blocking_work_finishes() {
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();

        let operation = tokio::spawn(run_native(permit, Duration::from_millis(20), move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok::<_, ()>(())
        }));
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(operation.await.unwrap(), Err(NativeTaskFailure::Timeout));
        assert!(semaphore.clone().try_acquire_owned().is_err());

        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if semaphore.available_permits() == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn native_task_reports_success_operation_error_and_panics() {
        for result in [Ok::<_, &'static str>(7), Err("operation")] {
            let permit = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
            assert_eq!(
                run_native(permit, Duration::from_secs(1), move || result).await,
                Ok(result)
            );
        }
        let permit = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
        assert_eq!(
            run_native(permit, Duration::from_secs(1), || -> Result<(), ()> {
                panic!("join failure fixture")
            })
            .await,
            Err(NativeTaskFailure::Join)
        );
    }

    #[tokio::test]
    async fn error_envelopes_and_security_headers_are_stable() {
        let response = ApiError(StatusCode::TOO_MANY_REQUESTS, "busy").into_response();
        assert_eq!(response.headers()["retry-after"], "60");
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["code"], "busy");
        assert_eq!(body["error"]["retryable"], true);
        assert!(body["error"]["request_id"].as_str().is_some());

        let response = no_store(StatusCode::OK.into_response()).await;
        assert_eq!(response.headers()["cache-control"], "no-store");
        let response = security_headers(response).await;
        for name in [
            "content-security-policy",
            "strict-transport-security",
            "x-content-type-options",
            "x-frame-options",
            "referrer-policy",
            "permissions-policy",
        ] {
            assert!(response.headers().contains_key(name));
        }
    }

    #[test]
    fn database_errors_map_to_non_sensitive_http_contracts() {
        use crate::db::{
            alerts::AlertError, auth::AuthDbError, projects::ManagementError, replays::ReplayError,
        };
        let cases: Vec<(anyhow::Error, StatusCode, &str)> = vec![
            (
                AuthDbError::Forbidden.into(),
                StatusCode::FORBIDDEN,
                "admin_required",
            ),
            (
                AuthDbError::SetupCompleted.into(),
                StatusCode::CONFLICT,
                "setup_completed",
            ),
            (
                AuthDbError::SetupUnauthorized.into(),
                StatusCode::FORBIDDEN,
                "setup_not_authorized",
            ),
            (
                AuthDbError::Conflict.into(),
                StatusCode::CONFLICT,
                "user_exists",
            ),
            (
                AuthDbError::NotFound.into(),
                StatusCode::NOT_FOUND,
                "user_not_found",
            ),
            (
                AuthDbError::LastAdmin.into(),
                StatusCode::CONFLICT,
                "last_admin_required",
            ),
            (
                AuthDbError::InvalidRole.into(),
                StatusCode::BAD_REQUEST,
                "invalid_role",
            ),
            (
                ManagementError::Forbidden.into(),
                StatusCode::FORBIDDEN,
                "admin_required",
            ),
            (
                ManagementError::Conflict.into(),
                StatusCode::CONFLICT,
                "project_exists",
            ),
            (
                ManagementError::NotFound.into(),
                StatusCode::NOT_FOUND,
                "project_or_key_not_found",
            ),
            (
                AlertError::Forbidden.into(),
                StatusCode::FORBIDDEN,
                "admin_required",
            ),
            (
                AlertError::Invalid.into(),
                StatusCode::BAD_REQUEST,
                "invalid_alert",
            ),
            (
                AlertError::NotFound.into(),
                StatusCode::NOT_FOUND,
                "alert_not_found",
            ),
            (
                AlertError::RevisionConflict.into(),
                StatusCode::CONFLICT,
                "alert_revision_conflict",
            ),
            (
                ReplayError::Conflict.into(),
                StatusCode::CONFLICT,
                "replay_segment_conflict",
            ),
            (
                ReplayError::TooLarge.into(),
                StatusCode::PAYLOAD_TOO_LARGE,
                "replay_too_large",
            ),
            (
                ReplayError::Forbidden.into(),
                StatusCode::FORBIDDEN,
                "replay_access_denied",
            ),
            (
                ReplayError::NotFound.into(),
                StatusCode::NOT_FOUND,
                "replay_not_found",
            ),
            (
                anyhow::anyhow!("private path"),
                StatusCode::SERVICE_UNAVAILABLE,
                "storage_unavailable",
            ),
        ];
        for (error, status, code) in cases {
            let error = ApiError::from(error);
            assert_eq!((error.0, error.1), (status, code));
        }
    }
}
