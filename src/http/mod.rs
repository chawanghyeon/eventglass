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
    router
}

#[cfg(test)]
mod tests {
    use super::{NativeTaskFailure, run_native};
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
}
