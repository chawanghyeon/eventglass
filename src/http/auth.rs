//! Authentication HTTP DTOs, header checks, cookies, and response conversion.

use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::{Value, json};

use super::{ApiError, ApiResult, HttpState};

const SESSION_TTL_US: i64 = 7 * 24 * 60 * 60 * 1_000_000;
const SESSION_MAX_AGE_SECONDS: i64 = 7 * 24 * 60 * 60;

pub(super) struct Principal {
    pub(super) id: i64,
    email: String,
    role: String,
    token: String,
}

fn one_header<'a>(headers: &'a HeaderMap, name: &'static str) -> Option<&'a str> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?.to_str().ok()?;
    values.next().is_none().then_some(value)
}

fn check_origin(headers: &HeaderMap, state: &HttpState) -> ApiResult<()> {
    let expected = state.app.config.base_url.origin().ascii_serialization();
    if one_header(headers, header::ORIGIN.as_str()) != Some(expected.as_str()) {
        return Err(ApiError(StatusCode::FORBIDDEN, "invalid_origin"));
    }
    Ok(())
}

fn record_attempt(state: &HttpState, kind: crate::auth::AttemptKind) -> ApiResult<()> {
    state
        .auth_attempts
        .record(kind)
        .map_err(|error| match error {
            crate::auth::AttemptError::Limited => {
                ApiError(StatusCode::TOO_MANY_REQUESTS, "auth_rate_limited")
            }
            crate::auth::AttemptError::Unavailable => {
                ApiError(StatusCode::SERVICE_UNAVAILABLE, "rate_limit_unavailable")
            }
        })
}

fn session_token(headers: &HeaderMap) -> ApiResult<String> {
    let mut found = None;
    for header_value in headers.get_all(header::COOKIE).iter() {
        let raw = header_value
            .to_str()
            .map_err(|_| ApiError(StatusCode::UNAUTHORIZED, "authentication_required"))?;
        for pair in raw.split(';') {
            let Some((name, value)) = pair.trim().split_once('=') else {
                continue;
            };
            if name == "eventglass_session" && found.replace(value).is_some() {
                return Err(ApiError(
                    StatusCode::UNAUTHORIZED,
                    "authentication_required",
                ));
            }
        }
    }
    found
        .filter(|value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .map(str::to_owned)
        .ok_or(ApiError(
            StatusCode::UNAUTHORIZED,
            "authentication_required",
        ))
}

fn session_cookie(state: &HttpState, token: &str, max_age_seconds: i64) -> String {
    let secure = if state.app.config.base_url.scheme() == "https" {
        "; Secure"
    } else {
        ""
    };
    format!(
        "eventglass_session={token}; HttpOnly; SameSite=Lax; Path=/; Max-Age={max_age_seconds}{secure}"
    )
}

fn no_store(response: &mut Response) {
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
}

fn json_no_store(value: Value) -> Response {
    let mut response = Json(value).into_response();
    no_store(&mut response);
    response
}

pub(super) async fn authenticate(
    state: &HttpState,
    headers: &HeaderMap,
    mutation: bool,
    admin: bool,
) -> ApiResult<Principal> {
    let token = session_token(headers)?;
    if mutation {
        check_origin(headers, state)?;
        let presented_csrf = one_header(headers, "x-csrf-token")
            .ok_or(ApiError(StatusCode::FORBIDDEN, "invalid_csrf"))?;
        let expected_csrf = crate::auth::csrf_token(&token);
        if !crate::auth::secure_eq(presented_csrf, &expected_csrf) {
            return Err(ApiError(StatusCode::FORBIDDEN, "invalid_csrf"));
        }
    }
    let token_hash = crate::auth::hash_token(&token);
    let principal = state
        .app
        .db
        .call(move |db| crate::db::auth::authenticate(db, &token_hash, crate::model::now_us()?))
        .await?
        .ok_or(ApiError(
            StatusCode::UNAUTHORIZED,
            "authentication_required",
        ))?;
    if admin && principal.role != "admin" {
        return Err(ApiError(StatusCode::FORBIDDEN, "admin_required"));
    }
    Ok(Principal {
        id: principal.id,
        email: principal.email,
        role: principal.role,
        token,
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SetupInput {
    token: String,
    email: String,
    password: String,
}

pub(super) async fn setup(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(input): Json<SetupInput>,
) -> ApiResult<StatusCode> {
    check_origin(&headers, &state)?;
    record_attempt(&state, crate::auth::AttemptKind::Setup)?;
    let email = crate::auth::normalize_credentials(&input.email, &input.password).ok_or(
        ApiError(StatusCode::BAD_REQUEST, "invalid_credentials_format"),
    )?;
    if input.token.len() != 64 || !input.token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ApiError(StatusCode::FORBIDDEN, "setup_not_authorized"));
    }
    let password_hash =
        crate::auth::hash_password(state.app.password_permit.clone(), input.password)
            .await
            .map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "hash_unavailable"))?;
    let token_hash = crate::auth::hash_token(&input.token);
    state
        .app
        .db
        .call(move |db| {
            crate::db::auth::complete_setup(
                db,
                &token_hash,
                &email,
                &password_hash,
                crate::model::now_us()?,
            )
        })
        .await?;
    Ok(StatusCode::CREATED)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Credentials {
    email: String,
    password: String,
}

pub(super) async fn login(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(input): Json<Credentials>,
) -> ApiResult<Response> {
    check_origin(&headers, &state)?;
    record_attempt(&state, crate::auth::AttemptKind::Login)?;
    if !crate::auth::valid_login_lengths(&input.email, &input.password) {
        return Err(ApiError(StatusCode::UNAUTHORIZED, "invalid_credentials"));
    }
    let email = input.email.to_lowercase();
    let candidate = state
        .app
        .db
        .call(move |db| crate::db::auth::login_candidate(db, &email))
        .await?;
    let expected_hash = candidate
        .as_ref()
        .map(|candidate| candidate.password_hash.clone());
    let valid = crate::auth::verify_password(
        state.app.password_permit.clone(),
        input.password,
        expected_hash,
    )
    .await
    .map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "hash_unavailable"))?;
    let Some(candidate) = candidate.filter(|_| valid) else {
        return Err(ApiError(StatusCode::UNAUTHORIZED, "invalid_credentials"));
    };

    let token = crate::auth::random_token();
    let token_hash = crate::auth::hash_token(&token);
    let expected_password_hash = candidate.password_hash;
    let user_id = candidate.id;
    let created = state
        .app
        .db
        .call(move |db| {
            let now = crate::model::now_us()?;
            let expires = now
                .checked_add(SESSION_TTL_US)
                .ok_or_else(|| anyhow::anyhow!("session expiry overflow"))?;
            crate::db::auth::create_session(
                db,
                user_id,
                &expected_password_hash,
                &token_hash,
                now,
                expires,
            )
        })
        .await?;
    if !created {
        return Err(ApiError(StatusCode::UNAUTHORIZED, "invalid_credentials"));
    }
    let mut response = (
        [(
            header::SET_COOKIE,
            session_cookie(&state, &token, SESSION_MAX_AGE_SECONDS),
        )],
        Json(json!({"csrf_token": crate::auth::csrf_token(&token)})),
    )
        .into_response();
    no_store(&mut response);
    Ok(response)
}

pub(super) async fn me(State(state): State<HttpState>, headers: HeaderMap) -> ApiResult<Response> {
    let principal = authenticate(&state, &headers, false, false).await?;
    Ok(json_no_store(json!({
        "id": principal.id.to_string(),
        "email": principal.email,
        "role": principal.role,
        "csrf_token": crate::auth::csrf_token(&principal.token)
    })))
}

pub(super) async fn logout(
    State(state): State<HttpState>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let principal = authenticate(&state, &headers, true, false).await?;
    let token_hash = crate::auth::hash_token(&principal.token);
    state
        .app
        .db
        .call(move |db| crate::db::auth::logout(db, &token_hash))
        .await?;
    let mut response = (
        [(header::SET_COOKIE, session_cookie(&state, "", 0))],
        StatusCode::NO_CONTENT,
    )
        .into_response();
    no_store(&mut response);
    Ok(response)
}

pub(super) async fn list_users(
    State(state): State<HttpState>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let principal = authenticate(&state, &headers, false, true).await?;
    let users = state
        .app
        .db
        .call(move |db| crate::db::auth::list_users(db, principal.id))
        .await?;
    Ok(json_no_store(json!({"items": users})))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CreateUserInput {
    email: String,
    password: String,
    role: String,
}

pub(super) async fn create_user(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(input): Json<CreateUserInput>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let principal = authenticate(&state, &headers, true, true).await?;
    let email = crate::auth::normalize_credentials(&input.email, &input.password).ok_or(
        ApiError(StatusCode::BAD_REQUEST, "invalid_credentials_format"),
    )?;
    if !matches!(input.role.as_str(), "admin" | "member") {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_role"));
    }
    let password_hash =
        crate::auth::hash_password(state.app.password_permit.clone(), input.password)
            .await
            .map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "hash_unavailable"))?;
    let role = input.role;
    let id = state
        .app
        .db
        .call(move |db| {
            crate::db::auth::create_user(
                db,
                principal.id,
                &email,
                &password_hash,
                &role,
                crate::model::now_us()?,
            )
        })
        .await?;
    Ok((StatusCode::CREATED, Json(json!({"id": id.to_string()}))))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct UpdateUserInput {
    role: Option<String>,
    is_active: Option<bool>,
}

pub(super) async fn update_user(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Json(input): Json<UpdateUserInput>,
) -> ApiResult<StatusCode> {
    let principal = authenticate(&state, &headers, true, true).await?;
    if input.role.is_none() && input.is_active.is_none() {
        return Err(ApiError(StatusCode::BAD_REQUEST, "empty_user_update"));
    }
    if input
        .role
        .as_deref()
        .is_some_and(|role| !matches!(role, "admin" | "member"))
    {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_role"));
    }
    state
        .app
        .db
        .call(move |db| {
            crate::db::auth::update_user(
                db,
                principal.id,
                id,
                input.role.as_deref(),
                input.is_active,
                crate::model::now_us()?,
            )
        })
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
