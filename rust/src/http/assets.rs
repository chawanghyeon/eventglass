//! Embedded production assets. Unknown API paths never fall back to HTML.

use axum::{
    http::{Method, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};

include!(concat!(env!("OUT_DIR"), "/web_assets.rs"));

pub(super) async fn serve(method: Method, uri: Uri) -> Response {
    if !matches!(method, Method::GET | Method::HEAD)
        || uri.path() == "/api"
        || uri.path().starts_with("/api/")
    {
        return StatusCode::NOT_FOUND.into_response();
    }
    let path = uri.path().trim_start_matches('/');
    let asset = ASSETS.iter().find(|(name, _)| *name == path).or_else(|| {
        if path
            .rsplit('/')
            .next()
            .is_some_and(|part| part.contains('.'))
        {
            None
        } else {
            ASSETS.iter().find(|(name, _)| *name == "index.html")
        }
    });
    let Some((name, bytes)) = asset else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mime = match name.rsplit('.').next().unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "ico" => "image/x-icon",
        "woff2" => "font/woff2",
        "json" => "application/json",
        _ => "application/octet-stream",
    };
    let cache = if name.starts_with("assets/") {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    };
    let body = if method == Method::HEAD {
        &[][..]
    } else {
        bytes
    };
    let mut response = (
        [
            (header::CONTENT_TYPE, mime),
            (header::CACHE_CONTROL, cache),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        body,
    )
        .into_response();
    response.headers_mut().insert(
        header::CONTENT_LENGTH,
        bytes
            .len()
            .to_string()
            .parse()
            .expect("byte length is a header value"),
    );
    response
}
