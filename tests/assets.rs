#![cfg(feature = "embed-ui")]

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use eventglass::{app::AppState, config::Config};
use tower::ServiceExt;

#[tokio::test]
async fn embedded_spa_assets_head_and_api_isolation() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let app = AppState::open(Config {
        addr: "127.0.0.1:0".parse()?,
        data_dir: dir.path().to_owned(),
        base_url: "http://localhost:8080".parse()?,
        s3_url: None,
        s3_endpoint: None,
        s3_initialize: false,
    })
    .await?;
    let router = eventglass::http::router(app);
    let home = router
        .clone()
        .oneshot(Request::get("/projects").body(Body::empty())?)
        .await?;
    assert_eq!(home.status(), StatusCode::OK);
    assert_eq!(home.headers()["cache-control"], "no-cache");
    assert_eq!(home.headers()["x-content-type-options"], "nosniff");
    let html = String::from_utf8(to_bytes(home.into_body(), 1_000_000).await?.to_vec())?;
    assert!(html.contains("<div id=\"root\">"));
    let asset = html
        .split("src=\"")
        .nth(1)
        .unwrap()
        .split('"')
        .next()
        .unwrap();
    let script = router
        .clone()
        .oneshot(Request::get(asset).body(Body::empty())?)
        .await?;
    assert_eq!(script.status(), StatusCode::OK);
    assert_eq!(
        script.headers()["cache-control"],
        "public, max-age=31536000, immutable"
    );
    let size = script.headers()["content-length"].clone();
    let head = router
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(asset)
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(head.headers()["content-length"], size);
    assert!(to_bytes(head.into_body(), 1024).await?.is_empty());
    for path in ["/api", "/api/missing", "/assets/missing.js"] {
        assert_eq!(
            router
                .clone()
                .oneshot(Request::get(path).body(Body::empty())?)
                .await?
                .status(),
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        router
            .oneshot(Request::post("/projects").body(Body::empty())?)
            .await?
            .status(),
        StatusCode::NOT_FOUND
    );
    Ok(())
}
