use eventglass::sentry::{
    SentryError,
    replay::{self, ReplaySegment},
};
use serde_json::{Value, json};

const PLAIN: &[u8] = include_bytes!("fixtures/replay/plain-0.envelope");
const COMPRESSED: &[u8] = include_bytes!("fixtures/replay/compressed-0.envelope");
fn decode(bytes: &[u8]) -> ReplaySegment {
    replay::decode_envelope(bytes, &[]).unwrap().unwrap()
}
fn pair(metadata: &Value, recording: &[u8]) -> Vec<u8> {
    let mut bytes = format!("{{}}\n{{\"type\":\"replay_event\"}}\n{metadata}\n{{\"type\":\"replay_recording\",\"length\":{}}}\n",recording.len()).into_bytes();
    bytes.extend(recording);
    bytes
}
fn metadata() -> Value {
    json!({"replay_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","segment_id":0,"replay_start_timestamp":1700000000,"timestamp":1700000010})
}

#[test]
fn movement_uses_sample_time_viewport_and_page_and_ignores_iframe_coordinates() {
    let mut analysis = eventglass::replay::Analysis::default();
    for event in [
        json!({"type":4,"timestamp":1000,"data":{"href":"https://example.test/first?secret=masked","width":1000,"height":500}}),
        json!({"type":2,"timestamp":1001,"data":{"node":{"type":0,"id":1,"childNodes":[{"type":2,"id":2},{"type":0,"id":10,"childNodes":[{"type":2,"id":11}]}]}}}),
        json!({"type":3,"timestamp":2000,"data":{"source":4,"width":500,"height":250}}),
        json!({"type":5,"timestamp":2000,"data":{"tag":"breadcrumb","payload":{"category":"navigation","data":{"to":"/second"}}}}),
        json!({"type":3,"timestamp":2100,"data":{"source":1,"positions":[{"id":2,"x":250,"y":125,"timeOffset":-200},{"id":2,"x":250,"y":125,"timeOffset":0},{"id":11,"x":1,"y":1,"timeOffset":0}]}}),
        json!({"type":3,"timestamp":2200,"data":{"source":3,"id":1,"y":500,"x":0}}),
        json!({"type":3,"timestamp":2300,"data":{"source":3,"id":10,"y":10000,"x":0}}),
    ] {
        analysis.event(&event);
    }
    analysis.finish(3000);
    assert_eq!(
        analysis.pages["https://example.test/first"].movement["5,5"],
        1
    );
    assert_eq!(
        analysis.viewport_class,
        eventglass::replay::ViewportClass::Mixed
    );
    assert_eq!(
        analysis.pages["https://example.test/first"].examples[0].timestamp_ms,
        1900
    );
    let second = &analysis.pages["https://example.test/second"];
    assert_eq!(second.movement.values().sum::<u32>(), 1);
    assert_eq!(second.movement["10,10"], 1);
    assert_eq!(second.max_scroll_viewports, 2.0);
    assert_eq!(second.scroll_reach_replays["2"], 1);
    assert_eq!(second.scroll_reach_replays["3"], 0);
    assert_eq!(analysis.journey[0].duration_ms, Some(1000));
    analysis.gap(4);
    analysis.event(&json!({"type":3,"timestamp":4000,"data":{"source":1,"positions":[{"id":2,"x":10,"y":10,"timeOffset":0}]}}));
    assert_eq!(
        analysis.pages["https://example.test/second"]
            .movement
            .values()
            .sum::<u32>(),
        1
    );
    let mut maps = eventglass::replay::PageMaps::default();
    maps.include(analysis);
    assert!(maps.truncated, "maps must disclose missing segments");
}

#[test]
fn actual_desktop_and_mobile_product_detail_recordings_explain_observed_page_behavior() {
    let mut maps = eventglass::replay::PageMaps::default();
    for (fixture, narrow) in [
        (
            include_bytes!("fixtures/replay/shopping-desktop-0.envelope").as_slice(),
            false,
        ),
        (
            include_bytes!("fixtures/replay/shopping-mobile-0.envelope").as_slice(),
            true,
        ),
    ] {
        let segment = decode(fixture);
        let text = String::from_utf8_lossy(&segment.recording);
        assert!(!text.contains("PRIVATE_TEXT_SENTINEL"));
        assert!(!text.contains("PRIVATE_PASSWORD_SENTINEL"));
        let mut analysis = eventglass::replay::Analysis::default();
        for event in replay::recording_events(&segment.recording).unwrap() {
            analysis.event(&event);
        }
        analysis.finish(segment.metadata.finished_at_ms);
        let (url, product) = analysis
            .pages
            .iter()
            .find(|(url, _)| url.ends_with("/products/linen-shirt"))
            .expect("product detail page");
        assert_eq!(
            analysis.viewport_class,
            if narrow {
                eventglass::replay::ViewportClass::Narrow
            } else {
                eventglass::replay::ViewportClass::Wide
            }
        );
        let example = product
            .examples
            .iter()
            .find(|e| e.kind == "element" && e.key.contains("expand-review"))
            .expect("real SDK click example");
        assert!(
            analysis
                .timeline
                .iter()
                .any(|e| e.timestamp_ms == example.timestamp_ms && e.label == example.key)
        );
        assert!(!url.contains("campaign"));
        assert_eq!(product.visits, 1);
        assert_eq!(product.sampled_replays, 1);
        assert_eq!(product.timed_visits, 1);
        assert!(product.observed_time_ms > 0);
        assert_eq!(
            product.last_observed_replays, 0,
            "cart navigation is observed, not a conversion"
        );
        assert_eq!(product.narrow_replays, u32::from(narrow));
        assert_eq!(product.wide_replays, u32::from(!narrow));
        assert!(product.max_scroll_viewports >= 2.0);
        assert!(
            product
                .next_pages
                .iter()
                .any(|(url, count)| url.ends_with("/cart") && *count == 1)
        );
        assert!(
            product
                .depth_clicks
                .iter()
                .any(|(depth, count)| depth.parse::<u32>().unwrap() >= 2 && *count > 0)
        );
        assert!(
            product
                .depth_elements
                .iter()
                .any(|(depth, elements)| depth.parse::<u32>().unwrap() >= 2
                    && elements.keys().any(|label| label.contains("expand-review")))
        );
        assert!(
            product
                .elements
                .keys()
                .any(|label| label.contains("add-to-cart"))
        );
        maps.include(analysis);
    }
    let product = maps
        .pages
        .iter()
        .find(|(url, _)| url.ends_with("/products/linen-shirt"))
        .unwrap()
        .1;
    assert_eq!(product.sampled_replays, 2);
    assert_eq!(product.visits, 2);
    assert_eq!((product.narrow_replays, product.wide_replays), (1, 1));
}

#[test]
fn delayed_frustration_belongs_to_the_click_page_not_the_later_cart_page() {
    let mut analysis = eventglass::replay::Analysis::default();
    for event in [
        json!({"type":4,"timestamp":1000,"data":{"href":"https://shop.test/products/shirt","width":390,"height":844}}),
        json!({"type":5,"timestamp":2000,"data":{"tag":"breadcrumb","payload":{"category":"navigation","data":{"to":"/cart"}}}}),
        // Emitted later by the SDK detector, retaining the original click timestamp.
        json!({"type":5,"timestamp":1500,"data":{"tag":"breadcrumb","payload":{"category":"ui.slowClickDetected","data":{"endReason":"timeout","node":{"tagName":"BUTTON"},"clickCount":5}}}}),
    ] {
        analysis.event(&event);
    }
    analysis.finish(10000);
    assert_eq!(
        analysis.pages["https://shop.test/products/shirt"]
            .frustration
            .rage,
        1
    );
    assert_eq!(analysis.pages["https://shop.test/cart"].frustration.rage, 0);
    let signal = analysis
        .timeline
        .iter()
        .find(|event| event.kind == "rage click")
        .unwrap();
    assert_eq!(
        signal.url.as_deref(),
        Some("https://shop.test/products/shirt")
    );
}

#[test]
fn official_sdk_plain_and_zlib_recordings_preserve_privacy() {
    for bytes in [PLAIN, COMPRESSED] {
        let segment = decode(bytes);
        assert_eq!(segment.metadata.sdk_version.as_deref(), Some("10.73.0"));
        assert_eq!(segment.metadata.segment_id, 0);
        assert_eq!(segment.metadata.user.unwrap()["id"], "replay-fixture-user");
        let text = String::from_utf8(segment.recording.clone()).unwrap();
        for secret in [
            "PRIVATE_TEXT_SENTINEL",
            "PRIVATE_PASSWORD_SENTINEL",
            "BLOCKED_SENTINEL",
        ] {
            assert!(!text.contains(secret), "{secret}");
        }
        assert!(text.contains("****"));
        let events = replay::recording_events(&segment.recording).unwrap();
        assert!(events.iter().any(|e| e["type"] == 2), "DOM snapshot");
        assert!(
            events
                .iter()
                .any(|e| e["type"] == 4 && e["data"]["width"] == 1000)
        );
        assert!(
            events
                .iter()
                .any(|e| e["type"] == 3 && e["data"]["source"] == 1),
            "movement"
        );
        assert!(
            events
                .iter()
                .any(|e| e["type"] == 3 && e["data"]["source"] == 2 && e["data"]["type"] == 2),
            "click"
        );
        assert!(!segment.metadata.trace_ids.is_empty());
    }
}

#[test]
fn official_sdk_later_segments_include_scroll_navigation_errors_network_and_dead_click() {
    let dead = decode(include_bytes!("fixtures/replay/plain-1.envelope"));
    let events = replay::recording_events(&dead.recording).unwrap();
    assert!(events.iter().any(|e| replay::frustration(e).dead == 1));
    let last = decode(include_bytes!("fixtures/replay/plain-2.envelope"));
    assert_eq!(dead.metadata.replay_id, last.metadata.replay_id);
    assert_eq!(last.metadata.segment_id, 2);
    assert_eq!(last.metadata.error_ids.len(), 1);
    assert!(!last.metadata.trace_ids.is_empty());
    assert!(last.metadata.urls.iter().any(|u| u.ends_with("/checkout")));
    let events = replay::recording_events(&last.recording).unwrap();
    assert!(
        events
            .iter()
            .any(|e| e["type"] == 3 && e["data"]["source"] == 3 && e["data"]["y"] == 1200)
    );
    assert!(
        events
            .iter()
            .any(|e| e.pointer("/data/payload/category") == Some(&json!("navigation")))
    );
    let network = events
        .iter()
        .find(|e| e.pointer("/data/payload/op") == Some(&json!("resource.fetch")))
        .unwrap();
    let ms = replay::event_timestamp_ms(network).unwrap();
    assert!(ms >= last.metadata.started_at_ms && ms <= last.metadata.finished_at_ms);
}

#[test]
fn segment_header_id_and_compression_are_validated() {
    for bytes in [
        b"{\"segment_id\":1}\n[]".as_slice(),
        b"{\"segment_id\":0}\nnot-zlib",
    ] {
        assert!(replay::decode_envelope(&pair(&metadata(), bytes), &[]).is_err());
    }
    assert!(replay::decode_envelope(b"{}\n{\"type\":\"replay_event\"}\n{}", &[]).is_err());
    assert!(
        replay::decode_envelope(b"{}\n{\"type\":\"transaction\"}\n{}", &[])
            .unwrap()
            .is_none()
    );
}

#[test]
fn compressed_bomb_is_rejected_before_json_allocation() {
    use std::io::Write;
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder
        .write_all(&vec![b' '; replay::MAX_RECORDING_BYTES + 1])
        .unwrap();
    let mut body = b"{\"segment_id\":0}\n".to_vec();
    body.extend(encoder.finish().unwrap());
    assert!(matches!(
        replay::decode_envelope(&pair(&metadata(), &body), &[]),
        Err(SentryError::TooLarge(_))
    ));
}

#[test]
fn known_secrets_scrubbed_without_unmasking_recording() {
    let event = json!({"type":5,"timestamp":1700000000000u64,"data":{"tag":"breadcrumb","payload":{"message":"****","data":{"password":"secret-value","custom_secret":"custom-value"}}}}); // pragma: allowlist secret (synthetic privacy sentinels)
    let body = format!("{{\"segment_id\":0}}\n[{event}]");
    let segment = replay::decode_envelope(
        &pair(&metadata(), body.as_bytes()),
        &["custom_secret".into()],
    )
    .unwrap()
    .unwrap();
    let text = String::from_utf8(segment.recording).unwrap();
    assert!(!text.contains("secret-value"));
    assert!(!text.contains("custom-value"));
    assert!(text.contains("****"));
}

#[test]
fn frustration_matches_sentry_predicates_without_guessing() {
    let mut event = json!({"type":5,"timestamp":1700000000000u64,"data":{"tag":"breadcrumb","payload":{"category":"ui.slowClickDetected","data":{"endReason":"timeout","node":{"tagName":"BUTTON"},"clickCount":5}}}});
    assert_eq!(
        replay::frustration(&event),
        replay::Frustration {
            slow: 1,
            dead: 1,
            rage: 1,
            multi: 0
        }
    );
    event["data"]["payload"]["data"]["endReason"] = json!("mutation");
    assert_eq!(replay::frustration(&event).rage, 0);
    event["data"]["payload"]["category"] = json!("ui.multiClick");
    assert_eq!(
        replay::frustration(&event),
        replay::Frustration {
            multi: 1,
            rage: 1,
            ..Default::default()
        }
    );
}

#[tokio::test]
async fn durable_http_segments_are_atomic_idempotent_order_independent_and_authorized()
-> anyhow::Result<()> {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    let dir = tempfile::tempdir()?;
    let state = eventglass::app::AppState::open(eventglass::config::Config {
        addr: "127.0.0.1:0".parse()?,
        data_dir: dir.path().to_owned(),
        base_url: "http://localhost:8080".parse()?,
        s3_url: None,
        s3_endpoint: None,
        s3_initialize: false,
    })
    .await?;
    let token = "a".repeat(64);
    use sha2::Digest;
    let hash = format!("{:x}", sha2::Sha256::digest(token.as_bytes()));
    state.db.call(move |db| {
        db.execute_batch("INSERT INTO projects(id,slug,name,created_at_us,updated_at_us) VALUES(1,'replay','Replay',0,0); INSERT INTO project_keys(id,project_id,public_key,created_at_us) VALUES(1,1,'public',0); INSERT INTO users(id,email,password_hash,role,created_at_us,updated_at_us) VALUES(1,'fixture@example.invalid','unused','admin',0,0);")?;
        eventglass::db::auth::create_session(db,1,"unused",&hash,eventglass::model::now_us()?,eventglass::model::now_us()?+1_000_000_000)?;
        Ok(())
    }).await?;
    let router = eventglass::http::router(state.clone());
    let replay_id = decode(PLAIN).metadata.replay_id;
    let last = include_bytes!("fixtures/replay/plain-2.envelope").as_slice();
    let middle = include_bytes!("fixtures/replay/plain-1.envelope").as_slice();
    let get = |path: String| {
        Request::get(path)
            .header("cookie", format!("eventglass_session={token}"))
            .body(Body::empty())
            .unwrap()
    };
    for bytes in [last, PLAIN, last] {
        let response = router
            .clone()
            .oneshot(
                Request::post("/api/1/envelope/?sentry_key=public")
                    .body(Body::from(bytes.to_vec()))?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }
    let path = format!("/api/replays/1/{replay_id}");
    let response = router.clone().oneshot(get(path.clone())).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let detail: Value = serde_json::from_slice(&to_bytes(response.into_body(), 1_000_000).await?)?;
    assert_eq!(detail["replay"]["segment_count"], 2);
    assert_eq!(detail["replay"]["partial"], true);
    assert_eq!(
        detail["replay"]["metadata"]["error_ids"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let response = router
        .clone()
        .oneshot(Request::get(&path).body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let partial_pages = router
        .clone()
        .oneshot(get("/api/replay-pages?project_id=1".into()))
        .await?;
    assert_eq!(partial_pages.status(), StatusCode::OK);
    let partial_pages: Value =
        serde_json::from_slice(&to_bytes(partial_pages.into_body(), 1_000_000).await?)?;
    assert_eq!(partial_pages["truncated"], true);
    let response = router
        .clone()
        .oneshot(
            Request::post("/api/1/envelope/?sentry_key=public")
                .body(Body::from(middle.to_vec()))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let response = router.clone().oneshot(get(path.clone())).await?;
    let detail: Value = serde_json::from_slice(&to_bytes(response.into_body(), 1_000_000).await?)?;
    assert_eq!(detail["replay"]["segment_count"], 3);
    assert_eq!(detail["replay"]["partial"], false);
    assert_eq!(detail["replay"]["frustration"]["dead"], 1);
    let response = router
        .clone()
        .oneshot(get(format!("{path}/analysis")))
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let analysis: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1_000_000).await?)?;
    assert_eq!(analysis["journey"].as_array().unwrap().len(), 2);
    assert!(
        analysis["pages"]
            .as_object()
            .unwrap()
            .values()
            .any(|p| p["max_scroll_viewports"].as_f64() == Some(2.0))
    );
    assert!(
        analysis["pages"]
            .as_object()
            .unwrap()
            .values()
            .any(|p| !p["clicks"].as_object().unwrap().is_empty())
    );
    assert!(
        analysis["pages"]
            .as_object()
            .unwrap()
            .values()
            .any(|p| !p["movement"].as_object().unwrap().is_empty())
    );
    let response = router
        .clone()
        .oneshot(get(format!("{path}/segments/0")))
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let events: Value = serde_json::from_slice(&to_bytes(response.into_body(), 1_000_000).await?)?;
    assert!(
        events["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["type"] == 2)
    );
    let started = decode(PLAIN).metadata.started_at_ms;
    for (range, count) in [
        (format!("started_after_ms={started}"), 1),
        (format!("started_before_ms={started}"), 0),
    ] {
        let response = router
            .clone()
            .oneshot(get(format!("/api/replays?project_id=1&{range}")))
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let value: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 1_000_000).await?)?;
        assert_eq!(value["items"].as_array().unwrap().len(), count);
    }
    let response = router
        .clone()
        .oneshot(get(
            "/api/replay-pages?project_id=1&started_after_ms=2&started_before_ms=1".into(),
        ))
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let response = router
        .clone()
        .oneshot(get("/api/replays?project_id=1&viewport=wide".into()))
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    for group in ["narrow", "wide", "mixed", "unknown"] {
        let response = router
            .clone()
            .oneshot(get(format!(
                "/api/replay-pages?project_id=1&viewport={group}"
            )))
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let value: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 1_000_000).await?)?;
        assert_eq!(
            value["replays_analyzed"],
            u32::from(analysis["viewport_class"] == group)
        );
        for page in value["pages"].as_object().unwrap().values() {
            for sample in page["examples"].as_array().unwrap() {
                assert_eq!(
                    sample["replay_id"],
                    detail["replay"]["metadata"]["replay_id"]
                );
            }
        }
    }
    // A conflicting retry rolls back a normal Event in the SAME envelope.
    let altered =
        String::from_utf8(PLAIN.to_vec())?.replace("replay-fixture@1", "replay-fixture@2");
    let mixed = format!("{altered}\n{{\"type\":\"event\"}}\n{{\"message\":\"must roll back\"}}");
    let response = router
        .clone()
        .oneshot(Request::post("/api/1/envelope/?sentry_key=public").body(Body::from(mixed))?)
        .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    state
        .db
        .call(|db| {
            assert_eq!(
                db.query_row("SELECT count(*) FROM inbox", [], |r| r.get::<_, i64>(0))?,
                0
            );
            Ok(())
        })
        .await?;
    let response = router
        .clone()
        .oneshot(get("/api/system/status".into()))
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let status: Value = serde_json::from_slice(&to_bytes(response.into_body(), 1_000_000).await?)?;
    assert_eq!(status["replay"]["active_replays"], "1");
    assert_eq!(status["replay"]["segments"], "3");
    assert_eq!(status["sentry_ingest_since_start"]["conflict"], "1");
    assert!(
        status["sentry_ingest_since_start"]["accepted"]
            .as_str()
            .unwrap()
            .parse::<u32>()?
            >= 3
    );
    let core = state.clone().start_core().await?;
    let response = router
        .clone()
        .oneshot(
            Request::post("/api/1/envelope/?sentry_key=public").body(Body::from(
                include_bytes!("fixtures/replay/plain-error.envelope").to_vec(),
            ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let response = router.clone().oneshot(get(path.clone())).await?;
        let detail: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 1_000_000).await?)?;
        if detail["associations"]["errors"]
            .as_array()
            .is_some_and(|v| v.len() == 1)
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "SDK Error association did not index"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    // Official Feedback can arrive independently of a recording and is idempotent.
    for _ in 0..2 {
        let response = router
            .clone()
            .oneshot(
                Request::post("/api/1/envelope/?sentry_key=public").body(Body::from(
                    include_bytes!("fixtures/replay/feedback.envelope").to_vec(),
                ))?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }
    let response = router
        .clone()
        .oneshot(get("/api/feedback?project_id=1".into()))
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let feedback: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1_000_000).await?)?;
    assert_eq!(feedback["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        feedback["items"][0]["contexts"]["feedback"]["message"],
        "Replay fixture feedback"
    );
    let blob = state
        .db
        .call(|db| {
            db.query_row(
                "SELECT blob_sha256 FROM replay_segments ORDER BY segment_id LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .map_err(Into::into)
        })
        .await?;
    let blob_path = state
        .config
        .data_dir
        .join(format!("replay-blobs/{blob}.zlib"));
    let original_blob = std::fs::read(&blob_path)?;
    std::fs::write(&blob_path, b"corrupt replay")?;
    let damaged = router
        .clone()
        .oneshot(get(format!("{path}/analysis")))
        .await?;
    assert_eq!(damaged.status(), StatusCode::SERVICE_UNAVAILABLE);
    std::fs::write(blob_path, original_blob)?;
    core.alerts.as_ref().unwrap().shutdown().await?;
    core.indexer.as_ref().unwrap().shutdown().await?;
    // Public ingest key never grants access to recordings; disabled projects lose access.
    state
        .db
        .call(|db| {
            db.execute("UPDATE projects SET is_active=0 WHERE id=1", [])?;
            Ok(())
        })
        .await?;
    assert_eq!(
        router.clone().oneshot(get(path)).await?.status(),
        StatusCode::FORBIDDEN
    );
    let refs = state
        .db
        .call(|db| eventglass::db::replays::blob_references(db))
        .await?;
    assert_eq!(refs.len(), 3);
    for reference in refs {
        assert!(!eventglass::storage::replay::read(dir.path(), &reference)?.is_empty());
    }
    let root = dir.path().to_owned();
    state
        .db
        .call(move |db| {
            eventglass::db::replays::expire_at_startup(
                db,
                &root,
                eventglass::model::now_us()? + eventglass::db::replays::RETENTION_US + 1,
            )?;
            assert_eq!(
                db.query_row("SELECT count(*) FROM replay_segments", [], |r| r
                    .get::<_, i64>(0))?,
                0
            );
            assert_eq!(std::fs::read_dir(root.join("replay-blobs"))?.count(), 0);
            Ok(())
        })
        .await?;
    Ok(())
}
