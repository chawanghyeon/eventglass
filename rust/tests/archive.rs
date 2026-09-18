use anyhow::Result;
use eventglass::{
    model::Boundary,
    search::active::ActiveShard,
    storage::{archive, manifest::ShardStats},
};
use flate2::{Compression, write::GzEncoder};
use std::fs::File;

fn sealed(root: &std::path::Path) -> Result<(String, String)> {
    let installation = uuid::Uuid::new_v4().to_string();
    let shard = uuid::Uuid::new_v4().to_string();
    let mut active = ActiveShard::create(root, &installation, &shard, Boundary::default())?;
    active.publish(Boundary::default())?;
    active.seal(
        root,
        ShardStats {
            record_count: 0,
            min_timestamp_us: None,
            max_timestamp_us: None,
            min_received_at_us: None,
            max_received_at_us: None,
            min_ingest_seq: None,
            max_ingest_seq: None,
        },
        1,
    )?;
    Ok((installation, shard))
}

#[test]
fn archive_uses_the_manifest_set_and_hydrates_atomically() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("source");
    std::fs::create_dir(&source)?;
    let (installation, shard) = sealed(&source)?;
    std::fs::write(source.join("ignored.tmp"), b"must not be archived")?;
    let compressed = directory.path().join("shard.tar.gz");
    let artifact = archive::create(&source, &compressed, &installation, &shard, || false)?;
    assert_eq!(artifact.sha256.len(), 64);
    assert_eq!(artifact.size, std::fs::metadata(&compressed)?.len());

    let restored = directory.path().join("restored");
    let manifest = archive::hydrate(
        &compressed,
        &restored,
        &installation,
        &shard,
        16 * 1024 * 1024,
        || false,
    )?;
    assert_eq!(manifest.shard_id, shard);
    assert!(!restored.join("ignored.tmp").exists());
    assert!(
        archive::hydrate(
            &compressed,
            &restored,
            &installation,
            &shard,
            16 * 1024 * 1024,
            || false,
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn hydrate_rejects_unsafe_entries_limits_and_cancellation_without_partial_install() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let malicious = directory.path().join("malicious.tar.gz");
    let output = File::create(&malicious)?;
    let encoder = GzEncoder::new(output, Compression::fast());
    let mut builder = tar::Builder::new(encoder);
    let bytes = b"escape";
    let mut header = tar::Header::new_gnu();
    header.set_size(bytes.len() as u64);
    header.set_mode(0o600);
    header.set_cksum();
    builder.append_data(&mut header, "nested/escape", &bytes[..])?;
    builder.into_inner()?.finish()?.sync_all()?;
    let destination = directory.path().join("unsafe");
    assert!(
        archive::hydrate(
            &malicious,
            &destination,
            &uuid::Uuid::new_v4().to_string(),
            &uuid::Uuid::new_v4().to_string(),
            1024,
            || false,
        )
        .is_err()
    );
    assert!(!destination.exists());

    let source = directory.path().join("source");
    std::fs::create_dir(&source)?;
    let (installation, shard) = sealed(&source)?;
    let cancelled = directory.path().join("cancelled.tar.gz");
    assert!(archive::create(&source, &cancelled, &installation, &shard, || true).is_err());
    assert!(!cancelled.exists());

    let valid = directory.path().join("valid.tar.gz");
    archive::create(&source, &valid, &installation, &shard, || false)?;
    let limited = directory.path().join("limited");
    assert!(archive::hydrate(&valid, &limited, &installation, &shard, 1, || false).is_err());
    assert!(!limited.exists());
    Ok(())
}

#[test]
fn compression_candidates_preserve_native_archives_and_report_work() -> Result<()> {
    use eventglass::{
        config::Limits,
        sentry::{self, ProjectContext},
    };
    use serde_json::json;
    use std::{io::Write, time::Instant};

    let mut reports = Vec::new();
    for (profile, count, varied) in [
        ("empty", 0u32, false),
        ("repeated_logs", 4000, false),
        ("varied_logs", 4000, true),
    ] {
        let directory = tempfile::tempdir()?;
        let source = directory.path().join("source");
        std::fs::create_dir(&source)?;
        let installation = uuid::Uuid::from_u128(20260912).to_string();
        let shard = uuid::Uuid::from_u128(20260913).to_string();
        let mut active = ActiveShard::create(&source, &installation, &shard, Boundary::default())?;
        active.publish(Boundary::default())?;
        let context = ProjectContext {
            project_id: 1,
            slug: "compression".into(),
            public_key: "fixture".into(),
            scrub_keys: vec![],
        };
        for batch in 0..count / 1000 {
            let mut records = Vec::new();
            for offset in 0..1000 {
                let index = batch * 1000 + offset;
                let message = if varied {
                    format!(
                        "request {} customer {:032x} status {} duration {}",
                        index,
                        index as u128 * 7919,
                        200 + index % 7,
                        index % 900
                    )
                } else {
                    "health observation for checkout request completed".to_owned()
                };
                let payload = json!({"version": 2, "items": [{
                    "body": message, "timestamp": 1788860000, "level": "info",
                    "attributes": {
                        "host": {"type": "string", "value": format!("node-{}", index % 40)},
                        "route": {"type": "string", "value": "/checkout"},
                        "request_id": {"type": "string", "value": format!("req-{index:016x}")}
                    }
                }]})
                .to_string();
                let envelope = format!(
                    "{{}}\n{}\n{payload}\n",
                    json!({"type": "log", "length": payload.len()})
                );
                let mut record = sentry::normalize_envelope(
                    envelope.as_bytes(),
                    &context,
                    uuid::Uuid::from_u128(index as u128 + 1),
                    1788860000000000,
                    &Limits::default(),
                )?
                .records
                .remove(0);
                assert_eq!(record.kind, eventglass::model::RecordKind::Log);
                record.ingest_seq = i64::from(index + 1);
                records.push(record);
            }
            let boundary = Boundary {
                inbox_id: i64::from(batch + 1),
                ingest_seq: i64::from((batch + 1) * 1000),
            };
            active.commit(&records, boundary)?;
            active.publish(boundary)?;
        }
        active.seal(
            &source,
            ShardStats {
                record_count: u64::from(count),
                min_timestamp_us: (count > 0).then_some(1788860000000000),
                max_timestamp_us: (count > 0).then_some(1788860000000000),
                min_received_at_us: (count > 0).then_some(1788860000000000),
                max_received_at_us: (count > 0).then_some(1788860000000000),
                min_ingest_seq: (count > 0).then_some(1),
                max_ingest_seq: (count > 0).then_some(i64::from(count)),
            },
            1788860000000000,
        )?;
        let original = directory.path().join("original.tar.gz");
        archive::create(&source, &original, &installation, &shard, || false)?;
        let raw = directory.path().join("archive.tar");
        std::io::copy(
            &mut flate2::read::GzDecoder::new(File::open(&original)?),
            &mut File::create(&raw)?,
        )?;
        let mut variants = Vec::new();
        for level in [1, 6] {
            let compressed = directory.path().join(format!("level-{level}.tar.gz"));
            let mut elapsed_us = Vec::new();
            for _ in 0..3 {
                let start = Instant::now();
                let mut encoder =
                    GzEncoder::new(File::create(&compressed)?, Compression::new(level));
                std::io::copy(&mut File::open(&raw)?, &mut encoder)?;
                let mut output = encoder.finish()?;
                output.flush()?;
                output.sync_all()?;
                elapsed_us.push(start.elapsed().as_micros());
            }
            let restored = directory.path().join(format!("restored-{level}"));
            let manifest = archive::hydrate(
                &compressed,
                &restored,
                &installation,
                &shard,
                64 * 1024 * 1024,
                || false,
            )?;
            let published = eventglass::search::active::open_sealed(&restored, &manifest)?;
            assert_eq!(published.searcher.num_docs(), count as u64);
            variants.push(json!({"level": level, "compressed_bytes": std::fs::metadata(&compressed)?.len(), "elapsed_us": elapsed_us}));
        }
        reports.push(json!({"profile": profile, "records": count, "tar_bytes": std::fs::metadata(&raw)?.len(), "variants": variants}));
    }
    println!(
        "COMPRESSION_CANDIDATES {}",
        serde_json::to_string(&reports)?
    );
    Ok(())
}
