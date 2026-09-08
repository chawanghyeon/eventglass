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
