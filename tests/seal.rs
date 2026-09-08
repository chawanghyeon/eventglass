use anyhow::Result;
use eventglass::{
    model::Boundary,
    search::active::ActiveShard,
    sentry::{self, ProjectContext},
    storage::manifest::{self, ShardStats},
};
use serde_json::json;

#[test]
fn native_seal_preserves_boundary_ignores_unreferenced_files_and_detects_corruption() -> Result<()>
{
    let directory = tempfile::tempdir()?;
    let installation = uuid::Uuid::new_v4().to_string();
    let id = uuid::Uuid::new_v4().to_string();
    let mut active =
        ActiveShard::create(directory.path(), &installation, &id, Boundary::default())?;
    active.publish(Boundary::default())?;
    let time = 1788825600000001;
    let mut records = sentry::normalize_store(
        &serde_json::to_vec(
            &json!({"message":"sealed data","timestamp":"2026-09-08T00:00:00.000001Z"}),
        )?,
        &ProjectContext {
            project_id: 1,
            slug: "test".into(),
            public_key: "public".into(),
            scrub_keys: vec![],
        },
        uuid::Uuid::new_v4(),
        time,
        &Default::default(),
    )?
    .records;
    records[0].ingest_seq = 1;
    active.commit(
        &records,
        Boundary {
            inbox_id: 1,
            ingest_seq: 1,
        },
    )?;
    let pinned = active.publish(Boundary {
        inbox_id: 1,
        ingest_seq: 1,
    })?;
    // A duplicate-only acceptance still advances the sealed commit boundary.
    let cut = Boundary {
        inbox_id: 2,
        ingest_seq: 2,
    };
    active.commit(&[], cut)?;
    active.publish(cut)?;
    std::fs::write(directory.path().join("unreferenced.tmp"), b"do not archive")?;
    let stats = ShardStats {
        record_count: 1,
        min_timestamp_us: Some(time),
        max_timestamp_us: Some(time),
        min_received_at_us: Some(time),
        max_received_at_us: Some(time),
        min_ingest_seq: Some(1),
        max_ingest_seq: Some(1),
    };
    let sealed = active.seal(directory.path(), stats, time)?;
    assert_eq!(sealed.boundary, cut);
    assert!(!sealed.files.iter().any(|f| f.path.contains("unreferenced")));
    assert_eq!(
        manifest::verify(directory.path(), &installation, &id)?,
        sealed
    );
    assert_eq!(pinned.searcher.num_docs(), 1);
    assert!(ActiveShard::open(directory.path(), &installation, &id).is_err());
    assert!(manifest::verify(directory.path(), &uuid::Uuid::new_v4().to_string(), &id).is_err());
    let component = sealed
        .files
        .iter()
        .find(|f| f.path.ends_with(".store"))
        .unwrap();
    let path = directory.path().join(&component.path);
    let original = std::fs::read(&path)?;
    let mut damaged = original.clone();
    damaged[0] ^= 1;
    std::fs::write(&path, damaged)?;
    assert!(manifest::verify(directory.path(), &installation, &id).is_err());
    std::fs::write(&path, original)?;
    assert_eq!(
        manifest::verify(directory.path(), &installation, &id)?,
        sealed
    );
    Ok(())
}

#[test]
fn seal_rejects_unpublished_boundary_and_manifest_path_escape() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let installation = uuid::Uuid::new_v4().to_string();
    let id = uuid::Uuid::new_v4().to_string();
    let stats = ShardStats {
        record_count: 0,
        min_timestamp_us: None,
        max_timestamp_us: None,
        min_received_at_us: None,
        max_received_at_us: None,
        min_ingest_seq: None,
        max_ingest_seq: None,
    };
    let active = ActiveShard::create(directory.path(), &installation, &id, Boundary::default())?;
    assert!(active.seal(directory.path(), stats.clone(), 1).is_err());
    let mut active = ActiveShard::open(directory.path(), &installation, &id)?;
    active.publish(Boundary::default())?;
    let mut sealed = active.seal(directory.path(), stats, 1)?;
    sealed.files[0].path = "../meta.db".into();
    std::fs::write(
        directory.path().join(manifest::NAME),
        serde_json::to_vec(&sealed)?,
    )?;
    assert!(manifest::verify(directory.path(), &installation, &id).is_err());
    Ok(())
}
