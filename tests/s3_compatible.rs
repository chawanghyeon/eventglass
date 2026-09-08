#![cfg(feature = "s3")]

use std::{env, path::Path};

use anyhow::{Context, Result, ensure};
use eventglass::{
    db,
    model::Boundary,
    search::active::ActiveShard,
    storage::{
        archive,
        checkpoint::{PinnedSnapshot, SnapshotLimits},
        manifest::{self, ShardStats},
        remote::{
            CheckpointDocument, InstallationDocument, LatestDocument, LocalCheckpoint,
            ObjectReference, RestoreLimits, ShardReference, ensure_installation, install_prepared,
            list_all, prepare_restore, publish, sha256,
        },
        s3::{AwsObjectStore, ObjectStore, S3Location},
    },
};
use rusqlite::params;
use serde_json::json;
use url::Url;

#[tokio::test]
#[ignore = "explicit compatible S3 gate: requires an isolated localhost MinIO server"]
async fn minio_checkpoint_and_object_contract() -> Result<()> {
    let endpoint = Url::parse(&required_env("EVENTGLASS_S3_TEST_ENDPOINT")?)?;
    ensure!(
        endpoint.scheme() == "http" && endpoint.host_str() == Some("127.0.0.1"),
        "compatible S3 test endpoint must be loopback HTTP"
    );
    let bucket = required_env("EVENTGLASS_S3_TEST_BUCKET")?;
    let prefix = required_env("EVENTGLASS_S3_TEST_PREFIX")?;
    let store = AwsObjectStore::load(
        S3Location::parse(&format!("s3://{bucket}/{prefix}"))?,
        Some(&endpoint),
    )
    .await?;

    let installation_id = uuid::Uuid::new_v4().to_string();
    let installation = InstallationDocument {
        format_version: 1,
        installation_id: installation_id.clone(),
        created_at_us: 1,
    };
    assert!(
        ensure_installation(&store, &installation, false)
            .await
            .is_err()
    );
    assert_eq!(
        ensure_installation(&store, &installation, true).await?,
        installation
    );
    assert!(
        prepare_restore(
            &store,
            &installation_id,
            &tempfile::tempdir()?.path().join("no-checkpoint"),
            RestoreLimits::default(),
            || false,
        )
        .await
        .is_err(),
        "an existing installation without a checkpoint must not become a fresh restore"
    );
    assert!(
        store
            .put_if_absent("installation.json", serde_json::to_vec(&installation)?)
            .await
            .is_err(),
        "conditional create must reject an existing key"
    );

    for ordinal in 0..1_005u32 {
        store
            .put_bytes(
                &format!("pagination/{ordinal:04}.json"),
                format!("{{\"ordinal\":{ordinal}}}").into_bytes(),
            )
            .await?;
    }
    let objects = list_all(&store).await?;
    ensure!(objects.len() == 1_006, "S3 pagination lost objects");

    store.put_bytes("bounded.bin", vec![7; 32]).await?;
    assert!(store.get_small("bounded.bin", 31).await.is_err());
    ensure!(store.get_small("bounded.bin", 32).await? == vec![7; 32]);

    let root = tempfile::tempdir()?;
    let multipart_path = root.path().join("multipart.bin");
    let multipart_bytes = (0..17 * 1024 * 1024)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    std::fs::write(&multipart_path, &multipart_bytes)?;
    let multipart_sha = sha256(&multipart_bytes);
    store
        .put_file("multipart.bin", &multipart_path, &multipart_sha)
        .await?;
    let multipart_download = root.path().join("multipart-download.bin");
    let downloaded = store
        .download(
            "multipart.bin",
            &multipart_download,
            multipart_bytes.len() as u64,
        )
        .await?;
    ensure!(downloaded.size == multipart_bytes.len() as u64);
    ensure!(std::fs::read(&multipart_download)? == multipart_bytes);
    assert!(
        store
            .put_file("multipart-aborted.bin", &multipart_path, &"0".repeat(64))
            .await
            .is_err()
    );
    assert!(
        store
            .get_small("multipart-aborted.bin", multipart_bytes.len() as u64)
            .await
            .is_err(),
        "failed multipart upload must not publish an object"
    );

    let candidate = restorable_candidate(root.path(), &installation_id, 1)?;
    let latest = publish(&store, &candidate).await?;
    ensure!(latest.sequence == 1);

    let cancelled_destination = root.path().join("cancelled-data").join(".restore");
    assert!(
        prepare_restore(
            &store,
            &installation_id,
            &cancelled_destination,
            RestoreLimits::default(),
            || true,
        )
        .await
        .is_err()
    );
    ensure!(!cancelled_destination.exists());

    let mut newer = candidate.document.clone();
    newer.checkpoint_id = uuid::Uuid::new_v4().to_string();
    newer.sequence = 2;
    newer.created_at_us = 2;
    newer.snapshot.key = format!("snapshots/{}.db", newer.checkpoint_id);
    store
        .put_bytes(&newer.snapshot.key, b"corrupt snapshot".to_vec())
        .await?;
    let newer_bytes = serde_json::to_vec(&newer)?;
    store
        .put_if_absent(
            &format!("checkpoints/{}.json", newer.checkpoint_id),
            newer_bytes.clone(),
        )
        .await?;
    let mut missing = candidate.document.clone();
    missing.checkpoint_id = uuid::Uuid::new_v4().to_string();
    missing.sequence = 3;
    missing.created_at_us = 3;
    missing.snapshot.key = format!("snapshots/{}.db", missing.checkpoint_id);
    let missing_bytes = serde_json::to_vec(&missing)?;
    store
        .put_if_absent(
            &format!("checkpoints/{}.json", missing.checkpoint_id),
            missing_bytes.clone(),
        )
        .await?;
    store
        .put_bytes(
            "latest.json",
            serde_json::to_vec(&LatestDocument {
                format_version: 1,
                installation_id: installation_id.clone(),
                checkpoint_id: missing.checkpoint_id.clone(),
                sequence: missing.sequence,
                checkpoint_sha256: sha256(&missing_bytes),
            })?,
        )
        .await?;
    store
        .put_bytes("shards/unreferenced.tar.gz", b"unreferenced".to_vec())
        .await?;

    let data_dir = root.path().join("lost-local-data");
    std::fs::create_dir(&data_dir)?;
    std::fs::write(data_dir.join(".eventglass-s3-test-data"), b"owned by test")?;
    std::fs::write(data_dir.join("destroyed-with-local-disk"), b"local only")?;
    reset_test_data_dir(root.path(), &data_dir)?;
    let destination = data_dir.join(".restore");
    let restored = prepare_restore(
        &store,
        &installation_id,
        &destination,
        RestoreLimits::default(),
        || false,
    )
    .await?;
    ensure!(restored.document.checkpoint_id == candidate.document.checkpoint_id);
    ensure!(restored.document.sequence == 1);
    ensure!(
        !destination.join("shards").join("unreferenced").exists(),
        "unreferenced archive was mixed into recovery"
    );

    install_prepared(&data_dir, restored)?;
    let database = db::inspect(&data_dir.join("meta.db"))?;
    let users: i64 = database.query_row("SELECT count(*) FROM users", [], |row| row.get(0))?;
    let sessions: i64 =
        database.query_row("SELECT count(*) FROM sessions", [], |row| row.get(0))?;
    let hmac_keys: i64 = database.query_row(
        "SELECT count(*) FROM settings WHERE key='token_hmac_v1'",
        [],
        |row| row.get(0),
    )?;
    ensure!(users == 1 && sessions == 0 && hmac_keys == 0);
    ensure!(
        data_dir
            .join("shards")
            .join(&candidate.document.shards[0].id)
            .is_dir()
    );

    store.put_bytes("latest.json", b"not json".to_vec()).await?;
    let fallback = root.path().join("fallback-data").join(".restore");
    let restored = prepare_restore(
        &store,
        &installation_id,
        &fallback,
        RestoreLimits::default(),
        || false,
    )
    .await?;
    ensure!(
        restored.document.sequence == 1,
        "corrupt latest did not fall back"
    );

    let wrong = AwsObjectStore::load(
        S3Location::parse(&format!("s3://{bucket}/{prefix}-wrong"))?,
        Some(&endpoint),
    )
    .await?;
    wrong
        .put_bytes("foreign-object", b"occupied".to_vec())
        .await?;
    assert!(
        ensure_installation(&wrong, &installation, false)
            .await
            .is_err()
    );

    println!(
        "{}",
        json!({
            "result": "pass",
            "server": "MinIO RELEASE.2025-09-07T16-13-09Z",
            "listed_objects_before_checkpoint": objects.len(),
            "multipart_bytes": multipart_bytes.len(),
            "checkpoint_sequence": 1,
            "fallback_from_missing_sequence": 3,
            "fallback_past_corrupt_sequence": 2,
            "full_loss_restore": true,
            "sessions_invalidated": true,
            "unreferenced_archive_ignored": true,
        })
    );
    Ok(())
}

fn required_env(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("{name} is required"))
}

fn reset_test_data_dir(root: &Path, data_dir: &Path) -> Result<()> {
    ensure!(
        data_dir.parent() == Some(root),
        "test data directory escaped its root"
    );
    ensure!(
        data_dir.join(".eventglass-s3-test-data").is_file(),
        "refusing to remove an unmarked data directory"
    );
    std::fs::remove_dir_all(data_dir)?;
    std::fs::create_dir(data_dir)?;
    Ok(())
}

fn restorable_candidate(
    root: &Path,
    installation_id: &str,
    sequence: u64,
) -> Result<LocalCheckpoint> {
    let shard_id = uuid::Uuid::new_v4().to_string();
    let shard_source = root.join("sealed");
    std::fs::create_dir(&shard_source)?;
    let mut active = ActiveShard::create(
        &shard_source,
        installation_id,
        &shard_id,
        Boundary::default(),
    )?;
    active.publish(Boundary::default())?;
    active.seal(
        &shard_source,
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
    let archive_path = root.join("shard.tar.gz");
    let archive = archive::create(
        &shard_source,
        &archive_path,
        installation_id,
        &shard_id,
        || false,
    )?;

    let database_path = root.join("source.db");
    let database = db::open(&database_path)?;
    let generation = uuid::Uuid::new_v4().to_string();
    database.execute(
        "INSERT INTO runtime_state(singleton,installation_id,storage_generation,next_ingest_seq)
         VALUES(1,?1,?2,1)",
        params![installation_id, generation],
    )?;
    database.execute(
        "INSERT INTO users(id,email,password_hash,role,is_active,created_at_us,updated_at_us)
         VALUES(1,'restore@example.test','hash','admin',1,1,1)",
        [],
    )?;
    database.execute(
        "INSERT INTO sessions(token_hash,user_id,expires_at_us,created_at_us,last_seen_at_us)
         VALUES('old-session',1,100,1,1)",
        [],
    )?;
    database.execute(
        "INSERT INTO settings(key,value_json,updated_at_us)
         VALUES('token_hmac_v1','[1,2,3]',1)",
        [],
    )?;
    let verified = manifest::verify(&shard_source, installation_id, &shard_id)?;
    database.execute(
        "INSERT INTO shards(id,schema_version,format_version,tokenizer_version,state,
            last_applied_inbox_id,record_count,size_bytes,created_at_us,sealed_at_us)
         VALUES(?1,1,?2,1,'local',0,0,?3,1,1)",
        params![
            shard_id,
            db::shards::FORMAT_VERSION,
            i64::try_from(manifest::local_size(&shard_source, &verified)?)?
        ],
    )?;
    drop(database);

    let checkpoint_id = uuid::Uuid::new_v4().to_string();
    let snapshot_path = root.join("snapshot.db");
    let snapshot = PinnedSnapshot::open(&database_path)?.backup_to(
        &snapshot_path,
        SnapshotLimits::default(),
        || false,
    )?;
    Ok(LocalCheckpoint {
        document: CheckpointDocument {
            format_version: 1,
            checkpoint_id: checkpoint_id.clone(),
            sequence,
            created_at_us: i64::try_from(sequence)?,
            cut: snapshot.cut,
            snapshot: ObjectReference {
                key: format!("snapshots/{checkpoint_id}.db"),
                size: snapshot.size,
                sha256: snapshot.sha256,
            },
            shards: vec![ShardReference {
                id: shard_id.clone(),
                object: ObjectReference {
                    key: format!("shards/{shard_id}.tar.gz"),
                    size: archive.size,
                    sha256: archive.sha256,
                },
            }],
        },
        snapshot_path,
        shard_archives: vec![(shard_id, archive_path)],
    })
}
