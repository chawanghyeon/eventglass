use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use anyhow::{Result, ensure};
use async_trait::async_trait;
use eventglass::{
    app::AppState,
    config::Config,
    model::Boundary,
    search::active::ActiveShard,
    storage::{
        archive,
        cold::ColdStorage,
        manifest::ShardStats,
        remote::sha256,
        s3::{ObjectMetadata, ObjectPage, ObjectStore},
    },
};

struct Store {
    bytes: Mutex<Vec<u8>>,
    downloads: AtomicUsize,
}

#[async_trait]
impl ObjectStore for Store {
    async fn list(&self, _continuation: Option<String>) -> Result<ObjectPage> {
        unreachable!()
    }

    async fn get_small(&self, _relative: &str, _max_bytes: u64) -> Result<Vec<u8>> {
        unreachable!()
    }

    async fn put_if_absent(&self, _relative: &str, _bytes: Vec<u8>) -> Result<()> {
        unreachable!()
    }

    async fn put_bytes(&self, _relative: &str, _bytes: Vec<u8>) -> Result<()> {
        unreachable!()
    }

    async fn put_file(&self, _relative: &str, _path: &std::path::Path, _sha: &str) -> Result<()> {
        unreachable!()
    }

    async fn download(
        &self,
        relative: &str,
        destination: &std::path::Path,
        max_bytes: u64,
    ) -> Result<ObjectMetadata> {
        let bytes = self.bytes.lock().unwrap().clone();
        ensure!(bytes.len() as u64 <= max_bytes, "test object too large");
        self.downloads.fetch_add(1, Ordering::SeqCst);
        tokio::fs::write(destination, &bytes).await?;
        Ok(ObjectMetadata {
            key: relative.to_owned(),
            size: bytes.len() as u64,
            etag: None,
        })
    }
}

fn config(path: &std::path::Path) -> Config {
    Config {
        addr: "127.0.0.1:0".parse().unwrap(),
        data_dir: path.to_owned(),
        base_url: "http://localhost:8080".parse().unwrap(),
        s3_url: None,
        s3_endpoint: None,
        s3_initialize: false,
    }
}

#[tokio::test]
async fn cold_hydration_is_single_flight_verified_and_atomic() -> Result<()> {
    let root = tempfile::tempdir()?;
    let app = AppState::open(config(root.path())).await?;
    let installation = app
        .db
        .call(|db| {
            db.query_row(
                "SELECT installation_id FROM runtime_state WHERE singleton=1",
                [],
                |row| row.get::<_, String>(0),
            )
            .map_err(Into::into)
        })
        .await?;
    let shard_id = uuid::Uuid::new_v4().to_string();
    let source = root.path().join("source");
    std::fs::create_dir(&source)?;
    let mut active = ActiveShard::create(&source, &installation, &shard_id, Boundary::default())?;
    active.publish(Boundary::default())?;
    active.seal(
        &source,
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
    let archive_path = root.path().join("shard.tar.gz");
    let artifact = archive::create(&source, &archive_path, &installation, &shard_id, || false)?;
    let bytes = std::fs::read(&archive_path)?;
    assert_eq!(sha256(&bytes), artifact.sha256);
    let id = shard_id.clone();
    let hash = artifact.sha256.clone();
    app.db
        .call(move |db| {
            db.execute(
                "INSERT INTO shards(
                    id,schema_version,format_version,tokenizer_version,state,
                    remote_archive_key,archive_sha256,recovery_checkpoint_id,
                    created_at_us,sealed_at_us
                 ) VALUES(?1,1,'tantivy-0.26.1/format-7',1,'remote_only',
                          'shards/test.tar.gz',?2,'checkpoint',1,1)",
                rusqlite::params![id, hash],
            )?;
            Ok(())
        })
        .await?;
    let store = Arc::new(Store {
        bytes: Mutex::new(bytes.clone()),
        downloads: AtomicUsize::new(0),
    });
    let registry = eventglass::storage::registry::Registry::new(root.path(), installation.clone());
    let cold = ColdStorage::new(
        app.db.clone(),
        root.path(),
        installation,
        store.clone(),
        registry.clone(),
        app.disk_budget.clone(),
    );
    let ids = vec![shard_id.clone()];
    let (left, right) = tokio::join!(cold.ensure_local(&ids), cold.ensure_local(&ids));
    assert_eq!(left? + right?, 1);
    assert_eq!(store.downloads.load(Ordering::SeqCst), 1);
    assert!(root.path().join("shards").join(&shard_id).is_dir());
    let state = app
        .db
        .call(move |db| {
            db.query_row("SELECT state FROM shards WHERE id=?1", [shard_id], |row| {
                row.get::<_, String>(0)
            })
            .map_err(Into::into)
        })
        .await?;
    assert_eq!(state, "remote_verified");
    let pin = registry.pin_local(&ids[0])?;
    assert!(!cold.evict_remote_verified(&ids[0]).await?);
    drop(pin);
    assert!(cold.evict_remote_verified(&ids[0]).await?);
    assert!(!root.path().join("shards").join(&ids[0]).exists());
    assert_eq!(cold.ensure_local(&ids).await?, 1);
    assert_eq!(store.downloads.load(Ordering::SeqCst), 2);
    assert!(cold.evict_remote_verified(&ids[0]).await?);
    *store.bytes.lock().unwrap() = b"corrupt archive".to_vec();
    assert!(cold.ensure_local(&ids).await.is_err());
    assert!(!root.path().join("shards").join(&ids[0]).exists());
    let id = ids[0].clone();
    let state = app
        .db
        .call(move |db| {
            db.query_row("SELECT state FROM shards WHERE id=?1", [id], |row| {
                row.get::<_, String>(0)
            })
            .map_err(Into::into)
        })
        .await?;
    assert_eq!(state, "remote_only");
    *store.bytes.lock().unwrap() = bytes;
    assert_eq!(cold.ensure_local(&ids).await?, 1);
    let id = ids[0].clone();
    app.db
        .call(move |db| {
            ensure!(
                db.execute(
                    "UPDATE shards SET state='remote_only' WHERE id=?1 AND state='remote_verified'",
                    [id],
                )? == 1,
                "failed to arrange interrupted install"
            );
            Ok(())
        })
        .await?;
    let indexer = eventglass::indexer::Indexer::start(app.db.clone(), root.path()).await?;
    let id = ids[0].clone();
    let state = app
        .db
        .call(move |db| {
            db.query_row("SELECT state FROM shards WHERE id=?1", [id], |row| {
                row.get::<_, String>(0)
            })
            .map_err(Into::into)
        })
        .await?;
    assert_eq!(state, "remote_verified");
    indexer.shutdown().await?;
    Ok(())
}
