use std::{
    fs::{File, OpenOptions},
    sync::Arc,
};

use anyhow::{Context, Result};
use fs4::fs_std::FileExt;
use rusqlite::OptionalExtension;
use tokio::sync::Semaphore;

use crate::{config::Config, db::worker::DbWorker};

#[derive(Clone)]
pub struct AppState {
    pub db: DbWorker,
    pub config: Arc<Config>,
    pub password_permit: Arc<Semaphore>,
    /// Conservative request reservation until streaming normalization is measured.
    pub ingress_permit: Arc<Semaphore>,
    pub indexer: Option<crate::indexer::Indexer>,
    pub query_permit: Arc<Semaphore>,
    pub tokens: Arc<crate::search::tokens::TokenCodec>,
    pub disk_budget: crate::storage::budget::DiskBudget,
    _directory_lock: Arc<File>,
}

impl AppState {
    pub async fn open(config: Config) -> Result<Self> {
        config.validate()?;
        let config = Arc::new(config);
        let opening_config = Arc::clone(&config);
        let lock = tokio::task::spawn_blocking(move || -> Result<_> {
            std::fs::create_dir_all(&opening_config.data_dir)?;
            let lock = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(opening_config.data_dir.join(".lock"))?;
            anyhow::ensure!(
                lock.try_lock_exclusive()
                    .context("acquire data directory lock")?,
                "data directory is already locked"
            );
            Ok(Arc::new(lock))
        })
        .await??;
        #[cfg(not(feature = "s3"))]
        if config.s3_url.is_some() {
            anyhow::bail!("this eventglass binary was built without S3 support");
        }
        #[cfg(feature = "s3")]
        let initial_installation = prepare_remote_storage(&config)
            .await?
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        #[cfg(not(feature = "s3"))]
        let initial_installation = uuid::Uuid::new_v4().to_string();
        let database_path = config.data_dir.join("meta.db");
        let database_lock = Arc::clone(&lock);
        let db =
            tokio::task::spawn_blocking(move || DbWorker::start(&database_path, database_lock))
                .await??;
        db.call(move |connection| {
            let tx = connection.transaction()?;
            let exists: Option<i64> = tx.query_row("SELECT singleton FROM runtime_state", [], |r| r.get(0)).optional()?;
            if exists.is_none() {
                tx.execute("INSERT INTO runtime_state(singleton,installation_id,storage_generation,next_ingest_seq) VALUES(1,?1,?2,1)", rusqlite::params![initial_installation,uuid::Uuid::new_v4().to_string()])?;
            }
            tx.commit()?;
            Ok(())
        }).await?;
        let token_key = db.call(crate::db::search::token_key).await?;
        let disk_budget = crate::storage::budget::DiskBudget::new(&config.data_dir);
        Ok(Self {
            db,
            config,
            password_permit: Arc::new(Semaphore::new(1)),
            ingress_permit: Arc::new(Semaphore::new(1)),
            indexer: None,
            query_permit: Arc::new(Semaphore::new(1)),
            tokens: Arc::new(crate::search::tokens::TokenCodec::new(token_key)),
            disk_budget,
            _directory_lock: lock,
        })
    }

    /// Administration CLI only needs metadata. The serving process explicitly
    /// starts and reconciles the core before binding its HTTP listener.
    pub async fn start_core(mut self) -> Result<Self> {
        anyhow::ensure!(self.indexer.is_none(), "core is already running");
        #[cfg(feature = "s3")]
        let backup = build_backup_coordinator(&self.config, &self.db).await;
        #[cfg(not(feature = "s3"))]
        let backup = None;
        self.indexer = Some(
            crate::indexer::Indexer::start_with_backup(
                self.db.clone(),
                &self.config.data_dir,
                backup,
            )
            .await?,
        );
        Ok(self)
    }
}

#[cfg(feature = "s3")]
async fn build_backup_coordinator(
    config: &Config,
    db: &DbWorker,
) -> Option<crate::storage::backup::BackupCoordinator> {
    let result: Result<Option<crate::storage::backup::BackupCoordinator>> = async {
        let Some(value) = &config.s3_url else {
            return Ok(None);
        };
        let location = crate::storage::s3::S3Location::parse(value)?;
        let store = Arc::new(
            crate::storage::s3::AwsObjectStore::load(location, config.s3_endpoint.as_ref()).await?,
        );
        let local_installation = db
            .call(|connection| {
                connection
                    .query_row(
                        "SELECT installation_id FROM runtime_state WHERE singleton=1",
                        [],
                        |row| row.get::<_, String>(0),
                    )
                    .map_err(Into::into)
            })
            .await?;
        let remote = crate::storage::remote::read_installation(store.as_ref())
            .await?
            .context("S3 installation identity is missing")?;
        anyhow::ensure!(
            remote.installation_id == local_installation,
            "S3 installation identity differs from the local database"
        );
        Ok(Some(crate::storage::backup::BackupCoordinator::new(
            db.clone(),
            &config.data_dir,
            store,
        )))
    }
    .await;
    match result {
        Ok(coordinator) => coordinator,
        Err(error) => {
            tracing::warn!(reason = %error, "S3 backup is unavailable; local service continues");
            None
        }
    }
}

#[cfg(feature = "s3")]
async fn prepare_remote_storage(config: &Config) -> Result<Option<String>> {
    let Some(value) = &config.s3_url else {
        return Ok(None);
    };
    let database = config.data_dir.join("meta.db");
    match std::fs::symlink_metadata(&database) {
        Ok(metadata) => {
            anyhow::ensure!(
                metadata.file_type().is_file(),
                "meta.db is not a regular file"
            );
            return Ok(None);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let location = crate::storage::s3::S3Location::parse(value)?;
    let store =
        crate::storage::s3::AwsObjectStore::load(location, config.s3_endpoint.as_ref()).await?;
    match crate::storage::remote::read_installation(&store).await? {
        None => {
            anyhow::ensure!(
                config.s3_initialize,
                "empty S3 prefix requires EVENTGLASS_S3_INITIALIZE=true"
            );
            let id = uuid::Uuid::new_v4().to_string();
            let document = crate::storage::remote::InstallationDocument {
                format_version: 1,
                installation_id: id.clone(),
                created_at_us: crate::model::now_us()?,
            };
            crate::storage::remote::ensure_installation(&store, &document, true).await?;
            Ok(Some(id))
        }
        Some(installation) => {
            let destination = config
                .data_dir
                .join(format!(".restore-{}", uuid::Uuid::new_v4()));
            let prepared = crate::storage::remote::prepare_restore(
                &store,
                &installation.installation_id,
                &destination,
                crate::storage::remote::RestoreLimits::default(),
                || false,
            )
            .await?;
            crate::storage::remote::install_prepared(&config.data_dir, prepared)?;
            Ok(None)
        }
    }
}
