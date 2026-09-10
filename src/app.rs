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
    pub live_permit: Arc<Semaphore>,
    pub alerts: Option<crate::alerts::AlertCoordinator>,
    pub replay_maintenance: Option<crate::storage::replay_maintenance::ReplayMaintenance>,
    pub ingest_stats: Arc<crate::operations::IngestStats>,
    pub cold: Option<crate::storage::cold::ColdStorage>,
    pub tokens: Arc<crate::search::tokens::TokenCodec>,
    pub disk_budget: crate::storage::budget::DiskBudget,
    #[cfg(test)]
    reject_alert_start: bool,
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
        let replay_root = config.data_dir.clone();
        db.call(move |connection| {
            crate::db::replays::expire_at_startup(connection, &replay_root, crate::model::now_us()?)
        })
        .await?;
        let token_key = db.call(crate::db::search::token_key).await?;
        let disk_budget = crate::storage::budget::DiskBudget::new(&config.data_dir);
        Ok(Self {
            db,
            config,
            password_permit: Arc::new(Semaphore::new(1)),
            ingress_permit: Arc::new(Semaphore::new(1)),
            indexer: None,
            query_permit: Arc::new(Semaphore::new(1)),
            live_permit: Arc::new(Semaphore::new(32)),
            alerts: None,
            cold: None,
            replay_maintenance: None,
            ingest_stats: Arc::new(crate::operations::IngestStats::default()),
            tokens: Arc::new(crate::search::tokens::TokenCodec::new(token_key)),
            disk_budget,
            #[cfg(test)]
            reject_alert_start: false,
            _directory_lock: lock,
        })
    }

    /// Administration CLI only needs metadata. The serving process explicitly
    /// starts and reconciles the core before binding its HTTP listener.
    pub async fn start_core(mut self) -> Result<Self> {
        anyhow::ensure!(self.indexer.is_none(), "core is already running");
        #[cfg(feature = "s3")]
        let remote = build_remote_store(&self.config).await;
        #[cfg(feature = "s3")]
        let backup = remote.as_ref().map(|store| {
            crate::storage::backup::BackupCoordinator::new(
                self.db.clone(),
                &self.config.data_dir,
                Arc::clone(store),
                self.disk_budget.clone(),
            )
        });
        #[cfg(not(feature = "s3"))]
        let backup = None;
        let indexer = crate::indexer::Indexer::start_with_backup(
            self.db.clone(),
            &self.config.data_dir,
            backup.clone(),
        )
        .await?;
        #[cfg(feature = "s3")]
        if let Some(store) = remote {
            let installation = self
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
            self.cold = Some(crate::storage::cold::ColdStorage::new(
                self.db.clone(),
                &self.config.data_dir,
                installation,
                store,
                indexer.registry(),
                self.disk_budget.clone(),
            ));
        }
        #[cfg(test)]
        let alerts = if self.reject_alert_start {
            Err(anyhow::anyhow!("injected alert startup failure"))
        } else {
            crate::alerts::AlertCoordinator::start(
                self.db.clone(),
                indexer.clone(),
                self.query_permit.clone(),
                self.cold.clone(),
            )
        };
        #[cfg(not(test))]
        let alerts = crate::alerts::AlertCoordinator::start(
            self.db.clone(),
            indexer.clone(),
            self.query_permit.clone(),
            self.cold.clone(),
        );
        self.alerts = Some(alerts?);
        self.replay_maintenance = Some(
            crate::storage::replay_maintenance::ReplayMaintenance::start(
                self.db.clone(),
                self.config.data_dir.clone(),
                self.ingress_permit.clone(),
                self.query_permit.clone(),
                backup,
            ),
        );
        self.indexer = Some(indexer);
        Ok(self)
    }
}

#[cfg(feature = "s3")]
async fn build_remote_store(config: &Config) -> Option<Arc<dyn crate::storage::s3::ObjectStore>> {
    let result: Result<Option<Arc<dyn crate::storage::s3::ObjectStore>>> = async {
        let Some(value) = &config.s3_url else {
            return Ok(None);
        };
        let location = crate::storage::s3::S3Location::parse(value)?;
        let store: Arc<dyn crate::storage::s3::ObjectStore> = Arc::new(
            crate::storage::s3::AwsObjectStore::load(location, config.s3_endpoint.as_ref()).await?,
        );
        // Keep the client through an outage. Each publication verifies the namespace
        // before writing, so a transient startup failure cannot disable backups forever.
        Ok(Some(store))
    }
    .await;
    match result {
        Ok(coordinator) => coordinator,
        Err(error) => {
            tracing::warn!(reason = %error, "S3 storage is unavailable; local service continues");
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

#[cfg(test)]
mod tests {
    use super::*;

    fn config(data_dir: std::path::PathBuf) -> Config {
        Config {
            addr: "127.0.0.1:0".parse().unwrap(),
            data_dir,
            base_url: url::Url::parse("http://127.0.0.1:8080").unwrap(),
            s3_url: None,
            s3_endpoint: None,
            s3_initialize: false,
        }
    }

    #[tokio::test]
    async fn open_initializes_runtime_and_start_core_installs_every_coordinator() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let app = AppState::open(config(directory.path().to_owned()))
            .await?
            .start_core()
            .await?;
        assert!(app.indexer.is_some());
        assert!(app.alerts.is_some());
        assert!(app.replay_maintenance.is_some());

        app.replay_maintenance.as_ref().unwrap().shutdown().await?;
        app.alerts.as_ref().unwrap().shutdown().await?;
        app.indexer.as_ref().unwrap().shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn alert_start_failure_prevents_a_partially_started_core() {
        let directory = tempfile::tempdir().expect("invalid webhook policy directory");
        let mut app = AppState::open(config(directory.path().to_owned()))
            .await
            .expect("open application before policy validation");
        app.reject_alert_start = true;
        assert!(app.start_core().await.is_err());
    }

    #[cfg(not(feature = "s3"))]
    #[tokio::test]
    async fn non_s3_binary_rejects_remote_storage_configuration() {
        let directory = tempfile::tempdir().unwrap();
        let mut value = config(directory.path().to_owned());
        value.s3_url = Some("s3://bucket/prefix".into());
        let error = AppState::open(value).await.err().unwrap();
        assert!(error.to_string().contains("without S3 support"));
    }
}
