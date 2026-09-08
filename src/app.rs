use std::{
    fs::{File, OpenOptions},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
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
    _directory_lock: Arc<File>,
}

impl AppState {
    pub async fn open(config: Config) -> Result<Self> {
        config.validate()?;
        let config = Arc::new(config);
        let opening_config = Arc::clone(&config);
        let (lock, db) = tokio::task::spawn_blocking(move || -> Result<_> {
            if opening_config.s3_url.is_some() {
                // Until checkpoint restore exists, refuse to pretend an S3
                // configured missing database is a fresh installation.
                bail!("S3 initialization/restore is not implemented yet");
            }
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
            let lock = Arc::new(lock);
            let db = DbWorker::start(&opening_config.data_dir.join("meta.db"), Arc::clone(&lock))?;
            Ok((lock, db))
        })
        .await??;
        db.call(|connection| {
            let tx = connection.transaction()?;
            let exists: Option<i64> = tx.query_row("SELECT singleton FROM runtime_state", [], |r| r.get(0)).optional()?;
            if exists.is_none() {
                tx.execute("INSERT INTO runtime_state(singleton,installation_id,storage_generation,next_ingest_seq) VALUES(1,?1,?2,1)", rusqlite::params![uuid::Uuid::new_v4().to_string(),uuid::Uuid::new_v4().to_string()])?;
            }
            tx.commit()?;
            Ok(())
        }).await?;
        let token_key = db.call(crate::db::search::token_key).await?;
        Ok(Self {
            db,
            config,
            password_permit: Arc::new(Semaphore::new(1)),
            ingress_permit: Arc::new(Semaphore::new(1)),
            indexer: None,
            query_permit: Arc::new(Semaphore::new(1)),
            tokens: Arc::new(crate::search::tokens::TokenCodec::new(token_key)),
            _directory_lock: lock,
        })
    }

    /// Administration CLI only needs metadata. The serving process explicitly
    /// starts and reconciles the core before binding its HTTP listener.
    pub async fn start_core(mut self) -> Result<Self> {
        anyhow::ensure!(self.indexer.is_none(), "core is already running");
        self.indexer =
            Some(crate::indexer::Indexer::start(self.db.clone(), &self.config.data_dir).await?);
        Ok(self)
    }
}
