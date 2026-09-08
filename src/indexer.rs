//! One sequential native commit → SQLite finalize → reader publication coordinator.

use std::{
    path::Path,
    sync::{Arc, Mutex, RwLock},
};

use anyhow::{Context, Result, ensure};
use tokio::sync::{Notify, watch};

use crate::{
    config::Limits,
    db::{indexer as metadata, shards, worker::DbWorker},
    search::active::{ActiveShard, Published},
};

#[derive(Clone)]
pub struct Indexer {
    control: Arc<Control>,
    view: Arc<RwLock<View>>,
}

struct Control {
    stop: watch::Sender<bool>,
    wake: Arc<Notify>,
    join: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Drop for Control {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

struct View {
    published: Published,
    failed: bool,
}

impl Indexer {
    /// Startup completes recovery before exposing a ready Indexer handle.
    pub async fn start(db: DbWorker, data_dir: &Path) -> Result<Self> {
        let mut catalog = db.call(|db| shards::startup(db)).await?;
        let directory = data_dir.to_owned();
        if let Some(active_id) = catalog.active_id.clone() {
            let path = directory.join("shards").join(&active_id);
            if path.join(crate::storage::manifest::NAME).exists() {
                let installation = catalog.installation_id.clone();
                let verified = tokio::task::spawn_blocking(move || -> Result<_> {
                    let manifest =
                        crate::storage::manifest::verify(&path, &installation, &active_id)?;
                    let size = manifest.files.iter().try_fold(
                        std::fs::metadata(path.join(crate::storage::manifest::NAME))?.len(),
                        |sum, file| {
                            sum.checked_add(file.size)
                                .context("sealed shard size overflow")
                        },
                    )?;
                    Ok((manifest, size))
                })
                .await??;
                db.call(move |db| shards::finalize_seal(db, &verified.0, verified.1))
                    .await?;
                catalog = db.call(|db| shards::startup(db)).await?;
            }
        }
        let installation = catalog.installation_id;
        let applied = catalog.applied;
        let existing = catalog.active_id;
        let is_new = existing.is_none();
        let shard_id = existing.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let native_id = shard_id.clone();
        let mut active = tokio::task::spawn_blocking(move || -> Result<_> {
            let root = directory.join("shards");
            let path = root.join(&native_id);
            if is_new {
                std::fs::create_dir_all(&root)?;
                std::fs::File::open(&directory)?.sync_all()?;
                // Never reuse an orphan candidate: a new UUID makes adoption explicit.
                std::fs::create_dir(&path)?;
                let active = ActiveShard::create(&path, &installation, &native_id, applied)?;
                std::fs::File::open(&root)?.sync_all()?;
                Ok(active)
            } else {
                ActiveShard::open(&path, &installation, &native_id)
            }
        })
        .await??;
        if is_new {
            let id = shard_id.clone();
            let created = crate::model::now_us()?;
            db.call(move |db| shards::adopt_initial(db, &id, applied, created))
                .await?;
        }
        let committed = active.committed().boundary;
        if committed != applied {
            ensure!(
                committed.inbox_id > applied.inbox_id && committed.ingest_seq > applied.ingest_seq,
                "native C is behind or inconsistent with SQLite A"
            );
            let batch = db
                .call(move |db| metadata::recover_batch(db, committed, &Limits::default()))
                .await?;
            let id = shard_id.clone();
            let finalized = db
                .call(move |db| metadata::finalize(db, &id, batch))
                .await?;
            ensure!(
                finalized == committed,
                "recovery finalized an unexpected boundary"
            );
        }
        let (active, published) = tokio::task::spawn_blocking(move || -> Result<_> {
            let published = active.publish(committed)?;
            Ok((active, published))
        })
        .await??;
        let view = Arc::new(RwLock::new(View {
            published,
            failed: false,
        }));
        let (stop, receiver) = watch::channel(false);
        let wake = Arc::new(Notify::new());
        let worker_view = view.clone();
        let worker_wake = wake.clone();
        let join = tokio::spawn(async move {
            if let Err(error) = run(
                db,
                active,
                shard_id,
                worker_view.clone(),
                worker_wake,
                receiver,
            )
            .await
            {
                if let Ok(mut view) = worker_view.write() {
                    view.failed = true;
                }
                // No raw payload, DB errors, keys or filesystem paths in public logs.
                tracing::error!(reason = %error, "indexer stopped; startup reconciliation required");
            }
        });
        Ok(Self {
            control: Arc::new(Control {
                stop,
                wake,
                join: Mutex::new(Some(join)),
            }),
            view,
        })
    }

    pub fn ready(&self) -> bool {
        self.view.read().map(|view| !view.failed).unwrap_or(false)
    }

    pub fn snapshot(&self) -> Result<Published> {
        let view = self
            .view
            .read()
            .map_err(|_| anyhow::anyhow!("index publication lock poisoned"))?;
        ensure!(!view.failed, "indexer unavailable");
        Ok(view.published.clone())
    }

    pub fn wake(&self) {
        self.control.wake.notify_one();
    }

    pub async fn shutdown(&self) -> Result<()> {
        let _ = self.control.stop.send(true);
        let join = self
            .control
            .join
            .lock()
            .map_err(|_| anyhow::anyhow!("indexer control poisoned"))?
            .take();
        if let Some(join) = join {
            join.await.context("indexer task panicked")?;
        }
        ensure!(self.ready(), "indexer stopped with a failure");
        Ok(())
    }
}

async fn run(
    db: DbWorker,
    mut active: ActiveShard,
    shard_id: String,
    view: Arc<RwLock<View>>,
    wake: Arc<Notify>,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    loop {
        if *stop.borrow() {
            return Ok(());
        }
        let batch = db
            .call(|db| metadata::prepare(db, &Limits::default()))
            .await
            .context("prepare_batch_failed")?;
        let Some(batch) = batch else {
            tokio::select! {
                _=wake.notified()=>{},
                _=tokio::time::sleep(std::time::Duration::from_millis(250))=>{},
                _=stop.changed()=>{},
            }
            continue;
        };
        let boundary = batch.boundary;
        crash_point("before_native_commit");
        let (returned, batch) = tokio::task::spawn_blocking(move || -> Result<_> {
            active.commit(&batch.records, boundary)?;
            Ok((active, batch))
        })
        .await
        .context("native_commit_worker_failed")?
        .context("native_commit_failed")?;
        active = returned;
        crash_point("after_native_commit");
        // Stop/cancellation is checked only between complete commit/finalize/publish batches.
        let id = shard_id.clone();
        let finalized = db
            .call(move |db| metadata::finalize(db, &id, batch))
            .await
            .context("sqlite_finalize_failed")?;
        ensure!(
            finalized == boundary,
            "finalized boundary differs from committed batch"
        );
        crash_point("after_sqlite_finalize");
        let (returned, published) = tokio::task::spawn_blocking(move || -> Result<_> {
            let published = active.publish(finalized)?;
            Ok((active, published))
        })
        .await
        .context("reader_reload_worker_failed")?
        .context("reader_reload_failed")?;
        active = returned;
        crash_point("after_reader_reload");
        view.write()
            .map_err(|_| anyhow::anyhow!("index publication lock poisoned"))?
            .published = published;
    }
}

#[inline]
fn crash_point(name: &str) {
    #[cfg(feature = "failpoints")]
    if std::env::var("EVENTGLASS_FAILPOINT").as_deref() == Ok(name) {
        // Exit bypasses destructors, like a process crash, without OS crash dialogs.
        std::process::exit(86);
    }
    #[cfg(not(feature = "failpoints"))]
    let _ = name;
}
