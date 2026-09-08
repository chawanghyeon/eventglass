//! One sequential native commit → SQLite finalize → reader publication coordinator.

use std::{
    collections::HashSet,
    path::Path,
    sync::{Arc, Mutex, RwLock},
};

use anyhow::{Context, Result, ensure};
use tokio::sync::{Notify, broadcast, watch};

use crate::{
    config::Limits,
    db::{indexer as metadata, shards, worker::DbWorker},
    search::active::{ActiveShard, Published},
};

#[derive(Clone)]
pub struct Indexer {
    control: Arc<Control>,
    view: Arc<RwLock<View>>,
    registry: crate::storage::registry::Registry,
}

pub enum ReadPin {
    Active(Published),
    Sealed(crate::storage::registry::ShardPin),
}

impl ReadPin {
    pub fn published(&self) -> &Published {
        match self {
            Self::Active(shard) => shard,
            Self::Sealed(pin) => pin.published(),
        }
    }
}

struct Control {
    stop: watch::Sender<bool>,
    wake: Arc<Notify>,
    join: Mutex<Option<tokio::task::JoinHandle<()>>>,
    backup: Option<crate::storage::backup::BackupCoordinator>,
    updates: broadcast::Sender<i64>,
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

struct RunContext {
    db: DbWorker,
    view: Arc<RwLock<View>>,
    wake: Arc<Notify>,
    data_dir: std::path::PathBuf,
    installation: String,
    backup: Option<crate::storage::backup::BackupCoordinator>,
    updates: broadcast::Sender<i64>,
}

struct RotateContext<'a> {
    db: &'a DbWorker,
    data_dir: &'a Path,
    installation: &'a str,
    backup: Option<&'a crate::storage::backup::BackupCoordinator>,
}

impl Indexer {
    /// Startup completes recovery before exposing a ready Indexer handle.
    pub async fn start(db: DbWorker, data_dir: &Path) -> Result<Self> {
        Self::start_with_backup(db, data_dir, None).await
    }

    pub async fn start_with_backup(
        db: DbWorker,
        data_dir: &Path,
        backup: Option<crate::storage::backup::BackupCoordinator>,
    ) -> Result<Self> {
        let mut catalog = db.call(|db| shards::startup(db)).await?;
        let directory = data_dir.to_owned();
        if let Some(active_id) = catalog.active_id.clone() {
            let path = directory.join("shards").join(&active_id);
            if path.join(crate::storage::manifest::NAME).exists() {
                let installation = catalog.installation_id.clone();
                let verified = tokio::task::spawn_blocking(move || -> Result<_> {
                    let manifest =
                        crate::storage::manifest::verify(&path, &installation, &active_id)?;
                    let size = crate::storage::manifest::local_size(&path, &manifest)?;
                    Ok((manifest, size))
                })
                .await??;
                db.call(move |db| shards::finalize_seal(db, &verified.0, verified.1))
                    .await?;
                catalog = db.call(|db| shards::startup(db)).await?;
            }
        }
        let remote_only = db
            .call(|db| {
                let mut statement =
                    db.prepare("SELECT id FROM shards WHERE state='remote_only' ORDER BY id")?;
                Ok(statement
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?)
            })
            .await?;
        let recovery_root = data_dir.join("shards");
        let recovery_installation = catalog.installation_id.clone();
        let recovered = tokio::task::spawn_blocking(move || -> Result<Vec<(String, u64)>> {
            let mut recovered = Vec::new();
            for id in remote_only {
                let path = recovery_root.join(&id);
                if !path.exists() {
                    continue;
                }
                let manifest =
                    crate::storage::manifest::verify(&path, &recovery_installation, &id)?;
                let size = crate::storage::manifest::local_size(&path, &manifest)?;
                recovered.push((id, size));
            }
            Ok(recovered)
        })
        .await??;
        if !recovered.is_empty() {
            db.call(move |db| {
                let transaction = db.transaction()?;
                for (id, size) in recovered {
                    ensure!(
                        transaction.execute(
                            "UPDATE shards SET state='remote_verified',size_bytes=?1
                             WHERE id=?2 AND state='remote_only'
                               AND remote_archive_key IS NOT NULL
                               AND archive_sha256 IS NOT NULL
                               AND recovery_checkpoint_id IS NOT NULL",
                            rusqlite::params![i64::try_from(size)?, id],
                        )? == 1,
                        "remote-only shard changed during startup reconciliation"
                    );
                }
                transaction.commit()?;
                Ok(())
            })
            .await?;
        }
        let local_catalog = db.call(|db| shards::local_catalog(db)).await?;
        let catalog_root = directory.join("shards");
        let catalog_installation = catalog.installation_id.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            for row in local_catalog {
                let path = catalog_root.join(&row.id);
                let manifest =
                    crate::storage::manifest::verify(&path, &catalog_installation, &row.id)?;
                let size = crate::storage::manifest::local_size(&path, &manifest)?;
                ensure!(
                    row.matches(&manifest, size),
                    "sealed shard manifest differs from SQLite catalog"
                );
            }
            Ok(())
        })
        .await??;
        let registered = db.call(|db| shards::catalog_ids(db)).await?;
        let orphan_directory = directory.clone();
        let orphan_installation = catalog.installation_id.clone();
        let orphan_applied = catalog.applied;
        let may_adopt = catalog.active_id.is_none();
        let orphan = tokio::task::spawn_blocking(move || {
            reconcile_empty_orphans(
                &orphan_directory,
                &orphan_installation,
                orphan_applied,
                &registered,
                may_adopt,
            )
        })
        .await??;
        let registry =
            crate::storage::registry::Registry::new(&directory, catalog.installation_id.clone());
        let installation = catalog.installation_id;
        let worker_installation = installation.clone();
        let applied = catalog.applied;
        let needs_adoption = catalog.active_id.is_none();
        let existing = catalog.active_id.or(orphan);
        let needs_creation = existing.is_none();
        let shard_id = existing.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let native_id = shard_id.clone();
        let worker_directory = directory.clone();
        let mut active = tokio::task::spawn_blocking(move || -> Result<_> {
            let root = directory.join("shards");
            let path = root.join(&native_id);
            if needs_creation {
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
        if needs_adoption {
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
        let (updates, _) = broadcast::channel(64);
        let wake = Arc::new(Notify::new());
        let worker_view = view.clone();
        let worker_wake = wake.clone();
        let worker_backup = backup.clone();
        let worker_updates = updates.clone();
        let join = tokio::spawn(async move {
            if let Err(error) = run(
                RunContext {
                    db,
                    view: worker_view.clone(),
                    wake: worker_wake,
                    data_dir: worker_directory,
                    installation: worker_installation,
                    backup: worker_backup,
                    updates: worker_updates,
                },
                active,
                shard_id,
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
                backup,
                updates,
            }),
            view,
            registry,
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

    pub fn pin_shards(&self, ids: &[String]) -> Result<Vec<ReadPin>> {
        let active = self.snapshot()?;
        ids.iter()
            .map(|id| {
                if id == &active.shard_id {
                    Ok(ReadPin::Active(active.clone()))
                } else {
                    self.registry.pin_local(id).map(ReadPin::Sealed)
                }
            })
            .collect()
    }

    pub fn registry(&self) -> crate::storage::registry::Registry {
        self.registry.clone()
    }

    pub fn wake(&self) {
        self.control.wake.notify_one();
    }

    pub fn subscribe(&self) -> broadcast::Receiver<i64> {
        self.control.updates.subscribe()
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
        if let Some(backup) = &self.control.backup {
            backup.shutdown().await;
        }
        ensure!(self.ready(), "indexer stopped with a failure");
        Ok(())
    }
}

fn reconcile_empty_orphans(
    data_dir: &Path,
    installation: &str,
    applied: crate::model::Boundary,
    registered: &HashSet<String>,
    may_adopt: bool,
) -> Result<Option<String>> {
    let root = data_dir.join("shards");
    if !root.exists() {
        return Ok(None);
    }
    let mut candidates = Vec::new();
    for entry in std::fs::read_dir(&root)? {
        let entry = entry?;
        let id = entry
            .file_name()
            .into_string()
            .map_err(|_| anyhow::anyhow!("shard directory name is not UTF-8"))?;
        if registered.contains(&id) {
            continue;
        }
        ensure!(
            entry.file_type()?.is_dir(),
            "unregistered shard entry is not a directory"
        );
        crate::search::active::verify_empty_orphan(&entry.path(), installation, &id, applied)?;
        candidates.push((id, entry.path()));
    }
    candidates.sort_by(|left, right| left.0.cmp(&right.0));
    let adopted = may_adopt
        .then(|| candidates.first().map(|candidate| candidate.0.clone()))
        .flatten();
    for (id, path) in candidates {
        if adopted.as_deref() != Some(&id) {
            std::fs::remove_dir_all(path)?;
        }
    }
    std::fs::File::open(root)?.sync_all()?;
    Ok(adopted)
}

async fn run(
    context: RunContext,
    mut active: ActiveShard,
    mut shard_id: String,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let RunContext {
        db,
        view,
        wake,
        data_dir,
        installation,
        backup,
        updates,
    } = context;
    loop {
        if *stop.borrow() {
            return Ok(());
        }
        let now = crate::model::now_us()?;
        let active_id = shard_id.clone();
        if let Some(plan) = db
            .call(move |db| shards::rotation_plan(db, &active_id, None, now))
            .await?
        {
            let rotated = rotate(
                active,
                shard_id,
                plan,
                now,
                RotateContext {
                    db: &db,
                    data_dir: &data_dir,
                    installation: &installation,
                    backup: backup.as_ref(),
                },
            )
            .await?;
            active = rotated.0;
            shard_id = rotated.1;
            view.write()
                .map_err(|_| anyhow::anyhow!("index publication lock poisoned"))?
                .published = rotated.2;
            continue;
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
        let _ = updates.send(finalized.ingest_seq);
        let active_path = data_dir.join("shards").join(&shard_id);
        let measured = tokio::task::spawn_blocking(move || {
            crate::storage::manifest::active_size(&active_path)
        })
        .await??;
        let now = crate::model::now_us()?;
        let active_id = shard_id.clone();
        if let Some(plan) = db
            .call(move |db| shards::rotation_plan(db, &active_id, Some(measured), now))
            .await?
        {
            let rotated = rotate(
                active,
                shard_id,
                plan,
                now,
                RotateContext {
                    db: &db,
                    data_dir: &data_dir,
                    installation: &installation,
                    backup: backup.as_ref(),
                },
            )
            .await?;
            active = rotated.0;
            shard_id = rotated.1;
            view.write()
                .map_err(|_| anyhow::anyhow!("index publication lock poisoned"))?
                .published = rotated.2;
        }
    }
}

async fn rotate(
    active: ActiveShard,
    shard_id: String,
    stats: crate::storage::manifest::ShardStats,
    now_us: i64,
    context: RotateContext<'_>,
) -> Result<(ActiveShard, String, Published)> {
    let RotateContext {
        db,
        data_dir,
        installation,
        backup,
    } = context;
    let path = data_dir.join("shards").join(&shard_id);
    let (manifest, size) = tokio::task::spawn_blocking(move || -> Result<_> {
        let manifest = active.seal(&path, stats, now_us)?;
        let size = crate::storage::manifest::local_size(&path, &manifest)?;
        Ok((manifest, size))
    })
    .await??;
    crash_point("after_seal_manifest");
    let boundary = manifest.boundary;
    db.call(move |db| shards::finalize_seal(db, &manifest, size))
        .await?;
    crash_point("after_seal_catalog");
    let backup_job = if let Some(backup) = backup {
        match backup.begin_cut().await {
            Ok(job) => job,
            Err(error) => {
                tracing::warn!(reason = %error, "checkpoint cut failed; indexing continues");
                None
            }
        }
    } else {
        None
    };

    let next_id = uuid::Uuid::new_v4().to_string();
    let native_id = next_id.clone();
    let directory = data_dir.to_owned();
    let installation = installation.to_owned();
    let mut next = tokio::task::spawn_blocking(move || -> Result<_> {
        let root = directory.join("shards");
        let path = root.join(&native_id);
        std::fs::create_dir(&path)?;
        let active = ActiveShard::create(&path, &installation, &native_id, boundary)?;
        std::fs::File::open(&root)?.sync_all()?;
        Ok(active)
    })
    .await??;
    crash_point("after_new_active_create");
    let adopted_id = next_id.clone();
    db.call(move |db| shards::adopt_initial(db, &adopted_id, boundary, now_us))
        .await?;
    crash_point("after_new_active_adopt");
    let published = tokio::task::spawn_blocking(move || -> Result<_> {
        let published = next.publish(boundary)?;
        Ok((next, published))
    })
    .await??;
    if let Some(job) = backup_job {
        job.launch();
    }
    Ok((published.0, next_id, published.1))
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
