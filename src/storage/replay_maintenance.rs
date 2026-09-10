//! Small local retention batches. Existing ingest/query/checkpoint gates pin all live readers.
use crate::db::worker::DbWorker;
use anyhow::Result;
use serde::Serialize;
use std::{
    fs::ReadDir,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Semaphore, watch};

#[derive(Debug, Default, Clone, Serialize)]
pub struct MaintenanceStatus {
    pub state: &'static str,
    pub last_success_us: Option<i64>,
    pub deleted_files: u64,
    pub deleted_bytes: u64,
}
struct Control {
    stop: watch::Sender<bool>,
    join: Mutex<Option<tokio::task::JoinHandle<()>>>,
    status: Arc<Mutex<MaintenanceStatus>>,
}
impl Drop for Control {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        if let Ok(join) = self.join.get_mut()
            && let Some(join) = join.take()
        {
            join.abort();
        }
    }
}
#[derive(Clone)]
pub struct ReplayMaintenance {
    control: Arc<Control>,
}
impl ReplayMaintenance {
    pub fn start(
        db: DbWorker,
        root: PathBuf,
        ingress: Arc<Semaphore>,
        query: Arc<Semaphore>,
        backup: Option<super::backup::BackupCoordinator>,
    ) -> Self {
        Self::start_with_interval(db, root, ingress, query, backup, Duration::from_secs(60))
    }

    fn start_with_interval(
        db: DbWorker,
        root: PathBuf,
        ingress: Arc<Semaphore>,
        query: Arc<Semaphore>,
        backup: Option<super::backup::BackupCoordinator>,
        interval: Duration,
    ) -> Self {
        let (stop, mut stopped) = watch::channel(false);
        let status = Arc::new(Mutex::new(MaintenanceStatus {
            state: "waiting",
            ..Default::default()
        }));
        let progress = status.clone();
        let cursor = Arc::new(Mutex::new(None));
        let join = tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            tick.tick().await; // Startup already performed a complete sweep.
            loop {
                tokio::select! { biased;
                    _ = stopped.changed() => break,
                    _ = tick.tick() => {}
                }
                let result = sweep(
                    &db,
                    root.clone(),
                    cursor.clone(),
                    &ingress,
                    &query,
                    backup.as_ref(),
                )
                .await;
                let mut state = progress.lock().unwrap_or_else(|e| e.into_inner());
                match result {
                    Ok(Some((files, bytes, now))) => {
                        state.state = "ready";
                        state.last_success_us = Some(now);
                        state.deleted_files += files;
                        state.deleted_bytes += bytes;
                    }
                    Ok(None) => state.state = "busy",
                    Err(error) => {
                        state.state = "failed";
                        tracing::warn!(reason = %error, "Replay local retention batch failed; will retry");
                    }
                }
            }
        });
        Self {
            control: Arc::new(Control {
                stop,
                join: Mutex::new(Some(join)),
                status,
            }),
        }
    }
    pub fn status(&self) -> MaintenanceStatus {
        self.control
            .status
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    pub async fn shutdown(&self) -> Result<()> {
        let _ = self.control.stop.send(true);
        let join = self
            .control
            .join
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(join) = join {
            join.await?;
        }
        Ok(())
    }
}

async fn sweep(
    db: &DbWorker,
    root: PathBuf,
    cursor: Arc<Mutex<Option<ReadDir>>>,
    ingress: &Arc<Semaphore>,
    query: &Arc<Semaphore>,
    backup: Option<&super::backup::BackupCoordinator>,
) -> Result<Option<(u64, u64, i64)>> {
    // Nonblocking acquisition avoids queues or competing with foreground work.
    let Ok(writer) = ingress.clone().try_acquire_owned() else {
        return Ok(None);
    };
    let Ok(reader) = query.clone().try_acquire_owned() else {
        return Ok(None);
    };
    let checkpoint = match backup {
        Some(backup) => match backup.try_maintenance_permit() {
            Some(permit) => Some(permit),
            None => return Ok(None),
        },
        None => None,
    };
    db.call(move |db| {
        // The database job, not the cancellable async caller, owns pins through unlink/fsync.
        let _pins = (writer, reader, checkpoint);
        let now = crate::model::now_us()?;
        crate::db::replays::expire_batch(db, now)?;
        let mut cursor = cursor.lock().unwrap_or_else(|e| e.into_inner());
        let result = collect_orphans(db, &root, &mut cursor);
        // Reopen on failure; orphan discovery is retryable after any crash or unlink failure.
        if result.is_err() {
            *cursor = None;
        }
        result.map(|(files, bytes)| Some((files, bytes, now)))
    })
    .await
}

fn collect_orphans(
    db: &rusqlite::Connection,
    root: &std::path::Path,
    cursor: &mut Option<ReadDir>,
) -> Result<(u64, u64)> {
    let directory = root.join("replay-blobs");
    if cursor.is_none() {
        match std::fs::read_dir(&directory) {
            Ok(entries) => *cursor = Some(entries),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok((0, 0)),
            Err(error) => return Err(error.into()),
        }
    }
    let mut candidates = Vec::new();
    for _ in 0..256 {
        let Some(entry) = cursor.as_mut().and_then(Iterator::next) else {
            *cursor = None;
            break;
        };
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(hash) = name.to_str().and_then(|s| s.strip_suffix(".zlib")) else {
            continue;
        };
        if super::replay::key(hash).is_ok() {
            candidates.push((hash.to_owned(), entry.path()));
        }
    }
    if candidates.is_empty() {
        return Ok((0, 0));
    }
    // One scan, bounded output. No full in-memory reference set or per-file table scans.
    let placeholders = vec!["?"; candidates.len()].join(",");
    let mut statement = db.prepare(&format!(
        "SELECT DISTINCT blob_sha256 FROM replay_segments WHERE blob_sha256 IN ({placeholders})"
    ))?;
    let referenced = statement
        .query_map(
            rusqlite::params_from_iter(candidates.iter().map(|(hash, _)| hash)),
            |row| row.get::<_, String>(0),
        )?
        .collect::<std::result::Result<std::collections::HashSet<_>, _>>()?;
    let (mut files, mut bytes) = (0, 0);
    for (hash, path) in candidates {
        if referenced.contains(&hash) {
            continue;
        }
        let size = std::fs::symlink_metadata(&path)?.len();
        std::fs::remove_file(path)?;
        files += 1;
        bytes += size;
    }
    if files > 0 {
        std::fs::File::open(directory)?.sync_all()?;
    }
    Ok((files, bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn wait_for_state(maintenance: &ReplayMaintenance, expected: &str) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if maintenance.status().state == expected {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn coordinator_reports_busy_failure_recovery_and_drop_shutdown() -> Result<()> {
        let root = tempfile::tempdir()?;
        let app = crate::app::AppState::open(crate::config::Config {
            addr: "127.0.0.1:0".parse()?,
            data_dir: root.path().to_owned(),
            base_url: "http://localhost:8080".parse()?,
            s3_url: None,
            s3_endpoint: None,
            s3_initialize: false,
        })
        .await?;
        let pin = app.ingress_permit.clone().acquire_owned().await?;
        let maintenance = ReplayMaintenance::start_with_interval(
            app.db.clone(),
            root.path().to_owned(),
            app.ingress_permit.clone(),
            app.query_permit.clone(),
            None,
            Duration::from_millis(1),
        );
        wait_for_state(&maintenance, "busy").await?;
        drop(pin);
        wait_for_state(&maintenance, "ready").await?;
        assert!(maintenance.status().last_success_us.is_some());
        maintenance.shutdown().await?;

        std::fs::write(root.path().join("replay-blobs"), b"not a directory")?;
        let failing = ReplayMaintenance::start_with_interval(
            app.db.clone(),
            root.path().to_owned(),
            app.ingress_permit.clone(),
            app.query_permit.clone(),
            None,
            Duration::from_millis(1),
        );
        wait_for_state(&failing, "failed").await?;
        failing.shutdown().await?;

        let dropped = ReplayMaintenance::start_with_interval(
            app.db,
            root.path().to_owned(),
            app.ingress_permit,
            app.query_permit,
            None,
            Duration::from_secs(60),
        );
        drop(dropped);
        Ok(())
    }

    #[tokio::test]
    async fn retention_respects_readers_writers_shared_blobs_and_retries_orphans() -> Result<()> {
        let root = tempfile::tempdir()?;
        let app = crate::app::AppState::open(crate::config::Config {
            addr: "127.0.0.1:0".parse()?,
            data_dir: root.path().to_owned(),
            base_url: "http://localhost:8080".parse()?,
            s3_url: None,
            s3_endpoint: None,
            s3_initialize: false,
        })
        .await?;
        let segment = crate::sentry::replay::decode_envelope(
            include_bytes!("../../tests/fixtures/replay/plain-0.envelope"),
            &[],
        )?
        .unwrap();
        let blob = super::super::replay::write(root.path(), &segment.recording)?;
        let reference = blob.clone();
        app.db.call(move |db| {
            let tx = db.transaction()?;
            for project in [1,2] {
                tx.execute("INSERT INTO projects(id,slug,name,created_at_us,updated_at_us) VALUES(?1,?2,'test',0,0)", rusqlite::params![project,project.to_string()])?;
                crate::db::replays::accept(&tx, project, &crate::db::replays::PreparedReplay {
                    metadata:segment.metadata.clone(), blob:reference.clone(), recording_bytes:segment.recording.len(), frustration:Default::default(),
                }, crate::model::now_us()?)?;
            }
            tx.execute("UPDATE replays SET expires_at_us=0 WHERE project_id=1", [])?;
            tx.commit()?;
            Ok(())
        }).await?;
        let cursor = Arc::new(Mutex::new(None));
        for gate in [&app.ingress_permit, &app.query_permit] {
            let pin = gate.clone().acquire_owned().await?;
            assert!(
                sweep(
                    &app.db,
                    root.path().to_owned(),
                    cursor.clone(),
                    &app.ingress_permit,
                    &app.query_permit,
                    None
                )
                .await?
                .is_none()
            );
            assert!(root.path().join(&blob.key).exists());
            app.db
                .call(|db| {
                    assert_eq!(
                        crate::operations::replay_status(db, crate::model::now_us()?)?
                            .expired_replays,
                        "1"
                    );
                    Ok(())
                })
                .await?;
            drop(pin);
        }
        assert!(
            sweep(
                &app.db,
                root.path().to_owned(),
                cursor.clone(),
                &app.ingress_permit,
                &app.query_permit,
                None
            )
            .await?
            .is_some()
        );
        assert!(
            root.path().join(&blob.key).exists(),
            "another project still references the same blob"
        );
        app.db
            .call(|db| {
                db.execute("UPDATE replays SET expires_at_us=0", [])?;
                Ok(())
            })
            .await?;
        // Model a crash after committing reference deletion but before physical unlink.
        app.db
            .call(|db| crate::db::replays::expire_batch(db, crate::model::now_us()?))
            .await?;
        assert!(root.path().join(&blob.key).exists());
        let before = std::fs::metadata(root.path().join(&blob.key))?.len();
        let result = sweep(
            &app.db,
            root.path().to_owned(),
            cursor.clone(),
            &app.ingress_permit,
            &app.query_permit,
            None,
        )
        .await?
        .unwrap();
        assert_eq!((result.0, result.1), (1, before));
        assert!(!root.path().join(&blob.key).exists());
        // Discovery stays bounded and never deletes unknown files or directories.
        for index in 0..300 {
            std::fs::write(
                root.path().join(format!("replay-blobs/{index:064x}.zlib")),
                b"orphan",
            )?;
        }
        std::fs::write(root.path().join("replay-blobs/keep.txt"), b"keep")?;
        std::fs::write(root.path().join("replay-blobs/not-a-hash.zlib"), b"keep")?;
        std::fs::create_dir(root.path().join("replay-blobs/keep-directory"))?;
        let first = sweep(
            &app.db,
            root.path().to_owned(),
            cursor.clone(),
            &app.ingress_permit,
            &app.query_permit,
            None,
        )
        .await?
        .unwrap();
        assert!(first.0 <= 256);
        let second = sweep(
            &app.db,
            root.path().to_owned(),
            cursor,
            &app.ingress_permit,
            &app.query_permit,
            None,
        )
        .await?
        .unwrap();
        assert_eq!(first.0 + second.0, 300);
        assert!(root.path().join("replay-blobs/keep.txt").exists());
        Ok(())
    }
}
