//! One connection-owning thread. No transaction crosses an async/network await.

use std::{fs::File, path::Path, sync::Arc};

use anyhow::{Context, Result};
use rusqlite::Connection;
use tokio::sync::{mpsc, oneshot};

type Job = Box<dyn FnOnce(&mut Connection) + Send>;

#[derive(Clone)]
pub struct DbWorker {
    sender: mpsc::Sender<Job>,
}

impl DbWorker {
    pub fn start(path: &Path, directory_lock: Arc<File>) -> Result<Self> {
        let connection = super::open(path)?;
        let (sender, mut receiver) = mpsc::channel::<Job>(64);
        std::thread::Builder::new()
            .name("eventglass-db".into())
            .spawn(move || {
                // Keep the OS lock until all queued writes finish and the write
                // connection closes, even when the last AppState is dropped.
                let _directory_lock = directory_lock;
                let mut connection = connection;
                while let Some(job) = receiver.blocking_recv() {
                    job(&mut connection);
                }
            })?;
        Ok(Self { sender })
    }

    /// Callers carrying request data must retain their byte-budget permit until
    /// this completes; the bounded command count alone is not a RAM budget.
    pub async fn call<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    {
        let (reply, receiver) = oneshot::channel();
        self.sender
            .send(Box::new(move |connection| {
                let result = operation(connection);
                // A disconnected client does not undo a committed operation.
                let _ = reply.send(result);
            }))
            .await
            .map_err(|_| anyhow::anyhow!("metadata worker stopped"))?;
        receiver
            .await
            .context("metadata worker reply interrupted")?
    }
}
