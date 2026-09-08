//! Bounded local sealed-shard reader cache. Returned pins keep readers alive
//! while a query runs, even if the cache later needs another handle.

use std::{
    collections::HashMap,
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, ensure};

use crate::search::active::{Published, open_sealed};

const MAX_OPEN_SEALED_SHARDS: usize = 8;

#[derive(Clone)]
pub struct Registry {
    root: PathBuf,
    installation_id: String,
    state: Arc<Mutex<State>>,
}

struct State {
    clock: u64,
    open: HashMap<String, Cached>,
}

struct Cached {
    last_used: u64,
    shard: Arc<Published>,
}

pub struct ShardPin {
    shard: Arc<Published>,
}

impl ShardPin {
    pub fn published(&self) -> &Published {
        &self.shard
    }
}

impl Registry {
    pub fn new(data_dir: &Path, installation_id: String) -> Self {
        Self {
            root: data_dir.join("shards"),
            installation_id,
            state: Arc::new(Mutex::new(State {
                clock: 0,
                open: HashMap::new(),
            })),
        }
    }

    pub fn pin_local(&self, shard_id: &str) -> Result<ShardPin> {
        uuid::Uuid::parse_str(shard_id).context("invalid sealed shard ID")?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("shard registry lock poisoned"))?;
        state.clock = state
            .clock
            .checked_add(1)
            .context("registry clock exhausted")?;
        let now = state.clock;
        if let Some(cached) = state.open.get_mut(shard_id) {
            cached.last_used = now;
            return Ok(ShardPin {
                shard: Arc::clone(&cached.shard),
            });
        }
        if state.open.len() >= MAX_OPEN_SEALED_SHARDS {
            let evict = state
                .open
                .iter()
                .filter(|(_, cached)| Arc::strong_count(&cached.shard) == 1)
                .min_by_key(|(_, cached)| cached.last_used)
                .map(|(id, _)| id.clone())
                .context("all sealed shard handles are pinned")?;
            state.open.remove(&evict);
        }
        // Keep the exclusion lock across verify+open so another local operation
        // cannot publish or evict this path between the checks.
        let path = self.root.join(shard_id);
        ensure!(path.is_dir(), "local sealed shard directory is missing");
        let manifest = super::manifest::verify(&path, &self.installation_id, shard_id)?;
        let shard = Arc::new(open_sealed(&path, &manifest)?);
        state.open.insert(
            shard_id.to_owned(),
            Cached {
                last_used: now,
                shard: Arc::clone(&shard),
            },
        );
        Ok(ShardPin { shard })
    }

    pub fn evict_local(
        &self,
        shard_id: &str,
        mark_remote_only: impl FnOnce() -> Result<bool>,
        rollback: impl FnOnce() -> Result<()>,
    ) -> Result<bool> {
        uuid::Uuid::parse_str(shard_id).context("invalid eviction shard ID")?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("shard registry lock poisoned"))?;
        if state
            .open
            .get(shard_id)
            .is_some_and(|cached| Arc::strong_count(&cached.shard) > 1)
        {
            return Ok(false);
        }
        let path = self.root.join(shard_id);
        let metadata = std::fs::symlink_metadata(&path)?;
        ensure!(
            metadata.file_type().is_dir(),
            "eviction target is not a directory"
        );
        if !mark_remote_only()? {
            return Ok(false);
        }
        let data_dir = self.root.parent().context("shard root has no parent")?;
        let tombstone = data_dir.join(format!(".evict-{}-{}.tmp", shard_id, uuid::Uuid::new_v4()));
        if let Err(error) = std::fs::rename(&path, &tombstone) {
            rollback()?;
            return Err(error.into());
        }
        state.open.remove(shard_id);
        File::open(&self.root)?.sync_all()?;
        std::fs::remove_dir_all(&tombstone)?;
        File::open(data_dir)?.sync_all()?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{model::Boundary, search::active::ActiveShard, storage::manifest::ShardStats};

    #[test]
    fn pinned_handles_block_lru_reuse_until_a_query_releases_one() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let root = directory.path().join("shards");
        std::fs::create_dir(&root)?;
        let installation = uuid::Uuid::new_v4().to_string();
        let mut ids = Vec::new();
        for _ in 0..=MAX_OPEN_SEALED_SHARDS {
            let id = uuid::Uuid::new_v4().to_string();
            let path = root.join(&id);
            std::fs::create_dir(&path)?;
            let mut active = ActiveShard::create(&path, &installation, &id, Boundary::default())?;
            active.publish(Boundary::default())?;
            active.seal(
                &path,
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
            ids.push(id);
        }
        let registry = Registry::new(directory.path(), installation);
        let mut pins = ids[..MAX_OPEN_SEALED_SHARDS]
            .iter()
            .map(|id| registry.pin_local(id))
            .collect::<Result<Vec<_>>>()?;
        assert!(registry.pin_local(&ids[MAX_OPEN_SEALED_SHARDS]).is_err());
        pins.pop();
        assert!(registry.pin_local(&ids[MAX_OPEN_SEALED_SHARDS]).is_ok());
        // A pin retained by the caller still has a usable native generation.
        assert_eq!(pins[0].published().searcher.num_docs(), 0);
        Ok(())
    }
}
