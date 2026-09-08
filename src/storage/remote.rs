//! Durable S3 installation identity and ordered checkpoint publication.

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, path::PathBuf};

use super::{
    checkpoint::CheckpointCut,
    s3::{ObjectMetadata, ObjectStore},
};

const MAX_CONTROL_BYTES: u64 = 4 * 1024 * 1024;
const MAX_LISTED_OBJECTS: usize = 100_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationDocument {
    pub format_version: u32,
    pub installation_id: String,
    pub created_at_us: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectReference {
    pub key: String,
    pub size: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardReference {
    pub id: String,
    pub object: ObjectReference,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointDocument {
    pub format_version: u32,
    pub checkpoint_id: String,
    pub sequence: u64,
    pub created_at_us: i64,
    pub cut: CheckpointCut,
    pub snapshot: ObjectReference,
    pub shards: Vec<ShardReference>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LatestDocument {
    pub format_version: u32,
    pub installation_id: String,
    pub checkpoint_id: String,
    pub sequence: u64,
    pub checkpoint_sha256: String,
}

#[derive(Debug, Clone)]
pub struct LocalCheckpoint {
    pub document: CheckpointDocument,
    pub snapshot_path: PathBuf,
    pub shard_archives: Vec<(String, PathBuf)>,
}

pub async fn ensure_installation(
    store: &dyn ObjectStore,
    requested: &InstallationDocument,
    initialize_empty: bool,
) -> Result<InstallationDocument> {
    validate_installation(requested)?;
    let objects = list_all(store).await?;
    if objects.is_empty() {
        ensure!(
            initialize_empty,
            "empty S3 prefix requires explicit initialization"
        );
        store
            .put_if_absent("installation.json", serde_json::to_vec(requested)?)
            .await?;
    } else {
        ensure!(
            objects
                .iter()
                .any(|object| object.key == "installation.json"),
            "non-empty S3 prefix has no installation identity"
        );
    }
    let actual: InstallationDocument = serde_json::from_slice(
        &store
            .get_small("installation.json", MAX_CONTROL_BYTES)
            .await?,
    )?;
    validate_installation(&actual)?;
    ensure!(
        actual.installation_id == requested.installation_id,
        "S3 installation identity mismatch"
    );
    Ok(actual)
}

pub async fn publish(
    store: &dyn ObjectStore,
    candidate: &LocalCheckpoint,
) -> Result<LatestDocument> {
    validate_checkpoint(&candidate.document)?;
    let archive_ids = candidate
        .shard_archives
        .iter()
        .map(|(id, _)| id.as_str())
        .collect::<BTreeSet<_>>();
    let document_ids = candidate
        .document
        .shards
        .iter()
        .map(|shard| shard.id.as_str())
        .collect::<BTreeSet<_>>();
    ensure!(
        archive_ids == document_ids,
        "checkpoint archive set mismatch"
    );

    for shard in &candidate.document.shards {
        let path = candidate
            .shard_archives
            .iter()
            .find_map(|(id, path)| (id == &shard.id).then_some(path))
            .context("checkpoint shard archive missing")?;
        ensure!(
            tokio::fs::metadata(path).await?.len() == shard.object.size,
            "checkpoint shard archive size changed"
        );
        store
            .put_file(&shard.object.key, path, &shard.object.sha256)
            .await?;
    }
    ensure!(
        tokio::fs::metadata(&candidate.snapshot_path).await?.len()
            == candidate.document.snapshot.size,
        "checkpoint snapshot size changed"
    );
    store
        .put_file(
            &candidate.document.snapshot.key,
            &candidate.snapshot_path,
            &candidate.document.snapshot.sha256,
        )
        .await?;

    let checkpoint_bytes = serde_json::to_vec(&candidate.document)?;
    let checkpoint_key = checkpoint_key(&candidate.document.checkpoint_id);
    store
        .put_if_absent(&checkpoint_key, checkpoint_bytes.clone())
        .await?;
    let latest = LatestDocument {
        format_version: 1,
        installation_id: candidate.document.cut.installation_id.clone(),
        checkpoint_id: candidate.document.checkpoint_id.clone(),
        sequence: candidate.document.sequence,
        checkpoint_sha256: sha256(&checkpoint_bytes),
    };
    store
        .put_bytes("latest.json", serde_json::to_vec(&latest)?)
        .await?;
    Ok(latest)
}

pub async fn list_all(store: &dyn ObjectStore) -> Result<Vec<ObjectMetadata>> {
    let mut objects = Vec::new();
    let mut continuation = None;
    let mut seen_tokens = BTreeSet::new();
    loop {
        let page = store.list(continuation).await?;
        objects.extend(page.objects);
        ensure!(
            objects.len() <= MAX_LISTED_OBJECTS,
            "S3 prefix contains too many objects"
        );
        let Some(next) = page.continuation else {
            break;
        };
        ensure!(
            !next.is_empty() && seen_tokens.insert(next.clone()),
            "S3 listing continuation did not advance"
        );
        continuation = Some(next);
    }
    Ok(objects)
}

pub fn checkpoint_key(id: &str) -> String {
    format!("checkpoints/{id}.json")
}

pub fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn validate_installation(document: &InstallationDocument) -> Result<()> {
    ensure!(
        document.format_version == 1 && document.created_at_us >= 0,
        "invalid installation document"
    );
    uuid::Uuid::parse_str(&document.installation_id)?;
    Ok(())
}

pub fn validate_checkpoint(document: &CheckpointDocument) -> Result<()> {
    ensure!(
        document.format_version == 1
            && document.sequence > 0
            && document.created_at_us >= 0
            && document.cut.format_version == 1,
        "invalid checkpoint document"
    );
    uuid::Uuid::parse_str(&document.checkpoint_id)?;
    uuid::Uuid::parse_str(&document.cut.installation_id)?;
    uuid::Uuid::parse_str(&document.cut.storage_generation)?;
    ensure!(
        document.cut.boundary.inbox_id >= 0
            && document.cut.boundary.ingest_seq >= 0
            && (document.cut.boundary.inbox_id == 0) == (document.cut.boundary.ingest_seq == 0),
        "invalid checkpoint boundary"
    );
    ensure!(
        document.snapshot.key == format!("snapshots/{}.db", document.checkpoint_id),
        "invalid checkpoint snapshot key"
    );
    validate_object(&document.snapshot)?;
    let cut_ids = document
        .cut
        .shards
        .iter()
        .map(|shard| shard.id.as_str())
        .collect::<BTreeSet<_>>();
    ensure!(
        cut_ids.len() == document.cut.shards.len()
            && document.cut.shards.iter().all(|shard| {
                uuid::Uuid::parse_str(&shard.id).is_ok()
                    && matches!(shard.state.as_str(), "local" | "remote_verified")
                    && shard.last_applied_inbox_id <= document.cut.boundary.inbox_id
            }),
        "duplicate checkpoint catalog shard"
    );
    let mut object_ids = BTreeSet::new();
    for shard in &document.shards {
        uuid::Uuid::parse_str(&shard.id)?;
        ensure!(
            object_ids.insert(shard.id.as_str()),
            "duplicate checkpoint shard object"
        );
        ensure!(
            shard.object.key == format!("shards/{}.tar.gz", shard.id),
            "invalid shard archive key"
        );
        validate_object(&shard.object)?;
    }
    ensure!(
        cut_ids == object_ids,
        "checkpoint catalog and archive set differ"
    );
    Ok(())
}

fn validate_object(object: &ObjectReference) -> Result<()> {
    ensure!(
        object.size > 0
            && object.sha256.len() == 64
            && object
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "invalid checkpoint object reference"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        model::Boundary,
        storage::{
            checkpoint::CheckpointShard,
            s3::{ObjectPage, ObjectStore},
        },
    };
    use async_trait::async_trait;
    use std::{
        collections::BTreeMap,
        path::Path,
        sync::{Arc, Mutex},
    };

    #[derive(Clone, Default)]
    struct MemoryStore {
        objects: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
        writes: Arc<Mutex<Vec<String>>>,
        fail_key: Arc<Mutex<Option<String>>>,
    }

    impl MemoryStore {
        fn keys(&self) -> Vec<String> {
            self.objects.lock().unwrap().keys().cloned().collect()
        }

        fn writes(&self) -> Vec<String> {
            self.writes.lock().unwrap().clone()
        }

        fn fail_on(&self, key: &str) {
            *self.fail_key.lock().unwrap() = Some(key.to_owned());
        }

        fn write(&self, key: &str, bytes: Vec<u8>, absent: bool) -> Result<()> {
            if self.fail_key.lock().unwrap().as_deref() == Some(key) {
                anyhow::bail!("injected object failure");
            }
            let mut objects = self.objects.lock().unwrap();
            ensure!(
                !absent || !objects.contains_key(key),
                "object already exists"
            );
            objects.insert(key.to_owned(), bytes);
            self.writes.lock().unwrap().push(key.to_owned());
            Ok(())
        }
    }

    #[async_trait]
    impl ObjectStore for MemoryStore {
        async fn list(&self, _continuation: Option<String>) -> Result<ObjectPage> {
            Ok(ObjectPage {
                objects: self
                    .objects
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(key, bytes)| ObjectMetadata {
                        key: key.clone(),
                        size: bytes.len() as u64,
                        etag: None,
                    })
                    .collect(),
                continuation: None,
            })
        }

        async fn get_small(&self, relative: &str, max_bytes: u64) -> Result<Vec<u8>> {
            let bytes = self
                .objects
                .lock()
                .unwrap()
                .get(relative)
                .context("object not found")?
                .clone();
            ensure!(bytes.len() as u64 <= max_bytes, "object too large");
            Ok(bytes)
        }

        async fn put_if_absent(&self, relative: &str, bytes: Vec<u8>) -> Result<()> {
            self.write(relative, bytes, true)
        }

        async fn put_bytes(&self, relative: &str, bytes: Vec<u8>) -> Result<()> {
            self.write(relative, bytes, false)
        }

        async fn put_file(
            &self,
            relative: &str,
            path: &Path,
            checksum_sha256_hex: &str,
        ) -> Result<()> {
            let bytes = std::fs::read(path)?;
            ensure!(sha256(&bytes) == checksum_sha256_hex, "checksum mismatch");
            self.write(relative, bytes, false)
        }

        async fn download(
            &self,
            relative: &str,
            destination: &Path,
            max_bytes: u64,
        ) -> Result<ObjectMetadata> {
            let bytes = self.get_small(relative, max_bytes).await?;
            std::fs::write(destination, &bytes)?;
            Ok(ObjectMetadata {
                key: relative.to_owned(),
                size: bytes.len() as u64,
                etag: None,
            })
        }
    }

    fn installation(id: &str) -> InstallationDocument {
        InstallationDocument {
            format_version: 1,
            installation_id: id.to_owned(),
            created_at_us: 1,
        }
    }

    fn candidate(root: &Path, installation_id: &str) -> Result<LocalCheckpoint> {
        let checkpoint_id = uuid::Uuid::new_v4().to_string();
        let shard_id = uuid::Uuid::new_v4().to_string();
        let snapshot_path = root.join("snapshot.db");
        let archive_path = root.join("shard.tar.gz");
        std::fs::write(&snapshot_path, b"sqlite snapshot")?;
        std::fs::write(&archive_path, b"sealed shard")?;
        Ok(LocalCheckpoint {
            document: CheckpointDocument {
                format_version: 1,
                checkpoint_id: checkpoint_id.clone(),
                sequence: 7,
                created_at_us: 2,
                cut: CheckpointCut {
                    format_version: 1,
                    installation_id: installation_id.to_owned(),
                    storage_generation: uuid::Uuid::new_v4().to_string(),
                    boundary: Boundary {
                        inbox_id: 4,
                        ingest_seq: 5,
                    },
                    shards: vec![CheckpointShard {
                        id: shard_id.clone(),
                        state: "local".into(),
                        remote_archive_key: None,
                        archive_sha256: None,
                        last_applied_inbox_id: 4,
                    }],
                },
                snapshot: ObjectReference {
                    key: format!("snapshots/{checkpoint_id}.db"),
                    size: std::fs::metadata(&snapshot_path)?.len(),
                    sha256: sha256(&std::fs::read(&snapshot_path)?),
                },
                shards: vec![ShardReference {
                    id: shard_id.clone(),
                    object: ObjectReference {
                        key: format!("shards/{shard_id}.tar.gz"),
                        size: std::fs::metadata(&archive_path)?.len(),
                        sha256: sha256(&std::fs::read(&archive_path)?),
                    },
                }],
            },
            snapshot_path,
            shard_archives: vec![(shard_id, archive_path)],
        })
    }

    #[tokio::test]
    async fn empty_prefix_requires_explicit_installation_and_preserves_identity() -> Result<()> {
        let store = MemoryStore::default();
        let id = uuid::Uuid::new_v4().to_string();
        let expected = installation(&id);
        assert!(ensure_installation(&store, &expected, false).await.is_err());
        assert!(store.keys().is_empty());
        assert_eq!(
            ensure_installation(&store, &expected, true).await?,
            expected
        );
        let other = installation(&uuid::Uuid::new_v4().to_string());
        assert!(ensure_installation(&store, &other, false).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_is_published_only_after_every_data_object() -> Result<()> {
        let root = tempfile::tempdir()?;
        let installation_id = uuid::Uuid::new_v4().to_string();
        let candidate = candidate(root.path(), &installation_id)?;
        let store = MemoryStore::default();
        ensure_installation(&store, &installation(&installation_id), true).await?;
        let latest = publish(&store, &candidate).await?;
        assert_eq!(latest.checkpoint_id, candidate.document.checkpoint_id);
        let writes = store.writes();
        let checkpoint = checkpoint_key(&candidate.document.checkpoint_id);
        assert!(
            writes
                .iter()
                .position(|key| key == &candidate.document.shards[0].object.key)
                < writes.iter().position(|key| key == &checkpoint)
        );
        assert!(
            writes
                .iter()
                .position(|key| key == &candidate.document.snapshot.key)
                < writes.iter().position(|key| key == &checkpoint)
        );
        assert!(
            writes.iter().position(|key| key == &checkpoint)
                < writes.iter().position(|key| key == "latest.json")
        );

        let failed = MemoryStore::default();
        failed.fail_on(&candidate.document.snapshot.key);
        assert!(publish(&failed, &candidate).await.is_err());
        assert!(!failed.keys().contains(&checkpoint));
        assert!(!failed.keys().contains(&"latest.json".to_owned()));
        Ok(())
    }
}
