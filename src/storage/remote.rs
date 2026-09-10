//! Durable S3 installation identity and ordered checkpoint publication.

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};

use super::{
    checkpoint::CheckpointCut,
    s3::{ObjectMetadata, ObjectStore},
};

const MAX_CONTROL_BYTES: u64 = 4 * 1024 * 1024;
const MAX_LISTED_OBJECTS: usize = 100_000;

#[derive(Debug, Clone, Copy)]
pub struct RestoreLimits {
    pub snapshot_bytes: u64,
    pub shard_archive_bytes: u64,
    pub shard_expanded_bytes: u64,
    pub total_download_bytes: u64,
}

impl Default for RestoreLimits {
    fn default() -> Self {
        Self {
            snapshot_bytes: 16 * 1024 * 1024 * 1024,
            shard_archive_bytes: 2 * 1024 * 1024 * 1024,
            shard_expanded_bytes: 4 * 1024 * 1024 * 1024,
            total_download_bytes: 64 * 1024 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PreparedRestore {
    pub document: CheckpointDocument,
    pub directory: PathBuf,
}

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
    pub replay_files: Vec<(String, PathBuf)>,
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

pub async fn read_installation(store: &dyn ObjectStore) -> Result<Option<InstallationDocument>> {
    let objects = list_all(store).await?;
    if objects.is_empty() {
        return Ok(None);
    }
    ensure!(
        objects
            .iter()
            .any(|object| object.key == "installation.json"),
        "non-empty S3 prefix has no installation identity"
    );
    let document: InstallationDocument = serde_json::from_slice(
        &store
            .get_small("installation.json", MAX_CONTROL_BYTES)
            .await?,
    )?;
    validate_installation(&document)?;
    Ok(Some(document))
}

pub async fn publish(
    store: &dyn ObjectStore,
    candidate: &LocalCheckpoint,
) -> Result<LatestDocument> {
    let checkpoint_bytes = serde_json::to_vec(&candidate.document)?;
    ensure!(
        checkpoint_bytes.len() as u64 <= MAX_CONTROL_BYTES,
        "checkpoint exceeds control document limit"
    );
    validate_checkpoint(&candidate.document)?;
    let installation: InstallationDocument = serde_json::from_slice(
        &store
            .get_small("installation.json", MAX_CONTROL_BYTES)
            .await?,
    )?;
    validate_installation(&installation)?;
    ensure!(
        installation.installation_id == candidate.document.cut.installation_id,
        "checkpoint publication namespace differs from local installation"
    );
    let replay_paths: BTreeMap<_, _> = candidate
        .replay_files
        .iter()
        .map(|(key, path)| (key.as_str(), path))
        .collect();
    let replay_keys: BTreeSet<_> = replay_paths.keys().copied().collect();
    let expected_keys: BTreeSet<_> = candidate
        .document
        .cut
        .replay_blobs
        .iter()
        .map(|r| r.key.as_str())
        .collect();
    ensure!(
        replay_keys.len() == candidate.replay_files.len() && replay_keys == expected_keys,
        "checkpoint replay file set mismatch"
    );
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
        archive_ids.len() == candidate.shard_archives.len() && archive_ids.is_subset(&document_ids),
        "checkpoint archive set mismatch"
    );

    let existing = if archive_ids.len() < document_ids.len()
        || !candidate.document.cut.replay_blobs.is_empty()
    {
        list_all(store).await?
    } else {
        Vec::new()
    };
    let existing_sizes: BTreeMap<_, _> = existing
        .iter()
        .map(|object| (&object.key, object.size))
        .collect();
    let mut previous_replays = BTreeMap::new();
    if !candidate.document.cut.replay_blobs.is_empty()
        && existing_sizes.contains_key(&"latest.json".to_owned())
    {
        // latest remains an optimization. A broken pointer cannot prevent a new
        // complete checkpoint from being uploaded from verified local files.
        let previous: Result<CheckpointDocument> = async {
            let latest: LatestDocument =
                serde_json::from_slice(&store.get_small("latest.json", MAX_CONTROL_BYTES).await?)?;
            let bytes = store
                .get_small(&checkpoint_key(&latest.checkpoint_id), MAX_CONTROL_BYTES)
                .await?;
            ensure!(
                sha256(&bytes) == latest.checkpoint_sha256,
                "previous checkpoint checksum mismatch"
            );
            let previous: CheckpointDocument = serde_json::from_slice(&bytes)?;
            validate_checkpoint(&previous)?;
            ensure!(
                previous.cut.installation_id == candidate.document.cut.installation_id,
                "previous Replay namespace mismatch"
            );
            Ok(previous)
        }
        .await;
        if let Ok(previous) = previous {
            previous_replays = previous
                .cut
                .replay_blobs
                .into_iter()
                .map(|r| (r.key.clone(), r))
                .collect();
        }
    }
    for reference in &candidate.document.cut.replay_blobs {
        let path = replay_paths
            .get(reference.key.as_str())
            .context("missing replay file")?;
        ensure!(
            tokio::fs::metadata(path).await?.len() == reference.size,
            "checkpoint replay size changed"
        );
        // Reuse only immutable objects already covered by a completed checkpoint.
        if previous_replays.get(&reference.key) != Some(reference)
            || existing_sizes.get(&reference.key) != Some(&reference.size)
        {
            store
                .put_file(&reference.key, path, &reference.sha256)
                .await?;
        }
    }
    for shard in &candidate.document.shards {
        let path = candidate
            .shard_archives
            .iter()
            .find_map(|(id, path)| (id == &shard.id).then_some(path));
        let Some(path) = path else {
            let catalog = candidate
                .document
                .cut
                .shards
                .iter()
                .find(|entry| entry.id == shard.id)
                .context("checkpoint catalog shard missing")?;
            ensure!(
                matches!(catalog.state.as_str(), "remote_verified" | "remote_only")
                    && catalog.remote_archive_key.as_ref() == Some(&shard.object.key)
                    && catalog.archive_sha256.as_ref() == Some(&shard.object.sha256)
                    && existing
                        .iter()
                        .any(|object| object.key == shard.object.key
                            && object.size == shard.object.size),
                "previously verified checkpoint archive is missing or changed"
            );
            continue;
        };
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

    let checkpoint_key = checkpoint_key(&candidate.document.checkpoint_id);
    if let Err(error) = store
        .put_if_absent(&checkpoint_key, checkpoint_bytes.clone())
        .await
    {
        // A lost response after immutable publication is safe to retry only byte-for-byte.
        let existing = store.get_small(&checkpoint_key, MAX_CONTROL_BYTES).await?;
        ensure!(
            existing == checkpoint_bytes,
            "immutable checkpoint differs after retry: {error}"
        );
    }
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

pub async fn prepare_restore(
    store: &dyn ObjectStore,
    installation_id: &str,
    destination: &Path,
    limits: RestoreLimits,
    cancelled: impl Fn() -> bool + Send + Sync + 'static,
) -> Result<PreparedRestore> {
    uuid::Uuid::parse_str(installation_id)?;
    ensure!(!destination.exists(), "restore destination already exists");
    ensure!(
        limits.snapshot_bytes > 0
            && limits.shard_archive_bytes > 0
            && limits.shard_expanded_bytes > 0
            && limits.total_download_bytes >= limits.snapshot_bytes,
        "invalid restore limits"
    );
    let candidates = checkpoint_candidates(store, installation_id).await?;
    ensure!(
        !candidates.is_empty(),
        "no checkpoint exists for installation"
    );
    let parent = destination.parent().context("restore destination parent")?;
    std::fs::create_dir_all(parent)?;

    let cancelled: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(cancelled);
    for document in candidates {
        ensure!(!cancelled(), "checkpoint restore cancelled");
        let attempt = parent.join(format!(".restore-{}.tmp", uuid::Uuid::new_v4()));
        std::fs::create_dir(&attempt)?;
        let guard = RemoveDirectory(attempt.clone());
        if let Err(error) = restore_candidate(store, &document, &attempt, limits, &cancelled).await
        {
            tracing::warn!(checkpoint_id = %document.checkpoint_id, reason = %error, "checkpoint restore failed; trying an older complete checkpoint");
            continue;
        }
        std::fs::rename(&attempt, destination)?;
        File::open(parent)?.sync_all()?;
        drop(guard);
        return Ok(PreparedRestore {
            document,
            directory: destination.to_owned(),
        });
    }
    anyhow::bail!("no complete checkpoint could be restored")
}

async fn checkpoint_candidates(
    store: &dyn ObjectStore,
    installation_id: &str,
) -> Result<Vec<CheckpointDocument>> {
    let objects = list_all(store).await?;
    ensure!(
        objects
            .iter()
            .any(|object| object.key == "installation.json"),
        "S3 prefix has no installation identity"
    );
    let installation: InstallationDocument = serde_json::from_slice(
        &store
            .get_small("installation.json", MAX_CONTROL_BYTES)
            .await?,
    )?;
    validate_installation(&installation)?;
    ensure!(
        installation.installation_id == installation_id,
        "S3 installation identity mismatch"
    );

    let mut candidates = BTreeMap::<String, CheckpointDocument>::new();
    if objects.iter().any(|object| object.key == "latest.json")
        && let Ok(bytes) = store.get_small("latest.json", MAX_CONTROL_BYTES).await
        && let Ok(latest) = serde_json::from_slice::<LatestDocument>(&bytes)
        && latest.format_version == 1
        && latest.installation_id == installation_id
        && is_sha256(&latest.checkpoint_sha256)
    {
        let key = checkpoint_key(&latest.checkpoint_id);
        if let Ok(bytes) = store.get_small(&key, MAX_CONTROL_BYTES).await
            && sha256(&bytes) == latest.checkpoint_sha256
            && let Ok(document) = serde_json::from_slice::<CheckpointDocument>(&bytes)
            && validate_checkpoint(&document).is_ok()
            && document.cut.installation_id == installation_id
            && document.sequence == latest.sequence
        {
            candidates.insert(document.checkpoint_id.clone(), document);
        }
    }
    for key in objects.iter().filter_map(|object| {
        object
            .key
            .strip_prefix("checkpoints/")
            .and_then(|name| name.strip_suffix(".json"))
            .map(|_| object.key.as_str())
    }) {
        let Ok(bytes) = store.get_small(key, MAX_CONTROL_BYTES).await else {
            continue;
        };
        let Ok(document) = serde_json::from_slice::<CheckpointDocument>(&bytes) else {
            continue;
        };
        if validate_checkpoint(&document).is_ok()
            && document.cut.installation_id == installation_id
            && key == checkpoint_key(&document.checkpoint_id)
        {
            candidates.insert(document.checkpoint_id.clone(), document);
        }
    }
    let mut candidates = candidates.into_values().collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        (right.sequence, right.created_at_us, &right.checkpoint_id).cmp(&(
            left.sequence,
            left.created_at_us,
            &left.checkpoint_id,
        ))
    });
    Ok(candidates)
}

async fn restore_candidate(
    store: &dyn ObjectStore,
    document: &CheckpointDocument,
    attempt: &Path,
    limits: RestoreLimits,
    cancelled: &Arc<dyn Fn() -> bool + Send + Sync>,
) -> Result<()> {
    ensure!(
        document.snapshot.size <= limits.snapshot_bytes,
        "checkpoint snapshot exceeds restore limit"
    );
    let replay_size = document
        .cut
        .replay_blobs
        .iter()
        .try_fold(0u64, |total, reference| {
            total
                .checked_add(reference.size)
                .context("replay restore size overflow")
        })?;
    let total = document.shards.iter().try_fold(
        document
            .snapshot
            .size
            .checked_add(replay_size)
            .context("restore size overflow")?,
        |sum, shard| -> Result<u64> {
            ensure!(
                shard.object.size <= limits.shard_archive_bytes,
                "shard archive exceeds restore limit"
            );
            sum.checked_add(shard.object.size)
                .context("restore download size overflow")
        },
    )?;
    ensure!(
        total <= limits.total_download_bytes,
        "checkpoint exceeds total restore limit"
    );
    let snapshot = attempt.join("meta.db");
    let downloaded = store
        .download(&document.snapshot.key, &snapshot, limits.snapshot_bytes)
        .await?;
    verify_download(&snapshot, &document.snapshot, &downloaded).await?;
    let snapshot_path = snapshot.clone();
    let cut = document.cut.clone();
    tokio::task::spawn_blocking(move || validate_snapshot(&snapshot_path, &cut)).await??;

    let replay_root = attempt.join("replay-blobs");
    std::fs::create_dir(&replay_root)?;
    for reference in &document.cut.replay_blobs {
        ensure!(!cancelled(), "checkpoint restore cancelled");
        let destination = attempt.join(&reference.key);
        let downloaded = store
            .download(&reference.key, &destination, reference.size)
            .await?;
        verify_download(&destination, reference, &downloaded).await?;
    }
    File::open(&replay_root)?.sync_all()?;
    let shard_root = attempt.join("shards");
    std::fs::create_dir(&shard_root)?;
    for shard in &document.shards {
        ensure!(!cancelled(), "checkpoint restore cancelled");
        let compressed = attempt.join(format!("{}.tar.gz", shard.id));
        let downloaded = store
            .download(&shard.object.key, &compressed, limits.shard_archive_bytes)
            .await?;
        verify_download(&compressed, &shard.object, &downloaded).await?;
        let archive_path = compressed.clone();
        let destination = shard_root.join(&shard.id);
        let installation = document.cut.installation_id.clone();
        let shard_id = shard.id.clone();
        let expanded = limits.shard_expanded_bytes;
        let cancelled = Arc::clone(cancelled);
        tokio::task::spawn_blocking(move || {
            super::archive::hydrate(
                &archive_path,
                &destination,
                &installation,
                &shard_id,
                expanded,
                || cancelled(),
            )
        })
        .await??;
        std::fs::remove_file(compressed)?;
    }
    let snapshot_path = snapshot.clone();
    let restored_document = document.clone();
    tokio::task::spawn_blocking(move || {
        secure_restored_snapshot(&snapshot_path, &restored_document)
    })
    .await??;
    File::open(&shard_root)?.sync_all()?;
    File::open(attempt)?.sync_all()?;
    Ok(())
}

pub fn install_prepared(data_dir: &Path, prepared: PreparedRestore) -> Result<()> {
    ensure!(
        prepared.directory.parent() == Some(data_dir),
        "restore staging directory must be inside the data directory"
    );
    ensure!(
        !data_dir.join("meta.db").exists(),
        "a local database already exists"
    );
    ensure!(
        prepared.directory.join("meta.db").is_file()
            && prepared.directory.join("shards").is_dir()
            && prepared.directory.join("replay-blobs").is_dir(),
        "restore staging layout is incomplete"
    );
    let quarantine = data_dir
        .join("quarantine")
        .join(format!("restore-{}", uuid::Uuid::new_v4()));
    let mut quarantined = false;
    for name in ["meta.db-wal", "meta.db-shm", "shards", "replay-blobs"] {
        let source = data_dir.join(name);
        if source.exists() {
            if !quarantined {
                std::fs::create_dir_all(&quarantine)?;
                quarantined = true;
            }
            std::fs::rename(source, quarantine.join(name))?;
        }
    }
    if quarantined {
        File::open(quarantine.parent().context("restore quarantine parent")?)?.sync_all()?;
    }
    std::fs::rename(prepared.directory.join("shards"), data_dir.join("shards"))?;
    std::fs::rename(
        prepared.directory.join("replay-blobs"),
        data_dir.join("replay-blobs"),
    )?;
    File::open(data_dir)?.sync_all()?;
    std::fs::rename(prepared.directory.join("meta.db"), data_dir.join("meta.db"))?;
    File::open(data_dir)?.sync_all()?;
    std::fs::remove_dir(prepared.directory)?;
    File::open(data_dir)?.sync_all()?;
    Ok(())
}

async fn verify_download(
    path: &Path,
    expected: &ObjectReference,
    downloaded: &ObjectMetadata,
) -> Result<()> {
    ensure!(
        downloaded.key == expected.key && downloaded.size == expected.size,
        "downloaded object metadata mismatch"
    );
    let path = path.to_owned();
    let expected = expected.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        ensure!(
            std::fs::metadata(&path)?.len() == expected.size,
            "downloaded object size mismatch"
        );
        ensure!(
            sha256_file(&path)? == expected.sha256,
            "downloaded object checksum mismatch"
        );
        Ok(())
    })
    .await??;
    Ok(())
}

fn validate_snapshot(path: &Path, cut: &CheckpointCut) -> Result<()> {
    let connection = crate::db::open_reader(path)?;
    let integrity: String = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
    ensure!(
        integrity == "ok",
        "restored SQLite snapshot failed integrity check"
    );
    let actual: (String, String, i64, i64, Option<String>) = connection.query_row(
        "SELECT installation_id,storage_generation,last_applied_inbox_id,
                last_applied_ingest_seq,active_shard_id
         FROM runtime_state WHERE singleton=1",
        [],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        },
    )?;
    ensure!(
        actual
            == (
                cut.installation_id.clone(),
                cut.storage_generation.clone(),
                cut.boundary.inbox_id,
                cut.boundary.ingest_seq,
                None,
            ),
        "restored SQLite snapshot cut mismatch"
    );
    let mut statement = connection.prepare(
        "SELECT id,state,remote_archive_key,archive_sha256,last_applied_inbox_id
         FROM shards ORDER BY id",
    )?;
    let actual = statement
        .query_map([], |row| {
            Ok(super::checkpoint::CheckpointShard {
                id: row.get(0)?,
                state: row.get(1)?,
                remote_archive_key: row.get(2)?,
                archive_sha256: row.get(3)?,
                last_applied_inbox_id: row.get(4)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(actual == cut.shards, "restored SQLite catalog mismatch");
    ensure!(
        crate::db::replays::revision(&connection)? == cut.replay_revision,
        "restored replay revision mismatch"
    );
    ensure!(
        crate::db::replays::blob_references(&connection)? == cut.replay_blobs,
        "restored Replay catalog mismatch"
    );
    Ok(())
}

fn secure_restored_snapshot(path: &Path, document: &CheckpointDocument) -> Result<()> {
    let mut connection = crate::db::open(path)?;
    let transaction = connection.transaction()?;
    transaction.execute("DELETE FROM sessions", [])?;
    transaction.execute("DELETE FROM settings WHERE key='token_hmac_v1'", [])?;
    transaction.execute(
        "UPDATE runtime_state
         SET storage_generation=?1,authorization_epoch=authorization_epoch+1
         WHERE singleton=1",
        [uuid::Uuid::new_v4().to_string()],
    )?;
    for shard in &document.shards {
        ensure!(
            transaction.execute(
                "UPDATE shards
                 SET state='remote_verified',remote_archive_key=?1,
                     archive_sha256=?2,recovery_checkpoint_id=?3
                 WHERE id=?4 AND state IN ('local','remote_verified','remote_only')",
                rusqlite::params![
                    shard.object.key,
                    shard.object.sha256,
                    document.checkpoint_id,
                    shard.id
                ],
            )? == 1,
            "restored checkpoint shard is missing from its SQLite catalog"
        );
    }
    transaction.commit()?;
    connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
    File::open(path)?.sync_all()?;
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 32 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

pub fn checkpoint_key(id: &str) -> String {
    format!("checkpoints/{id}.json")
}

pub async fn read_checkpoint(
    store: &dyn ObjectStore,
    id: &str,
    installation: &str,
) -> Result<CheckpointDocument> {
    uuid::Uuid::parse_str(id)?;
    let bytes = store
        .get_small(&checkpoint_key(id), MAX_CONTROL_BYTES)
        .await?;
    let document: CheckpointDocument = serde_json::from_slice(&bytes)?;
    validate_checkpoint(&document)?;
    ensure!(
        document.checkpoint_id == id && document.cut.installation_id == installation,
        "recovery checkpoint identity mismatch"
    );
    Ok(document)
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
    let mut replay_keys = BTreeSet::new();
    for reference in &document.cut.replay_blobs {
        validate_object(reference)?;
        ensure!(
            reference.key == super::replay::key(&reference.sha256)?
                && reference.size <= (crate::sentry::replay::MAX_RECORDING_BYTES + 65536) as u64
                && replay_keys.insert(&reference.key),
            "invalid checkpoint Replay reference"
        );
    }
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
                    && matches!(
                        shard.state.as_str(),
                        "local" | "remote_verified" | "remote_only"
                    )
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

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

struct RemoveDirectory(PathBuf);

impl Drop for RemoveDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db,
        model::Boundary,
        search::active::ActiveShard,
        storage::{
            archive,
            checkpoint::{CheckpointShard, PinnedSnapshot, SnapshotLimits},
            manifest::ShardStats,
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
        list_pages: Arc<Mutex<Option<Vec<ObjectPage>>>>,
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
            if let Some(pages) = self.list_pages.lock().unwrap().as_mut() {
                return Ok(pages.remove(0));
            }
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
            replay_files: Vec::new(),
            document: CheckpointDocument {
                format_version: 1,
                checkpoint_id: checkpoint_id.clone(),
                sequence: 7,
                created_at_us: 2,
                cut: CheckpointCut {
                    replay_blobs: Vec::new(),
                    replay_revision: 0,
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

    fn restorable_candidate(
        root: &Path,
        installation_id: &str,
        sequence: u64,
    ) -> Result<LocalCheckpoint> {
        std::fs::create_dir_all(root)?;
        let shard_id = uuid::Uuid::new_v4().to_string();
        let shard_source = root.join("sealed");
        std::fs::create_dir(&shard_source)?;
        let mut active = ActiveShard::create(
            &shard_source,
            installation_id,
            &shard_id,
            Boundary::default(),
        )
        .expect("create restore fixture shard");
        active
            .publish(Boundary::default())
            .expect("publish restore fixture shard");
        active
            .seal(
                &shard_source,
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
            )
            .expect("seal restore fixture shard");
        let archive_path = root.join("shard.tar.gz");
        let archive = archive::create(
            &shard_source,
            &archive_path,
            installation_id,
            &shard_id,
            || false,
        )
        .expect("archive restore fixture shard");

        let database_path = root.join("source.db");
        let database = db::open(&database_path)?;
        let generation = uuid::Uuid::new_v4().to_string();
        database.execute(
            "INSERT INTO runtime_state(singleton,installation_id,storage_generation,next_ingest_seq)
             VALUES(1,?1,?2,1)",
            rusqlite::params![installation_id, generation],
        )
        .expect("initialize restore runtime state");
        database.execute(
            "INSERT INTO users(id,email,password_hash,role,is_active,created_at_us,updated_at_us)
             VALUES(1,'restore@example.test','hash','admin',1,1,1)",
            [],
        )
        .expect("seed restore user");
        database.execute(
            "INSERT INTO sessions(token_hash,user_id,expires_at_us,created_at_us,last_seen_at_us)
             VALUES('old-session',1,100,1,1)",
            [],
        )
        .expect("seed restore session");
        database
            .execute(
                "INSERT INTO settings(key,value_json,updated_at_us)
             VALUES('token_hmac_v1','[1,2,3]',1)",
                [],
            )
            .expect("seed restore signing key");
        database
            .execute(
                "INSERT INTO shards(id,schema_version,format_version,tokenizer_version,state,
                last_applied_inbox_id,record_count,size_bytes,created_at_us,sealed_at_us)
             VALUES(?1,1,?2,1,'local',0,0,?3,1,1)",
                rusqlite::params![
                    shard_id,
                    crate::db::shards::FORMAT_VERSION,
                    i64::try_from(
                        crate::storage::manifest::local_size(
                            &shard_source,
                            &crate::storage::manifest::verify(
                                &shard_source,
                                installation_id,
                                &shard_id
                            )
                            .expect("verify restore fixture manifest"),
                        )
                        .expect("measure restore fixture shard")
                    )
                    .expect("fixture shard size fits i64")
                ],
            )
            .expect("seed restore shard catalog");
        drop(database);
        let checkpoint_id = uuid::Uuid::new_v4().to_string();
        let snapshot_path = root.join("snapshot.db");
        let snapshot = PinnedSnapshot::open(&database_path)
            .expect("open pinned restore fixture")
            .backup_to(&snapshot_path, SnapshotLimits::default(), || false)
            .expect("create restore fixture snapshot");
        Ok(LocalCheckpoint {
            replay_files: Vec::new(),
            document: CheckpointDocument {
                format_version: 1,
                checkpoint_id: checkpoint_id.clone(),
                sequence,
                created_at_us: i64::try_from(sequence)?,
                cut: snapshot.cut,
                snapshot: ObjectReference {
                    key: format!("snapshots/{checkpoint_id}.db"),
                    size: snapshot.size,
                    sha256: snapshot.sha256,
                },
                shards: vec![ShardReference {
                    id: shard_id.clone(),
                    object: ObjectReference {
                        key: format!("shards/{shard_id}.tar.gz"),
                        size: archive.size,
                        sha256: archive.sha256,
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
        assert_eq!(read_installation(&store).await?, None);
        assert!(ensure_installation(&store, &expected, false).await.is_err());
        assert!(store.keys().is_empty());
        assert_eq!(
            ensure_installation(&store, &expected, true).await?,
            expected
        );
        assert_eq!(read_installation(&store).await?, Some(expected.clone()));
        let other = installation(&uuid::Uuid::new_v4().to_string());
        assert!(ensure_installation(&store, &other, false).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn object_listing_requires_advancing_nonempty_continuations() {
        let object = ObjectMetadata {
            key: "one".into(),
            size: 1,
            etag: None,
        };
        let store = MemoryStore::default();
        *store.list_pages.lock().unwrap() = Some(vec![
            ObjectPage {
                objects: vec![object.clone()],
                continuation: Some("next".into()),
            },
            ObjectPage {
                objects: vec![object],
                continuation: None,
            },
        ]);
        assert_eq!(list_all(&store).await.expect("paged list").len(), 2);

        for continuation in ["", "same"] {
            let store = MemoryStore::default();
            *store.list_pages.lock().unwrap() = Some(vec![
                ObjectPage {
                    objects: Vec::new(),
                    continuation: Some(continuation.into()),
                },
                ObjectPage {
                    objects: Vec::new(),
                    continuation: Some(continuation.into()),
                },
            ]);
            assert!(list_all(&store).await.is_err());
        }
    }

    #[tokio::test]
    async fn malformed_or_missing_control_objects_fail_closed_without_candidates() {
        let installation_id = uuid::Uuid::new_v4().to_string();
        let malformed = MemoryStore::default();
        malformed
            .objects
            .lock()
            .unwrap()
            .insert("installation.json".into(), b"{".to_vec());
        assert!(
            ensure_installation(&malformed, &installation(&installation_id), false)
                .await
                .is_err()
        );
        assert!(read_installation(&malformed).await.is_err());
        let root = tempfile::tempdir().expect("malformed control directory");
        let candidate = candidate(root.path(), &installation_id).expect("local checkpoint");
        assert!(publish(&malformed, &candidate).await.is_err());
        assert!(
            checkpoint_candidates(&malformed, &installation_id)
                .await
                .is_err()
        );

        let corrupt_checkpoint = MemoryStore::default();
        corrupt_checkpoint.objects.lock().unwrap().extend([
            (
                "installation.json".into(),
                serde_json::to_vec(&installation(&installation_id))
                    .expect("installation serialization"),
            ),
            ("checkpoints/corrupt.json".into(), b"{".to_vec()),
        ]);
        assert!(
            checkpoint_candidates(&corrupt_checkpoint, &installation_id)
                .await
                .expect("ignore corrupt checkpoint")
                .is_empty()
        );

        let missing_checkpoint = MemoryStore::default();
        missing_checkpoint.objects.lock().unwrap().insert(
            "installation.json".into(),
            serde_json::to_vec(&installation(&installation_id))
                .expect("installation serialization"),
        );
        *missing_checkpoint.list_pages.lock().unwrap() = Some(vec![ObjectPage {
            objects: vec![
                ObjectMetadata {
                    key: "installation.json".into(),
                    size: 1,
                    etag: None,
                },
                ObjectMetadata {
                    key: "checkpoints/missing.json".into(),
                    size: 1,
                    etag: None,
                },
            ],
            continuation: None,
        }]);
        assert!(
            checkpoint_candidates(&missing_checkpoint, &installation_id)
                .await
                .expect("ignore missing checkpoint")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn restore_rejects_download_size_overflow_before_network_io() {
        let root = tempfile::tempdir().expect("restore overflow directory");
        let installation_id = uuid::Uuid::new_v4().to_string();
        let mut candidate = candidate(root.path(), &installation_id).expect("local checkpoint");
        candidate.document.snapshot.size = u64::MAX;
        let attempt = root.path().join("attempt");
        std::fs::create_dir(&attempt).expect("restore attempt");
        let cancelled: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(|| false);
        assert!(
            restore_candidate(
                &MemoryStore::default(),
                &candidate.document,
                &attempt,
                RestoreLimits {
                    snapshot_bytes: u64::MAX,
                    shard_archive_bytes: u64::MAX,
                    shard_expanded_bytes: u64::MAX,
                    total_download_bytes: u64::MAX,
                },
                &cancelled,
            )
            .await
            .is_err()
        );
    }

    #[test]
    fn snapshot_validation_rejects_runtime_types_and_missing_catalog() {
        let installation_id = uuid::Uuid::new_v4().to_string();
        let typed_root = tempfile::tempdir().expect("typed snapshot root");
        let typed = restorable_candidate(typed_root.path(), &installation_id, 1)
            .expect("typed snapshot fixture");
        let database =
            rusqlite::Connection::open(&typed.snapshot_path).expect("open typed snapshot");
        database
            .execute(
                "UPDATE runtime_state SET installation_id=x'80' WHERE singleton=1",
                [],
            )
            .expect("damage runtime identity type");
        drop(database);
        assert!(validate_snapshot(&typed.snapshot_path, &typed.document.cut).is_err());

        let missing_root = tempfile::tempdir().expect("missing catalog snapshot root");
        let missing = restorable_candidate(missing_root.path(), &installation_id, 2)
            .expect("missing catalog snapshot fixture");
        let database =
            rusqlite::Connection::open(&missing.snapshot_path).expect("open missing snapshot");
        database
            .execute_batch("PRAGMA foreign_keys=OFF; DROP TABLE shards")
            .expect("drop snapshot shard catalog");
        drop(database);
        assert!(validate_snapshot(&missing.snapshot_path, &missing.document.cut).is_err());
    }

    #[tokio::test]
    async fn checkpoint_is_published_only_after_every_data_object() -> Result<()> {
        let root = tempfile::tempdir()?;
        let installation_id = uuid::Uuid::new_v4().to_string();
        let mut candidate = candidate(root.path(), &installation_id)?;
        let replay_bytes = b"immutable replay".to_vec();
        let replay_hash = sha256(&replay_bytes);
        let replay_key = format!("replay-blobs/{replay_hash}.zlib");
        let replay_path = root.path().join("replay.zlib");
        std::fs::write(&replay_path, &replay_bytes)?;
        candidate.document.cut.replay_blobs = vec![ObjectReference {
            key: replay_key.clone(),
            size: replay_bytes.len() as u64,
            sha256: replay_hash,
        }];
        candidate.replay_files = vec![(replay_key.clone(), replay_path)];
        let store = MemoryStore::default();
        ensure_installation(&store, &installation(&installation_id), true).await?;
        let latest = publish(&store, &candidate).await?;
        assert_eq!(latest.checkpoint_id, candidate.document.checkpoint_id);
        assert_eq!(publish(&store, &candidate).await?, latest);
        let mut next = candidate.clone();
        next.document.checkpoint_id = uuid::Uuid::new_v4().to_string();
        next.document.sequence += 1;
        next.document.snapshot.key = format!("snapshots/{}.db", next.document.checkpoint_id);
        publish(&store, &next).await?;
        assert_eq!(
            store
                .writes()
                .iter()
                .filter(|key| *key == &replay_key)
                .count(),
            1,
            "completed checkpoint reuses the immutable Replay object"
        );
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
        ensure_installation(&failed, &installation(&installation_id), true).await?;
        failed.fail_on(&candidate.document.snapshot.key);
        assert!(publish(&failed, &candidate).await.is_err());
        assert!(!failed.keys().contains(&checkpoint));
        assert!(!failed.keys().contains(&"latest.json".to_owned()));
        let writes_before = store.writes();
        candidate.document.cut.replay_blobs = (0..25_000)
            .map(|n| {
                let hash = format!("{n:064x}");
                ObjectReference {
                    key: format!("replay-blobs/{hash}.zlib"),
                    size: 1,
                    sha256: hash,
                }
            })
            .collect();
        let error = publish(&store, &candidate).await.unwrap_err();
        assert!(error.to_string().contains("control document limit"));
        assert_eq!(
            store.writes(),
            writes_before,
            "oversized checkpoint cannot publish a false recovery point"
        );
        Ok(())
    }

    #[tokio::test]
    async fn restore_falls_back_without_mixing_incomplete_candidates() -> Result<()> {
        let root = tempfile::tempdir()?;
        let installation_id = uuid::Uuid::new_v4().to_string();
        let store = MemoryStore::default();
        ensure_installation(&store, &installation(&installation_id), true).await?;
        let older = restorable_candidate(&root.path().join("older"), &installation_id, 1)?;
        publish(&store, &older).await?;
        let newer = restorable_candidate(&root.path().join("newer"), &installation_id, 2)?;
        publish(&store, &newer).await?;
        store.objects.lock().unwrap().insert(
            newer.document.snapshot.key.clone(),
            b"corrupt snapshot".to_vec(),
        );

        let data_dir = root.path().join("data");
        std::fs::create_dir(&data_dir)?;
        std::fs::create_dir(data_dir.join("shards"))?;
        std::fs::write(data_dir.join("shards/orphan"), b"old local state")?;
        std::fs::create_dir(data_dir.join("replay-blobs"))?;
        std::fs::write(data_dir.join("replay-blobs/orphan"), b"old replay state")?;
        std::fs::write(data_dir.join("meta.db-wal"), b"old wal")?;
        std::fs::write(data_dir.join("meta.db-shm"), b"old shm")?;
        let destination = data_dir.join(".prepared");
        let restored = prepare_restore(
            &store,
            &installation_id,
            &destination,
            RestoreLimits::default(),
            || false,
        )
        .await?;
        assert_eq!(restored.document.sequence, 1);
        assert!(destination.join("meta.db").is_file());
        assert!(
            destination
                .join("shards")
                .join(&older.document.shards[0].id)
                .is_dir()
        );
        assert!(
            !destination
                .join("shards")
                .join(&newer.document.shards[0].id)
                .exists()
        );
        let old_generation = restored.document.cut.storage_generation.clone();
        install_prepared(&data_dir, restored)?;
        assert!(data_dir.join("meta.db").is_file());
        assert!(
            data_dir
                .join("shards")
                .join(&older.document.shards[0].id)
                .is_dir()
        );
        let restored_db = crate::db::open_reader(&data_dir.join("meta.db"))?;
        assert_eq!(
            restored_db.query_row("SELECT count(*) FROM sessions", [], |row| {
                row.get::<_, i64>(0)
            })?,
            0
        );
        assert_eq!(
            restored_db
                .query_row(
                    "SELECT count(*) FROM settings WHERE key='token_hmac_v1'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("read restored signing key count"),
            0
        );
        assert_ne!(
            restored_db
                .query_row(
                    "SELECT storage_generation FROM runtime_state WHERE singleton=1",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .expect("read restored generation"),
            old_generation
        );
        assert!(data_dir.join("quarantine").is_dir());

        let cancelled = root.path().join("cancelled");
        assert!(
            prepare_restore(
                &store,
                &installation_id,
                &cancelled,
                RestoreLimits::default(),
                || true,
            )
            .await
            .is_err()
        );
        assert!(!cancelled.exists());
        Ok(())
    }
}
