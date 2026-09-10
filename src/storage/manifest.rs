use crate::{
    model::Boundary,
    search::{active::CommitPayload, schema},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Component, Path},
};
use tantivy::Index;

pub const NAME: &str = "eventglass-manifest.json";
const MAX_MANIFEST_BYTES: u64 = 4 * 1024 * 1024;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileEntry {
    pub path: String,
    pub size: u64,
    pub sha256: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardStats {
    pub record_count: u64,
    pub min_timestamp_us: Option<i64>,
    pub max_timestamp_us: Option<i64>,
    pub min_received_at_us: Option<i64>,
    pub max_received_at_us: Option<i64>,
    pub min_ingest_seq: Option<i64>,
    pub max_ingest_seq: Option<i64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub manifest_version: u32,
    pub installation_id: String,
    pub shard_id: String,
    pub schema_version: u32,
    pub normalizer_version: u32,
    pub tokenizer_version: u32,
    pub tantivy_version: String,
    pub index_format_version: u32,
    pub boundary: Boundary,
    pub stats: ShardStats,
    pub sealed_at_us: i64,
    pub files: Vec<FileEntry>,
}
fn regular(path: &Path) -> Result<u64> {
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.file_type().is_file(),
        "manifest entry must be a regular file"
    );
    Ok(metadata.len())
}
fn safe_name(name: &str) -> bool {
    !name.is_empty()
        && !name.contains('\\')
        && !name.contains(':')
        && Path::new(name).components().count() == 1
        && matches!(
            Path::new(name).components().next(),
            Some(Component::Normal(_))
        )
        && name != NAME
        && !name.ends_with(".lock")
        && !name.ends_with(".tmp")
}
fn entry(root: &Path, name: String, sync: bool) -> Result<FileEntry> {
    ensure!(safe_name(&name), "unsafe native filename");
    let path = root.join(&name);
    let size = regular(&path)?;
    let mut file = File::open(path)?;
    let mut sha = Sha256::new();
    let mut buffer = [0u8; 32 * 1024];
    let mut read_size = 0u64;
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        sha.update(&buffer[..count]);
        read_size += count as u64;
    }
    ensure!(read_size == size, "native file changed while hashing");
    if sync {
        file.sync_all()?;
    }
    Ok(FileEntry {
        path: name,
        size,
        sha256: format!("{:x}", sha.finalize()),
    })
}
fn native_names(root: &Path, index: &Index) -> Result<BTreeSet<String>> {
    let mut names = BTreeSet::from(["meta.json".to_owned(), ".managed.json".to_owned()]);
    for segment in index.load_metas()?.segments {
        // Native metadata lists possible optional components as well as required ones.
        // Opening the native reader below validates required components; no directory glob.
        for path in segment.list_files() {
            match fs::symlink_metadata(root.join(&path)) {
                Ok(metadata) => {
                    ensure!(
                        metadata.file_type().is_file(),
                        "non-regular native component"
                    );
                    names.insert(
                        path.to_str()
                            .context("native filename encoding")?
                            .to_owned(),
                    );
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    Ok(names)
}
fn validate(manifest: &Manifest) -> Result<()> {
    ensure!(
        manifest.manifest_version == 1
            && manifest.schema_version == schema::APPLICATION_SCHEMA_VERSION
            && manifest.normalizer_version == 1
            && manifest.tokenizer_version == schema::TOKENIZER_VERSION
            && manifest.tantivy_version == tantivy::version_string()
            && manifest.index_format_version == tantivy::INDEX_FORMAT_VERSION,
        "unsupported shard manifest version"
    );
    uuid::Uuid::parse_str(&manifest.installation_id)?;
    uuid::Uuid::parse_str(&manifest.shard_id)?;
    ensure!(
        manifest.boundary.inbox_id >= 0
            && manifest.boundary.ingest_seq >= 0
            && (manifest.boundary.inbox_id == 0) == (manifest.boundary.ingest_seq == 0)
            && manifest.sealed_at_us >= 0,
        "invalid manifest boundary"
    );
    let stats = &manifest.stats;
    for pair in [
        (stats.min_timestamp_us, stats.max_timestamp_us),
        (stats.min_received_at_us, stats.max_received_at_us),
        (stats.min_ingest_seq, stats.max_ingest_seq),
    ] {
        ensure!(
            match pair {
                (None, None) => stats.record_count == 0,
                (Some(min), Some(max)) => stats.record_count > 0 && min <= max,
                _ => false,
            },
            "invalid manifest range"
        );
    }
    ensure!(
        stats.min_ingest_seq.is_none_or(|n| n > 0)
            && stats
                .max_ingest_seq
                .is_none_or(|n| n <= manifest.boundary.ingest_seq),
        "manifest sequence outside boundary"
    );
    let mut names = BTreeSet::new();
    for file in &manifest.files {
        ensure!(
            safe_name(&file.path)
                && names.insert(file.path.as_str())
                && file.sha256.len() == 64
                && file
                    .sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "invalid manifest file entry"
        );
    }
    ensure!(
        names.contains("meta.json") && names.contains(".managed.json"),
        "missing native metadata"
    );
    Ok(())
}

/// Caller has consumed the writer with wait_merging_threads. Never call on a live writer.
pub(crate) fn write(
    root: &Path,
    committed: &CommitPayload,
    stats: ShardStats,
    sealed_at_us: i64,
) -> Result<Manifest> {
    ensure!(!root.join(NAME).exists(), "sealed manifest already exists");
    let index = Index::open_in_dir(root)?;
    ensure!(index.schema() == schema::build(), "sealed schema mismatch");
    let actual: CommitPayload = serde_json::from_str(
        &index
            .load_metas()?
            .payload
            .context("missing native boundary")?,
    )?;
    ensure!(&actual == committed, "seal changed native boundary");
    ensure!(
        index.reader()?.searcher().num_docs() == stats.record_count,
        "seal record count mismatch"
    );
    let files = native_names(root, &index)?
        .into_iter()
        .map(|name| entry(root, name, true))
        .collect::<Result<Vec<_>>>()?;
    let manifest = Manifest {
        manifest_version: 1,
        installation_id: committed.installation_id.clone(),
        shard_id: committed.shard_id.clone(),
        schema_version: schema::APPLICATION_SCHEMA_VERSION,
        normalizer_version: 1,
        tokenizer_version: schema::TOKENIZER_VERSION,
        tantivy_version: tantivy::version_string().into(),
        index_format_version: tantivy::INDEX_FORMAT_VERSION,
        boundary: committed.boundary,
        stats,
        sealed_at_us,
        files,
    };
    validate(&manifest)?;
    let bytes = serde_json::to_vec_pretty(&manifest)?;
    ensure!(
        bytes.len() as u64 <= MAX_MANIFEST_BYTES,
        "manifest exceeds bound"
    );
    File::open(root)?.sync_all()?;
    let temporary = root.join(format!("{NAME}.{}.tmp", uuid::Uuid::new_v4()));
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    output.write_all(&bytes)?;
    output.sync_all()?;
    drop(output);
    crash_point("before_seal_manifest_rename");
    fs::rename(temporary, root.join(NAME))?;
    File::open(root)?.sync_all()?;
    crash_point("after_seal_manifest_rename");
    Ok(manifest)
}

pub fn verify(root: &Path, installation: &str, shard: &str) -> Result<Manifest> {
    let path = root.join(NAME);
    ensure!(
        regular(&path)? <= MAX_MANIFEST_BYTES,
        "manifest exceeds bound"
    );
    let manifest: Manifest =
        serde_json::from_reader(File::open(path)?.take(MAX_MANIFEST_BYTES + 1))?;
    validate(&manifest)?;
    ensure!(
        manifest.installation_id == installation && manifest.shard_id == shard,
        "manifest identity mismatch"
    );
    for expected in &manifest.files {
        ensure!(
            entry(root, expected.path.clone(), false)? == *expected,
            "native checksum mismatch"
        );
    }
    let index = Index::open_in_dir(root)?;
    ensure!(index.schema() == schema::build(), "sealed schema mismatch");
    let actual: CommitPayload = serde_json::from_str(
        &index
            .load_metas()?
            .payload
            .context("missing native boundary")?,
    )?;
    ensure!(
        actual.version == 1
            && actual.installation_id == installation
            && actual.shard_id == shard
            && actual.boundary == manifest.boundary,
        "sealed native identity mismatch"
    );
    ensure!(
        native_names(root, &index)?
            == manifest
                .files
                .iter()
                .map(|file| file.path.clone())
                .collect(),
        "native manifest file set mismatch"
    );
    ensure!(
        index.reader()?.searcher().num_docs() == manifest.stats.record_count,
        "sealed native count mismatch"
    );
    Ok(manifest)
}

pub fn local_size(root: &Path, manifest: &Manifest) -> Result<u64> {
    manifest
        .files
        .iter()
        .try_fold(regular(&root.join(NAME))?, |sum, file| {
            sum.checked_add(file.size)
                .context("sealed shard size overflow")
        })
}

pub fn active_size(root: &Path) -> Result<u64> {
    fs::read_dir(root)?.try_fold(0u64, |sum, entry| {
        let metadata = fs::symlink_metadata(entry?.path())?;
        ensure!(
            metadata.file_type().is_file(),
            "active shard contains a non-regular entry"
        );
        sum.checked_add(metadata.len())
            .context("active shard size overflow")
    })
}

#[inline]
fn crash_point(name: &str) {
    #[cfg(feature = "failpoints")]
    if std::env::var("EVENTGLASS_FAILPOINT").as_deref() == Ok(name) {
        std::process::exit(86);
    }
    #[cfg(not(feature = "failpoints"))]
    let _ = name;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_manifest() -> Manifest {
        Manifest {
            manifest_version: 1,
            installation_id: uuid::Uuid::new_v4().to_string(),
            shard_id: uuid::Uuid::new_v4().to_string(),
            schema_version: schema::APPLICATION_SCHEMA_VERSION,
            normalizer_version: 1,
            tokenizer_version: schema::TOKENIZER_VERSION,
            tantivy_version: tantivy::version_string().into(),
            index_format_version: tantivy::INDEX_FORMAT_VERSION,
            boundary: Boundary::default(),
            stats: ShardStats {
                record_count: 0,
                min_timestamp_us: None,
                max_timestamp_us: None,
                min_received_at_us: None,
                max_received_at_us: None,
                min_ingest_seq: None,
                max_ingest_seq: None,
            },
            sealed_at_us: 0,
            files: vec![
                FileEntry {
                    path: "meta.json".into(),
                    size: 0,
                    sha256: "0".repeat(64),
                },
                FileEntry {
                    path: ".managed.json".into(),
                    size: 0,
                    sha256: "0".repeat(64),
                },
            ],
        }
    }

    #[test]
    fn manifest_validation_rejects_unsafe_names_and_partial_ranges() {
        for name in [
            "",
            ".",
            "..",
            "../escape",
            "a/b",
            "a\\b",
            "a:b",
            NAME,
            "x.lock",
            "x.tmp",
        ] {
            assert!(!safe_name(name), "{name}");
        }
        assert!(safe_name("segment.store"));

        let mut manifest = valid_manifest();
        assert!(validate(&manifest).is_ok());
        manifest.stats.min_timestamp_us = Some(1);
        assert!(validate(&manifest).is_err());
        manifest.stats.max_timestamp_us = Some(0);
        assert!(validate(&manifest).is_err());
    }

    #[test]
    fn size_helpers_reject_missing_manifest_and_non_regular_active_entries() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let manifest = valid_manifest();
        assert!(local_size(directory.path(), &manifest).is_err());
        fs::create_dir(directory.path().join("nested"))?;
        assert!(active_size(directory.path()).is_err());
        Ok(())
    }
}
