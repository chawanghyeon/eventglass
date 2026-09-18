use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tantivy::collector::Count;
use tantivy::indexer::NoMergePolicy;
use tantivy::query::AllQuery;
use tantivy::schema::{STORED, STRING, Schema};
use tantivy::{INDEX_FORMAT_VERSION, Index, doc};

#[derive(Debug, Serialize)]
struct ManifestFile {
    path: String,
    size: u64,
    sha256: String,
}

#[derive(Debug, Serialize)]
struct SealManifest {
    manifest_version: u32,
    shard_id: String,
    tantivy_version: String,
    index_format_version: u32,
    files: Vec<ManifestFile>,
}

fn is_native_file(path: &Path) -> bool {
    let name = path.file_name().unwrap().to_string_lossy();
    name != "observe-manifest.json"
        && name != "observe-manifest.json.tmp"
        && !name.ends_with(".lock")
}

fn sha256(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn native_snapshot(dir: &Path) -> Result<BTreeMap<PathBuf, (u64, String)>> {
    let mut result = BTreeMap::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && is_native_file(&path) {
            let relative = path.strip_prefix(dir)?.to_path_buf();
            result.insert(relative, (entry.metadata()?.len(), sha256(&path)?));
        }
    }
    Ok(result)
}

#[test]
fn g06_waited_merge_manifest_files_reopen_without_native_mutation() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut schema = Schema::builder();
    let record_id = schema.add_text_field("record_id", STRING | STORED);
    let schema = schema.build();
    let index = Index::create_in_dir(dir.path(), schema)?;
    let mut writer = index.writer_with_num_threads(1, 15_000_000)?;
    writer.set_merge_policy(Box::new(NoMergePolicy));
    for id in ["one", "two"] {
        writer.add_document(doc!(record_id => id))?;
        writer.commit()?;
    }
    let segment_ids = index.searchable_segment_ids()?;
    assert_eq!(segment_ids.len(), 2, "fixture must exercise a real merge");
    writer.merge(&segment_ids).wait()?;
    writer.wait_merging_threads()?;
    assert_eq!(index.searchable_segment_ids()?.len(), 1);

    let sealed_files = native_snapshot(dir.path())?;
    assert!(sealed_files.contains_key(Path::new("meta.json")));
    assert!(sealed_files.contains_key(Path::new(".managed.json")));
    assert!(sealed_files.keys().any(|path| path.extension().is_some()));

    let files = sealed_files
        .iter()
        .map(|(path, (size, digest))| ManifestFile {
            path: path.to_string_lossy().into_owned(),
            size: *size,
            sha256: digest.clone(),
        })
        .collect();
    let manifest = SealManifest {
        manifest_version: 1,
        shard_id: "contract-shard".into(),
        tantivy_version: tantivy::version_string().into(),
        index_format_version: INDEX_FORMAT_VERSION,
        files,
    };
    assert_eq!(manifest.tantivy_version, "tantivy v0.26.1, index_format v7");
    assert_eq!(manifest.index_format_version, 7);

    let temporary = dir.path().join("observe-manifest.json.tmp");
    let final_path = dir.path().join("observe-manifest.json");
    let mut output = File::create(&temporary)?;
    serde_json::to_writer_pretty(&mut output, &manifest)?;
    output.sync_all()?;
    fs::rename(&temporary, &final_path)?;
    File::open(dir.path())?.sync_all()?;

    drop(index);
    let reopened = Index::open_in_dir(dir.path())?;
    assert_eq!(reopened.reader()?.searcher().search(&AllQuery, &Count)?, 2);
    assert_eq!(native_snapshot(dir.path())?, sealed_files);
    assert!(final_path.is_file());
    Ok(())
}
