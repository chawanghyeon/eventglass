//! Immutable content-addressed Replay recordings; SQLite owns references, not raw blobs.
use super::remote::{ObjectReference, sha256};
use crate::sentry::replay::MAX_RECORDING_BYTES;
use anyhow::{Result, ensure};
use flate2::{Compression, read::ZlibDecoder, write::ZlibEncoder};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

pub fn key(hash: &str) -> Result<String> {
    ensure!(
        hash.len() == 64
            && hash
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
        "invalid replay blob hash"
    );
    Ok(format!("replay-blobs/{hash}.zlib"))
}

pub fn path(root: &Path, hash: &str) -> Result<PathBuf> {
    Ok(root.join(key(hash)?))
}

/// Publish fully fsynced bytes before an acceptance transaction references them.
/// A failed transaction leaves an unreferenced immutable file, reclaimed by local retention or at startup.
pub fn write(root: &Path, bytes: &[u8]) -> Result<ObjectReference> {
    ensure!(
        bytes.len() <= MAX_RECORDING_BYTES,
        "recording exceeds blob limit"
    );
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(bytes)?;
    let compressed = encoder.finish()?;
    let hash = sha256(&compressed);
    let directory = root.join("replay-blobs");
    fs::create_dir_all(&directory)?;
    File::open(root)?.sync_all()?;
    let destination = path(root, &hash)?;
    if destination.exists() {
        ensure!(
            fs::read(&destination)? == compressed,
            "existing replay blob is corrupt"
        );
    } else {
        let temporary = directory.join(format!(".{}.tmp", uuid::Uuid::new_v4()));
        let guard = RemoveFile(temporary.clone());
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(&compressed)?;
        file.sync_all()?;
        fs::rename(&temporary, &destination)?;
        File::open(&directory)?.sync_all()?;
        drop(guard);
    }
    Ok(ObjectReference {
        key: key(&hash)?,
        size: compressed.len() as u64,
        sha256: hash,
    })
}

struct RemoveFile(PathBuf);

impl Drop for RemoveFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

pub fn read(root: &Path, reference: &ObjectReference) -> Result<Vec<u8>> {
    ensure!(
        reference.key == key(&reference.sha256)?
            && reference.size <= (MAX_RECORDING_BYTES + 65536) as u64,
        "invalid replay blob reference"
    );
    let file = File::open(root.join(&reference.key))?;
    ensure!(
        file.metadata()?.len() == reference.size,
        "replay blob size mismatch"
    );
    let mut compressed = Vec::new();
    file.take(reference.size + 1).read_to_end(&mut compressed)?;
    ensure!(
        sha256(&compressed) == reference.sha256,
        "replay blob checksum mismatch"
    );
    let mut output = Vec::new();
    ZlibDecoder::new(compressed.as_slice())
        .take((MAX_RECORDING_BYTES + 1) as u64)
        .read_to_end(&mut output)?;
    ensure!(
        output.len() <= MAX_RECORDING_BYTES,
        "replay blob decompression limit"
    );
    Ok(output)
}
