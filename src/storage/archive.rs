//! Streaming, manifest-bound sealed-shard archives and atomic hydration.

use anyhow::{Context, Result, ensure};
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
};

use super::manifest::{self, Manifest};

const COPY_BUFFER_BYTES: usize = 32 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveArtifact {
    pub size: u64,
    pub sha256: String,
}

pub fn create(
    source: &Path,
    destination: &Path,
    installation: &str,
    shard: &str,
    mut cancelled: impl FnMut() -> bool,
) -> Result<ArchiveArtifact> {
    ensure!(!destination.exists(), "archive destination already exists");
    let manifest = manifest::verify(source, installation, shard)?;
    let parent = destination.parent().context("archive destination parent")?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(".archive-{}.tmp", uuid::Uuid::new_v4()));
    let guard = RemoveFile(temporary.clone());
    let output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    let encoder = GzEncoder::new(output, Compression::fast());
    let mut archive = tar::Builder::new(encoder);
    archive.mode(tar::HeaderMode::Deterministic);

    append_file(
        &mut archive,
        source.join(manifest::NAME),
        manifest::NAME,
        &mut cancelled,
    )?;
    for entry in &manifest.files {
        append_file(
            &mut archive,
            source.join(&entry.path),
            &entry.path,
            &mut cancelled,
        )?;
    }
    let encoder = archive.into_inner()?;
    let output = encoder.finish()?;
    output.sync_all()?;
    drop(output);

    let artifact = hash_file(&temporary, &mut cancelled)?;
    fs::rename(&temporary, destination)?;
    File::open(parent)?.sync_all()?;
    std::mem::forget(guard);
    Ok(artifact)
}

pub fn hydrate(
    source: &Path,
    destination: &Path,
    installation: &str,
    shard: &str,
    max_expanded_bytes: u64,
    mut cancelled: impl FnMut() -> bool,
) -> Result<Manifest> {
    ensure!(!destination.exists(), "hydrate destination already exists");
    let parent = destination.parent().context("hydrate destination parent")?;
    fs::create_dir_all(parent)?;
    let staging = parent.join(format!(".hydrate-{}.tmp", uuid::Uuid::new_v4()));
    fs::create_dir(&staging)?;
    let guard = RemoveDirectory(staging.clone());
    let mut archive = tar::Archive::new(GzDecoder::new(File::open(source)?));
    let mut names = BTreeSet::new();
    let mut expanded = 0u64;

    for item in archive.entries()? {
        ensure!(!cancelled(), "archive hydration cancelled");
        let mut entry = item?;
        ensure!(
            entry.header().entry_type().is_file(),
            "archive entry is not a regular file"
        );
        let path = entry.path()?;
        ensure!(path.components().count() == 1, "unsafe archive path");
        let name = match path.components().next() {
            Some(Component::Normal(name)) => name
                .to_str()
                .context("archive filename is not UTF-8")?
                .to_owned(),
            _ => anyhow::bail!("unsafe archive path"),
        };
        ensure!(safe_name(&name), "unsafe archive filename");
        ensure!(names.insert(name.clone()), "duplicate archive path");
        let declared = entry.header().size()?;
        expanded = expanded
            .checked_add(declared)
            .context("archive expanded size overflow")?;
        ensure!(
            expanded <= max_expanded_bytes,
            "archive exceeds expanded size limit"
        );
        let path = staging.join(name);
        let mut output = OpenOptions::new().write(true).create_new(true).open(path)?;
        let copied = copy_exact(&mut entry, &mut output, declared, &mut cancelled)?;
        ensure!(copied == declared, "archive entry size mismatch");
        output.sync_all()?;
    }
    ensure!(
        names.contains(manifest::NAME),
        "archive manifest is missing"
    );
    let verified = manifest::verify(&staging, installation, shard)?;
    let expected = std::iter::once(manifest::NAME.to_owned())
        .chain(verified.files.iter().map(|file| file.path.clone()))
        .collect::<BTreeSet<_>>();
    ensure!(names == expected, "archive file set differs from manifest");
    File::open(&staging)?.sync_all()?;
    fs::rename(&staging, destination)?;
    File::open(parent)?.sync_all()?;
    std::mem::forget(guard);
    Ok(verified)
}

fn append_file<W: Write>(
    archive: &mut tar::Builder<W>,
    path: PathBuf,
    name: &str,
    cancelled: &mut dyn FnMut() -> bool,
) -> Result<()> {
    ensure!(safe_name(name), "unsafe archive filename");
    let metadata = fs::symlink_metadata(&path)?;
    ensure!(
        metadata.file_type().is_file(),
        "archive source is not a regular file"
    );
    let mut header = tar::Header::new_gnu();
    header.set_size(metadata.len());
    header.set_mode(0o600);
    header.set_mtime(0);
    header.set_uid(0);
    header.set_gid(0);
    header.set_cksum();
    let reader = CancelReader {
        inner: File::open(path)?,
        cancelled,
    };
    archive.append_data(&mut header, name, reader)?;
    Ok(())
}

fn safe_name(name: &str) -> bool {
    name == manifest::NAME
        || (!name.is_empty()
            && !name.contains(['\\', ':'])
            && !name.ends_with(".lock")
            && !name.ends_with(".tmp")
            && Path::new(name).components().count() == 1
            && matches!(
                Path::new(name).components().next(),
                Some(Component::Normal(_))
            ))
}

fn copy_exact(
    input: &mut dyn Read,
    output: &mut dyn Write,
    expected: u64,
    cancelled: &mut dyn FnMut() -> bool,
) -> Result<u64> {
    let mut copied = 0u64;
    let mut buffer = [0u8; COPY_BUFFER_BYTES];
    while copied < expected {
        ensure!(!cancelled(), "archive hydration cancelled");
        let remaining = usize::try_from((expected - copied).min(COPY_BUFFER_BYTES as u64))?;
        let read = input.read(&mut buffer[..remaining])?;
        if read == 0 {
            break;
        }
        output.write_all(&buffer[..read])?;
        copied += read as u64;
    }
    Ok(copied)
}

fn hash_file(path: &Path, cancelled: &mut dyn FnMut() -> bool) -> Result<ArchiveArtifact> {
    let mut input = File::open(path)?;
    let size = input.metadata()?.len();
    let mut sha = Sha256::new();
    let mut hashed = 0u64;
    let mut buffer = [0u8; COPY_BUFFER_BYTES];
    loop {
        ensure!(!cancelled(), "archive creation cancelled");
        let read = input.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        sha.update(&buffer[..read]);
        hashed += read as u64;
    }
    ensure!(hashed == size, "archive changed while hashing");
    Ok(ArchiveArtifact {
        size,
        sha256: format!("{:x}", sha.finalize()),
    })
}

struct CancelReader<'a> {
    inner: File,
    cancelled: &'a mut dyn FnMut() -> bool,
}

impl Read for CancelReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if (self.cancelled)() {
            return Err(std::io::Error::other("archive creation cancelled"));
        }
        self.inner.read(buffer)
    }
}

struct RemoveFile(PathBuf);

impl Drop for RemoveFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

struct RemoveDirectory(PathBuf);

impl Drop for RemoveDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
