//! Native immutable shard files and their durable integrity manifest.
pub mod archive;
pub mod backup;
pub mod budget;
pub mod checkpoint;
pub mod cold;
pub mod manifest;
pub mod registry;
pub mod remote;
pub mod replay;
pub mod replay_maintenance;
pub mod s3;

/// Called only while holding the exclusive data-directory lock, before workers start.
pub(crate) fn reclaim_interrupted_temporary_work(root: &std::path::Path) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        if !is_interrupted_name(name.to_str()) {
            continue;
        }
        let kind = entry.file_type()?;
        if kind.is_dir() {
            std::fs::remove_dir_all(entry.path())?;
        } else if kind.is_file() {
            std::fs::remove_file(entry.path())?;
        }
        // Never follow unexpected links or remove arbitrary unknown entries.
    }
    std::fs::File::open(root)?.sync_all()?;
    Ok(())
}

fn is_interrupted_name(name: Option<&str>) -> bool {
    let Some(name) = name else {
        return false;
    };
    let single = [".backup-", ".restore-"].iter().any(|prefix| {
        name.strip_prefix(prefix)
            .and_then(|value| value.strip_suffix(".tmp"))
            .is_some_and(|value| uuid::Uuid::parse_str(value).is_ok())
    });
    let double = [(".cold-", ".tar.gz"), (".evict-", ".tmp")]
        .iter()
        .any(|(prefix, suffix)| {
            name.strip_prefix(prefix)
                .and_then(|value| value.strip_suffix(suffix))
                .is_some_and(|value| {
                    value.is_ascii()
                        && value.len() == 73
                        && value.as_bytes()[36] == b'-'
                        && uuid::Uuid::parse_str(&value[..36]).is_ok()
                        && uuid::Uuid::parse_str(&value[37..]).is_ok()
                })
        });
    single || double
}

#[cfg(test)]
mod cleanup_tests {
    #[test]
    fn interrupted_work_is_removed_without_touching_recovery_or_unknown_data() -> anyhow::Result<()>
    {
        let root = tempfile::tempdir()?;
        let id = uuid::Uuid::new_v4();
        let backup = root.path().join(format!(".backup-{id}.tmp"));
        std::fs::create_dir(&backup)?;
        std::fs::write(backup.join("partial"), b"partial")?;
        let cold = root.path().join(format!(".cold-{id}-{id}.tar.gz"));
        std::fs::write(&cold, b"partial")?;
        let restore_file = root.path().join(format!(".restore-{id}.tmp"));
        std::fs::write(&restore_file, b"partial")?;
        let prepared = root.path().join(format!(".restore-{id}"));
        std::fs::create_dir(&prepared)?;
        let unknown = root.path().join(".backup-user-data.tmp");
        std::fs::write(&unknown, b"preserve")?;
        super::reclaim_interrupted_temporary_work(root.path())?;
        assert!(!backup.exists());
        assert!(!cold.exists());
        assert!(!restore_file.exists());
        assert!(prepared.exists());
        assert!(unknown.exists());
        Ok(())
    }

    #[test]
    fn non_utf8_names_are_never_classified_as_interrupted_work() {
        assert!(!super::is_interrupted_name(None));
    }

    #[cfg(unix)]
    #[test]
    fn interrupted_named_symlinks_are_preserved() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let target = root.path().join("target");
        std::fs::write(&target, b"preserve")?;
        let link = root
            .path()
            .join(format!(".backup-{}.tmp", uuid::Uuid::new_v4()));
        std::os::unix::fs::symlink(&target, &link)?;
        super::reclaim_interrupted_temporary_work(root.path())?;
        assert!(link.symlink_metadata()?.file_type().is_symlink());
        assert_eq!(std::fs::read(target)?, b"preserve");
        Ok(())
    }
}

pub mod observed;
