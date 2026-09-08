//! Shared local-disk reservation admission for bounded temporary work.

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, ensure};

const MINIMUM_FREE_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub struct DiskStatus {
    pub total_bytes: u64,
    pub free_bytes: u64,
    pub reserved_bytes: u64,
    pub minimum_free_bytes: u64,
    pub ingest_accepting: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReserveError {
    Unavailable,
    Exhausted,
}

impl std::fmt::Display for ReserveError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable => formatter.write_str("disk capacity is unavailable"),
            Self::Exhausted => formatter.write_str("disk reserve would be crossed"),
        }
    }
}

impl std::error::Error for ReserveError {}

#[derive(Clone)]
pub struct DiskBudget {
    root: PathBuf,
    reserved: Arc<Mutex<u64>>,
}

pub struct Reservation {
    bytes: u64,
    reserved: Arc<Mutex<u64>>,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if let Ok(mut reserved) = self.reserved.lock() {
            *reserved = reserved.saturating_sub(self.bytes);
        }
    }
}

impl DiskBudget {
    pub fn new(root: &Path) -> Self {
        Self {
            root: root.to_owned(),
            reserved: Arc::new(Mutex::new(0)),
        }
    }

    pub fn reserve(&self, bytes: u64) -> Result<Reservation> {
        ensure!(bytes > 0, "disk reservation must be positive");
        let free = fs4::available_space(&self.root).map_err(|_| ReserveError::Unavailable)?;
        let total = fs4::total_space(&self.root).map_err(|_| ReserveError::Unavailable)?;
        let mut reserved = self
            .reserved
            .lock()
            .map_err(|_| ReserveError::Unavailable)?;
        admit(free, total, *reserved, bytes).map_err(anyhow::Error::from)?;
        *reserved = reserved
            .checked_add(bytes)
            .context("disk reservation counter overflow")?;
        Ok(Reservation {
            bytes,
            reserved: Arc::clone(&self.reserved),
        })
    }

    pub fn reserved_bytes(&self) -> Result<u64> {
        self.reserved
            .lock()
            .map(|value| *value)
            .map_err(|_| ReserveError::Unavailable.into())
    }

    pub fn status(&self) -> Result<DiskStatus> {
        let free = fs4::available_space(&self.root).map_err(|_| ReserveError::Unavailable)?;
        let total = fs4::total_space(&self.root).map_err(|_| ReserveError::Unavailable)?;
        let reserved = self.reserved_bytes()?;
        let minimum = MINIMUM_FREE_BYTES.max(total / 10);
        let next_ingest = 2 * crate::config::Limits::default().decoded_bytes as u64;
        Ok(DiskStatus {
            total_bytes: total,
            free_bytes: free,
            reserved_bytes: reserved,
            minimum_free_bytes: minimum,
            ingest_accepting: admit(free, total, reserved, next_ingest).is_ok(),
        })
    }
}

fn admit(free: u64, total: u64, reserved: u64, requested: u64) -> Result<(), ReserveError> {
    let floor = MINIMUM_FREE_BYTES.max(total / 10);
    let required = floor
        .checked_add(reserved)
        .and_then(|value| value.checked_add(requested))
        .ok_or(ReserveError::Exhausted)?;
    if free < required {
        return Err(ReserveError::Exhausted);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_preserves_absolute_and_percentage_floors_and_counts_concurrency() {
        let gib = 1024 * 1024 * 1024;
        assert_eq!(admit(gib, 4 * gib, 0, 512 * 1024 * 1024), Ok(()));
        assert_eq!(
            admit(gib - 1, 4 * gib, 0, 512 * 1024 * 1024),
            Err(ReserveError::Exhausted)
        );
        assert_eq!(admit(2 * gib, 20 * gib, 0, 1), Err(ReserveError::Exhausted));
        assert_eq!(admit(3 * gib, 20 * gib, gib / 2, gib / 2), Ok(()));
        assert_eq!(
            admit(3 * gib - 1, 20 * gib, gib / 2, gib / 2),
            Err(ReserveError::Exhausted)
        );
    }
}
