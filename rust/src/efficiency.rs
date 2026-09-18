//! Bounded, process-local observations. Never part of acceptance or recovery state.
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use serde::Serialize;

#[derive(Default)]
struct Counter(AtomicU64);

impl Counter {
    fn add(&self, value: u64, incomplete: &AtomicBool) {
        let previous = self
            .0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                Some(old.saturating_add(value))
            })
            .expect("counter update always supplies a value");
        if previous.checked_add(value).is_none() {
            incomplete.store(true, Ordering::Relaxed);
        }
    }

    fn read(&self) -> String {
        self.0.load(Ordering::Relaxed).to_string()
    }
}

#[derive(Clone, Copy)]
pub enum Operation {
    List,
    Read,
    Create,
    Write,
    Upload,
    Download,
}

impl Operation {
    const ALL: [Self; 6] = [
        Self::List,
        Self::Read,
        Self::Create,
        Self::Write,
        Self::Upload,
        Self::Download,
    ];
    fn name(self) -> &'static str {
        match self {
            Self::List => "list",
            Self::Read => "read_metadata",
            Self::Create => "conditional_create",
            Self::Write => "write_metadata",
            Self::Upload => "upload",
            Self::Download => "download",
        }
    }
}

#[derive(Default)]
struct OperationCounters {
    started: Counter,
    succeeded: Counter,
    failed: Counter,
    cancelled: Counter,
    completed_read_bytes: Counter,
}

pub struct Efficiency {
    epoch: String,
    started: Instant,
    incomplete: AtomicBool,
    body_bytes: Counter,
    decoded_bytes: Counter,
    local_reuses: Counter,
    hydrated: Counter,
    evicted: Counter,
    operations: [OperationCounters; 6],
}

impl Default for Efficiency {
    fn default() -> Self {
        Self {
            epoch: uuid::Uuid::new_v4().to_string(),
            started: Instant::now(),
            incomplete: AtomicBool::new(false),
            body_bytes: Counter::default(),
            decoded_bytes: Counter::default(),
            local_reuses: Counter::default(),
            hydrated: Counter::default(),
            evicted: Counter::default(),
            operations: std::array::from_fn(|_| OperationCounters::default()),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Snapshot {
    pub process_epoch: String,
    pub uptime_ms: String,
    pub incomplete: bool,
    pub observed_body_bytes: String,
    pub observed_decoded_bytes: String,
    pub local_reuses: String,
    pub hydrated_shards: String,
    pub evicted_shards: String,
    pub remote: std::collections::BTreeMap<&'static str, RemoteSnapshot>,
}

#[derive(Debug, Serialize)]
pub struct RemoteSnapshot {
    pub started: String,
    pub succeeded: String,
    pub failed: String,
    pub cancelled: String,
    /// Bytes from completed reads only. Partial/error bodies and SDK retries are unknown.
    pub completed_read_bytes: String,
}

impl Efficiency {
    pub fn observe_body(&self, bytes: usize) {
        self.body_bytes.add(bytes as u64, &self.incomplete);
    }
    pub fn observe_decoded(&self, bytes: usize) {
        self.decoded_bytes.add(bytes as u64, &self.incomplete);
    }
    pub fn reuse_local(&self) {
        self.local_reuses.add(1, &self.incomplete);
    }
    pub fn hydrated(&self) {
        self.hydrated.add(1, &self.incomplete);
    }
    pub fn evicted(&self) {
        self.evicted.add(1, &self.incomplete);
    }
    pub fn begin(&self, operation: Operation) -> Observation<'_> {
        let counts = &self.operations[operation as usize];
        counts.started.add(1, &self.incomplete);
        Observation {
            counts,
            incomplete: &self.incomplete,
            finished: false,
        }
    }
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            process_epoch: self.epoch.clone(),
            uptime_ms: self.started.elapsed().as_millis().to_string(),
            incomplete: self.incomplete.load(Ordering::Relaxed),
            observed_body_bytes: self.body_bytes.read(),
            observed_decoded_bytes: self.decoded_bytes.read(),
            local_reuses: self.local_reuses.read(),
            hydrated_shards: self.hydrated.read(),
            evicted_shards: self.evicted.read(),
            remote: Operation::ALL
                .into_iter()
                .map(|operation| {
                    let counts = &self.operations[operation as usize];
                    (
                        operation.name(),
                        RemoteSnapshot {
                            started: counts.started.read(),
                            succeeded: counts.succeeded.read(),
                            failed: counts.failed.read(),
                            cancelled: counts.cancelled.read(),
                            completed_read_bytes: counts.completed_read_bytes.read(),
                        },
                    )
                })
                .collect(),
        }
    }
}

pub struct Observation<'a> {
    counts: &'a OperationCounters,
    incomplete: &'a AtomicBool,
    finished: bool,
}

impl Observation<'_> {
    pub fn finish(mut self, success: bool, completed_read_bytes: u64) {
        if success {
            self.counts.succeeded.add(1, self.incomplete);
            self.counts
                .completed_read_bytes
                .add(completed_read_bytes, self.incomplete);
        } else {
            self.counts.failed.add(1, self.incomplete);
        }
        self.finished = true;
    }
}

impl Drop for Observation<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.counts.cancelled.add(1, self.incomplete);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshots_distinguish_outcomes_bytes_and_process_lifetimes() {
        let stats = Efficiency::default();
        stats.observe_body(100);
        stats.observe_decoded(300);
        stats.reuse_local();
        stats.hydrated();
        stats.evicted();
        for operation in Operation::ALL {
            stats.begin(operation).finish(true, 23);
            stats.begin(operation).finish(false, 999);
            drop(stats.begin(operation));
        }
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.observed_body_bytes, "100");
        assert_eq!(snapshot.observed_decoded_bytes, "300");
        assert_eq!(
            (
                snapshot.local_reuses.as_str(),
                snapshot.hydrated_shards.as_str(),
                snapshot.evicted_shards.as_str()
            ),
            ("1", "1", "1")
        );
        assert!(!snapshot.incomplete);
        for counts in snapshot.remote.values() {
            assert_eq!(
                (
                    counts.started.as_str(),
                    counts.succeeded.as_str(),
                    counts.failed.as_str(),
                    counts.cancelled.as_str(),
                    counts.completed_read_bytes.as_str()
                ),
                ("3", "1", "1", "1", "23")
            );
        }
        let reset = Efficiency::default().snapshot();
        assert_ne!(snapshot.process_epoch, reset.process_epoch);
        assert_eq!(reset.observed_body_bytes, "0");
        assert!(snapshot.uptime_ms.parse::<u128>().is_ok());
        assert_eq!(
            serde_json::to_value(snapshot).unwrap()["remote"]["download"]["completed_read_bytes"],
            "23"
        );
    }

    #[test]
    fn saturation_never_wraps_and_concurrent_updates_are_not_lost() {
        let stats = Efficiency::default();
        stats.body_bytes.0.store(u64::MAX - 1, Ordering::Relaxed);
        stats.observe_body(5);
        assert_eq!(stats.snapshot().observed_body_bytes, u64::MAX.to_string());
        assert!(stats.snapshot().incomplete);
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    for _ in 0..1000 {
                        stats.reuse_local();
                    }
                });
            }
        });
        assert_eq!(stats.snapshot().local_reuses, "4000");
    }
}
