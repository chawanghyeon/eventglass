//! Observe the existing ObjectStore boundary without extra requests or file reads.
use std::{path::Path, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;

use super::s3::{ObjectMetadata, ObjectPage, ObjectStore};
use crate::efficiency::{Efficiency, Operation};

pub struct ObservedStore {
    inner: Arc<dyn ObjectStore>,
    stats: Arc<Efficiency>,
}

impl ObservedStore {
    pub fn new(inner: Arc<dyn ObjectStore>, stats: Arc<Efficiency>) -> Self {
        Self { inner, stats }
    }
}

#[async_trait]
impl ObjectStore for ObservedStore {
    async fn list(&self, continuation: Option<String>) -> Result<ObjectPage> {
        let observation = self.stats.begin(Operation::List);
        let result = self.inner.list(continuation).await;
        observation.finish(result.is_ok(), 0);
        result
    }
    async fn get_small(&self, relative: &str, max_bytes: u64) -> Result<Vec<u8>> {
        let observation = self.stats.begin(Operation::Read);
        let result = self.inner.get_small(relative, max_bytes).await;
        observation.finish(
            result.is_ok(),
            result.as_ref().map_or(0, |bytes| bytes.len() as u64),
        );
        result
    }
    async fn put_if_absent(&self, relative: &str, bytes: Vec<u8>) -> Result<()> {
        let observation = self.stats.begin(Operation::Create);
        let result = self.inner.put_if_absent(relative, bytes).await;
        observation.finish(result.is_ok(), 0);
        result
    }
    async fn put_bytes(&self, relative: &str, bytes: Vec<u8>) -> Result<()> {
        let observation = self.stats.begin(Operation::Write);
        let result = self.inner.put_bytes(relative, bytes).await;
        observation.finish(result.is_ok(), 0);
        result
    }
    async fn put_file(&self, relative: &str, path: &Path, checksum_sha256_hex: &str) -> Result<()> {
        let observation = self.stats.begin(Operation::Upload);
        let result = self
            .inner
            .put_file(relative, path, checksum_sha256_hex)
            .await;
        observation.finish(result.is_ok(), 0);
        result
    }
    async fn download(
        &self,
        relative: &str,
        destination: &Path,
        max_bytes: u64,
    ) -> Result<ObjectMetadata> {
        let observation = self.stats.begin(Operation::Download);
        let result = self.inner.download(relative, destination, max_bytes).await;
        observation.finish(
            result.is_ok(),
            result.as_ref().map_or(0, |metadata| metadata.size),
        );
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Store {
        calls: AtomicUsize,
        fail: bool,
    }
    impl Store {
        fn called(&self) -> Result<()> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            anyhow::ensure!(!self.fail, "source failure");
            Ok(())
        }
    }
    #[async_trait]
    impl ObjectStore for Store {
        async fn list(&self, continuation: Option<String>) -> Result<ObjectPage> {
            if continuation.as_deref() == Some("wait") {
                self.called()?;
                std::future::pending::<()>().await;
            }
            assert_eq!(continuation.as_deref(), Some("page"));
            self.called()?;
            Ok(ObjectPage {
                objects: vec![],
                continuation: Some("next".into()),
            })
        }
        async fn get_small(&self, relative: &str, max_bytes: u64) -> Result<Vec<u8>> {
            assert_eq!((relative, max_bytes), ("key", 5));
            self.called()?;
            Ok(vec![1, 2, 3])
        }
        async fn put_if_absent(&self, relative: &str, bytes: Vec<u8>) -> Result<()> {
            assert_eq!((relative, bytes), ("key", vec![1]));
            self.called()
        }
        async fn put_bytes(&self, relative: &str, bytes: Vec<u8>) -> Result<()> {
            assert_eq!((relative, bytes), ("key", vec![2]));
            self.called()
        }
        async fn put_file(&self, relative: &str, path: &Path, checksum: &str) -> Result<()> {
            assert_eq!(
                (relative, path, checksum),
                ("key", Path::new("not-read-by-observer"), "digest")
            );
            self.called()
        }
        async fn download(
            &self,
            relative: &str,
            destination: &Path,
            max_bytes: u64,
        ) -> Result<ObjectMetadata> {
            assert_eq!(
                (relative, destination, max_bytes),
                ("key", Path::new("not-read-by-observer"), 5)
            );
            self.called()?;
            Ok(ObjectMetadata {
                key: "key".into(),
                size: 4,
                etag: Some("etag".into()),
            })
        }
    }

    #[tokio::test]
    async fn forwarding_preserves_every_result_and_performs_exactly_one_operation() {
        for fail in [false, true] {
            let inner = Arc::new(Store {
                calls: AtomicUsize::new(0),
                fail,
            });
            let stats = Arc::new(Efficiency::default());
            let store = ObservedStore::new(inner.clone(), stats.clone());
            let list = store.list(Some("page".into())).await;
            let read = store.get_small("key", 5).await;
            let create = store.put_if_absent("key", vec![1]).await;
            let write = store.put_bytes("key", vec![2]).await;
            let upload = store
                .put_file("key", Path::new("not-read-by-observer"), "digest")
                .await;
            let download = store
                .download("key", Path::new("not-read-by-observer"), 5)
                .await;
            for result in [
                list.as_ref().map(|_| ()),
                read.as_ref().map(|_| ()),
                create.as_ref().map(|_| ()),
                write.as_ref().map(|_| ()),
                upload.as_ref().map(|_| ()),
                download.as_ref().map(|_| ()),
            ] {
                assert_eq!(result.is_err(), fail);
                if let Err(error) = result {
                    assert_eq!(error.to_string(), "source failure");
                }
            }
            if !fail {
                assert_eq!(list.unwrap().continuation.as_deref(), Some("next"));
                assert_eq!(read.unwrap(), vec![1, 2, 3]);
                assert_eq!(download.unwrap().etag.as_deref(), Some("etag"));
            }
            assert_eq!(inner.calls.load(Ordering::Relaxed), 6);
            let snapshot = stats.snapshot();
            for counts in snapshot.remote.values() {
                assert_eq!(counts.started, "1");
                assert_eq!(counts.failed, u8::from(fail).to_string());
                assert_eq!(counts.succeeded, u8::from(!fail).to_string());
                assert_eq!(counts.cancelled, "0");
            }
            assert_eq!(
                snapshot.remote["read_metadata"].completed_read_bytes,
                if fail { "0" } else { "3" }
            );
            assert_eq!(
                snapshot.remote["download"].completed_read_bytes,
                if fail { "0" } else { "4" }
            );
        }
    }
    #[tokio::test]
    async fn cancellation_drops_the_observation_without_an_extra_request() {
        let inner = Arc::new(Store {
            calls: AtomicUsize::new(0),
            fail: false,
        });
        let stats = Arc::new(Efficiency::default());
        let store = ObservedStore::new(inner.clone(), stats.clone());
        tokio::select! {
            biased;
            _ = store.list(Some("wait".into())) => panic!("source is pending"),
            _ = tokio::task::yield_now() => {}
        }
        assert_eq!(inner.calls.load(Ordering::Relaxed), 1);
        assert_eq!(stats.snapshot().remote["list"].cancelled, "1");
        assert_eq!(stats.snapshot().remote["list"].succeeded, "0");
    }
}
