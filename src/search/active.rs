//! Native active-shard writer. SQLite finalization must precede explicit publication.

use std::path::Path;

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use tantivy::{
    Index, IndexReader, IndexSettings, IndexWriter, ReloadPolicy, Searcher, TantivyDocument,
};

use crate::model::{Boundary, Record};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitPayload {
    pub version: u32,
    pub installation_id: String,
    pub shard_id: String,
    pub boundary: Boundary,
}

#[derive(Clone)]
pub struct Published {
    pub shard_id: String,
    pub boundary: Boundary,
    pub searcher: Searcher,
}

pub struct ActiveShard {
    index: Index,
    writer: IndexWriter<TantivyDocument>,
    reader: IndexReader,
    committed: CommitPayload,
    published: Option<Published>,
    uncertain: bool,
}

impl ActiveShard {
    /// Caller creates a new unique directory; never use this to replace a cataloged index.
    pub fn create(
        path: &Path,
        installation: &str,
        shard: &str,
        inherited: Boundary,
    ) -> Result<Self> {
        ensure!(
            inherited.inbox_id >= 0 && inherited.ingest_seq >= 0,
            "negative inherited boundary"
        );
        let settings = IndexSettings {
            docstore_compression: tantivy::store::Compressor::Zstd(Default::default()),
            ..Default::default()
        };
        let index = Index::builder()
            .schema(super::schema::build())
            .settings(settings)
            .create_in_dir(path)?;
        let mut writer = index.writer_with_num_threads(1, 32 * 1024 * 1024)?;
        let committed = CommitPayload {
            version: 1,
            installation_id: installation.into(),
            shard_id: shard.into(),
            boundary: inherited,
        };
        let mut commit = writer.prepare_commit()?;
        commit.set_payload(&serde_json::to_string(&committed)?);
        commit.commit()?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        Ok(Self {
            index,
            writer,
            reader,
            committed,
            published: None,
            uncertain: false,
        })
    }

    pub fn open(path: &Path, installation: &str, shard: &str) -> Result<Self> {
        ensure!(
            !path.join(crate::storage::manifest::NAME).exists(),
            "sealed shard cannot reopen a writer"
        );
        let index = Index::open_in_dir(path)?;
        ensure!(
            index.schema() == super::schema::build(),
            "active shard schema mismatch"
        );
        let committed: CommitPayload = serde_json::from_str(
            &index
                .load_metas()?
                .payload
                .context("missing commit boundary")?,
        )?;
        ensure!(
            committed.version == 1
                && committed.installation_id == installation
                && committed.shard_id == shard,
            "active shard identity mismatch"
        );
        ensure!(
            committed.boundary.inbox_id >= 0 && committed.boundary.ingest_seq >= 0,
            "invalid committed boundary"
        );
        let writer = index.writer_with_num_threads(1, 32 * 1024 * 1024)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        Ok(Self {
            index,
            writer,
            reader,
            committed,
            published: None,
            uncertain: false,
        })
    }

    pub fn committed(&self) -> &CommitPayload {
        &self.committed
    }

    pub fn snapshot(&self) -> Option<Published> {
        self.published.clone()
    }

    /// Records have already passed deterministic DB + batch Error deduplication.
    /// Any native failure poisons this instance: reopen/reconcile C before further writes.
    pub fn commit(&mut self, records: &[Record], boundary: Boundary) -> Result<()> {
        ensure!(
            !self.uncertain,
            "native commit outcome is uncertain; reopen required"
        );
        ensure!(
            self.published
                .as_ref()
                .is_some_and(|p| p.boundary == self.committed.boundary),
            "previous native commit is not finalized and published"
        );
        ensure!(
            boundary.inbox_id > self.committed.boundary.inbox_id
                && boundary.ingest_seq > self.committed.boundary.ingest_seq,
            "non-forward batch boundary"
        );
        ensure!(
            records
                .windows(2)
                .all(|pair| pair[0].ingest_seq < pair[1].ingest_seq)
                && records.iter().all(|record| record.ingest_seq
                    > self.committed.boundary.ingest_seq
                    && record.ingest_seq <= boundary.ingest_seq),
            "record outside commit boundary"
        );
        self.uncertain = true;
        for record in records {
            self.writer
                .add_document(super::schema::document(&self.index.schema(), record)?)?;
        }
        let mut next = self.committed.clone();
        next.boundary = boundary;
        let mut commit = self.writer.prepare_commit()?;
        commit.set_payload(&serde_json::to_string(&next)?);
        commit.commit()?;
        self.committed = next;
        self.uncertain = false;
        Ok(())
    }

    /// Consumes the sole writer and waits for every native merge before sealing.
    pub fn seal(
        self,
        path: &Path,
        stats: crate::storage::manifest::ShardStats,
        sealed_at_us: i64,
    ) -> Result<crate::storage::manifest::Manifest> {
        ensure!(
            !self.uncertain
                && self
                    .published
                    .as_ref()
                    .is_some_and(|p| p.boundary == self.committed.boundary),
            "seal requires finalized published boundary"
        );
        let Self {
            writer,
            reader,
            index,
            committed,
            published,
            ..
        } = self;
        writer.wait_merging_threads()?;
        drop(published);
        drop(reader);
        drop(index);
        crate::storage::manifest::write(path, &committed, stats, sealed_at_us)
    }

    /// Call only after SQLite returns its committed A. W and reader change together.
    pub fn publish(&mut self, applied: Boundary) -> Result<Published> {
        ensure!(!self.uncertain, "native commit outcome is uncertain");
        ensure!(
            applied == self.committed.boundary,
            "SQLite/native boundary mismatch"
        );
        self.reader.reload()?;
        let published = Published {
            shard_id: self.committed.shard_id.clone(),
            boundary: applied,
            searcher: self.reader.searcher(),
        };
        self.published = Some(published.clone());
        Ok(published)
    }
}

/// Opens an immutable shard after its durable manifest has been verified.
pub fn open_sealed(
    path: &Path,
    manifest: &crate::storage::manifest::Manifest,
) -> Result<Published> {
    let index = Index::open_in_dir(path)?;
    ensure!(
        index.schema() == super::schema::build(),
        "sealed shard schema mismatch"
    );
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    let searcher = reader.searcher();
    ensure!(
        searcher.num_docs() == manifest.stats.record_count,
        "sealed shard record count mismatch"
    );
    Ok(Published {
        shard_id: manifest.shard_id.clone(),
        boundary: manifest.boundary,
        searcher,
    })
}

pub fn verify_empty_orphan(
    path: &Path,
    installation: &str,
    shard: &str,
    inherited: Boundary,
) -> Result<()> {
    ensure!(
        !path.join(crate::storage::manifest::NAME).exists(),
        "orphan candidate is sealed"
    );
    uuid::Uuid::parse_str(shard)?;
    let index = Index::open_in_dir(path)?;
    ensure!(
        index.schema() == super::schema::build(),
        "orphan candidate schema mismatch"
    );
    let committed: CommitPayload = serde_json::from_str(
        &index
            .load_metas()?
            .payload
            .context("missing orphan commit boundary")?,
    )?;
    ensure!(
        committed.version == 1
            && committed.installation_id == installation
            && committed.shard_id == shard
            && committed.boundary == inherited
            && index.reader()?.searcher().num_docs() == 0,
        "unregistered native index is not a safe empty active candidate"
    );
    Ok(())
}
