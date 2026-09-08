use anyhow::Result;
use serde::{Deserialize, Serialize};
use tantivy::schema::{STORED, STRING, Schema};
use tantivy::{Index, doc};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CommitBoundary {
    version: u32,
    installation_id: String,
    shard_id: String,
    inbox_id: i64,
    ingest_seq: i64,
}

fn payload(inbox_id: i64, ingest_seq: i64, shard_id: &str) -> Result<String> {
    Ok(serde_json::to_string(&CommitBoundary {
        version: 1,
        installation_id: "installation-contract".into(),
        shard_id: shard_id.into(),
        inbox_id,
        ingest_seq,
    })?)
}

fn persisted_boundary(index: &Index) -> Result<CommitBoundary> {
    let raw = index
        .load_metas()?
        .payload
        .expect("contract commit must carry a boundary payload");
    Ok(serde_json::from_str(&raw)?)
}

#[test]
fn g02_payload_survives_non_empty_and_duplicate_only_commits_and_reopen() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut schema = Schema::builder();
    let record_id = schema.add_text_field("record_id", STRING | STORED);
    let schema = schema.build();
    let index = Index::create_in_dir(dir.path(), schema)?;
    let mut writer: tantivy::IndexWriter<tantivy::TantivyDocument> =
        index.writer_with_num_threads(1, 15_000_000)?;

    writer.add_document(doc!(record_id => "record-a"))?;
    let first = payload(10, 100, "shard-a")?;
    let mut prepared = writer.prepare_commit()?;
    prepared.set_payload(&first);
    prepared.commit()?;
    assert_eq!(persisted_boundary(&index)?.ingest_seq, 100);

    // A duplicate-only batch adds no document but still advances the durable boundary.
    let duplicate_only = payload(11, 101, "shard-a")?;
    let mut prepared = writer.prepare_commit()?;
    prepared.set_payload(&duplicate_only);
    prepared.commit()?;
    assert_eq!(index.load_metas()?.segments.len(), 1);
    drop(writer);
    drop(index);

    // This is the recovery path for an uncertain commit result: reopen and trust persisted C.
    let reopened = Index::open_in_dir(dir.path())?;
    assert_eq!(
        persisted_boundary(&reopened)?,
        serde_json::from_str(&duplicate_only)?
    );
    assert_eq!(reopened.reader()?.searcher().num_docs(), 1);
    Ok(())
}

#[test]
fn g02_new_active_accepts_an_initial_empty_metadata_commit() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let schema = Schema::builder().build();
    let index = Index::create_in_dir(dir.path(), schema)?;
    let mut writer: tantivy::IndexWriter<tantivy::TantivyDocument> =
        index.writer_with_num_threads(1, 15_000_000)?;
    let initial = payload(11, 101, "shard-b")?;
    let mut prepared = writer.prepare_commit()?;
    prepared.set_payload(&initial);
    prepared.commit()?;
    drop(writer);
    drop(index);

    let reopened = Index::open_in_dir(dir.path())?;
    assert_eq!(persisted_boundary(&reopened)?.shard_id, "shard-b");
    assert_eq!(reopened.reader()?.searcher().num_docs(), 0);
    Ok(())
}
