use anyhow::Result;
use eventglass::{
    model::Boundary,
    search::{active::ActiveShard, schema},
    sentry::{self, ProjectContext},
};
use serde_json::json;
use tantivy::Index;

#[test]
fn malformed_commit_payload_is_rejected_by_writer_and_orphan_recovery() -> Result<()> {
    for orphan in [false, true] {
        let directory = tempfile::tempdir()?;
        let index = Index::create_in_dir(directory.path(), schema::build())?;
        let mut writer: tantivy::IndexWriter<tantivy::TantivyDocument> =
            index.writer_with_num_threads(1, 15_000_000)?;
        let mut commit = writer.prepare_commit()?;
        commit.set_payload("{");
        commit.commit()?;
        drop(writer);
        let shard = uuid::Uuid::new_v4().to_string();
        if orphan {
            assert!(
                eventglass::search::active::verify_empty_orphan(
                    directory.path(),
                    "installation",
                    &shard,
                    Boundary::default(),
                )
                .is_err()
            );
        } else {
            assert!(ActiveShard::open(directory.path(), "installation", &shard).is_err());
        }
    }
    Ok(())
}

#[test]
fn native_commit_cannot_publish_before_matching_sqlite_boundary() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut shard = ActiveShard::create(dir.path(), "install", "shard", Boundary::default())?;
    assert!(shard.snapshot().is_none());
    let first = shard.publish(Boundary::default())?;
    let context = ProjectContext {
        project_id: 1,
        slug: "test".into(),
        public_key: "public".into(),
        scrub_keys: vec![],
    };
    let mut records = sentry::normalize_store(
        json!({"message":"native durable", "timestamp":1788860000})
            .to_string()
            .as_bytes(),
        &context,
        uuid::Uuid::new_v4(),
        1788860000000000,
        &Default::default(),
    )?
    .records;
    records[0].ingest_seq = 1;
    let boundary = Boundary {
        inbox_id: 1,
        ingest_seq: 1,
    };
    shard.commit(&records, boundary)?;
    assert!(shard.publish(Boundary::default()).is_err());
    assert_eq!(shard.snapshot().unwrap().searcher.num_docs(), 0);
    assert!(
        shard
            .commit(
                &[],
                Boundary {
                    inbox_id: 2,
                    ingest_seq: 2
                }
            )
            .is_err()
    );
    let second = shard.publish(boundary)?;
    assert_eq!(second.searcher.num_docs(), 1);
    assert_eq!(
        first.searcher.num_docs(),
        0,
        "pinned readers retain the old view"
    );
    shard.commit(
        &[],
        Boundary {
            inbox_id: 2,
            ingest_seq: 2,
        },
    )?;
    drop(shard);
    let mut reopened = ActiveShard::open(dir.path(), "install", "shard")?;
    assert_eq!(
        reopened.committed().boundary,
        Boundary {
            inbox_id: 2,
            ingest_seq: 2
        }
    );
    assert!(
        reopened.snapshot().is_none(),
        "startup waits for SQLite reconciliation"
    );
    assert_eq!(
        reopened
            .publish(Boundary {
                inbox_id: 2,
                ingest_seq: 2
            })?
            .searcher
            .num_docs(),
        1
    );
    drop(reopened);
    assert!(ActiveShard::open(dir.path(), "wrong-install", "shard").is_err());
    Ok(())
}

#[test]
fn invalid_document_poison_requires_reopen_not_partial_commit() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut shard = ActiveShard::create(dir.path(), "install", "shard", Boundary::default())?;
    shard.publish(Boundary::default())?;
    let context = ProjectContext {
        project_id: 1,
        slug: "test".into(),
        public_key: "public".into(),
        scrub_keys: vec![],
    };
    let mut records = sentry::normalize_store(
        b"{\"message\":\"valid\"}",
        &context,
        uuid::Uuid::new_v4(),
        1788860000000000,
        &Default::default(),
    )?
    .records;
    records[0].ingest_seq = 1;
    let mut invalid = records[0].clone();
    invalid.ingest_seq = 2;
    invalid.timestamp_us = i64::MAX;
    records.push(invalid);
    assert!(
        shard
            .commit(
                &records,
                Boundary {
                    inbox_id: 1,
                    ingest_seq: 2
                }
            )
            .is_err()
    );
    assert!(shard.publish(Boundary::default()).is_err());
    assert!(
        shard
            .commit(
                &[],
                Boundary {
                    inbox_id: 1,
                    ingest_seq: 2
                }
            )
            .is_err()
    );
    drop(shard);
    let mut reopened = ActiveShard::open(dir.path(), "install", "shard")?;
    assert_eq!(
        reopened.publish(Boundary::default())?.searcher.num_docs(),
        0
    );
    Ok(())
}
