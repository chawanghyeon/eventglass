use std::cmp::Reverse;
use std::ops::Bound;

use anyhow::Result;
use tantivy::collector::TopDocs;
use tantivy::collector::sort_key::SortByStaticFastValue;
use tantivy::query::{AllQuery, BooleanQuery, Occur, Query, RangeQuery};
use tantivy::schema::{DateOptions, DateTimePrecision, FAST, STORED, Schema};
use tantivy::{DateTime, Index, Order, Term, doc};

#[derive(Clone, Copy)]
struct SortFields {
    timestamp: tantivy::schema::Field,
    ingest_seq: tantivy::schema::Field,
}

fn create_shard(
    path: &std::path::Path,
    schema: Schema,
    fields: SortFields,
    rows: &[(i64, u64)],
) -> Result<Index> {
    std::fs::create_dir(path)?;
    let index = Index::create_in_dir(path, schema)?;
    let mut writer = index.writer_with_num_threads(1, 15_000_000)?;
    for &(timestamp_us, seq) in rows {
        writer.add_document(doc!(
            fields.timestamp => DateTime::from_timestamp_micros(timestamp_us),
            fields.ingest_seq => seq,
        ))?;
    }
    writer.commit()?;
    drop(writer);
    drop(index);
    Ok(Index::open_in_dir(path)?)
}

fn native_top(index: &Index, query: &dyn Query, limit: usize) -> Result<Vec<(i64, u64)>> {
    let reader = index.reader()?;
    let searcher = reader.searcher();
    let collector = TopDocs::with_limit(limit).order_by::<(Option<DateTime>, Option<u64>)>((
        (SortByStaticFastValue::for_field("timestamp"), Order::Desc),
        (SortByStaticFastValue::for_field("ingest_seq"), Order::Desc),
    ));
    let hits = searcher.search(query, &collector)?;
    Ok(hits
        .into_iter()
        .map(|((timestamp, seq), _)| {
            (
                timestamp
                    .expect("timestamp is required")
                    .into_timestamp_micros(),
                seq.expect("ingest_seq is required"),
            )
        })
        .collect())
}

fn merge_top(mut hits: Vec<Vec<(i64, u64)>>, limit: usize) -> Vec<(i64, u64)> {
    let mut flat: Vec<_> = hits.drain(..).flatten().collect();
    flat.sort_unstable_by_key(|&(timestamp, seq)| Reverse((timestamp, seq)));
    flat.truncate(limit);
    flat
}

fn after_cursor(fields: SortFields, cursor: (i64, u64)) -> BooleanQuery {
    let cursor_time = DateTime::from_timestamp_micros(cursor.0);
    let older_time = RangeQuery::new(
        Bound::Unbounded,
        Bound::Excluded(Term::from_field_date(fields.timestamp, cursor_time)),
    );
    let same_time = RangeQuery::new(
        Bound::Included(Term::from_field_date(fields.timestamp, cursor_time)),
        Bound::Included(Term::from_field_date(fields.timestamp, cursor_time)),
    );
    let lower_seq = RangeQuery::new(
        Bound::Unbounded,
        Bound::Excluded(Term::from_field_u64(fields.ingest_seq, cursor.1)),
    );
    BooleanQuery::new(vec![
        (Occur::Should, Box::new(older_time)),
        (
            Occur::Should,
            Box::new(BooleanQuery::new(vec![
                (Occur::Must, Box::new(same_time)),
                (Occur::Must, Box::new(lower_seq)),
            ])),
        ),
    ])
}

#[test]
fn g04_native_tuple_sort_shard_merge_and_keyset_match_baseline() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut schema = Schema::builder();
    let fields = SortFields {
        timestamp: schema.add_date_field(
            "timestamp",
            DateOptions::default()
                .set_fast()
                .set_precision(DateTimePrecision::Microseconds),
        ),
        ingest_seq: schema.add_u64_field("ingest_seq", FAST | STORED),
    };
    let schema = schema.build();
    let shard_a = create_shard(
        &dir.path().join("a"),
        schema.clone(),
        fields,
        &[(1_000_001, 2), (1_000_000, 6), (999_999, 7)],
    )?;
    let shard_b = create_shard(
        &dir.path().join("b"),
        schema,
        fields,
        &[(1_000_001, 5), (1_000_000, 9), (1_000_002, 1)],
    )?;

    let baseline = [
        (1_000_002, 1),
        (1_000_001, 5),
        (1_000_001, 2),
        (1_000_000, 9),
        (1_000_000, 6),
        (999_999, 7),
    ];
    let first = merge_top(
        vec![
            native_top(&shard_a, &AllQuery, 3)?,
            native_top(&shard_b, &AllQuery, 3)?,
        ],
        3,
    );
    assert_eq!(first, baseline[..3]);

    let cursor = *first.last().unwrap();
    let keyset = after_cursor(fields, cursor);
    let second = merge_top(
        vec![
            native_top(&shard_a, &keyset, 3)?,
            native_top(&shard_b, &keyset, 3)?,
        ],
        3,
    );
    assert_eq!(second, baseline[3..]);
    Ok(())
}
