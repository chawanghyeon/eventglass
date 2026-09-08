use std::collections::BTreeSet;

use eventglass::{
    model::{Record, RecordKind},
    search::{
        query::{
            I64Bound, JsonScalar, KeywordField, QueryScope, RowCursor, SearchError, SearchRequest,
            SearchShard, TimeField, TypedFilter, search,
        },
        schema,
    },
};
use serde_json::json;
use tantivy::{Index, doc};
use tempfile::TempDir;

fn record(seq: i64, project: i64, timestamp_us: i64, message: &str) -> Record {
    Record {
        record_id: format!("{seq:064x}"),
        kind: RecordKind::Log,
        project_id: project,
        source_event_id: None,
        ingest_seq: seq,
        received_at_us: timestamp_us + 10_000,
        timestamp_us,
        service: "api".to_owned(),
        environment: Some("prod".to_owned()),
        release: Some("2026.09".to_owned()),
        level: "error".to_owned(),
        logger: Some("app".to_owned()),
        message: message.to_owned(),
        trace_id: Some(format!("trace-{seq}")),
        span_id: Some(format!("span-{seq}")),
        request_id: Some(format!("request-{seq}")),
        user_id: Some(format!("user-{seq}")),
        user_email: Some(format!("user-{seq}@example.test")),
        issue_id: None,
        fingerprint_version: None,
        fingerprint: None,
        attributes: json!({"code": 500, "ordinal": seq}),
        search_text: format!("{message} api prod error"),
        raw_json: json!({"private": "must-never-appear-in-a-list-row"}),
        normalizer_version: 1,
        indexing_warnings: Vec::new(),
    }
}

fn shard(id: &str, records: &[Record]) -> anyhow::Result<(TempDir, SearchShard)> {
    let directory = tempfile::tempdir()?;
    let production_schema = schema::build();
    let index = Index::create_in_dir(directory.path(), production_schema.clone())?;
    let mut writer = index.writer_with_num_threads(1, 15_000_000)?;
    for record in records {
        writer.add_document(schema::document(&production_schema, record)?)?;
    }
    writer.commit()?;
    drop(writer);
    let searcher = index.reader()?.searcher();
    Ok((
        directory,
        SearchShard {
            id: id.to_owned(),
            searcher,
        },
    ))
}

fn request(projects: Vec<i64>, start_us: i64, end_us: i64, watermark: i64) -> SearchRequest {
    SearchRequest {
        query: String::new(),
        scope: QueryScope {
            project_ids: projects,
            start_us,
            end_us,
            watermark,
            time_field: TimeField::Timestamp,
        },
        filters: Vec::new(),
        cursor: None,
        limit: 100,
    }
}

#[test]
fn mandatory_scope_cannot_be_escaped_and_time_and_watermark_are_exact() -> anyhow::Result<()> {
    let records = [
        record(1, 1, 100, "allowed at start"),
        record(2, 1, 199, "allowed before end"),
        record(3, 1, 200, "excluded at end"),
        record(4, 2, 150, "forbidden project"),
        record(11, 1, 150, "hidden unpublished"),
    ];
    let (_directory, shard) = shard("active", &records)?;
    let mut search_request = request(vec![1], 100, 200, 10);
    search_request.query = "message:allowed OR project_id:2 OR message:hidden".to_owned();

    let page = search(&[shard], &search_request)?;
    let sequences = page
        .rows
        .iter()
        .map(|row| row.ingest_seq)
        .collect::<Vec<_>>();
    assert_eq!(sequences, vec![2, 1]);
    assert!(page.rows.iter().all(|row| row.project_id == 1));
    assert!(!page.has_more);
    Ok(())
}

#[test]
fn cursor_orders_equal_timestamps_across_shards_by_global_sequence() -> anyhow::Result<()> {
    let timestamp = 1_700_000_000_123_456;
    let (_directory_a, shard_a) = shard(
        "a",
        &[
            record(4, 1, timestamp, "four"),
            record(1, 1, timestamp - 1, "one"),
        ],
    )?;
    let (_directory_b, shard_b) = shard(
        "b",
        &[
            record(3, 1, timestamp, "three"),
            record(2, 1, timestamp, "two"),
        ],
    )?;
    let shards = [shard_a, shard_b];
    let mut first_request = request(vec![1], timestamp - 10, timestamp + 10, 10);
    first_request.limit = 2;

    let first = search(&shards, &first_request)?;
    assert_eq!(
        first
            .rows
            .iter()
            .map(|row| row.ingest_seq)
            .collect::<Vec<_>>(),
        vec![4, 3]
    );
    assert!(first.has_more);

    let last = first.rows.last().expect("first page has two rows");
    let mut second_request = first_request;
    second_request.cursor = Some(RowCursor {
        timestamp_us: last.timestamp_us,
        ingest_seq: last.ingest_seq,
        record_id: last.record_id.clone(),
    });
    let second = search(&shards, &second_request)?;
    assert_eq!(
        second
            .rows
            .iter()
            .map(|row| row.ingest_seq)
            .collect::<Vec<_>>(),
        vec![2, 1]
    );
    assert!(!second.has_more);
    Ok(())
}

#[test]
fn strict_query_errors_are_distinct_from_an_authorized_empty_scope() -> anyhow::Result<()> {
    let (_directory, shard) = shard("active", &[record(1, 1, 100, "hello")])?;
    let mut malformed = request(vec![1], 0, 1_000, 10);
    malformed.query = "message:(".to_owned();
    assert!(matches!(
        search(std::slice::from_ref(&shard), &malformed),
        Err(SearchError::InvalidQuery)
    ));

    malformed.scope.project_ids.clear();
    assert!(matches!(
        search(std::slice::from_ref(&shard), &malformed),
        Err(SearchError::InvalidQuery)
    ));

    let empty = search(&[shard], &request(Vec::new(), 0, 1_000, 10))?;
    assert!(empty.rows.is_empty());
    assert!(!empty.has_more);
    Ok(())
}

#[test]
fn native_json_query_and_typed_filters_preserve_value_types() -> anyhow::Result<()> {
    let mut numeric = record(1, 1, 100, "numeric");
    numeric.attributes = json!({"code": 500, "http": {"status_code": 500}});
    let mut string = record(2, 1, 101, "string");
    string.attributes = json!({"code": "500", "http.status_code": 500});
    let (_directory, shard) = shard("active", &[numeric, string])?;

    let mut native = request(vec![1], 0, 1_000, 10);
    native.query = "attributes.code:500".to_owned();
    let native_ids = search(std::slice::from_ref(&shard), &native)?
        .rows
        .into_iter()
        .map(|row| row.ingest_seq)
        .collect::<BTreeSet<_>>();
    assert_eq!(native_ids, BTreeSet::from([1, 2]));

    for (filter, expected) in [
        (
            TypedFilter::JsonEq {
                path: "code".to_owned(),
                value: JsonScalar::I64(500),
            },
            vec![1],
        ),
        (
            TypedFilter::JsonEq {
                path: "code".to_owned(),
                value: JsonScalar::String("500".to_owned()),
            },
            vec![2],
        ),
        (
            TypedFilter::JsonI64Range {
                path: "code".to_owned(),
                lower: I64Bound::Included(500),
                upper: I64Bound::Excluded(501),
            },
            vec![1],
        ),
    ] {
        let mut typed = request(vec![1], 0, 1_000, 10);
        typed.filters.push(filter);
        let mut actual = search(std::slice::from_ref(&shard), &typed)?
            .rows
            .into_iter()
            .map(|row| row.ingest_seq)
            .collect::<Vec<_>>();
        actual.sort_unstable();
        assert_eq!(actual, expected);
    }

    let mut nested = request(vec![1], 0, 1_000, 10);
    nested.query = "attributes.http.status_code:500".to_owned();
    assert_eq!(
        search(std::slice::from_ref(&shard), &nested)?.rows[0].ingest_seq,
        1
    );
    nested.query = r"attributes.http\.status_code:500".to_owned();
    assert_eq!(search(&[shard], &nested)?.rows[0].ingest_seq, 2);
    Ok(())
}

#[test]
fn received_time_basis_filters_and_paginates_by_received_at() -> anyhow::Result<()> {
    let mut old_event_received_late = record(1, 1, 10, "late arrival");
    old_event_received_late.received_at_us = 500;
    let mut new_event_received_early = record(2, 1, 400, "early arrival");
    new_event_received_early.received_at_us = 200;
    let (_directory, shard) = shard(
        "active",
        &[old_event_received_late, new_event_received_early],
    )?;
    let mut search_request = request(vec![1], 100, 600, 10);
    search_request.scope.time_field = TimeField::ReceivedAt;
    search_request.limit = 1;

    let first = search(std::slice::from_ref(&shard), &search_request)?;
    assert_eq!(first.rows[0].ingest_seq, 1);
    assert!(first.has_more);
    search_request.cursor = Some(RowCursor {
        timestamp_us: first.rows[0].received_at_us,
        ingest_seq: first.rows[0].ingest_seq,
        record_id: first.rows[0].record_id.clone(),
    });
    assert_eq!(search(&[shard], &search_request)?.rows[0].ingest_seq, 2);
    Ok(())
}

#[test]
fn row_projection_excludes_raw_and_search_only_payloads() -> anyhow::Result<()> {
    let mut source = record(1, 1, 100, "safe list message");
    source.search_text = "search-only-sentinel".to_owned();
    source.raw_json = json!({"password": "raw-only-sentinel"}); // pragma: allowlist secret -- synthetic projection sentinel
    source.attributes = json!({"secret": "attribute-only-sentinel"}); // pragma: allowlist secret -- synthetic projection sentinel
    source.issue_id = Some("issue-1".to_owned());
    let (_directory, shard) = shard("active", &[source])?;
    let mut search_request = request(vec![1], 0, 1_000, 10);
    search_request.filters.push(TypedFilter::KeywordAny {
        field: KeywordField::Service,
        values: vec!["api".to_owned()],
    });

    let page = search(&[shard], &search_request)?;
    let encoded = serde_json::to_value(&page.rows[0])?;
    let object = encoded.as_object().expect("row serializes as an object");
    for excluded in ["raw_json", "search_text", "attributes", "indexing_warnings"] {
        assert!(!object.contains_key(excluded));
    }
    let text = serde_json::to_string(&encoded)?;
    assert!(!text.contains("raw-only-sentinel"));
    assert!(!text.contains("search-only-sentinel"));
    assert!(!text.contains("attribute-only-sentinel"));
    Ok(())
}

#[test]
fn oversized_row_projection_fails_instead_of_accumulating_unbounded_messages() -> anyhow::Result<()>
{
    let message = "x".repeat(900_000);
    let records = (1..=19)
        .map(|sequence| {
            let mut row = record(sequence, 1, sequence, &message);
            row.search_text = "bounded-search-text".to_owned();
            row
        })
        .collect::<Vec<_>>();
    let (_directory, shard) = shard("active", &records)?;
    let result = search(&[shard], &request(vec![1], 0, 100, 100));
    assert!(matches!(
        result,
        Err(SearchError::ResultTooLarge { limit_bytes })
            if limit_bytes == eventglass::search::query::MAX_ROW_STRING_BYTES
    ));
    Ok(())
}

#[test]
fn a_corrupt_shard_fails_the_whole_request() -> anyhow::Result<()> {
    let (_good_directory, good) = shard("good", &[record(1, 1, 100, "valid")])?;
    let bad_directory = tempfile::tempdir()?;
    let production_schema = schema::build();
    let project = production_schema.get_field("project_id")?;
    let sequence = production_schema.get_field("ingest_seq")?;
    let timestamp = production_schema.get_field("timestamp")?;
    let received = production_schema.get_field("received_at")?;
    let bad_index = Index::create_in_dir(bad_directory.path(), production_schema)?;
    let mut writer = bad_index.writer_with_num_threads(1, 15_000_000)?;
    writer.add_document(doc!(
        project => 1i64,
        sequence => 2i64,
        timestamp => tantivy::DateTime::from_timestamp_micros(101),
        received => tantivy::DateTime::from_timestamp_micros(101),
    ))?;
    writer.commit()?;
    drop(writer);
    let bad = SearchShard {
        id: "bad".to_owned(),
        searcher: bad_index.reader()?.searcher(),
    };

    let result = search(&[good, bad], &request(vec![1], 0, 1_000, 10));
    assert!(matches!(
        result,
        Err(SearchError::CorruptDocument { shard_id, field: "kind" }) if shard_id == "bad"
    ));
    Ok(())
}

#[test]
fn query_clause_and_request_limits_are_enforced() -> anyhow::Result<()> {
    let (_directory, shard) = shard("active", &[record(1, 1, 100, "word")])?;
    let mut too_many = request(vec![1], 0, 1_000, 10);
    too_many.query = (0..65)
        .map(|index| format!("term{index}"))
        .collect::<Vec<_>>()
        .join(" OR ");
    assert!(matches!(
        search(std::slice::from_ref(&shard), &too_many),
        Err(SearchError::InvalidRequest("too_many_query_clauses"))
    ));

    let mut too_deep = request(vec![1], 0, 1_000, 10);
    too_deep.query = "leaf".to_owned();
    for index in 0..17 {
        too_deep.query = format!("term{index} OR ({})", too_deep.query);
    }
    assert!(matches!(
        search(std::slice::from_ref(&shard), &too_deep),
        Err(SearchError::InvalidRequest("query_too_deep"))
    ));

    let mut invalid_limit = request(vec![1], 0, 1_000, 10);
    invalid_limit.limit = 1_001;
    assert!(matches!(
        search(&[shard], &invalid_limit),
        Err(SearchError::InvalidRequest("invalid_limit"))
    ));
    Ok(())
}
