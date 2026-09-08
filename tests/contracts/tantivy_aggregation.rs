use std::ops::Bound;

use anyhow::Result;
use serde_json::{Value, json};
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::agg_result::AggregationResults;
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use tantivy::aggregation::{
    AggContextParams, AggregationError, AggregationLimitsGuard, DistributedAggregationCollector,
};
use tantivy::indexer::NoMergePolicy;
use tantivy::query::{AllQuery, Query, RangeQuery};
use tantivy::schema::{FAST, JsonObjectOptions, STRING, Schema};
use tantivy::{Index, TantivyError, Term, doc};

fn request(value: Value) -> Result<Aggregations> {
    Ok(serde_json::from_value(value)?)
}

fn terms_request(size: u32, segment_size: u32) -> Result<Aggregations> {
    request(json!({
        "groups": {
            "terms": {
                "field": "service",
                "size": size,
                "segment_size": segment_size,
                "show_term_doc_count_error": true
            }
        }
    }))
}

fn distributed(
    indexes: &[&Index],
    query: &dyn Query,
    aggs: Aggregations,
    memory_limit: u64,
    bucket_limit: u32,
) -> tantivy::Result<AggregationResults> {
    let final_limits = AggregationLimitsGuard::new(Some(memory_limit), Some(bucket_limit));
    let mut merged = IntermediateAggregationResults::default();
    for index in indexes {
        let context = AggContextParams::new(final_limits.clone(), index.tokenizers().clone());
        let collector = DistributedAggregationCollector::from_aggs(aggs.clone(), context);
        let intermediate = index.reader()?.searcher().search(query, &collector)?;
        merged.merge_fruits(intermediate)?;
    }
    merged.into_final_result(aggs, final_limits)
}

fn bucket_rows(result: &AggregationResults) -> Result<Vec<Value>> {
    let value = serde_json::to_value(result)?;
    Ok(value["groups"]["buckets"]
        .as_array()
        .expect("terms result must contain buckets")
        .clone())
}

fn adversarial_shard(
    path: &std::path::Path,
    schema: Schema,
    service: tantivy::schema::Field,
    local: &str,
) -> Result<Index> {
    std::fs::create_dir(path)?;
    let index = Index::create_in_dir(path, schema)?;
    let mut writer = index.writer_with_num_threads(1, 15_000_000)?;
    writer.set_merge_policy(Box::new(NoMergePolicy));
    for (value, count) in [(local, 10), ("global-x", 9)] {
        for _ in 0..count {
            writer.add_document(doc!(service => value))?;
        }
    }
    // The local winner and global candidate must share a segment so segment_size=1 drops X.
    writer.commit()?;
    writer.add_document(doc!(service => format!("{local}-minor")))?;
    writer.commit()?;
    drop(writer);
    drop(index);
    Ok(Index::open_in_dir(path)?)
}

#[test]
fn g03_distributed_terms_preserve_global_winner_only_with_full_candidates() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut schema = Schema::builder();
    let service = schema.add_text_field("service", STRING | FAST);
    let schema = schema.build();
    let shard_a = adversarial_shard(&dir.path().join("a"), schema.clone(), service, "only-a")?;
    let shard_b = adversarial_shard(&dir.path().join("b"), schema, service, "only-b")?;

    let truncated = distributed(
        &[&shard_a, &shard_b],
        &AllQuery,
        terms_request(1, 1)?,
        16 * 1024 * 1024,
        20_000,
    )?;
    assert!(
        bucket_rows(&truncated)?
            .iter()
            .all(|bucket| bucket["key"] != "global-x")
    );

    let exact = distributed(
        &[&shard_a, &shard_b],
        &AllQuery,
        terms_request(20_001, 20_001)?,
        16 * 1024 * 1024,
        20_000,
    )?;
    let buckets = bucket_rows(&exact)?;
    assert_eq!(buckets[0]["key"], "global-x");
    assert_eq!(buckets[0]["doc_count"], 18);
    assert_eq!(buckets.len(), 5);
    Ok(())
}

#[test]
fn g03_distributed_metrics_are_weighted_and_json_non_numbers_are_not_coerced() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut schema = Schema::builder();
    let attributes =
        schema.add_json_field("attributes", JsonObjectOptions::default().set_fast(None));
    let schema = schema.build();

    let path_a = dir.path().join("a");
    let path_b = dir.path().join("b");
    std::fs::create_dir(&path_a)?;
    std::fs::create_dir(&path_b)?;
    let index_a = Index::create_in_dir(path_a, schema.clone())?;
    let mut writer_a = index_a.writer_with_num_threads(1, 15_000_000)?;
    writer_a.add_document(doc!(attributes => json!({"duration_ms": [1, 3]})))?;
    writer_a.add_document(doc!(attributes => json!({"duration_ms": "500"})))?;
    writer_a.add_document(doc!(attributes => json!({"duration_ms": null})))?;
    writer_a.commit()?;
    drop(writer_a);

    let index_b = Index::create_in_dir(path_b, schema)?;
    let mut writer_b = index_b.writer_with_num_threads(1, 15_000_000)?;
    writer_b.add_document(doc!(attributes => json!({"duration_ms": 100})))?;
    writer_b.add_document(doc!(attributes => json!({"duration_ms": false})))?;
    writer_b.commit()?;
    drop(writer_b);

    let aggs = request(json!({
        "average": {"avg": {"field": "attributes.duration_ms"}},
        "sum": {"sum": {"field": "attributes.duration_ms"}},
        "minimum": {"min": {"field": "attributes.duration_ms"}},
        "maximum": {"max": {"field": "attributes.duration_ms"}},
        "missing_metric": {"avg": {"field": "attributes.absent"}},
        "observed_types": {
            "terms": {
                "field": "attributes.duration_ms",
                "size": 10,
                "segment_size": 10
            }
        }
    }))?;
    let result = distributed(
        &[&index_a, &index_b],
        &AllQuery,
        aggs,
        16 * 1024 * 1024,
        20_000,
    )?;
    let value = serde_json::to_value(result)?;
    assert_eq!(value["sum"]["value"], 104.0);
    assert_eq!(value["minimum"]["value"], 1.0);
    assert_eq!(value["maximum"]["value"], 100.0);
    let average = value["average"]["value"].as_f64().unwrap();
    assert!((average - 104.0 / 3.0).abs() <= 1e-9);
    assert!(value["missing_metric"]["value"].is_null());
    assert_eq!(
        value["observed_types"]["buckets"].as_array().unwrap().len(),
        5
    );

    // The terms result retains the observed types, so it can support a verifiable mixed-type
    // warning. The metric itself exposes no excluded-value counter; product code must not invent
    // a zero-valued count from this response.
    assert_eq!(
        value["average"]
            .as_object()
            .unwrap()
            .keys()
            .collect::<Vec<_>>(),
        vec!["value"]
    );
    Ok(())
}

#[test]
fn g03_exact_bucket_and_native_memory_limits_fail_closed() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut schema = Schema::builder();
    let service = schema.add_text_field("service", STRING | FAST);
    let ordinal = schema.add_u64_field("ordinal", FAST);
    let schema = schema.build();
    let index = Index::create_in_dir(dir.path(), schema)?;
    let mut writer = index.writer_with_num_threads(1, 15_000_000)?;
    for value in 0..20_001_u64 {
        writer.add_document(doc!(
            service => format!("bucket-{value:05}"),
            ordinal => value,
        ))?;
    }
    writer.commit()?;
    drop(writer);

    let first_twenty_thousand = RangeQuery::new(
        Bound::Included(Term::from_field_u64(ordinal, 0)),
        Bound::Included(Term::from_field_u64(ordinal, 19_999)),
    );
    let exact = distributed(
        &[&index],
        &first_twenty_thousand,
        terms_request(20_001, 20_001)?,
        16 * 1024 * 1024,
        20_000,
    )?;
    assert_eq!(bucket_rows(&exact)?.len(), 20_000);

    let bucket_error = distributed(
        &[&index],
        &AllQuery,
        terms_request(20_001, 20_001)?,
        16 * 1024 * 1024,
        20_000,
    )
    .unwrap_err();
    assert!(matches!(
        bucket_error,
        TantivyError::AggregationError(AggregationError::BucketLimitExceeded {
            limit: 20_000,
            current: 20_001
        })
    ));

    let memory_error = distributed(
        &[&index],
        &AllQuery,
        terms_request(20_001, 20_001)?,
        1_024,
        20_001,
    )
    .unwrap_err();
    assert!(matches!(
        memory_error,
        TantivyError::AggregationError(AggregationError::MemoryExceeded { .. })
    ));
    Ok(())
}
