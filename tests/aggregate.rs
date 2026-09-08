use anyhow::Result;
use eventglass::{
    model::{Record, RecordKind},
    search::{
        aggregate::{
            AggregateError, AggregateRequest, AggregateWarning, BucketDimension, BucketKey,
            GroupField, GroupSpec, HistogramSpec, MetricOp, MetricSpec, MetricValue, NumericField,
            aggregate,
        },
        query::{QueryScope, SearchShard, TimeField},
        schema,
    },
};
use serde_json::{Value, json};
use tantivy::{Index, indexer::NoMergePolicy};
use tempfile::TempDir;

struct Fixture {
    _directory: TempDir,
    shard: SearchShard,
}

fn record(seq: i64, project: i64, timestamp_us: i64, service: &str) -> Record {
    Record {
        record_id: format!("{seq:064x}"),
        kind: RecordKind::Log,
        project_id: project,
        source_event_id: None,
        ingest_seq: seq,
        received_at_us: timestamp_us + 10,
        timestamp_us,
        service: service.to_owned(),
        environment: Some("prod".to_owned()),
        release: Some("2026.09".to_owned()),
        level: "info".to_owned(),
        logger: None,
        message: "aggregate fixture".to_owned(),
        trace_id: None,
        span_id: None,
        request_id: None,
        user_id: None,
        user_email: None,
        issue_id: None,
        fingerprint_version: None,
        fingerprint: None,
        attributes: json!({}),
        search_text: "aggregate fixture".to_owned(),
        raw_json: json!({}),
        normalizer_version: 1,
        indexing_warnings: Vec::new(),
    }
}

fn fixture(id: &str, batches: Vec<Vec<Record>>) -> Result<Fixture> {
    let directory = tempfile::tempdir()?;
    let production_schema = schema::build();
    let index = Index::create_in_dir(directory.path(), production_schema.clone())?;
    let mut writer = index.writer_with_num_threads(1, 30_000_000)?;
    writer.set_merge_policy(Box::new(NoMergePolicy));
    for batch in batches {
        for row in batch {
            writer.add_document(schema::document(&production_schema, &row)?)?;
        }
        writer.commit()?;
    }
    drop(writer);
    let searcher = index.reader()?.searcher();
    Ok(Fixture {
        _directory: directory,
        shard: SearchShard {
            id: id.to_owned(),
            searcher,
        },
    })
}

fn request(metrics: Vec<MetricSpec>) -> AggregateRequest {
    AggregateRequest {
        query: String::new(),
        scope: QueryScope {
            project_ids: vec![1],
            start_us: 0,
            end_us: 1_000_000,
            watermark: 1_000_000,
            time_field: TimeField::Timestamp,
        },
        filters: Vec::new(),
        metrics,
        group_by: Vec::new(),
        histogram: None,
    }
}

fn numeric_metric(page: &[MetricValue], name: &str) -> (Option<f64>, Option<u64>) {
    page.iter()
        .find_map(|metric| match metric {
            MetricValue::Number {
                name: metric_name,
                value,
                numeric_value_count,
                ..
            } if metric_name == name => Some((*value, *numeric_value_count)),
            _ => None,
        })
        .expect("requested numeric metric is present")
}

#[test]
fn distributed_merge_finds_a_winner_outside_every_local_top_one() -> Result<()> {
    let mut next_seq = 1;
    let mut adversarial = |local: &str| {
        let mut first_segment = Vec::new();
        for (service, count) in [(local, 10), ("global-x", 9)] {
            for _ in 0..count {
                first_segment.push(record(next_seq, 1, next_seq, service));
                next_seq += 1;
            }
        }
        let second_segment = vec![record(next_seq, 1, next_seq, &format!("{local}-minor"))];
        next_seq += 1;
        vec![first_segment, second_segment]
    };
    let shard_a = fixture("a", adversarial("only-a"))?;
    let shard_b = fixture("b", adversarial("only-b"))?;
    let mut aggregate_request = request(vec![MetricSpec::Count {
        name: "records".to_owned(),
    }]);
    aggregate_request.group_by.push(GroupSpec {
        field: GroupField::Service,
        limit: 1,
    });

    let page = aggregate(
        &[shard_a.shard.clone(), shard_b.shard.clone()],
        &aggregate_request,
    )?;
    assert_eq!(page.record_count, 40);
    let groups = page.buckets.expect("service groups are returned");
    assert_eq!(
        groups.dimension,
        BucketDimension::Group {
            field: GroupField::Service
        }
    );
    assert!(groups.has_more);
    assert_eq!(groups.buckets.len(), 1);
    assert_eq!(groups.buckets[0].key, BucketKey::String("global-x".into()));
    assert_eq!(groups.buckets[0].doc_count, 18);
    assert_eq!(
        groups.buckets[0].metrics,
        vec![MetricValue::Count {
            name: "records".to_owned(),
            value: 18,
        }]
    );
    Ok(())
}

#[test]
fn metrics_merge_weighted_values_and_do_not_coerce_mixed_json_types() -> Result<()> {
    let mut array = record(1, 1, 100, "api");
    array.attributes = json!({"duration_ms": [1, 3]});
    let mut string = record(2, 1, 101, "api");
    string.attributes = json!({"duration_ms": "500"});
    let mut null = record(3, 1, 102, "api");
    null.attributes = json!({"duration_ms": null});
    let mut number = record(4, 1, 103, "worker");
    number.attributes = json!({"duration_ms": 100});
    let mut boolean = record(5, 1, 104, "worker");
    boolean.attributes = json!({"duration_ms": false});
    let shard_a = fixture("a", vec![vec![array, string, null]])?;
    let shard_b = fixture("b", vec![vec![number, boolean]])?;
    let duration = NumericField::Json("duration_ms".to_owned());
    let missing = NumericField::Json("absent".to_owned());
    let aggregate_request = request(vec![
        MetricSpec::Count {
            name: "records".to_owned(),
        },
        MetricSpec::Sum {
            name: "duration_sum".to_owned(),
            field: duration.clone(),
        },
        MetricSpec::Avg {
            name: "duration_avg".to_owned(),
            field: duration.clone(),
        },
        MetricSpec::Min {
            name: "duration_min".to_owned(),
            field: duration.clone(),
        },
        MetricSpec::Max {
            name: "duration_max".to_owned(),
            field: duration.clone(),
        },
        MetricSpec::Avg {
            name: "missing_avg".to_owned(),
            field: missing,
        },
    ]);

    let page = aggregate(
        &[shard_a.shard.clone(), shard_b.shard.clone()],
        &aggregate_request,
    )?;
    assert_eq!(page.record_count, 5);
    assert_eq!(
        page.metrics[0],
        MetricValue::Count {
            name: "records".to_owned(),
            value: 5,
        }
    );
    assert_eq!(
        numeric_metric(&page.metrics, "duration_sum"),
        (Some(104.0), Some(3))
    );
    let (average, count) = numeric_metric(&page.metrics, "duration_avg");
    assert_eq!(count, Some(3));
    assert!((average.expect("numeric average") - 104.0 / 3.0).abs() <= 1e-9);
    assert_eq!(
        numeric_metric(&page.metrics, "duration_min"),
        (Some(1.0), None)
    );
    assert_eq!(
        numeric_metric(&page.metrics, "duration_max"),
        (Some(100.0), None)
    );
    assert_eq!(
        numeric_metric(&page.metrics, "missing_avg"),
        (None, Some(0))
    );
    assert_eq!(page.warnings.len(), 5);
    assert!(
        page.warnings
            .contains(&AggregateWarning::NumericExclusionCountUnavailable {
                metric: "duration_avg".to_owned(),
                field: duration,
            })
    );
    Ok(())
}

#[test]
fn histogram_uses_utc_grid_and_the_shared_half_open_scope_and_watermark() -> Result<()> {
    let rows = [
        (1, 1, 0, "api"),
        (2, 1, 999_999, "api"),
        (3, 1, 1_000_000, "worker"),
        (4, 1, 3_999_999, "api"),
        (5, 1, 4_000_000, "excluded-end"),
        (6, 2, 2_000_000, "forbidden-project"),
        (11, 1, 2_000_000, "hidden-watermark"),
    ]
    .into_iter()
    .map(|(seq, project, timestamp, service)| record(seq, project, timestamp, service))
    .collect();
    let fixture = fixture("active", vec![rows])?;
    let mut aggregate_request = request(vec![MetricSpec::Count {
        name: "records".to_owned(),
    }]);
    aggregate_request.scope.end_us = 4_000_000;
    aggregate_request.scope.watermark = 10;
    aggregate_request.histogram = Some(HistogramSpec { interval_ms: 1_000 });
    aggregate_request.group_by.push(GroupSpec {
        field: GroupField::Service,
        limit: 2,
    });

    let page = aggregate(std::slice::from_ref(&fixture.shard), &aggregate_request)?;
    assert_eq!(page.record_count, 4);
    let histogram = page.buckets.expect("histogram is present");
    assert_eq!(
        histogram.dimension,
        BucketDimension::Histogram { interval_ms: 1_000 }
    );
    assert!(!histogram.has_more);
    assert_eq!(
        histogram
            .buckets
            .iter()
            .map(|bucket| (bucket.key.clone(), bucket.doc_count))
            .collect::<Vec<_>>(),
        vec![
            (BucketKey::TimestampUs(0), 2),
            (BucketKey::TimestampUs(1_000_000), 1),
            (BucketKey::TimestampUs(2_000_000), 0),
            (BucketKey::TimestampUs(3_000_000), 1),
        ]
    );
    assert!(
        histogram.buckets[2]
            .children
            .as_ref()
            .is_some_and(|children| children.buckets.is_empty())
    );
    Ok(())
}

#[test]
fn exact_bucket_limit_accepts_twenty_thousand_and_rejects_the_next() -> Result<()> {
    let batches = (0..4)
        .map(|batch| {
            let start = batch * 5_000 + 1;
            let end = start + 5_000;
            (start..end)
                .map(|seq| record(seq, 1, seq, &format!("service-{seq:05}")))
                .collect::<Vec<_>>()
        })
        .chain(std::iter::once(vec![record(
            20_001,
            1,
            20_001,
            "service-20001",
        )]))
        .collect();
    let fixture = fixture("cardinality", batches)?;
    let mut aggregate_request = request(Vec::new());
    aggregate_request.scope.end_us = 30_000;
    aggregate_request.scope.watermark = 20_000;
    aggregate_request.group_by.push(GroupSpec {
        field: GroupField::Service,
        limit: 1,
    });

    let exact = aggregate(std::slice::from_ref(&fixture.shard), &aggregate_request)?;
    assert_eq!(exact.record_count, 20_000);
    assert!(exact.buckets.expect("groups").has_more);

    aggregate_request.scope.watermark = 20_001;
    assert!(matches!(
        aggregate(std::slice::from_ref(&fixture.shard), &aggregate_request),
        Err(AggregateError::BucketLimitExceeded {
            limit: 20_000,
            current: 20_001
        })
    ));
    Ok(())
}

#[test]
fn non_finite_native_metric_and_invalid_shapes_fail_closed() -> Result<()> {
    let mut first = record(1, 1, 100, "api");
    first.attributes = json!({"huge": 1e308});
    let mut second = record(2, 1, 101, "api");
    second.attributes = json!({"huge": 1e308});
    let fixture = fixture("active", vec![vec![first, second]])?;
    let huge = NumericField::Json("huge".to_owned());
    let aggregate_request = request(vec![MetricSpec::Sum {
        name: "huge_sum".to_owned(),
        field: huge,
    }]);
    assert!(matches!(
        aggregate(std::slice::from_ref(&fixture.shard), &aggregate_request),
        Err(AggregateError::NumericOverflow { metric }) if metric == "huge_sum"
    ));

    let mut invalid = request(Vec::new());
    invalid.group_by = vec![
        GroupSpec {
            field: GroupField::Level,
            limit: 10,
        },
        GroupSpec {
            field: GroupField::Level,
            limit: 10,
        },
    ];
    assert!(matches!(
        aggregate(&[fixture.shard], &invalid),
        Err(AggregateError::InvalidRequest("duplicate_group_field"))
    ));
    Ok(())
}

#[test]
fn parser_errors_remain_bad_requests_and_empty_numeric_values_are_null() -> Result<()> {
    let fixture = fixture("active", vec![vec![record(1, 1, 100, "api")]])?;
    let mut malformed = request(Vec::new());
    malformed.query = "message:(".to_owned();
    let error = aggregate(std::slice::from_ref(&fixture.shard), &malformed).unwrap_err();
    assert!(error.is_bad_request());

    let mut empty = request(vec![MetricSpec::Avg {
        name: "average".to_owned(),
        field: NumericField::Json("absent".to_owned()),
    }]);
    empty.scope.project_ids.clear();
    let page = aggregate(&[fixture.shard], &empty)?;
    assert_eq!(page.record_count, 0);
    assert_eq!(numeric_metric(&page.metrics, "average"), (None, Some(0)));
    Ok(())
}

#[test]
fn response_dto_never_invents_an_excluded_numeric_count() -> Result<()> {
    let warning = AggregateWarning::NumericExclusionCountUnavailable {
        metric: "duration".to_owned(),
        field: NumericField::Json("duration_ms".to_owned()),
    };
    let encoded: Value = serde_json::to_value(warning)?;
    let text = serde_json::to_string(&encoded)?;
    assert!(text.contains("numeric_exclusion_count_unavailable"));
    assert!(!text.contains("excluded_non_numeric_values"));

    let metric = MetricValue::Number {
        name: "duration".to_owned(),
        op: MetricOp::Avg,
        value: Some(1.0),
        numeric_value_count: Some(1),
    };
    let object = serde_json::to_value(metric)?;
    assert_eq!(object["number"]["numeric_value_count"], 1);
    assert!(
        object["number"]
            .get("excluded_non_numeric_values")
            .is_none()
    );
    Ok(())
}
