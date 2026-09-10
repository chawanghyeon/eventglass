//! Fixed Explore aggregate DTO mapped to the shared read scope and native aggregation.
use super::ApiJson;

use super::{
    ApiError, ApiResult, HttpState,
    search::{
        Filters, ProjectId, ReadInput, capture_read, hydrate_candidates, revalidate_read,
        token_error,
    },
};
use crate::search::{
    aggregate::{
        AggregateError, AggregateRequest, BucketKey, BucketSet, GroupField, GroupSpec,
        HistogramSpec, MetricOp, MetricSpec, MetricValue, NumericField,
    },
    tokens::Position,
};
use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use serde::Deserialize;
use serde_json::{Value, json};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AggregateInput {
    #[serde(default)]
    projects: Vec<ProjectId>,
    start: String,
    end: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    filters: Filters,
    read_token: Option<String>,
    #[serde(default)]
    metrics: Vec<MetricInput>,
    #[serde(default)]
    group_by: Vec<GroupField>,
    group_limit: Option<usize>,
    histogram: Option<HistogramInput>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MetricInput {
    op: MetricOp,
    name: Option<String>,
    field: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HistogramInput {
    field: String,
    interval: String,
}
fn invalid() -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, "invalid_aggregate_request")
}
fn unavailable() -> ApiError {
    ApiError(StatusCode::SERVICE_UNAVAILABLE, "aggregate_unavailable")
}
fn metric(input: MetricInput, index: usize) -> ApiResult<MetricSpec> {
    let name = input.name.unwrap_or_else(|| format!("metric_{index}"));
    let numeric = |field: Option<String>| {
        let field = field.ok_or_else(invalid)?;
        let path = field
            .strip_prefix("attributes.")
            .filter(|s| !s.is_empty())
            .ok_or_else(invalid)?;
        Ok::<_, ApiError>(NumericField::Json(path.to_owned()))
    };
    match input.op {
        MetricOp::Count if input.field.is_none() => Ok(MetricSpec::Count { name }),
        MetricOp::Count => Err(invalid()),
        MetricOp::Sum => Ok(MetricSpec::Sum {
            name,
            field: numeric(input.field)?,
        }),
        MetricOp::Min => Ok(MetricSpec::Min {
            name,
            field: numeric(input.field)?,
        }),
        MetricOp::Max => Ok(MetricSpec::Max {
            name,
            field: numeric(input.field)?,
        }),
        MetricOp::Avg => Ok(MetricSpec::Avg {
            name,
            field: numeric(input.field)?,
        }),
    }
}
fn histogram(input: HistogramInput, start_us: i64, end_us: i64) -> ApiResult<HistogramSpec> {
    if input.field != "timestamp" || end_us <= start_us {
        return Err(invalid());
    }
    let span_ms = u64::try_from((i128::from(end_us) - i128::from(start_us) + 999) / 1000)
        .map_err(|_| invalid())?;
    let interval_ms = match input.interval.as_str() {
        "10s" => 10_000,
        "1m" => 60_000,
        "5m" => 300_000,
        "1h" => 3_600_000,
        "6h" => 21_600_000,
        "1d" => 86_400_000,
        "auto" => match span_ms {
            0..=3_600_000 => 10_000,
            3_600_001..=21_600_000 => 60_000,
            21_600_001..=86_400_000 => 300_000,
            86_400_001..=604_800_000 => 3_600_000,
            604_800_001..=2_592_000_000 => 21_600_000,
            _ => {
                let minimum = span_ms.div_ceil(19_999);
                minimum.div_ceil(86_400_000).max(1) * 86_400_000
            }
        },
        _ => return Err(invalid()),
    };
    Ok(HistogramSpec { interval_ms })
}
fn metric_value(value: MetricValue) -> Value {
    match value {
        MetricValue::Count { name, value } => {
            json!({"name":name,"op":"count","value":value.to_string(),"numeric_value_count":null})
        }
        MetricValue::Number {
            name,
            op,
            value,
            numeric_value_count,
        } => {
            json!({"name":name,"op":op,"value":value,"numeric_value_count":numeric_value_count.map(|n|n.to_string())})
        }
    }
}
fn buckets(set: BucketSet) -> Value {
    let values:Vec<_>=set.buckets.into_iter().map(|bucket| {
        let key=match bucket.key {BucketKey::TimestampUs(value)=>json!({"type":"timestamp","timestamp_us":value.to_string()}),BucketKey::String(value)=>json!({"type":"string","value":value})};
        json!({"key":key,"doc_count":bucket.doc_count.to_string(),"metrics":bucket.metrics.into_iter().map(metric_value).collect::<Vec<_>>(),"children":bucket.children.map(buckets)})
    }).collect();
    json!({"dimension":set.dimension,"buckets":values,"has_more":set.has_more})
}
fn native_error(error: &AggregateError) -> ApiError {
    if error.is_bad_request() {
        return invalid();
    }
    match error {
        AggregateError::BucketLimitExceeded { .. } => {
            ApiError(StatusCode::UNPROCESSABLE_ENTITY, "bucket_limit_exceeded")
        }
        AggregateError::MemoryLimitExceeded { .. } => {
            ApiError(StatusCode::UNPROCESSABLE_ENTITY, "aggregation_memory_limit")
        }
        AggregateError::NumericOverflow { .. } => {
            ApiError(StatusCode::UNPROCESSABLE_ENTITY, "numeric_overflow")
        }
        _ => unavailable(),
    }
}
pub(super) async fn post_aggregate(
    State(state): State<HttpState>,
    headers: HeaderMap,
    ApiJson(input): ApiJson<AggregateInput>,
) -> ApiResult<Json<Value>> {
    let started = std::time::Instant::now();
    if input.metrics.len() > 8 || input.group_by.len() > 2 {
        return Err(invalid());
    }
    let group_limit = input.group_limit.unwrap_or(10);
    if !(1..=1000).contains(&group_limit) {
        return Err(invalid());
    }
    let metrics = input
        .metrics
        .into_iter()
        .enumerate()
        .map(|(index, value)| metric(value, index))
        .collect::<ApiResult<Vec<_>>>()?;
    let prepared = capture_read(
        &state,
        &headers,
        ReadInput {
            projects: input.projects,
            start: input.start,
            end: input.end,
            query: input.query,
            filters: input.filters,
            read_token: input.read_token,
        },
    )
    .await?;
    let super::search::ReadView {
        auth,
        candidate_ids,
        read_context,
        scope,
        query,
        filters,
        ..
    } = prepared;
    let hydrated_shards = hydrate_candidates(&state, &candidate_ids).await?;
    let histogram = input
        .histogram
        .map(|value| histogram(value, scope.start_us, scope.end_us))
        .transpose()?;
    let watermark = scope.watermark;
    let request = AggregateRequest {
        query,
        scope,
        filters,
        metrics,
        group_by: input
            .group_by
            .into_iter()
            .map(|field| GroupSpec {
                field,
                limit: group_limit,
            })
            .collect(),
        histogram,
    };
    let permit = state
        .app
        .query_permit
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError(StatusCode::TOO_MANY_REQUESTS, "query_busy"))?;
    let indexer = state.app.indexer.clone().ok_or_else(unavailable)?;
    let searched_shards = candidate_ids.len();
    let result = super::run_native(permit, std::time::Duration::from_secs(10), move || {
        indexer.aggregate(&candidate_ids, &request)
    })
    .await
    .map_err(|failure| match failure {
        super::NativeTaskFailure::Timeout => ApiError(StatusCode::GATEWAY_TIMEOUT, "query_timeout"),
        super::NativeTaskFailure::Join => unavailable(),
    })?
    .map_err(|error| {
        error
            .downcast_ref::<AggregateError>()
            .map_or_else(unavailable, native_error)
    })?;
    revalidate_read(&state, &headers, &auth).await?;
    let read_token = state
        .app
        .tokens
        .issue(
            read_context,
            watermark,
            Position::Read,
            crate::model::now_us()?,
        )
        .map_err(token_error)?;
    Ok(Json(
        json!({"record_count":result.record_count.to_string(),"metrics":result.metrics.into_iter().map(metric_value).collect::<Vec<_>>(),
        "buckets":result.buckets.map(buckets),"warnings":result.warnings,"read_token":read_token,"watermark":watermark.to_string(),
        "complete":true,"took_ms":started.elapsed().as_millis().to_string(),"searched_shards":searched_shards.to_string(),"hydrated_shards":hydrated_shards.to_string()}),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::aggregate::{AggregateBucket, BucketDimension};

    fn metric_input(op: MetricOp, field: Option<&str>) -> MetricInput {
        MetricInput {
            op,
            name: None,
            field: field.map(str::to_owned),
        }
    }

    #[test]
    fn metric_dto_rejects_ambiguous_fields_and_maps_every_operation() {
        assert!(
            matches!(metric(metric_input(MetricOp::Count, None), 2).unwrap(), MetricSpec::Count { ref name } if name == "metric_2")
        );
        assert!(metric(metric_input(MetricOp::Count, Some("attributes.ms")), 0).is_err());
        assert!(metric(metric_input(MetricOp::Sum, None), 0).is_err());
        assert!(metric(metric_input(MetricOp::Sum, Some("ms")), 0).is_err());
        assert!(metric(metric_input(MetricOp::Min, Some("attributes.")), 0).is_err());
        for op in [MetricOp::Sum, MetricOp::Min, MetricOp::Max, MetricOp::Avg] {
            assert!(metric(metric_input(op, Some("attributes.ms")), 0).is_ok());
        }
    }

    #[test]
    fn histogram_dto_maps_fixed_and_auto_intervals_and_rejects_invalid_input() {
        for (interval, expected) in [
            ("10s", 10_000),
            ("1m", 60_000),
            ("5m", 300_000),
            ("1h", 3_600_000),
            ("6h", 21_600_000),
            ("1d", 86_400_000),
        ] {
            assert_eq!(
                histogram(
                    HistogramInput {
                        field: "timestamp".into(),
                        interval: interval.into()
                    },
                    0,
                    1
                )
                .unwrap()
                .interval_ms,
                expected
            );
        }
        for (span_ms, expected) in [
            (1, 10_000),
            (3_600_001, 60_000),
            (21_600_001, 300_000),
            (86_400_001, 3_600_000),
            (604_800_001, 21_600_000),
            (2_592_000_001, 86_400_000),
        ] {
            assert_eq!(
                histogram(
                    HistogramInput {
                        field: "timestamp".into(),
                        interval: "auto".into()
                    },
                    0,
                    span_ms * 1_000
                )
                .unwrap()
                .interval_ms,
                expected
            );
        }
        assert!(
            histogram(
                HistogramInput {
                    field: "received_at".into(),
                    interval: "1m".into()
                },
                0,
                1
            )
            .is_err()
        );
        assert!(
            histogram(
                HistogramInput {
                    field: "timestamp".into(),
                    interval: "auto".into(),
                },
                1,
                1
            )
            .is_err()
        );
        assert!(
            histogram(
                HistogramInput {
                    field: "timestamp".into(),
                    interval: "2m".into()
                },
                0,
                1
            )
            .is_err()
        );
        assert!(
            histogram(
                HistogramInput {
                    field: "timestamp".into(),
                    interval: "auto".into()
                },
                1,
                0
            )
            .is_err()
        );
    }

    #[test]
    fn aggregate_response_mapping_preserves_types_children_and_error_statuses() {
        let values = [
            metric_value(MetricValue::Count {
                name: "count".into(),
                value: 3,
            }),
            metric_value(MetricValue::Number {
                name: "avg".into(),
                op: MetricOp::Avg,
                value: Some(1.5),
                numeric_value_count: Some(2),
            }),
        ];
        assert_eq!(values[0]["value"], "3");
        assert_eq!(values[1]["numeric_value_count"], "2");
        let child = BucketSet {
            dimension: BucketDimension::Group {
                field: GroupField::Level,
            },
            buckets: vec![],
            has_more: false,
        };
        let value = buckets(BucketSet {
            dimension: BucketDimension::Histogram { interval_ms: 1_000 },
            buckets: vec![
                AggregateBucket {
                    key: BucketKey::TimestampUs(1),
                    doc_count: 2,
                    metrics: vec![],
                    children: Some(child),
                },
                AggregateBucket {
                    key: BucketKey::String("info".into()),
                    doc_count: 1,
                    metrics: vec![],
                    children: None,
                },
            ],
            has_more: true,
        });
        assert_eq!(value["buckets"][0]["key"]["type"], "timestamp");
        assert_eq!(value["buckets"][1]["key"]["type"], "string");

        for (error, status) in [
            (
                AggregateError::InvalidRequest("bad"),
                StatusCode::BAD_REQUEST,
            ),
            (
                AggregateError::BucketLimitExceeded {
                    limit: 1,
                    current: 2,
                },
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                AggregateError::MemoryLimitExceeded {
                    limit: 1,
                    current: 2,
                },
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                AggregateError::NumericOverflow { metric: "x".into() },
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                AggregateError::UnexpectedNativeResult,
                StatusCode::SERVICE_UNAVAILABLE,
            ),
        ] {
            assert_eq!(native_error(&error).0, status);
        }
    }
}
