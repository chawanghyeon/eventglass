//! Fixed Explore aggregate DTO mapped to the shared read scope and native aggregation.
use super::{
    ApiError, ApiResult, HttpState,
    search::{
        Filters, ProjectId, ReadInput, capture_read, hydrate_candidates, revalidate_read,
        token_error,
    },
};
use crate::search::{
    aggregate::{
        self, AggregateError, AggregateRequest, BucketKey, BucketSet, GroupField, GroupSpec,
        HistogramSpec, MetricOp, MetricSpec, MetricValue, NumericField,
    },
    query::SearchShard,
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
    if input.op == MetricOp::Count {
        if input.field.is_some() {
            return Err(invalid());
        }
        return Ok(MetricSpec::Count { name });
    }
    let field = input.field.ok_or_else(invalid)?;
    let path = field
        .strip_prefix("attributes.")
        .filter(|s| !s.is_empty())
        .ok_or_else(invalid)?;
    let field = NumericField::Json(path.to_owned());
    Ok(match input.op {
        MetricOp::Sum => MetricSpec::Sum { name, field },
        MetricOp::Min => MetricSpec::Min { name, field },
        MetricOp::Max => MetricSpec::Max { name, field },
        MetricOp::Avg => MetricSpec::Avg { name, field },
        MetricOp::Count => unreachable!(),
    })
}
fn histogram(input: HistogramInput, start_us: i64, end_us: i64) -> ApiResult<HistogramSpec> {
    if input.field != "timestamp" {
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
    Json(input): Json<AggregateInput>,
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
    let task = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let pins = indexer.pin_shards(&candidate_ids)?;
        let shards = pins
            .iter()
            .map(|pin| SearchShard {
                id: pin.published().shard_id.clone(),
                searcher: pin.published().searcher.clone(),
            })
            .collect::<Vec<_>>();
        aggregate::aggregate(&shards, &request).map_err(anyhow::Error::from)
    });
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), task)
        .await
        .map_err(|_| ApiError(StatusCode::GATEWAY_TIMEOUT, "query_timeout"))?
        .map_err(|_| unavailable())?
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
