//! Fixed native Tantivy aggregations over the shared scoped query boundary.

use std::{collections::HashSet, error::Error, fmt};

use serde::{Deserialize, Serialize};
use tantivy::{
    TantivyError,
    aggregation::{
        AggContextParams, AggregationError, AggregationLimitsGuard,
        DistributedAggregationCollector,
        agg_req::{Aggregation, AggregationVariants, Aggregations},
        agg_result::{
            AggregationResult, AggregationResults, BucketEntries, BucketResult,
            MetricResult as NativeMetricResult,
        },
        bucket::{DateHistogramAggregationReq, HistogramBounds, TermsAggregation},
        intermediate_agg_result::IntermediateAggregationResults,
        metric::{MaxAggregation, MinAggregation, Stats, StatsAggregation},
    },
    collector::Count,
};

use crate::search::{
    query::{QueryScope, SearchError, SearchShard, TimeField, TypedFilter, scoped_query},
    schema,
};

pub const MAX_METRICS: usize = 8;
pub const MAX_GROUPS: usize = 2;
pub const MAX_BUCKETS: u32 = 20_000;
pub const NATIVE_MEMORY_LIMIT_BYTES: u64 = 16 * 1024 * 1024;
const NATIVE_CANDIDATE_BUCKETS: u32 = MAX_BUCKETS + 1;
const MAX_GROUP_LIMIT: usize = 1_000;
const MAX_METRIC_NAME_BYTES: usize = 128;
const MAX_JSON_PATH_BYTES: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricOp {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NumericField {
    Json(String),
}

impl NumericField {
    fn native_name(&self) -> String {
        match self {
            Self::Json(path) => format!("attributes.{path}"),
        }
    }

    fn path(&self) -> &str {
        match self {
            Self::Json(path) => path,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricSpec {
    Count { name: String },
    Sum { name: String, field: NumericField },
    Min { name: String, field: NumericField },
    Max { name: String, field: NumericField },
    Avg { name: String, field: NumericField },
}

impl MetricSpec {
    fn name(&self) -> &str {
        match self {
            Self::Count { name }
            | Self::Sum { name, .. }
            | Self::Min { name, .. }
            | Self::Max { name, .. }
            | Self::Avg { name, .. } => name,
        }
    }

    fn op(&self) -> MetricOp {
        match self {
            Self::Count { .. } => MetricOp::Count,
            Self::Sum { .. } => MetricOp::Sum,
            Self::Min { .. } => MetricOp::Min,
            Self::Max { .. } => MetricOp::Max,
            Self::Avg { .. } => MetricOp::Avg,
        }
    }

    fn field(&self) -> Option<&NumericField> {
        match self {
            Self::Count { .. } => None,
            Self::Sum { field, .. }
            | Self::Min { field, .. }
            | Self::Max { field, .. }
            | Self::Avg { field, .. } => Some(field),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupField {
    Service,
    Level,
    Environment,
    Release,
}

impl GroupField {
    fn native_name(self) -> &'static str {
        match self {
            Self::Service => "service",
            Self::Level => "level",
            Self::Environment => "environment",
            Self::Release => "release",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupSpec {
    pub field: GroupField,
    /// Presentation top K. Native collection retains every candidate up to the exact bucket cap.
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistogramSpec {
    /// Fixed UTC-grid interval. Tantivy 0.26.1 date histograms have millisecond granularity.
    pub interval_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregateRequest {
    pub query: String,
    pub scope: QueryScope,
    pub filters: Vec<TypedFilter>,
    pub metrics: Vec<MetricSpec>,
    pub group_by: Vec<GroupSpec>,
    pub histogram: Option<HistogramSpec>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricValue {
    Count {
        name: String,
        value: u64,
    },
    Number {
        name: String,
        op: MetricOp,
        value: Option<f64>,
        /// Available for native stats-backed sum/avg, including multi-valued JSON attributes.
        numeric_value_count: Option<u64>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateWarning {
    NumericExclusionCountUnavailable { metric: String, field: NumericField },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BucketDimension {
    Histogram { interval_ms: u64 },
    Group { field: GroupField },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BucketKey {
    TimestampUs(i64),
    String(String),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct BucketSet {
    pub dimension: BucketDimension,
    pub buckets: Vec<AggregateBucket>,
    /// True only when the presentation limit trimmed fully collected group buckets.
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AggregateBucket {
    pub key: BucketKey,
    pub doc_count: u64,
    pub metrics: Vec<MetricValue>,
    pub children: Option<BucketSet>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AggregatePage {
    pub record_count: u64,
    pub metrics: Vec<MetricValue>,
    pub buckets: Option<BucketSet>,
    pub warnings: Vec<AggregateWarning>,
}

#[derive(Debug)]
pub enum AggregateError {
    InvalidRequest(&'static str),
    Search(SearchError),
    BucketLimitExceeded { limit: u32, current: u32 },
    MemoryLimitExceeded { limit: u64, current: u64 },
    NumericOverflow { metric: String },
    InexactNativeResult,
    UnexpectedNativeResult,
    Native(TantivyError),
}

impl AggregateError {
    pub fn is_bad_request(&self) -> bool {
        match self {
            Self::InvalidRequest(_) => true,
            Self::Search(error) => error.is_bad_request(),
            _ => false,
        }
    }

    pub fn is_unprocessable(&self) -> bool {
        matches!(
            self,
            Self::BucketLimitExceeded { .. }
                | Self::MemoryLimitExceeded { .. }
                | Self::NumericOverflow { .. }
        )
    }
}

impl fmt::Display for AggregateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest(code) => write!(formatter, "invalid aggregation request: {code}"),
            Self::Search(error) => error.fmt(formatter),
            Self::BucketLimitExceeded { limit, current } => {
                write!(
                    formatter,
                    "aggregation bucket limit exceeded: {current} > {limit}"
                )
            }
            Self::MemoryLimitExceeded { limit, current } => {
                write!(
                    formatter,
                    "aggregation memory limit exceeded: {current} > {limit}"
                )
            }
            Self::NumericOverflow { metric } => {
                write!(formatter, "aggregation metric {metric} is not finite")
            }
            Self::InexactNativeResult => {
                formatter.write_str("native aggregation discarded candidate buckets")
            }
            Self::UnexpectedNativeResult => {
                formatter.write_str("native aggregation returned an unexpected result shape")
            }
            Self::Native(error) => write!(formatter, "native aggregation failed: {error}"),
        }
    }
}

impl Error for AggregateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Search(error) => Some(error),
            Self::Native(error) => Some(error),
            _ => None,
        }
    }
}

impl From<SearchError> for AggregateError {
    fn from(error: SearchError) -> Self {
        Self::Search(error)
    }
}

impl From<TantivyError> for AggregateError {
    fn from(error: TantivyError) -> Self {
        match error {
            TantivyError::AggregationError(AggregationError::BucketLimitExceeded {
                limit,
                current,
            }) => Self::BucketLimitExceeded { limit, current },
            TantivyError::AggregationError(AggregationError::MemoryExceeded { limit, current }) => {
                Self::MemoryLimitExceeded {
                    limit: limit.get_bytes(),
                    current: current.get_bytes(),
                }
            }
            other => Self::Native(other),
        }
    }
}

type Result<T> = std::result::Result<T, AggregateError>;

#[derive(Debug, Clone)]
enum DimensionPlan {
    Histogram(HistogramSpec),
    Group(GroupSpec),
}

/// Executes a complete distributed native aggregation over all supplied pinned shards.
pub fn aggregate(shards: &[SearchShard], request: &AggregateRequest) -> Result<AggregatePage> {
    aggregate_lazy(shards.len(), |index| Ok(&shards[index]), request)
}

/// Keeps native intermediate results while releasing each shard before opening the next.
pub fn aggregate_lazy<G: AsRef<SearchShard>>(
    shard_count: usize,
    mut load: impl FnMut(usize) -> Result<G>,
    request: &AggregateRequest,
) -> Result<AggregatePage> {
    validate_request(request)?;
    let production_schema = schema::build();
    let query = scoped_query(
        &production_schema,
        &request.query,
        &request.scope,
        &request.filters,
    )?;
    let dimensions = dimensions(request);
    let native_request = native_request(request, &dimensions)?;
    let limits = AggregationLimitsGuard::new(Some(NATIVE_MEMORY_LIMIT_BYTES), Some(MAX_BUCKETS));
    let mut merged = IntermediateAggregationResults::default();
    let mut record_count = 0u64;
    for index in 0..shard_count {
        let guard = load(index)?;
        let shard = guard.as_ref();
        if shard.searcher.schema() != &production_schema {
            return Err(AggregateError::Search(SearchError::SchemaMismatch {
                shard_id: shard.id.clone(),
            }));
        }
        let context =
            AggContextParams::new(limits.clone(), shard.searcher.index().tokenizers().clone());
        let collector = DistributedAggregationCollector::from_aggs(native_request.clone(), context);
        let (shard_count, intermediate) =
            shard.searcher.search(query.as_ref(), &(Count, collector))?;
        let shard_count =
            u64::try_from(shard_count).map_err(|_| AggregateError::UnexpectedNativeResult)?;
        record_count = add_record_count(record_count, shard_count)?;
        merged.merge_fruits(intermediate)?;
    }
    let native_result = merged.into_final_result(native_request, limits)?;
    project_page(native_result, record_count, request, &dimensions)
}

fn project_page(
    native_result: AggregationResults,
    record_count: u64,
    request: &AggregateRequest,
    dimensions: &[DimensionPlan],
) -> Result<AggregatePage> {
    let metrics = project_metrics(&native_result, record_count, &request.metrics)?;
    let buckets = if dimensions.is_empty() {
        None
    } else {
        Some(project_bucket_set(
            &native_result,
            &request.metrics,
            dimensions,
            0,
        )?)
    };
    let warnings = request
        .metrics
        .iter()
        .filter_map(|metric| {
            metric
                .field()
                .map(|field| AggregateWarning::NumericExclusionCountUnavailable {
                    metric: metric.name().to_owned(),
                    field: field.clone(),
                })
        })
        .collect();
    Ok(AggregatePage {
        record_count,
        metrics,
        buckets,
        warnings,
    })
}

fn add_record_count(total: u64, shard: u64) -> Result<u64> {
    total
        .checked_add(shard)
        .ok_or_else(|| AggregateError::NumericOverflow {
            metric: "record_count".to_owned(),
        })
}

fn validate_request(request: &AggregateRequest) -> Result<()> {
    if request.metrics.len() > MAX_METRICS {
        return Err(AggregateError::InvalidRequest("too_many_metrics"));
    }
    if request.group_by.len() > MAX_GROUPS {
        return Err(AggregateError::InvalidRequest("too_many_groups"));
    }
    let mut metric_names = HashSet::with_capacity(request.metrics.len());
    for metric in &request.metrics {
        let name = metric.name();
        if name.is_empty()
            || name.len() > MAX_METRIC_NAME_BYTES
            || name.chars().any(char::is_control)
            || !metric_names.insert(name)
        {
            return Err(AggregateError::InvalidRequest("invalid_metric_name"));
        }
        if let Some(field) = metric.field() {
            validate_json_path(field.path())?;
        }
    }
    let mut group_fields = HashSet::with_capacity(request.group_by.len());
    for group in &request.group_by {
        if group.limit == 0 || group.limit > MAX_GROUP_LIMIT {
            return Err(AggregateError::InvalidRequest("invalid_group_limit"));
        }
        if !group_fields.insert(group.field) {
            return Err(AggregateError::InvalidRequest("duplicate_group_field"));
        }
    }
    if let Some(histogram) = &request.histogram {
        if request.scope.time_field != TimeField::Timestamp {
            return Err(AggregateError::InvalidRequest(
                "histogram_requires_timestamp_scope",
            ));
        }
        if histogram.interval_ms == 0 || histogram.interval_ms > i64::MAX as u64 {
            return Err(AggregateError::InvalidRequest("invalid_histogram_interval"));
        }
    }
    Ok(())
}

fn validate_json_path(path: &str) -> Result<()> {
    if path.is_empty() || path.len() > MAX_JSON_PATH_BYTES || path.ends_with('\\') {
        Err(AggregateError::InvalidRequest("invalid_json_path"))
    } else {
        Ok(())
    }
}

fn dimensions(request: &AggregateRequest) -> Vec<DimensionPlan> {
    let mut dimensions =
        Vec::with_capacity(request.group_by.len() + usize::from(request.histogram.is_some()));
    if let Some(histogram) = &request.histogram {
        dimensions.push(DimensionPlan::Histogram(histogram.clone()));
    }
    dimensions.extend(request.group_by.iter().cloned().map(DimensionPlan::Group));
    dimensions
}

fn native_request(
    request: &AggregateRequest,
    dimensions: &[DimensionPlan],
) -> Result<Aggregations> {
    let metric_aggs = native_metrics(&request.metrics);
    let mut aggregations = metric_aggs.clone();
    if !dimensions.is_empty() {
        aggregations.insert(
            bucket_name(0),
            native_bucket(request, dimensions, 0, &metric_aggs)?,
        );
    }
    Ok(aggregations)
}

fn native_metrics(metrics: &[MetricSpec]) -> Aggregations {
    let mut aggregations = Aggregations::default();
    for (index, metric) in metrics.iter().enumerate() {
        let agg = match metric {
            MetricSpec::Count { .. } => continue,
            MetricSpec::Sum { field, .. } | MetricSpec::Avg { field, .. } => {
                AggregationVariants::Stats(StatsAggregation::from_field_name(field.native_name()))
            }
            MetricSpec::Min { field, .. } => {
                AggregationVariants::Min(MinAggregation::from_field_name(field.native_name()))
            }
            MetricSpec::Max { field, .. } => {
                AggregationVariants::Max(MaxAggregation::from_field_name(field.native_name()))
            }
        };
        aggregations.insert(
            metric_name(index),
            Aggregation {
                agg,
                sub_aggregation: Aggregations::default(),
            },
        );
    }
    aggregations
}

fn native_bucket(
    request: &AggregateRequest,
    dimensions: &[DimensionPlan],
    depth: usize,
    metric_aggs: &Aggregations,
) -> Result<Aggregation> {
    let mut sub_aggregation = metric_aggs.clone();
    if depth + 1 < dimensions.len() {
        sub_aggregation.insert(
            bucket_name(depth + 1),
            native_bucket(request, dimensions, depth + 1, metric_aggs)?,
        );
    }
    let agg = match &dimensions[depth] {
        DimensionPlan::Group(group) => AggregationVariants::Terms(TermsAggregation {
            field: group.field.native_name().to_owned(),
            size: Some(NATIVE_CANDIDATE_BUCKETS),
            segment_size: Some(NATIVE_CANDIDATE_BUCKETS),
            show_term_doc_count_error: Some(true),
            min_doc_count: Some(1),
            ..TermsAggregation::default()
        }),
        DimensionPlan::Histogram(histogram) => {
            let bounds = histogram_bounds(&request.scope, histogram.interval_ms)?;
            AggregationVariants::DateHistogram(DateHistogramAggregationReq {
                field: "timestamp".to_owned(),
                fixed_interval: Some(format!("{}ms", histogram.interval_ms)),
                min_doc_count: Some(0),
                // The shared native query is the authoritative `[start,end)` value filter.
                // These bucket-key bounds only extend the UTC grid to include empty buckets.
                hard_bounds: None,
                extended_bounds: Some(bounds),
                ..DateHistogramAggregationReq::default()
            })
        }
    };
    Ok(Aggregation {
        agg,
        sub_aggregation,
    })
}

fn histogram_bounds(scope: &QueryScope, interval_ms: u64) -> Result<HistogramBounds> {
    let interval_us = i64::try_from(interval_ms)
        .ok()
        .and_then(|interval| interval.checked_mul(1_000))
        .ok_or(AggregateError::InvalidRequest("invalid_histogram_interval"))?;
    let last_us = scope
        .end_us
        .checked_sub(1)
        .ok_or(AggregateError::InvalidRequest("invalid_time_range"))?;
    let first_bucket_us = scope.start_us.div_euclid(interval_us) * interval_us;
    let last_bucket_us = last_us.div_euclid(interval_us) * interval_us;
    Ok(HistogramBounds {
        min: (first_bucket_us / 1_000) as f64,
        max: (last_bucket_us / 1_000) as f64,
    })
}

fn metric_name(index: usize) -> String {
    format!("__eventglass_metric_{index}")
}

fn bucket_name(depth: usize) -> String {
    format!("__eventglass_bucket_{depth}")
}

fn project_metrics(
    results: &AggregationResults,
    doc_count: u64,
    specs: &[MetricSpec],
) -> Result<Vec<MetricValue>> {
    specs
        .iter()
        .enumerate()
        .map(|(index, spec)| match spec {
            MetricSpec::Count { name } => Ok(MetricValue::Count {
                name: name.clone(),
                value: doc_count,
            }),
            MetricSpec::Sum { name, .. } | MetricSpec::Avg { name, .. } => {
                let stats = native_stats(results, index)?;
                let value = if stats.count == 0 {
                    None
                } else if matches!(spec, MetricSpec::Sum { .. }) {
                    Some(stats.sum)
                } else {
                    stats.avg
                };
                validate_finite(name, value)?;
                Ok(MetricValue::Number {
                    name: name.clone(),
                    op: spec.op(),
                    value,
                    numeric_value_count: Some(stats.count),
                })
            }
            MetricSpec::Min { name, .. } => {
                let value = native_single(results, index, MetricOp::Min)?;
                validate_finite(name, value)?;
                Ok(MetricValue::Number {
                    name: name.clone(),
                    op: MetricOp::Min,
                    value,
                    numeric_value_count: None,
                })
            }
            MetricSpec::Max { name, .. } => {
                let value = native_single(results, index, MetricOp::Max)?;
                validate_finite(name, value)?;
                Ok(MetricValue::Number {
                    name: name.clone(),
                    op: MetricOp::Max,
                    value,
                    numeric_value_count: None,
                })
            }
        })
        .collect()
}

fn native_stats(results: &AggregationResults, index: usize) -> Result<&Stats> {
    match results.0.get(&metric_name(index)) {
        Some(AggregationResult::MetricResult(NativeMetricResult::Stats(stats))) => Ok(stats),
        _ => Err(AggregateError::UnexpectedNativeResult),
    }
}

fn native_single(results: &AggregationResults, index: usize, op: MetricOp) -> Result<Option<f64>> {
    let metric = results
        .0
        .get(&metric_name(index))
        .ok_or(AggregateError::UnexpectedNativeResult)?;
    match (op, metric) {
        (MetricOp::Min, AggregationResult::MetricResult(NativeMetricResult::Min(value)))
        | (MetricOp::Max, AggregationResult::MetricResult(NativeMetricResult::Max(value))) => {
            Ok(value.value)
        }
        _ => Err(AggregateError::UnexpectedNativeResult),
    }
}

fn validate_finite(name: &str, value: Option<f64>) -> Result<()> {
    if value.is_some_and(|value| !value.is_finite()) {
        Err(AggregateError::NumericOverflow {
            metric: name.to_owned(),
        })
    } else {
        Ok(())
    }
}

fn project_bucket_set(
    results: &AggregationResults,
    metrics: &[MetricSpec],
    dimensions: &[DimensionPlan],
    depth: usize,
) -> Result<BucketSet> {
    let result = results
        .0
        .get(&bucket_name(depth))
        .ok_or(AggregateError::UnexpectedNativeResult)?;
    match (&dimensions[depth], result) {
        (
            DimensionPlan::Group(group),
            AggregationResult::BucketResult(BucketResult::Terms {
                buckets,
                sum_other_doc_count,
                doc_count_error_upper_bound,
            }),
        ) => {
            if *sum_other_doc_count != 0 || doc_count_error_upper_bound.unwrap_or(0) != 0 {
                return Err(AggregateError::InexactNativeResult);
            }
            let mut buckets = buckets
                .iter()
                .map(|bucket| Ok((bucket, native_string_key(bucket)?)))
                .collect::<Result<Vec<_>>>()?;
            buckets.sort_by(|(left, left_key), (right, right_key)| {
                right
                    .doc_count
                    .cmp(&left.doc_count)
                    .then_with(|| left_key.cmp(right_key))
            });
            let has_more = buckets.len() > group.limit;
            let buckets = buckets
                .into_iter()
                .take(group.limit)
                .map(|(bucket, native_key)| {
                    let key = BucketKey::String(native_key.to_owned());
                    project_bucket(
                        key,
                        bucket.doc_count,
                        &bucket.sub_aggregation,
                        metrics,
                        dimensions,
                        depth,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(BucketSet {
                dimension: BucketDimension::Group { field: group.field },
                buckets,
                has_more,
            })
        }
        (
            DimensionPlan::Histogram(histogram),
            AggregationResult::BucketResult(BucketResult::Histogram {
                buckets: BucketEntries::Vec(buckets),
            }),
        ) => {
            let mut buckets = buckets
                .iter()
                .map(|bucket| Ok((bucket, histogram_key_us(bucket)?)))
                .collect::<Result<Vec<_>>>()?;
            buckets.sort_by_key(|(_, key)| *key);
            let buckets = buckets
                .into_iter()
                .map(|(bucket, key)| {
                    project_bucket(
                        BucketKey::TimestampUs(key),
                        bucket.doc_count,
                        &bucket.sub_aggregation,
                        metrics,
                        dimensions,
                        depth,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(BucketSet {
                dimension: BucketDimension::Histogram {
                    interval_ms: histogram.interval_ms,
                },
                buckets,
                has_more: false,
            })
        }
        _ => Err(AggregateError::UnexpectedNativeResult),
    }
}

fn project_bucket(
    key: BucketKey,
    doc_count: u64,
    results: &AggregationResults,
    metrics: &[MetricSpec],
    dimensions: &[DimensionPlan],
    depth: usize,
) -> Result<AggregateBucket> {
    let projected_metrics = project_metrics(results, doc_count, metrics)?;
    let children = if depth + 1 < dimensions.len() {
        Some(project_bucket_set(results, metrics, dimensions, depth + 1)?)
    } else {
        None
    };
    Ok(AggregateBucket {
        key,
        doc_count,
        metrics: projected_metrics,
        children,
    })
}

fn native_string_key(bucket: &tantivy::aggregation::agg_result::BucketEntry) -> Result<&str> {
    match &bucket.key {
        tantivy::aggregation::Key::Str(value) => Ok(value),
        _ => Err(AggregateError::UnexpectedNativeResult),
    }
}

fn histogram_key_us(bucket: &tantivy::aggregation::agg_result::BucketEntry) -> Result<i64> {
    let milliseconds = match bucket.key {
        tantivy::aggregation::Key::F64(value)
            if value.is_finite()
                && value.fract() == 0.0
                && value >= i64::MIN as f64
                && value <= i64::MAX as f64 =>
        {
            value as i64
        }
        tantivy::aggregation::Key::I64(value) => value,
        _ => return Err(AggregateError::UnexpectedNativeResult),
    };
    milliseconds
        .checked_mul(1_000)
        .ok_or(AggregateError::UnexpectedNativeResult)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::aggregation::{Key, agg_result::BucketEntry, metric::SingleMetricResult};

    fn request() -> AggregateRequest {
        AggregateRequest {
            query: String::new(),
            scope: QueryScope {
                project_ids: vec![1],
                start_us: 0,
                end_us: 1,
                watermark: 1,
                time_field: TimeField::Timestamp,
            },
            filters: Vec::new(),
            metrics: Vec::new(),
            group_by: Vec::new(),
            histogram: None,
        }
    }

    #[test]
    fn metric_group_and_error_variants_have_stable_mappings() {
        let field = NumericField::Json("duration".into());
        let metrics = [
            MetricSpec::Count {
                name: "count".into(),
            },
            MetricSpec::Sum {
                name: "sum".into(),
                field: field.clone(),
            },
            MetricSpec::Min {
                name: "min".into(),
                field: field.clone(),
            },
            MetricSpec::Max {
                name: "max".into(),
                field: field.clone(),
            },
            MetricSpec::Avg {
                name: "avg".into(),
                field,
            },
        ];
        assert_eq!(
            metrics.map(|metric| metric.op()),
            [
                MetricOp::Count,
                MetricOp::Sum,
                MetricOp::Min,
                MetricOp::Max,
                MetricOp::Avg
            ]
        );
        assert_eq!(
            [
                GroupField::Service,
                GroupField::Level,
                GroupField::Environment,
                GroupField::Release
            ]
            .map(GroupField::native_name),
            ["service", "level", "environment", "release"]
        );

        let errors = [
            AggregateError::InvalidRequest("bad"),
            AggregateError::BucketLimitExceeded {
                limit: 1,
                current: 2,
            },
            AggregateError::MemoryLimitExceeded {
                limit: 1,
                current: 2,
            },
            AggregateError::NumericOverflow { metric: "m".into() },
            AggregateError::InexactNativeResult,
            AggregateError::UnexpectedNativeResult,
            AggregateError::Native(TantivyError::InvalidArgument("bad".into())),
        ];
        for error in &errors {
            assert!(!error.to_string().is_empty());
        }
        assert!(!errors[0].is_unprocessable());
        assert!(errors[1].is_unprocessable());
        assert!(errors[2].is_unprocessable());
        assert!(errors[3].is_unprocessable());
        assert!(Error::source(&errors[6]).is_some());
        assert!(Error::source(&errors[0]).is_none());

        let search = AggregateError::from(SearchError::InvalidQuery);
        assert!(search.to_string().contains("invalid Tantivy query"));
        assert!(Error::source(&search).is_some());

        let bucket = AggregateError::from(TantivyError::AggregationError(
            AggregationError::BucketLimitExceeded {
                limit: 10,
                current: 11,
            },
        ));
        assert!(matches!(
            bucket,
            AggregateError::BucketLimitExceeded {
                limit: 10,
                current: 11
            }
        ));
        let memory = AggregateError::from(TantivyError::AggregationError(
            AggregationError::MemoryExceeded {
                limit: tantivy::ByteCount::from(10u64),
                current: tantivy::ByteCount::from(11u64),
            },
        ));
        assert!(matches!(
            memory,
            AggregateError::MemoryLimitExceeded {
                limit: 10,
                current: 11
            }
        ));
        let native = AggregateError::from(TantivyError::InvalidArgument("bad".into()));
        assert!(matches!(native, AggregateError::Native(_)));
        assert_eq!(add_record_count(1, 2).expect("bounded count"), 3);
        assert!(matches!(
            add_record_count(u64::MAX, 1),
            Err(AggregateError::NumericOverflow { metric }) if metric == "record_count"
        ));
    }

    #[test]
    fn request_validation_rejects_every_aggregation_dimension() {
        let mut input = request();
        input.metrics = (0..=MAX_METRICS)
            .map(|index| MetricSpec::Count {
                name: index.to_string(),
            })
            .collect();
        assert!(validate_request(&input).is_err());
        input = request();
        input.group_by = (0..=MAX_GROUPS)
            .map(|_| GroupSpec {
                field: GroupField::Service,
                limit: 1,
            })
            .collect();
        assert!(validate_request(&input).is_err());
        for name in ["", "\n", &"x".repeat(MAX_METRIC_NAME_BYTES + 1)] {
            input = request();
            input.metrics.push(MetricSpec::Count { name: name.into() });
            assert!(validate_request(&input).is_err());
        }
        input = request();
        input.metrics = vec![
            MetricSpec::Count {
                name: "same".into(),
            },
            MetricSpec::Count {
                name: "same".into(),
            },
        ];
        assert!(validate_request(&input).is_err());
        for path in ["", "tail\\", &"x".repeat(MAX_JSON_PATH_BYTES + 1)] {
            input = request();
            input.metrics.push(MetricSpec::Sum {
                name: "sum".into(),
                field: NumericField::Json(path.into()),
            });
            assert!(validate_request(&input).is_err());
        }
        for limit in [0, MAX_GROUP_LIMIT + 1] {
            input = request();
            input.group_by.push(GroupSpec {
                field: GroupField::Service,
                limit,
            });
            assert!(validate_request(&input).is_err());
        }
        input = request();
        input.group_by = vec![
            GroupSpec {
                field: GroupField::Service,
                limit: 1,
            },
            GroupSpec {
                field: GroupField::Service,
                limit: 1,
            },
        ];
        assert!(validate_request(&input).is_err());
        input = request();
        input.scope.time_field = TimeField::ReceivedAt;
        input.histogram = Some(HistogramSpec { interval_ms: 1 });
        assert!(validate_request(&input).is_err());
        input.scope.time_field = TimeField::Timestamp;
        input.histogram = Some(HistogramSpec { interval_ms: 0 });
        assert!(validate_request(&input).is_err());
    }

    #[test]
    fn native_projection_rejects_wrong_shapes_nonfinite_values_and_keys() {
        let empty = AggregationResults::default();
        assert!(native_stats(&empty, 0).is_err());
        assert!(native_single(&empty, 0, MetricOp::Min).is_err());
        assert!(validate_finite("metric", Some(f64::NAN)).is_err());
        assert!(validate_finite("metric", None).is_ok());

        let mut results = AggregationResults::default();
        results.0.insert(
            metric_name(0),
            AggregationResult::MetricResult(NativeMetricResult::Max(SingleMetricResult::from(1.0))),
        );
        assert!(native_single(&results, 0, MetricOp::Min).is_err());
        let bucket = |key| BucketEntry {
            key_as_string: None,
            key,
            doc_count: 1,
            sub_aggregation: AggregationResults::default(),
        };
        assert!(native_string_key(&bucket(Key::I64(1))).is_err());
        assert_eq!(histogram_key_us(&bucket(Key::I64(2))).unwrap(), 2_000);
        assert_eq!(histogram_key_us(&bucket(Key::F64(2.0))).unwrap(), 2_000);
        assert!(histogram_key_us(&bucket(Key::F64(2.5))).is_err());
        assert!(histogram_key_us(&bucket(Key::U64(2))).is_err());
        assert!(histogram_key_us(&bucket(Key::I64(i64::MAX))).is_err());

        let dimensions = [DimensionPlan::Group(GroupSpec {
            field: GroupField::Service,
            limit: 1,
        })];
        let mut inexact = AggregationResults::default();
        inexact.0.insert(
            bucket_name(0),
            AggregationResult::BucketResult(BucketResult::Terms {
                buckets: Vec::new(),
                sum_other_doc_count: 1,
                doc_count_error_upper_bound: None,
            }),
        );
        assert!(matches!(
            project_bucket_set(&inexact, &[], &dimensions, 0),
            Err(AggregateError::InexactNativeResult)
        ));

        let mut wrong = AggregationResults::default();
        wrong.0.insert(
            bucket_name(0),
            AggregationResult::MetricResult(NativeMetricResult::Max(SingleMetricResult::from(1.0))),
        );
        assert!(matches!(
            project_bucket_set(&wrong, &[], &dimensions, 0),
            Err(AggregateError::UnexpectedNativeResult)
        ));

        let request = request();
        assert!(project_page(wrong, 0, &request, &dimensions).is_err());
    }
}
