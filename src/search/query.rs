//! Strict, scoped native Tantivy row search.

use std::{cmp::Reverse, collections::BinaryHeap, error::Error, fmt, ops::Bound};

use serde::{Deserialize, Serialize};
use tantivy::{
    DateTime, DocAddress, Order, Searcher, TantivyDocument, Term,
    collector::{TopDocs, sort_key::SortByStaticFastValue},
    query::{AllQuery, BooleanQuery, Occur, Query, QueryParser, RangeQuery, TermQuery},
    query_grammar::{UserInputAst, UserInputLeaf},
    schema::{Field, IndexRecordOption, Schema, Value},
    tokenizer::TokenizerManager,
};

use crate::{model::RecordKind, search::schema};

pub const MAX_QUERY_BYTES: usize = 8 * 1024;
pub const MAX_QUERY_CLAUSES: usize = 64;
pub const MAX_QUERY_DEPTH: usize = 16;
pub const MAX_SEARCH_LIMIT: usize = 1_000;
pub const MAX_ROW_STRING_BYTES: usize = 16 * 1024 * 1024;
const MAX_PROJECTS: usize = 1_000;
const MAX_FILTERS: usize = 64;
const MAX_FILTER_VALUES: usize = 64;
const MAX_FILTER_STRING_BYTES: usize = 8 * 1024;
const MAX_JSON_PATH_BYTES: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeField {
    Timestamp,
    ReceivedAt,
}

impl TimeField {
    fn field_name(self) -> &'static str {
        match self {
            Self::Timestamp => "timestamp",
            Self::ReceivedAt => "received_at",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryScope {
    pub project_ids: Vec<i64>,
    pub start_us: i64,
    pub end_us: i64,
    pub watermark: i64,
    pub time_field: TimeField,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowCursor {
    /// The last row's value for the request's selected [`TimeField`].
    pub timestamp_us: i64,
    pub ingest_seq: i64,
    /// Retained as signed-token identity. Global ordering is fully determined by ingest sequence.
    pub record_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeywordField {
    RecordId,
    Kind,
    Service,
    Level,
    Environment,
    Release,
    Logger,
    IssueId,
    Fingerprint,
    TraceId,
    SpanId,
    RequestId,
    UserId,
    UserEmail,
}

impl KeywordField {
    fn field_name(self) -> &'static str {
        match self {
            Self::RecordId => "record_id",
            Self::Kind => "kind",
            Self::Service => "service",
            Self::Level => "level",
            Self::Environment => "environment",
            Self::Release => "release",
            Self::Logger => "logger",
            Self::IssueId => "issue_id",
            Self::Fingerprint => "fingerprint",
            Self::TraceId => "trace_id",
            Self::SpanId => "span_id",
            Self::RequestId => "request_id",
            Self::UserId => "user_id",
            Self::UserEmail => "user_email",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsonScalar {
    String(String),
    I64(i64),
    U64(u64),
    F64(f64),
    Bool(bool),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum I64Bound {
    Unbounded,
    Included(i64),
    Excluded(i64),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TypedFilter {
    KeywordAny {
        field: KeywordField,
        values: Vec<String>,
    },
    JsonEq {
        path: String,
        value: JsonScalar,
    },
    JsonI64Range {
        path: String,
        lower: I64Bound,
        upper: I64Bound,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchRequest {
    pub query: String,
    pub scope: QueryScope,
    pub filters: Vec<TypedFilter>,
    pub cursor: Option<RowCursor>,
    pub limit: usize,
}

#[derive(Clone)]
pub struct SearchShard {
    pub id: String,
    pub searcher: Searcher,
}

impl AsRef<SearchShard> for SearchShard {
    fn as_ref(&self) -> &SearchShard {
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LogRow {
    pub shard_id: String,
    pub record_id: String,
    pub kind: RecordKind,
    pub project_id: i64,
    pub ingest_seq: i64,
    pub timestamp_us: i64,
    pub received_at_us: i64,
    pub service: String,
    pub level: String,
    pub message: String,
    pub environment: Option<String>,
    pub release: Option<String>,
    pub logger: Option<String>,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub request_id: Option<String>,
    pub issue_id: Option<String>,
    pub fingerprint: Option<String>,
    pub user_id: Option<String>,
    pub user_email: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SearchPage {
    pub rows: Vec<LogRow>,
    pub has_more: bool,
}

#[derive(Debug)]
pub enum SearchError {
    ShardUnavailable,
    InvalidRequest(&'static str),
    InvalidQuery,
    SchemaMismatch {
        shard_id: String,
    },
    CorruptDocument {
        shard_id: String,
        field: &'static str,
    },
    ResultTooLarge {
        limit_bytes: usize,
    },
    Native(tantivy::TantivyError),
}

impl SearchError {
    pub fn is_bad_request(&self) -> bool {
        matches!(self, Self::InvalidRequest(_) | Self::InvalidQuery)
    }

    pub fn is_unprocessable(&self) -> bool {
        matches!(self, Self::ResultTooLarge { .. })
    }
}

impl fmt::Display for SearchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ShardUnavailable => formatter.write_str("candidate shard is unavailable"),
            Self::InvalidRequest(code) => write!(formatter, "invalid search request: {code}"),
            Self::InvalidQuery => formatter.write_str("invalid Tantivy query syntax"),
            Self::SchemaMismatch { shard_id } => {
                write!(formatter, "shard {shard_id} has an incompatible schema")
            }
            Self::CorruptDocument { shard_id, field } => {
                write!(formatter, "shard {shard_id} has a document missing {field}")
            }
            Self::ResultTooLarge { limit_bytes } => {
                write!(formatter, "search row strings exceed {limit_bytes} bytes")
            }
            Self::Native(error) => write!(formatter, "native search failed: {error}"),
        }
    }
}

impl Error for SearchError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Native(error) => Some(error),
            _ => None,
        }
    }
}

impl From<tantivy::TantivyError> for SearchError {
    fn from(error: tantivy::TantivyError) -> Self {
        Self::Native(error)
    }
}

type Result<T> = std::result::Result<T, SearchError>;

pub(super) fn scoped_query(
    schema: &Schema,
    query: &str,
    scope: &QueryScope,
    filters: &[TypedFilter],
) -> Result<Box<dyn Query>> {
    let request = SearchRequest {
        query: query.to_owned(),
        scope: scope.clone(),
        filters: filters.to_vec(),
        cursor: None,
        limit: 1,
    };
    validate_request(&request)?;
    build_query(schema, &request)
}

/// Runs one exact request across every supplied shard.
///
/// Callers must supply the complete, already pinned candidate-shard set. Any shard error fails the
/// whole request, and stored documents are loaded only for the final global `limit + 1` candidates.
pub fn search(shards: &[SearchShard], request: &SearchRequest) -> Result<SearchPage> {
    search_lazy(shards.len(), |index| Ok(&shards[index]), request)
}

/// Opens one immutable shard at a time and reopens only final row candidates.
pub fn search_lazy<G: AsRef<SearchShard>>(
    shard_count: usize,
    mut load: impl FnMut(usize) -> Result<G>,
    request: &SearchRequest,
) -> Result<SearchPage> {
    validate_request(request)?;
    let expected_schema = schema::build();

    let query = build_query(&expected_schema, request)?;
    if request.scope.project_ids.is_empty() || shard_count == 0 {
        return Ok(SearchPage {
            rows: Vec::new(),
            has_more: false,
        });
    }

    let candidate_limit = request
        .limit
        .checked_add(1)
        .ok_or(SearchError::InvalidRequest("limit_overflow"))?;
    let time_field = request.scope.time_field.field_name();
    let mut selected = BinaryHeap::with_capacity(candidate_limit);
    for shard_index in 0..shard_count {
        let guard = load(shard_index)?;
        let shard = guard.as_ref();
        if shard.searcher.schema() != &expected_schema {
            return Err(SearchError::SchemaMismatch {
                shard_id: shard.id.clone(),
            });
        }
        let collector = TopDocs::with_limit(candidate_limit)
            .order_by::<(Option<DateTime>, Option<i64>)>((
                (SortByStaticFastValue::for_field(time_field), Order::Desc),
                (SortByStaticFastValue::for_field("ingest_seq"), Order::Desc),
            ));
        let native_hits = shard.searcher.search(query.as_ref(), &collector)?;
        for ((time, sequence), address) in native_hits {
            let time = required_fast_value(time, shard, time_field)?;
            let sequence = required_fast_value(sequence, shard, "ingest_seq")?;
            let candidate = Candidate {
                time_us: time.into_timestamp_micros(),
                ingest_seq: sequence,
                shard_index,
                address,
            };
            keep_smallest(&mut selected, Reverse(candidate), candidate_limit);
        }
    }

    let mut selected = selected
        .into_iter()
        .map(|candidate| candidate.0)
        .collect::<Vec<_>>();
    selected.sort_unstable_by(|left, right| right.cmp(left));
    let has_more = selected.len() > request.limit;
    let mut rows = Vec::with_capacity(request.limit.min(selected.len()));
    let mut row_string_bytes = 0usize;
    for candidate in selected.into_iter().take(request.limit) {
        let guard = load(candidate.shard_index)?;
        let row = load_row(guard.as_ref(), candidate.address)?;
        row_string_bytes = checked_row_string_bytes(row_string_bytes, row.string_bytes())?;
        rows.push(row);
    }
    Ok(SearchPage { rows, has_more })
}

/// Live catch-up uses the same validated query and projection but orders the
/// globally unique ingest sequence ascending after an acknowledged scan point.
pub fn search_live(
    shards: &[SearchShard],
    request: &SearchRequest,
    after_sequence: i64,
) -> Result<SearchPage> {
    search_live_lazy(
        shards.len(),
        |index| Ok(&shards[index]),
        request,
        after_sequence,
    )
}

pub fn search_live_lazy<G: AsRef<SearchShard>>(
    shard_count: usize,
    mut load: impl FnMut(usize) -> Result<G>,
    request: &SearchRequest,
    after_sequence: i64,
) -> Result<SearchPage> {
    validate_request(request)?;
    if request.cursor.is_some()
        || request.scope.time_field != TimeField::ReceivedAt
        || after_sequence < 0
        || after_sequence > request.scope.watermark
    {
        return Err(SearchError::InvalidRequest("invalid_live_scope"));
    }
    let expected_schema = schema::build();
    if request.scope.project_ids.is_empty() || shard_count == 0 {
        return Ok(SearchPage {
            rows: Vec::new(),
            has_more: false,
        });
    }
    let sequence = expected_schema
        .get_field("ingest_seq")
        .map_err(SearchError::Native)?;
    let query = Box::new(BooleanQuery::new(vec![
        (Occur::Must, build_query(&expected_schema, request)?),
        (
            Occur::Must,
            Box::new(RangeQuery::new(
                Bound::Excluded(Term::from_field_i64(sequence, after_sequence)),
                Bound::Included(Term::from_field_i64(sequence, request.scope.watermark)),
            )),
        ),
    ]));
    let candidate_limit = request
        .limit
        .checked_add(1)
        .ok_or(SearchError::InvalidRequest("limit_overflow"))?;
    let mut selected = BinaryHeap::with_capacity(candidate_limit);
    for shard_index in 0..shard_count {
        let guard = load(shard_index)?;
        let shard = guard.as_ref();
        if shard.searcher.schema() != &expected_schema {
            return Err(SearchError::SchemaMismatch {
                shard_id: shard.id.clone(),
            });
        }
        let collector = TopDocs::with_limit(candidate_limit)
            .order_by::<Option<i64>>((SortByStaticFastValue::for_field("ingest_seq"), Order::Asc));
        for (sequence, address) in shard.searcher.search(query.as_ref(), &collector)? {
            let sequence = required_fast_value(sequence, shard, "ingest_seq")?;
            let candidate = LiveCandidate {
                ingest_seq: sequence,
                shard_index,
                address,
            };
            keep_smallest(&mut selected, candidate, candidate_limit);
        }
    }
    let mut selected = selected.into_vec();
    selected.sort_unstable();
    let has_more = selected.len() > request.limit;
    let mut rows = Vec::with_capacity(request.limit.min(selected.len()));
    let mut row_string_bytes = 0usize;
    for candidate in selected.into_iter().take(request.limit) {
        let guard = load(candidate.shard_index)?;
        let row = load_row(guard.as_ref(), candidate.address)?;
        row_string_bytes = checked_row_string_bytes(row_string_bytes, row.string_bytes())?;
        rows.push(row);
    }
    Ok(SearchPage { rows, has_more })
}

fn validate_request(request: &SearchRequest) -> Result<()> {
    if request.query.len() > MAX_QUERY_BYTES {
        return Err(SearchError::InvalidRequest("query_too_long"));
    }
    if request.limit == 0 || request.limit > MAX_SEARCH_LIMIT {
        return Err(SearchError::InvalidRequest("invalid_limit"));
    }
    if request.scope.project_ids.len() > MAX_PROJECTS
        || request
            .scope
            .project_ids
            .iter()
            .any(|project| *project <= 0)
    {
        return Err(SearchError::InvalidRequest("invalid_projects"));
    }
    if request.scope.start_us >= request.scope.end_us {
        return Err(SearchError::InvalidRequest("invalid_time_range"));
    }
    checked_datetime(request.scope.start_us)?;
    checked_datetime(request.scope.end_us)?;
    if request.scope.watermark < 0 {
        return Err(SearchError::InvalidRequest("invalid_watermark"));
    }
    if let Some(cursor) = &request.cursor {
        checked_datetime(cursor.timestamp_us)?;
        if cursor.ingest_seq <= 0 || cursor.record_id.is_empty() {
            return Err(SearchError::InvalidRequest("invalid_cursor"));
        }
        validate_string(&cursor.record_id)?;
    }
    if request.filters.len() > MAX_FILTERS {
        return Err(SearchError::InvalidRequest("too_many_filters"));
    }
    for filter in &request.filters {
        match filter {
            TypedFilter::KeywordAny { values, .. } => {
                if values.is_empty() || values.len() > MAX_FILTER_VALUES {
                    return Err(SearchError::InvalidRequest("invalid_filter_values"));
                }
                for value in values {
                    validate_string(value)?;
                }
            }
            TypedFilter::JsonEq { path, value } => {
                validate_path(path)?;
                if let JsonScalar::String(value) = value {
                    validate_string(value)?;
                }
                if matches!(value, JsonScalar::F64(value) if !value.is_finite()) {
                    return Err(SearchError::InvalidRequest("non_finite_json_number"));
                }
            }
            TypedFilter::JsonI64Range { path, lower, upper } => {
                validate_path(path)?;
                if bound_value(*lower)
                    .zip(bound_value(*upper))
                    .is_some_and(|(a, b)| a > b)
                {
                    return Err(SearchError::InvalidRequest("invalid_json_range"));
                }
            }
        }
    }

    validate_query_text(&request.query)?;
    Ok(())
}

pub fn validate_query_text(query: &str) -> Result<()> {
    if query.len() > MAX_QUERY_BYTES {
        return Err(SearchError::InvalidRequest("query_too_long"));
    }
    if query.trim().is_empty() {
        return Ok(());
    }
    let ast = tantivy::query_grammar::parse_query(query).map_err(|_| SearchError::InvalidQuery)?;
    let (clauses, depth) = ast_cost(&ast, 1);
    if clauses > MAX_QUERY_CLAUSES {
        return Err(SearchError::InvalidRequest("too_many_query_clauses"));
    }
    if depth > MAX_QUERY_DEPTH {
        return Err(SearchError::InvalidRequest("query_too_deep"));
    }
    let schema = schema::build();
    let message = schema.get_field("message").map_err(SearchError::Native)?;
    let search_text = schema
        .get_field("search_text")
        .map_err(SearchError::Native)?;
    let mut parser = QueryParser::new(
        schema,
        vec![message, search_text],
        TokenizerManager::default(),
    );
    parser.set_conjunction_by_default();
    parser
        .parse_query(query)
        .map_err(|_| SearchError::InvalidQuery)?;
    Ok(())
}

fn validate_string(value: &str) -> Result<()> {
    if value.len() > MAX_FILTER_STRING_BYTES {
        Err(SearchError::InvalidRequest("filter_string_too_long"))
    } else {
        Ok(())
    }
}

fn validate_path(path: &str) -> Result<()> {
    if path.is_empty() || path.len() > MAX_JSON_PATH_BYTES || path.ends_with('\\') {
        Err(SearchError::InvalidRequest("invalid_json_path"))
    } else {
        Ok(())
    }
}

fn bound_value(bound: I64Bound) -> Option<i64> {
    match bound {
        I64Bound::Unbounded => None,
        I64Bound::Included(value) | I64Bound::Excluded(value) => Some(value),
    }
}

fn ast_cost(ast: &UserInputAst, depth: usize) -> (usize, usize) {
    let mut clauses = 0usize;
    let mut max_depth = depth;
    let mut pending = vec![(ast, depth)];
    while let Some((node, node_depth)) = pending.pop() {
        max_depth = max_depth.max(node_depth);
        match node {
            UserInputAst::Clause(children) => {
                pending.extend(
                    children
                        .iter()
                        .map(|(_, child)| (child, node_depth.saturating_add(1))),
                );
            }
            UserInputAst::Boost(child, _) => {
                pending.push((child, node_depth.saturating_add(1)));
            }
            UserInputAst::Leaf(leaf) => {
                clauses = clauses.saturating_add(match leaf.as_ref() {
                    UserInputLeaf::Set { elements, .. } => elements.len().max(1),
                    _ => 1,
                });
            }
        }
    }
    (clauses, max_depth)
}

fn checked_datetime(micros: i64) -> Result<DateTime> {
    micros
        .checked_mul(1_000)
        .map(DateTime::from_timestamp_nanos)
        .ok_or(SearchError::InvalidRequest("time_out_of_native_range"))
}

fn build_query(schema: &Schema, request: &SearchRequest) -> Result<Box<dyn Query>> {
    let message = schema.get_field("message").map_err(SearchError::Native)?;
    let search_text = schema
        .get_field("search_text")
        .map_err(SearchError::Native)?;
    let user_query: Box<dyn Query> = if request.query.trim().is_empty() {
        Box::new(AllQuery)
    } else {
        let mut parser = QueryParser::new(
            schema.clone(),
            vec![message, search_text],
            TokenizerManager::default(),
        );
        parser.set_conjunction_by_default();
        parser
            .parse_query(&request.query)
            .map_err(|_| SearchError::InvalidQuery)?
    };

    let mut mandatory: Vec<(Occur, Box<dyn Query>)> = vec![(Occur::Must, user_query)];
    mandatory.push((
        Occur::Must,
        project_query(schema, &request.scope.project_ids)?,
    ));

    let time = schema
        .get_field(request.scope.time_field.field_name())
        .map_err(SearchError::Native)?;
    mandatory.push((
        Occur::Must,
        Box::new(RangeQuery::new(
            Bound::Included(Term::from_field_date(
                time,
                checked_datetime(request.scope.start_us)?,
            )),
            Bound::Excluded(Term::from_field_date(
                time,
                checked_datetime(request.scope.end_us)?,
            )),
        )),
    ));

    let sequence = schema
        .get_field("ingest_seq")
        .map_err(SearchError::Native)?;
    mandatory.push((
        Occur::Must,
        Box::new(RangeQuery::new(
            Bound::Unbounded,
            Bound::Included(Term::from_field_i64(sequence, request.scope.watermark)),
        )),
    ));

    if let Some(cursor) = &request.cursor {
        mandatory.push((Occur::Must, cursor_query(time, sequence, cursor)?));
    }
    for filter in &request.filters {
        mandatory.push((Occur::Must, filter_query(schema, filter)?));
    }

    Ok(Box::new(BooleanQuery::new(mandatory)))
}

fn project_query(schema: &Schema, projects: &[i64]) -> Result<Box<dyn Query>> {
    let field = schema
        .get_field("project_id")
        .map_err(SearchError::Native)?;
    let should = projects
        .iter()
        .map(|project| {
            (
                Occur::Should,
                Box::new(TermQuery::new(
                    Term::from_field_i64(field, *project),
                    IndexRecordOption::Basic,
                )) as Box<dyn Query>,
            )
        })
        .collect();
    Ok(Box::new(BooleanQuery::new(should)))
}

fn cursor_query(time: Field, sequence: Field, cursor: &RowCursor) -> Result<Box<dyn Query>> {
    let cursor_time = checked_datetime(cursor.timestamp_us)?;
    let older_time: Box<dyn Query> = Box::new(RangeQuery::new(
        Bound::Unbounded,
        Bound::Excluded(Term::from_field_date(time, cursor_time)),
    ));
    // Date inverted terms are indexed at Tantivy's fixed second precision. A native range on the
    // fast field preserves the schema's microsecond precision for cursor equality.
    let same_time: Box<dyn Query> = Box::new(RangeQuery::new(
        Bound::Included(Term::from_field_date(time, cursor_time)),
        Bound::Included(Term::from_field_date(time, cursor_time)),
    ));
    let older_sequence: Box<dyn Query> = Box::new(RangeQuery::new(
        Bound::Unbounded,
        Bound::Excluded(Term::from_field_i64(sequence, cursor.ingest_seq)),
    ));
    Ok(Box::new(BooleanQuery::new(vec![
        (Occur::Should, older_time),
        (
            Occur::Should,
            Box::new(BooleanQuery::new(vec![
                (Occur::Must, same_time),
                (Occur::Must, older_sequence),
            ])),
        ),
    ])))
}

fn filter_query(schema: &Schema, filter: &TypedFilter) -> Result<Box<dyn Query>> {
    match filter {
        TypedFilter::KeywordAny { field, values } => {
            let field = schema
                .get_field(field.field_name())
                .map_err(SearchError::Native)?;
            let should = values
                .iter()
                .map(|value| {
                    (
                        Occur::Should,
                        Box::new(TermQuery::new(
                            Term::from_field_text(field, value),
                            IndexRecordOption::Basic,
                        )) as Box<dyn Query>,
                    )
                })
                .collect();
            Ok(Box::new(BooleanQuery::new(should)))
        }
        TypedFilter::JsonEq { path, value } => {
            let attributes = schema
                .get_field("attributes")
                .map_err(SearchError::Native)?;
            let mut term = Term::from_field_json_path(attributes, path, false);
            match value {
                JsonScalar::String(value) => term.append_type_and_str(value),
                JsonScalar::I64(value) => term.append_type_and_fast_value(*value),
                JsonScalar::U64(value) => term.append_type_and_fast_value(*value),
                JsonScalar::F64(value) => term.append_type_and_fast_value(*value),
                JsonScalar::Bool(value) => term.append_type_and_fast_value(*value),
            }
            Ok(Box::new(TermQuery::new(term, IndexRecordOption::Basic)))
        }
        TypedFilter::JsonI64Range { path, lower, upper } => {
            let attributes = schema
                .get_field("attributes")
                .map_err(SearchError::Native)?;
            Ok(Box::new(RangeQuery::new(
                json_i64_bound(attributes, path, *lower),
                json_i64_bound(attributes, path, *upper),
            )))
        }
    }
}

fn json_i64_bound(field: Field, path: &str, bound: I64Bound) -> Bound<Term> {
    let term = |value| {
        let mut term = Term::from_field_json_path(field, path, false);
        term.append_type_and_fast_value(value);
        term
    };
    match bound {
        I64Bound::Unbounded => Bound::Unbounded,
        I64Bound::Included(value) => Bound::Included(term(value)),
        I64Bound::Excluded(value) => Bound::Excluded(term(value)),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Candidate {
    time_us: i64,
    ingest_seq: i64,
    shard_index: usize,
    address: DocAddress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct LiveCandidate {
    ingest_seq: i64,
    shard_index: usize,
    address: DocAddress,
}

fn required_fast_value<T>(value: Option<T>, shard: &SearchShard, field: &'static str) -> Result<T> {
    value.ok_or_else(|| SearchError::CorruptDocument {
        shard_id: shard.id.clone(),
        field,
    })
}

fn keep_smallest<T: Ord>(selected: &mut BinaryHeap<T>, candidate: T, limit: usize) {
    if selected.len() < limit {
        selected.push(candidate);
    } else if selected.peek().is_some_and(|worst| candidate < *worst) {
        selected.pop();
        selected.push(candidate);
    }
}

fn checked_row_string_bytes(total: usize, next: usize) -> Result<usize> {
    let total = total.checked_add(next).ok_or(SearchError::ResultTooLarge {
        limit_bytes: MAX_ROW_STRING_BYTES,
    })?;
    if total > MAX_ROW_STRING_BYTES {
        Err(SearchError::ResultTooLarge {
            limit_bytes: MAX_ROW_STRING_BYTES,
        })
    } else {
        Ok(total)
    }
}

impl LogRow {
    fn string_bytes(&self) -> usize {
        let required = [
            &self.shard_id,
            &self.record_id,
            &self.service,
            &self.level,
            &self.message,
        ];
        let optional = [
            &self.environment,
            &self.release,
            &self.logger,
            &self.trace_id,
            &self.span_id,
            &self.request_id,
            &self.issue_id,
            &self.fingerprint,
            &self.user_id,
            &self.user_email,
        ];
        required
            .into_iter()
            .map(|value| value.len())
            .chain(
                optional
                    .into_iter()
                    .filter_map(|value| value.as_ref().map(|value| value.len())),
            )
            .fold(0usize, usize::saturating_add)
    }
}

fn load_row(shard: &SearchShard, address: DocAddress) -> Result<LogRow> {
    let schema = shard.searcher.schema();
    let document = shard.searcher.doc::<TantivyDocument>(address)?;
    let required_str = |name: &'static str| -> Result<String> {
        let field = schema.get_field(name).map_err(SearchError::Native)?;
        document
            .get_first(field)
            .and_then(|value| value.as_str())
            .map(str::to_owned)
            .ok_or_else(|| SearchError::CorruptDocument {
                shard_id: shard.id.clone(),
                field: name,
            })
    };
    let optional_str = |name: &'static str| -> Result<Option<String>> {
        let field = schema.get_field(name).map_err(SearchError::Native)?;
        document
            .get_first(field)
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| SearchError::CorruptDocument {
                        shard_id: shard.id.clone(),
                        field: name,
                    })
            })
            .transpose()
    };
    let required_i64 = |name: &'static str| -> Result<i64> {
        let field = schema.get_field(name).map_err(SearchError::Native)?;
        document
            .get_first(field)
            .and_then(|value| value.as_i64())
            .ok_or_else(|| SearchError::CorruptDocument {
                shard_id: shard.id.clone(),
                field: name,
            })
    };
    let required_date = |name: &'static str| -> Result<i64> {
        let field = schema.get_field(name).map_err(SearchError::Native)?;
        document
            .get_first(field)
            .and_then(|value| value.as_datetime())
            .map(DateTime::into_timestamp_micros)
            .ok_or_else(|| SearchError::CorruptDocument {
                shard_id: shard.id.clone(),
                field: name,
            })
    };

    let kind = match required_str("kind")?.as_str() {
        "log" => RecordKind::Log,
        "error" => RecordKind::Error,
        _ => {
            return Err(SearchError::CorruptDocument {
                shard_id: shard.id.clone(),
                field: "kind",
            });
        }
    };
    Ok(LogRow {
        shard_id: shard.id.clone(),
        record_id: required_str("record_id")?,
        kind,
        project_id: required_i64("project_id")?,
        ingest_seq: required_i64("ingest_seq")?,
        timestamp_us: required_date("timestamp")?,
        received_at_us: required_date("received_at")?,
        service: required_str("service")?,
        level: required_str("level")?,
        message: required_str("message")?,
        environment: optional_str("environment")?,
        release: optional_str("release")?,
        logger: optional_str("logger")?,
        trace_id: optional_str("trace_id")?,
        span_id: optional_str("span_id")?,
        request_id: optional_str("request_id")?,
        issue_id: optional_str("issue_id")?,
        fingerprint: optional_str("fingerprint")?,
        user_id: optional_str("user_id")?,
        user_email: optional_str("user_email")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored_row(
        missing: Option<&str>,
        kind: &str,
    ) -> (tempfile::TempDir, SearchShard, DocAddress) {
        let directory = tempfile::tempdir().unwrap();
        let schema = schema::build();
        let index = tantivy::Index::create_in_dir(directory.path(), schema.clone()).unwrap();
        let mut document = TantivyDocument::default();
        for (name, value) in [
            ("kind", kind),
            ("record_id", "record"),
            ("service", "api"),
            ("level", "error"),
            ("message", "message"),
        ] {
            if missing != Some(name) {
                document.add_text(schema.get_field(name).unwrap(), value);
            }
        }
        for (name, value) in [("project_id", 1), ("ingest_seq", 1)] {
            if missing != Some(name) {
                document.add_i64(schema.get_field(name).unwrap(), value);
            }
        }
        for name in ["timestamp", "received_at"] {
            if missing != Some(name) {
                document.add_date(
                    schema.get_field(name).unwrap(),
                    DateTime::from_timestamp_micros(1),
                );
            }
        }
        let mut writer = index.writer_with_num_threads(1, 15_000_000).unwrap();
        writer.add_document(document).unwrap();
        writer.commit().unwrap();
        drop(writer);
        let searcher = index.reader().unwrap().searcher();
        let address = searcher
            .search(&AllQuery, &TopDocs::with_limit(1).order_by_score())
            .unwrap()[0]
            .1;
        (
            directory,
            SearchShard {
                id: "stored-row".into(),
                searcher,
            },
            address,
        )
    }

    fn request() -> SearchRequest {
        SearchRequest {
            query: String::new(),
            scope: QueryScope {
                project_ids: vec![1],
                start_us: 0,
                end_us: 1,
                watermark: 1,
                time_field: TimeField::Timestamp,
            },
            filters: vec![],
            cursor: None,
            limit: 10,
        }
    }

    #[test]
    fn field_and_bound_mappings_cover_every_supported_variant() {
        assert_eq!(TimeField::Timestamp.field_name(), "timestamp");
        assert_eq!(TimeField::ReceivedAt.field_name(), "received_at");
        for (field, name) in [
            (KeywordField::RecordId, "record_id"),
            (KeywordField::Kind, "kind"),
            (KeywordField::Service, "service"),
            (KeywordField::Level, "level"),
            (KeywordField::Environment, "environment"),
            (KeywordField::Release, "release"),
            (KeywordField::Logger, "logger"),
            (KeywordField::IssueId, "issue_id"),
            (KeywordField::Fingerprint, "fingerprint"),
            (KeywordField::TraceId, "trace_id"),
            (KeywordField::SpanId, "span_id"),
            (KeywordField::RequestId, "request_id"),
            (KeywordField::UserId, "user_id"),
            (KeywordField::UserEmail, "user_email"),
        ] {
            assert_eq!(field.field_name(), name);
        }
        assert_eq!(bound_value(I64Bound::Unbounded), None);
        assert_eq!(bound_value(I64Bound::Included(1)), Some(1));
        assert_eq!(bound_value(I64Bound::Excluded(2)), Some(2));
    }

    #[test]
    fn search_errors_are_classified_and_formatted_without_request_values() {
        let errors = [
            SearchError::ShardUnavailable,
            SearchError::InvalidRequest("invalid_limit"),
            SearchError::InvalidQuery,
            SearchError::SchemaMismatch {
                shard_id: "one".into(),
            },
            SearchError::CorruptDocument {
                shard_id: "one".into(),
                field: "kind",
            },
            SearchError::ResultTooLarge { limit_bytes: 10 },
        ];
        for error in &errors {
            assert!(!error.to_string().is_empty());
            assert!(error.source().is_none());
        }
        assert!(errors[1].is_bad_request());
        assert!(errors[2].is_bad_request());
        assert!(errors[5].is_unprocessable());
        assert!(!errors[0].is_bad_request());

        let native: SearchError = tantivy::TantivyError::InvalidArgument("test-only".into()).into();
        assert_eq!(
            native.to_string(),
            "native search failed: An invalid argument was passed: 'test-only'"
        );
        assert!(native.source().is_some());
    }

    #[test]
    fn request_validation_rejects_every_bounded_input_dimension() {
        let mut cases = Vec::new();
        let mut value = request();
        value.query = "x".repeat(MAX_QUERY_BYTES + 1);
        cases.push(value);
        let mut value = request();
        value.limit = 0;
        cases.push(value);
        let mut value = request();
        value.limit = MAX_SEARCH_LIMIT + 1;
        cases.push(value);
        let mut value = request();
        value.scope.project_ids = vec![0];
        cases.push(value);
        let mut value = request();
        value.scope.project_ids = vec![1; MAX_PROJECTS + 1];
        cases.push(value);
        let mut value = request();
        value.scope.end_us = 0;
        cases.push(value);
        let mut value = request();
        value.scope.start_us = i64::MAX;
        value.scope.end_us = i64::MAX;
        cases.push(value);
        let mut value = request();
        value.scope.start_us = i64::MIN;
        cases.push(value);
        let mut value = request();
        value.scope.watermark = -1;
        cases.push(value);
        let mut value = request();
        value.cursor = Some(RowCursor {
            timestamp_us: 0,
            ingest_seq: 0,
            record_id: String::new(),
        });
        cases.push(value);
        let mut value = request();
        value.cursor = Some(RowCursor {
            timestamp_us: 0,
            ingest_seq: 1,
            record_id: "x".repeat(MAX_FILTER_STRING_BYTES + 1),
        });
        cases.push(value);
        let mut value = request();
        value.filters = vec![TypedFilter::KeywordAny {
            field: KeywordField::Level,
            values: vec![],
        }];
        cases.push(value);
        let mut value = request();
        value.filters = vec![TypedFilter::KeywordAny {
            field: KeywordField::Level,
            values: vec!["x".into(); MAX_FILTER_VALUES + 1],
        }];
        cases.push(value);
        let mut value = request();
        value.filters = vec![TypedFilter::JsonEq {
            path: String::new(),
            value: JsonScalar::Bool(true),
        }];
        cases.push(value);
        let mut value = request();
        value.filters = vec![TypedFilter::JsonEq {
            path: "bad\\".into(),
            value: JsonScalar::Bool(true),
        }];
        cases.push(value);
        let mut value = request();
        value.filters = vec![TypedFilter::JsonEq {
            path: "x".into(),
            value: JsonScalar::F64(f64::NAN),
        }];
        cases.push(value);
        let mut value = request();
        value.filters = vec![TypedFilter::JsonI64Range {
            path: "x".into(),
            lower: I64Bound::Included(2),
            upper: I64Bound::Excluded(1),
        }];
        cases.push(value);
        let mut value = request();
        value.filters = vec![
            TypedFilter::KeywordAny {
                field: KeywordField::Level,
                values: vec!["x".into()]
            };
            MAX_FILTERS + 1
        ];
        cases.push(value);
        for value in cases {
            assert!(validate_request(&value).is_err());
        }
    }

    #[test]
    fn query_validation_and_building_cover_typed_filters_and_empty_scopes() {
        assert!(validate_query_text("").is_ok());
        assert!(validate_query_text("(").is_err());
        assert!(validate_query_text(&"x".repeat(MAX_QUERY_BYTES + 1)).is_err());
        let clauses = (0..=MAX_QUERY_CLAUSES)
            .map(|index| format!("term{index}"))
            .collect::<Vec<_>>()
            .join(" OR ");
        assert!(validate_query_text(&clauses).is_err());
        let mut nested = tantivy::query_grammar::parse_query("x").unwrap();
        for _ in 0..=MAX_QUERY_DEPTH {
            nested = UserInputAst::Boost(Box::new(nested), 1.0.into());
        }
        assert!(ast_cost(&nested, 1).1 > MAX_QUERY_DEPTH);
        let set = UserInputAst::Leaf(Box::new(UserInputLeaf::Set {
            field: None,
            elements: vec!["one".into(), "two".into()],
        }));
        assert_eq!(ast_cost(&set, 1), (2, 1));

        let schema = schema::build();
        for scalar in [
            JsonScalar::String("x".into()),
            JsonScalar::I64(-1),
            JsonScalar::U64(1),
            JsonScalar::F64(1.5),
            JsonScalar::Bool(true),
        ] {
            assert!(
                filter_query(
                    &schema,
                    &TypedFilter::JsonEq {
                        path: "duration".into(),
                        value: scalar
                    }
                )
                .is_ok()
            );
        }
        for bound in [
            I64Bound::Unbounded,
            I64Bound::Included(1),
            I64Bound::Excluded(1),
        ] {
            assert!(
                filter_query(
                    &schema,
                    &TypedFilter::JsonI64Range {
                        path: "duration".into(),
                        lower: bound,
                        upper: I64Bound::Unbounded
                    }
                )
                .is_ok()
            );
        }
        assert!(
            filter_query(
                &schema,
                &TypedFilter::KeywordAny {
                    field: KeywordField::Service,
                    values: vec!["api".into()]
                }
            )
            .is_ok()
        );
        assert!(scoped_query(&schema, "", &request().scope, &[]).is_ok());
        let mut empty = request();
        empty.scope.project_ids.clear();
        assert!(search(&[], &empty).unwrap().rows.is_empty());
        empty.scope.time_field = TimeField::ReceivedAt;
        assert!(search_live(&[], &empty, 0).unwrap().rows.is_empty());
        assert!(search_live(&[], &empty, -1).is_err());
    }

    #[test]
    fn row_and_live_search_reject_an_incompatible_shard_schema() {
        let directory = tempfile::tempdir().unwrap();
        let index =
            tantivy::Index::create_in_dir(directory.path(), Schema::builder().build()).unwrap();
        let shard = SearchShard {
            id: "wrong-schema".into(),
            searcher: index.reader().unwrap().searcher(),
        };

        assert!(matches!(
            search(std::slice::from_ref(&shard), &request()),
            Err(SearchError::SchemaMismatch { shard_id }) if shard_id == "wrong-schema"
        ));
        let mut live = request();
        live.scope.time_field = TimeField::ReceivedAt;
        assert!(matches!(
            search_live(std::slice::from_ref(&shard), &live, 0),
            Err(SearchError::SchemaMismatch { shard_id }) if shard_id == "wrong-schema"
        ));
    }

    #[test]
    fn row_loading_rejects_missing_required_fields_and_unknown_kinds() {
        for field in [
            "kind",
            "record_id",
            "project_id",
            "ingest_seq",
            "timestamp",
            "received_at",
            "service",
            "level",
            "message",
        ] {
            let (_directory, shard, address) = stored_row(Some(field), "log");
            assert!(matches!(
                load_row(&shard, address),
                Err(SearchError::CorruptDocument { field: actual, .. }) if actual == field
            ));
        }
        let (_directory, shard, address) = stored_row(None, "unexpected");
        assert!(matches!(
            load_row(&shard, address),
            Err(SearchError::CorruptDocument { field: "kind", .. })
        ));
    }

    #[test]
    fn row_loading_reports_a_missing_schema_field_as_native_failure() {
        let directory = tempfile::tempdir().unwrap();
        let mut builder = Schema::builder();
        let dummy = builder.add_text_field("dummy", tantivy::schema::STORED);
        let index = tantivy::Index::create_in_dir(directory.path(), builder.build()).unwrap();
        let mut writer = index.writer_with_num_threads(1, 15_000_000).unwrap();
        let mut document = TantivyDocument::default();
        document.add_text(dummy, "stored");
        writer.add_document(document).unwrap();
        writer.commit().unwrap();
        drop(writer);
        let searcher = index.reader().unwrap().searcher();
        let address = searcher
            .search(&AllQuery, &TopDocs::with_limit(1).order_by_score())
            .unwrap()[0]
            .1;
        let shard = SearchShard {
            id: "missing-schema".into(),
            searcher,
        };
        assert!(matches!(
            load_row(&shard, address),
            Err(SearchError::Native(_))
        ));
    }

    #[test]
    fn bounded_selection_and_row_byte_accounting_cover_every_outcome() {
        let mut values = BinaryHeap::new();
        keep_smallest(&mut values, 2, 2);
        keep_smallest(&mut values, 3, 2);
        keep_smallest(&mut values, 4, 2);
        keep_smallest(&mut values, 1, 2);
        assert_eq!(values.into_sorted_vec(), vec![1, 2]);

        assert_eq!(checked_row_string_bytes(1, 2).unwrap(), 3);
        assert!(checked_row_string_bytes(MAX_ROW_STRING_BYTES, 1).is_err());
        assert!(checked_row_string_bytes(usize::MAX, 1).is_err());

        let directory = tempfile::tempdir().unwrap();
        let index = tantivy::Index::create_in_dir(directory.path(), schema::build()).unwrap();
        let shard = SearchShard {
            id: "missing-fast-value".into(),
            searcher: index.reader().unwrap().searcher(),
        };
        assert!(matches!(
            required_fast_value::<i64>(None, &shard, "ingest_seq"),
            Err(SearchError::CorruptDocument {
                field: "ingest_seq",
                ..
            })
        ));
        assert_eq!(
            required_fast_value(Some(7), &shard, "ingest_seq").unwrap(),
            7
        );
    }
}
