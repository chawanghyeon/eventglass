//! Exact native stored-payload lookup for a signed record identity.

use std::ops::Bound;

use anyhow::{Result, ensure};
use serde::Serialize;
use serde_json::Value;
use tantivy::{
    Order, TantivyDocument, Term,
    collector::TopDocs,
    query::{BooleanQuery, Occur, RangeQuery, TermQuery},
    schema::{IndexRecordOption, Value as _},
};

use super::{active::Published, schema};

/// The only stored payload exposed by the detail endpoint.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RecordDetail {
    pub record_id: String,
    pub raw: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail_token: Option<String>,
    #[serde(skip_serializing)]
    pub correlation: CorrelationSeed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorrelationSeed {
    pub project_id: i64,
    pub ingest_seq: i64,
    pub timestamp_us: i64,
    pub service: String,
    pub trace_id: Option<String>,
    pub request_id: Option<String>,
    pub user_id: Option<String>,
}

/// Loads one record from the already selected active-shard snapshot.
///
/// The signed project, record and watermark all remain mandatory native clauses. A duplicate exact
/// identity is corruption rather than an arbitrary winner. `None` is a proven no-match in this
/// snapshot; all schema, stored-field and JSON failures are errors.
pub fn load(
    published: &Published,
    project_id: i64,
    record_id: &str,
    watermark: i64,
) -> Result<Option<RecordDetail>> {
    load_bounded(
        published,
        project_id,
        record_id,
        SequenceConstraint::AtMost(watermark),
    )
}

/// Loads the one native document identified by a durable Issue occurrence.
pub fn load_occurrence(
    published: &Published,
    project_id: i64,
    record_id: &str,
    ingest_seq: i64,
) -> Result<Option<RecordDetail>> {
    load_bounded(
        published,
        project_id,
        record_id,
        SequenceConstraint::Exact(ingest_seq),
    )
}

#[derive(Clone, Copy)]
enum SequenceConstraint {
    AtMost(i64),
    Exact(i64),
}

fn load_bounded(
    published: &Published,
    project_id: i64,
    record_id: &str,
    sequence: SequenceConstraint,
) -> Result<Option<RecordDetail>> {
    let watermark = match sequence {
        SequenceConstraint::AtMost(watermark) => watermark,
        SequenceConstraint::Exact(ingest_seq) => ingest_seq,
    };
    ensure!(project_id > 0, "invalid detail project");
    ensure!(watermark >= 0, "invalid detail watermark");
    ensure!(
        watermark <= published.boundary.ingest_seq,
        "detail watermark is not published"
    );
    ensure!(
        record_id.len() == 64
            && record_id
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "invalid detail record identity"
    );

    let expected_schema = schema::build();
    ensure!(
        published.searcher.schema() == &expected_schema,
        "detail shard schema mismatch"
    );
    let record_field = expected_schema.get_field("record_id")?;
    let project_field = expected_schema.get_field("project_id")?;
    let sequence_field = expected_schema.get_field("ingest_seq")?;
    let sequence_query: Box<dyn tantivy::query::Query> = match sequence {
        SequenceConstraint::AtMost(_) => Box::new(RangeQuery::new(
            Bound::Unbounded,
            Bound::Included(Term::from_field_i64(sequence_field, watermark)),
        )),
        SequenceConstraint::Exact(ingest_seq) => Box::new(TermQuery::new(
            Term::from_field_i64(sequence_field, ingest_seq),
            IndexRecordOption::Basic,
        )),
    };
    let query = BooleanQuery::new(vec![
        (
            Occur::Must,
            Box::new(TermQuery::new(
                Term::from_field_text(record_field, record_id),
                IndexRecordOption::Basic,
            )),
        ),
        (
            Occur::Must,
            Box::new(TermQuery::new(
                Term::from_field_i64(project_field, project_id),
                IndexRecordOption::Basic,
            )),
        ),
        (Occur::Must, sequence_query),
    ]);
    let collector = TopDocs::with_limit(2).order_by_fast_field::<i64>("ingest_seq", Order::Asc);
    let hits = published.searcher.search(&query, &collector)?;
    ensure!(hits.len() <= 1, "duplicate native record identity");
    let Some((_, address)) = hits.into_iter().next() else {
        return Ok(None);
    };

    let document = published.searcher.doc::<TantivyDocument>(address)?;
    let stored_record = exactly_one_str(&document, record_field, "record_id")?;
    let stored_project = exactly_one_i64(&document, project_field, "project_id")?;
    let stored_sequence = exactly_one_i64(&document, sequence_field, "ingest_seq")?;
    ensure!(
        stored_record == record_id
            && stored_project == project_id
            && stored_sequence > 0
            && match sequence {
                SequenceConstraint::AtMost(_) => stored_sequence <= watermark,
                SequenceConstraint::Exact(ingest_seq) => stored_sequence == ingest_seq,
            },
        "native detail identity mismatch"
    );
    let raw_field = expected_schema.get_field("raw_json")?;
    let raw = serde_json::from_str(exactly_one_str(&document, raw_field, "raw_json")?)
        .map_err(|error| anyhow::anyhow!("invalid native raw_json: {error}"))?;
    let timestamp_field = expected_schema.get_field("timestamp")?;
    let service_field = expected_schema.get_field("service")?;
    Ok(Some(RecordDetail {
        record_id: stored_record.to_owned(),
        raw,
        detail_token: None,
        correlation: CorrelationSeed {
            project_id: stored_project,
            ingest_seq: stored_sequence,
            timestamp_us: exactly_one_date(&document, timestamp_field, "timestamp")?,
            service: exactly_one_str(&document, service_field, "service")?.to_owned(),
            trace_id: optional_str(
                &document,
                expected_schema.get_field("trace_id")?,
                "trace_id",
            )?,
            request_id: optional_str(
                &document,
                expected_schema.get_field("request_id")?,
                "request_id",
            )?,
            user_id: optional_str(&document, expected_schema.get_field("user_id")?, "user_id")?,
        },
    }))
}

fn optional_str(
    document: &TantivyDocument,
    field: tantivy::schema::Field,
    name: &'static str,
) -> Result<Option<String>> {
    let mut values = document.get_all(field);
    let value = values.next().map(|value| {
        value
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| anyhow::anyhow!("invalid native {name}"))
    });
    ensure!(values.next().is_none(), "duplicate native {name}");
    value.transpose()
}

fn exactly_one_date(
    document: &TantivyDocument,
    field: tantivy::schema::Field,
    name: &'static str,
) -> Result<i64> {
    let mut values = document.get_all(field);
    let value = values
        .next()
        .and_then(|value| value.as_datetime())
        .ok_or_else(|| anyhow::anyhow!("missing or invalid native {name}"))?;
    ensure!(values.next().is_none(), "duplicate native {name}");
    Ok(value.into_timestamp_micros())
}

fn exactly_one_str<'a>(
    document: &'a TantivyDocument,
    field: tantivy::schema::Field,
    name: &'static str,
) -> Result<&'a str> {
    let mut values = document.get_all(field);
    let value = values
        .next()
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing or invalid native {name}"))?;
    ensure!(values.next().is_none(), "duplicate native {name}");
    Ok(value)
}

fn exactly_one_i64(
    document: &TantivyDocument,
    field: tantivy::schema::Field,
    name: &'static str,
) -> Result<i64> {
    let mut values = document.get_all(field);
    let value = values
        .next()
        .and_then(|value| value.as_i64())
        .ok_or_else(|| anyhow::anyhow!("missing or invalid native {name}"))?;
    ensure!(values.next().is_none(), "duplicate native {name}");
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::DateTime;

    #[test]
    fn stored_field_readers_reject_missing_wrong_and_duplicate_values() -> Result<()> {
        let schema = schema::build();
        let text = schema.get_field("service")?;
        let integer = schema.get_field("project_id")?;
        let date = schema.get_field("timestamp")?;
        let mut document = TantivyDocument::default();

        assert!(exactly_one_str(&document, text, "service").is_err());
        assert!(exactly_one_i64(&document, integer, "project_id").is_err());
        assert!(exactly_one_date(&document, date, "timestamp").is_err());
        assert_eq!(optional_str(&document, text, "service")?, None);

        document.add_i64(text, 1);
        document.add_text(integer, "wrong");
        document.add_text(date, "wrong");
        assert!(exactly_one_str(&document, text, "service").is_err());
        assert!(optional_str(&document, text, "service").is_err());
        assert!(exactly_one_i64(&document, integer, "project_id").is_err());
        assert!(exactly_one_date(&document, date, "timestamp").is_err());

        let mut document = TantivyDocument::default();
        document.add_text(text, "one");
        document.add_text(text, "two");
        document.add_i64(integer, 1);
        document.add_i64(integer, 2);
        document.add_date(date, DateTime::from_timestamp_micros(1));
        document.add_date(date, DateTime::from_timestamp_micros(2));
        assert!(optional_str(&document, text, "service").is_err());
        assert!(exactly_one_str(&document, text, "service").is_err());
        assert!(exactly_one_i64(&document, integer, "project_id").is_err());
        assert!(exactly_one_date(&document, date, "timestamp").is_err());
        assert!(document.get_first(text).unwrap().as_str().is_some());
        Ok(())
    }
}
