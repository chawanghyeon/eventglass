use anyhow::Result;
use serde_json::json;
use tantivy::collector::Count;
use tantivy::query::QueryParser;
use tantivy::schema::{
    DateOptions, DateTimePrecision, FAST, IndexRecordOption, JsonObjectOptions, STORED, STRING,
    Schema, TEXT, TextFieldIndexing, TextOptions,
};
use tantivy::{DateTime, Index, doc};

fn count(parser: &QueryParser, index: &Index, input: &str) -> Result<usize> {
    let query = parser.parse_query(input)?;
    let reader = index.reader()?;
    Ok(reader.searcher().search(&query, &Count)?)
}

#[test]
fn g01_native_parser_json_paths_types_unicode_and_microseconds() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut schema = Schema::builder();
    let message = schema.add_text_field("message", TEXT | STORED);
    let search_text = schema.add_text_field("search_text", TEXT);
    let service = schema.add_text_field("service", STRING | FAST | STORED);
    let json_text = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer("raw")
            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
    );
    let attributes = schema.add_json_field(
        "attributes",
        JsonObjectOptions::from(json_text).set_fast(Some("raw")),
    );
    let timestamp = schema.add_date_field(
        "timestamp",
        DateOptions::default()
            .set_indexed()
            .set_fast()
            .set_stored()
            .set_precision(DateTimePrecision::Microseconds),
    );
    let schema = schema.build();
    assert!(!schema.get_field_entry(attributes).is_expand_dots_enabled());

    let index = Index::create_in_dir(dir.path(), schema)?;
    let mut writer = index.writer_with_num_threads(1, 15_000_000)?;
    writer.add_document(doc!(
        message => "database connection timeout 배포 실패",
        search_text => "worker crashed after deploy",
        service => "api",
        attributes => json!({
            "http": {"status_code": 500},
            "http.status_code": 501,
            "code": 500,
            "tenant": "한글-고객"
        }),
        timestamp => DateTime::from_timestamp_micros(1_788_765_432_123_456),
    ))?;
    writer.add_document(doc!(
        message => "database connection healthy",
        search_text => "normal operation",
        service => "worker",
        attributes => json!({"code": "500", "tenant": "다른-고객"}),
        timestamp => DateTime::from_timestamp_micros(1_788_765_432_123_457),
    ))?;
    writer.commit()?;
    drop(writer);

    // Reopen the actual directory so this test also covers persisted schema/query behavior.
    let index = Index::open_in_dir(dir.path())?;
    let mut parser = QueryParser::for_index(&index, vec![message, search_text]);
    parser.set_conjunction_by_default();

    assert_eq!(count(&parser, &index, "database timeout")?, 1);
    assert_eq!(count(&parser, &index, "message:\"connection timeout\"")?, 1);
    // 0.26.1 does not treat an unquoted single term suffix as a prefix query.
    assert_eq!(count(&parser, &index, "message:conn*")?, 0);
    assert_eq!(count(&parser, &index, "message:\"database conn\"*")?, 2);
    assert_eq!(
        count(
            &parser,
            &index,
            "(service:api OR service:worker) -message:healthy"
        )?,
        1
    );
    assert_eq!(count(&parser, &index, "message:\"배포 실패\"")?, 1);

    assert_eq!(
        count(&parser, &index, "attributes.http.status_code:500")?,
        1
    );
    assert_eq!(
        count(&parser, &index, r"attributes.http\.status_code:501")?,
        1
    );
    assert_eq!(
        count(&parser, &index, "attributes.http.status_code:501")?,
        0
    );
    assert_eq!(count(&parser, &index, "attributes.tenant:한글-고객")?, 1);

    // A JSON literal is expanded into both its inferred numeric term and its raw text term.
    // A numeric range is the native way to select only the numeric type.
    assert_eq!(count(&parser, &index, "attributes.code:500")?, 2);
    assert_eq!(count(&parser, &index, "attributes.code:[500 TO 500]")?, 1);

    assert_eq!(
        count(
            &parser,
            &index,
            "timestamp:[2026-09-07T07:17:12.123456Z TO 2026-09-07T07:17:12.123457Z}"
        )?,
        1
    );
    assert!(parser.parse_query("message:\"unterminated").is_err());
    Ok(())
}
