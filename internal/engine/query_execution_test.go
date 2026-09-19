//go:build duckdb_use_static_lib

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
)

func TestExecuteQueryRowsAndReducerKeepGlobalOrder(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	firstInput := filepath.Join(root, "first.parquet")
	secondInput := filepath.Join(root, "second.parquet")
	writeQueryParquet(t, ctx, firstInput, `SELECT * FROM (VALUES
		(100::BIGINT,1::INTEGER,'`+strings.Repeat("c", 64)+`'),
		(100::BIGINT,1::INTEGER,'`+strings.Repeat("a", 64)+`')) t(event_time_us,event_time_ns_remainder,record_id)`)
	writeQueryParquet(t, ctx, secondInput, `SELECT * FROM (VALUES
		(100::BIGINT,1::INTEGER,'`+strings.Repeat("b", 64)+`'),
		(99::BIGINT,999::INTEGER,'`+strings.Repeat("f", 64)+`')) t(event_time_us,event_time_ns_remainder,record_id)`)
	operation := engine.QueryOperation{
		Version: engine.QueryExecutionProtocolVersion, Kind: "rows", MaxRows: 5,
		Result:    engine.QueryResultPlan{Kind: "rows", Limit: 4, Sort: "event_desc"},
		ScanSQL:   `SELECT * FROM input_rows ORDER BY event_time_us DESC,event_time_ns_remainder DESC,record_id DESC LIMIT 5`,
		ReduceSQL: `SELECT * FROM input_rows ORDER BY event_time_us DESC,event_time_ns_remainder DESC,record_id DESC LIMIT 5`,
		EmptySQL:  `SELECT CAST(0 AS BIGINT) event_time_us,CAST(0 AS INTEGER) event_time_ns_remainder,CAST('' AS VARCHAR) record_id WHERE false`,
	}
	partials := make([]string, 2)
	for index, input := range []string{firstInput, secondInput} {
		partials[index] = filepath.Join(root, fmt.Sprintf("partial-%d.parquet", index))
		summary, err := engine.ExecuteQuery(ctx, engine.QueryRequest{
			Version: engine.QueryExecutionProtocolVersion, QueryID: "query", Task: model.QueryTaskKey{Stage: model.QueryTaskScan},
			Operation: operation, InputPaths: []string{input}, OutputPath: partials[index], SpillDirectory: filepath.Join(root, fmt.Sprintf("spill-%d", index)),
		})
		if err != nil || summary.Rows != 2 {
			t.Fatalf("scan %d summary=%#v err=%v", index, summary, err)
		}
	}
	output := filepath.Join(root, "root.parquet")
	summary, err := engine.ExecuteQuery(ctx, engine.QueryRequest{
		Version: engine.QueryExecutionProtocolVersion, QueryID: "query", Task: model.QueryTaskKey{Stage: model.QueryTaskReduce, Level: 1},
		Operation: operation, InputPaths: partials, OutputPath: output, SpillDirectory: filepath.Join(root, "spill-root"),
	})
	if err != nil || summary.Rows != 4 {
		t.Fatalf("reduce summary=%#v err=%v", summary, err)
	}
	db, err := engine.Open(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT record_id FROM read_parquet(?) ORDER BY event_time_us DESC,event_time_ns_remainder DESC,record_id DESC`, output)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		got = append(got, id[:1])
	}
	if strings.Join(got, "") != "cbaf" {
		t.Fatalf("order=%v", got)
	}
}

func TestExecuteQueryNativeLimbsSurviveLocalOverflowCancellation(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	inputs := []string{filepath.Join(root, "positive.parquet"), filepath.Join(root, "negative.parquet")}
	maximum := strings.Repeat("9", 38)
	writeQueryParquet(t, ctx, inputs[0], `SELECT * FROM (VALUES (CAST('`+maximum+`' AS DECIMAL(38,0))),(CAST('1' AS DECIMAL(38,0)))) t(value)`)
	writeQueryParquet(t, ctx, inputs[1], `SELECT CAST('-1' AS DECIMAL(38,0)) AS value`)
	limbs := make([]string, query.IntegerLimbCount)
	for index := range limbs {
		expression, err := query.IntegerLimbSQL("value", index)
		if err != nil {
			t.Fatal(err)
		}
		limbs[index] = fmt.Sprintf("SUM(%s) AS l%d", expression, index)
	}
	operation := engine.QueryOperation{
		Version: engine.QueryExecutionProtocolVersion, Kind: "aggregate", MaxRows: 1,
		Result: engine.QueryResultPlan{Kind: "aggregate", Top: 1, OrderMetric: "count", OrderDirection: "desc",
			Metrics: []engine.QueryResultMetric{{Name: "value", Op: "sum", FieldType: "integer"}}},
		ScanSQL:   `SELECT ` + strings.Join(limbs, ",") + `,count(*)::BIGINT AS valid_count FROM input_rows`,
		ReduceSQL: `SELECT sum(l0) l0,sum(l1) l1,sum(l2) l2,sum(l3) l3,sum(l4) l4,sum(valid_count)::BIGINT valid_count FROM input_rows`,
		EmptySQL:  `SELECT 0::DECIMAL(38,0) l0,0::DECIMAL(38,0) l1,0::DECIMAL(38,0) l2,0::DECIMAL(38,0) l3,0::DECIMAL(38,0) l4,0::BIGINT valid_count`,
	}
	partials := make([]string, 2)
	for index, input := range inputs {
		partials[index] = filepath.Join(root, fmt.Sprintf("limb-partial-%d.parquet", index))
		_, err := engine.ExecuteQuery(ctx, engine.QueryRequest{
			Version: 1, QueryID: "limbs", Task: model.QueryTaskKey{Stage: model.QueryTaskScan}, Operation: operation,
			InputPaths: []string{input}, OutputPath: partials[index], SpillDirectory: filepath.Join(root, fmt.Sprintf("limb-spill-%d", index)),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	output := filepath.Join(root, "limb-root.parquet")
	_, err := engine.ExecuteQuery(ctx, engine.QueryRequest{
		Version: 1, QueryID: "limbs", Task: model.QueryTaskKey{Stage: model.QueryTaskReduce, Level: 1}, Operation: operation,
		InputPaths: partials, OutputPath: output, SpillDirectory: filepath.Join(root, "limb-spill-root"),
	})
	if err != nil {
		t.Fatal(err)
	}
	db, err := engine.Open(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var state query.IntegerState
	if err := db.QueryRowContext(ctx, `SELECT l0::VARCHAR,l1::VARCHAR,l2::VARCHAR,l3::VARCHAR,l4::VARCHAR,valid_count FROM read_parquet(?)`, output).Scan(
		&state.Limbs[0], &state.Limbs[1], &state.Limbs[2], &state.Limbs[3], &state.Limbs[4], &state.Count); err != nil {
		t.Fatal(err)
	}
	value, err := query.FinalizeIntegerSum(state)
	if err != nil || value == nil || *value != maximum {
		t.Fatalf("value=%v err=%v limbs=%v", value, err, state.Limbs)
	}
}

func TestExecuteQueryRejectsTwentyThousandAndOneGroups(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	acceptedInput := filepath.Join(root, "groups-accepted.parquet")
	writeQueryParquet(t, ctx, acceptedInput, `SELECT range::BIGINT AS group_id FROM range(20000)`)
	operation := engine.QueryOperation{Version: 1, Kind: "aggregate", MaxRows: 20000,
		Result: engine.QueryResultPlan{Kind: "aggregate", Top: 100, OrderMetric: "count", OrderDirection: "desc",
			Metrics: []engine.QueryResultMetric{{Name: "events", Op: "count"}}},
		ScanSQL: `SELECT group_id,count(*) count FROM input_rows GROUP BY group_id`, ReduceSQL: `SELECT * FROM input_rows`,
		EmptySQL: `SELECT 0::BIGINT group_id,0::BIGINT count WHERE false`}
	summary, err := engine.ExecuteQuery(ctx, engine.QueryRequest{
		Version: 1, QueryID: "groups-accepted", Task: model.QueryTaskKey{Stage: model.QueryTaskScan}, InputPaths: []string{acceptedInput},
		OutputPath: filepath.Join(root, "groups-accepted-output.parquet"), SpillDirectory: filepath.Join(root, "groups-accepted-spill"), Operation: operation,
	})
	if err != nil || summary.Rows != 20000 {
		t.Fatalf("20,000 groups summary=%#v err=%v", summary, err)
	}
	input := filepath.Join(root, "groups.parquet")
	writeQueryParquet(t, ctx, input, `SELECT range::BIGINT AS group_id FROM range(20001)`)
	_, err = engine.ExecuteQuery(ctx, engine.QueryRequest{
		Version: 1, QueryID: "groups", Task: model.QueryTaskKey{Stage: model.QueryTaskScan}, InputPaths: []string{input},
		OutputPath: filepath.Join(root, "groups-output.parquet"), SpillDirectory: filepath.Join(root, "groups-spill"), Operation: operation,
	})
	if !errors.Is(err, engine.ErrQueryExecutionLimit) {
		t.Fatalf("group cap=%v", err)
	}
}

func TestExecuteQueryGeneratedAggregateUsesWeightedIntegerAverage(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	inputs := []string{filepath.Join(root, "aggregate-a.parquet"), filepath.Join(root, "aggregate-b.parquet")}
	row := func(service string, severity int) string {
		return fmt.Sprintf("(1::BIGINT,2::BIGINT,'log',1::BIGINT,1::BIGINT,0::INTEGER,0::BIGINT,'%s',%d::SMALLINT,[]::STRUCT(namespace VARCHAR,path VARCHAR,value_type VARCHAR,string_value VARCHAR,integer_value DECIMAL(38,0),double_value DOUBLE,boolean_value BOOLEAN,json_value VARCHAR,unit VARCHAR)[])", service, severity)
	}
	writeQueryParquet(t, ctx, inputs[0], `SELECT * FROM (VALUES `+row("A", 100)+`,`+row("B", 11)+`) t(tenant_id,project_id,kind,event_time_us,received_time_us,lane_id,batch_seq,service,severity_number,attrs)`)
	secondRows := []string{row("B", 11)}
	for range 9 {
		secondRows = append(secondRows, row("A", 0))
	}
	writeQueryParquet(t, ctx, inputs[1], `SELECT * FROM (VALUES `+strings.Join(secondRows, ",")+`) t(tenant_id,project_id,kind,event_time_us,received_time_us,lane_id,batch_seq,service,severity_number,attrs)`)

	filter := &query.Node{Op: "constant", Constant: true}
	canonical, err := query.CanonicalFilter(filter)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := query.BuildPlan(model.DatasetSpec{
		TenantID: 1, ProjectIDs: []int64{2}, Kinds: []model.Kind{model.KindLog}, TimeBasis: model.QueryTimeEvent,
		StartUS: 0, EndUS: 100, Filter: canonical,
	}, model.SnapshotScope{}, filter)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := query.BuildAggregateOperation(query.AggregateOperationSpec{
		Plan: plan, GroupBy: []query.GroupDimension{{Op: "field", Name: "service"}},
		Metrics: []query.AggregateMetric{{Name: "severity_avg", Op: "avg", Field: &query.NumericField{Op: "field", Name: "severity_number", Type: query.IntegerType}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	partials := make([]string, len(inputs))
	for index, input := range inputs {
		partials[index] = filepath.Join(root, fmt.Sprintf("aggregate-partial-%d.parquet", index))
		if _, err := engine.ExecuteQuery(ctx, engine.QueryRequest{
			Version: 1, QueryID: "aggregate", Task: model.QueryTaskKey{Stage: model.QueryTaskScan}, Operation: operation,
			InputPaths: []string{input}, OutputPath: partials[index], SpillDirectory: filepath.Join(root, fmt.Sprintf("aggregate-spill-%d", index)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	output := filepath.Join(root, "aggregate-root.parquet")
	if _, err := engine.ExecuteQuery(ctx, engine.QueryRequest{
		Version: 1, QueryID: "aggregate", Task: model.QueryTaskKey{Stage: model.QueryTaskReduce, Level: 1}, Operation: operation,
		InputPaths: partials, OutputPath: output, SpillDirectory: filepath.Join(root, "aggregate-spill-root"),
	}); err != nil {
		t.Fatal(err)
	}
	db, err := engine.Open(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT g0_string,m0_l0::VARCHAR,m0_l1::VARCHAR,m0_l2::VARCHAR,m0_l3::VARCHAR,m0_l4::VARCHAR,m0_valid,m0_excluded FROM read_parquet(?)`, output)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var groups []query.AggregateRankedGroup
	for rows.Next() {
		var key string
		state := &query.IntegerState{}
		if err := rows.Scan(&key, &state.Limbs[0], &state.Limbs[1], &state.Limbs[2], &state.Limbs[3], &state.Limbs[4], &state.Count, &state.Excluded); err != nil {
			t.Fatal(err)
		}
		groups = append(groups, query.AggregateRankedGroup{Key: []byte(key), IntegerState: state})
	}
	index := 0
	selected, err := query.SelectAggregateTopK(func() (*query.AggregateRankedGroup, error) {
		if index == len(groups) {
			return nil, nil
		}
		group := groups[index]
		index++
		return &group, nil
	}, 1, query.AggregateRankOrder{Kind: query.RankByIntegerAvg, Descending: true})
	if err != nil || len(selected) != 1 || string(selected[0].Key) != "B" {
		t.Fatalf("weighted average winner=%#v err=%v", selected, err)
	}
}

func TestExecuteQueryGeneratedEmptyHistogramUsesNegativeEpochFloor(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	filter := &query.Node{Op: "constant", Constant: true}
	canonical, err := query.CanonicalFilter(filter)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := query.BuildPlan(model.DatasetSpec{
		TenantID: 1, ProjectIDs: []int64{2}, Kinds: []model.Kind{model.KindLog}, TimeBasis: model.QueryTimeEvent,
		StartUS: -1, EndUS: 1_000_001, Filter: canonical,
	}, model.SnapshotScope{}, filter)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := query.BuildAggregateOperation(query.AggregateOperationSpec{
		Plan: plan, Metrics: []query.AggregateMetric{{Name: "events", Op: "count"}},
		Histogram: &query.AggregateHistogram{IntervalUS: 1_000_000, EmptyBuckets: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(root, "empty-histogram.parquet")
	summary, err := engine.ExecuteQuery(ctx, engine.QueryRequest{
		Version: 1, QueryID: "empty-histogram", Task: model.QueryTaskKey{Stage: model.QueryTaskReduce, Level: 1}, Operation: operation,
		OutputPath: output, SpillDirectory: filepath.Join(root, "empty-histogram-spill"),
	})
	if err != nil || summary.Rows != 3 {
		t.Fatalf("summary=%#v err=%v", summary, err)
	}
	db, err := engine.Open(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT bucket_start_us,m0_valid FROM read_parquet(?) ORDER BY bucket_start_us`, output)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	want := []int64{-1_000_000, 0, 1_000_000}
	index := 0
	for rows.Next() {
		var bucket, count int64
		if err := rows.Scan(&bucket, &count); err != nil {
			t.Fatal(err)
		}
		if index >= len(want) || bucket != want[index] || count != 0 {
			t.Fatalf("row %d bucket=%d count=%d", index, bucket, count)
		}
		index++
	}
	if index != len(want) {
		t.Fatalf("buckets=%d want=%d", index, len(want))
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(root, "partial-histogram-input.parquet")
	writeQueryParquet(t, ctx, input, `SELECT 1::BIGINT tenant_id,2::BIGINT project_id,'log'::VARCHAR kind,
		0::BIGINT event_time_us,0::BIGINT received_time_us,0::INTEGER lane_id,0::BIGINT batch_seq`)
	partialOutput := filepath.Join(root, "partial-histogram.parquet")
	summary, err = engine.ExecuteQuery(ctx, engine.QueryRequest{
		Version: 1, QueryID: "partial-histogram", Task: model.QueryTaskKey{Stage: model.QueryTaskScan}, Operation: operation,
		InputPaths: []string{input}, OutputPath: partialOutput, SpillDirectory: filepath.Join(root, "partial-histogram-spill"),
	})
	if err != nil || summary.Rows != 3 {
		t.Fatalf("partial summary=%#v err=%v", summary, err)
	}
	var total, zeroBuckets int64
	if err := db.QueryRowContext(ctx, `SELECT sum(m0_valid),count(*) FILTER (WHERE m0_valid=0) FROM read_parquet(?)`, partialOutput).Scan(&total, &zeroBuckets); err != nil {
		t.Fatal(err)
	}
	if total != 1 || zeroBuckets != 2 {
		t.Fatalf("partial histogram total=%d zero_buckets=%d", total, zeroBuckets)
	}
}

func TestExportQueryResultPreservesExactDecimalAndEmptyRows(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	maximum := strings.Repeat("9", 38)
	input := filepath.Join(root, "export-input.parquet")
	writeQueryParquet(t, ctx, input, `SELECT CAST('`+maximum+`' AS DECIMAL(38,0)) AS exact_value`)
	output := filepath.Join(root, "export.jsonl")
	summary, err := engine.ExportQueryResult(ctx, engine.QueryExportRequest{Version: 1, InputPath: input, OutputPath: output})
	if err != nil || summary.Rows != 1 {
		t.Fatalf("summary=%#v err=%v", summary, err)
	}
	data, err := os.ReadFile(output)
	if err != nil || !strings.Contains(string(data), maximum) {
		t.Fatalf("export=%q err=%v", data, err)
	}
	emptyInput := filepath.Join(root, "export-empty.parquet")
	writeQueryParquet(t, ctx, emptyInput, `SELECT 1::BIGINT value WHERE false`)
	emptyOutput := filepath.Join(root, "export-empty.jsonl")
	empty, err := engine.ExportQueryResult(ctx, engine.QueryExportRequest{Version: 1, InputPath: emptyInput, OutputPath: emptyOutput})
	if err != nil || empty.Rows != 0 || empty.Evidence.Bytes != 0 {
		t.Fatalf("empty=%#v err=%v", empty, err)
	}
}

func TestExecuteQueryDetailJoinsOnlyPairedAuthorizedPayload(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	recordID := strings.Repeat("a", 64)
	otherID := strings.Repeat("b", 64)
	analytics := filepath.Join(root, "analytics.parquet")
	payload := filepath.Join(root, "payload.parquet")
	writeQueryParquet(t, ctx, analytics, `SELECT * FROM (VALUES
		(1::BIGINT,2::BIGINT,'`+recordID+`','log',10::BIGINT,11::BIGINT,0::INTEGER,1::BIGINT,0::INTEGER,NULL::VARCHAR,NULL::INTEGER),
		(1::BIGINT,3::BIGINT,'`+otherID+`','log',10::BIGINT,11::BIGINT,0::INTEGER,1::BIGINT,1::INTEGER,NULL::VARCHAR,NULL::INTEGER))
		t(tenant_id,project_id,record_id,kind,event_time_us,received_time_us,lane_id,batch_seq,record_ordinal,issue_id,grouping_version)`)
	writeQueryParquet(t, ctx, payload, `SELECT * FROM (VALUES
		('`+recordID+`','{"message":"kept"}','{"name":"sdk"}','[]','{"record_id":"`+recordID+`","tenant_id":1,"project_id":2,"event_time_us":10,"arrival_time_us":11}'),
		('`+otherID+`','{"message":"forbidden"}',NULL,'[]','{"record_id":"`+otherID+`"}'))
		t(record_id,raw_json,envelope_sdk_json,normalization_warnings_json,canonical_metadata_json)`)
	filter := &query.Node{Op: "constant", Constant: true}
	canonical, err := query.CanonicalFilter(filter)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := query.BuildPlan(model.DatasetSpec{
		TenantID: 1, ProjectIDs: []int64{2}, Kinds: []model.Kind{model.KindLog}, TimeBasis: model.QueryTimeEvent,
		StartUS: 0, EndUS: 100, Filter: canonical,
	}, model.SnapshotScope{LaneCuts: [model.LaneCount]int64{1}}, filter)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := query.BuildDetailOperation(plan, recordID)
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(root, "detail.parquet")
	summary, err := engine.ExecuteQuery(ctx, engine.QueryRequest{
		Version: 1, QueryID: "detail", Task: model.QueryTaskKey{Stage: model.QueryTaskScan}, Operation: operation,
		InputPaths: []string{analytics}, PayloadPaths: []string{payload}, OutputPath: output, SpillDirectory: filepath.Join(root, "detail-spill"),
	})
	if err != nil || summary.Rows != 1 {
		t.Fatalf("summary=%#v err=%v", summary, err)
	}
	db, err := engine.Open(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var gotID, raw string
	if err := db.QueryRowContext(ctx, `SELECT record_id,raw_json FROM read_parquet(?)`, output).Scan(&gotID, &raw); err != nil {
		t.Fatal(err)
	}
	if gotID != recordID || !strings.Contains(raw, "kept") || strings.Contains(raw, "forbidden") {
		t.Fatalf("id=%s raw=%s", gotID, raw)
	}
}

func TestExportQueryResultRejectsWholeResultAbovePublicLimit(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	input := filepath.Join(root, "oversized.parquet")
	output := filepath.Join(root, "oversized.jsonl")
	writeQueryParquet(t, ctx, input, `SELECT repeat('x', 8388608) payload`)
	_, err := engine.ExportQueryResult(ctx, engine.QueryExportRequest{Version: 1, InputPath: input, OutputPath: output})
	if !errors.Is(err, engine.ErrQueryExecutionLimit) {
		t.Fatalf("oversized export error=%v", err)
	}
	if _, statErr := os.Stat(output); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("oversized output was retained: %v", statErr)
	}
}

func writeQueryParquet(t *testing.T, ctx context.Context, path, statement string) {
	t.Helper()
	db, err := engine.Open(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `COPY (`+statement+`) TO '`+strings.ReplaceAll(path, "'", "''")+`' (FORMAT PARQUET)`); err != nil {
		t.Fatal(err)
	}
}
