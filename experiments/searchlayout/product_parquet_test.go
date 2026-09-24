//go:build duckdb_use_static_lib

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	queryplan "github.com/chawanghyeon/eventglass/internal/query"
)

type productParquetOracle struct {
	count int64
	sum   int64
}

func productParquetRaw(i int, noisy bool) ([]byte, error) {
	message := "request completed"
	if i%97 == 0 {
		message = "fatal timeout 한글"
	}
	rawFields := map[string]any{"message": message, "service": []string{"api", "worker", "sdk"}[i%3], "latency": int64(i % 1000), "secret": "[Filtered]"}
	if noisy {
		rawFields["trace"] = fmt.Sprintf("trace-%064x", i)
	}
	return json.Marshal(rawFields)
}

func productParquetFixture(t *testing.T, path string, rows int, noisy bool) map[string]productParquetOracle {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	encoder := json.NewEncoder(f)
	want := map[string]productParquetOracle{"scalar": {}, "regex": {}, "dynamic": {}, "dynamic_sparse": {}}
	const epoch = int64(1_700_000_000_000_000)
	for i := range rows {
		project := int64(10 + i%2)
		service := []string{"api", "worker", "sdk"}[i%3]
		message := "request completed"
		if i%97 == 0 {
			message = "fatal timeout 한글"
		}
		severity := int16(i % 10)
		latency := int64(i % 1000)
		latencyText := fmt.Sprint(latency)
		raw, err := productParquetRaw(i, noisy)
		if err != nil {
			t.Fatal(err)
		}
		id := fmt.Sprintf("%064x", i+1)
		record := model.Record{
			TenantID: 1, ProjectID: project, RecordID: id,
			AcceptanceID: "00000000-0000-4000-8000-000000000111", ItemOrdinal: i,
			Kind: model.KindLog, EventTimeUS: epoch + int64(i)*1000, ArrivalTimeUS: epoch + int64(i)*1000 + 1,
			Message: message, Level: "info", SeverityNumber: &severity, Service: &service,
			Attrs:        []model.Attribute{{Namespace: "attributes", Path: "/latency", ValueType: "integer", IntegerValue: &latencyText}},
			SearchValues: []string{message, service}, Raw: raw,
			SchemaVersion: model.SchemaVersion, NormalizerVersion: model.NormalizerVersion, ScrubVersion: model.ScrubVersion,
		}
		if noisy {
			value := fmt.Sprintf("v%d", i)
			record.Attrs = append(record.Attrs, model.Attribute{Namespace: "attributes", Path: fmt.Sprintf("/custom/%d", i%1000), ValueType: "string", StringValue: &value})
			record.SearchValues = append(record.SearchValues, "trace-"+record.RecordID)
		}
		staged := engine.StageRecord{
			Version: engine.ConversionProtocolVersion, GlobalOrdinal: i,
			BatchID: "00000000-0000-4000-8000-000000000222", LaneID: 3, BatchSeq: 4,
			ReceivedTimeUS: epoch + int64(i)*1000 + 2, GroupingVersion: 1, Record: record,
		}
		if err := encoder.Encode(staged); err != nil {
			t.Fatal(err)
		}
		if project != 10 {
			continue
		}
		if message == "fatal timeout 한글" {
			entry := want["scalar"]
			entry.count++
			entry.sum += int64(severity)
			want["scalar"] = entry
			entry = want["regex"]
			entry.count++
			entry.sum += int64(severity)
			want["regex"] = entry
		}
		if latency >= 500 {
			entry := want["dynamic"]
			entry.count++
			entry.sum += latency
			want["dynamic"] = entry
		}
		if noisy && i%1000 == 236 {
			entry := want["dynamic_sparse"]
			entry.count++
			entry.sum += int64(severity)
			want["dynamic_sparse"] = entry
		}
	}
	return want
}

func productParquetMetric(t *testing.T, ctx context.Context, db *sql.DB, path, query string) productParquetOracle {
	t.Helper()
	var got productParquetOracle
	if err := db.QueryRowContext(ctx, query, path).Scan(&got.count, &got.sum); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestActualConverterSingleParquetParity(t *testing.T) {
	if os.Getenv("EVENTGLASS_PRODUCT_PARQUET") != "1" {
		t.Skip("opt-in actual converter Parquet comparison")
	}
	rows := 10000
	if value := os.Getenv("EVENTGLASS_PRODUCT_PARQUET_ROWS"); value != "" {
		if value != "100000" {
			t.Fatal("EVENTGLASS_PRODUCT_PARQUET_ROWS must be 100000 when set")
		}
		rows = 100000
	}
	ctx := context.Background()
	root := t.TempDir()
	stage := filepath.Join(root, "selected.jsonl")
	noisy := os.Getenv("EVENTGLASS_PRODUCT_PARQUET_NOISY") == "1"
	want := productParquetFixture(t, stage, rows, noisy)
	firstRaw, err := productParquetRaw(0, noisy)
	if err != nil {
		t.Fatal(err)
	}
	request := engine.ConversionRequest{
		Version: engine.ConversionProtocolVersion, StagePath: stage,
		OutputDirectory: filepath.Join(root, "output"), SpillDirectory: filepath.Join(root, "spill"),
		TenantID: 1, LaneID: 3, BatchSeq: 4, BatchID: "00000000-0000-4000-8000-000000000222",
		SelectedRecords: rows, NativeMemoryBytes: 256 << 20, NativeSpillBytes: 128 << 20,
	}
	var bundles []engine.ConvertedBundle
	summary, err := engine.Convert(ctx, request, func(bundle engine.ConvertedBundle) error {
		bundles = append(bundles, bundle)
		return nil
	})
	if err != nil || summary.DuckDBVersion != "v2.0.0-dev84020" || len(bundles) != 1 || bundles[0].RowCount != int64(rows) {
		t.Fatalf("conversion summary=%+v bundles=%d err=%v", summary, len(bundles), err)
	}
	analytics, payload := bundles[0].Analytics.Path, bundles[0].Payload.Path
	single := filepath.Join(root, "single.parquet")
	db, err := engine.Open(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	statement := "COPY (SELECT a.*,p.raw_json,p.envelope_sdk_json,p.normalization_warnings_json,p.canonical_metadata_json FROM read_parquet(" + universalSQLLiteral(analytics) + ") a JOIN read_parquet(" + universalSQLLiteral(payload) + ") p USING(record_id) ORDER BY a.project_id,a.service NULLS FIRST,a.event_time_us,a.record_id) TO " + universalSQLLiteral(single) + " (FORMAT PARQUET,COMPRESSION ZSTD,COMPRESSION_LEVEL 3,ROW_GROUP_SIZE 16384)"
	if _, err := db.ExecContext(ctx, statement); err != nil {
		t.Fatal(err)
	}
	queries := map[string]string{
		"scalar":  "SELECT count(*),coalesce(sum(severity_number),0)::BIGINT FROM read_parquet(?) WHERE project_id=10 AND list_contains(search_values,'fatal timeout 한글')",
		"regex":   "SELECT count(*),coalesce(sum(severity_number),0)::BIGINT FROM read_parquet(?) WHERE project_id=10 AND regexp_matches(message,'timeout|한글')",
		"dynamic": "SELECT count(*),coalesce(sum(attr.integer_value::BIGINT),0)::BIGINT FROM read_parquet(?) r, UNNEST(r.attrs) AS x(attr) WHERE project_id=10 AND attr.namespace='attributes' AND attr.path='/latency' AND attr.integer_value>=500",
	}
	if noisy {
		queries["dynamic_sparse"] = "SELECT count(*),coalesce(sum(severity_number),0)::BIGINT FROM read_parquet(?) r, UNNEST(r.attrs) AS x(attr) WHERE project_id=10 AND attr.namespace='attributes' AND attr.path='/custom/236'"
	}
	for name, query := range queries {
		paired := productParquetMetric(t, ctx, db, analytics, query)
		merged := productParquetMetric(t, ctx, db, single, query)
		if paired != want[name] || merged != want[name] {
			t.Fatalf("query=%s paired=%+v single=%+v oracle=%+v", name, paired, merged, want[name])
		}
	}
	for _, path := range []string{payload, single} {
		resultRows, err := db.QueryContext(ctx, "SELECT record_id,raw_json FROM read_parquet(?) ORDER BY record_id", path)
		if err != nil {
			t.Fatal(err)
		}
		seen := 0
		for resultRows.Next() {
			var id, raw string
			if err := resultRows.Scan(&id, &raw); err != nil {
				resultRows.Close()
				t.Fatal(err)
			}
			value, err := strconv.ParseUint(id, 16, 64)
			if err != nil || value == 0 || value > uint64(rows) {
				resultRows.Close()
				t.Fatalf("invalid record id=%s err=%v", id, err)
			}
			wantRaw, err := productParquetRaw(int(value)-1, noisy)
			if err != nil || !reflect.DeepEqual([]byte(raw), wantRaw) || !strings.Contains(raw, "[Filtered]") {
				resultRows.Close()
				t.Fatalf("raw mismatch id=%s", id)
			}
			seen++
		}
		if err := resultRows.Err(); err != nil {
			resultRows.Close()
			t.Fatal(err)
		}
		resultRows.Close()
		if seen != rows {
			t.Fatalf("path=%s records=%d want=%d", path, seen, rows)
		}
	}
	// The current child requires distinct analytics and payload paths. A hard
	// link proves one physical Parquet can provide both roles without changing
	// its query SQL; an S3 integration would need two scoped capability URLs.
	alias := filepath.Join(root, "single-payload.parquet")
	if err := os.Link(single, alias); err != nil {
		t.Fatal(err)
	}
	filter := &queryplan.Node{Op: "constant", Constant: true}
	canonical, err := queryplan.CanonicalFilter(filter)
	if err != nil {
		t.Fatal(err)
	}
	var cuts [model.LaneCount]int64
	cuts[3] = 4
	plan, err := queryplan.BuildPlan(model.DatasetSpec{
		TenantID: 1, ProjectIDs: []int64{10}, Kinds: []model.Kind{model.KindLog},
		TimeBasis: model.QueryTimeEvent, StartUS: 1_700_000_000_000_000,
		EndUS: 1_700_000_000_000_000 + int64(rows)*1000, Filter: canonical,
	}, model.SnapshotScope{LaneCuts: cuts}, filter)
	if err != nil {
		t.Fatal(err)
	}
	aggregate, err := queryplan.BuildAggregateOperation(queryplan.AggregateOperationSpec{Plan: plan, Metrics: []queryplan.AggregateMetric{{Name: "count", Op: "count"}}})
	if err != nil {
		t.Fatal(err)
	}
	id := fmt.Sprintf("%064x", 1)
	detail, err := queryplan.BuildDetailOperation(plan, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ExecuteQuery(ctx, engine.QueryRequest{
		Version: 1, QueryID: "same-path-rejected", Task: model.QueryTaskKey{Stage: model.QueryTaskScan},
		Operation: detail, InputPaths: []string{single}, PayloadPaths: []string{single},
		OutputPath: filepath.Join(root, "same-path.parquet"), SpillDirectory: filepath.Join(root, "same-path.spill"),
	}); err == nil || !strings.Contains(err.Error(), "invalid query input path") {
		t.Fatalf("same-path detail was not rejected: %v", err)
	}
	for _, source := range []struct{ name, analytics, payload string }{{"pair", analytics, payload}, {"single", single, alias}} {
		for _, operation := range []engine.QueryOperation{aggregate, detail} {
			output := filepath.Join(root, source.name+"-"+operation.Kind+".parquet")
			var payloadPaths []string
			if operation.Kind == "detail" {
				payloadPaths = []string{source.payload}
			}
			result, err := engine.ExecuteQuery(ctx, engine.QueryRequest{
				Version: 1, QueryID: source.name + "-" + operation.Kind,
				Task: model.QueryTaskKey{Stage: model.QueryTaskScan}, Operation: operation,
				InputPaths: []string{source.analytics}, OutputPath: output, SpillDirectory: output + ".spill",
				NativeMemoryBytes: 128 << 20, NativeSpillBytes: 128 << 20,
				PayloadPaths: payloadPaths,
			})
			if err != nil || result.Rows != 1 {
				t.Fatalf("product query source=%s kind=%s rows=%d err=%v", source.name, operation.Kind, result.Rows, err)
			}
			if operation.Kind == "aggregate" {
				var count int64
				if err := db.QueryRowContext(ctx, "SELECT m0_valid FROM read_parquet(?)", output).Scan(&count); err != nil || count != int64(rows/2) {
					t.Fatalf("product aggregate source=%s count=%d err=%v", source.name, count, err)
				}
			} else {
				var gotID, raw string
				if err := db.QueryRowContext(ctx, "SELECT record_id,raw_json FROM read_parquet(?)", output).Scan(&gotID, &raw); err != nil || gotID != id || !reflect.DeepEqual([]byte(raw), firstRaw) {
					t.Fatalf("product detail source=%s id=%s err=%v", source.name, gotID, err)
				}
			}
		}
	}
	if os.Getenv("EVENTGLASS_PRODUCT_GATEWAY") == "1" {
		measureProductGateway(t, ctx, root, db, analytics, payload, single, aggregate, detail, firstRaw, rows)
	}
	if os.Getenv("EVENTGLASS_PRODUCT_MINIO") == "1" {
		measureProductMinIO(t, ctx, root, db, analytics, payload, single, aggregate, detail, firstRaw, rows)
	}
	if os.Getenv("EVENTGLASS_PRODUCT_POSTINGS") == "1" {
		measureProductPostingSidecar(t, ctx, db, root, single, rows, noisy)
	}
	if os.Getenv("EVENTGLASS_PRODUCT_UNIFIED") == "1" {
		measureProductUnified(t, ctx, root, stage, analytics, payload, rows, noisy)
	}
	analyticsInfo, err := os.Stat(analytics)
	if err != nil {
		t.Fatal(err)
	}
	payloadInfo, err := os.Stat(payload)
	if err != nil {
		t.Fatal(err)
	}
	singleInfo, err := os.Stat(single)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("actual_converter rows=%d noisy=%t analytics_bytes=%d payload_bytes=%d pair_bytes=%d single_bytes=%d", rows, noisy, analyticsInfo.Size(), payloadInfo.Size(), analyticsInfo.Size()+payloadInfo.Size(), singleInfo.Size())
}
