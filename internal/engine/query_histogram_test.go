//go:build duckdb_use_static_lib

package engine_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
)

func histogramTestOperation(t testing.TB) engine.QueryOperation {
	t.Helper()
	filter := &query.Node{Op: "constant", Constant: true}
	canonical, err := query.CanonicalFilter(filter)
	if err != nil {
		t.Fatal(err)
	}
	scope := model.SnapshotScope{}
	for lane := range scope.LaneCuts {
		scope.LaneCuts[lane] = 100
	}
	plan, err := query.BuildPlan(model.DatasetSpec{TenantID: 1, ProjectIDs: []int64{2}, Kinds: []model.Kind{model.KindLog}, TimeBasis: model.QueryTimeEvent,
		StartUS: 0, EndUS: 16 * 60_000_000, Filter: canonical}, scope, filter)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := query.BuildAggregateOperation(query.AggregateOperationSpec{Plan: plan,
		Metrics: []query.AggregateMetric{{Name: "events", Op: "count"}}, Histogram: &query.AggregateHistogram{IntervalUS: 60_000_000, EmptyBuckets: true}})
	if err != nil {
		t.Fatal(err)
	}
	return operation
}

func histogramTestFiles(t testing.TB, count int) ([]string, int64, string) {
	t.Helper()
	root := t.TempDir()
	db, err := engine.Open(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var bytes int64
	digest := sha256.New()
	paths := make([]string, count)
	for index := range paths {
		paths[index] = filepath.Join(root, fmt.Sprintf("input-%d.parquet", index))
		statement := fmt.Sprintf(`COPY (SELECT 1::BIGINT tenant_id,2::BIGINT project_id,'log'::VARCHAR kind,
			%d+i::BIGINT event_time_us,1::BIGINT received_time_us,0::INTEGER lane_id,1::BIGINT batch_seq
			FROM range(100) t(i)) TO '%s' (FORMAT PARQUET,COMPRESSION ZSTD)`, index%15*60_000_000, strings.ReplaceAll(paths[index], "'", "''"))
		if _, err := db.ExecContext(context.Background(), statement); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(paths[index])
		if err != nil {
			t.Fatal(err)
		}
		bytes += info.Size()
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(info.Size()))
		digest.Write(length[:])
		input, err := os.Open(paths[index])
		if err != nil {
			t.Fatal(err)
		}
		_, copyErr := io.Copy(digest, input)
		closeErr := input.Close()
		if copyErr != nil || closeErr != nil {
			t.Fatalf("hash fixture: %v %v", copyErr, closeErr)
		}
	}
	return paths, bytes, hex.EncodeToString(digest.Sum(nil))
}

func TestHistogramUsesOneNativeInputScan(t *testing.T) {
	paths, _, _ := histogramTestFiles(t, 4)
	db, err := engine.Open(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	quoted := make([]string, len(paths))
	for index, path := range paths {
		quoted[index] = "'" + strings.ReplaceAll(path, "'", "''") + "'"
	}
	if _, err := db.ExecContext(context.Background(), "CREATE TEMP VIEW input_rows AS SELECT * FROM read_parquet(["+strings.Join(quoted, ",")+"])"); err != nil {
		t.Fatal(err)
	}
	operation := histogramTestOperation(t)
	arguments := make([]any, len(operation.ScanArguments))
	for index, argument := range operation.ScanArguments {
		switch argument.Type {
		case "int64":
			arguments[index], err = strconv.ParseInt(argument.Value, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
		case "string":
			arguments[index] = argument.Value
		default:
			t.Fatalf("unexpected fixture argument: %+v", argument)
		}
	}
	var label, encoded string
	if err := db.QueryRowContext(context.Background(), "EXPLAIN (FORMAT JSON) "+operation.ScanSQL, arguments...).Scan(&label, &encoded); err != nil {
		t.Fatal(err)
	}
	type planNode struct {
		Name     string            `json:"name"`
		Children []json.RawMessage `json:"children"`
	}
	var roots []json.RawMessage
	if err := json.Unmarshal([]byte(encoded), &roots); err != nil {
		t.Fatal(err)
	}
	var countScans func(json.RawMessage) int
	countScans = func(encoded json.RawMessage) int {
		var node planNode
		if err := json.Unmarshal(encoded, &node); err != nil {
			t.Fatal(err)
		}
		count := 0
		if strings.Contains(strings.ToUpper(node.Name), "PARQUET") {
			count++
		}
		for _, child := range node.Children {
			count += countScans(child)
		}
		return count
	}
	count := 0
	for _, node := range roots {
		count += countScans(node)
	}
	if count != 1 {
		t.Fatalf("native histogram reads Parquet %d times; plan=%s", count, encoded)
	}
}

func TestHistogramMaterializedGroupsPreserveScopeAndMetricState(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	input := filepath.Join(root, "scope.parquet")
	writeQueryParquet(t, ctx, input, `SELECT * FROM (VALUES
		(1,2,'log',-1,50,0,1,'error',10),
		(1,2,'log',0,50,0,1,'error',NULL),
		(1,2,'log',999999,50,0,1,'error',20),
		(1,2,'log',1000000,50,0,1,'error',NULL),
		(1,2,'log',1000001,50,0,1,'error',99),
		(1,2,'log',-2,50,0,1,'error',99),
		(9,2,'log',0,50,0,1,'error',99),
		(1,9,'log',0,50,0,1,'error',99),
		(1,2,'error',0,50,0,1,'error',99),
		(1,2,'log',0,49,0,1,'error',99),
		(1,2,'log',0,50,0,2,'error',99),
		(1,2,'log',0,50,1,1,'error',99),
		(1,2,'log',0,50,0,1,'info',99))
		t(tenant_id,project_id,kind,event_time_us,received_time_us,lane_id,batch_seq,level,severity_number)`)
	db, err := engine.Open(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, matches := range []bool{true, false} {
		level := "error"
		if !matches {
			level = "missing"
		}
		filter, err := query.ParseCEL("level == '" + level + "'")
		if err != nil {
			t.Fatal(err)
		}
		canonical, err := query.CanonicalFilter(filter)
		if err != nil {
			t.Fatal(err)
		}
		scope := model.SnapshotScope{RetentionFloorUS: 50}
		scope.LaneCuts[0] = 1
		plan, err := query.BuildPlan(model.DatasetSpec{TenantID: 1, ProjectIDs: []int64{2}, Kinds: []model.Kind{model.KindLog}, TimeBasis: model.QueryTimeEvent,
			StartUS: -1, EndUS: 1_000_001, Filter: canonical}, scope, filter)
		if err != nil {
			t.Fatal(err)
		}
		operation, err := query.BuildAggregateOperation(query.AggregateOperationSpec{Plan: plan,
			Metrics:   []query.AggregateMetric{{Name: "events", Op: "count"}, {Name: "severity", Op: "avg", Field: &query.NumericField{Op: "field", Name: "severity_number", Type: query.IntegerType}}},
			Histogram: &query.AggregateHistogram{IntervalUS: 1_000_000, EmptyBuckets: true}})
		if err != nil {
			t.Fatal(err)
		}
		output := filepath.Join(root, fmt.Sprintf("state-%t.parquet", matches))
		summary, err := engine.ExecuteQuery(ctx, engine.QueryRequest{Version: 1, QueryID: "histogram-state", Task: model.QueryTaskKey{Stage: model.QueryTaskScan},
			Operation: operation, InputPaths: []string{input}, OutputPath: output, SpillDirectory: filepath.Join(root, "spill")})
		if err != nil || summary.Rows != 3 {
			t.Fatalf("matches=%t summary=%+v err=%v", matches, summary, err)
		}
		rows, err := db.QueryContext(ctx, `SELECT bucket_start_us,m0_valid,m1_valid,m1_excluded,m1_l0::VARCHAR,
			m1_l1::VARCHAR,m1_l2::VARCHAR,m1_l3::VARCHAR,m1_l4::VARCHAR FROM read_parquet(?) ORDER BY bucket_start_us`, output)
		if err != nil {
			t.Fatal(err)
		}
		index := 0
		for rows.Next() {
			var bucket, count, valid, excluded int64
			var limbs [5]string
			if err := rows.Scan(&bucket, &count, &valid, &excluded, &limbs[0], &limbs[1], &limbs[2], &limbs[3], &limbs[4]); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			wantCount, wantValid, wantExcluded, wantSum := int64(0), int64(0), int64(0), "0"
			if matches {
				wantCount, wantValid, wantSum = 1, 1, strconv.Itoa((index+1)*10)
				if index == 1 {
					wantCount, wantExcluded = 2, 1
				}
				if index == 2 {
					// A populated bucket with only missing operands is not an
					// empty bucket: preserve its excluded count and zero sum.
					wantValid, wantExcluded, wantSum = 0, 1, "0"
				}
			}
			if index >= 3 || bucket != int64(index-1)*1_000_000 || count != wantCount || valid != wantValid || excluded != wantExcluded || limbs != [5]string{wantSum, "0", "0", "0", "0"} {
				rows.Close()
				t.Fatalf("matches=%t bucket=%d count=%d valid=%d excluded=%d limbs=%v", matches, bucket, count, valid, excluded, limbs)
			}
			index++
		}
		rowErr := rows.Err()
		rows.Close()
		if rowErr != nil || index != 3 {
			t.Fatalf("rows=%d err=%v", index, rowErr)
		}
	}
}

func TestHistogramMaterializedGroupsMaximumBuckets(t *testing.T) {
	paths, _, _ := histogramTestFiles(t, 1)
	filter := &query.Node{Op: "constant", Constant: true}
	canonical, err := query.CanonicalFilter(filter)
	if err != nil {
		t.Fatal(err)
	}
	scope := model.SnapshotScope{}
	scope.LaneCuts[0] = 1
	plan, err := query.BuildPlan(model.DatasetSpec{TenantID: 1, ProjectIDs: []int64{2}, Kinds: []model.Kind{model.KindLog}, TimeBasis: model.QueryTimeEvent,
		StartUS: 0, EndUS: 2_000_000_000, Filter: canonical}, scope, filter)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := query.BuildAggregateOperation(query.AggregateOperationSpec{Plan: plan, Metrics: []query.AggregateMetric{{Name: "events", Op: "count"}}, Histogram: &query.AggregateHistogram{IntervalUS: 1_000_000, EmptyBuckets: true}})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	output := filepath.Join(root, "maximum.parquet")
	summary, err := engine.ExecuteQuery(context.Background(), engine.QueryRequest{Version: 1, QueryID: "maximum-buckets", Task: model.QueryTaskKey{Stage: model.QueryTaskScan},
		Operation: operation, InputPaths: paths, OutputPath: output, SpillDirectory: filepath.Join(root, "spill"), NativeMemoryBytes: 32 << 20, NativeSpillBytes: 64 << 20})
	if err != nil || summary.Rows != 2000 {
		t.Fatalf("summary=%+v err=%v", summary, err)
	}
	db, err := engine.Open(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var total, zeros, last int64
	if err := db.QueryRowContext(context.Background(), `SELECT sum(m0_valid),count(*) FILTER (WHERE m0_valid=0),max(bucket_start_us) FROM read_parquet(?)`, output).Scan(&total, &zeros, &last); err != nil {
		t.Fatal(err)
	}
	if total != 100 || zeros != 1999 || last != 1_999_000_000 {
		t.Fatalf("total=%d zeros=%d last=%d", total, zeros, last)
	}
}

// Includes actual native open/COPY/output SHA inspection, but no PG/S3 or
// process supervisor. Fixture setup is outside each timed sample.
func BenchmarkHistogramEmptyBuckets(b *testing.B) {
	for _, files := range []int{32, 256} {
		b.Run(fmt.Sprintf("files=%d", files), func(b *testing.B) {
			paths, inputBytes, digest := histogramTestFiles(b, files)
			b.Logf("fixture sha256=%s files=%d rows=%d bytes=%d", digest, files, files*100, inputBytes)
			root := b.TempDir()
			request := engine.QueryRequest{Version: 1, QueryID: "histogram-benchmark", Task: model.QueryTaskKey{Stage: model.QueryTaskScan},
				Operation: histogramTestOperation(b), InputPaths: paths, OutputPath: filepath.Join(root, "output.parquet"), SpillDirectory: filepath.Join(root, "spill"),
				NativeMemoryBytes: 256 << 20, NativeSpillBytes: 256 << 20}
			b.ReportAllocs()
			for b.Loop() {
				if err := os.Remove(request.OutputPath); err != nil && !errors.Is(err, os.ErrNotExist) {
					b.Fatal(err)
				}
				summary, err := engine.ExecuteQuery(context.Background(), request)
				if err != nil || summary.Rows != 16 || summary.DuckDBVersion != "v2.0.0-dev84020" {
					b.Fatalf("summary=%+v err=%v", summary, err)
				}
			}
			// Outside timed work, compare every bucket against the fixture's
			// independently counted distribution, not just the output row count.
			db, err := engine.Open(context.Background(), "")
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			rows, err := db.QueryContext(context.Background(), `SELECT bucket_start_us,m0_valid,m0_excluded FROM read_parquet(?) ORDER BY bucket_start_us`, request.OutputPath)
			if err != nil {
				b.Fatal(err)
			}
			defer rows.Close()
			index := 0
			for rows.Next() {
				var bucket, count, excluded int64
				if err := rows.Scan(&bucket, &count, &excluded); err != nil {
					b.Fatal(err)
				}
				want := 0
				if index < 15 {
					want = files / 15 * 100
					if index < files%15 {
						want += 100
					}
				}
				if bucket != int64(index)*60_000_000 || count != int64(want) || excluded != 0 {
					b.Fatalf("bucket=%d count=%d want=%d excluded=%d", bucket, count, want, excluded)
				}
				index++
			}
			if err := rows.Err(); err != nil || index != 16 {
				b.Fatalf("buckets=%d err=%v", index, err)
			}
			b.ReportMetric(float64(inputBytes), "input-B")
		})
	}
}
