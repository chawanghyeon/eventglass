//go:build duckdb_use_static_lib

package engine_test

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
)

// A high-cardinality second attribute prevents accidental dictionary collapse.
// This single-file fixture provides an independent exact arithmetic count and
// matched scan measurements. The SDK multi-file regression (which exhausted
// the old compiler's query budget) lives in tests/comparison/native_fixture_test.go.
func TestAttributeMillionRowScanWithinNativeProfiles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	root := t.TempDir()
	input := filepath.Join(root, "million.parquet")
	db, err := engine.Open(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, setting := range []string{"SET memory_limit='128MiB'", "SET max_temp_directory_size='0B'", "SET threads=1"} {
		if _, err := db.ExecContext(ctx, setting); err != nil {
			db.Close()
			t.Fatal(err)
		}
	}
	_, err = db.ExecContext(ctx, `COPY (SELECT
		1::BIGINT tenant_id,1::BIGINT project_id,'log' kind,
		100::BIGINT event_time_us,100::BIGINT received_time_us,0::INTEGER lane_id,1::BIGINT batch_seq,
		[{'namespace':'attributes','path':'/v','value_type':'integer','integer_value':(i%1000-500)::DECIMAL(38,0),'string_value':NULL::VARCHAR,'double_value':NULL::DOUBLE,'boolean_value':NULL::BOOLEAN,'json_value':NULL::VARCHAR,'unit':NULL::VARCHAR},
		 {'namespace':'attributes','path':'/unique','value_type':'string','integer_value':NULL::DECIMAL(38,0),'string_value':i::VARCHAR,'double_value':NULL::DOUBLE,'boolean_value':NULL::BOOLEAN,'json_value':NULL::VARCHAR,'unit':NULL::VARCHAR}] attrs
		FROM range(1000000) t(i)) TO '`+strings.ReplaceAll(input, "'", "''")+`' (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 16384)`)
	closeErr := db.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("fixture: %v close=%v", err, closeErr)
	}
	for _, memory := range []int64{192 << 20, 256 << 20} {
		t.Run(fmt.Sprintf("native-%dMiB", memory>>20), func(t *testing.T) {
			node, err := query.ParseCEL(`iattr("attributes", "/v") > 100`)
			if err != nil {
				t.Fatal(err)
			}
			canonical, err := query.CanonicalFilter(node)
			if err != nil {
				t.Fatal(err)
			}
			var cuts [model.LaneCount]int64
			cuts[0] = 1
			plan, err := query.BuildPlan(model.DatasetSpec{TenantID: 1, ProjectIDs: []int64{1}, Kinds: []model.Kind{model.KindLog}, TimeBasis: model.QueryTimeEvent, StartUS: 1, EndUS: 200, Filter: canonical}, model.SnapshotScope{LaneCuts: cuts, RetentionFloorUS: 1}, node)
			if err != nil {
				t.Fatal(err)
			}
			operation, err := query.BuildAggregateOperation(query.AggregateOperationSpec{Plan: plan, Metrics: []query.AggregateMetric{{Name: "count", Op: "count"}}})
			if err != nil {
				t.Fatal(err)
			}
			output := filepath.Join(root, fmt.Sprintf("result-%d.parquet", memory))
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			started := time.Now()
			summary, err := engine.ExecuteQuery(ctx, engine.QueryRequest{Version: 1, QueryID: "attribute-memory", Task: model.QueryTaskKey{Stage: model.QueryTaskScan}, Operation: operation, InputPaths: []string{input}, OutputPath: output, SpillDirectory: output + ".spill", NativeMemoryBytes: memory, NativeSpillBytes: 256 << 20})
			if err != nil || summary.Rows != 1 {
				t.Fatalf("scan rows=%d elapsed=%s err=%v", summary.Rows, time.Since(started), err)
			}
			reader, err := engine.Open(ctx, "")
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			var count int64
			if err := reader.QueryRowContext(ctx, `SELECT m0_valid FROM read_parquet(?)`, output).Scan(&count); err != nil || count != 399000 {
				t.Fatalf("count=%d want=399000 err=%v", count, err)
			}
			elapsed := time.Since(started)
			runtime.ReadMemStats(&after)
			var usage syscall.Rusage
			if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
				t.Fatal(err)
			}
			// Linux maxrss includes fixture construction and prior subtests; Go
			// allocations do not include the engine's unmanaged memory.
			t.Logf("million-row attribute count=%d elapsed=%s native_memory=%d go_alloc_bytes=%d go_allocs=%d process_maxrss=%d platform=%s s3_requests=0 network_bytes=0", count, elapsed, memory, after.TotalAlloc-before.TotalAlloc, after.Mallocs-before.Mallocs, usage.Maxrss, runtime.GOOS)
		})
	}
}
