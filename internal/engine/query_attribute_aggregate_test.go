//go:build duckdb_use_static_lib

package engine_test

import (
	"context"
	"math"
	"path/filepath"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
)

func TestAttributeDoubleGroupsAndHistogramProjection(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	input := filepath.Join(root, "doubles.parquet")
	writeQueryParquet(t, ctx, input, `SELECT
		1::BIGINT tenant_id,2::BIGINT project_id,'log' kind,
		1::BIGINT event_time_us,1::BIGINT received_time_us,0::INTEGER lane_id,0::BIGINT batch_seq,
		[{'namespace':'attributes','path':'/g','value_type':'double','double_value':g,'integer_value':NULL::DECIMAL(38,0),'string_value':NULL::VARCHAR,'boolean_value':NULL::BOOLEAN},
		 {'namespace':'attributes','path':'/v','value_type':CASE WHEN v IS NULL THEN 'null' ELSE 'double' END,'double_value':v,'integer_value':NULL::DECIMAL(38,0),'string_value':NULL::VARCHAR,'boolean_value':NULL::BOOLEAN}] attrs
		FROM (VALUES (CAST('-0.0' AS DOUBLE),1.25::DOUBLE),(0.0::DOUBLE,2.5::DOUBLE),(0.0::DOUBLE,NULL::DOUBLE)) t(g,v)`)
	node, err := query.ParseCEL(`exists("attributes", "/v")`)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := query.CanonicalFilter(node)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := query.BuildPlan(model.DatasetSpec{TenantID: 1, ProjectIDs: []int64{2}, Kinds: []model.Kind{model.KindLog}, TimeBasis: model.QueryTimeEvent, StartUS: -1, EndUS: 1_000_001, Filter: canonical}, model.SnapshotScope{}, node)
	if err != nil {
		t.Fatal(err)
	}
	for _, variant := range []string{"typed", "untyped", "histogram"} {
		t.Run(variant, func(t *testing.T) {
			spec := query.AggregateOperationSpec{Plan: plan, Metrics: []query.AggregateMetric{{Name: "value", Op: "avg", Field: &query.NumericField{Op: "attr", Namespace: "attributes", Path: "/v", Type: query.DoubleType}}}}
			if variant == "histogram" {
				spec.Histogram = &query.AggregateHistogram{IntervalUS: 1_000_000, EmptyBuckets: true}
			} else {
				group := query.GroupDimension{Op: "group_attr", Namespace: "attributes", Path: "/g"}
				if variant == "typed" {
					group.Op, group.Type = "attr", query.DoubleType
				}
				spec.GroupBy = []query.GroupDimension{group}
			}
			op, err := query.BuildAggregateOperation(spec)
			if err != nil {
				t.Fatal(err)
			}
			output := filepath.Join(root, variant+".parquet")
			result, err := engine.ExecuteQuery(ctx, engine.QueryRequest{Version: 1, QueryID: "double-aggregate", Task: model.QueryTaskKey{Stage: model.QueryTaskScan}, Operation: op, InputPaths: []string{input}, OutputPath: output, SpillDirectory: output + ".spill"})
			wantRows := int64(1)
			if variant == "histogram" {
				wantRows = 3
			}
			if err != nil || result.Rows != wantRows {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			db, err := engine.Open(ctx, "")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var sum, min, max float64
			var valid, excluded int64
			if err := db.QueryRowContext(ctx, `SELECT m0_sum,m0_min,m0_max,m0_valid,m0_excluded FROM read_parquet(?) WHERE m0_valid>0`, output).Scan(&sum, &min, &max, &valid, &excluded); err != nil {
				t.Fatal(err)
			}
			if sum != 3.75 || min != 1.25 || max != 2.5 || valid != 2 || excluded != 1 {
				t.Fatalf("sum/min/max=%g/%g/%g valid/excluded=%d/%d", sum, min, max, valid, excluded)
			}
			if variant != "histogram" {
				var kind string
				var zero float64
				if err := db.QueryRowContext(ctx, `SELECT g0_type,g0_double FROM read_parquet(?)`, output).Scan(&kind, &zero); err != nil || kind != "double" || zero != 0 || math.Signbit(zero) {
					t.Fatalf("group=%s/%g err=%v", kind, zero, err)
				}
			} else {
				var empty int
				if err := db.QueryRowContext(ctx, `SELECT count(*) FROM read_parquet(?) WHERE m0_valid=0 AND m0_excluded=0 AND m0_sum IS NULL AND bucket_start_us IN (-1000000,1000000)`, output).Scan(&empty); err != nil || empty != 2 {
					t.Fatalf("empty=%d err=%v", empty, err)
				}
			}
		})
	}
}
