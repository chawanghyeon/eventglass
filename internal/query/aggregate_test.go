package query

import (
	"fmt"
	"strings"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/model"
)

func TestEightAttributeMetricsAndTwoGroupsFitOperationBudget(t *testing.T) {
	spec := AggregateOperationSpec{Plan: aggregateTestPlan(t, 0, 10)}
	for i := range 2 {
		spec.GroupBy = append(spec.GroupBy, GroupDimension{Op: "group_attr", Namespace: "attributes", Path: "/" + strings.Repeat("g", 1022) + fmt.Sprint(i)})
	}
	for i := range 8 {
		spec.Metrics = append(spec.Metrics, AggregateMetric{Name: fmt.Sprintf("metric%d", i), Op: "sum", Field: &NumericField{Op: "attr", Namespace: "attributes", Path: "/" + strings.Repeat("v", 1022) + fmt.Sprint(i), Type: IntegerType}})
	}
	op, err := BuildAggregateOperation(spec)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := CanonicalOperation(op)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > 65536 {
		t.Fatalf("operation exceeds 64KiB budget: %d", len(encoded))
	}
	if got, want := strings.Count(op.ScanSQL, "?"), len(op.ScanArguments); got != want {
		t.Fatalf("placeholders=%d arguments=%d", got, want)
	}
	t.Logf("maximum attribute aggregate operation bytes=%d", len(encoded))
}

func TestBuildAggregateOperationBindsRepeatedTypedAttributeExpressions(t *testing.T) {
	plan := aggregateTestPlan(t, 0, 10_000_000)
	operation, err := BuildAggregateOperation(AggregateOperationSpec{
		Plan:    plan,
		GroupBy: []GroupDimension{{Op: "attr", Namespace: "attributes", Path: "/region", Type: StringType}},
		Metrics: []AggregateMetric{
			{Name: "events", Op: "count"},
			{Name: "latency", Op: "avg", Field: &NumericField{Op: "attr", Namespace: "attributes", Path: "/latency", Type: IntegerType}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Count(operation.ScanSQL, "?"), len(operation.ScanArguments); got != want {
		t.Fatalf("scan placeholders=%d arguments=%d\n%s", got, want, operation.ScanSQL)
	}
	if !strings.Contains(operation.ScanSQL, "m1_l4") || !strings.Contains(operation.ReduceSQL, "sum(r.m1_l4)") {
		t.Fatalf("integer limbs not preserved: scan=%s reduce=%s", operation.ScanSQL, operation.ReduceSQL)
	}
}

func TestBuildAggregateOperationNormalizesDoubleNegativeZero(t *testing.T) {
	operation, err := BuildAggregateOperation(AggregateOperationSpec{
		Plan: aggregateTestPlan(t, 0, 10),
		GroupBy: []GroupDimension{
			{Op: "attr", Namespace: "attributes", Path: "/typed", Type: DoubleType},
			{Op: "group_attr", Namespace: "attributes", Path: "/dynamic"},
		},
		Metrics: []AggregateMetric{{Name: "events", Op: "count"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Count(operation.ScanSQL, "?"), len(operation.ScanArguments); got != want {
		t.Fatalf("scan placeholders=%d arguments=%d\n%s", got, want, operation.ScanSQL)
	}
	if got := strings.Count(operation.ScanSQL, "=0 THEN 0.0"); got != 2 {
		t.Fatalf("double zero normalization count=%d: %s", got, operation.ScanSQL)
	}
}

func TestBuildAggregateOperationEmitsNegativeEpochEmptyBuckets(t *testing.T) {
	operation, err := BuildAggregateOperation(AggregateOperationSpec{
		Plan: aggregateTestPlan(t, -1, 1_000_001), Metrics: []AggregateMetric{{Name: "events", Op: "count"}},
		Histogram: &AggregateHistogram{IntervalUS: 1_000_000, EmptyBuckets: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, bucket := range []string{"-1000000", "0", "1000000"} {
		if !strings.Contains(operation.EmptySQL, bucket) {
			t.Fatalf("empty histogram lacks bucket %s: %s", bucket, operation.EmptySQL)
		}
	}
	if !strings.Contains(operation.ScanSQL, "NOT EXISTS") {
		t.Fatalf("scan does not fill missing buckets: %s", operation.ScanSQL)
	}
	if got, want := strings.Count(operation.ScanSQL, "?"), len(operation.ScanArguments); got != want {
		t.Fatalf("scan placeholders=%d arguments=%d", got, want)
	}
}

func aggregateTestPlan(t *testing.T, startUS, endUS int64) CompiledPlan {
	t.Helper()
	root := &Node{Op: "constant", Constant: true}
	filter, err := CanonicalFilter(root)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildPlan(model.DatasetSpec{
		TenantID: 1, ProjectIDs: []int64{2}, Kinds: []model.Kind{model.KindLog},
		TimeBasis: model.QueryTimeEvent, StartUS: startUS, EndUS: endUS, Filter: filter,
	}, model.SnapshotScope{}, root)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}
