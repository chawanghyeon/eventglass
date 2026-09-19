package query

import (
	"strings"
	"testing"
)

func TestBuildDetailOperationPreservesScopeAndBindsRecord(t *testing.T) {
	plan := aggregateTestPlan(t, 0, 10)
	operation, err := BuildDetailOperation(plan, strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	if operation.Kind != "detail" || operation.MaxRows != 2 || !strings.Contains(operation.ScanSQL, "JOIN input_payload") || !strings.Contains(operation.ScanSQL, "r.record_id=?") {
		t.Fatalf("unexpected detail operation: %#v", operation)
	}
	if got, want := strings.Count(operation.ScanSQL, "?"), len(operation.ScanArguments); got != want {
		t.Fatalf("placeholders=%d arguments=%d", got, want)
	}
}
