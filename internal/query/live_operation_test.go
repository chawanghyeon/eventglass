package query

import (
	"testing"

	"github.com/chawanghyeon/eventglass/internal/model"
)

func TestBuildLiveOperationBindsEveryLanePosition(t *testing.T) {
	filter := &Node{Op: "constant", Constant: true}
	canonical, _ := CanonicalFilter(filter)
	plan, err := BuildPlan(model.DatasetSpec{TenantID: 1, ProjectIDs: []int64{2}, Kinds: []model.Kind{model.KindLog}, TimeBasis: model.QueryTimeReceived, StartUS: 1, EndUS: 10, Filter: canonical}, model.SnapshotScope{}, filter)
	if err != nil {
		t.Fatal(err)
	}
	var positions [model.LaneCount]LivePosition
	for lane := range positions {
		positions[lane].Ordinal = -1
	}
	operation, err := BuildLiveOperation(plan, positions)
	if err != nil {
		t.Fatal(err)
	}
	if operation.Kind != "live" || operation.Result.Sort != "live_asc" || len(operation.ScanArguments) != len(plan.Where.Args)+model.LaneCount*4+1 {
		t.Fatalf("operation=%#v", operation)
	}
}
