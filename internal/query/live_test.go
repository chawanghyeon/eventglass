package query

import (
	"math"
	"strings"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/model"
)

func TestLiveScopeHashCanonicalizesSetsAndBindsFilter(t *testing.T) {
	firstFilter, _ := CanonicalFilter(&Node{Op: "constant", Constant: true})
	secondFilter, _ := CanonicalFilter(&Node{Op: "constant", Constant: false})
	first, err := LiveScopeHash(LiveScope{TenantID: 1, Projects: []int64{9, 2}, Kinds: []model.Kind{model.KindLog, model.KindError}, Filter: firstFilter})
	if err != nil {
		t.Fatal(err)
	}
	reordered, err := LiveScopeHash(LiveScope{TenantID: 1, Projects: []int64{2, 9}, Kinds: []model.Kind{model.KindError, model.KindLog}, Filter: firstFilter})
	if err != nil || first != reordered {
		t.Fatalf("reordered=%q first=%q err=%v", reordered, first, err)
	}
	changed, err := LiveScopeHash(LiveScope{TenantID: 1, Projects: []int64{2, 9}, Kinds: []model.Kind{model.KindError, model.KindLog}, Filter: secondFilter})
	if err != nil || changed == first {
		t.Fatalf("changed=%q first=%q err=%v", changed, first, err)
	}
}

func TestAdvanceLivePositionsMovesZeroMatchIntervalToCut(t *testing.T) {
	var cuts [model.LaneCount]int64
	cuts[0], cuts[4] = 9, 3
	positions, err := AdvanceLivePositions(InitialLivePositions([model.LaneCount]int64{}, true), cuts, nil, true)
	if err != nil || positions[0] != (LivePosition{BatchSeq: 9, Ordinal: math.MaxInt32}) || positions[4].BatchSeq != 3 {
		t.Fatalf("positions=%#v err=%v", positions, err)
	}
}

func TestAdvanceLivePositionsPartialPageDoesNotSkipOtherLanes(t *testing.T) {
	var cuts [model.LaneCount]int64
	cuts[0], cuts[1] = 5, 8
	start := InitialLivePositions([model.LaneCount]int64{}, true)
	positions, err := AdvanceLivePositions(start, cuts, []LiveRowPosition{{LaneID: 0, BatchSeq: 2, RecordOrdinal: 7, RecordID: strings.Repeat("a", 64)}}, false)
	if err != nil || positions[0] != (LivePosition{BatchSeq: 2, Ordinal: 7}) || positions[1] != (LivePosition{BatchSeq: 0, Ordinal: -1}) {
		t.Fatalf("positions=%#v err=%v", positions, err)
	}
}

func TestInitialLivePositionsStartsAfterCurrentCutWithoutCatchup(t *testing.T) {
	var cuts [model.LaneCount]int64
	cuts[2] = 12
	positions := InitialLivePositions(cuts, false)
	if positions[2] != (LivePosition{BatchSeq: 12, Ordinal: math.MaxInt32}) || positions[0] != (LivePosition{BatchSeq: 0, Ordinal: -1}) {
		t.Fatalf("positions=%#v", positions)
	}
}
