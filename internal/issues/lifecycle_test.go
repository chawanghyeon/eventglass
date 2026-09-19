package issues

import (
	"math"
	"strings"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/model"
)

func TestLifecycleBacklogRegressionIgnoredAndTupleRelease(t *testing.T) {
	newRelease := func(value string) *string { return &value }
	created, transition, err := ApplyUniqueOccurrence(nil, Occurrence{
		RecordID: strings.Repeat("b", 64), LaneID: 0, BatchSeq: 9, EventTimeUS: 20, EventNS: 1, ReceivedUS: 100, ReleaseJSON: newRelease(`"second"`),
	}, "Issue")
	if err != nil || transition == nil || *transition != TransitionCreated || created.Status != StatusUnresolved || created.Revision != 1 || created.OccurrenceCount != 1 {
		t.Fatalf("created=%#v transition=%v err=%v", created, transition, err)
	}
	cut := make([]int64, model.LaneCount)
	cut[0], cut[1] = 10, 4
	resolved, transition, err := Resolve(created, cut)
	if err != nil || transition == nil || *transition != TransitionResolved || resolved.Status != StatusResolved || resolved.Revision != 2 {
		t.Fatalf("resolved=%#v transition=%v err=%v", resolved, transition, err)
	}
	movedCut := make([]int64, model.LaneCount)
	movedCut[0] = 999
	stillResolved, transition, err := Resolve(resolved, movedCut)
	if err != nil || transition != nil || stillResolved.Revision != resolved.Revision || stillResolved.ResolvedCut[0] != 10 {
		t.Fatalf("same-state resolve moved cut: %#v transition=%v err=%v", stillResolved, transition, err)
	}
	backlog, transition, err := ApplyUniqueOccurrence(&resolved, Occurrence{
		RecordID: strings.Repeat("a", 64), LaneID: 0, BatchSeq: 9, EventTimeUS: 10, EventNS: 2, ReceivedUS: 110, ReleaseJSON: nil,
	}, "ignored title")
	if err != nil || transition != nil || backlog.Status != StatusResolved || backlog.OccurrenceCount != 2 || backlog.First.RecordID != strings.Repeat("a", 64) || backlog.First.ReleaseJSON != nil {
		t.Fatalf("backlog=%#v transition=%v err=%v", backlog, transition, err)
	}
	regressed, transition, err := ApplyUniqueOccurrence(&backlog, Occurrence{
		RecordID: strings.Repeat("c", 64), LaneID: 1, BatchSeq: 5, EventTimeUS: 30, EventNS: 0, ReceivedUS: 120, ReleaseJSON: newRelease(`"third"`),
	}, "ignored title")
	if err != nil || transition == nil || *transition != TransitionRegressed || regressed.Status != StatusUnresolved || regressed.Revision != 3 || regressed.ResolvedCut != nil {
		t.Fatalf("regressed=%#v transition=%v err=%v", regressed, transition, err)
	}
	regressedAgain, transition, err := ApplyUniqueOccurrence(&regressed, Occurrence{
		RecordID: strings.Repeat("e", 64), LaneID: 0, BatchSeq: 11, EventTimeUS: 35, ReceivedUS: 125,
	}, "ignored title")
	if err != nil || transition != nil || regressedAgain.Revision != regressed.Revision {
		t.Fatalf("second regression=%#v transition=%v err=%v", regressedAgain, transition, err)
	}
	regressed = regressedAgain
	ignored, transition, err := Ignore(regressed)
	if err != nil || transition == nil || *transition != TransitionIgnored {
		t.Fatal(ignored, transition, err)
	}
	ignored, transition, err = ApplyUniqueOccurrence(&ignored, Occurrence{
		RecordID: strings.Repeat("d", 64), LaneID: 1, BatchSeq: 6, EventTimeUS: 40, ReceivedUS: 130,
	}, "ignored title")
	if err != nil || transition != nil || ignored.Status != StatusIgnored || ignored.OccurrenceCount != 5 {
		t.Fatalf("ignored occurrence=%#v transition=%v err=%v", ignored, transition, err)
	}
}

func TestLifecycleRejectsInvalidDurableInputs(t *testing.T) {
	invalidRelease := `{"not":"a string"}`
	cases := []Occurrence{
		{RecordID: strings.Repeat("A", 64), LaneID: 0, BatchSeq: 1},
		{RecordID: strings.Repeat("g", 64), LaneID: 0, BatchSeq: 1},
		{RecordID: strings.Repeat("a", 64), LaneID: model.LaneCount, BatchSeq: 1},
		{RecordID: strings.Repeat("a", 64), LaneID: 0, BatchSeq: 0},
		{RecordID: strings.Repeat("a", 64), LaneID: 0, BatchSeq: 1, EventNS: 1000},
		{RecordID: strings.Repeat("a", 64), LaneID: 0, BatchSeq: 1, ReleaseJSON: &invalidRelease},
	}
	for _, occurrence := range cases {
		if _, _, err := ApplyUniqueOccurrence(nil, occurrence, "Issue"); err == nil {
			t.Fatalf("invalid occurrence accepted: %#v", occurrence)
		}
	}
	cut := make([]int64, model.LaneCount)
	cut[3] = -1
	if _, _, err := Resolve(State{Status: StatusUnresolved, Revision: 1}, cut); err == nil {
		t.Fatal("negative resolve cut was accepted")
	}
	if _, _, err := Ignore(State{Status: Status("invalid"), Revision: 1}); err == nil {
		t.Fatal("invalid current status was accepted")
	}
	if _, _, err := Reopen(State{Status: StatusResolved, Revision: math.MaxInt64}); err == nil {
		t.Fatal("revision overflow was accepted")
	}
}

func TestLifecycleTieBreakAndSameStateNoOp(t *testing.T) {
	state, _, err := ApplyUniqueOccurrence(nil, Occurrence{RecordID: strings.Repeat("b", 64), LaneID: 0, BatchSeq: 1, EventTimeUS: 10, EventNS: 1}, "Issue")
	if err != nil {
		t.Fatal(err)
	}
	state, _, err = ApplyUniqueOccurrence(&state, Occurrence{RecordID: strings.Repeat("a", 64), LaneID: 0, BatchSeq: 2, EventTimeUS: 10, EventNS: 1}, "Issue")
	if err != nil || state.First.RecordID != strings.Repeat("a", 64) || state.Last.RecordID != strings.Repeat("b", 64) {
		t.Fatalf("tuple ordering=%#v err=%v", state, err)
	}
	state, transition, err := Reopen(state)
	if err != nil || transition != nil || state.Revision != 1 {
		t.Fatalf("same-state reopen=%#v transition=%v err=%v", state, transition, err)
	}
}
