package issues

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"slices"

	"github.com/chawanghyeon/eventglass/internal/model"
)

type Status string

const (
	StatusUnresolved Status = "unresolved"
	StatusResolved   Status = "resolved"
	StatusIgnored    Status = "ignored"
)

type TransitionType string

const (
	TransitionCreated   TransitionType = "created"
	TransitionRegressed TransitionType = "regressed"
	TransitionResolved  TransitionType = "resolved"
	TransitionIgnored   TransitionType = "ignored"
	TransitionReopened  TransitionType = "reopened"
)

type EventTuple struct {
	TimeUS      int64
	NS          uint16
	RecordID    string
	ReleaseJSON *string
}

type Occurrence struct {
	RecordID    string
	LaneID      int
	BatchSeq    int64
	EventTimeUS int64
	EventNS     uint16
	ReceivedUS  int64
	ReleaseJSON *string
}

type State struct {
	Status          Status
	Revision        int64
	OccurrenceCount int64
	First           EventTuple
	Last            EventTuple
	LastReceivedUS  int64
	ResolvedCut     []int64
	Title           string
}

func ApplyUniqueOccurrence(current *State, occurrence Occurrence, title string) (State, *TransitionType, error) {
	if !validRecordID(occurrence.RecordID) || occurrence.LaneID < 0 || occurrence.LaneID >= model.LaneCount || occurrence.BatchSeq <= 0 || occurrence.EventNS > 999 || !validReleaseJSON(occurrence.ReleaseJSON) {
		return State{}, nil, errors.New("invalid Issue occurrence")
	}
	tuple := EventTuple{occurrence.EventTimeUS, occurrence.EventNS, occurrence.RecordID, occurrence.ReleaseJSON}
	if current == nil {
		transition := TransitionCreated
		return State{Status: StatusUnresolved, Revision: 1, OccurrenceCount: 1, First: tuple, Last: tuple, LastReceivedUS: occurrence.ReceivedUS, Title: truncateRunes(title, 512)}, &transition, nil
	}
	next := *current
	next.ResolvedCut = slices.Clone(current.ResolvedCut)
	if next.OccurrenceCount == math.MaxInt64 {
		return State{}, nil, errors.New("Issue occurrence count overflow")
	}
	next.OccurrenceCount++
	if lessEventTuple(tuple, next.First) {
		next.First = tuple
	}
	if lessEventTuple(next.Last, tuple) {
		next.Last = tuple
	}
	if occurrence.ReceivedUS > next.LastReceivedUS {
		next.LastReceivedUS = occurrence.ReceivedUS
	}
	if next.Status == StatusResolved {
		if len(next.ResolvedCut) != model.LaneCount {
			return State{}, nil, errors.New("resolved Issue requires a complete lane cut")
		}
		if occurrence.BatchSeq > next.ResolvedCut[occurrence.LaneID] {
			if next.Revision == math.MaxInt64 {
				return State{}, nil, errors.New("Issue revision overflow")
			}
			next.Status = StatusUnresolved
			next.Revision++
			next.ResolvedCut = nil
			transition := TransitionRegressed
			return next, &transition, nil
		}
	}
	return next, nil, nil
}

func Resolve(current State, cut []int64) (State, *TransitionType, error) {
	if len(cut) != model.LaneCount {
		return State{}, nil, errors.New("resolve requires all lane cuts")
	}
	for _, batchSeq := range cut {
		if batchSeq < 0 {
			return State{}, nil, errors.New("resolve cut cannot contain negative sequence")
		}
	}
	if current.Status == StatusResolved {
		return current, nil, nil
	}
	return mutateStatus(current, StatusResolved, TransitionResolved, cut)
}

func Ignore(current State) (State, *TransitionType, error) {
	if current.Status == StatusIgnored {
		return current, nil, nil
	}
	return mutateStatus(current, StatusIgnored, TransitionIgnored, nil)
}

func Reopen(current State) (State, *TransitionType, error) {
	if current.Status == StatusUnresolved {
		return current, nil, nil
	}
	return mutateStatus(current, StatusUnresolved, TransitionReopened, nil)
}

func mutateStatus(current State, status Status, transition TransitionType, cut []int64) (State, *TransitionType, error) {
	if current.Revision <= 0 || current.Revision == math.MaxInt64 || !validStatus(current.Status) {
		return State{}, nil, errors.New("invalid Issue revision")
	}
	current.Status = status
	current.Revision++
	current.ResolvedCut = slices.Clone(cut)
	return current, &transition, nil
}

func validRecordID(value string) bool {
	if len(value) != 64 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && hex.EncodeToString(decoded) == value
}

func validReleaseJSON(value *string) bool {
	if value == nil {
		return true
	}
	var decoded string
	return json.Unmarshal([]byte(*value), &decoded) == nil
}

func validStatus(status Status) bool {
	return status == StatusUnresolved || status == StatusResolved || status == StatusIgnored
}

func lessEventTuple(left, right EventTuple) bool {
	if left.TimeUS != right.TimeUS {
		return left.TimeUS < right.TimeUS
	}
	if left.NS != right.NS {
		return left.NS < right.NS
	}
	return left.RecordID < right.RecordID
}
