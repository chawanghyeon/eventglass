package query

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"sort"

	"github.com/chawanghyeon/eventglass/internal/model"
)

const (
	LivePageRows       = 100
	LiveMaximumRows    = 10_000
	LiveMaximumPending = 256 << 10
)

type LiveScope struct {
	TenantID int64
	Projects []int64
	Kinds    []model.Kind
	Filter   []byte
}

type LiveRowPosition struct {
	LaneID        int
	BatchSeq      int64
	RecordOrdinal int
	RecordID      string
}

func LiveScopeHash(scope LiveScope) (string, error) {
	if scope.TenantID <= 0 || len(scope.Projects) == 0 || len(scope.Kinds) == 0 || len(scope.Filter) == 0 {
		return "", errors.New("invalid live scope")
	}
	projects := append([]int64(nil), scope.Projects...)
	sort.Slice(projects, func(i, j int) bool { return projects[i] < projects[j] })
	for index, project := range projects {
		if project <= 0 || index > 0 && projects[index-1] == project {
			return "", errors.New("invalid live projects")
		}
	}
	kinds := append([]model.Kind(nil), scope.Kinds...)
	sort.Slice(kinds, func(i, j int) bool { return kinds[i] < kinds[j] })
	for index, kind := range kinds {
		if kind != model.KindError && kind != model.KindLog && kind != model.KindTransaction || index > 0 && kinds[index-1] == kind {
			return "", errors.New("invalid live kinds")
		}
	}
	encoded, err := json.Marshal(struct {
		Version  int          `json:"version"`
		TenantID int64        `json:"tenant_id"`
		Projects []int64      `json:"projects"`
		Kinds    []model.Kind `json:"kinds"`
		Filter   []byte       `json:"filter"`
	}{1, scope.TenantID, projects, kinds, scope.Filter})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func InitialLivePositions(cuts [model.LaneCount]int64, catchup bool) [model.LaneCount]LivePosition {
	var result [model.LaneCount]LivePosition
	for lane := range result {
		result[lane].Ordinal = -1
		if !catchup && cuts[lane] > 0 {
			result[lane] = LivePosition{BatchSeq: cuts[lane], Ordinal: math.MaxInt32}
		}
	}
	return result
}

// AdvanceLivePositions advances only delivered rows for a partial page. Once a
// captured interval is complete, every lane advances to the captured cut even
// when the filter matched zero rows.
func AdvanceLivePositions(current [model.LaneCount]LivePosition, cuts [model.LaneCount]int64, rows []LiveRowPosition, complete bool) ([model.LaneCount]LivePosition, error) {
	result := current
	for _, row := range rows {
		if row.LaneID < 0 || row.LaneID >= model.LaneCount || row.BatchSeq <= 0 || row.BatchSeq > cuts[row.LaneID] || row.RecordOrdinal < 0 || !validDigest(row.RecordID) {
			return current, errors.New("invalid live row position")
		}
		candidate := LivePosition{BatchSeq: row.BatchSeq, Ordinal: row.RecordOrdinal}
		if compareLivePosition(candidate, result[row.LaneID]) <= 0 {
			return current, errors.New("live rows are not strictly advancing")
		}
		result[row.LaneID] = candidate
	}
	if complete {
		for lane, cut := range cuts {
			if cut < result[lane].BatchSeq {
				return current, errors.New("live cut regressed")
			}
			if cut > 0 {
				result[lane] = LivePosition{BatchSeq: cut, Ordinal: math.MaxInt32}
			}
		}
	}
	return result, nil
}

func compareLivePosition(left, right LivePosition) int {
	if left.BatchSeq < right.BatchSeq || left.BatchSeq == right.BatchSeq && left.Ordinal < right.Ordinal {
		return -1
	}
	if left == right {
		return 0
	}
	return 1
}

// A partial batch remains eligible; only a fully emitted batch can be pruned.
// Compacted files spanning both old and new sequences must remain eligible too.
func LiveMinimumSequences(positions [model.LaneCount]LivePosition) [model.LaneCount]int64 {
	var result [model.LaneCount]int64
	for lane, position := range positions {
		result[lane] = position.BatchSeq
		if position.Ordinal == math.MaxInt32 && position.BatchSeq < math.MaxInt64 {
			result[lane]++
		}
	}
	return result
}
