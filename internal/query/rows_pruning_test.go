package query

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
)

func pruningScope(t testing.TB, sort string, limit int) PlanScope {
	t.Helper()
	scope := testPlanScope()
	filter, err := CanonicalFilter(&Node{Op: "constant", Constant: true})
	if err != nil {
		t.Fatal(err)
	}
	dataset := model.DatasetSpec{TenantID: scope.TenantID, ProjectIDs: []int64{1}, Kinds: []model.Kind{model.KindLog}, TimeBasis: model.QueryTimeReceived, StartUS: 0, EndUS: 1000, Filter: filter}
	digest, encoded, err := DatasetHash(dataset)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := &model.QuerySnapshot{TenantID: scope.TenantID, SnapshotID: scope.SnapshotID, StorageGeneration: scope.Generation, DatasetSHA256: digest, DatasetBytes: encoded}
	snapshotScope := model.SnapshotScope{}
	for lane := range snapshot.Lanes {
		snapshot.Lanes[lane] = model.SnapshotLane{LaneID: lane, CutSeq: 100}
		snapshotScope.LaneCuts[lane] = 100
	}
	compiled, err := BuildPlan(dataset, snapshotScope, &Node{Op: "constant", Constant: true})
	if err != nil {
		t.Fatal(err)
	}
	operation, err := BuildRowOperation(RowOperationSpec{Plan: compiled, Sort: sort, Limit: limit})
	if err != nil {
		t.Fatal(err)
	}
	operation.Result.CursorHash = "response-token-binding"
	scope.Snapshot = snapshot
	setPruningOperation(t, &scope, operation)
	return scope
}

func setPruningOperation(t testing.TB, scope *PlanScope, operation engine.QueryOperation) {
	t.Helper()
	encoded, err := CanonicalOperation(operation)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(encoded)
	scope.Operation, scope.OperationHash = encoded, hex.EncodeToString(digest[:])
}

func pruningFile(index int, minimum, maximum, rows int64) model.CatalogFile {
	return model.CatalogFile{FileID: fmt.Sprintf("%08x-0000-4000-8000-000000000000", index), ObjectKey: fmt.Sprintf("v1/query/%d", index), Bytes: 100, RowCount: rows,
		AllProjectsSelected: true, Kind: model.KindLog, MinEventTimeUS: minimum, MaxEventTimeUS: maximum, MinReceivedTimeUS: minimum, MaxReceivedTimeUS: maximum, MinBatchSeq: 1, MaxBatchSeq: 1}
}

func TestFirstPagePruningKeepsTiesPartialFilesAndLimitPlusOne(t *testing.T) {
	for _, order := range []string{"event_desc", "received_desc"} {
		t.Run(order, func(t *testing.T) {
			scope := pruningScope(t, order, 2)
			files := []model.CatalogFile{pruningFile(1, 0, 49, 100), pruningFile(2, 50, 60, 3), pruningFile(3, 10, 50, 1), pruningFile(4, 51, 2000, 1)}
			files[3].AllProjectsSelected = false
			got := pruneFirstPageRows(scope, files)
			if len(got) != 3 || got[0].FileID != files[1].FileID || got[1].FileID != files[2].FileID || got[2].FileID != files[3].FileID {
				t.Fatalf("selected=%+v", got)
			}
			files[1].RowCount = 2 // The extra has-more row can still be older.
			if got := pruneFirstPageRows(scope, files); len(got) != len(files) {
				t.Fatalf("limit without lookahead lost files: %+v", got)
			}
		})
	}
}

func TestFirstPagePruningFallsBackWhenProofIsMissing(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*PlanScope, []model.CatalogFile)
	}{
		{"no_snapshot", func(s *PlanScope, _ []model.CatalogFile) { s.Snapshot = nil }},
		{"wrong_tenant", func(s *PlanScope, _ []model.CatalogFile) { s.Snapshot.TenantID++ }},
		{"wrong_snapshot", func(s *PlanScope, _ []model.CatalogFile) { s.Snapshot.SnapshotID = "other" }},
		{"wrong_generation", func(s *PlanScope, _ []model.CatalogFile) { s.Snapshot.StorageGeneration++ }},
		{"bad_dataset", func(s *PlanScope, _ []model.CatalogFile) { s.Snapshot.DatasetBytes = nil }},
		{"partial_project", func(_ *PlanScope, f []model.CatalogFile) { f[1].AllProjectsSelected = false }},
		{"partial_time_end", func(_ *PlanScope, f []model.CatalogFile) { f[1].MaxReceivedTimeUS = 1000 }},
		{"partial_time_start", func(_ *PlanScope, f []model.CatalogFile) { f[1].MinReceivedTimeUS = -1 }},
		{"partial_retention", func(s *PlanScope, _ []model.CatalogFile) { s.Snapshot.RetentionFloorUS = 51 }},
		{"partial_lane_cut", func(_ *PlanScope, f []model.CatalogFile) { f[1].MaxBatchSeq = 101 }},
		{"bad_lane", func(_ *PlanScope, f []model.CatalogFile) { f[1].LaneID = model.LaneCount }},
		{"bad_bounds", func(_ *PlanScope, f []model.CatalogFile) { f[1].MinEventTimeUS = 100 }},
		{"wrong_kind", func(_ *PlanScope, f []model.CatalogFile) { f[1].Kind = model.KindError }},
		{"empty_count", func(_ *PlanScope, f []model.CatalogFile) { f[1].RowCount = 0 }},
		{"negative_count", func(_ *PlanScope, f []model.CatalogFile) { f[1].RowCount = -1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			scope := pruningScope(t, "received_desc", 2)
			files := []model.CatalogFile{pruningFile(1, 0, 49, 1), pruningFile(2, 50, 60, 3)}
			test.mutate(&scope, files)
			if got := pruneFirstPageRows(scope, files); len(got) != len(files) {
				t.Fatalf("unsafe pruning: %+v", got)
			}
		})
	}
	for _, kind := range []string{"cursor", "extra_predicate", "aggregate", "limit", "noncanonical"} {
		t.Run(kind, func(t *testing.T) {
			scope := pruningScope(t, "received_desc", 2)
			var operation engine.QueryOperation
			if err := json.Unmarshal(scope.Operation, &operation); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "cursor":
				operation.ScanArguments = append(operation.ScanArguments, engine.QueryArgument{Type: "int64", Value: "10"})
			case "extra_predicate":
				operation.ScanSQL += " AND r.message='match'"
			case "aggregate":
				operation.Kind = "aggregate"
			case "limit":
				operation.Result.Limit = 1001
			}
			setPruningOperation(t, &scope, operation)
			if kind == "noncanonical" {
				scope.Operation = append(scope.Operation, ' ')
			}
			files := []model.CatalogFile{pruningFile(1, 0, 49, 1), pruningFile(2, 50, 60, 3)}
			if got := pruneFirstPageRows(scope, files); len(got) != len(files) {
				t.Fatalf("changed operation pruned: %+v", got)
			}
		})
	}
}

func TestFirstPagePruningRowCountsCannotOverflowAndPlansAreDeterministic(t *testing.T) {
	scope := pruningScope(t, "received_desc", 2)
	files := []model.CatalogFile{pruningFile(1, 0, 49, 1), pruningFile(2, 50, 60, math.MaxInt64), pruningFile(3, 50, 60, math.MaxInt64)}
	first, err := BuildExecutionPlan(scope, files)
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildExecutionPlan(scope, files)
	if err != nil || first.SHA256 != second.SHA256 {
		t.Fatalf("retry hash differs: %v", err)
	}
	var manifest TaskManifest
	if err := decodeExact(first.Tasks[0].Manifest, &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest.Files) != 2 || manifest.Files[0].AllProjectsSelected || bytes.Contains(first.Tasks[0].Manifest, []byte("AllProjectsSelected")) {
		t.Fatalf("proof leaked or plan not pruned: %+v", manifest.Files)
	}
	// Before pruning, retain original catalog order/cardinality validation.
	if _, err := BuildExecutionPlan(scope, append(files, files[0])); err == nil {
		t.Fatal("invalid original catalog accepted")
	}
}

func TestFirstPagePruningMatchesIndependentRowOracle(t *testing.T) {
	random := rand.New(rand.NewPCG(17, 29))
	for trial := 0; trial < 300; trial++ {
		scope := pruningScope(t, "received_desc", 1+random.IntN(20))
		type row struct {
			timestamp int64
			id, file  int
		}
		var rows []row
		var files []model.CatalogFile
		for index := range 40 {
			count := 1 + random.IntN(15)
			base := int64(random.IntN(30)) * 20
			file := pruningFile(index, math.MaxInt64, math.MinInt64, int64(count))
			for range count {
				timestamp := base + int64(random.IntN(20))
				rows = append(rows, row{timestamp: timestamp, id: len(rows), file: index})
				file.MinReceivedTimeUS, file.MaxReceivedTimeUS = min(file.MinReceivedTimeUS, timestamp), max(file.MaxReceivedTimeUS, timestamp)
			}
			file.MinEventTimeUS, file.MaxEventTimeUS = file.MinReceivedTimeUS, file.MaxReceivedTimeUS
			files = append(files, file)
		}
		selected := pruneFirstPageRows(scope, files)
		keep := make(map[string]bool)
		for _, file := range selected {
			keep[file.FileID] = true
		}
		slices.SortFunc(rows, func(a, b row) int {
			if a.timestamp != b.timestamp {
				return int(b.timestamp - a.timestamp)
			}
			return b.id - a.id
		})
		var operation engine.QueryOperation
		if err := json.Unmarshal(scope.Operation, &operation); err != nil {
			t.Fatal(err)
		}
		for _, row := range rows[:operation.Result.Limit+1] {
			if !keep[files[row.file].FileID] {
				t.Fatalf("trial=%d lost top row=%+v", trial, row)
			}
		}
	}
}

// Measures only deterministic planning over the same already-verified catalog.
// It excludes S3/PG, native execution and all service latency or RSS claims.
func BenchmarkFirstPagePlanning(b *testing.B) {
	for _, count := range []int{256, 1024, 8192} {
		files := make([]model.CatalogFile, count)
		for index := range files {
			stamp := int64(index * 900 / count)
			files[index] = pruningFile(index, stamp, stamp, 100)
		}
		for _, pruned := range []bool{false, true} {
			b.Run(fmt.Sprintf("files=%d/pruned=%v", count, pruned), func(b *testing.B) {
				scope := pruningScope(b, "received_desc", 100)
				if !pruned {
					scope.Snapshot = nil
				}
				b.ReportAllocs()
				for b.Loop() {
					plan, err := BuildExecutionPlan(scope, files)
					if err != nil || len(plan.Tasks) == 0 {
						b.Fatalf("plan=%+v err=%v", plan, err)
					}
				}
			})
		}
	}
}
