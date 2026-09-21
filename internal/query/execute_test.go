package query

import (
	"errors"
	"fmt"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/model"
)

func TestBuildExecutionPlanAssignsFilesOnceAndBuildsFixedFanInTree(t *testing.T) {
	scope := testPlanScope()
	files := make([]model.CatalogFile, MaxPlanFiles)
	for index := range files {
		files[index] = model.CatalogFile{
			FileID: fmt.Sprintf("%08x-0000-4000-8000-000000000000", index), ObjectKey: fmt.Sprintf("v1/query/%d.parquet", index),
			Bytes: 8 << 20, SHA256: fmt.Sprintf("%064x", index+1), RowCount: 1,
		}
	}
	plan, err := BuildExecutionPlan(scope, files)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ScanCount != 4096 || plan.ReducerCount != 585 || len(plan.Tasks) != 4681 || plan.ManifestBytes > MaxPlanBytes || len(plan.SHA256) != 64 {
		t.Fatalf("plan scans=%d reducers=%d tasks=%d bytes=%d hash=%q", plan.ScanCount, plan.ReducerCount, len(plan.Tasks), plan.ManifestBytes, plan.SHA256)
	}
	seen := make(map[string]bool, len(files))
	for _, task := range plan.Tasks[:plan.ScanCount] {
		var manifest TaskManifest
		if err := decodeExact(task.Manifest, &manifest); err != nil {
			t.Fatal(err)
		}
		if len(manifest.Files) != int(TargetScanBytes/(8<<20)) {
			t.Fatalf("partition %d files=%d", task.Key.PartitionID, len(manifest.Files))
		}
		for _, file := range manifest.Files {
			if seen[file.FileID] {
				t.Fatalf("file assigned twice: %s", file.FileID)
			}
			seen[file.FileID] = true
		}
	}
	if len(seen) != len(files) {
		t.Fatalf("assigned=%d want=%d", len(seen), len(files))
	}
	root := plan.Tasks[len(plan.Tasks)-1]
	if root.Key.Stage != model.QueryTaskReduce || root.Key.Level != 4 || root.Key.PartitionID != 0 || len(root.InputOrdinal) != 8 {
		t.Fatalf("root=%#v", root)
	}
}

func TestBuildExecutionPlanBatchesTinyRowsButBoundsDetailPayloadFanout(t *testing.T) {
	files := make([]model.CatalogFile, MaxFilesPerScan+1)
	for index := range files {
		files[index] = model.CatalogFile{FileID: fmt.Sprintf("%08x-0000-4000-8000-000000000000", index), ObjectKey: fmt.Sprintf("v1/query/tiny-%d.parquet", index), Bytes: 1, SHA256: fmt.Sprintf("%064x", index+1), RowCount: 1}
	}
	rows, err := BuildExecutionPlan(testPlanScope(), files)
	if err != nil || rows.ScanCount != 2 {
		t.Fatalf("rows scans=%d err=%v", rows.ScanCount, err)
	}
	detailScope := testPlanScope()
	detailScope.Operation = []byte(`{"kind":"detail"}`)
	detail, err := BuildExecutionPlan(detailScope, files)
	if err != nil || detail.ScanCount != (len(files)+MaxDetailFilesPerScan-1)/MaxDetailFilesPerScan {
		t.Fatalf("detail scans=%d err=%v", detail.ScanCount, err)
	}
}

func TestScanFileLimitKeepsByteAndManifestCaps(t *testing.T) {
	files := make([]model.CatalogFile, MaxFilesPerScan)
	for index := range files {
		files[index] = model.CatalogFile{FileID: fmt.Sprintf("%08x-0000-4000-8000-000000000000", index), ObjectKey: fmt.Sprintf("v1/query/bounded-%d.parquet", index), Bytes: TargetScanBytes / MaxFilesPerScan, SHA256: fmt.Sprintf("%064x", index+1), RowCount: 1}
	}
	plan, err := BuildExecutionPlan(testPlanScope(), files)
	if err != nil || plan.ScanCount != 1 || len(plan.Tasks) != 1 || len(plan.Tasks[0].Manifest) > MaxTaskManifestBytes {
		t.Fatalf("bounded scan plan=%#v err=%v", plan, err)
	}
	files[0].Bytes++
	plan, err = BuildExecutionPlan(testPlanScope(), files)
	if err != nil || plan.ScanCount != 2 {
		t.Fatalf("byte over-limit plan=%#v err=%v", plan, err)
	}
	for index := range files {
		files[index].Bytes = 1
		files[index].BlockSHA256 = make([]string, 128)
		for block := range files[index].BlockSHA256 {
			files[index].BlockSHA256[block] = fmt.Sprintf("%064x", block+1)
		}
	}
	plan, err = BuildExecutionPlan(testPlanScope(), files)
	if err != nil || plan.ScanCount < 2 {
		t.Fatalf("metadata-heavy files were not split before sealing: scans=%d err=%v", plan.ScanCount, err)
	}
	seen := make(map[string]bool)
	for index, task := range plan.Tasks[:plan.ScanCount] {
		if len(task.Manifest) > MaxTaskManifestBytes || task.Key.PartitionID != index {
			t.Fatalf("invalid bounded partition: %+v bytes=%d", task.Key, len(task.Manifest))
		}
		var manifest TaskManifest
		if err := decodeExact(task.Manifest, &manifest); err != nil {
			t.Fatal(err)
		}
		for _, file := range manifest.Files {
			if seen[file.FileID] {
				t.Fatalf("duplicate file %s", file.FileID)
			}
			seen[file.FileID] = true
		}
	}
	if len(seen) != len(files) {
		t.Fatalf("lost files: %d != %d", len(seen), len(files))
	}
	repeated, err := BuildExecutionPlan(testPlanScope(), files)
	if err != nil || repeated.SHA256 != plan.SHA256 {
		t.Fatalf("metadata split is not deterministic: %s != %s err=%v", repeated.SHA256, plan.SHA256, err)
	}
	many := make([]model.CatalogFile, 2048)
	for index := range many {
		many[index] = files[index%len(files)]
		many[index].FileID = fmt.Sprintf("%08x-0000-4000-8000-000000000000", index)
	}
	if result, err := BuildExecutionPlan(testPlanScope(), many); !errors.Is(err, ErrQueryLimit) || len(result.Tasks) != 0 {
		t.Fatalf("total plan metadata cap bypassed: tasks=%d err=%v", len(result.Tasks), err)
	}
	files[0].BlockSHA256 = make([]string, MaxTaskManifestBytes/64+1)
	for index := range files[0].BlockSHA256 {
		files[0].BlockSHA256[index] = fmt.Sprintf("%064x", index+1)
	}
	if _, err := BuildExecutionPlan(testPlanScope(), files[:1]); !errors.Is(err, ErrQueryLimit) {
		t.Fatalf("oversized single-file manifest accepted: %v", err)
	}
}

func TestBuildExecutionPlanRejectsMetadataCapsAndKeepsLargeFileAlone(t *testing.T) {
	scope := testPlanScope()
	large := model.CatalogFile{FileID: "00000000-0000-4000-8000-000000000001", ObjectKey: "v1/query/large.parquet", Bytes: TargetScanBytes + 1, SHA256: fmt.Sprintf("%064x", 1), RowCount: 1}
	small := model.CatalogFile{FileID: "00000000-0000-4000-8000-000000000002", ObjectKey: "v1/query/small.parquet", Bytes: 1, SHA256: fmt.Sprintf("%064x", 2), RowCount: 1}
	plan, err := BuildExecutionPlan(scope, []model.CatalogFile{large, small})
	if err != nil || plan.ScanCount != 2 {
		t.Fatalf("large-file plan=%#v err=%v", plan, err)
	}
	tooMany := make([]model.CatalogFile, MaxPlanFiles+1)
	if _, err := BuildExecutionPlan(scope, tooMany); !errors.Is(err, ErrQueryLimit) {
		t.Fatalf("file cap=%v", err)
	}
	if _, err := BuildExecutionPlan(scope, []model.CatalogFile{small, large}); err == nil {
		t.Fatal("unsorted catalog accepted")
	}
}

func TestBuildExecutionPlanIsDeterministicAndEmptyIsSealed(t *testing.T) {
	scope := testPlanScope()
	first, err := BuildExecutionPlan(scope, nil)
	if err != nil || first.ScanCount != 0 || first.ReducerCount != 1 || len(first.Tasks) != 1 || len(first.Tasks[0].InputOrdinal) != 0 {
		t.Fatalf("empty plan=%#v err=%v", first, err)
	}
	second, err := BuildExecutionPlan(scope, nil)
	if err != nil || second.SHA256 != first.SHA256 {
		t.Fatalf("hashes %q %q err=%v", first.SHA256, second.SHA256, err)
	}
}

func testPlanScope() PlanScope {
	return PlanScope{
		QueryID: "00000000-0000-4000-8000-000000000101", TenantID: 1,
		SnapshotID: "00000000-0000-4000-8000-000000000102", Generation: 1,
		OperationHash: "abababababababababababababababababababababababababababababababab",
		Operation:     []byte(`{"kind":"search"}`), DeadlineUS: 1_800_000_000_000_000,
	}
}
