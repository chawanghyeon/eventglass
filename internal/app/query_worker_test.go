package app

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

type queryControlFixture struct {
	registration *control.QueryIntentRegistration
	uploaded     *control.IntentAuthority
	completed    *control.CompleteQueryTaskCommand
}

func (fixture *queryControlFixture) RegisterQueryIntent(_ context.Context, value control.QueryIntentRegistration) error {
	fixture.registration = &value
	return nil
}
func (fixture *queryControlFixture) MarkQueryIntentUploaded(_ context.Context, _ control.QueryTaskAuthority, value control.IntentAuthority) error {
	fixture.uploaded = &value
	return nil
}
func (fixture *queryControlFixture) CompleteQueryTask(_ context.Context, value control.CompleteQueryTaskCommand) error {
	fixture.completed = &value
	return nil
}

type queryRunnerFixture struct{ request *engine.QueryRequest }

func (fixture *queryRunnerFixture) Run(_ context.Context, request engine.QueryRequest) (engine.QuerySummary, error) {
	fixture.request = &request
	if err := os.WriteFile(request.OutputPath, []byte("query parquet fixture"), 0o600); err != nil {
		return engine.QuerySummary{}, err
	}
	evidence, err := storage.InspectFile(request.OutputPath)
	return engine.QuerySummary{DuckDBVersion: "fixture", Rows: 2, Evidence: evidence}, err
}

func TestDurableQueryWorkflowDownloadsRunsAndCommitsFencedOutput(t *testing.T) {
	inputBytes := []byte("verified analytics fixture")
	operationBytes, _ := json.Marshal(engine.QueryOperation{
		Version: 1, Kind: "rows", ScanSQL: "SELECT * FROM input_rows", ReduceSQL: "SELECT * FROM input_rows",
		EmptySQL: "SELECT 1 WHERE false", MaxRows: 101, Result: engine.QueryResultPlan{Kind: "rows", Limit: 100, Sort: "event_desc"},
	})
	authority := control.QueryTaskAuthority{
		InstallationID: "00000000-0000-4000-8000-000000000001", StorageGeneration: 1, TenantID: 17,
		QueryID: "00000000-0000-4000-8000-000000000002", Key: model.QueryTaskKey{Stage: model.QueryTaskScan},
		Owner: "worker", Fence: 3, Attempt: 1,
	}
	manifestBytes, _ := json.Marshal(query.TaskManifest{
		Version: 1, QueryID: authority.QueryID, TenantID: authority.TenantID, SnapshotID: "00000000-0000-4000-8000-000000000003",
		Generation: 1, OperationHash: digestBytes(operationBytes), Operation: operationBytes, DeadlineUS: 1,
		Task: authority.Key, Files: []model.CatalogFile{{FileID: "file", ObjectKey: "analytics", Bytes: int64(len(inputBytes)), SHA256: digestBytes(inputBytes)}},
	})
	task := control.QueryTask{Authority: authority, Manifest: manifestBytes}
	controlFixture := &queryControlFixture{}
	storeFixture := &workflowStoreFixture{journal: inputBytes, objects: map[string][]byte{}}
	runnerFixture := &queryRunnerFixture{}
	workflow := DurableQueryWorkflow{
		Control: controlFixture, Store: storeFixture, Runner: runnerFixture,
		InstallationID: authority.InstallationID, ScratchDir: filepath.Join(t.TempDir(), "query-worker"),
	}
	if err := workflow.Execute(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	if runnerFixture.request == nil || len(runnerFixture.request.InputPaths) != 1 || controlFixture.registration == nil || controlFixture.uploaded == nil || controlFixture.completed == nil {
		t.Fatalf("request=%v registration=%v upload=%v complete=%v", runnerFixture.request != nil, controlFixture.registration != nil, controlFixture.uploaded != nil, controlFixture.completed != nil)
	}
	if controlFixture.registration.Authority != authority || controlFixture.completed.Authority != authority || controlFixture.completed.Rows != 2 || len(storeFixture.objects) != 1 {
		t.Fatalf("registration=%#v completion=%#v objects=%d", controlFixture.registration, controlFixture.completed, len(storeFixture.objects))
	}
}
