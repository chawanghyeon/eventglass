package query

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/resource"
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

type queryRunnerFixture struct {
	request *engine.QueryRequest
	input   []byte
}

func (fixture *queryRunnerFixture) Run(ctx context.Context, request engine.QueryRequest) (engine.QuerySummary, error) {
	fixture.request = &request
	readRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, request.InputPaths[0], nil)
	if err != nil {
		return engine.QuerySummary{}, err
	}
	readRequest.Header.Set("Range", "bytes=0-")
	response, err := http.DefaultClient.Do(readRequest)
	if err != nil {
		return engine.QuerySummary{}, err
	}
	fixture.input, err = io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if err != nil || closeErr != nil || response.StatusCode != http.StatusPartialContent {
		return engine.QuerySummary{}, errors.Join(err, closeErr)
	}
	if err := os.WriteFile(request.OutputPath, []byte("query parquet fixture"), 0o600); err != nil {
		return engine.QuerySummary{}, err
	}
	evidence, err := storage.InspectFile(request.OutputPath)
	return engine.QuerySummary{DuckDBVersion: "fixture", Rows: 2, Evidence: evidence}, err
}

func TestDurableQueryWorkflowUsesRangeCacheAndCommitsFencedOutput(t *testing.T) {
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
	manifest := TaskManifest{
		Version: 1, QueryID: authority.QueryID, TenantID: authority.TenantID, SnapshotID: "00000000-0000-4000-8000-000000000003",
		Generation: 1, OperationHash: digestBytes(operationBytes), Operation: operationBytes, DeadlineUS: 1,
		Task: authority.Key, Files: []model.CatalogFile{{FileID: "file", ObjectKey: "analytics", Bytes: int64(len(inputBytes)), SHA256: digestBytes(inputBytes), BlockSHA256: []string{digestBytes(inputBytes)}}},
	}
	manifestBytes, _ := json.Marshal(manifest)
	task := control.QueryTask{Authority: authority, Manifest: manifestBytes}
	controlFixture := &queryControlFixture{}
	storeFixture := &workflowStoreFixture{journal: inputBytes, objects: map[string][]byte{}}
	runnerFixture := &queryRunnerFixture{}
	disk := resource.NewBudget(4 << 30)
	cache, err := storage.NewBlockCache(filepath.Join(t.TempDir(), "cache"), 1<<20, disk)
	if err != nil {
		t.Fatal(err)
	}
	workflow := Workflow{Disk: disk, Cache: cache,
		Control: controlFixture, Store: storeFixture, Runner: runnerFixture,
		InstallationID: authority.InstallationID, ScratchDir: filepath.Join(t.TempDir(), "query-worker"),
	}
	if err := workflow.Execute(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	if runnerFixture.request == nil || len(runnerFixture.request.InputPaths) != 1 || string(runnerFixture.input) != string(inputBytes) || controlFixture.registration == nil || controlFixture.uploaded == nil || controlFixture.completed == nil {
		t.Fatalf("request=%v registration=%v upload=%v complete=%v", runnerFixture.request != nil, controlFixture.registration != nil, controlFixture.uploaded != nil, controlFixture.completed != nil)
	}
	if controlFixture.registration.Authority != authority || controlFixture.completed.Authority != authority || controlFixture.completed.Rows != 2 || len(storeFixture.objects) != 1 {
		t.Fatalf("registration=%#v completion=%#v objects=%d", controlFixture.registration, controlFixture.completed, len(storeFixture.objects))
	}
	if storeFixture.rangeRequests != 1 || controlFixture.completed.CacheBytes != 0 {
		t.Fatalf("cold range requests=%d cache bytes=%d", storeFixture.rangeRequests, controlFixture.completed.CacheBytes)
	}
	warmAuthority := authority
	warmAuthority.QueryID = "00000000-0000-4000-8000-000000000004"
	manifest.QueryID = warmAuthority.QueryID
	warmManifest, _ := json.Marshal(manifest)
	if err := workflow.Execute(context.Background(), control.QueryTask{Authority: warmAuthority, Manifest: warmManifest}); err != nil {
		t.Fatal(err)
	}
	if storeFixture.rangeRequests != 1 || controlFixture.completed.CacheBytes != int64(len(inputBytes)) {
		t.Fatalf("warm range requests=%d cache bytes=%d", storeFixture.rangeRequests, controlFixture.completed.CacheBytes)
	}
}

func TestEmptyReducerDoesNotStartCapabilityGateway(t *testing.T) {
	disk := resource.NewBudget(1 << 20)
	cache, err := storage.NewBlockCache(filepath.Join(t.TempDir(), "cache"), 1<<19, disk)
	if err != nil {
		t.Fatal(err)
	}
	workflow := Workflow{Disk: disk, Cache: cache, InstallationID: "installation"}
	task := control.QueryTask{Authority: control.QueryTaskAuthority{Key: model.QueryTaskKey{Stage: model.QueryTaskReduce}}}
	inputs, payloads, release, err := workflow.prepareQueryInputs(context.Background(), task, TaskManifest{}, engine.QueryOperation{Kind: "rows"}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(inputs) != 0 || len(payloads) != 0 || release == nil {
		t.Fatalf("inputs=%v payloads=%v release=%v", inputs, payloads, release != nil)
	}
	if cacheBytes, err := release(); err != nil || cacheBytes != 0 {
		t.Fatalf("cache bytes=%d err=%v", cacheBytes, err)
	}
}

type workflowStoreFixture struct {
	journal       []byte
	objects       map[string][]byte
	rangeRequests int
}

func (fixture *workflowStoreFixture) ReadRange(_ context.Context, _ string, offset, length int64) ([]byte, error) {
	fixture.rangeRequests++
	if offset < 0 || length < 0 || offset+length > int64(len(fixture.journal)) {
		return nil, io.ErrUnexpectedEOF
	}
	return append([]byte(nil), fixture.journal[offset:offset+length]...), nil
}

func (fixture *workflowStoreFixture) DownloadToFile(_ context.Context, _ string, path string, size int64, checksum string, maxBytes int64) error {
	if size > maxBytes || int64(len(fixture.journal)) != size || digestBytes(fixture.journal) != checksum {
		return io.ErrUnexpectedEOF
	}
	return os.WriteFile(path, fixture.journal, 0o600)
}
func (fixture *workflowStoreFixture) PutStream(_ context.Context, key string, body io.ReadSeeker, size int64, checksum string) (storage.ObjectInfo, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return storage.ObjectInfo{}, err
	}
	if int64(len(data)) != size || digestBytes(data) != checksum {
		return storage.ObjectInfo{}, io.ErrUnexpectedEOF
	}
	fixture.objects[key] = data
	return storage.ObjectInfo{Key: key, Size: size, SHA256: checksum}, nil
}
func (fixture *workflowStoreFixture) VerifyObject(_ context.Context, key string, size int64, checksum string) error {
	data, ok := fixture.objects[key]
	if !ok || int64(len(data)) != size || digestBytes(data) != checksum {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func digestBytes(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}
