package query

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

type stagedBlockingRunner struct {
	started chan engine.QueryRequest
	finish  chan struct{}
}

func (runner stagedBlockingRunner) Run(ctx context.Context, request engine.QueryRequest) (engine.QuerySummary, error) {
	runner.started <- request
	<-runner.finish
	return engine.QuerySummary{}, ctx.Err()
}

func TestStagedAggregateCancellationKeepsFilesPermitAndPinUntilRunnerJoins(t *testing.T) {
	data := []byte("verified input")
	disk := resource.NewBudget(4 << 30)
	cache, err := storage.NewBlockCache(filepath.Join(t.TempDir(), "cache"), int64(len(data)), disk)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	runner := stagedBlockingRunner{started: make(chan engine.QueryRequest, 1), finish: make(chan struct{})}
	workflow := Workflow{Disk: disk, Cache: cache, Store: &workflowStoreFixture{journal: data},
		Control: &queryControlFixture{}, Runner: runner, InstallationID: "installation", ScratchDir: filepath.Join(t.TempDir(), "worker")}
	input := smallInputManifest(0, data)
	primeSmallInput(t, workflow, input)
	operation, _ := json.Marshal(engine.QueryOperation{Version: 1, Kind: "aggregate"})
	authority := control.QueryTaskAuthority{InstallationID: "installation", TenantID: 1, QueryID: "query", StorageGeneration: 1, Key: model.QueryTaskKey{Stage: model.QueryTaskScan}}
	manifest := TaskManifest{Version: 1, QueryID: authority.QueryID, TenantID: 1, Generation: 1, Task: authority.Key,
		Operation: operation, OperationHash: digestBytes(operation), Files: []model.CatalogFile{{FileID: input.ObjectID, ObjectKey: input.ObjectKey, Bytes: input.Size, SHA256: input.SHA256, BlockSHA256: input.BlockSHA256}}}
	encoded, _ := json.Marshal(manifest)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	joined := make(chan error, 1)
	go func() { joined <- workflow.Execute(ctx, control.QueryTask{Authority: authority, Manifest: encoded}) }()
	var request engine.QueryRequest
	select {
	case request = <-runner.started:
	case err := <-joined:
		t.Fatalf("workflow failed before runner started: %v", err)
	}
	cancel()
	if !filepath.IsAbs(request.InputPaths[0]) {
		t.Error("warm aggregate remained remote")
	}
	if got, err := os.ReadFile(request.InputPaths[0]); err != nil || !bytes.Equal(got, data) {
		t.Error("cancel removed input before runner joined")
	}
	if disk.Used() <= int64(len(data)) {
		t.Error("cancel released task disk before runner joined")
	}
	_, _, _, pinErr := cache.Read(context.Background(), storage.CacheBlockKey{InstallationID: "installation", ObjectID: "other", ContentSHA256: input.SHA256}, input.Size, input.SHA256, func(context.Context) ([]byte, error) { return data, nil })
	if !errors.Is(pinErr, resource.ErrLimited) {
		t.Errorf("cancel released input pin before join: %v", pinErr)
	}
	close(runner.finish)
	if err := <-joined; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := os.Stat(request.InputPaths[0]); !os.IsNotExist(err) || disk.Used() != int64(len(data)) {
		t.Fatalf("joined task leaked file/permit: file=%v disk=%d", err, disk.Used())
	}
}

func smallInputManifest(index int, data []byte) storage.ObjectManifest {
	id := fmt.Sprintf("object-%d", index)
	return storage.ObjectManifest{InstallationID: "installation", ObjectID: id, Capability: id, ObjectKey: id,
		Size: int64(len(data)), SHA256: digestBytes(data), BlockSize: storage.DefaultBlockSize, BlockSHA256: []string{digestBytes(data)}}
}

func smallInputWorkflow(t *testing.T, data []byte, cacheSize int64) Workflow {
	t.Helper()
	disk := resource.NewBudget(64 << 20)
	cache, err := storage.NewBlockCache(filepath.Join(t.TempDir(), "cache"), cacheSize, disk)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cache.Close)
	return Workflow{Cache: cache, Disk: disk, Store: &workflowStoreFixture{journal: data}}
}

func primeSmallInput(t *testing.T, workflow Workflow, manifest storage.ObjectManifest) {
	t.Helper()
	_, release, _, err := workflow.Cache.Read(context.Background(), storage.CacheBlockKey{
		InstallationID: manifest.InstallationID, ObjectID: manifest.ObjectID, ContentSHA256: manifest.SHA256,
	}, manifest.Size, manifest.SHA256, func(ctx context.Context) ([]byte, error) {
		return workflow.Store.ReadRange(ctx, manifest.ObjectKey, 0, manifest.Size)
	})
	if err != nil {
		t.Fatal(err)
	}
	release()
}

func TestSmallAggregateInputStagingUsesVerifiedPinnedCache(t *testing.T) {
	data := []byte("verified small parquet bytes")
	workflow := smallInputWorkflow(t, data, int64(len(data)))
	manifest := smallInputManifest(0, data)
	cold, releaseCold, err := workflow.stageAggregateInputs(context.Background(), t.TempDir(), []storage.ObjectManifest{manifest})
	if err != nil || len(cold) != 0 || workflow.Store.(*workflowStoreFixture).rangeRequests != 0 {
		t.Fatalf("cold inputs must stay lazy: paths=%d error=%v", len(cold), err)
	}
	releaseCold()
	primeSmallInput(t, workflow, manifest)
	for range 2 {
		root := t.TempDir()
		paths, release, err := workflow.stageAggregateInputs(context.Background(), root, []storage.ObjectManifest{manifest})
		if err != nil {
			t.Fatal(err)
		}
		stored, err := os.ReadFile(paths[manifest.Capability])
		info, statErr := os.Stat(paths[manifest.Capability])
		if err != nil || statErr != nil || !bytes.Equal(stored, data) || info.Mode().Perm() != 0o600 {
			t.Fatalf("private verified bytes: read=%v stat=%v", err, statErr)
		}
		other := storage.CacheBlockKey{InstallationID: "installation", ObjectID: "other", ContentSHA256: manifest.SHA256}
		if _, _, _, err := workflow.Cache.Read(context.Background(), other, manifest.Size, manifest.SHA256, func(context.Context) ([]byte, error) { return data, nil }); !errors.Is(err, resource.ErrLimited) {
			t.Fatalf("staged input lost pin before child joined: %v", err)
		}
		wantCached := int64(len(data))
		if got := release(); got != wantCached {
			t.Fatalf("cache bytes=%d want=%d", got, wantCached)
		}
		release() // Idempotent, including error cleanup.
	}
	if got := workflow.Store.(*workflowStoreFixture).rangeRequests; got != 1 {
		t.Fatalf("warm stage issued additional source reads: %d", got)
	}
}

func TestSmallAggregateInputStagingBoundsAndFallback(t *testing.T) {
	data := bytes.Repeat([]byte("x"), maxStagedAggregateFileBytes)
	workflow := smallInputWorkflow(t, data, 16<<20)
	manifests := make([]storage.ObjectManifest, MaxFilesPerScan)
	for index := range manifests {
		manifests[index] = smallInputManifest(index, data)
		primeSmallInput(t, workflow, manifests[index])
	}
	paths, release, err := workflow.stageAggregateInputs(context.Background(), t.TempDir(), manifests)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if len(paths) != maxStagedAggregateBytes/maxStagedAggregateFileBytes || workflow.Store.(*workflowStoreFixture).rangeRequests != len(manifests) {
		t.Fatalf("staged=%d reads=%d", len(paths), workflow.Store.(*workflowStoreFixture).rangeRequests)
	}
	oversize := smallInputManifest(0, bytes.Repeat([]byte("x"), maxStagedAggregateFileBytes+1))
	paths, releaseLarge, err := workflow.stageAggregateInputs(context.Background(), t.TempDir(), []storage.ObjectManifest{oversize})
	if err != nil || len(paths) != 0 {
		t.Fatalf("large file must remain lazy: paths=%d error=%v", len(paths), err)
	}
	releaseLarge()
	got, err := queryDiskReservation(control.QueryTask{}, TaskManifest{}, engine.QueryOperation{Kind: "aggregate"})
	if err != nil || got != engine.DefaultNativeSpillBytes+engine.MaxQueryOutputBytes+maxStagedAggregateBytes {
		t.Fatalf("staging not reserved: bytes=%d error=%v", got, err)
	}
}

func TestPreparedAggregateMixesWarmFileAndLazyGateway(t *testing.T) {
	for _, kind := range []string{"aggregate", "rows"} {
		t.Run(kind, func(t *testing.T) {
			data := []byte("verified")
			workflow := smallInputWorkflow(t, data, 1024)
			workflow.InstallationID = "installation"
			warm, cold := smallInputManifest(0, data), smallInputManifest(1, data)
			primeSmallInput(t, workflow, warm)
			manifest := TaskManifest{}
			for _, input := range []storage.ObjectManifest{warm, cold} {
				manifest.Files = append(manifest.Files, model.CatalogFile{FileID: input.ObjectID, ObjectKey: input.ObjectKey, Bytes: input.Size, SHA256: input.SHA256, BlockSHA256: input.BlockSHA256})
			}
			task := control.QueryTask{Authority: control.QueryTaskAuthority{Key: model.QueryTaskKey{Stage: model.QueryTaskScan}}}
			inputs, _, release, err := workflow.prepareQueryInputs(context.Background(), task, manifest, engine.QueryOperation{Kind: kind}, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			if filepath.IsAbs(inputs[0]) != (kind == "aggregate") || filepath.IsAbs(inputs[1]) {
				t.Fatalf("wrong local/lazy paths: %v", inputs)
			}
			if workflow.Store.(*workflowStoreFixture).rangeRequests != 1 {
				t.Fatal("preparation downloaded a cold file")
			}
			request, err := http.NewRequest(http.MethodGet, inputs[1], nil)
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Range", "bytes=0-")
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil || response.StatusCode != http.StatusPartialContent || !bytes.Equal(got, data) || workflow.Store.(*workflowStoreFixture).rangeRequests != 2 {
				t.Fatalf("cold lazy range: status=%d error=%v", response.StatusCode, err)
			}
		})
	}
}

func TestSmallAggregateInputStagingRejectsCorruptionAndCanceledWork(t *testing.T) {
	data := []byte("expected")
	for _, scenario := range []string{"inconsistent_hash", "canceled", "existing_file", "late_failure"} {
		t.Run(scenario, func(t *testing.T) {
			workflow := smallInputWorkflow(t, data, int64(len(data)))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			root := t.TempDir()
			manifest := smallInputManifest(0, data)
			primeSmallInput(t, workflow, manifest)
			manifests := []storage.ObjectManifest{manifest}
			switch scenario {
			case "inconsistent_hash":
				manifests[0].SHA256 = digestBytes([]byte("other"))
			case "canceled":
				cancel()
			case "existing_file":
				if err := os.WriteFile(filepath.Join(root, "input-0.parquet"), []byte("keep"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "late_failure":
				other := smallInputManifest(1, data)
				other.SHA256 = digestBytes([]byte("invalid"))
				manifests = append(manifests, other)
			}
			if _, _, err := workflow.stageAggregateInputs(ctx, root, manifests); err == nil {
				t.Fatal("invalid stage succeeded")
			}
			if scenario == "existing_file" {
				got, err := os.ReadFile(filepath.Join(root, "input-0.parquet"))
				if err != nil || string(got) != "keep" {
					t.Fatal("preexisting file overwritten")
				}
			}
			// Failure must release any earlier pin so the bounded cache can be
			// reused. The task directory remains owned by Execute until cleanup.
			_, release, _, err := workflow.Cache.Read(context.Background(), storage.CacheBlockKey{
				InstallationID: "installation", ObjectID: "retry", ContentSHA256: digestBytes(data),
			}, int64(len(data)), digestBytes(data), func(context.Context) ([]byte, error) { return data, nil })
			if err != nil {
				t.Fatal(err)
			}
			release()
		})
	}
}

func TestSmallAggregateInputStagingRejectsChangedCachedBytes(t *testing.T) {
	for _, changed := range [][]byte{[]byte("changed!"), []byte("short")} {
		data := []byte("expected")
		workflow := smallInputWorkflow(t, data, int64(len(data)))
		manifest := smallInputManifest(0, data)
		// Locate this isolated cache's single file through its configured test
		// directory rather than reaching into storage's private ownership state.
		root := t.TempDir()
		cache, err := storage.NewBlockCache(filepath.Join(root, "cache"), 1024, resource.NewBudget(1024))
		if err != nil {
			t.Fatal(err)
		}
		defer cache.Close()
		workflow.Cache = cache
		primeSmallInput(t, workflow, manifest)
		files, err := filepath.Glob(filepath.Join(root, "cache", "*.block"))
		if err != nil || len(files) != 1 {
			t.Fatal("missing cache fixture")
		}
		if err := os.WriteFile(files[0], changed, 0o600); err != nil {
			t.Fatal(err)
		}
		before := workflow.Store.(*workflowStoreFixture).rangeRequests
		paths, release, err := workflow.stageAggregateInputs(context.Background(), t.TempDir(), []storage.ObjectManifest{manifest})
		if err != nil || len(paths) != 0 || workflow.Store.(*workflowStoreFixture).rangeRequests != before {
			t.Fatalf("corruption must fall back to a verified lazy read: paths=%d error=%v", len(paths), err)
		}
		release()
	}
}
