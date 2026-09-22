package query

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/resource"
	"github.com/chawanghyeon/eventglass/internal/storage"
	"github.com/google/uuid"
)

type queryWorkerControl interface {
	RegisterQueryIntent(context.Context, control.QueryIntentRegistration) error
	MarkQueryIntentUploaded(context.Context, control.QueryTaskAuthority, control.IntentAuthority) error
	CompleteQueryTask(context.Context, control.CompleteQueryTaskCommand) error
}

type Workflow struct {
	Control        queryWorkerControl
	Store          WorkerStore
	Runner         QueryRunner
	InstallationID string
	ScratchDir     string
	Disk           *resource.Budget
	Cache          *storage.BlockCache
}

func (workflow Workflow) Execute(ctx context.Context, task control.QueryTask) (retErr error) {
	if workflow.Control == nil || workflow.Store == nil || workflow.Runner == nil || workflow.Disk == nil || workflow.InstallationID == "" || workflow.ScratchDir == "" || task.Authority.InstallationID != workflow.InstallationID {
		return errors.New("invalid durable query workflow")
	}
	if err := storage.EnsurePrivateDirectory(workflow.ScratchDir); err != nil {
		return err
	}
	taskDirectory, err := os.MkdirTemp(workflow.ScratchDir, "query-")
	if err != nil {
		return err
	}
	var permit *resource.Permit
	defer func() {
		cleanupErr := os.RemoveAll(taskDirectory)
		if cleanupErr == nil && permit != nil {
			permit.Release()
		}
		retErr = errors.Join(retErr, cleanupErr)
	}()
	var manifest TaskManifest
	if err := decodeTaskJSON(task.Manifest, &manifest); err != nil {
		return errors.Join(engine.ErrQueryExecutionInvalid, err)
	}
	if manifest.Version != QueryProtocolVersion || manifest.QueryID != task.Authority.QueryID || manifest.TenantID != task.Authority.TenantID || manifest.Generation != task.Authority.StorageGeneration || manifest.Task != task.Authority.Key {
		return errors.Join(engine.ErrQueryExecutionInvalid, errors.New("query task manifest scope mismatch"))
	}
	digest := sha256.Sum256(manifest.Operation)
	if hex.EncodeToString(digest[:]) != manifest.OperationHash {
		return errors.Join(engine.ErrQueryExecutionInvalid, errors.New("query task operation digest mismatch"))
	}
	var operation engine.QueryOperation
	if err := decodeTaskJSON(manifest.Operation, &operation); err != nil {
		return errors.Join(engine.ErrQueryExecutionInvalid, err)
	}
	reservation, err := queryDiskReservation(task, manifest, operation)
	if err != nil {
		return err
	}
	permit, err = workflow.Disk.Acquire(reservation)
	if err != nil {
		return err
	}
	// The runner joins its child before returning. Remove owned files before
	// releasing their shared disk reservation, including failure/cancellation.
	inputs, payloads, releaseInputs, err := workflow.prepareQueryInputs(ctx, task, manifest, operation, taskDirectory)
	if err != nil {
		return err
	}
	request := engine.QueryRequest{
		Version: engine.QueryExecutionProtocolVersion, QueryID: task.Authority.QueryID, Task: task.Authority.Key,
		Operation: operation, InputPaths: inputs, PayloadPaths: payloads, OutputPath: filepath.Join(taskDirectory, "result.parquet"),
		SpillDirectory: filepath.Join(taskDirectory, "spill"),
	}
	summary, err := workflow.Runner.Run(ctx, request)
	cacheBytes, releaseErr := releaseInputs()
	if err != nil {
		return errors.Join(err, releaseErr)
	}
	if releaseErr != nil {
		return releaseErr
	}
	intentUUID, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	intentID := intentUUID.String()
	objectKey := fmt.Sprintf("v1/query/%s/%s/%d/%d/attempt-%d-%s.parquet", task.Authority.QueryID,
		task.Authority.Key.Stage, task.Authority.Key.Level, task.Authority.Key.PartitionID, task.Authority.Attempt, intentID)
	intent := control.IntentAuthority{
		IntentID: intentID, Owner: task.Authority.Owner, Fence: task.Authority.Fence,
		Bytes: summary.Evidence.Bytes, SHA256: summary.Evidence.SHA256,
	}
	if err := workflow.Control.RegisterQueryIntent(ctx, control.QueryIntentRegistration{Authority: task.Authority, ObjectKey: objectKey, Intent: intent}); err != nil {
		return err
	}
	output, err := os.Open(request.OutputPath)
	if err != nil {
		return err
	}
	_, uploadErr := workflow.Store.PutStream(ctx, objectKey, output, intent.Bytes, intent.SHA256)
	closeErr := output.Close()
	if uploadErr != nil || closeErr != nil {
		return errors.Join(uploadErr, closeErr)
	}
	if err := workflow.Control.MarkQueryIntentUploaded(ctx, task.Authority, intent); err != nil {
		return err
	}
	return workflow.Control.CompleteQueryTask(ctx, control.CompleteQueryTaskCommand{
		Authority: task.Authority, IntentID: intent.IntentID, SHA256: intent.SHA256, BlockSHA256: summary.Evidence.BlockSHA256, Rows: summary.Rows, Bytes: intent.Bytes, CacheBytes: cacheBytes,
	})
}

func queryDiskReservation(task control.QueryTask, manifest TaskManifest, operation engine.QueryOperation) (int64, error) {
	bytes := engine.DefaultNativeSpillBytes + engine.MaxQueryOutputBytes
	if operation.Kind == "aggregate" {
		bytes += maxStagedAggregateBytes
	}
	if len(manifest.Files) > MaxFilesPerScan || operation.Kind == "detail" && len(manifest.Files) > MaxDetailFilesPerScan || len(task.Inputs) > ReduceFanIn {
		return 0, engine.ErrQueryExecutionInvalid
	}
	// Scan inputs are streamed through the bounded range cache. Cache bytes have
	// their own reservations in the same shared disk budget.
	for _, file := range manifest.Files {
		if file.Bytes <= 0 || file.Bytes > engine.MaxBundleFileBytes || operation.Kind == "detail" && (file.PayloadBytes <= 0 || file.PayloadBytes > engine.MaxBundleFileBytes) {
			return 0, engine.ErrQueryExecutionInvalid
		}
	}
	for _, input := range task.Inputs {
		if input.Bytes <= 0 || input.Bytes > engine.MaxQueryOutputBytes || len(input.BlockSHA256) != int((input.Bytes+storage.DefaultBlockSize-1)/storage.DefaultBlockSize) {
			return 0, engine.ErrQueryExecutionInvalid
		}
	}
	return bytes, nil
}

func (workflow Workflow) prepareQueryInputs(ctx context.Context, task control.QueryTask, manifest TaskManifest, operation engine.QueryOperation, taskDirectory string) ([]string, []string, func() (int64, error), error) {
	if workflow.Cache == nil {
		return nil, nil, nil, errors.Join(engine.ErrQueryExecutionInvalid, errors.New("query range cache is required"))
	}
	manifests := make([]storage.ObjectManifest, 0, len(manifest.Files)*2+len(task.Inputs))
	var inputs []string
	var payloads []string
	switch task.Authority.Key.Stage {
	case model.QueryTaskScan:
		if len(task.Inputs) != 0 || len(manifest.Files) < 1 || len(manifest.Files) > MaxFilesPerScan || operation.Kind == "detail" && len(manifest.Files) > MaxDetailFilesPerScan {
			return nil, nil, nil, errors.Join(engine.ErrQueryExecutionInvalid, errors.New("query scan inputs are invalid"))
		}
		inputs = make([]string, len(manifest.Files))
		if operation.Kind == "detail" {
			payloads = make([]string, len(manifest.Files))
		}
		for index, file := range manifest.Files {
			capability, err := uuid.NewRandom()
			if err != nil {
				return nil, nil, nil, err
			}
			manifests = append(manifests, storage.ObjectManifest{
				InstallationID: workflow.InstallationID, ObjectID: file.FileID, Capability: capability.String(),
				ObjectKey: file.ObjectKey, Size: file.Bytes, SHA256: file.SHA256,
				BlockSize: storage.DefaultBlockSize, BlockSHA256: file.BlockSHA256,
			})
			inputs[index] = capability.String()
			if operation.Kind == "detail" {
				if file.PayloadFileID == "" || file.PayloadObjectKey == "" || file.PayloadBytes <= 0 || len(file.PayloadBlockSHA256) == 0 {
					return nil, nil, nil, errors.Join(engine.ErrQueryExecutionInvalid, errors.New("query payload block manifest is invalid"))
				}
				payloadCapability, err := uuid.NewRandom()
				if err != nil {
					return nil, nil, nil, err
				}
				manifests = append(manifests, storage.ObjectManifest{
					InstallationID: workflow.InstallationID, ObjectID: file.PayloadFileID, Capability: payloadCapability.String(),
					ObjectKey: file.PayloadObjectKey, Size: file.PayloadBytes, SHA256: file.PayloadSHA256,
					BlockSize: storage.DefaultBlockSize, BlockSHA256: file.PayloadBlockSHA256,
				})
				payloads[index] = payloadCapability.String()
			}
		}
	case model.QueryTaskReduce:
		if len(manifest.Files) != 0 || len(task.Inputs) != len(manifest.Inputs) || len(task.Inputs) > ReduceFanIn {
			return nil, nil, nil, errors.Join(engine.ErrQueryExecutionInvalid, errors.New("query reducer inputs are invalid"))
		}
		inputs = make([]string, len(task.Inputs))
		for index, artifact := range task.Inputs {
			if artifact.Ordinal != index || manifest.Inputs[index].Producer != artifact.Producer || len(artifact.BlockSHA256) == 0 {
				return nil, nil, nil, errors.Join(engine.ErrQueryExecutionInvalid, errors.New("query reducer input order or blocks are invalid"))
			}
			capability, err := uuid.NewRandom()
			if err != nil {
				return nil, nil, nil, err
			}
			manifests = append(manifests, storage.ObjectManifest{
				InstallationID: workflow.InstallationID, ObjectID: artifact.IntentID, Capability: capability.String(),
				ObjectKey: artifact.ObjectKey, Size: artifact.Bytes, SHA256: artifact.SHA256,
				BlockSize: storage.DefaultBlockSize, BlockSHA256: artifact.BlockSHA256,
			})
			inputs[index] = capability.String()
		}
	default:
		return nil, nil, nil, errors.Join(engine.ErrQueryExecutionInvalid, errors.New("query task stage is invalid"))
	}
	if len(manifests) == 0 {
		return inputs, payloads, func() (int64, error) { return 0, nil }, nil
	}
	gateway, err := storage.NewGatewayWithCache(workflow.Store, workflow.Cache, manifests)
	if err != nil {
		return nil, nil, nil, err
	}
	releaseStaged := func() int64 { return 0 }
	if operation.Kind == "aggregate" {
		staged, release, err := workflow.stageAggregateInputs(ctx, taskDirectory, manifests)
		if err != nil {
			return nil, nil, nil, err
		}
		releaseStaged = release
		for index, capability := range inputs {
			if path := staged[capability]; path != "" {
				inputs[index] = path
			}
		}
		if len(staged) == len(inputs) {
			return inputs, payloads, func() (int64, error) { return releaseStaged(), nil }, nil
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		releaseStaged()
		return nil, nil, nil, err
	}
	server := &http.Server{Handler: gateway, ReadHeaderTimeout: 5 * time.Second}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	baseURL := "http://" + listener.Addr().String() + "/objects/"
	for index := range inputs {
		if !filepath.IsAbs(inputs[index]) {
			inputs[index] = baseURL + inputs[index]
		}
	}
	for index := range payloads {
		payloads[index] = baseURL + payloads[index]
	}
	return inputs, payloads, func() (int64, error) {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		shutdownErr := server.Shutdown(shutdownCtx)
		if shutdownErr != nil {
			shutdownErr = errors.Join(shutdownErr, server.Close())
		}
		serveErr := <-serveDone
		gateway.ReleasePins()
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		return gateway.CacheBytes() + releaseStaged(), errors.Join(shutdownErr, serveErr)
	}, nil
}

// WorkerStore is the verified I/O required by query execution, not a repository.
type WorkerStore interface {
	ReadRange(context.Context, string, int64, int64) ([]byte, error)
	PutStream(context.Context, string, io.ReadSeeker, int64, string) (storage.ObjectInfo, error)
}
type QueryRunner interface {
	Run(context.Context, engine.QueryRequest) (engine.QuerySummary, error)
}

func decodeTaskJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("trailing task JSON")
	}
	return nil
}
