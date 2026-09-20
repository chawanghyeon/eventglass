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
	"os"
	"path/filepath"

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
	inputs, payloads, err := workflow.downloadQueryInputs(ctx, taskDirectory, task, manifest, operation)
	if err != nil {
		return err
	}
	request := engine.QueryRequest{
		Version: engine.QueryExecutionProtocolVersion, QueryID: task.Authority.QueryID, Task: task.Authority.Key,
		Operation: operation, InputPaths: inputs, PayloadPaths: payloads, OutputPath: filepath.Join(taskDirectory, "result.parquet"),
		SpillDirectory: filepath.Join(taskDirectory, "spill"),
	}
	summary, err := workflow.Runner.Run(ctx, request)
	if err != nil {
		return err
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
		Authority: task.Authority, IntentID: intent.IntentID, SHA256: intent.SHA256, Rows: summary.Rows, Bytes: intent.Bytes,
	})
}

func queryDiskReservation(task control.QueryTask, manifest TaskManifest, operation engine.QueryOperation) (int64, error) {
	bytes := engine.DefaultNativeSpillBytes + engine.MaxQueryOutputBytes
	add := func(size int64) error {
		if size <= 0 || size > engine.MaxBundleFileBytes {
			return engine.ErrQueryExecutionInvalid
		}
		bytes += size
		return nil
	}
	if len(manifest.Files) > MaxFilesPerScan || len(task.Inputs) > ReduceFanIn {
		return 0, engine.ErrQueryExecutionInvalid
	}
	for _, file := range manifest.Files {
		if err := add(file.Bytes); err != nil {
			return 0, err
		}
		if operation.Kind == "detail" {
			if err := add(file.PayloadBytes); err != nil {
				return 0, err
			}
		}
	}
	for _, input := range task.Inputs {
		if err := add(input.Bytes); err != nil {
			return 0, err
		}
	}
	return bytes, nil
}

func (workflow Workflow) downloadQueryInputs(ctx context.Context, directory string, task control.QueryTask, manifest TaskManifest, operation engine.QueryOperation) ([]string, []string, error) {
	type input struct {
		key      string
		bytes    int64
		checksum string
	}
	var selected []input
	switch task.Authority.Key.Stage {
	case model.QueryTaskScan:
		if len(task.Inputs) != 0 || len(manifest.Files) < 1 || len(manifest.Files) > MaxFilesPerScan {
			return nil, nil, errors.Join(engine.ErrQueryExecutionInvalid, errors.New("query scan inputs are invalid"))
		}
		for _, file := range manifest.Files {
			selected = append(selected, input{file.ObjectKey, file.Bytes, file.SHA256})
		}
	case model.QueryTaskReduce:
		if len(manifest.Files) != 0 || len(task.Inputs) != len(manifest.Inputs) || len(task.Inputs) > ReduceFanIn {
			return nil, nil, errors.Join(engine.ErrQueryExecutionInvalid, errors.New("query reducer inputs are invalid"))
		}
		for index, artifact := range task.Inputs {
			if artifact.Ordinal != index || manifest.Inputs[index].Producer != artifact.Producer {
				return nil, nil, errors.Join(engine.ErrQueryExecutionInvalid, errors.New("query reducer input order is invalid"))
			}
			selected = append(selected, input{artifact.ObjectKey, artifact.Bytes, artifact.SHA256})
		}
	default:
		return nil, nil, errors.Join(engine.ErrQueryExecutionInvalid, errors.New("query task stage is invalid"))
	}
	paths := make([]string, len(selected))
	for index, item := range selected {
		path := filepath.Join(directory, fmt.Sprintf("input-%03d.parquet", index))
		if err := workflow.Store.DownloadToFile(ctx, item.key, path, item.bytes, item.checksum); err != nil {
			return nil, nil, err
		}
		paths[index] = path
	}
	var payloads []string
	if task.Authority.Key.Stage == model.QueryTaskScan && operation.Kind == "detail" {
		payloads = make([]string, len(manifest.Files))
		for index, file := range manifest.Files {
			if file.PayloadObjectKey == "" || file.PayloadBytes <= 0 || len(file.PayloadSHA256) != 64 {
				return nil, nil, errors.Join(engine.ErrQueryExecutionInvalid, errors.New("query payload manifest is invalid"))
			}
			path := filepath.Join(directory, fmt.Sprintf("payload-%03d.parquet", index))
			if err := workflow.Store.DownloadToFile(ctx, file.PayloadObjectKey, path, file.PayloadBytes, file.PayloadSHA256); err != nil {
				return nil, nil, err
			}
			payloads[index] = path
		}
	}
	return paths, payloads, nil
}

// WorkerStore is the verified I/O required by query execution, not a repository.
type WorkerStore interface {
	DownloadToFile(context.Context, string, string, int64, string) error
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
