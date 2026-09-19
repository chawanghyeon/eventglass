package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
)

type queryWorkerControl interface {
	RegisterQueryIntent(context.Context, control.QueryIntentRegistration) error
	MarkQueryIntentUploaded(context.Context, control.QueryTaskAuthority, control.IntentAuthority) error
	CompleteQueryTask(context.Context, control.CompleteQueryTaskCommand) error
}

type DurableQueryWorkflow struct {
	Control        queryWorkerControl
	Store          publicationStore
	Runner         QueryRunner
	InstallationID string
	ScratchDir     string
}

func (workflow DurableQueryWorkflow) Execute(ctx context.Context, task control.QueryTask) error {
	if workflow.Control == nil || workflow.Store == nil || workflow.Runner == nil || workflow.InstallationID == "" || workflow.ScratchDir == "" || task.Authority.InstallationID != workflow.InstallationID {
		return errors.New("invalid durable query workflow")
	}
	if err := ensurePrivateDirectory(workflow.ScratchDir); err != nil {
		return err
	}
	taskDirectory, err := os.MkdirTemp(workflow.ScratchDir, "query-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(taskDirectory)
	var manifest query.TaskManifest
	if err := strictAppJSON(task.Manifest, &manifest); err != nil {
		return errors.Join(engine.ErrQueryExecutionInvalid, err)
	}
	if manifest.Version != query.QueryProtocolVersion || manifest.QueryID != task.Authority.QueryID || manifest.TenantID != task.Authority.TenantID || manifest.Generation != task.Authority.StorageGeneration || manifest.Task != task.Authority.Key {
		return engine.ErrQueryExecutionInvalid
	}
	digest := sha256.Sum256(manifest.Operation)
	if hex.EncodeToString(digest[:]) != manifest.OperationHash {
		return engine.ErrQueryExecutionInvalid
	}
	var operation engine.QueryOperation
	if err := strictAppJSON(manifest.Operation, &operation); err != nil {
		return errors.Join(engine.ErrQueryExecutionInvalid, err)
	}
	inputs, err := workflow.downloadQueryInputs(ctx, taskDirectory, task, manifest)
	if err != nil {
		return err
	}
	request := engine.QueryRequest{
		Version: engine.QueryExecutionProtocolVersion, QueryID: task.Authority.QueryID, Task: task.Authority.Key,
		Operation: operation, InputPaths: inputs, OutputPath: filepath.Join(taskDirectory, "result.parquet"),
		SpillDirectory: filepath.Join(taskDirectory, "spill"),
	}
	summary, err := workflow.Runner.Run(ctx, request)
	if err != nil {
		return err
	}
	intentID, err := randomUUID()
	if err != nil {
		return err
	}
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

func (workflow DurableQueryWorkflow) downloadQueryInputs(ctx context.Context, directory string, task control.QueryTask, manifest query.TaskManifest) ([]string, error) {
	type input struct {
		key      string
		bytes    int64
		checksum string
	}
	var selected []input
	switch task.Authority.Key.Stage {
	case model.QueryTaskScan:
		if len(task.Inputs) != 0 || len(manifest.Files) < 1 || len(manifest.Files) > query.MaxFilesPerScan {
			return nil, engine.ErrQueryExecutionInvalid
		}
		for _, file := range manifest.Files {
			selected = append(selected, input{file.ObjectKey, file.Bytes, file.SHA256})
		}
	case model.QueryTaskReduce:
		if len(manifest.Files) != 0 || len(task.Inputs) != len(manifest.Inputs) || len(task.Inputs) > query.ReduceFanIn {
			return nil, engine.ErrQueryExecutionInvalid
		}
		for index, artifact := range task.Inputs {
			if artifact.Ordinal != index || manifest.Inputs[index].Producer != artifact.Producer {
				return nil, engine.ErrQueryExecutionInvalid
			}
			selected = append(selected, input{artifact.ObjectKey, artifact.Bytes, artifact.SHA256})
		}
	default:
		return nil, engine.ErrQueryExecutionInvalid
	}
	paths := make([]string, len(selected))
	for index, item := range selected {
		path := filepath.Join(directory, fmt.Sprintf("input-%03d.parquet", index))
		if err := workflow.Store.DownloadToFile(ctx, item.key, path, item.bytes, item.checksum); err != nil {
			return nil, err
		}
		paths[index] = path
	}
	return paths, nil
}
