package maintenance

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/google/uuid"
)

type RetentionControl interface {
	LoadRetention(context.Context, control.RetentionTask) (control.RetentionWork, error)
	RegisterMaintenanceIntent(context.Context, control.MaintenanceIntentRegistration) error
	MarkMaintenanceIntentUploaded(context.Context, control.MaintenanceAuthority, control.IntentAuthority) error
	PrepareRetention(context.Context, control.RetentionTask, model.BundleManifest) error
	SwapRetention(context.Context, control.RetentionTask) (control.CompactionSwapResult, error)
}

type RetentionWorkflow struct {
	Control        RetentionControl
	Store          CompactionStore
	Runner         CompactionRunner
	InstallationID string
	ScratchDir     string
}

func (workflow RetentionWorkflow) Execute(ctx context.Context, task control.RetentionTask) error {
	if workflow.Control == nil || workflow.Store == nil || workflow.Runner == nil || workflow.InstallationID == "" || workflow.ScratchDir == "" || task.Authority.InstallationID != workflow.InstallationID {
		return errors.New("invalid retention workflow")
	}
	if task.Prepared {
		_, err := workflow.Control.SwapRetention(ctx, task)
		return err
	}
	work, err := workflow.Control.LoadRetention(ctx, task)
	if err != nil {
		return err
	}
	if work.FullyExpired {
		_, err := workflow.Control.SwapRetention(ctx, task)
		return err
	}
	if err := os.MkdirAll(workflow.ScratchDir, 0o700); err != nil {
		return err
	}
	directory, err := os.MkdirTemp(workflow.ScratchDir, "retain-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(directory)
	inputs := make([]engine.CompactionInput, len(work.Inputs))
	for index, input := range work.Inputs {
		inputDir := filepath.Join(directory, fmt.Sprintf("input-%03d", index))
		if err := os.Mkdir(inputDir, 0o700); err != nil {
			return err
		}
		analytics, payload := filepath.Join(inputDir, "analytics.parquet"), filepath.Join(inputDir, "payload.parquet")
		if err := workflow.Store.DownloadToFile(ctx, input.Analytics.ObjectKey, analytics, input.Analytics.Bytes, input.Analytics.SHA256); err != nil {
			return err
		}
		if err := workflow.Store.DownloadToFile(ctx, input.Payload.ObjectKey, payload, input.Payload.Bytes, input.Payload.SHA256); err != nil {
			return err
		}
		inputs[index] = engine.CompactionInput{BundleID: input.BundleID, AnalyticsPath: analytics, PayloadPath: payload, IdentitySHA256: input.IdentitySHA256}
	}
	result, err := workflow.Runner.Run(ctx, engine.CompactionRequest{
		Version: engine.CompactionProtocolVersion, TenantID: task.Authority.TenantID, LaneID: task.Authority.LaneID,
		SchemaVersion: task.Partition.SchemaVersion, GroupingVersion: task.Partition.GroupingVersion,
		EventDay: task.Partition.EventDay, Kind: task.Partition.Kind, MinReceivedTimeUS: task.RetentionFloor, Inputs: inputs,
		OutputDirectory: filepath.Join(directory, "output"), SpillDirectory: filepath.Join(directory, "spill"),
		NativeMemoryBytes: engine.DefaultNativeMemoryBytes, NativeSpillBytes: engine.DefaultNativeSpillBytes,
	})
	if err != nil {
		return err
	}
	bundleID := uuid.NewString()
	upload := func(role string, file engine.ConvertedFile) (model.FileManifest, error) {
		fileID, intentID := uuid.NewString(), uuid.NewString()
		key := fmt.Sprintf("v1/%s/maintenance/%d/%d/%s/%d/%s/%s.parquet", workflow.InstallationID, task.Authority.TenantID, task.Authority.LaneID, task.Authority.TaskID, task.Authority.Fence, bundleID, role)
		authority := control.IntentAuthority{IntentID: intentID, Owner: task.Authority.Owner, Fence: task.Authority.Fence, Bytes: file.Evidence.Bytes, SHA256: file.Evidence.SHA256}
		if err := workflow.Control.RegisterMaintenanceIntent(ctx, control.MaintenanceIntentRegistration{Authority: task.Authority, Role: role, ObjectKey: key, Intent: authority}); err != nil {
			return model.FileManifest{}, err
		}
		reader, err := os.Open(file.Path)
		if err != nil {
			return model.FileManifest{}, err
		}
		_, uploadErr := workflow.Store.PutStream(ctx, key, reader, authority.Bytes, authority.SHA256)
		closeErr := reader.Close()
		if uploadErr != nil || closeErr != nil {
			return model.FileManifest{}, errors.Join(uploadErr, closeErr)
		}
		if err := workflow.Control.MarkMaintenanceIntentUploaded(ctx, task.Authority, authority); err != nil {
			return model.FileManifest{}, err
		}
		blocks := make([]model.FileBlockManifest, len(file.Evidence.BlockSHA256))
		for index, checksum := range file.Evidence.BlockSHA256 {
			blocks[index] = model.FileBlockManifest{Index: index, SHA256: checksum}
		}
		return model.FileManifest{FileID: fileID, IntentID: intentID, Role: role, Bytes: file.Evidence.Bytes, SHA256: file.Evidence.SHA256, RowCount: file.RowCount, MinEventTimeUS: file.MinEventTimeUS, MaxEventTimeUS: file.MaxEventTimeUS, MinReceivedTimeUS: file.MinReceivedTimeUS, MaxReceivedTimeUS: file.MaxReceivedTimeUS, MinBatchSeq: file.MinBatchSeq, MaxBatchSeq: file.MaxBatchSeq, Blocks: blocks}, nil
	}
	analytics, err := upload("analytics", result.Bundle.Analytics)
	if err != nil {
		return err
	}
	payload, err := upload("payload", result.Bundle.Payload)
	if err != nil {
		return err
	}
	manifest := model.BundleManifest{BundleID: bundleID, EventDay: task.Partition.EventDay, Kind: task.Partition.Kind, InputSeqMin: analytics.MinBatchSeq, InputSeqMax: analytics.MaxBatchSeq, RowCount: result.Bundle.RowCount, IdentitySHA256: result.Bundle.IdentitySHA256, ProjectIDs: result.Bundle.ProjectIDs, Analytics: analytics, Payload: payload}
	return workflow.Control.PrepareRetention(ctx, task, manifest)
}
