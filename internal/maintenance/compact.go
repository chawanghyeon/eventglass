package maintenance

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/storage"
	"github.com/google/uuid"
)

type CompactionControl interface {
	LoadCompaction(context.Context, control.MaintenanceAuthority) (control.CompactionWork, error)
	RegisterMaintenanceIntent(context.Context, control.MaintenanceIntentRegistration) error
	MarkMaintenanceIntentUploaded(context.Context, control.MaintenanceAuthority, control.IntentAuthority) error
	PrepareCompaction(context.Context, control.MaintenanceAuthority, model.BundleManifest) error
	SwapCompaction(context.Context, control.MaintenanceAuthority) (control.CompactionSwapResult, error)
}

type CompactionStore interface {
	DownloadToFile(context.Context, string, string, int64, string) error
	PutStream(context.Context, string, io.ReadSeeker, int64, string) (storage.ObjectInfo, error)
}

type CompactionRunner interface {
	Run(context.Context, engine.CompactionRequest) (engine.CompactionResult, error)
}

type Workflow struct {
	Control        CompactionControl
	Store          CompactionStore
	Runner         CompactionRunner
	InstallationID string
	ScratchDir     string
}

func (workflow Workflow) CompactAndPrepare(ctx context.Context, task control.CompactionTask) error {
	if workflow.Control == nil || workflow.Store == nil || workflow.Runner == nil || workflow.InstallationID == "" || workflow.ScratchDir == "" || task.Prepared || task.Authority.InstallationID != workflow.InstallationID {
		return errors.New("invalid compaction workflow")
	}
	if err := os.MkdirAll(workflow.ScratchDir, 0o700); err != nil {
		return err
	}
	directory, err := os.MkdirTemp(workflow.ScratchDir, "compact-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(directory)
	work, err := workflow.Control.LoadCompaction(ctx, task.Authority)
	if err != nil {
		return err
	}
	inputs := make([]engine.CompactionInput, len(work.Inputs))
	for index, input := range work.Inputs {
		inputDir := filepath.Join(directory, fmt.Sprintf("input-%03d", index))
		if err := os.Mkdir(inputDir, 0o700); err != nil {
			return err
		}
		analytics := filepath.Join(inputDir, "analytics.parquet")
		payload := filepath.Join(inputDir, "payload.parquet")
		if err := workflow.Store.DownloadToFile(ctx, input.Analytics.ObjectKey, analytics, input.Analytics.Bytes, input.Analytics.SHA256); err != nil {
			return err
		}
		if err := workflow.Store.DownloadToFile(ctx, input.Payload.ObjectKey, payload, input.Payload.Bytes, input.Payload.SHA256); err != nil {
			return err
		}
		inputs[index] = engine.CompactionInput{BundleID: input.BundleID, AnalyticsPath: analytics, PayloadPath: payload, IdentitySHA256: input.IdentitySHA256}
	}
	outputDir, spillDir := filepath.Join(directory, "output"), filepath.Join(directory, "spill")
	request := engine.CompactionRequest{
		Version: engine.CompactionProtocolVersion, TenantID: task.Authority.TenantID, LaneID: task.Authority.LaneID,
		SchemaVersion: task.Partition.SchemaVersion, GroupingVersion: task.Partition.GroupingVersion,
		EventDay: task.Partition.EventDay, Kind: task.Partition.Kind, Inputs: inputs,
		OutputDirectory: outputDir, SpillDirectory: spillDir,
		NativeMemoryBytes: engine.DefaultNativeMemoryBytes, NativeSpillBytes: engine.DefaultNativeSpillBytes,
	}
	result, err := workflow.Runner.Run(ctx, request)
	if err != nil {
		return err
	}
	converted := result.Bundle
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
	analytics, err := upload("analytics", converted.Analytics)
	if err != nil {
		return err
	}
	payload, err := upload("payload", converted.Payload)
	if err != nil {
		return err
	}
	bundle := model.BundleManifest{BundleID: bundleID, EventDay: task.Partition.EventDay, Kind: task.Partition.Kind, InputSeqMin: analytics.MinBatchSeq, InputSeqMax: analytics.MaxBatchSeq, RowCount: converted.RowCount, IdentitySHA256: converted.IdentitySHA256, ProjectIDs: converted.ProjectIDs, Analytics: analytics, Payload: payload}
	return workflow.Control.PrepareCompaction(ctx, task.Authority, bundle)
}

func (workflow Workflow) Swap(ctx context.Context, task control.CompactionTask) (control.CompactionSwapResult, error) {
	if workflow.Control == nil || !task.Prepared {
		return control.CompactionSwapResult{}, errors.New("invalid prepared compaction")
	}
	return workflow.Control.SwapCompaction(ctx, task.Authority)
}
