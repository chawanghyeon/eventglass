package maintenance

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/resource"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

type CompactionControl interface {
	LoadCompaction(context.Context, control.MaintenanceAuthority) (control.CompactionWork, error)
	RegisterMaintenanceIntent(context.Context, control.MaintenanceIntentRegistration) error
	MarkMaintenanceIntentUploaded(context.Context, control.MaintenanceAuthority, control.IntentAuthority) error
	PrepareCompaction(context.Context, control.MaintenanceAuthority, model.BundleManifest) error
	SwapCompaction(context.Context, control.MaintenanceAuthority) (control.CompactionSwapResult, error)
}

type CompactionStore interface {
	DownloadToFile(context.Context, string, string, int64, string, int64) error
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
	Disk           *resource.Budget
}

func (workflow Workflow) CompactAndPrepare(ctx context.Context, task control.CompactionTask) (retErr error) {
	if workflow.Control == nil || workflow.Store == nil || workflow.Runner == nil || workflow.InstallationID == "" || workflow.ScratchDir == "" || task.Prepared || task.Authority.InstallationID != workflow.InstallationID {
		return errors.New("invalid compaction workflow")
	}
	if err := storage.EnsurePrivateDirectory(workflow.ScratchDir); err != nil {
		return err
	}
	directory, err := os.MkdirTemp(workflow.ScratchDir, "compact-")
	if err != nil {
		return err
	}
	var permit *resource.Permit
	defer func() {
		cleanupErr := os.RemoveAll(directory)
		if cleanupErr == nil && permit != nil {
			permit.Release()
		}
		retErr = errors.Join(retErr, cleanupErr)
	}()
	work, err := workflow.Control.LoadCompaction(ctx, task.Authority)
	if err != nil {
		return err
	}
	permit, err = reserveDisk(workflow.Disk, work)
	if err != nil {
		return err
	}
	inputs, err := downloadInputs(ctx, workflow.Store, directory, work)
	if err != nil {
		return err
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
	bundle, err := uploadBundle(ctx, workflow.Control, workflow.Store, workflow.InstallationID, task.Authority, task.Partition, result.Bundle)
	if err != nil {
		return err
	}
	return workflow.Control.PrepareCompaction(ctx, task.Authority, bundle)
}

func (workflow Workflow) Swap(ctx context.Context, task control.CompactionTask) (control.CompactionSwapResult, error) {
	if workflow.Control == nil || !task.Prepared {
		return control.CompactionSwapResult{}, errors.New("invalid prepared compaction")
	}
	return workflow.Control.SwapCompaction(ctx, task.Authority)
}
