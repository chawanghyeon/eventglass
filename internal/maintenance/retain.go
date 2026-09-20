package maintenance

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/resource"
	"github.com/chawanghyeon/eventglass/internal/storage"
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
	Disk           *resource.Budget
}

func (workflow RetentionWorkflow) Execute(ctx context.Context, task control.RetentionTask) (retErr error) {
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
	if err := storage.EnsurePrivateDirectory(workflow.ScratchDir); err != nil {
		return err
	}
	directory, err := os.MkdirTemp(workflow.ScratchDir, "retain-")
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
	permit, err = reserveDisk(workflow.Disk, work.CompactionWork)
	if err != nil {
		return err
	}
	inputs, err := downloadInputs(ctx, workflow.Store, directory, work.CompactionWork)
	if err != nil {
		return err
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
	bundle, err := uploadBundle(ctx, workflow.Control, workflow.Store, workflow.InstallationID, task.Authority, task.Partition, result.Bundle)
	if err != nil {
		return err
	}
	return workflow.Control.PrepareRetention(ctx, task, bundle)
}
