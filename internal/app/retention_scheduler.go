package app

import (
	"context"
	"errors"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/google/uuid"
)

const retentionTickInterval = 60 * time.Second

func (runtime *Runtime) runRetentionScheduler(ctx context.Context) error {
	alertDone := make(chan struct{})
	if runtime.alertEvaluator != nil {
		go func() { defer close(alertDone); runtime.runAlertScheduler(ctx) }()
	} else {
		close(alertDone)
	}
	defer func() { <-alertDone }()
	for {
		_, _, _ = runtime.queryControl.AdvanceRetentionFloor(ctx)
		_ = runtime.reserveOneCompaction(ctx)
		timer := time.NewTimer(retentionTickInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		}
	}
}

func (runtime *Runtime) reserveOneCompaction(ctx context.Context) error {
	if runtime.maintenance == nil {
		return nil
	}
	candidate, err := runtime.maintenance.FindCompactionCandidate(ctx)
	if errors.Is(err, control.ErrMaintenanceNoWork) {
		return nil
	}
	if err != nil {
		return err
	}
	_, err = runtime.maintenance.ReserveCompaction(ctx, control.ReserveCompactionCommand{
		InstallationID: runtime.installation.InstallationID, StorageGeneration: runtime.installation.StorageGeneration,
		TaskID: uuid.NewString(), TenantID: candidate.TenantID, LaneID: candidate.LaneID, BundleIDs: candidate.BundleIDs,
	})
	if errors.Is(err, control.ErrMaintenanceBusy) {
		return nil
	}
	return err
}

func (runtime *Runtime) runAlertScheduler(ctx context.Context) {
	for {
		_ = runtime.alertEvaluator.EvaluateOnce(ctx)
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}
