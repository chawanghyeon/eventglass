package app

import (
	"context"
	"errors"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/google/uuid"
)

const (
	retentionTickInterval       = 60 * time.Second
	maintenanceScheduleInterval = time.Second
)

func (runtime *Runtime) runRetentionScheduler(ctx context.Context) error {
	alertDone := make(chan struct{})
	if runtime.alertEvaluator != nil {
		go func() { defer close(alertDone); runtime.runAlertScheduler(ctx) }()
	} else {
		close(alertDone)
	}
	defer func() { <-alertDone }()
	runRetention := func() {
		started := time.Now()
		_, _, err := runtime.queryControl.AdvanceRetentionFloor(ctx)
		runtime.stats.record(ctx, "retention_tick", started, err == nil, err)
		started = time.Now()
		progressed, cleanupErr := runtime.maintenance.RunRetentionCleanup(ctx)
		runtime.stats.record(ctx, "retention_cleanup", started, progressed, cleanupErr)
	}
	runSchedule := func() {
		started := time.Now()
		err := runtime.reserveOneCompaction(ctx)
		runtime.stats.record(ctx, "maintenance_schedule", started, err == nil, err)
	}
	runRetention()
	runSchedule()
	retentionTicker := time.NewTicker(retentionTickInterval)
	maintenanceTicker := time.NewTicker(maintenanceScheduleInterval)
	defer retentionTicker.Stop()
	defer maintenanceTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-retentionTicker.C:
			runRetention()
		case <-maintenanceTicker.C:
			runSchedule()
		}
	}
}

func (runtime *Runtime) reserveOneCompaction(ctx context.Context) error {
	if runtime.maintenance == nil {
		return nil
	}
	retention, retentionErr := runtime.maintenance.FindRetentionCandidate(ctx)
	if retentionErr == nil {
		_, err := runtime.maintenance.ReserveRetention(ctx, control.ReserveRetentionCommand{
			InstallationID: runtime.installation.InstallationID, StorageGeneration: runtime.installation.StorageGeneration,
			TaskID: uuid.NewString(), TenantID: retention.TenantID, LaneID: retention.LaneID, BundleID: retention.BundleID,
		})
		if err == nil || errors.Is(err, control.ErrMaintenanceBusy) {
			return nil
		}
		return err
	}
	if !errors.Is(retentionErr, control.ErrMaintenanceNoWork) {
		return retentionErr
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
		started := time.Now()
		err := runtime.alertEvaluator.EvaluateOnce(ctx)
		runtime.stats.record(ctx, "alert_evaluation", started, err == nil, err)
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
