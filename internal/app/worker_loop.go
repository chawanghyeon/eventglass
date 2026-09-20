package app

import (
	"context"
	"errors"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/query"
)

const (
	workerLease     = 30 * time.Second
	workerHeartbeat = 10 * time.Second
	workerIdle      = 100 * time.Millisecond
)

func (runtime *Runtime) runWorker(ctx context.Context) error {
	work := []func(context.Context) (bool, error){runtime.runOnePublication, runtime.runOneDelivery, runtime.runOneConversion, runtime.runOneQueryCoordinator, runtime.runOneQuery, runtime.runOneRetention, runtime.runOneCompaction, runtime.runOneGC}
	next := 0
	for ctx.Err() == nil {
		var progressed bool
		var err error
		for offset := range work {
			index := (next + offset) % len(work)
			progressed, err = work[index](ctx)
			if err != nil || progressed {
				next = (index + 1) % len(work)
				break
			}
		}
		if ctx.Err() != nil {
			break
		}
		delay := workerIdle
		if err != nil {
			delay = 250 * time.Millisecond
		}
		if progressed && err == nil {
			continue
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
		}
	}
	return nil
}

func (runtime *Runtime) runOneRetention(ctx context.Context) (bool, error) {
	if runtime.maintenance == nil || runtime.retainer == nil {
		return false, nil
	}
	task, err := runtime.maintenance.ClaimRetention(ctx, runtime.installation.InstallationID, runtime.workerOwner, workerLease)
	if err != nil || task == nil {
		return false, err
	}
	err = runtime.withCompactionHeartbeat(ctx, task.Authority, func(taskContext context.Context) error { return runtime.retainer.Execute(taskContext, *task) })
	if err != nil && ctx.Err() == nil {
		failErr := runtime.maintenance.FailCompaction(ctx, task.Authority, "retention_worker_failed", true)
		return true, errors.Join(err, failErr)
	}
	return true, err
}

func (runtime *Runtime) runOneGC(ctx context.Context) (bool, error) {
	if runtime.collector == nil {
		return false, nil
	}
	return runtime.collector.RunOnce(ctx)
}

func (runtime *Runtime) runOneCompaction(ctx context.Context) (bool, error) {
	if runtime.maintenance == nil || runtime.compactor == nil {
		return false, nil
	}
	task, err := runtime.maintenance.ClaimCompaction(ctx, runtime.installation.InstallationID, runtime.workerOwner, workerLease)
	if err != nil || task == nil {
		return false, err
	}
	err = runtime.withCompactionHeartbeat(ctx, task.Authority, func(taskContext context.Context) error {
		if task.Prepared {
			_, swapErr := runtime.compactor.Swap(taskContext, *task)
			return swapErr
		}
		return runtime.compactor.CompactAndPrepare(taskContext, *task)
	})
	if err != nil && ctx.Err() == nil {
		failErr := runtime.maintenance.FailCompaction(ctx, task.Authority, "compaction_worker_failed", true)
		return true, errors.Join(err, failErr)
	}
	return true, err
}

func (runtime *Runtime) withCompactionHeartbeat(ctx context.Context, authority control.MaintenanceAuthority, task func(context.Context) error) error {
	taskContext, cancel := context.WithCancel(ctx)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- task(taskContext) }()
	ticker := time.NewTicker(workerHeartbeat)
	defer ticker.Stop()
	for {
		select {
		case err := <-result:
			return err
		case <-ctx.Done():
			cancel()
			return errors.Join(ctx.Err(), <-result)
		case <-ticker.C:
			if err := runtime.maintenance.HeartbeatCompaction(ctx, authority, workerLease); err != nil {
				cancel()
				return errors.Join(err, <-result)
			}
		}
	}
}

func (runtime *Runtime) runOneDelivery(ctx context.Context) (bool, error) {
	if runtime.deliveryWorker == nil {
		return false, nil
	}
	return runtime.deliveryWorker.RunOnce(ctx)
}

func (runtime *Runtime) runOneQueryCoordinator(ctx context.Context) (bool, error) {
	if runtime.queryControl == nil || runtime.queryPlanner == nil {
		return false, nil
	}
	job, err := runtime.queryControl.ClaimQueryCoordinator(ctx, runtime.installation.InstallationID, runtime.installation.StorageGeneration, runtime.workerOwner)
	if err != nil || job == nil {
		return false, err
	}
	err = runtime.withQueryCoordinatorHeartbeat(ctx, job.Authority, func(planContext context.Context) error {
		return runtime.queryPlanner.Execute(planContext, job.Authority)
	})
	return true, err
}

func (runtime *Runtime) withQueryCoordinatorHeartbeat(ctx context.Context, authority control.QueryCoordinatorAuthority, plan func(context.Context) error) error {
	planContext, cancel := context.WithCancel(ctx)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- plan(planContext) }()
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case err := <-result:
			return err
		case <-ctx.Done():
			cancel()
			return errors.Join(ctx.Err(), <-result)
		case <-ticker.C:
			if _, err := runtime.queryControl.HeartbeatQueryCoordinator(ctx, authority); err != nil {
				cancel()
				return errors.Join(err, <-result)
			}
		}
	}
}

func (runtime *Runtime) runOneQuery(ctx context.Context) (bool, error) {
	if runtime.queryControl == nil || runtime.queryWorker == nil {
		return false, nil
	}
	task, err := runtime.queryControl.ClaimQueryTask(ctx, runtime.installation.InstallationID, runtime.installation.StorageGeneration, runtime.workerOwner)
	if err != nil || task == nil {
		return false, err
	}
	err = runtime.withQueryHeartbeat(ctx, task.Authority, func(taskContext context.Context) error {
		return runtime.queryWorker.Execute(taskContext, *task)
	})
	if err != nil && ctx.Err() == nil {
		transient := !errors.Is(err, engine.ErrQueryExecutionInvalid) && !errors.Is(err, engine.ErrQueryExecutionLimit) && !errors.Is(err, query.ErrQueryLimit)
		failErr := runtime.queryControl.FailQueryTask(ctx, task.Authority, queryFailureCode(err), transient)
		return true, errors.Join(err, failErr)
	}
	return true, err
}

func (runtime *Runtime) withQueryHeartbeat(ctx context.Context, authority control.QueryTaskAuthority, task func(context.Context) error) error {
	return executeQueryTaskWithHeartbeat(ctx, runtime.queryControl, authority, task)
}

func queryFailureCode(err error) string {
	switch {
	case errors.Is(err, engine.ErrQueryExecutionLimit), errors.Is(err, query.ErrQueryLimit):
		return "query_limit_exceeded"
	case errors.Is(err, engine.ErrQueryExecutionInvalid):
		return "invalid_query_plan"
	default:
		return "query_worker_failed"
	}
}

func (runtime *Runtime) runOneConversion(ctx context.Context) (bool, error) {
	job, err := runtime.publication.ClaimConversion(ctx, runtime.installation.InstallationID, runtime.installation.StorageGeneration, runtime.workerOwner, workerLease)
	if err != nil || job == nil {
		return false, err
	}
	err = runtime.withJobHeartbeat(ctx, job.Authority, func(taskContext context.Context) error {
		return runtime.converter.ConvertAndPrepare(taskContext, *job)
	})
	return true, err
}

func (runtime *Runtime) runOnePublication(ctx context.Context) (bool, error) {
	lanes, err := runtime.publication.PublicationLanes(ctx, runtime.installation.StorageGeneration, 64)
	if err != nil {
		return false, err
	}
	for _, lane := range lanes {
		job, err := runtime.publication.ClaimPublication(ctx, runtime.installation.InstallationID, runtime.installation.StorageGeneration, lane.TenantID, lane.LaneID, runtime.workerOwner, workerLease)
		if err != nil {
			return false, err
		}
		if job == nil {
			continue
		}
		err = runtime.withJobHeartbeat(ctx, job.Authority, func(taskContext context.Context) error {
			_, publishErr := runtime.publisher.VerifyAndPublish(taskContext, *job)
			return publishErr
		})
		return true, err
	}
	return false, nil
}

func (runtime *Runtime) withJobHeartbeat(ctx context.Context, authority control.JobAuthority, task func(context.Context) error) error {
	taskContext, cancel := context.WithCancel(ctx)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- task(taskContext) }()
	ticker := time.NewTicker(workerHeartbeat)
	defer ticker.Stop()
	for {
		select {
		case err := <-result:
			return err
		case <-ctx.Done():
			cancel()
			return errors.Join(ctx.Err(), <-result)
		case <-ticker.C:
			if _, err := runtime.publication.Heartbeat(ctx, authority, workerLease); err != nil {
				cancel()
				return errors.Join(err, <-result)
			}
		}
	}
}
