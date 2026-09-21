package app

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/query"
)

const (
	workerLease     = 30 * time.Second
	workerHeartbeat = 10 * time.Second
	workerIdle      = 100 * time.Millisecond
	queryTaskBurst  = control.MaxRunningQueryTasks * 2
)

func (runtime *Runtime) runWorker(ctx context.Context) error {
	// Network delivery and publication cannot wait behind a long native query.
	// Native work stays in one lane and retains the shared child gate. Every loop
	// is joined on shutdown; durable claims remain the cross-process authority.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	groups := [][]workerOperation{
		{{"publication", runtime.runOnePublication}},
		{{"delivery", runtime.runOneDelivery}},
		runtime.nativeWorkerOperations(),
	}
	results := make(chan error, len(groups))
	for _, group := range groups {
		go func() { results <- runtime.runWorkerGroup(ctx, group) }()
	}
	var result error
	for range groups {
		result = errors.Join(result, <-results)
		cancel()
	}
	return result
}

func (runtime *Runtime) nativeWorkerOperations() []workerOperation {
	// A distributed query has up to MaxRunningQueryTasks ready partitions. Give
	// those partitions plus one bounded reducer level a bounded burst so
	// a conversion does not get inserted between every short query child. The
	// following conversion slot preserves durable ingest progress under a
	// continuous query queue.
	work := []workerOperation{{"conversion", runtime.runOneConversion}, {"query_plan", runtime.runOneQueryCoordinator}}
	for range queryTaskBurst {
		work = append(work, workerOperation{"query", runtime.runOneQuery})
	}
	// All background maintenance shares this lane's spare-time budget. GC still
	// owns its backup/snapshot interlocks; moving its dispatch does not relax them.
	return append(work, workerOperation{"retention", runtime.runOneRetention}, workerOperation{"compaction", runtime.runOneCompaction}, workerOperation{"gc", runtime.runOneGC})
}

type workerOperation struct {
	name string
	run  func(context.Context) (bool, error)
}

func (runtime *Runtime) runWorkerGroup(ctx context.Context, work []workerOperation) error {
	next := 0
	idle := make([]string, 0, len(work))
	budget := maintenanceTimeBudget{origin: time.Now()}
	hasMaintenance := slices.ContainsFunc(work, func(operation workerOperation) bool { return maintenanceOperation(operation.name) })
	var maintenanceRetryAt time.Time
	for ctx.Err() == nil {
		idle = idle[:0]
		var progressed bool
		var err error
		for offset := range work {
			if ctx.Err() != nil {
				break
			}
			index := (next + offset) % len(work)
			// Check every foreground slot before considering maintenance, even
			// if the rotation cursor currently points at a maintenance slot.
			if maintenanceOperation(work[index].name) {
				continue
			}
			// Repeated slots give ready queries a bounded burst. Once a claim
			// finds no work, do not repeat its database transaction in this
			// sweep. Recheck after other progress or the normal idle interval.
			if slices.Contains(idle, work[index].name) {
				continue
			}
			started := time.Now()
			progressed, err = work[index].run(ctx)
			runtime.stats.record(ctx, work[index].name, started, progressed, err)
			if err != nil || progressed {
				next = (index + 1) % len(work)
				break
			}
			idle = append(idle, work[index].name)
		}
		maintenanceFailed := false
		if !progressed && err == nil && hasMaintenance && ctx.Err() == nil && !time.Now().Before(maintenanceRetryAt) {
			for offset := range work {
				index := (next + offset) % len(work)
				if !maintenanceOperation(work[index].name) {
					continue
				}
				started := time.Now()
				allowance := budget.allowance(started)
				if allowance < maintenanceJoinAllowance+maintenanceMinWorkSlice || ctx.Err() != nil {
					break
				}
				taskContext, cancel := context.WithTimeout(ctx, allowance-maintenanceJoinAllowance)
				// run joins native/IO work and cleanup before it returns. A
				// deadline requests cancellation; it never releases ownership.
				progressed, err = work[index].run(taskContext)
				cancel()
				finished := time.Now()
				budget.record(started, finished, true)
				runtime.stats.recordInterval(ctx, work[index].name, started, finished, progressed, err)
				if finished.Sub(started) > allowance {
					runtime.stats.record(ctx, "maintenance_budget_overrun", started, true, context.DeadlineExceeded)
				}
				if err != nil {
					// A frozen GC interlock or a maintenance retry must not
					// put ready foreground work to sleep with this attempt.
					maintenanceRetryAt = finished.Add(250 * time.Millisecond)
					maintenanceFailed = true
				}
				if err != nil || progressed {
					next = (index + 1) % len(work)
					break
				}
			}
		}
		if ctx.Err() != nil {
			break
		}
		if maintenanceFailed {
			continue
		}
		delay := workerIdle
		if err != nil {
			delay = 250 * time.Millisecond
		}
		if progressed && err == nil {
			continue
		}
		idleStarted := time.Now()
		nativeSequence, nativeBusy := runtime.nativeTasks.activity()
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
			sequence, busy := runtime.nativeTasks.activity()
			if hasMaintenance && err == nil && !nativeBusy && !busy && sequence == nativeSequence && ctx.Err() == nil {
				idleFinished := budget.recordIdleWait(idleStarted, time.Now())
				runtime.stats.recordInterval(ctx, "maintenance_spare", idleStarted, idleFinished, true, nil)
			}
		}
	}
	return nil
}

func maintenanceOperation(name string) bool {
	return name == "retention" || name == "compaction" || name == "gc"
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
	return superviseTask(ctx, workerHeartbeat, func(ctx context.Context) error {
		return runtime.maintenance.HeartbeatCompaction(ctx, authority, workerLease)
	}, task)
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
	return superviseTask(ctx, 15*time.Second, func(ctx context.Context) error {
		_, err := runtime.queryControl.HeartbeatQueryCoordinator(ctx, authority)
		return err
	}, plan)
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
	return superviseTask(ctx, workerHeartbeat, func(ctx context.Context) error {
		_, err := runtime.publication.Heartbeat(ctx, authority, workerLease)
		return err
	}, task)
}
