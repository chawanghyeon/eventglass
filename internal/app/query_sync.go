package app

import (
	"context"
	"errors"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/query"
)

// DurableQuerySyncExecutor helps only the calling query. All claims, leases,
// attempts, outputs, and terminal transitions still go through the same durable
// control-plane path used by background workers.
type DurableQuerySyncExecutor struct {
	Control           *control.QueryOperations
	Workflow          *DurableQueryWorkflow
	InstallationID    string
	StorageGeneration int64
	Owner             string
}

func (executor *DurableQuerySyncExecutor) Execute(ctx context.Context, tokenHash [32]byte, tenantID int64, queryID string) error {
	if executor == nil || executor.Control == nil || executor.Workflow == nil || executor.InstallationID == "" || executor.StorageGeneration <= 0 || executor.Owner == "" {
		return errors.New("synchronous query executor is unavailable")
	}
	for {
		status, err := executor.Control.GetQueryStatus(ctx, tokenHash, tenantID, queryID)
		if err != nil {
			return err
		}
		switch status.State {
		case "succeeded":
			return nil
		case "failed", "canceled":
			return control.ErrQueryTerminal
		case "planning":
			return errors.New("synchronous query plan was not sealed")
		}
		task, err := executor.Control.ClaimQueryTaskForQuery(ctx, executor.InstallationID, executor.StorageGeneration, executor.Owner, queryID)
		if err != nil {
			return err
		}
		if task == nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(20 * time.Millisecond):
				continue
			}
		}
		err = executeQueryTaskWithHeartbeat(ctx, executor.Control, task.Authority, func(taskContext context.Context) error {
			return executor.Workflow.Execute(taskContext, *task)
		})
		if err == nil {
			continue
		}
		if ctx.Err() != nil {
			return errors.Join(ctx.Err(), err)
		}
		transient := !errors.Is(err, engine.ErrQueryExecutionInvalid) && !errors.Is(err, engine.ErrQueryExecutionLimit) && !errors.Is(err, query.ErrQueryLimit)
		failErr := executor.Control.FailQueryTask(ctx, task.Authority, queryFailureCode(err), transient)
		if failErr != nil {
			return errors.Join(err, failErr)
		}
	}
}

func executeQueryTaskWithHeartbeat(ctx context.Context, operations *control.QueryOperations, authority control.QueryTaskAuthority, execute func(context.Context) error) error {
	taskContext, cancel := context.WithCancel(ctx)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- execute(taskContext) }()
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
			if _, err := operations.HeartbeatQueryTask(ctx, authority); err != nil {
				cancel()
				return errors.Join(err, <-result)
			}
		}
	}
}
