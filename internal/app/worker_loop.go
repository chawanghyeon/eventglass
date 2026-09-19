package app

import (
	"context"
	"errors"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
)

const (
	workerLease     = 30 * time.Second
	workerHeartbeat = 10 * time.Second
	workerIdle      = 100 * time.Millisecond
)

func (runtime *Runtime) runWorker(ctx context.Context) error {
	for ctx.Err() == nil {
		progressed, err := runtime.runOnePublication(ctx)
		if err == nil && !progressed {
			progressed, err = runtime.runOneConversion(ctx)
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
