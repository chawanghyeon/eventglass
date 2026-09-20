package app

import (
	"context"
	"errors"
	"time"
)

// superviseTask owns task cancellation and joins the task before returning.
// Heartbeat failure must not release files/permits still used by native work.
// Authority and transaction policy remain in the concrete heartbeat callback.
func superviseTask(ctx context.Context, interval time.Duration, heartbeat func(context.Context) error, task func(context.Context) error) error {
	if interval <= 0 || heartbeat == nil || task == nil {
		return errors.New("invalid task supervision")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	taskContext, cancel := context.WithCancel(ctx)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- task(taskContext) }()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case err := <-result:
			return err
		case <-ctx.Done():
			cancel()
			return errors.Join(ctx.Err(), <-result)
		case <-ticker.C:
			if err := heartbeat(taskContext); err != nil {
				cancel()
				return errors.Join(err, <-result)
			}
		}
	}
}
