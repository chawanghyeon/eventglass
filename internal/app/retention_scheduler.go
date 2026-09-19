package app

import (
	"context"
	"time"
)

const retentionTickInterval = 60 * time.Second

func (runtime *Runtime) runRetentionScheduler(ctx context.Context) error {
	for {
		_, _, _ = runtime.queryControl.AdvanceRetentionFloor(ctx)
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
