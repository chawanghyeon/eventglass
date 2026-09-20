package app

import (
	"context"
	"time"
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
