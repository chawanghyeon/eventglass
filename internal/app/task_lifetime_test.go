package app

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTaskLifetimeJoinsAfterHeartbeatFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	lost := errors.New("lease lost")
	taskFailure := errors.New("task stopped")
	joined := false
	err := superviseTask(ctx, time.Millisecond, func(context.Context) error { return lost }, func(ctx context.Context) error {
		<-ctx.Done()
		joined = true
		return taskFailure
	})
	if !joined || !errors.Is(err, lost) || !errors.Is(err, taskFailure) {
		t.Fatalf("joined=%v err=%v", joined, err)
	}
}

func TestTaskLifetimeCanceledBeforeStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := superviseTask(ctx, time.Second, func(context.Context) error { t.Fatal("heartbeat started"); return nil }, func(context.Context) error { t.Error("task started"); return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
