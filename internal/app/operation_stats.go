package app

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/resource"
)

type operationStats struct {
	mu      sync.Mutex
	entries map[string]operationSample
}

type operationSample struct {
	calls, completed, failures int64
	busy                       time.Duration
	lastReport                 time.Time
}

// Names are fixed at role assembly, never tenant IDs or user-supplied labels.
// Do not log err.Error(): drivers may include SQL, object URLs or credentials.
func (stats *operationStats) record(ctx context.Context, name string, started time.Time, completed bool, err error) {
	now := time.Now()
	stats.mu.Lock()
	if stats.entries == nil {
		stats.entries = make(map[string]operationSample)
	}
	sample := stats.entries[name]
	sample.calls++
	if completed {
		sample.completed++
		sample.busy += now.Sub(started)
	}
	if err != nil {
		sample.failures++
	}
	report := sample.lastReport.IsZero() || now.Sub(sample.lastReport) >= 30*time.Second
	if report {
		sample.lastReport = now
	}
	stats.entries[name] = sample
	stats.mu.Unlock()
	if !report {
		return
	}
	level := slog.LevelInfo
	if err != nil {
		level = slog.LevelWarn
	}
	slog.Log(ctx, level, "runtime operation", "operation", name, "calls", sample.calls, "completed", sample.completed, "failures", sample.failures, "busy_ms", sample.busy.Milliseconds(), "last_duration_ms", now.Sub(started).Milliseconds(), "code", operationErrorCode(err))
}

func operationErrorCode(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, control.ErrBackupInterlock):
		return "backup_verification_required"
	case errors.Is(err, resource.ErrLimited):
		return "resource_exhausted"
	case errors.Is(err, control.ErrMaintenanceFence):
		return "lease_lost"
	default:
		return "operation_failed"
	}
}
