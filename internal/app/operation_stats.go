package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
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
	elapsed                    time.Duration
	lastReport                 time.Time
}

// Names are fixed at role assembly, never tenant IDs or user-supplied labels.
// Do not log err.Error(): drivers may include SQL, object URLs or credentials.
func (stats *operationStats) record(ctx context.Context, name string, started time.Time, completed bool, err error) {
	stats.recordInterval(ctx, name, started, time.Now(), completed, err)
}

func (stats *operationStats) recordInterval(ctx context.Context, name string, started, finished time.Time, completed bool, err error) {
	now := time.Now()
	duration := max(time.Duration(0), finished.Sub(started))
	stats.mu.Lock()
	if stats.entries == nil {
		stats.entries = make(map[string]operationSample)
	}
	sample := stats.entries[name]
	sample.calls++
	sample.elapsed += duration
	if completed {
		sample.completed++
		sample.busy += duration
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
	slog.Log(ctx, level, "runtime operation", "operation", name, "calls", sample.calls, "completed", sample.completed, "failures", sample.failures, "busy_ms", sample.busy.Milliseconds(), "last_duration_ms", duration.Milliseconds(), "code", operationErrorCode(err))
}

// Export the same fixed role-operation labels used by the logs. Copy under the
// lock so a slow metrics client never holds up a worker's accounting.
func (stats *operationStats) writeMetrics(writer io.Writer) {
	stats.mu.Lock()
	samples := make(map[string]operationSample, len(stats.entries))
	names := make([]string, 0, len(stats.entries))
	for name, sample := range stats.entries {
		samples[name] = sample
		names = append(names, name)
	}
	stats.mu.Unlock()
	sort.Strings(names)
	for _, name := range names {
		sample := samples[name]
		_, _ = fmt.Fprintf(writer, "eventglass_operation_calls_total{operation=%q} %d\n", name, sample.calls)
		// Work counts include claimed attempts that subsequently failed.
		_, _ = fmt.Fprintf(writer, "eventglass_operation_work_total{operation=%q} %d\n", name, sample.completed)
		_, _ = fmt.Fprintf(writer, "eventglass_operation_failures_total{operation=%q} %d\n", name, sample.failures)
		_, _ = fmt.Fprintf(writer, "eventglass_operation_elapsed_ms_total{operation=%q} %d\n", name, sample.elapsed.Milliseconds())
		_, _ = fmt.Fprintf(writer, "eventglass_operation_work_ms_total{operation=%q} %d\n", name, sample.busy.Milliseconds())
	}
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
