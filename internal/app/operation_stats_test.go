package app

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestOperationStatsSanitizesAndBoundsRepeatedFailures(t *testing.T) {
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	var stats operationStats
	for range 20 {
		stats.record(context.Background(), "query", time.Now(), true, errors.New("password=private-secret")) // pragma: allowlist secret -- synthetic redaction sentinel
	}
	if strings.Contains(output.String(), "private-secret") || strings.Count(output.String(), "runtime operation") != 1 {
		t.Fatal(output.String())
	}
	if stats.entries["query"].failures != 20 {
		t.Fatal("lost operation counters")
	}
	stats.record(context.Background(), "query", time.Now().Add(-time.Second), false, nil)
	var metrics bytes.Buffer
	stats.writeMetrics(&metrics)
	if strings.Contains(metrics.String(), "private-secret") ||
		!strings.Contains(metrics.String(), `eventglass_operation_calls_total{operation="query"} 21`) ||
		!strings.Contains(metrics.String(), `eventglass_operation_work_total{operation="query"} 20`) ||
		!strings.Contains(metrics.String(), `eventglass_operation_failures_total{operation="query"} 20`) ||
		stats.entries["query"].elapsed-stats.entries["query"].busy < time.Second {
		t.Fatalf("idle/work accounting or redaction failed: %s", metrics.String())
	}
}
