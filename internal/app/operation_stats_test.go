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
}
