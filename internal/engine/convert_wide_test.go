//go:build duckdb_use_static_lib && linux

package engine_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/ingest"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/sdk"
)

// Normalize actual SDK-shaped values so canonical attributes and raw payload
// have the same wide-field duplication as the durable integration regression.
// This isolates native execution; it does not claim to exercise durable ACK.
func wideConversionRequest(t testing.TB, count int) (engine.ConversionRequest, map[string]string) {
	return wideConversionBatch(t, count, 0)
}

func wideConversionBatch(t testing.TB, count, batch int) (engine.ConversionRequest, map[string]string) {
	return wideConversionScopedBatch(t, count, batch, 10, nil)
}

func wideConversionScopedBatch(t testing.TB, count, batch int, projectID int64, service *string) (engine.ConversionRequest, map[string]string) {
	t.Helper()
	root := t.TempDir()
	random := rand.New(rand.NewSource(1780 + int64(batch)))
	batchID := fmt.Sprintf("00000000-0000-4000-8000-%012x", 0x222+batch)
	items := make([]sdk.Item, count)
	for ordinal := range items {
		blob := make([]byte, 72<<10)
		if _, err := random.Read(blob); err != nil {
			t.Fatal(err)
		}
		value := map[string]any{
			"event_id": fmt.Sprintf("%032x", batch*count+ordinal+1), "message": fmt.Sprintf("wide conversion %d", ordinal),
			"extra": map[string]any{"blob": base64.StdEncoding.EncodeToString(blob)},
		}
		if service != nil {
			value["tags"] = map[string]any{"service.name": *service}
		}
		items[ordinal] = sdk.Item{Ordinal: ordinal, Type: "event", Value: value}
	}
	normalized, err := ingest.NormalizeEnvelope(sdk.Envelope{Items: items}, ingest.NormalizeOptions{
		TenantID: 1, ProjectID: projectID, AcceptanceID: fmt.Sprintf("00000000-0000-4000-8000-%012x", 0x111+batch), ArrivalTime: time.Unix(1, 0)})
	if err != nil {
		t.Fatal(err)
	}
	stage := filepath.Join(root, "selected.jsonl")
	file, err := os.OpenFile(stage, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	expected := make(map[string]string, count)
	for ordinal, record := range normalized.Records {
		staged := engine.StageRecord{Version: 1, GlobalOrdinal: ordinal,
			BatchID: batchID, LaneID: 3, BatchSeq: int64(4 + batch),
			ReceivedTimeUS: 2_000_000 + int64(batch), GroupingVersion: 1, Record: record,
			IssueID: fmt.Sprintf("%064x", ordinal+1), FingerprintSHA256: fmt.Sprintf("%064x", ordinal+1), IssueTitle: record.Message}
		if err := encoder.Encode(staged); err != nil {
			t.Fatal(err)
		}
		expected[record.RecordID] = string(record.Raw)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return engine.ConversionRequest{Version: 1, StagePath: stage,
		OutputDirectory: filepath.Join(root, "output"), SpillDirectory: filepath.Join(root, "spill"),
		TenantID: 1, LaneID: 3, BatchSeq: int64(4 + batch), BatchID: batchID,
		SelectedRecords: count, SelectedErrors: count, NativeMemoryBytes: 256 << 20, NativeSpillBytes: 256 << 20}, expected
}

func TestConvertWideSelectedBatchWithinNativeMemory(t *testing.T) {
	for _, memory := range []int64{192 << 20, 256 << 20} {
		t.Run(fmt.Sprintf("native-%dMiB", memory>>20), func(t *testing.T) {
			testWideSelectedBatch(t, memory)
		})
	}
}

func testWideSelectedBatch(t *testing.T, memory int64) {
	t.Helper()
	request, expected := wideConversionRequest(t, 64)
	request.NativeMemoryBytes = memory
	runtime.GC()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var bundle engine.ConvertedBundle
	summary, err := engine.Convert(ctx, request, func(value engine.ConvertedBundle) error { bundle = value; return nil })
	if err != nil || summary.BundleCount != 1 || bundle.RowCount != 64 || bundle.Kind != model.KindError {
		t.Fatalf("wide conversion: summary=%+v rows=%d err=%v", summary, bundle.RowCount, err)
	}
	db, err := engine.Open(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "SET memory_limit='64MiB'"); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryContext(ctx, `SELECT record_id,raw_json FROM read_parquet(?) ORDER BY record_id`, bundle.Payload.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, raw string
		if err := rows.Scan(&id, &raw); err != nil {
			t.Fatal(err)
		}
		if value, exists := expected[id]; !exists || value != raw {
			t.Fatalf("wide payload changed for %s", id)
		}
		delete(expected, id)
	}
	if err := rows.Err(); err != nil || len(expected) != 0 {
		t.Fatalf("missing payloads=%d err=%v", len(expected), err)
	}
	t.Logf("wide rows=%d analytics_bytes=%d payload_bytes=%d native_limit=%d", bundle.RowCount, bundle.Analytics.Evidence.Bytes, bundle.Payload.Evidence.Bytes, request.NativeMemoryBytes)
}

func BenchmarkConvertWideSelectedBatch(b *testing.B) {
	request, _ := wideConversionRequest(b, 16)
	runtime.GC()
	b.ReportAllocs()
	for b.Loop() {
		summary, err := engine.Convert(context.Background(), request, func(engine.ConvertedBundle) error { return nil })
		if err != nil || summary.SelectedRecordCount != 16 || summary.BundleCount != 1 {
			b.Fatalf("wide conversion: %+v %v", summary, err)
		}
		if err := os.RemoveAll(request.OutputDirectory); err != nil {
			b.Fatal(err)
		}
	}
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(usage.Maxrss)*1024, "process-maxrss-B")
	b.ReportMetric(16*float64(b.N)/b.Elapsed().Seconds(), "records/s")
	b.ReportMetric(0, "S3-requests/op")
	b.ReportMetric(0, "S3-bytes/op")
}

func TestConvertWideCancellationAndRetry(t *testing.T) {
	request, _ := wideConversionRequest(t, 64)
	request.NativeMemoryBytes = 192 << 20
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := engine.Convert(ctx, request, func(engine.ConvertedBundle) error { return nil })
		done <- err
	}()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	observed := false
wait:
	for {
		select {
		case err := <-done:
			t.Fatalf("conversion completed before in-flight cancellation: %v", err)
		case <-deadline.C:
			break wait
		case <-ticker.C:
			path := filepath.Join(request.OutputDirectory, "bundle-000000-analytics.parquet")
			if info, err := os.Stat(path); err == nil && info.Size() > 0 {
				observed = true
				break wait
			}
		}
	}
	cancel()
	if err := <-done; err == nil || !observed {
		t.Fatalf("in-flight cancellation observed=%t err=%v", observed, err)
	}
	// Cleanup assertions occur only after native execution and database close
	// have returned. The immutable input remains available for the retry.
	if entries, err := os.ReadDir(request.OutputDirectory); err != nil || len(entries) != 0 {
		t.Fatalf("partial output survived joined cancellation: %v %v", entries, err)
	}
	if _, err := os.Stat(request.SpillDirectory); !os.IsNotExist(err) {
		t.Fatalf("spill survived joined cancellation: %v", err)
	}
	retry, stop := context.WithTimeout(context.Background(), time.Minute)
	defer stop()
	summary, err := engine.Convert(retry, request, func(engine.ConvertedBundle) error { return nil })
	if err != nil || summary.SelectedRecordCount != 64 || summary.BundleCount != 1 {
		t.Fatalf("retry after cancellation: %+v %v", summary, err)
	}
}
