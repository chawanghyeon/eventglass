package contracts

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/app"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
)

// Includes the real child process and the supervisor's verification/cleanup,
// unlike a direct engine.Convert benchmark. No PG or S3 requests are made.
func BenchmarkConversionProcess(b *testing.B) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "arm64" {
		b.Fatal("process conversion measurements require Linux ARM64")
	}
	binary := os.Getenv("EVENTGLASS_TEST_BINARY")
	if binary == "" {
		b.Fatal("the pinned ARM64 Eventglass binary is required")
	}
	for _, profile := range []struct{ records, days int }{{1, 1}, {100, 1}, {8, 8}} {
		b.Run(fmt.Sprintf("records=%d/days=%d", profile.records, profile.days), func(b *testing.B) {
			root := b.TempDir()
			stagePath := filepath.Join(root, "selected.jsonl")
			stage, err := os.OpenFile(stagePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
			if err != nil {
				b.Fatal(err)
			}
			encoder := json.NewEncoder(stage)
			const batchID = "00000000-0000-4000-8000-000000000222"
			for index := range profile.records {
				record := model.Record{
					TenantID: 1, ProjectID: 2, RecordID: fmt.Sprintf("%064x", index+1), AcceptanceID: "00000000-0000-4000-8000-000000000111",
					ItemOrdinal: index, Kind: model.KindLog, EventTimeUS: 1_700_000_000_000_000 + int64(index%profile.days)*86_400_000_000,
					ArrivalTimeUS: 1_700_000_000_000_001, Message: "process benchmark", Raw: json.RawMessage(`{"body":"process benchmark"}`),
					Attrs: []model.Attribute{}, SearchValues: []string{"process benchmark"}, Warnings: []string{},
					SchemaVersion: model.SchemaVersion, NormalizerVersion: model.NormalizerVersion, ScrubVersion: model.ScrubVersion,
				}
				if err := encoder.Encode(engine.StageRecord{Version: 1, GlobalOrdinal: index, BatchID: batchID, LaneID: 0, BatchSeq: 1,
					ReceivedTimeUS: 1_700_000_000_000_002, GroupingVersion: 1, Record: record}); err != nil {
					stage.Close()
					b.Fatal(err)
				}
			}
			if err := stage.Close(); err != nil {
				b.Fatal(err)
			}
			request := engine.ConversionRequest{
				Version: 1, StagePath: stagePath, OutputDirectory: filepath.Join(root, "output"), SpillDirectory: filepath.Join(root, "spill"),
				TenantID: 1, LaneID: 0, BatchSeq: 1, BatchID: batchID, SelectedRecords: profile.records,
				NativeMemoryBytes: 256 << 20, NativeSpillBytes: 256 << 20,
			}
			runner := app.ProcessConversionRunner{BinaryPath: binary, Gate: app.NewNativeTaskGate()}
			b.ReportAllocs()
			for b.Loop() {
				pairs := 0
				summary, err := runner.Run(context.Background(), request, func(engine.ConvertedBundle) error { pairs++; return nil })
				if err != nil || summary.DuckDBVersion != "v2.0.0-dev84020" || summary.SelectedRecordCount != profile.records || summary.BundleCount != profile.days || pairs != profile.days {
					b.Fatalf("summary=%+v pairs=%d err=%v", summary, pairs, err)
				}
				if err := os.RemoveAll(request.OutputDirectory); err != nil {
					b.Fatal(err)
				}
			}
			// Kernel maxima are cumulative for this benchmark process, not
			// per-operation allocations or attributed to only the last sample.
			for label, who := range map[string]int{"child-peak-rss-B": syscall.RUSAGE_CHILDREN, "supervisor-peak-rss-B": syscall.RUSAGE_SELF} {
				var usage syscall.Rusage
				if err := syscall.Getrusage(who, &usage); err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(usage.Maxrss)*1024, label)
			}
		})
	}
}
