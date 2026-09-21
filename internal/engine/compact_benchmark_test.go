//go:build duckdb_use_static_lib && linux

package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/model"
)

// This local native microbenchmark excludes fixture generation and all S3/PG
// work. Each input is a real canonical conversion output, not a placeholder.
// Linux maxrss includes setup and preceding samples; Go allocation metrics do
// not include unmanaged native memory. Run matched binaries without other work.
func BenchmarkCompactionPairedInputs(b *testing.B) {
	for _, count := range []int{2, 8, 32, 128} {
		b.Run(fmt.Sprintf("inputs=%d", count), func(b *testing.B) {
			root := b.TempDir()
			const perInput = 16
			var compressed int64
			inputs := make([]CompactionInput, count)
			identity := sha256.New()
			for index := range inputs {
				directory := filepath.Join(root, fmt.Sprint(index))
				if err := os.Mkdir(directory, 0o700); err != nil {
					b.Fatal(err)
				}
				records := make([]StageRecord, perInput)
				for ordinal := range records {
					records[ordinal] = conversionRecord("a", model.KindLog, 1_700_000_000_000_000+int64(ordinal), ordinal)
					records[ordinal].Record.RecordID = fmt.Sprintf("%064x", index*perInput+ordinal+1)
					records[ordinal].BatchSeq = int64(index + 1)
					fmt.Fprintln(identity, records[ordinal].Record.RecordID)
				}
				request := ConversionRequest{Version: ConversionProtocolVersion,
					StagePath: writeConversionStage(b, directory, records), OutputDirectory: filepath.Join(directory, "output"), SpillDirectory: filepath.Join(directory, "spill"),
					TenantID: 1, LaneID: 3, BatchSeq: int64(index + 1), BatchID: records[0].BatchID, SelectedRecords: perInput,
					NativeMemoryBytes: 256 << 20, NativeSpillBytes: 256 << 20}
				_, err := Convert(context.Background(), request, func(bundle ConvertedBundle) error {
					inputs[index] = CompactionInput{BundleID: fmt.Sprint(index), AnalyticsPath: bundle.Analytics.Path, PayloadPath: bundle.Payload.Path, IdentitySHA256: bundle.IdentitySHA256}
					compressed += bundle.Analytics.Evidence.Bytes + bundle.Payload.Evidence.Bytes
					return nil
				})
				if err != nil {
					b.Fatal(err)
				}
			}
			expected := hex.EncodeToString(identity.Sum(nil))
			request := CompactionRequest{Version: CompactionProtocolVersion, TenantID: 1, LaneID: 3, SchemaVersion: 1, GroupingVersion: 1,
				EventDay: "2023-11-14", Kind: model.KindLog, Inputs: inputs, OutputDirectory: filepath.Join(root, "merged"), SpillDirectory: filepath.Join(root, "merge-spill"), NativeMemoryBytes: 256 << 20, NativeSpillBytes: 256 << 20}
			b.ReportAllocs()
			b.SetBytes(compressed)
			for b.Loop() {
				result, err := Compact(context.Background(), request)
				if err != nil || result.Bundle.RowCount != int64(count*perInput) || result.Bundle.Payload.RowCount != int64(count*perInput) || result.Bundle.IdentitySHA256 != expected {
					b.Fatalf("native paired compaction=%+v err=%v", result, err)
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
			b.ReportMetric(float64(count*perInput)*float64(b.N)/b.Elapsed().Seconds(), "records/s")
			b.ReportMetric(0, "S3-requests/op")
			b.ReportMetric(0, "S3-bytes/op")
		})
	}
}
