//go:build duckdb_use_static_lib && linux

package engine_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
)

func wideCompactionFixture(t testing.TB, count int) (engine.CompactionRequest, map[string][32]byte) {
	t.Helper()
	inputs := make([]engine.CompactionInput, count)
	expected := make(map[string][32]byte, count*16)
	var inputBytes int64
	for index := range inputs {
		request, raw := wideConversionBatch(t, 16, index)
		_, err := engine.Convert(context.Background(), request, func(bundle engine.ConvertedBundle) error {
			inputs[index] = engine.CompactionInput{BundleID: fmt.Sprint(index), AnalyticsPath: bundle.Analytics.Path, PayloadPath: bundle.Payload.Path, IdentitySHA256: bundle.IdentitySHA256}
			inputBytes += bundle.Analytics.Evidence.Bytes + bundle.Payload.Evidence.Bytes
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		for id, value := range raw {
			if _, exists := expected[id]; exists {
				t.Fatal("fixture contains duplicate identity")
			}
			expected[id] = sha256.Sum256([]byte(value))
		}
		if err := os.Remove(request.StagePath); err != nil {
			t.Fatal(err)
		}
	}
	root := t.TempDir()
	t.Logf("actual paired inputs=%d records=%d compressed_bytes=%d", count, len(expected), inputBytes)
	return engine.CompactionRequest{Version: 1, TenantID: 1, LaneID: 3, SchemaVersion: 1, GroupingVersion: 1,
		EventDay: "1970-01-01", Kind: model.KindError, Inputs: inputs, OutputDirectory: filepath.Join(root, "output"), SpillDirectory: filepath.Join(root, "spill"),
		NativeMemoryBytes: 256 << 20, NativeSpillBytes: engine.DefaultNativeSpillBytes}, expected
}

func TestCompactWideInputsWithinNativeMemory(t *testing.T) {
	for _, memory := range []int64{192 << 20, 256 << 20} {
		t.Run(fmt.Sprintf("native-%dMiB", memory>>20), func(t *testing.T) {
			testCompactWideInputs(t, memory)
		})
	}
}

func testCompactWideInputs(t *testing.T, memory int64) {
	request, expected := wideCompactionFixture(t, 56)
	request.NativeMemoryBytes = memory
	runtime.GC()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	started := time.Now()
	result, err := engine.Compact(ctx, request)
	if err != nil || result.Bundle.RowCount != int64(len(expected)) {
		t.Fatalf("wide compaction rows=%d elapsed=%s err=%v", result.Bundle.RowCount, time.Since(started), err)
	}
	db, err := engine.Open(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "SET memory_limit='192MiB'"); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryContext(ctx, `SELECT record_id,sha256(raw_json) FROM read_parquet(?)`, result.Bundle.Payload.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, raw string
		if err := rows.Scan(&id, &raw); err != nil {
			t.Fatal(err)
		}
		if want, exists := expected[id]; !exists || hex.EncodeToString(want[:]) != raw {
			t.Fatalf("wide payload changed for %s", id)
		}
		delete(expected, id)
	}
	if err := rows.Err(); err != nil || len(expected) != 0 {
		t.Fatalf("remaining=%d err=%v", len(expected), err)
	}
	rows.Close()
	db.Close()
	verifyWideCompactionValues(t, request, result)
	t.Logf("wide merged records=%d analytics_bytes=%d payload_bytes=%d", result.Bundle.RowCount, result.Bundle.Analytics.Evidence.Bytes, result.Bundle.Payload.Evidence.Bytes)
}

// Hash every actual native column, including nested/decimal values and all
// payload fragments, independently of Compact's identity/count inspection.
func verifyWideCompactionValues(t *testing.T, request engine.CompactionRequest, result engine.CompactionResult) {
	t.Helper()
	db, err := engine.Open(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("SET memory_limit='192MiB'"); err != nil {
		t.Fatal(err)
	}
	for _, payload := range []bool{false, true} {
		paths := make([]string, len(request.Inputs))
		for i, input := range request.Inputs {
			path := input.AnalyticsPath
			if payload {
				path = input.PayloadPath
			}
			paths[i] = "'" + strings.ReplaceAll(path, "'", "''") + "'"
		}
		expected := map[string]string{}
		rows, err := db.Query(`SELECT record_id,sha256(to_json(a)) FROM read_parquet([` + strings.Join(paths, ",") + `]) a`)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var id, hash string
			if err := rows.Scan(&id, &hash); err != nil {
				t.Fatal(err)
			}
			expected[id] = hash
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
		path := result.Bundle.Analytics.Path
		order := "received_time_us,lane_id,batch_seq,record_ordinal,record_id"
		if payload {
			path = result.Bundle.Payload.Path
			order = "record_id"
		}
		rows, err = db.Query(`SELECT record_id,sha256(to_json(a)) FROM read_parquet(?) a`, path)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var id, hash string
			if err := rows.Scan(&id, &hash); err != nil {
				t.Fatal(err)
			}
			if expected[id] != hash {
				t.Fatalf("payload=%t changed columns for %s", payload, id)
			}
			delete(expected, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil || len(expected) != 0 {
			t.Fatalf("column hashes remaining=%d err=%v", len(expected), err)
		}
		var mismatches int64
		if err := db.QueryRow(`SELECT count(*) FROM (SELECT file_row_number,row_number() OVER(ORDER BY `+order+`)-1 expected FROM read_parquet(?,file_row_number=true)) WHERE file_row_number<>expected`, path).Scan(&mismatches); err != nil || mismatches != 0 {
			t.Fatalf("payload=%t physical sort mismatches=%d err=%v", payload, mismatches, err)
		}
	}
}

func TestCompactWideCancellationAndRetry(t *testing.T) {
	request, _ := wideCompactionFixture(t, 32)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := engine.Compact(ctx, request); done <- err }()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	observed := false
	for !observed {
		select {
		case err := <-done:
			t.Fatalf("compaction returned before cancellation barrier: %v", err)
		case <-deadline.C:
			cancel()
			<-done
			t.Fatal("no active partition write")
		case <-ticker.C:
			_ = filepath.WalkDir(request.SpillDirectory, func(path string, entry os.DirEntry, err error) error {
				if err == nil && !entry.IsDir() && strings.HasSuffix(path, ".parquet") {
					if info, err := entry.Info(); err == nil && info.Size() > 0 {
						observed = true
					}
				}
				return nil
			})
		}
	}
	cancel()
	if err := <-done; err == nil {
		t.Fatal("canceled compaction succeeded")
	}
	if _, err := os.Stat(request.SpillDirectory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("spill survived joined cancellation: %v", err)
	}
	if err := os.RemoveAll(request.OutputDirectory); err != nil {
		t.Fatal(err)
	}
	result, err := engine.Compact(context.Background(), request)
	if err != nil || result.Bundle.RowCount != 512 {
		t.Fatalf("retry rows=%d err=%v", result.Bundle.RowCount, err)
	}
	verifyWideCompactionValues(t, request, result)
}

func TestCompactWideRetentionBoundaryAndSpillAdmission(t *testing.T) {
	request, _ := wideCompactionFixture(t, 32)
	request.MinReceivedTimeUS = 2_000_016
	request.NativeSpillBytes = 64 << 20
	if _, err := engine.Compact(context.Background(), request); err == nil || !strings.Contains(err.Error(), "intermediate budget exceeded") {
		t.Fatalf("insufficient intermediate reservation: %v", err)
	}
	if _, err := os.Stat(request.SpillDirectory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed admission leaked spill: %v", err)
	}
	if err := os.RemoveAll(request.OutputDirectory); err != nil {
		t.Fatal(err)
	}
	request.NativeSpillBytes = engine.DefaultNativeSpillBytes
	result, err := engine.Compact(context.Background(), request)
	if err != nil || result.Bundle.RowCount != 256 || result.Bundle.Analytics.MinReceivedTimeUS != request.MinReceivedTimeUS {
		t.Fatalf("retention boundary: rows=%d min=%d err=%v", result.Bundle.RowCount, result.Bundle.Analytics.MinReceivedTimeUS, err)
	}
	expected := request
	expected.Inputs = request.Inputs[16:]
	verifyWideCompactionValues(t, expected, result)
	if err := os.RemoveAll(request.OutputDirectory); err != nil {
		t.Fatal(err)
	}
	request.MinReceivedTimeUS = 2_000_000
	result, err = engine.Compact(context.Background(), request)
	if err != nil || result.Bundle.RowCount != 512 {
		t.Fatalf("all retained rows=%d err=%v", result.Bundle.RowCount, err)
	}
	verifyWideCompactionValues(t, request, result)
}

func BenchmarkCompactWideInputs(b *testing.B) {
	request, expected := wideCompactionFixture(b, 16)
	ids := make([]string, 0, len(expected))
	for id := range expected {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	hash := sha256.New()
	for _, id := range ids {
		fmt.Fprintln(hash, id)
	}
	identity := hex.EncodeToString(hash.Sum(nil))
	runtime.GC()
	b.ReportAllocs()
	for b.Loop() {
		result, err := engine.Compact(context.Background(), request)
		if err != nil || result.Bundle.RowCount != int64(len(expected)) || result.Bundle.IdentitySHA256 != identity {
			b.Fatalf("wide compaction rows=%d err=%v", result.Bundle.RowCount, err)
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
	b.ReportMetric(float64(len(expected))*float64(b.N)/b.Elapsed().Seconds(), "records/s")
	b.ReportMetric(0, "S3-requests/op")
	b.ReportMetric(0, "S3-bytes/op")
}
