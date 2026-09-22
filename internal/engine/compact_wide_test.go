//go:build duckdb_use_static_lib && linux

package engine_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
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
	return wideCompactionScopedFixture(t, count, false)
}

func wideCompactionScopedFixture(t testing.TB, count int, scoped bool) (engine.CompactionRequest, map[string][32]byte) {
	return wideCompactionSizedFixture(t, count, 16, scoped)
}

func wideCompactionSizedFixture(t testing.TB, count, perInput int, scoped bool) (engine.CompactionRequest, map[string][32]byte) {
	t.Helper()
	inputs := make([]engine.CompactionInput, count)
	expected := make(map[string][32]byte, count*perInput)
	var inputBytes int64
	for index := range inputs {
		var service *string
		projectID := int64(10)
		if scoped {
			projectID = 12 - int64(index/4)
			if index%4 != 3 {
				value := []string{"서비스", "billing", ""}[index%4]
				service = &value
			}
		}
		request, raw := wideConversionScopedBatch(t, perInput, index, projectID, service)
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

func TestCompactWideCanonicalLayoutAcrossProjectsAndServices(t *testing.T) {
	request, _ := wideCompactionScopedFixture(t, 12, true)
	request.NativeMemoryBytes = 192 << 20
	db, err := engine.Open(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, input := range request.Inputs {
		paths = append(paths, "'"+strings.ReplaceAll(input.AnalyticsPath, "'", "''")+"'")
	}
	var uncompressed int64
	err = db.QueryRow(`SELECT sum(total_uncompressed_size) FROM parquet_metadata([` + strings.Join(paths, ",") + `])`).Scan(&uncompressed)
	db.Close()
	if err != nil || uncompressed <= request.NativeMemoryBytes/8 {
		t.Fatalf("fixture must exercise partitioned COPY: bytes=%d err=%v", uncompressed, err)
	}
	result, err := engine.Compact(context.Background(), request)
	if err != nil || result.Bundle.RowCount != 192 || len(result.Bundle.ProjectIDs) != 3 {
		t.Fatalf("scoped rows=%d projects=%v err=%v", result.Bundle.RowCount, result.Bundle.ProjectIDs, err)
	}
	verifyWideCompactionValues(t, request, result)
}

func TestCompactWideInputsWithinNativeMemory(t *testing.T) {
	for _, memory := range []int64{192 << 20, 256 << 20} {
		t.Run(fmt.Sprintf("native-%dMiB", memory>>20), func(t *testing.T) {
			testCompactWideInputs(t, memory)
		})
	}
}

func TestCompactMaximumWideWorkerInputs(t *testing.T) {
	request, expected := wideCompactionSizedFixture(t, 14, 64, false)
	request.NativeMemoryBytes = 96 << 20
	runtime.GC()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	started := time.Now()
	result, err := engine.Compact(ctx, request)
	t.Logf("maximum native compaction elapsed=%s managed_memory=%d", time.Since(started), request.NativeMemoryBytes)
	if err != nil || result.Bundle.RowCount != int64(len(expected)) {
		t.Fatalf("maximum worker inputs: rows=%d err=%v", result.Bundle.RowCount, err)
	}
	verifyWideCompactionValues(t, request, result)
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
	if _, err := db.Exec("SET memory_limit='96MiB'"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("SET temp_directory='" + strings.ReplaceAll(t.TempDir(), "'", "''") + "'"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("SET max_temp_directory_size='2GiB'"); err != nil {
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
		expected := make(map[string]string)
		for _, path := range paths {
			// A16-ID page must not decode every input file's wide vectors.
			// Input files are independent here; merge their full-column evidence
			// while rejecting duplicate IDs and keeping the same total bound.
			for id, digest := range boundedWideColumnHashes(t, db, `read_parquet(`+path+`)`) {
				if _, exists := expected[id]; exists || len(expected) >= 8192 {
					t.Fatal("duplicate or unbounded input oracle identity")
				}
				expected[id] = digest
			}
		}
		t.Logf("complete-column input oracle payload=%t records=%d", payload, len(expected))
		path := result.Bundle.Analytics.Path
		order := "project_id,service NULLS FIRST,event_time_us,record_id"
		if payload {
			path = result.Bundle.Payload.Path
		}
		actual := boundedWideColumnHashes(t, db, `read_parquet('`+strings.ReplaceAll(path, "'", "''")+`')`)
		t.Logf("complete-column output oracle payload=%t records=%d", payload, len(actual))
		for id, hash := range actual {
			if expected[id] != hash {
				t.Fatalf("payload=%t changed columns for %s", payload, id)
			}
			delete(expected, id)
		}
		if len(expected) != 0 {
			t.Fatalf("column hashes remaining=%d", len(expected))
		}
		var mismatches int64
		ordered := `SELECT file_row_number,row_number() OVER(ORDER BY ` + order + `)-1 expected FROM read_parquet(?,file_row_number=true)`
		args := []any{path}
		if payload {
			ordered = `SELECT p.file_row_number,row_number() OVER(ORDER BY a.project_id,a.service NULLS FIRST,a.event_time_us,p.record_id)-1 expected FROM read_parquet(?,file_row_number=true) p JOIN read_parquet(?) a USING(record_id)`
			args = append(args, result.Bundle.Analytics.Path)
		}
		if err := db.QueryRow(`SELECT count(*) FROM (`+ordered+`) WHERE file_row_number<>expected`, args...).Scan(&mismatches); err != nil || mismatches != 0 {
			t.Fatalf("payload=%t physical sort mismatches=%d err=%v", payload, mismatches, err)
		}
	}
}

func TestWideColumnOracleIncludesNestedValuesAndPhysicalTypes(t *testing.T) {
	db, err := engine.Open(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	source := `(SELECT 'row'::VARCHAR record_id,NULL::VARCHAR optional,[{s:'blob',n:1},NULL] attrs,1::INTEGER n,3.125::DECIMAL(18,3) exact)`
	want := boundedWideColumnHashes(t, db, source)["row"]
	if got := boundedWideColumnHashes(t, db, source)["row"]; got == "" || got != want {
		t.Fatal("non-deterministic complete-column oracle")
	}
	for _, change := range []struct{ name, from, to string }{
		{"null_vs_empty", "NULL::VARCHAR", "''::VARCHAR"},
		{"nested_string", "'blob'", "'other'"},
		{"nested_integer", "n:1", "n:2"},
		{"nested_null", "},NULL]", "},{s:NULL,n:NULL}]"},
		{"physical_type", "1::INTEGER", "1::BIGINT"},
		{"decimal_value", "3.125", "3.126"},
		{"column_name", " optional,", " renamed,"},
		{"additional_column", " exact)", " exact,0 added)"},
	} {
		t.Run(change.name, func(t *testing.T) {
			got := boundedWideColumnHashes(t, db, strings.ReplaceAll(source, change.from, change.to))["row"]
			if got == "" || got == want {
				t.Fatal("changed native value/type/schema escaped complete-column oracle")
			}
		})
	}
}

// Hash every named/typed column with native JSON semantics, then chain their
// digests in schema order. Scan only one value column and16 identities at once;
// neither a complete wide-row JSON copy nor all wide vectors coexist. Every
// nested/decimal/null value remains covered, with at most8192 fixture records.
func boundedWideColumnHashes(t *testing.T, db *sql.DB, source string) map[string]string {
	t.Helper()
	columns, err := db.Query(`DESCRIBE SELECT * FROM ` + source)
	if err != nil {
		t.Fatal(err)
	}
	var terms []string
	for columns.Next() {
		var name, kind string
		var rest [4]sql.NullString
		if err := columns.Scan(&name, &kind, &rest[0], &rest[1], &rest[2], &rest[3]); err != nil {
			t.Fatal(err)
		}
		if len(terms) >= 64 {
			t.Fatal("unbounded oracle schema")
		}
		quoted := `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
		// Include the physical type as well, not merely a coincident JSON value.
		terms = append(terms, quoted+`:=struct_pack(type:='`+strings.ReplaceAll(kind, "'", "''")+`',value:=sha256(to_json(`+quoted+`)))`)
	}
	err = columns.Err()
	columns.Close()
	if err != nil || len(terms) == 0 {
		t.Fatalf("oracle schema: columns=%d err=%v", len(terms), err)
	}
	result := map[string]string{}
	after := ""
	for {
		rows, err := db.Query(`SELECT record_id FROM `+source+` WHERE record_id>? ORDER BY record_id LIMIT 16`, after)
		if err != nil {
			t.Fatal(err)
		}
		var ids []any
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
		if len(ids) == 0 {
			return result
		}
		page := make(map[string][32]byte, len(ids))
		for _, id := range ids {
			page[id.(string)] = [32]byte{}
		}
		for _, term := range terms {
			expression := `sha256(to_json(struct_pack(` + term + `)))`
			rows, err = db.Query(`SELECT record_id,`+expression+` FROM `+source+` a WHERE record_id IN (`+strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")+`)`, ids...)
			if err != nil {
				t.Fatal(err)
			}
			seen := make(map[string]bool, len(ids))
			for rows.Next() {
				var id, digest string
				if err := rows.Scan(&id, &digest); err != nil {
					t.Fatal(err)
				}
				previous, exists := page[id]
				if !exists || seen[id] || len(digest) != 64 {
					t.Fatal("invalid native column identity/digest")
				}
				seen[id] = true
				page[id] = sha256.Sum256(append(previous[:], digest...))
			}
			err = rows.Err()
			rows.Close()
			if err != nil || len(seen) != len(ids) {
				t.Fatalf("bounded column hashes=%d ids=%d err=%v", len(seen), len(ids), err)
			}
		}
		for id, digest := range page {
			if _, exists := result[id]; exists || len(result) >= 8192 {
				t.Fatal("duplicate identity or unbounded native verification fixture")
			}
			result[id] = hex.EncodeToString(digest[:])
		}
		after = ids[len(ids)-1].(string)
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
	inputHash := sha256.New()
	var inputBytes int64
	for _, input := range request.Inputs {
		for _, path := range []string{input.AnalyticsPath, input.PayloadPath} {
			file, err := os.Open(path)
			if err != nil {
				b.Fatal(err)
			}
			info, err := file.Stat()
			if err != nil {
				file.Close()
				b.Fatal(err)
			}
			if err := binary.Write(inputHash, binary.BigEndian, uint64(info.Size())); err != nil {
				file.Close()
				b.Fatal(err)
			}
			count, err := io.Copy(inputHash, file)
			closeErr := file.Close()
			if err != nil || closeErr != nil || count != info.Size() {
				b.Fatalf("hash actual input: bytes=%d err=%v close=%v", count, err, closeErr)
			}
			inputBytes += count
		}
	}
	b.Logf("framed actual input SHA256=%x bytes=%d", inputHash.Sum(nil), inputBytes)
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
