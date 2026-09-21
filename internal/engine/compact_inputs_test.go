package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/model"
)

func compactionInspectionFixture(t *testing.T) CompactionRequest {
	t.Helper()
	root := t.TempDir()
	request := CompactionRequest{Version: CompactionProtocolVersion, TenantID: 1, LaneID: 3, SchemaVersion: 1, GroupingVersion: 1,
		EventDay: "2023-11-14", Kind: model.KindLog, OutputDirectory: filepath.Join(root, "merged"), SpillDirectory: filepath.Join(root, "spill"), NativeMemoryBytes: 64 << 20, NativeSpillBytes: 64 << 20}
	for index := range 2 {
		directory := filepath.Join(root, fmt.Sprint(index))
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		records := []StageRecord{conversionRecord("a", model.KindLog, 1_700_000_000_000_000, 0), conversionRecord("b", model.KindLog, 1_700_000_000_000_001, 1)}
		for ordinal := range records {
			records[ordinal].Record.RecordID = fmt.Sprintf("%064x", index*2+ordinal+1)
		}
		_, err := Convert(context.Background(), ConversionRequest{Version: ConversionProtocolVersion, TenantID: 1, LaneID: 3, BatchSeq: 4, BatchID: records[0].BatchID, SelectedRecords: 2,
			StagePath: writeConversionStage(t, directory, records), OutputDirectory: filepath.Join(directory, "output"), SpillDirectory: filepath.Join(directory, "spill"), NativeMemoryBytes: 64 << 20, NativeSpillBytes: 64 << 20}, func(bundle ConvertedBundle) error {
			request.Inputs = append(request.Inputs, CompactionInput{BundleID: fmt.Sprint(index), AnalyticsPath: bundle.Analytics.Path, PayloadPath: bundle.Payload.Path, IdentitySHA256: bundle.IdentitySHA256})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return request
}

func TestCompactionBatchedInspectionRejectsEachBadInput(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name, projection, suffix string
		payload                  bool
	}{
		{"tenant", "* REPLACE (2::BIGINT AS tenant_id)", "", false},
		{"lane", "* REPLACE (4::INTEGER AS lane_id)", "", false},
		{"kind", "* REPLACE ('error' AS kind)", "", false},
		{"schema", "* REPLACE (2::INTEGER AS schema_version)", "", false},
		{"grouping", "* REPLACE (2::INTEGER AS grouping_version)", "", false},
		{"day-before", "* REPLACE (1699919999999999::BIGINT AS event_time_us)", "", false},
		{"day-end", "* REPLACE (1700006400000000::BIGINT AS event_time_us)", "", false},
		{"project-null", "* REPLACE (NULL::BIGINT AS project_id)", "", false},
		{"project-zero", "* REPLACE (0::BIGINT AS project_id)", "", false},
		{"batch-null", "* REPLACE (NULL::BIGINT AS batch_seq)", "", false},
		{"batch-zero", "* REPLACE (0::BIGINT AS batch_seq)", "", false},
		{"received-null", "* REPLACE (NULL::BIGINT AS received_time_us)", "", false},
		{"invalid-id", "* REPLACE (repeat('g',64) AS record_id)", "", false},
		{"duplicate-id", "* REPLACE (repeat('a',64) AS record_id)", "", false},
		{"payload-duplicate", "* REPLACE (repeat('a',64) AS record_id)", "", true},
		{"analytics-empty", "*", " WHERE false", false},
		{"payload-empty", "*", " WHERE false", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := compactionInspectionFixture(t)
			db, err := Open(ctx, "")
			if err != nil {
				t.Fatal(err)
			}
			for _, setting := range []string{"SET memory_limit='64MiB'", "SET max_temp_directory_size='0B'"} {
				if _, err := db.ExecContext(ctx, setting); err != nil {
					db.Close()
					t.Fatal(err)
				}
			}
			path := filepath.Join(t.TempDir(), "mutated.parquet")
			source := &request.Inputs[1].AnalyticsPath
			if test.payload {
				source = &request.Inputs[1].PayloadPath
			}
			_, err = db.ExecContext(ctx, `COPY (SELECT `+test.projection+` FROM read_parquet(?)`+test.suffix+`) TO '`+quoteSQLString(path)+`' (FORMAT PARQUET,COMPRESSION ZSTD)`, *source)
			closeErr := db.Close()
			if err != nil || closeErr != nil {
				t.Fatalf("mutation: %v close: %v", err, closeErr)
			}
			*source = path
			if _, err := Compact(ctx, request); err == nil {
				t.Fatal("invalid individual input passed batched inspection")
			}
			entries, err := os.ReadDir(request.OutputDirectory)
			if err != nil || len(entries) != 0 {
				t.Fatalf("invalid input wrote outputs: %v %v", entries, err)
			}
		})
	}
}

func TestCompactionBatchedInspectionDoesNotAcceptOnlyUnionIdentity(t *testing.T) {
	request := compactionInspectionFixture(t)
	request.Inputs[0].PayloadPath, request.Inputs[1].PayloadPath = request.Inputs[1].PayloadPath, request.Inputs[0].PayloadPath
	if _, err := Compact(context.Background(), request); err == nil {
		t.Fatal("swapped pairs with identical union were accepted")
	}
	request.Inputs[0].PayloadPath, request.Inputs[1].PayloadPath = request.Inputs[1].PayloadPath, request.Inputs[0].PayloadPath
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Compact(ctx, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	request.Inputs[1].AnalyticsPath = request.Inputs[0].AnalyticsPath
	if _, err := Compact(context.Background(), request); err == nil {
		t.Fatal("same file admitted twice")
	}
}

func TestCompactionInputProvenanceCannotBeShadowed(t *testing.T) {
	for _, analytics := range []bool{true, false} {
		t.Run(fmt.Sprintf("analytics=%t", analytics), func(t *testing.T) {
			request := compactionInspectionFixture(t)
			ctx := context.Background()
			db, err := Open(ctx, "")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for _, setting := range []string{"SET memory_limit='64MiB'", "SET max_temp_directory_size='0B'"} {
				if _, err := db.ExecContext(ctx, setting); err != nil {
					t.Fatal(err)
				}
			}
			paths := []string{filepath.Join(t.TempDir(), "first.parquet"), filepath.Join(t.TempDir(), "second.parquet")}
			for index := range paths {
				source := request.Inputs[1-index].PayloadPath
				if analytics {
					source = request.Inputs[1-index].AnalyticsPath
				}
				// Swap the real record sets but forge filename to identify the
				// other path: trusting file data would incorrectly accept both.
				if _, err := db.ExecContext(ctx, `COPY (SELECT *, ?::VARCHAR AS filename FROM read_parquet(?)) TO '`+quoteSQLString(paths[index])+`' (FORMAT PARQUET,COMPRESSION ZSTD)`, paths[1-index], source); err != nil {
					t.Fatal(err)
				}
			}
			for index := range paths {
				if analytics {
					request.Inputs[index].AnalyticsPath = paths[index]
				} else {
					request.Inputs[index].PayloadPath = paths[index]
				}
			}
			if _, err := Compact(ctx, request); err == nil {
				t.Fatal("file contents spoofed scan provenance")
			}
		})
	}
}

// Sparse files exercise admission byte arithmetic only. They are deliberately
// not Parquet and are never evidence of a maximum-byte native rewrite.
func TestCompactionInputByteBounds(t *testing.T) {
	for _, test := range []struct {
		name  string
		sizes [4]int64
		valid bool
	}{
		{"exact-total", [4]int64{64 << 20, 64 << 20, 64 << 20, 64 << 20}, true},
		{"one-file-over", [4]int64{(128 << 20) + 1, 1, 1, 1}, false},
		{"total-over", [4]int64{128 << 20, 64 << 20, 64 << 20, 1}, false},
		{"empty-file", [4]int64{1, 1, 1, 0}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			paths := make([]string, len(test.sizes))
			for index, size := range test.sizes {
				paths[index] = filepath.Join(root, fmt.Sprint(index))
				file, err := os.Create(paths[index])
				if err != nil {
					t.Fatal(err)
				}
				err = file.Truncate(size)
				closeErr := file.Close()
				if err != nil || closeErr != nil {
					t.Fatalf("sparse fixture: %v %v", err, closeErr)
				}
			}
			request := CompactionRequest{Version: CompactionProtocolVersion, TenantID: 1, LaneID: 3, SchemaVersion: 1, GroupingVersion: 1, EventDay: "2023-11-14", Kind: model.KindLog,
				OutputDirectory: filepath.Join(root, "output"), SpillDirectory: filepath.Join(root, "spill"), NativeMemoryBytes: 64 << 20, NativeSpillBytes: 64 << 20,
				Inputs: []CompactionInput{{BundleID: "a", AnalyticsPath: paths[0], PayloadPath: paths[1], IdentitySHA256: strings.Repeat("a", 64)}, {BundleID: "b", AnalyticsPath: paths[2], PayloadPath: paths[3], IdentitySHA256: strings.Repeat("b", 64)}}}
			if err := validateCompactionRequest(request); (err == nil) != test.valid {
				t.Fatalf("valid=%t err=%v", test.valid, err)
			}
		})
	}
}
