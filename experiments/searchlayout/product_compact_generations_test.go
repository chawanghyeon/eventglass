//go:build duckdb_use_static_lib

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

type compactGenerationPart struct {
	base int
	rows int
	data []byte
}

func compactGenerationBuild(t *testing.T, records []engine.StageRecord, base int) compactGenerationPart {
	t.Helper()
	var input bytes.Buffer
	encoder := json.NewEncoder(&input)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			t.Fatal(err)
		}
	}
	built := buildCompact(t, &input, len(records), true)
	return compactGenerationPart{base: base, rows: len(records), data: built.combined}
}

func compactGenerationAnswer(t *testing.T, parts []compactGenerationPart, term, filter string) map[string]productParquetOracle {
	t.Helper()
	got := make(map[string]productParquetOracle)
	for _, part := range parts {
		core, index, err := compactDecodeCore(part.data, part.rows, true)
		if err != nil {
			t.Fatal(err)
		}
		found, err := compactLookup(index, term)
		if err != nil {
			t.Fatal(err)
		}
		for _, posting := range found {
			if posting.id >= uint64(len(core)) {
				t.Fatalf("posting outside segment at base=%d", part.base)
			}
			row := core[posting.id]
			if row.project != 10 {
				continue
			}
			if filter != "" {
				match, err := compactDynamicMatch(row.attrs, filter)
				if err != nil {
					t.Fatal(err)
				}
				if !match {
					continue
				}
			}
			value := got[row.service]
			value.count++
			value.sum += row.severity
			got[row.service] = value
		}
	}
	return got
}

func compactGenerationOracle(t *testing.T, records []engine.StageRecord, term, filter string) map[string]productParquetOracle {
	t.Helper()
	want := make(map[string]productParquetOracle)
	for _, staged := range records {
		record := staged.Record
		if record.ProjectID != 10 {
			continue
		}
		matches := false
		for _, value := range record.SearchValues {
			for _, token := range strings.Fields(strings.ToLower(value)) {
				matches = matches || token == term
			}
		}
		if !matches {
			continue
		}
		if filter != "" {
			match := false
			for _, attr := range record.Attrs {
				if attr.Namespace != "attributes" {
					continue
				}
				if filter == "sparse" && attr.Path == "/custom/236" {
					match = true
				}
				if filter == "latency" && attr.Path == "/latency" && attr.ValueType == "integer" && attr.IntegerValue != nil {
					n, err := strconv.ParseInt(*attr.IntegerValue, 10, 64)
					if err != nil {
						t.Fatal(err)
					}
					match = n >= 500
				}
			}
			if !match {
				continue
			}
		}
		value := want[*record.Service]
		value.count++
		value.sum += int64(*record.SeverityNumber)
		want[*record.Service] = value
	}
	return want
}

func TestProductCompactSplitMergePinnedGeneration(t *testing.T) {
	rows := 10000
	if os.Getenv("EVENTGLASS_PRODUCT_COMPACT_GENERATIONS_ROWS") == "100000" {
		rows = 100000
	}
	stage := filepath.Join(t.TempDir(), "staged.jsonl")
	productParquetFixture(t, stage, rows, true)
	input, err := os.Open(stage)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(input)
	records := make([]engine.StageRecord, rows)
	for i := range records {
		if err := decoder.Decode(&records[i]); err != nil {
			t.Fatal(err)
		}
	}
	var extra engine.StageRecord
	if err := decoder.Decode(&extra); err != io.EOF {
		t.Fatalf("unexpected stage tail: %v", err)
	}
	if err := input.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(stage); err != nil {
		t.Fatal(err)
	}
	var split []compactGenerationPart
	for base := 0; base < rows; base += rows / 4 {
		split = append(split, compactGenerationBuild(t, records[base:base+rows/4], base))
	}
	merged := []compactGenerationPart{compactGenerationBuild(t, records, 0)}
	cases := []struct{ term, filter string }{
		{"fatal", ""}, {"request", ""}, {"request", "latency"},
		{"request", "sparse"}, {productParquetTrace(236), ""}, {"missing", ""},
	}
	for _, tc := range cases {
		want := compactGenerationOracle(t, records, tc.term, tc.filter)
		for _, generation := range [][]compactGenerationPart{split, merged} {
			if got := compactGenerationAnswer(t, generation, tc.term, tc.filter); !reflect.DeepEqual(got, want) {
				t.Fatalf("segments=%d term=%q filter=%q got=%v want=%v", len(generation), tc.term, tc.filter, got, want)
			}
		}
	}
	updated := make([]engine.StageRecord, len(records))
	copy(updated, records)
	updated[236].Record.SearchValues = append(append([]string(nil), records[236].Record.SearchValues...), "revision")
	latest := append([]compactGenerationPart(nil), split...)
	latest[0] = compactGenerationBuild(t, updated[:rows/4], 0)
	for _, tc := range []struct {
		generation []compactGenerationPart
		records    []engine.StageRecord
	}{{latest, updated}, {split, records}} {
		want := compactGenerationOracle(t, tc.records, "revision", "")
		if got := compactGenerationAnswer(t, tc.generation, "revision", ""); !reflect.DeepEqual(got, want) {
			t.Fatalf("pinned generation got=%v want=%v", got, want)
		}
	}
	var splitBytes, mergedBytes int
	for _, part := range split {
		splitBytes += len(part.data)
	}
	for _, part := range merged {
		mergedBytes += len(part.data)
	}
	t.Logf("compact_generation rows=%d split=4 merged=1 queries=%d split_index_bytes=%d merged_index_bytes=%d pinned_old_generation=1", rows, len(cases), splitBytes, mergedBytes)
	if os.Getenv("EVENTGLASS_PRODUCT_COMPACT_GENERATIONS_MINIO") == "1" {
		queries := []struct{ term, filter string }{{"request", ""}, {"request", "latency"}, {productParquetTrace(236), ""}}
		wants := make([]map[string]productParquetOracle, len(queries))
		for i, query := range queries {
			wants[i] = compactGenerationOracle(t, records, query.term, query.filter)
		}
		records, updated = nil, nil
		runtime.GC()
		compactGenerationMinIO(t, split, merged, queries, wants)
	}
}

func compactGenerationMinIO(t *testing.T, split, merged []compactGenerationPart, queries []struct{ term, filter string }, wants []map[string]productParquetOracle) {
	t.Helper()
	ctx := context.Background()
	endpoint, bucket := os.Getenv("EVENTGLASS_PRODUCT_MINIO_ENDPOINT"), os.Getenv("EVENTGLASS_PRODUCT_MINIO_BUCKET")
	if endpoint == "" || bucket == "" {
		t.Fatal("EVENTGLASS_PRODUCT_MINIO_ENDPOINT and EVENTGLASS_PRODUCT_MINIO_BUCKET are required")
	}
	store, err := storage.NewS3Store(ctx, storage.S3Config{
		Endpoint: endpoint, Region: "us-east-1", Bucket: bucket,
		Prefix:      fmt.Sprintf("searchlayout-generations-%d", time.Now().UnixNano()),
		AccessKeyID: os.Getenv("AWS_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"), PathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	put := func(key string, data []byte) {
		t.Helper()
		hash := fmt.Sprintf("%x", sha256.Sum256(data))
		if _, err := store.PutStream(ctx, key, bytes.NewReader(data), int64(len(data)), hash); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
	}
	for i, part := range split {
		put(fmt.Sprintf("split-%d", i), part.data)
	}
	put("merged", merged[0].data)
	var splitBytes int
	for _, part := range split {
		splitBytes += len(part.data)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := store.Delete(cleanupCtx, keys); err != nil {
			t.Error(err)
		}
	})
	upload := store.OperationCounts()
	t.Logf("compact_generation_minio_upload put=%d put_bytes=%d head=%d verify_get=%d verify_bytes=%d", upload.PutRequests, upload.PutBytes, upload.HeadRequests, upload.FullGetRequests, upload.FullGetBytes)
	cpuUS := func() int64 {
		var usage syscall.Rusage
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
			t.Fatal(err)
		}
		return usage.Utime.Sec*1_000_000 + usage.Utime.Usec + usage.Stime.Sec*1_000_000 + usage.Stime.Usec
	}
	for queryIndex, query := range queries {
		want := wants[queryIndex]
		var wall [2][]int64
		var cpu [2][]int64
		for i := range 30 {
			for turn := range 2 {
				choice := (i + turn) % 2
				parts := split
				partKeys := keys[:len(split)]
				if choice == 1 {
					parts, partKeys = merged, keys[len(split):]
				}
				startCPU, start := cpuUS(), time.Now()
				fetched := make([]compactGenerationPart, len(parts))
				for j, part := range parts {
					data, err := store.ReadRange(ctx, partKeys[j], 0, int64(len(part.data)))
					if err != nil {
						t.Fatal(err)
					}
					fetched[j] = compactGenerationPart{base: part.base, rows: part.rows, data: data}
				}
				got := compactGenerationAnswer(t, fetched, query.term, query.filter)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("MinIO segments=%d term=%q filter=%q got=%v want=%v", len(parts), query.term, query.filter, got, want)
				}
				wall[choice] = append(wall[choice], time.Since(start).Microseconds())
				cpu[choice] = append(cpu[choice], cpuUS()-startCPU)
			}
		}
		for choice := range 2 {
			slices.Sort(wall[choice])
			slices.Sort(cpu[choice])
			t.Logf("compact_generation_minio term=%q filter=%q segments=%d reps=30 p50_us=%d p95_us=%d p99_us=%d cpu_p50_us=%d cpu_p95_us=%d gets_per_query=%d bytes_per_query=%d", query.term, query.filter, []int{len(split), len(merged)}[choice], wall[choice][15], wall[choice][28], wall[choice][29], cpu[choice][15], cpu[choice][28], []int{len(split), len(merged)}[choice], []int{splitBytes, len(merged[0].data)}[choice])
		}
	}
	after := store.OperationCounts()
	expectedGets := uint64(30 * len(queries) * (len(split) + len(merged)))
	if got := after.RangeRequests - upload.RangeRequests; got != expectedGets {
		t.Fatalf("range GETs=%d want=%d", got, expectedGets)
	}
	expectedBytes := uint64(30 * len(queries) * (splitBytes + len(merged[0].data)))
	if got := after.RangeBytes - upload.RangeBytes; got != expectedBytes {
		t.Fatalf("range bytes=%d want=%d", got, expectedBytes)
	}
	t.Logf("compact_generation_minio_query range_get=%d range_bytes=%d", after.RangeRequests-upload.RangeRequests, after.RangeBytes-upload.RangeBytes)
}
