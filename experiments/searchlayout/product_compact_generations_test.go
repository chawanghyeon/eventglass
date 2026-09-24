//go:build duckdb_use_static_lib

package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/engine"
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
	defer input.Close()
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
}
