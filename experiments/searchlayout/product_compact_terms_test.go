//go:build duckdb_use_static_lib

package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/klauspost/compress/zstd"
)

type compactPosting struct {
	id, tf uint64
}

// One compressed vocabulary favors storage; queries must decode the whole
// block. It is not a random-access production index.
func measureProductCompactTerms(t *testing.T, stage, single, analytics, payload string, rows int, noisy bool) {
	t.Helper()
	f, err := os.Open(stage)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	lists := make(map[string][]compactPosting)
	for id := range rows {
		var staged engine.StageRecord
		if err := dec.Decode(&staged); err != nil {
			t.Fatal(err)
		}
		perDoc := make(map[string]uint64)
		for _, value := range staged.Record.SearchValues {
			for _, term := range strings.Fields(strings.ToLower(value)) {
				perDoc[term]++
			}
		}
		for term, tf := range perDoc {
			lists[term] = append(lists[term], compactPosting{uint64(id), tf})
		}
	}
	var extra engine.StageRecord
	if err := dec.Decode(&extra); err != io.EOF {
		t.Fatalf("unexpected stage tail: %v", err)
	}
	terms := make([]string, 0, len(lists))
	for term := range lists {
		terms = append(terms, term)
	}
	slices.Sort(terms)
	var raw []byte
	raw = binary.AppendUvarint(raw, uint64(len(terms)))
	prevTerm := ""
	postings := 0
	for _, term := range terms {
		prefix := 0
		for prefix < len(term) && prefix < len(prevTerm) && term[prefix] == prevTerm[prefix] {
			prefix++
		}
		raw = binary.AppendUvarint(raw, uint64(prefix))
		raw = binary.AppendUvarint(raw, uint64(len(term)-prefix))
		raw = append(raw, term[prefix:]...)
		list := lists[term]
		raw = binary.AppendUvarint(raw, uint64(len(list)))
		prior := uint64(0)
		for _, p := range list {
			raw = binary.AppendUvarint(raw, p.id-prior+1)
			raw = binary.AppendUvarint(raw, p.tf)
			prior = p.id + 1
		}
		postings += len(list)
		prevTerm = term
	}
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	packed := encoder.EncodeAll(raw, nil)
	encoder.Close()
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decoder.DecodeAll(packed, nil)
	decoder.Close()
	if err != nil || !bytes.Equal(decoded, raw) {
		t.Fatalf("compressed index roundtrip: %v", err)
	}
	read := func() uint64 {
		v, n := binary.Uvarint(decoded)
		if n <= 0 {
			t.Fatal("truncated varint")
		}
		decoded = decoded[n:]
		return v
	}
	if count := read(); count != uint64(len(terms)) {
		t.Fatalf("term count %d", count)
	}
	prevTerm = ""
	for _, want := range terms {
		prefix, suffix := read(), read()
		if prefix > uint64(len(prevTerm)) || suffix > uint64(len(decoded)) {
			t.Fatal("invalid dictionary entry")
		}
		term := prevTerm[:prefix] + string(decoded[:suffix])
		decoded = decoded[suffix:]
		if term != want || read() != uint64(len(lists[want])) {
			t.Fatalf("term mismatch %q/%q", term, want)
		}
		prior := uint64(0)
		for _, entry := range lists[want] {
			delta, tf := read(), read()
			if delta == 0 || prior+delta-1 != entry.id || tf != entry.tf {
				t.Fatalf("posting mismatch term=%q id=%d", want, entry.id)
			}
			prior = entry.id + 1
		}
		prevTerm = term
	}
	if len(decoded) != 0 {
		t.Fatalf("trailing index bytes: %d", len(decoded))
	}
	for _, term := range []string{"fatal", "request", productParquetTrace(236)} {
		if strings.HasPrefix(term, "trace-") && !noisy {
			continue
		}
		var count, sum int64
		for _, p := range lists[term] {
			if p.id%2 == 0 {
				count++
				sum += int64(p.id % 10)
			}
		}
		var wantCount, wantSum int64
		for id := range rows {
			if id%2 != 0 || !(term == "fatal" && id%97 == 0 || term == "request" && id%97 != 0 || strings.HasPrefix(term, "trace-") && id == 236) {
				continue
			}
			wantCount++
			wantSum += int64(id % 10)
		}
		if count != wantCount || sum != wantSum {
			t.Fatalf("term=%q count/sum=%d/%d want=%d/%d", term, count, sum, wantCount, wantSum)
		}
	}
	stat := func(path string) int64 {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		return info.Size()
	}
	t.Logf("compact_terms rows=%d noisy=%t random_trace_128=%t random_trace_256=%t terms=%d postings=%d raw_bytes=%d zstd_bytes=%d single_plus_index=%d pair_bytes=%d", rows, noisy, os.Getenv("EVENTGLASS_PRODUCT_PARQUET_RANDOM_TRACE_128") == "1", os.Getenv("EVENTGLASS_PRODUCT_PARQUET_RANDOM_TRACE") == "1", len(terms), postings, len(raw), len(packed), stat(single)+int64(len(packed)), stat(analytics)+stat(payload))
}
