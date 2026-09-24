//go:build duckdb_use_static_lib

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/engine"
)

func measureProductUnified(t *testing.T, ctx context.Context, root, stage, analytics, payload string, rows int, noisy bool) {
	t.Helper()
	input, err := os.Open(stage)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	decoder := json.NewDecoder(input)
	docs := make([]universalDoc, 0, rows)
	for len(docs) < rows {
		var staged engine.StageRecord
		if err := decoder.Decode(&staged); err != nil {
			t.Fatal(err)
		}
		if staged.Record.SeverityNumber == nil || staged.Record.Service == nil {
			t.Fatal("fixture lacks fields required by this segment probe")
		}
		canonical, err := json.Marshal(staged)
		if err != nil {
			t.Fatal(err)
		}
		fields := map[string]universalValue{"service": {Kind: "string", S: *staged.Record.Service}}
		for _, attr := range staged.Record.Attrs {
			key := attr.Namespace + attr.Path
			switch attr.ValueType {
			case "integer":
				value, err := strconv.ParseInt(*attr.IntegerValue, 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				fields[key] = universalValue{Kind: "int", I: value}
			case "string":
				fields[key] = universalValue{Kind: "string", S: *attr.StringValue}
			default:
				t.Fatalf("fixture attribute type %q is not in probe", attr.ValueType)
			}
		}
		docs = append(docs, universalDoc{
			ID: len(docs), Tenant: int(staged.Record.TenantID), Project: staged.Record.ProjectID,
			Canonical: canonical, When: staged.Record.EventTimeUS, Live: true,
			Duration: int64(*staged.Record.SeverityNumber), Search: slices.Clone(staged.Record.SearchValues), Fields: fields,
		})
	}
	var extra engine.StageRecord
	if err := decoder.Decode(&extra); err != io.EOF {
		t.Fatalf("stage has more than %d rows: %v", rows, err)
	}
	path := filepath.Join(root, "universal.bin")
	dir, size, err := writeUniversalObject(path, docs)
	if err != nil {
		t.Fatal(err)
	}
	var fieldsBytes, termBytes, sourceBytes, dictionaryBytes int64
	for _, part := range dir.Fields {
		fieldsBytes += part.Size
	}
	for _, part := range dir.Terms {
		termBytes += part.Size
	}
	for _, part := range dir.Sources {
		sourceBytes += part.Size
	}
	for _, block := range dir.FieldBlocks {
		dictionaryBytes += block.Data.Size
	}
	for _, block := range dir.TermBlocks {
		dictionaryBytes += block.Data.Size
	}
	local := localSource{dir: root}
	readDir, err := readUniversalDirectory(ctx, local, size)
	if err != nil || readDir.Count != rows || len(readDir.Sources) != len(dir.Sources) {
		t.Fatalf("universal directory count=%d err=%v", readDir.Count, err)
	}
	seen := 0
	for _, part := range readDir.Sources {
		data, err := readPart(ctx, local, segment{Name: "universal.bin"}, part)
		if err != nil {
			t.Fatal(err)
		}
		var page []universalDoc
		if err := json.Unmarshal(data, &page); err != nil {
			t.Fatal(err)
		}
		for _, doc := range page {
			if doc.ID != seen || !bytes.Equal(doc.Canonical, docs[seen].Canonical) {
				t.Fatalf("canonical source mismatch at id=%d", seen)
			}
			var staged engine.StageRecord
			if err := json.Unmarshal(doc.Canonical, &staged); err != nil {
				t.Fatal(err)
			}
			raw, err := productParquetRaw(seen, noisy)
			if err != nil || !bytes.Equal(staged.Record.Raw, raw) || staged.Record.RecordID != fmt.Sprintf("%064x", seen+1) {
				t.Fatalf("source raw or record ID mismatch at id=%d err=%v", seen, err)
			}
			seen++
		}
	}
	if seen != rows {
		t.Fatalf("roundtrip rows=%d want=%d", seen, rows)
	}
	const epoch = int64(1_700_000_000_000_000)
	scope := universalScope{tenant: 1, project: 10, start: epoch, end: epoch + int64(rows)*1000}
	type queryCase struct {
		name string
		p    universalPredicate
		keep func(int) bool
	}
	cases := []queryCase{
		{"fatal", universalPredicate{op: "term", text: "fatal"}, func(i int) bool { return i%97 == 0 }},
		{"request", universalPredicate{op: "term", text: "request"}, func(i int) bool { return i%97 != 0 }},
		{"regex", universalPredicate{op: "regex", text: "timeout|한글"}, func(i int) bool { return i%97 == 0 }},
		{"dynamic", universalPredicate{op: "gte", path: "attributes/latency", n: 500}, func(i int) bool { return i%1000 >= 500 }},
		{"service", universalPredicate{op: "eq", path: "service", text: "api"}, func(i int) bool { return i%3 == 0 }},
		{"scope_or", universalPredicate{op: "or", children: []universalPredicate{{op: "term", text: "fatal"}, {op: "not", children: []universalPredicate{{op: "term", text: "fatal"}}}}}, func(int) bool { return true }},
	}
	if noisy {
		cases = append(cases,
			queryCase{"sparse", universalPredicate{op: "exists", path: "attributes/custom/236"}, func(i int) bool { return i%1000 == 236 }},
			queryCase{"trace", universalPredicate{op: "term", text: fmt.Sprintf("trace-%064x", 237)}, func(i int) bool { return i == 236 }},
		)
	}
	for _, tc := range cases {
		want := universalAnswer{group: make(map[string]int64)}
		for i := range rows {
			if i%2 != 0 || !tc.keep(i) {
				continue
			}
			severity := int64(i % 10)
			want.count++
			want.sum += severity
			want.group[fmt.Sprintf("string:%s:0", []string{"api", "worker", "sdk"}[i%3])] += severity
			want.top = append(want.top, i)
		}
		slices.SortFunc(want.top, func(a, b int) int {
			if a%10 != b%10 {
				return (b % 10) - (a % 10)
			}
			return a - b
		})
		want.top = want.top[:min(5, len(want.top))]
		got, details, err := runUniversalRangeScoped(ctx, local, size, tc.p, "service", scope)
		if err != nil || !reflect.DeepEqual(got, want) || len(details) != len(want.top) {
			t.Fatalf("query=%s got=%+v want=%+v details=%d err=%v", tc.name, got, want, len(details), err)
		}
		for i, id := range want.top {
			if details[i].ID != id || !bytes.Equal(details[i].Canonical, docs[id].Canonical) {
				t.Fatalf("query=%s detail[%d] mismatch", tc.name, i)
			}
		}
		t.Logf("product_unified query=%s count=%d sum=%d", tc.name, got.count, got.sum)
	}
	analyticsInfo, err := os.Stat(analytics)
	if err != nil {
		t.Fatal(err)
	}
	payloadInfo, err := os.Stat(payload)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("product_unified rows=%d noisy=%t segment_bytes=%d pair_bytes=%d", rows, noisy, size, analyticsInfo.Size()+payloadInfo.Size())
	t.Logf("product_unified_parts core=%d fields=%d postings=%d source=%d dictionary=%d other=%d", dir.Core.Size, fieldsBytes, termBytes, sourceBytes, dictionaryBytes, size-dir.Core.Size-fieldsBytes-termBytes-sourceBytes-dictionaryBytes)
}
