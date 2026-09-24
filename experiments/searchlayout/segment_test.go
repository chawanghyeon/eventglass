package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type handlerTransport struct{ handler http.Handler }

func (t handlerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	recorder := httptest.NewRecorder()
	t.handler.ServeHTTP(recorder, req)
	return recorder.Result(), nil
}

func TestQueryMatchesFullScan(t *testing.T) {
	dir := t.TempDir()
	docs := generated(20_000, 257)
	if _, err := build(dir, docs, 2048, 256); err != nil {
		t.Fatal(err)
	}
	c, err := load(dir)
	if err != nil {
		t.Fatal(err)
	}
	raw := filepath.Join(dir, "raw.jsonl.z")
	if _, err := writeRaw(raw, docs); err != nil {
		t.Fatal(err)
	}
	queries := []query{
		{Terms: []string{"fatal"}, Tenant: -1, K: 10},
		{Terms: []string{"timeout"}, Tenant: -1, K: 10},
		{Terms: []string{"request"}, Tenant: -1, K: 10},
		{Terms: []string{"fatal", "timeout"}, Tenant: -1, K: 10},
		{Terms: []string{"tag7"}, Tenant: -1, K: 10},
		{Terms: []string{"timeout", "tag7"}, All: true, Tenant: -1, K: 10},
		{Terms: []string{"service", "request"}, All: true, Tenant: 3, K: 10},
		{Terms: []string{"missing"}, Tenant: -1, K: 10},
		{Terms: []string{"request"}, Tenant: 2, K: 0},
	}
	for _, q := range queries {
		want, err := scanRaw(raw, c, q)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range []mode{sparse, denseColumns, densePayload, coalesced2, coalesced4} {
			got, err := run(context.Background(), localSource{dir: dir}, c, q, m)
			if err != nil {
				t.Fatalf("%+v mode %d: %v", q, m, err)
			}
			if !equivalent(got, want) {
				t.Fatalf("%+v mode %d: result differs from full scan", q, m)
			}
		}
	}
}

func TestRangeHTTPAndCorruption(t *testing.T) {
	dir := t.TempDir()
	docs := generated(5000, 257)
	if _, err := build(dir, docs, 1000, 128); err != nil {
		t.Fatal(err)
	}
	c, err := load(dir)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := fileSHA256(filepath.Join(dir, c.Segs[0].Name))
	if err != nil || hash != c.Segs[0].SHA256 {
		t.Fatal("segment SHA-256 mismatch before corruption")
	}
	client := &http.Client{Transport: handlerTransport{handler: http.FileServer(http.Dir(dir))}}
	q := query{Terms: []string{"timeout"}, Tenant: -1, K: 10}
	want, err := run(context.Background(), localSource{dir: dir}, c, q, densePayload)
	if err != nil {
		t.Fatal(err)
	}
	src := &measuredSource{src: httpRangeSource{base: "http://local", client: client}}
	got, err := run(context.Background(), src, c, q, densePayload)
	if err != nil {
		t.Fatal(err)
	}
	if !equivalent(got, want) || src.calls == 0 {
		t.Fatal("HTTP Range query differed from local query")
	}
	p := c.Segs[0].Postings["request"]
	f, err := os.OpenFile(filepath.Join(dir, c.Segs[0].Name), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	one := []byte{0}
	if _, err := f.ReadAt(one, p.Offset); err != nil {
		t.Fatal(err)
	}
	one[0] ^= 1
	if _, err := f.WriteAt(one, p.Offset); err != nil {
		t.Fatal(err)
	}
	f.Close()
	_, err = run(context.Background(), localSource{dir: dir}, c, query{Terms: []string{"request"}, Tenant: -1, K: 10}, sparse)
	if err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("corrupt range was not rejected: %v", err)
	}
}

func TestCatalogRejectsTruncation(t *testing.T) {
	dir := t.TempDir()
	if _, err := build(dir, generated(1050, 17), 1000, 256); err != nil {
		t.Fatal(err)
	}
	c, err := load(dir)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := marshalCorpus(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, cut := range []int{0, 5, 20, len(raw) / 2, len(raw) - 1} {
		if _, err := unmarshalCorpus(raw[:cut]); err == nil {
			t.Fatalf("truncated catalog accepted at %d", cut)
		}
	}
}

func TestCancelledQuery(t *testing.T) {
	dir := t.TempDir()
	if _, err := build(dir, generated(1000, 17), 500, 128); err != nil {
		t.Fatal(err)
	}
	c, err := load(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = run(ctx, localSource{dir: dir}, c, query{Terms: []string{"request"}, Tenant: -1, K: 10}, densePayload)
	if err == nil {
		t.Fatal("cancelled query succeeded")
	}
}

func TestPersistedAlternativeLayoutsMatchFullScan(t *testing.T) {
	root := t.TempDir()
	docs := generated(12_000, 997)
	baseDir := filepath.Join(root, "shared")
	if _, err := build(baseDir, docs, 2000, 256); err != nil {
		t.Fatal(err)
	}
	base, err := load(baseDir)
	if err != nil {
		t.Fatal(err)
	}
	packedDir := filepath.Join(root, "packed")
	if _, err := buildPacked(packedDir, docs, 2000, 256); err != nil {
		t.Fatal(err)
	}
	packed, err := load(packedDir)
	if err != nil {
		t.Fatal(err)
	}
	coverDir := filepath.Join(root, "covering")
	cover, err := buildCovering(coverDir, docs, base)
	if err != nil {
		t.Fatal(err)
	}
	rowDir := filepath.Join(root, "row")
	row, err := buildRowOnly(rowDir, baseDir, base)
	if err != nil {
		t.Fatal(err)
	}
	for _, layout := range []struct {
		dir string
		c   corpus
	}{{baseDir, base}, {packedDir, packed}, {coverDir, cover}, {rowDir, row}} {
		if size, err := storedSize(layout.dir, layout.c); err != nil || size == 0 {
			t.Fatalf("missing persisted layout: %d %v", size, err)
		}
	}
	raw := filepath.Join(root, "raw.jsonl.z")
	if _, err := writeRaw(raw, docs); err != nil {
		t.Fatal(err)
	}
	for _, q := range []query{
		{Terms: []string{"fatal"}, Tenant: -1, K: 10},
		{Terms: []string{"timeout"}, Tenant: -1, K: 10},
		{Terms: []string{"request"}, Tenant: -1, K: 10},
		{Terms: []string{"fatal", "timeout"}, Tenant: -1, K: 10},
		{Terms: []string{"tag7"}, Tenant: -1, K: 10},
		{Terms: []string{"timeout", "tag7"}, All: true, Tenant: -1, K: 10},
		{Terms: []string{"service", "request"}, All: true, Tenant: 3, K: 10},
		{Terms: []string{"missing"}, Tenant: -1, K: 10},
	} {
		want, err := scanRaw(raw, base, q)
		if err != nil {
			t.Fatal(err)
		}
		covered, err := run(context.Background(), localSource{dir: coverDir}, cover, q, denseColumns)
		if err != nil || !equivalent(covered, want) {
			t.Fatalf("covering query %+v: %v equivalent=%v", q, err, equivalent(covered, want))
		}
		bitpacked, err := run(context.Background(), localSource{dir: packedDir}, packed, q, denseColumns)
		if err != nil || !equivalent(bitpacked, want) {
			t.Fatalf("packed query %+v: %v equivalent=%v", q, err, equivalent(bitpacked, want))
		}
		scanned, err := runRowScan(context.Background(), localSource{dir: rowDir}, row, q)
		if err != nil || !equivalent(scanned, want) {
			t.Fatalf("row query %+v: %v equivalent=%v", q, err, equivalent(scanned, want))
		}
	}
}

func TestPackedColumnsRoundTripBounds(t *testing.T) {
	docs := []document{
		{Tenant: 0, Group: 0, Duration: 65535, Text: "one"},
		{Tenant: 255, Group: 65535, Duration: 0, Text: strings.Repeat("x ", 254) + "x"},
	}
	got, err := decodePackedColumns(encodePackedColumns(docs))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 12 || got[0] != 0 || got[5] != 1 || got[6] != 255 || got[11] != 255 {
		t.Fatalf("incorrect packed columns: %v", got)
	}
	if _, err := decodePackedColumns([]byte{0}); err == nil {
		t.Fatal("accepted truncated packed page")
	}
}
