package main

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestQueryTermsNormalizeOnce(t *testing.T) {
	terms, err := normalizeQueryTerms([]string{"ALPHA", "alpha", "한글"})
	if err != nil || !slices.Equal(terms, []string{"alpha", "한글"}) {
		t.Fatalf("normalized terms = %v, %v", terms, err)
	}
}

func TestRandomizedLayoutsAgainstFullScan(t *testing.T) {
	words := []string{"alpha", "beta", "request", "한글", "café", "trace-42"}
	for seed := int64(0); seed < 20; seed++ {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			docs := make([]document, 30+rng.Intn(90))
			for i := range docs {
				terms := make([]string, 1+rng.Intn(8))
				for j := range terms {
					terms[j] = words[rng.Intn(len(words))]
				}
				docs[i] = document{Tenant: uint8(rng.Intn(4)), Group: uint16(rng.Intn(65536)),
					Duration: uint16(rng.Intn(65536)), Text: strings.Join(terms, " ")}
			}
			root := t.TempDir()
			segmentDocs, pageDocs := 7+rng.Intn(25), 1+rng.Intn(19)
			baseDir := filepath.Join(root, "shared")
			if _, err := build(baseDir, docs, segmentDocs, pageDocs); err != nil {
				t.Fatal(err)
			}
			base, err := load(baseDir)
			if err != nil {
				t.Fatal(err)
			}
			packedDir := filepath.Join(root, "packed")
			if _, err := buildPacked(packedDir, docs, segmentDocs, pageDocs); err != nil {
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
			raw := filepath.Join(root, "raw.jsonl.z")
			if _, err := writeRaw(raw, docs); err != nil {
				t.Fatal(err)
			}
			queries := []query{
				{Terms: []string{"alpha"}, Tenant: -1, K: 1},
				{Terms: []string{"ALPHA"}, Tenant: -1, K: 10},
				{Terms: []string{"alpha", "ALPHA"}, All: true, Tenant: -1, K: 10},
				{Terms: []string{"한글", "café"}, Tenant: 2, K: 10},
				{Terms: []string{"request", "trace-42"}, All: true, Tenant: 3, K: 0},
				{Terms: []string{"missing", "beta"}, Tenant: -1, K: 10},
				{Terms: []string{"missing", "alpha"}, All: true, Tenant: -1, K: 10},
			}
			for i := 0; i < 12; i++ {
				queries = append(queries, query{Terms: []string{words[rng.Intn(len(words))], words[rng.Intn(len(words))]},
					All: rng.Intn(2) == 0, Tenant: rng.Intn(6) - 1, K: []int{0, 1, 7}[rng.Intn(3)]})
			}
			for _, q := range queries {
				want, err := scanRaw(raw, base, q)
				if err != nil {
					t.Fatal(err)
				}
				for _, candidate := range []struct {
					name string
					get  func() (result, error)
				}{
					{"shared", func() (result, error) {
						return run(context.Background(), localSource{dir: baseDir}, base, q, denseColumns)
					}},
					{"packed", func() (result, error) {
						return run(context.Background(), localSource{dir: packedDir}, packed, q, denseColumns)
					}},
					{"covering", func() (result, error) {
						return run(context.Background(), localSource{dir: coverDir}, cover, q, denseColumns)
					}},
					{"row", func() (result, error) { return runRowScan(context.Background(), localSource{dir: rowDir}, row, q) }},
				} {
					got, err := candidate.get()
					if err != nil || !equivalent(got, want) {
						t.Fatalf("%s query %+v: %v equivalent=%v", candidate.name, q, err, equivalent(got, want))
					}
				}
			}
		})
	}
}

func TestConcurrentPackedQueries(t *testing.T) {
	docs := generated(1000, 100)
	dir := t.TempDir()
	if _, err := buildPacked(dir, docs, 101, 17); err != nil {
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
	q := query{Terms: []string{"request"}, Tenant: 3, K: 10}
	want, err := scanRaw(raw, c, q)
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 8)
	for range 8 {
		go func() {
			got, err := run(context.Background(), localSource{dir: dir}, c, q, denseColumns)
			if err == nil && !equivalent(got, want) {
				err = fmt.Errorf("concurrent result differs from full scan")
			}
			finished <- err
		}()
	}
	for range 8 {
		if err := <-finished; err != nil {
			t.Fatal(err)
		}
	}
}
