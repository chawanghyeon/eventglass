package main

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"testing"
)

type universalGenerationPart struct {
	dir  string
	base int
	size int64
}

func writeUniversalGenerationPart(t *testing.T, docs []universalDoc, base int) universalGenerationPart {
	t.Helper()
	dir := t.TempDir()
	local := slices.Clone(docs)
	for i := range local {
		local[i].ID = i
	}
	_, size, err := writeUniversalObject(filepath.Join(dir, "universal.bin"), local)
	if err != nil {
		t.Fatal(err)
	}
	return universalGenerationPart{dir: dir, base: base, size: size}
}

func checkUniversalGeneration(t *testing.T, parts []universalGenerationPart, docs []universalDoc, cases []universalPredicate) {
	t.Helper()
	for _, p := range cases {
		for _, groupPath := range []string{"tags/region", "attrs/note", "attrs/custom/237"} {
			got := universalAnswer{group: make(map[string]int64)}
			var details []universalDoc
			for _, part := range parts {
				a, found, err := runUniversalRange(context.Background(), localSource{dir: part.dir}, part.size, p, groupPath)
				if err != nil {
					t.Fatal(err)
				}
				got.count += a.count
				got.sum += a.sum
				for key, sum := range a.group {
					got.group[key] += sum
				}
				for _, d := range found {
					d.ID += part.base
					details = append(details, d)
				}
			}
			sort.Slice(details, func(i, j int) bool {
				if details[i].Duration != details[j].Duration {
					return details[i].Duration > details[j].Duration
				}
				return details[i].ID < details[j].ID
			})
			details = details[:min(5, len(details))]
			for _, d := range details {
				got.top = append(got.top, d.ID)
			}
			want, wantDetails := oracleUniversalRange(docs, p, groupPath)
			if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(details, wantDetails) {
				t.Fatalf("generation op=%s group=%s got=%+v want=%+v", p.op, groupPath, got, want)
			}
		}
	}
}

func TestUniversalGenerationMergeAndPinnedRead(t *testing.T) {
	docs := universalFixture(10000)
	cases := append(universalCases(), universalPredicate{op: "term", text: "trace1237"})
	old := []universalGenerationPart{
		writeUniversalGenerationPart(t, docs[:5000], 0),
		writeUniversalGenerationPart(t, docs[5000:], 5000),
	}
	checkUniversalGeneration(t, old, docs, cases)
	merged := writeUniversalGenerationPart(t, docs, 0)
	checkUniversalGeneration(t, []universalGenerationPart{merged}, docs, cases)
	updated := slices.Clone(docs)
	updated[1237].Live = false
	newGeneration := writeUniversalGenerationPart(t, updated, 0)
	checkUniversalGeneration(t, []universalGenerationPart{newGeneration}, updated, cases)
	checkUniversalGeneration(t, old, docs, cases)
	for _, part := range old {
		if _, err := os.Stat(filepath.Join(part.dir, "universal.bin")); err != nil {
			t.Fatal("pinned generation was removed", err)
		}
	}
	t.Logf("split_segments=%d merged_segments=1 old_snapshot_kept=1 cases=%d groups=3", len(old), len(cases))
}
