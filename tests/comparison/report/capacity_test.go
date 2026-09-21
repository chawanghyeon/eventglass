package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func capacityFixture(workers int, elapsed float64) map[string]any {
	return map[string]any{
		"Kind": "fixed-work-publication-v1", "Architecture": "linux/arm64", "Revision": "fixture-revision",
		"Complete": true, "Cycles": float64(128), "Workers": float64(workers), "Accepted": float64(13440), "PublishedRecords": float64(13440),
		"PreloadedJobs": float64(768), "DrainElapsedMS": elapsed, "RecordsPerSecond": 13440000 / elapsed,
		"Duplicates": float64(0), "Conflicts": float64(0),
		"FixtureSHA256":      strings.Repeat("a", 64),
		"PublishedByProject": []any{map[string]any{"log": float64(3200), "error": float64(160)}, map[string]any{"log": float64(3200), "error": float64(160)}, map[string]any{"log": float64(3200), "error": float64(160)}, map[string]any{"log": float64(3200), "error": float64(160)}},
		"Progress":           []any{map[string]any{"ElapsedMS": float64(0), "Published": float64(0), "Backlog": float64(768)}, map[string]any{"ElapsedMS": elapsed, "Published": float64(13440), "Backlog": float64(0)}},
		"SubmittedInput":     map[string]any{"Envelopes": float64(768), "Bytes": float64(100000), "SHA256": strings.Repeat("b", 64)},
		"Targets":            map[string]any{"resource_evidence_complete": true, "no_cgroup_oom": true, "go_units_within_512mib": true, "capacity_evidence_complete": true},
	}
}

func TestCapacityEvidenceRejectsPartialOrDifferentWork(t *testing.T) {
	if !validCapacity(capacityFixture(1, 10000)) {
		t.Fatal("valid complete fixed-work fixture rejected")
	}
	for name, mutate := range map[string]func(map[string]any){
		"unpublished":         func(r map[string]any) { r["PublishedRecords"] = float64(13439) },
		"batched differently": func(r map[string]any) { r["PreloadedJobs"] = float64(767) },
		"no timer":            func(r map[string]any) { r["DrainElapsedMS"] = float64(0) },
		"invented rate":       func(r map[string]any) { r["RecordsPerSecond"] = float64(999) },
		"missing project":     func(r map[string]any) { r["PublishedByProject"] = r["PublishedByProject"].([]any)[:3] },
		"wrong project count": func(r map[string]any) { r["PublishedByProject"].([]any)[0].(map[string]any)["error"] = float64(159) },
		"already progressing": func(r map[string]any) { r["Progress"].([]any)[0].(map[string]any)["Published"] = float64(1) },
		"undrained":           func(r map[string]any) { r["Progress"].([]any)[1].(map[string]any)["Backlog"] = float64(1) },
		"no input hash":       func(r map[string]any) { r["SubmittedInput"].(map[string]any)["SHA256"] = "" },
		"nonhex definition":   func(r map[string]any) { r["FixtureSHA256"] = strings.Repeat("z", 64) },
		"invalid workers":     func(r map[string]any) { r["Workers"] = float64(3) },
		"conflicting input":   func(r map[string]any) { r["Conflicts"] = float64(1) },
	} {
		t.Run(name, func(t *testing.T) {
			r := capacityFixture(1, 10000)
			mutate(r)
			if validCapacity(r) {
				t.Fatal("incomplete/different capacity evidence accepted")
			}
		})
	}
}

func TestCapacityResourcePolicyDoesNotRelaxSustainedIncarnations(t *testing.T) {
	r := resourceReport{PeakByContainerBytes: map[string]int64{}, ObservedCgroupPeakBytes: map[string]int64{}, OOMObserved: map[string]bool{}, CgroupIncarnations: map[string]int{}}
	for _, role := range []string{"api", "scheduler", "postgres-1", "minio-1", "worker-1"} {
		name := "test-" + role
		r.PeakByContainerBytes[name] = 1
		r.ObservedCgroupPeakBytes[name] = 1
		r.OOMObserved[name] = true
		r.CgroupIncarnations[name] = 1
	}
	if !resourceEvidenceForProfile(r, 1, false) || resourceEvidenceComplete(r, 1) {
		t.Fatal("capacity/sustained incarnation policies confused")
	}
	r.CgroupIncarnations["test-worker-1"] = 3
	if resourceEvidenceForProfile(r, 1, false) || !resourceEvidenceComplete(r, 1) {
		t.Fatal("unexpected capacity replacement accepted")
	}
	delete(r.PeakByContainerBytes, "test-minio-1")
	if resourceEvidenceForProfile(r, 1, false) || resourceEvidenceComplete(r, 1) {
		t.Fatal("shared provider resource omission accepted")
	}
}

func TestCapacityMatrixReportsMeasuredEfficiencyNotAssumedLinearity(t *testing.T) {
	dir := t.TempDir()
	list := filepath.Join(dir, "reports.txt")
	out := filepath.Join(dir, "matrix.json")
	var paths strings.Builder
	for _, workers := range []int{1, 2, 4} {
		for sample := range 3 {
			elapsed := float64(10000)
			if workers == 2 {
				elapsed = 6000
			}
			if workers == 4 {
				elapsed = 5000
			}
			path := filepath.Join(dir, fmt.Sprintf("%d-%d.json", workers, sample))
			writeCapacityObject(path, capacityFixture(workers, elapsed))
			fmt.Fprintln(&paths, path)
		}
	}
	if err := os.WriteFile(list, []byte(paths.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	capacityMatrixMain([]string{list, out})
	r := readObject(out)
	profiles := r["Profiles"].([]any)
	last := profiles[2].(map[string]any)
	if r["Diagnostic"] != false || last["Speedup"] != float64(2) || last["Efficiency"] != float64(.5) || r["ThroughputGrewAtFour"] != true {
		t.Fatalf("incorrect efficiency: %+v", r)
	}
	if _, ok := r["Cost"]; ok {
		t.Fatal("fixed-work drain invented a steady-state monthly cost")
	}
}
