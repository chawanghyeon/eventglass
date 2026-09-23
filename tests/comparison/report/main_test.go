package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPricingFixtureAndMonthlyCostIncludeSharedServices(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "pricing.json"))
	if err != nil {
		t.Fatal(err)
	}
	var price prices
	if err := json.Unmarshal(data, &price); err != nil {
		t.Fatal(err)
	}
	if price.AsOf != "2026-09-21" || price.FargateCPU <= 0 || price.FargateMemory <= 0 || price.RDSHour <= 0 || price.RDSStorage <= 0 || price.RDSBackup <= 0 || price.S3Storage <= 0 || price.S3PUT <= 0 || price.S3LIST <= 0 || price.S3GET <= 0 || len(price.Sources) != 3 {
		t.Fatalf("incomplete price fixture: %#v", price)
	}
	report := map[string]any{
		"WarmupSeconds": float64(300), "LoadSeconds": float64(1800), "Workers": float64(2),
		"PGDatabaseStartBytes": float64(1 << 30), "PGDatabaseEndBytes": float64(2 << 30), "PGWALBytes": float64(1 << 30),
		"S3StoredBytes": float64(3 << 30), "S3PutRequests": float64(1000), "S3ListRequests": float64(500), "S3GetRequests": float64(2000),
		"S3HeadRequests": float64(3000), "S3RangeRequests": float64(4000),
	}
	cost := calculateCost(report, price)
	if cost.FargateTasks != 4 || cost.ProjectedPGStorageGB < price.RDSMinGB || cost.ProjectedS3StorageGB <= 3 || cost.ProjectedS3PUT <= 1000 || cost.ProjectedS3LIST <= 500 || cost.ProjectedS3GET <= 9000 || cost.FargateUSD <= 0 || cost.RDSComputeUSD <= 0 || cost.RDSStorageUSD <= 0 || cost.S3StorageUSD <= 0 || cost.S3RequestsUSD <= 0 || cost.TotalUSD <= cost.FargateUSD {
		t.Fatalf("incomplete whole-installation cost: %#v", cost)
	}
	wantRequestsUSD := cost.ProjectedS3PUT/1000*price.S3PUT + cost.ProjectedS3LIST/1000*price.S3LIST + cost.ProjectedS3GET/1000*price.S3GET
	if math.Abs(cost.S3RequestsUSD-wantRequestsUSD) > 1e-9 {
		t.Fatalf("S3 request cost=%f want PUT+LIST+GET/HEAD=%f", cost.S3RequestsUSD, wantRequestsUSD)
	}
}

func TestReadResourcesUsesConcurrentPeakAndOOM(t *testing.T) {
	directory := t.TempDir()
	stats := filepath.Join(directory, "stats.jsonl")
	oom := filepath.Join(directory, "oom.tsv")
	content := "" +
		`{"sample":1,"name":"run-api","mem_usage":"100MiB / 512MiB"}` + "\n" +
		`{"sample":1,"name":"run-postgres-1","mem_usage":"200MiB / 4GiB"}` + "\n" +
		`{"sample":2,"name":"run-api","mem_usage":"120MiB / 512MiB"}` + "\n" +
		`{"sample":2,"name":"run-postgres-1","mem_usage":"150MiB / 4GiB"}` + "\n"
	if err := os.WriteFile(stats, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(oom, []byte("/run-api false\n/run-worker-1 true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	resource := readResources(stats, oom, filepath.Join(directory, "missing-cgroup.tsv"))
	if resource.PeakWholeInstallationBytes != 300<<20 || resource.PeakByContainerBytes["run-api"] != 120<<20 || len(resource.OOMKilled) != 1 || resource.OOMKilled[0] != "run-worker-1" || !resource.GoUnitsWithin512MiB {
		t.Fatalf("resource report=%#v", resource)
	}
	bytes, err := parseBytes("1.5GiB")
	if err != nil || math.Abs(float64(bytes)-1.5*(1<<30)) > 1 {
		t.Fatalf("parsed bytes=%d err=%v", bytes, err)
	}
}

func TestResourceEvidenceRequiresAllContainersAndWorkerIncarnations(t *testing.T) {
	directory := t.TempDir()
	stats, oom, cgroups := filepath.Join(directory, "stats"), filepath.Join(directory, "oom"), filepath.Join(directory, "cgroups")
	var samples, observations, memory strings.Builder
	id := 0
	for _, role := range []string{"api", "scheduler", "postgres-1", "minio-1", "worker-1"} {
		name := "run-" + role
		fmt.Fprintf(&samples, "{\"sample\":1,\"name\":%q,\"mem_usage\":\"10MiB / 512MiB\"}\n", name)
		fmt.Fprintf(&observations, "/%s false\n", name)
		incarnations := 1
		if role == "worker-1" {
			incarnations = 3
		}
		for range incarnations {
			id++
			fmt.Fprintf(&memory, "%s %064x 20971520 0 0 536870912 0\n", name, id)
		}
	}
	for path, value := range map[string]string{stats: samples.String(), oom: observations.String(), cgroups: memory.String()} {
		if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	resource := readResources(stats, oom, cgroups)
	if !resourceEvidenceComplete(resource, 1) || !resource.GoLimitsVerified || resource.ObservedCgroupPeakBytes["run-worker-1"] != 20<<20 {
		t.Fatalf("complete evidence rejected: %+v", resource)
	}
	resource.CgroupIncarnations["run-worker-1"]--
	if resourceEvidenceComplete(resource, 1) {
		t.Fatal("missing old worker peak accepted")
	}
	resource.CgroupIncarnations["run-worker-1"]++
	delete(resource.OOMObserved, "run-minio-1")
	if resourceEvidenceComplete(resource, 1) {
		t.Fatal("missing shared-backend OOM accepted")
	}
	if resourceEvidenceComplete(resourceReport{}, 1) {
		t.Fatal("empty samples counted as no OOM")
	}
	if err := os.WriteFile(cgroups, []byte(strings.ReplaceAll(memory.String(), "536870912 0", "1073741824 1")), 0o600); err != nil {
		t.Fatal(err)
	}
	if readResources(stats, oom, cgroups).GoLimitsVerified {
		t.Fatal("wrong memory/swap limits accepted")
	}
}

func TestPostLoadEvidenceCannotBeOmittedOrShortenedForOfficialProfile(t *testing.T) {
	report, targets := map[string]any{}, map[string]any{}
	if validPostLoad(report, targets) {
		t.Fatal("missing post-load evidence passed")
	}
	report["PostLoad"] = map[string]any{"Complete": true, "IdleSeconds": float64(10), "Operations": map[string]any{
		"maintenance_spare": map[string]any{"Calls": float64(100), "WorkMS": float64(10000)},
		"compaction":        map[string]any{"ElapsedMS": float64(2500)},
	}}
	for _, name := range []string{"cold_all_history_regex", "warm_same_snapshot_rows", "idle_no_query_work", "postload_maintenance_time_accounted"} {
		targets[name] = true
	}
	if !validPostLoad(report, targets) {
		t.Fatal("complete quick evidence rejected")
	}
	report["WarmupSeconds"], report["LoadSeconds"] = float64(300), float64(1800)
	if validPostLoad(report, targets) {
		t.Fatal("official idle phase silently shortened")
	}
	report["PostLoad"].(map[string]any)["IdleSeconds"] = float64(60)
	if !validPostLoad(report, targets) {
		t.Fatal("complete official phase rejected")
	}
	targets["warm_same_snapshot_rows"] = false
	if validPostLoad(report, targets) {
		t.Fatal("mismatched warm result accepted")
	}
}

func TestPostLoadRejectsUnaccountedMaintenanceDespiteLegacyTargets(t *testing.T) {
	for _, scenario := range []string{"missing", "startup_overrun", "excess_attempts", "zero_idle", "negative", "fractional", "unsafe_integer"} {
		t.Run(scenario, func(t *testing.T) {
			spare := map[string]any{"Calls": float64(100), "WorkMS": float64(10000)}
			compaction := map[string]any{"ElapsedMS": float64(2500)}
			operations := map[string]any{"maintenance_spare": spare, "compaction": compaction}
			switch scenario {
			case "missing":
				delete(operations, "maintenance_spare")
			case "startup_overrun":
				operations["maintenance_budget_overrun"] = map[string]any{"Calls": float64(1)}
			case "excess_attempts":
				operations["retention"] = map[string]any{"ElapsedMS": float64(1)}
			case "zero_idle":
				spare["WorkMS"] = float64(0)
			case "negative":
				compaction["ElapsedMS"] = float64(-1)
			case "fractional":
				compaction["ElapsedMS"] = 0.5
			case "unsafe_integer":
				spare["WorkMS"] = float64(1 << 54)
			}
			report := map[string]any{"PostLoad": map[string]any{"Complete": true, "IdleSeconds": float64(60), "Operations": operations}}
			targets := map[string]any{}
			for _, name := range []string{"cold_all_history_regex", "warm_same_snapshot_rows", "idle_no_query_work", "postload_maintenance_time_accounted"} {
				targets[name] = true
			}
			if validPostLoad(report, targets) {
				t.Fatal("unaccounted post-load maintenance accepted")
			}
		})
	}
}
