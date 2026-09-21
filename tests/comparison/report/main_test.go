package main

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
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
	if price.AsOf != "2026-09-21" || price.FargateCPU <= 0 || price.FargateMemory <= 0 || price.RDSHour <= 0 || price.RDSStorage <= 0 || price.RDSBackup <= 0 || price.S3Storage <= 0 || price.S3PUT <= 0 || price.S3GET <= 0 || len(price.Sources) != 3 {
		t.Fatalf("incomplete price fixture: %#v", price)
	}
	report := map[string]any{
		"WarmupSeconds": float64(300), "LoadSeconds": float64(1800), "Workers": float64(2),
		"PGDatabaseStartBytes": float64(1 << 30), "PGDatabaseEndBytes": float64(2 << 30), "PGWALBytes": float64(1 << 30),
		"S3StoredBytes": float64(3 << 30), "S3PutRequests": float64(1000), "S3GetRequests": float64(2000),
		"S3HeadRequests": float64(3000), "S3RangeRequests": float64(4000),
	}
	cost := calculateCost(report, price)
	if cost.FargateTasks != 4 || cost.ProjectedPGStorageGB < price.RDSMinGB || cost.ProjectedS3StorageGB <= 3 || cost.ProjectedS3PUT <= 1000 || cost.ProjectedS3GET <= 9000 || cost.FargateUSD <= 0 || cost.RDSComputeUSD <= 0 || cost.RDSStorageUSD <= 0 || cost.S3StorageUSD <= 0 || cost.S3RequestsUSD <= 0 || cost.TotalUSD <= cost.FargateUSD {
		t.Fatalf("incomplete whole-installation cost: %#v", cost)
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
	resource := readResources(stats, oom)
	if resource.PeakWholeInstallationBytes != 300<<20 || resource.PeakByContainerBytes["run-api"] != 120<<20 || len(resource.OOMKilled) != 1 || resource.OOMKilled[0] != "run-worker-1" || !resource.GoUnitsWithin512MiB {
		t.Fatalf("resource report=%#v", resource)
	}
	bytes, err := parseBytes("1.5GiB")
	if err != nil || math.Abs(float64(bytes)-1.5*(1<<30)) > 1 {
		t.Fatalf("parsed bytes=%d err=%v", bytes, err)
	}
}
