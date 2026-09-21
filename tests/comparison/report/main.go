package main

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

const gib = 1 << 30

type prices struct {
	AsOf            string            `json:"as_of"`
	Region          string            `json:"region"`
	Currency        string            `json:"currency"`
	MonthSeconds    float64           `json:"month_seconds"`
	PITRDays        float64           `json:"pitr_days"`
	FargateCPU      float64           `json:"fargate_arm_vcpu_second"`
	FargateMemory   float64           `json:"fargate_arm_gb_second"`
	FargateMemoryGB float64           `json:"fargate_billed_memory_gb_per_1_vcpu"`
	RDSHour         float64           `json:"rds_postgresql_t4g_micro_single_az_hour"`
	RDSStorage      float64           `json:"rds_gp3_gb_month"`
	RDSBackup       float64           `json:"rds_backup_gb_month"`
	RDSMinGB        float64           `json:"rds_min_storage_gb"`
	S3Storage       float64           `json:"s3_standard_gb_month"`
	S3PUT           float64           `json:"s3_put_per_1000"`
	S3GET           float64           `json:"s3_get_per_1000"`
	Sources         map[string]string `json:"sources"`
	Assumptions     []string          `json:"assumptions"`
	Exclusions      []string          `json:"exclusions"`
}

type stat struct {
	Sample   int64  `json:"sample"`
	Name     string `json:"name"`
	MemUsage string `json:"mem_usage"`
}

type resourceReport struct {
	PeakWholeInstallationBytes int64            `json:"peak_whole_installation_bytes"`
	PeakByContainerBytes       map[string]int64 `json:"peak_by_container_bytes"`
	OOMKilled                  []string         `json:"oom_killed"`
	GoUnitsWithin512MiB        bool             `json:"go_units_within_512_mib"`
	ObservedCgroupPeakBytes    map[string]int64 `json:"observed_cgroup_peak_bytes"`
	CgroupIncarnations         map[string]int   `json:"cgroup_incarnations"`
	OOMObserved                map[string]bool  `json:"oom_observed"`
	CgroupOOMEvents            uint64           `json:"cgroup_oom_events"`
	CgroupOOMKills             uint64           `json:"cgroup_oom_kills"`
	GoLimitsVerified           bool             `json:"go_limits_verified"`
}

type costReport struct {
	AsOf                         string            `json:"as_of"`
	Region                       string            `json:"region"`
	Currency                     string            `json:"currency"`
	MeasurementSeconds           float64           `json:"measurement_seconds"`
	MonthlyProjectionFactor      float64           `json:"monthly_projection_factor"`
	FargateTasks                 int               `json:"fargate_tasks"`
	FargateBilledMemoryGBPerTask float64           `json:"fargate_billed_memory_gb_per_task"`
	ProjectedPGStorageGB         float64           `json:"projected_pg_storage_gb"`
	BackupWindowGB               float64           `json:"backup_window_gb"`
	ChargedBackupGB              float64           `json:"charged_backup_gb"`
	ProjectedS3StorageGB         float64           `json:"projected_s3_storage_gb"`
	ProjectedS3PUT               float64           `json:"projected_s3_put"`
	ProjectedS3GET               float64           `json:"projected_s3_get"`
	FargateUSD                   float64           `json:"fargate_usd"`
	RDSComputeUSD                float64           `json:"rds_compute_usd"`
	RDSStorageUSD                float64           `json:"rds_storage_usd"`
	RDSBackupUSD                 float64           `json:"rds_backup_usd"`
	S3StorageUSD                 float64           `json:"s3_storage_usd"`
	S3RequestsUSD                float64           `json:"s3_requests_usd"`
	TotalUSD                     float64           `json:"total_usd"`
	Sources                      map[string]string `json:"sources"`
	Assumptions                  []string          `json:"assumptions"`
	Exclusions                   []string          `json:"exclusions"`
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "capacity" {
		capacityMain(os.Args[2:])
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "capacity-matrix" {
		capacityMatrixMain(os.Args[2:])
		return
	}
	if len(os.Args) != 6 {
		fatal("usage: comparison-report report.json stats.jsonl oom.tsv pricing.json cgroup.tsv")
	}
	report := readObject(os.Args[1])
	var price prices
	readJSON(os.Args[4], &price)
	resource := readResources(os.Args[2], os.Args[3], os.Args[5])
	cost := calculateCost(report, price)
	report["Resources"], report["Cost"] = resource, cost
	targets, _ := report["Targets"].(map[string]any)
	if targets == nil {
		targets = make(map[string]any)
		report["Targets"] = targets
	}
	complete := resourceEvidenceComplete(resource, int(number(report, "Workers")))
	targets["resource_evidence_complete"] = complete
	targets["no_cgroup_oom"] = complete && len(resource.OOMKilled) == 0 && resource.CgroupOOMEvents == 0 && resource.CgroupOOMKills == 0
	targets["go_units_within_512mib"] = complete && resource.GoUnitsWithin512MiB && resource.GoLimitsVerified
	targets["postload_complete"] = validPostLoad(report, targets)
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fatal(err.Error())
	}
	data = append(data, '\n')
	if err := os.WriteFile(os.Args[1], data, 0o600); err != nil {
		fatal(err.Error())
	}
	if targets["no_cgroup_oom"] != true || targets["go_units_within_512mib"] != true || targets["postload_complete"] != true {
		fatal("comparison resource/post-load target failed")
	}
}

func validPostLoad(report, targets map[string]any) bool {
	phase, ok := report["PostLoad"].(map[string]any)
	if !ok || phase["Complete"] != true {
		return false
	}
	for _, name := range []string{"cold_all_history_regex", "warm_same_snapshot_rows", "idle_no_query_work"} {
		if targets[name] != true {
			return false
		}
	}
	minimumIdle := float64(10)
	if report["WarmupSeconds"] == float64(300) && report["LoadSeconds"] == float64(1800) {
		minimumIdle = 60
	}
	idle, ok := phase["IdleSeconds"].(float64)
	return ok && idle >= minimumIdle
}

func readObject(path string) map[string]any {
	var value map[string]any
	readJSON(path, &value)
	return value
}

func readJSON(path string, value any) {
	data, err := os.ReadFile(path)
	if err != nil {
		fatal(err.Error())
	}
	if err := json.Unmarshal(data, value); err != nil {
		fatal(err.Error())
	}
}

func readResources(statsPath, oomPath, cgroupPath string) resourceReport {
	result := resourceReport{PeakByContainerBytes: make(map[string]int64), OOMKilled: []string{}, GoUnitsWithin512MiB: true,
		ObservedCgroupPeakBytes: make(map[string]int64), CgroupIncarnations: make(map[string]int), OOMObserved: make(map[string]bool), GoLimitsVerified: true}
	file, err := os.Open(statsPath)
	if err != nil {
		fatal(err.Error())
	}
	defer file.Close()
	sums := make(map[int64]int64)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var sample stat
		if json.Unmarshal(scanner.Bytes(), &sample) != nil || sample.Sample <= 0 || sample.Name == "" {
			continue
		}
		bytes, err := parseBytes(strings.TrimSpace(strings.Split(sample.MemUsage, "/")[0]))
		if err != nil {
			fatal(err.Error())
		}
		sums[sample.Sample] += bytes
		result.PeakByContainerBytes[sample.Name] = max(result.PeakByContainerBytes[sample.Name], bytes)
		if isGoUnit(sample.Name) && bytes > 512<<20 {
			result.GoUnitsWithin512MiB = false
		}
	}
	if err := scanner.Err(); err != nil {
		fatal(err.Error())
	}
	for _, bytes := range sums {
		result.PeakWholeInstallationBytes = max(result.PeakWholeInstallationBytes, bytes)
	}
	oom, err := os.ReadFile(oomPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		fatal(err.Error())
	}
	for _, line := range strings.Split(string(oom), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && (fields[1] == "true" || fields[1] == "false") {
			result.OOMObserved[strings.TrimPrefix(fields[0], "/")] = true
		}
		if len(fields) == 2 && fields[1] == "true" {
			result.OOMKilled = append(result.OOMKilled, strings.TrimPrefix(fields[0], "/"))
		}
	}
	cgroup, err := os.ReadFile(cgroupPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		fatal(err.Error())
	}
	seen := make(map[string]bool)
	for _, line := range strings.Split(strings.TrimSpace(string(cgroup)), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 7 || len(fields[1]) != 64 || seen[fields[1]] {
			fatal("invalid/duplicate cgroup evidence")
		}
		if _, err := hex.DecodeString(fields[1]); err != nil {
			fatal("invalid cgroup identity")
		}
		seen[fields[1]] = true
		peak, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil || peak <= 0 {
			fatal("invalid cgroup peak")
		}
		oom, err := strconv.ParseUint(fields[3], 10, 64)
		if err != nil {
			fatal("invalid cgroup OOM counter")
		}
		killed, err := strconv.ParseUint(fields[4], 10, 64)
		if err != nil {
			fatal("invalid cgroup OOM kill counter")
		}
		name := fields[0]
		result.ObservedCgroupPeakBytes[name] = max(result.ObservedCgroupPeakBytes[name], peak)
		result.CgroupIncarnations[name]++
		result.CgroupOOMEvents += oom
		result.CgroupOOMKills += killed
		if isGoUnit(name) {
			if peak > 512<<20 {
				result.GoUnitsWithin512MiB = false
			}
			if fields[5] != strconv.Itoa(512<<20) || fields[6] != "0" {
				result.GoLimitsVerified = false
			}
		}
	}
	sort.Strings(result.OOMKilled)
	return result
}

func resourceEvidenceComplete(resource resourceReport, workers int) bool {
	return resourceEvidenceForProfile(resource, workers, true)
}

func resourceEvidenceForProfile(resource resourceReport, workers int, restartAndCold bool) bool {
	if workers != 1 && workers != 2 && workers != 4 {
		return false
	}
	if len(resource.PeakByContainerBytes) != workers+4 || len(resource.ObservedCgroupPeakBytes) != workers+4 {
		return false
	}
	roles := make(map[string]bool)
	for name, bytes := range resource.PeakByContainerBytes {
		if bytes <= 0 || resource.ObservedCgroupPeakBytes[name] <= 0 || !resource.OOMObserved[name] {
			return false
		}
		role := ""
		for _, suffix := range []string{"api", "scheduler", "postgres-1", "minio-1"} {
			if strings.HasSuffix(name, "-"+suffix) {
				role = suffix
			}
		}
		expectedIncarnations := 1
		for index := 1; index <= workers; index++ {
			suffix := fmt.Sprintf("worker-%d", index)
			if strings.HasSuffix(name, "-"+suffix) {
				role = suffix
				if restartAndCold {
					expectedIncarnations = 2
					if index == 1 {
						expectedIncarnations = 3
					}
				}
			}
		}
		if role == "" || roles[role] || resource.CgroupIncarnations[name] != expectedIncarnations {
			return false
		}
		roles[role] = true
	}
	return len(roles) == workers+4
}

func calculateCost(report map[string]any, price prices) costReport {
	seconds := number(report, "WarmupSeconds") + number(report, "LoadSeconds")
	if seconds <= 0 || price.MonthSeconds <= 0 {
		fatal("invalid comparison duration or pricing month")
	}
	factor := price.MonthSeconds / seconds
	workers := int(number(report, "Workers"))
	pgGrowth := math.Max(0, number(report, "PGDatabaseEndBytes")-number(report, "PGDatabaseStartBytes"))
	pgGB := math.Max(price.RDSMinGB, pgGrowth*factor/gib)
	backupGB := (number(report, "PGDatabaseEndBytes") + number(report, "PGWALBytes")/seconds*(price.PITRDays*86400)) / gib
	chargedBackupGB := math.Max(0, backupGB-pgGB)
	s3GB := number(report, "S3StoredBytes") * factor / gib
	puts := number(report, "S3PutRequests") * factor
	gets := (number(report, "S3GetRequests") + number(report, "S3HeadRequests") + number(report, "S3RangeRequests")) * factor
	tasks := workers + 2
	fargate := float64(tasks) * price.MonthSeconds * (price.FargateCPU + price.FargateMemoryGB*price.FargateMemory)
	rdsCompute := price.RDSHour * price.MonthSeconds / 3600
	rdsStorage, rdsBackup := pgGB*price.RDSStorage, chargedBackupGB*price.RDSBackup
	s3Storage, s3Requests := s3GB*price.S3Storage, puts/1000*price.S3PUT+gets/1000*price.S3GET
	return costReport{
		AsOf: price.AsOf, Region: price.Region, Currency: price.Currency, MeasurementSeconds: seconds, MonthlyProjectionFactor: factor,
		FargateTasks: tasks, FargateBilledMemoryGBPerTask: price.FargateMemoryGB, ProjectedPGStorageGB: pgGB,
		BackupWindowGB: backupGB, ChargedBackupGB: chargedBackupGB, ProjectedS3StorageGB: s3GB,
		ProjectedS3PUT: puts, ProjectedS3GET: gets, FargateUSD: fargate, RDSComputeUSD: rdsCompute,
		RDSStorageUSD: rdsStorage, RDSBackupUSD: rdsBackup, S3StorageUSD: s3Storage, S3RequestsUSD: s3Requests,
		TotalUSD: fargate + rdsCompute + rdsStorage + rdsBackup + s3Storage + s3Requests,
		Sources:  price.Sources, Assumptions: price.Assumptions, Exclusions: price.Exclusions,
	}
}

func number(object map[string]any, key string) float64 {
	value, ok := object[key].(float64)
	if !ok {
		fatal("missing numeric report field " + key)
	}
	return value
}

func parseBytes(raw string) (int64, error) {
	units := []struct {
		suffix string
		factor float64
	}{{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"GB", 1e9}, {"MB", 1e6}, {"kB", 1e3}, {"B", 1}}
	for _, unit := range units {
		if strings.HasSuffix(raw, unit.suffix) {
			value, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimSuffix(raw, unit.suffix)), 64)
			return int64(value * unit.factor), err
		}
	}
	return 0, fmt.Errorf("unknown docker byte value %q", raw)
}

func isGoUnit(name string) bool {
	return strings.Contains(name, "-api") || strings.Contains(name, "-scheduler") || strings.Contains(name, "-worker-")
}

func fatal(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}
