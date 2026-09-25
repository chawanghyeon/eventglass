package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
)

func capacityMain(args []string) {
	if len(args) != 4 {
		fatal("usage: comparison-report capacity report.json stats.jsonl oom.tsv cgroup.tsv")
	}
	report := readObject(args[0])
	resource := readResources(args[1], args[2], args[3])
	report["Resources"] = resource
	targets, ok := report["Targets"].(map[string]any)
	if !ok {
		targets = make(map[string]any)
		report["Targets"] = targets
	}
	complete := resourceEvidenceForProfile(resource, int(number(report, "Workers")), false)
	targets["resource_evidence_complete"] = complete
	targets["no_cgroup_oom"] = complete && len(resource.OOMKilled) == 0 && resource.CgroupOOMEvents == 0 && resource.CgroupOOMKills == 0
	targets["go_units_within_512mib"] = complete && resource.GoUnitsWithin512MiB && resource.GoLimitsVerified
	targets["capacity_evidence_complete"] = validCapacity(report)
	writeCapacityObject(args[0], report)
	for _, name := range []string{"resource_evidence_complete", "no_cgroup_oom", "go_units_within_512mib", "capacity_evidence_complete"} {
		if targets[name] != true {
			fatal("capacity resource/completeness target failed: " + name)
		}
	}
}

func validCapacity(report map[string]any) bool {
	cycles, workers := number(report, "Cycles"), number(report, "Workers")
	if report["Kind"] != "fixed-work-publication-v1" || report["Architecture"] != "linux/arm64" || report["Complete"] != true ||
		cycles < 16 || cycles > 512 || math.Mod(cycles, 4) != 0 || workers != 1 && workers != 2 && workers != 4 ||
		number(report, "Accepted") != cycles*105 || number(report, "PublishedRecords") != cycles*105 || number(report, "Duplicates") != 0 || number(report, "Conflicts") != 0 ||
		number(report, "PreloadedJobs") != cycles*6 || number(report, "DrainElapsedMS") <= 0 || number(report, "DrainElapsedMS") > 900000 {
		return false
	}
	rate := number(report, "PublishedRecords") * 1000 / number(report, "DrainElapsedMS")
	if math.Abs(number(report, "RecordsPerSecond")-rate) > rate*1e-9 {
		return false
	}
	fixture, ok := report["FixtureSHA256"].(string)
	if !ok || len(fixture) != 64 {
		return false
	}
	if _, err := hex.DecodeString(fixture); err != nil {
		return false
	}
	projects, ok := report["PublishedByProject"].([]any)
	if !ok || len(projects) != 4 {
		return false
	}
	for _, project := range projects {
		counts, ok := project.(map[string]any)
		if !ok || number(counts, "log") != cycles/4*100 || number(counts, "error") != cycles/4*5 {
			return false
		}
	}
	progress, ok := report["Progress"].([]any)
	if !ok || len(progress) < 2 || len(progress) > 902 {
		return false
	}
	previousTime, previousRows := float64(-1), float64(-1)
	for index, value := range progress {
		sample, ok := value.(map[string]any)
		if !ok {
			return false
		}
		elapsed, rows, backlog := number(sample, "ElapsedMS"), number(sample, "Published"), number(sample, "Backlog")
		if elapsed < previousTime || rows < previousRows || rows > cycles*105 || backlog < 0 {
			return false
		}
		if index == 0 && (rows != 0 || backlog != number(report, "PreloadedJobs")) {
			return false
		}
		if index == len(progress)-1 && (rows != cycles*105 || backlog != 0) {
			return false
		}
		previousTime, previousRows = elapsed, rows
	}
	input, ok := report["SubmittedInput"].(map[string]any)
	if !ok || input["Complete"] != true || input["Framing"] != "eventglass-submitted-envelope-set-v2:sorted-BE64-length+SHA256(body)" ||
		input["WorkloadFraming"] != "eventglass-comparison-logical-workload-set-v1:DSN-key-normalized+sorted-BE64-length+SHA256(body)" ||
		number(input, "Envelopes") < cycles*6 || number(input, "Bytes") <= 0 {
		return false
	}
	if number(input, "WorkloadEnvelopes") < cycles*6 || number(input, "WorkloadBytes") <= 0 {
		return false
	}
	sha, ok := input["SHA256"].(string)
	if !ok || len(sha) != 64 {
		return false
	}
	_, err := hex.DecodeString(sha)
	if err != nil {
		return false
	}
	workloadSHA, ok := input["WorkloadSHA256"].(string)
	if !ok || len(workloadSHA) != 64 {
		return false
	}
	_, err = hex.DecodeString(workloadSHA)
	return err == nil
}

type capacityMatrixProfile struct {
	Workers                int
	Samples                int
	MedianRecordsPerSecond float64
	Speedup, Efficiency    float64
	Reports                []string
}

func capacityMatrixMain(args []string) {
	if len(args) != 2 {
		fatal("usage: comparison-report capacity-matrix reports.txt output.json")
	}
	data, err := os.ReadFile(args[0])
	if err != nil {
		fatal(err.Error())
	}
	groups := make(map[int][]float64)
	paths := make(map[int][]string)
	var revision, fixture string
	var cycles float64
	seen := make(map[string]bool)
	reportPaths := strings.Fields(string(data))
	if len(reportPaths) < 3 || len(reportPaths) > 9 {
		fatal("capacity matrix requires three to nine samples")
	}
	for _, path := range reportPaths {
		if seen[path] {
			fatal("duplicate capacity sample: " + path)
		}
		seen[path] = true
		report := readObject(path)
		targets, ok := report["Targets"].(map[string]any)
		if !ok || !validCapacity(report) {
			fatal("incomplete capacity sample: " + path)
		}
		for _, name := range []string{"resource_evidence_complete", "no_cgroup_oom", "go_units_within_512mib", "capacity_evidence_complete"} {
			if targets[name] != true {
				fatal("failed capacity evidence: " + path)
			}
		}
		currentRevision, _ := report["Revision"].(string)
		currentFixture, _ := report["FixtureSHA256"].(string)
		if revision == "" {
			revision, fixture, cycles = currentRevision, currentFixture, number(report, "Cycles")
		}
		if currentRevision == "" || currentRevision != revision || currentFixture != fixture || number(report, "Cycles") != cycles {
			fatal("capacity samples do not describe identical source/work")
		}
		workers := int(number(report, "Workers"))
		groups[workers] = append(groups[workers], number(report, "RecordsPerSecond"))
		paths[workers] = append(paths[workers], path)
	}
	profiles := make([]capacityMatrixProfile, 0, 3)
	baseline := float64(0)
	diagnostic := cycles != 128 || strings.HasSuffix(revision, "-dirty") || strings.HasPrefix(revision, "unverified-")
	for _, workers := range []int{1, 2, 4} {
		rates := groups[workers]
		if len(rates) == 0 {
			fatal(fmt.Sprintf("capacity matrix missing %d-worker samples", workers))
		}
		sort.Float64s(rates)
		median := rates[len(rates)/2]
		if len(rates)%2 == 0 {
			median = (rates[len(rates)/2-1] + median) / 2
		}
		if workers == 1 {
			baseline = median
		}
		if len(rates) != 3 {
			diagnostic = true
		}
		profiles = append(profiles, capacityMatrixProfile{workers, len(rates), median, median / baseline, median / (baseline * float64(workers)), paths[workers]})
	}
	writeCapacityObject(args[1], map[string]any{"Kind": "fixed-work-publication-v1", "Revision": revision, "FixtureSHA256": fixture, "Cycles": cycles, "Diagnostic": diagnostic,
		"Scope":    "identical durable-ACK preload drained by 1/2/4 workers; includes Docker resume time, excludes preload and query verification from throughput; not mixed-load SLO or steady-state maximum capacity",
		"Profiles": profiles, "ThroughputGrewAtTwo": profiles[1].MedianRecordsPerSecond > profiles[0].MedianRecordsPerSecond, "ThroughputGrewAtFour": profiles[2].MedianRecordsPerSecond > profiles[1].MedianRecordsPerSecond})
}

func writeCapacityObject(path string, value any) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		fatal(err.Error())
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		fatal(err.Error())
	}
}
