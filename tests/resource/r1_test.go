//go:build resource

package resource_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/app"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/ingest"
	"github.com/chawanghyeon/eventglass/internal/issues"
	"github.com/chawanghyeon/eventglass/internal/model"
	resourcebudget "github.com/chawanghyeon/eventglass/internal/resource"
	"github.com/chawanghyeon/eventglass/internal/sdk"
)

const (
	r1LogsPerCycle   = 500
	r1ErrorsPerCycle = 25
	r1CycleInterval  = 5 * time.Second
)

func TestR1CgroupProfile(t *testing.T) {
	requireR1(t)
	cpu := readCgroup(t, "cpu.max")
	memory := readCgroup(t, "memory.max")
	swap := readCgroup(t, "memory.swap.max")
	parts := strings.Fields(cpu)
	if len(parts) != 2 {
		t.Fatalf("cpu.max=%q", cpu)
	}
	quota, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	period, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || quota != period {
		t.Fatalf("expected exactly one CPU, cpu.max=%q", cpu)
	}
	if memory != strconv.FormatInt(512<<20, 10) || swap != "0" {
		t.Fatalf("memory.max=%s memory.swap.max=%s", memory, swap)
	}
	t.Logf("R1 cgroup cpu_max=%s memory_max=%s swap_max=%s", cpu, memory, swap)
}

func TestR1SustainedMixedNativeWorkload(t *testing.T) {
	requireR1(t)
	duration := 30 * time.Second
	if raw := os.Getenv("EVENTGLASS_RESOURCE_DURATION"); raw != "" {
		var err error
		duration, err = time.ParseDuration(raw)
		if err != nil || duration < 30*time.Second || duration > 10*time.Minute {
			t.Fatalf("invalid EVENTGLASS_RESOURCE_DURATION=%q", raw)
		}
	}
	binary := os.Getenv("EVENTGLASS_TEST_BINARY")
	if binary == "" {
		t.Fatal("EVENTGLASS_TEST_BINARY is required")
	}
	root := t.TempDir()
	started := time.Now()
	deadline := started.Add(duration)
	var cycles, records int
	var worstCycle time.Duration
	for time.Now().Before(deadline) {
		cycleStarted := time.Now()
		batch := normalizeR1Batch(t, cycles)
		bundles := convertR1Batch(t, binary, root, cycles, batch.Records)
		queryR1Bundles(t, binary, root, cycles, bundles)
		if err := os.RemoveAll(filepath.Join(root, fmt.Sprintf("cycle-%04d", cycles))); err != nil {
			t.Fatal(err)
		}
		cycles++
		records += len(batch.Records)
		elapsed := time.Since(cycleStarted)
		if elapsed > worstCycle {
			worstCycle = elapsed
		}
		if elapsed > r1CycleInterval {
			t.Fatalf("work backlog would grow: cycle=%d duration=%s target=%s", cycles, elapsed, r1CycleInterval)
		}
		remaining := r1CycleInterval - elapsed
		if time.Now().Add(remaining).After(deadline) {
			break
		}
		time.Sleep(remaining)
	}
	elapsed := time.Since(started)
	if cycles < 6 || records != cycles*(r1LogsPerCycle+r1ErrorsPerCycle) {
		t.Fatalf("cycles=%d records=%d", cycles, records)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("scratch not reclaimed: entries=%d err=%v", len(entries), err)
	}
	peak := readCgroup(t, "memory.peak")
	events := cgroupEvents(t)
	if events["oom"] != 0 || events["oom_kill"] != 0 {
		t.Fatalf("cgroup OOM during bounded workload: %v", events)
	}
	t.Logf("R1 mixed duration=%s cycles=%d records=%d logical_rate=105/s worst_cycle=%s cgroup_memory_peak_bytes=%s cgroup_oom=0 scratch_entries=0", elapsed.Round(time.Millisecond), cycles, records, worstCycle.Round(time.Millisecond), peak)
}

func TestR1PermitDrainWaitsForOwner(t *testing.T) {
	requireR1(t)
	budget := resourcebudget.NewBudget(192 << 20)
	permit, err := budget.Acquire(192 << 20)
	if err != nil {
		t.Fatal(err)
	}
	drainContext, stop := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer stop()
	if err := budget.Drain(drainContext); !errors.Is(err, context.DeadlineExceeded) || budget.Used() != 192<<20 {
		t.Fatalf("drain released live permit: err=%v used=%d", err, budget.Used())
	}
	permit.Release()
	if err := budget.Drain(context.Background()); err != nil || budget.Used() != 0 {
		t.Fatalf("joined permit was not reclaimed: err=%v used=%d", err, budget.Used())
	}
}

func TestR1NativeOOMAndCancellationJoinBeforeCleanup(t *testing.T) {
	requireR1(t)
	binary := os.Getenv("EVENTGLASS_TEST_BINARY")
	root := t.TempDir()
	batch := normalizeR1Batch(t, 999)
	bundles := convertR1Batch(t, binary, root, 999, batch.Records)
	inputs := make([]string, len(bundles))
	for index := range bundles {
		inputs[index] = bundles[index].Analytics.Path
	}
	operation := r1RowsOperation()
	operation.ScanSQL = `SELECT list(repeat(a.message,1024)) AS oversized FROM input_rows a CROSS JOIN range(100000) r`
	oomOutput := filepath.Join(root, "oom.parquet")
	oomContext, stopOOM := context.WithTimeout(context.Background(), 30*time.Second)
	defer stopOOM()
	_, err := (app.ProcessQueryRunner{BinaryPath: binary}).Run(oomContext, engine.QueryRequest{
		Version: engine.QueryExecutionProtocolVersion, QueryID: "r1-oom", Task: model.QueryTaskKey{Stage: model.QueryTaskScan},
		Operation: operation, InputPaths: inputs, OutputPath: oomOutput, SpillDirectory: filepath.Join(root, "oom-spill"),
		NativeMemoryBytes: 32 << 20, NativeSpillBytes: 64 << 20,
	})
	if !errors.Is(err, resourcebudget.ErrLimited) {
		t.Fatalf("native OOM classification=%v", err)
	}
	if pathExists(oomOutput) || pathExists(filepath.Join(root, "oom-spill")) {
		t.Fatal("OOM output or spill survived child join")
	}

	cancelOutput := filepath.Join(root, "cancel.parquet")
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	operation.ScanSQL = `SELECT a.record_id FROM input_rows a CROSS JOIN range(100000000) r ORDER BY hash(a.record_id,r.range) LIMIT 100`
	_, err = (app.ProcessQueryRunner{BinaryPath: binary}).Run(ctx, engine.QueryRequest{
		Version: engine.QueryExecutionProtocolVersion, QueryID: "r1-cancel", Task: model.QueryTaskKey{Stage: model.QueryTaskScan},
		Operation: operation, InputPaths: inputs, OutputPath: cancelOutput, SpillDirectory: filepath.Join(root, "cancel-spill"),
		NativeMemoryBytes: 64 << 20, NativeSpillBytes: 64 << 20,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("native cancellation=%v", err)
	}
	if pathExists(cancelOutput) || pathExists(filepath.Join(root, "cancel-spill")) || engineChildren() != 0 {
		t.Fatalf("canceled child was not joined and cleaned: output=%v spill=%v children=%d", pathExists(cancelOutput), pathExists(filepath.Join(root, "cancel-spill")), engineChildren())
	}

	queryR1Bundles(t, binary, root, 1000, bundles)
	t.Logf("R1 native_failure oom=resource_exhausted cancel=joined cleanup=true cgroup_memory_peak_bytes=%s", readCgroup(t, "memory.peak"))
}

func requireR1(t *testing.T) {
	t.Helper()
	if os.Getenv("EVENTGLASS_RESOURCE_REQUIRED") != "1" {
		t.Fatal("R1 resource gate environment is required")
	}
}

func readCgroup(t *testing.T, name string) string {
	t.Helper()
	encoded, err := os.ReadFile(filepath.Join("/sys/fs/cgroup", name))
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(encoded))
}

func cgroupEvents(t *testing.T) map[string]int64 {
	t.Helper()
	result := map[string]int64{}
	for _, line := range strings.Split(readCgroup(t, "memory.events"), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			t.Fatalf("memory.events line=%q", line)
		}
		value, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		result[fields[0]] = value
	}
	return result
}

func normalizeR1Batch(t *testing.T, cycle int) model.NormalizedRequest {
	t.Helper()
	logs := make([]any, r1LogsPerCycle)
	for index := range logs {
		logs[index] = map[string]any{"body": fmt.Sprintf("r1 log %d %d", cycle, index), "severity_text": "info"}
	}
	items := []sdk.Item{{Ordinal: 0, Type: "log", Header: map[string]any{"item_count": json.Number(strconv.Itoa(len(logs)))}, Value: map[string]any{"items": logs}}}
	for index := 0; index < r1ErrorsPerCycle; index++ {
		items = append(items, sdk.Item{Ordinal: index + 1, Type: "event", Value: map[string]any{
			"event_id": fmt.Sprintf("%032x", cycle*r1ErrorsPerCycle+index+1), "message": fmt.Sprintf("r1 error %d %d", cycle, index), "level": "error",
		}})
	}
	batch, err := ingest.NormalizeEnvelope(sdk.Envelope{Header: map[string]any{}, Items: items}, ingest.NormalizeOptions{
		TenantID: 1, ProjectID: 1, AcceptanceID: fmt.Sprintf("00000000-0000-4000-8000-%012d", cycle+1), ArrivalTime: time.Unix(1_700_000_000+int64(cycle), 0),
	})
	if err != nil || len(batch.Records) != r1LogsPerCycle+r1ErrorsPerCycle {
		t.Fatalf("normalize records=%d err=%v", len(batch.Records), err)
	}
	return batch
}

func convertR1Batch(t *testing.T, binary, root string, cycle int, records []model.Record) []engine.ConvertedBundle {
	t.Helper()
	directory := filepath.Join(root, fmt.Sprintf("cycle-%04d", cycle))
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	stagePath := filepath.Join(directory, "stage.jsonl")
	stage, err := os.OpenFile(stagePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(stage)
	batchID := fmt.Sprintf("00000000-0000-4000-8001-%012d", cycle+1)
	errorsSelected := 0
	for index, record := range records {
		staged := engine.StageRecord{Version: 1, GlobalOrdinal: index, BatchID: batchID, LaneID: 0, BatchSeq: int64(cycle + 1), ReceivedTimeUS: record.ArrivalTimeUS, GroupingVersion: 1, Record: record}
		if record.Kind == model.KindError {
			errorsSelected++
			group, err := issues.GroupRecord(record)
			if err != nil {
				stage.Close()
				t.Fatal(err)
			}
			staged.IssueID, staged.FingerprintSHA256, staged.IssueTitle = group.IssueID, group.FingerprintSHA, group.Title
		}
		if err := encoder.Encode(staged); err != nil {
			stage.Close()
			t.Fatal(err)
		}
	}
	if err := stage.Close(); err != nil {
		t.Fatal(err)
	}
	request := engine.ConversionRequest{
		Version: 1, StagePath: stagePath, OutputDirectory: filepath.Join(directory, "output"), SpillDirectory: filepath.Join(directory, "spill"),
		TenantID: 1, LaneID: 0, BatchSeq: int64(cycle + 1), BatchID: batchID, SelectedRecords: len(records), SelectedErrors: errorsSelected,
		NativeMemoryBytes: 192 << 20, NativeSpillBytes: 256 << 20,
	}
	var bundles []engine.ConvertedBundle
	summary, err := (app.ProcessConversionRunner{BinaryPath: binary}).Run(context.Background(), request, func(bundle engine.ConvertedBundle) error {
		bundles = append(bundles, bundle)
		return nil
	})
	if err != nil || summary.SelectedRecordCount != len(records) || len(bundles) == 0 {
		t.Fatalf("convert summary=%#v bundles=%d err=%v", summary, len(bundles), err)
	}
	return bundles
}

func queryR1Bundles(t *testing.T, binary, root string, cycle int, bundles []engine.ConvertedBundle) {
	t.Helper()
	inputs := make([]string, len(bundles))
	for index := range bundles {
		inputs[index] = bundles[index].Analytics.Path
	}
	directory := filepath.Join(root, fmt.Sprintf("cycle-%04d", cycle))
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	summary, err := (app.ProcessQueryRunner{BinaryPath: binary}).Run(context.Background(), engine.QueryRequest{
		Version: engine.QueryExecutionProtocolVersion, QueryID: fmt.Sprintf("r1-query-%d", cycle), Task: model.QueryTaskKey{Stage: model.QueryTaskScan},
		Operation: r1RowsOperation(), InputPaths: inputs, OutputPath: filepath.Join(directory, "query.parquet"), SpillDirectory: filepath.Join(directory, "query-spill"),
		NativeMemoryBytes: 192 << 20, NativeSpillBytes: 256 << 20,
	})
	if err != nil || summary.Rows != 100 {
		t.Fatalf("query rows=%d err=%v", summary.Rows, err)
	}
}

func r1RowsOperation() engine.QueryOperation {
	return engine.QueryOperation{
		Version: engine.QueryExecutionProtocolVersion, Kind: "rows", MaxRows: 100,
		Result:    engine.QueryResultPlan{Kind: "rows", Limit: 100, Sort: "received_desc"},
		ScanSQL:   `SELECT record_id,event_time_us,event_time_ns_remainder,received_time_us FROM input_rows ORDER BY received_time_us DESC,record_id DESC LIMIT 100`,
		ReduceSQL: `SELECT * FROM input_rows ORDER BY received_time_us DESC,record_id DESC LIMIT 100`,
		EmptySQL:  `SELECT ''::VARCHAR record_id,0::BIGINT event_time_us,0::INTEGER event_time_ns_remainder,0::BIGINT received_time_us WHERE false`,
	}
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func engineChildren() int {
	entries, _ := os.ReadDir("/proc")
	count := 0
	for _, entry := range entries {
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue
		}
		file, err := os.Open(filepath.Join("/proc", entry.Name(), "cmdline"))
		if err != nil {
			continue
		}
		line, _ := bufio.NewReader(file).ReadString(0)
		file.Close()
		if strings.Contains(line, "eventglass-go\x00engine-child") {
			count++
		}
	}
	return count
}
