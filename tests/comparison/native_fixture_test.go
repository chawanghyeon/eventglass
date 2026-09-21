//go:build nativeoracle

package comparison

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/app"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/ingest"
	"github.com/chawanghyeon/eventglass/internal/issues"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
	"github.com/chawanghyeon/eventglass/internal/sdk"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

const nativeEpoch = int64(1_710_000_000)
const nativeBatchRows = 9_996 // 476 legal envelopes (20 logs + one error), below journal caps.

func TestNativeFixtureBoundaryAndRetryContracts(t *testing.T) {
	// Include a valid source ID (the first envelope's error is intentionally
	// ID-less) and a final partial envelope containing only an error.
	for _, bounds := range [][2]int{{0, 21}, {21, 42}, {999_999, 1_000_000}} {
		envelope, err := sdk.ParseEnvelope(nativeEnvelope(t, bounds[0], bounds[1]))
		if err != nil {
			t.Fatal(err)
		}
		options := ingest.NormalizeOptions{TenantID: 1, ProjectID: 1, AcceptanceID: "00000000-0000-4000-8000-000000000123", ArrivalTime: time.Unix(nativeEpoch, 0)}
		batch, err := ingest.NormalizeEnvelope(envelope, options)
		if err != nil || len(batch.Records) != bounds[1]-bounds[0] {
			t.Fatalf("bounds=%v records=%d err=%v", bounds, len(batch.Records), err)
		}
		nativeRetryOracle(t, envelope, options, batch)
		if bounds[0] == 21 {
			var eligible int
			for _, record := range batch.Records {
				_, ok, err := ingest.DedupePayloadSHA256(record)
				if err != nil {
					t.Fatal(err)
				}
				if ok {
					eligible++
				}
			}
			if eligible != 1 {
				t.Fatalf("valid-ID duplicate fixture has %d eligible records", eligible)
			}
		}
	}
}

// This is Mode A: real SDK parsing, normalization, journal replay, conversion,
// query planning and native scan/reduce. There is deliberately no invented PG
// receipt/authorization/ACK or S3 measurement; Mode B owns those checks.
func TestNativeDatasetOracle(t *testing.T) {
	rows, err := strconv.Atoi(os.Getenv("EVENTGLASS_NATIVE_ORACLE_ROWS"))
	if err != nil || fixedFixtureSummaries[rows].Rows == 0 {
		t.Fatal("EVENTGLASS_NATIVE_ORACLE_ROWS must select 10000, 100000, 1000000 or 10000000")
	}
	if runtime.GOARCH != "arm64" || runtime.GOOS != "linux" || runtime.Version() != "go1.27.1" {
		t.Fatal("native oracle requires Go 1.27.1 Linux ARM64")
	}
	for name, want := range map[string]string{"cpu.max": "100000 100000", "memory.max": "536870912", "memory.swap.max": "0"} {
		if got := nativeCgroup(t, name); got != want {
			t.Fatalf("%s=%s want=%s", name, got, want)
		}
	}
	t.Cleanup(func() {
		t.Logf("cgroup peak=%s events=%s", nativeCgroup(t, "memory.peak"), nativeCgroup(t, "memory.events"))
	})
	binaryPath := os.Getenv("EVENTGLASS_TEST_BINARY")
	if binaryPath == "" {
		t.Fatal("EVENTGLASS_TEST_BINARY is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Minute)
	defer cancel()
	root := t.TempDir()
	started := time.Now()
	var allocatedBefore, allocatedAfter runtime.MemStats
	runtime.ReadMemStats(&allocatedBefore)
	digest := sha256.New()
	digest.Write([]byte("eventglass-native-fixture-v1\x00"))
	var files []model.CatalogFile
	var parquetBytes, journalBytes int64
	var normalizeTime, convertTime time.Duration
	var top []nativeRow
	acceptanceSequence := 0
	for start := 0; start < rows; start += nativeBatchRows {
		if err := ctx.Err(); err != nil {
			t.Fatal(err)
		}
		end := min(start+nativeBatchRows, rows)
		batchNo := start/nativeBatchRows + 1
		batchID := fmt.Sprintf("00000000-0000-4000-9000-%012x", batchNo)
		const lane = 0
		batch := model.JournalBatch{TenantID: 1, LaneID: lane, BatchID: batchID}
		var records []model.Record
		var expectedIDs []string
		expectedPartitions := map[string][]string{}
		normalizeStarted := time.Now()
		for requestStart := start; requestStart < end; requestStart += 21 {
			requestEnd := min(requestStart+21, end)
			var acceptance string
			for {
				acceptanceSequence++
				acceptance = fmt.Sprintf("00000000-0000-4000-8000-%012x", acceptanceSequence)
				candidate, err := model.LaneForAcceptance(acceptance)
				if err != nil {
					t.Fatal(err)
				}
				if candidate == lane {
					break
				}
			}
			body := nativeEnvelope(t, requestStart, requestEnd)
			var length [8]byte
			binary.BigEndian.PutUint64(length[:], uint64(len(body)))
			digest.Write(length[:])
			digest.Write(body)
			envelope, err := sdk.ParseEnvelope(body)
			if err != nil {
				t.Fatal(err)
			}
			options := ingest.NormalizeOptions{TenantID: 1, ProjectID: 1, AcceptanceID: acceptance, ArrivalTime: time.Unix(nativeEpoch+int64(rows), 0)}
			request, err := ingest.NormalizeEnvelope(envelope, options)
			if err != nil || len(request.Records) != requestEnd-requestStart {
				t.Fatalf("normalize batch=%d: %v", batchNo, err)
			}
			for index := requestStart; index < requestEnd; index++ {
				want := nativeExpectedRow(index, requestStart, acceptance)
				top = append(top, want)
				expectedIDs = append(expectedIDs, want.ID)
				key := time.UnixMicro(want.EventUS).UTC().Format(time.DateOnly) + "/" + want.Kind
				expectedPartitions[key] = append(expectedPartitions[key], want.ID)
			}
			for _, record := range request.Records {
				index, err := strconv.Atoi(strings.TrimPrefix(record.Message, "native row "))
				if err != nil || index < start || index >= end {
					t.Fatalf("unexpected message %q", record.Message)
				}
				want := nativeExpectedRow(index, requestStart, acceptance)
				if record.RecordID != want.ID || record.EventTimeUS != want.EventUS || record.EventTimeNSRemainder != 123 || string(record.Kind) != want.Kind {
					t.Fatalf("normalization index=%d got=%s/%d/%d/%s want=%+v", index, record.RecordID, record.EventTimeUS, record.EventTimeNSRemainder, record.Kind, want)
				}
				if index%21 != 0 && record.SourceEventID != nil {
					t.Fatal("ID-less log gained source ID")
				}
				if index%21 == 0 && index%17 != 0 && (record.SourceEventID == nil || *record.SourceEventID != fmt.Sprintf("%032x", index+1)) {
					t.Fatal("error source ID changed")
				}
				if index%21 == 0 && index%17 == 0 && record.SourceEventID != nil {
					t.Fatal("ID-less error gained source ID")
				}
			}
			if requestStart == 0 {
				nativeRetryOracle(t, envelope, options, request)
			}
			batch.Requests = append(batch.Requests, request)
			records = append(records, request.Records...)
		}
		sort.Slice(top, func(i, j int) bool {
			if top[i].EventUS != top[j].EventUS {
				return top[i].EventUS > top[j].EventUS
			}
			return top[i].ID > top[j].ID
		})
		top = top[:min(202, len(top))]
		normalizeTime += time.Since(normalizeStarted)
		directory := filepath.Join(root, fmt.Sprintf("batch-%06d", batchNo))
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		journal, err := os.Create(filepath.Join(directory, "journal.zst"))
		if err != nil {
			t.Fatal(err)
		}
		info, writeErr := storage.WriteJournal(journal, batch)
		if err := journal.Close(); err != nil {
			t.Fatal(err)
		}
		if writeErr != nil {
			t.Fatal(writeErr)
		}
		journalBytes += info.Bytes
		// Only verified journal replay, never a second SDK normalization, feeds conversion.
		stage, err := os.Create(filepath.Join(directory, "stage.jsonl"))
		if err != nil {
			t.Fatal(err)
		}
		reader, err := os.Open(journal.Name())
		if err != nil {
			stage.Close()
			t.Fatal(err)
		}
		encoder := json.NewEncoder(stage)
		errorsSelected := 0
		_, replayErr := storage.ReplayJournal(reader, info, func(_ storage.JournalRequest, ordinal int, record model.Record) error {
			if !reflect.DeepEqual(record, records[ordinal]) {
				return fmt.Errorf("journal changed ordinal %d", ordinal)
			}
			staged := engine.StageRecord{Version: 1, GlobalOrdinal: ordinal, BatchID: batchID, LaneID: lane, BatchSeq: int64(batchNo), ReceivedTimeUS: record.ArrivalTimeUS, GroupingVersion: 1, Record: record}
			if record.Kind == model.KindError {
				errorsSelected++
				group, err := issues.GroupRecord(record)
				if err != nil {
					return err
				}
				staged.IssueID, staged.FingerprintSHA256, staged.IssueTitle = group.IssueID, group.FingerprintSHA, group.Title
			}
			return encoder.Encode(staged)
		})
		closeErr := stage.Close()
		reader.Close()
		if replayErr != nil || closeErr != nil {
			t.Fatalf("journal replay=%v stage=%v", replayErr, closeErr)
		}
		if info, err := os.Stat(stage.Name()); err != nil || info.Size() > 32<<20 {
			t.Fatalf("stage exceeds fixture disk budget: %v", err)
		}
		batch.Requests = nil
		records = nil
		runtime.GC() // Do not retain the parsed batch while the native child owns memory.
		convertStarted := time.Now()
		conversionRequest := engine.ConversionRequest{
			Version: 1, StagePath: stage.Name(), OutputDirectory: filepath.Join(directory, "output"), SpillDirectory: filepath.Join(directory, "spill"),
			TenantID: 1, LaneID: lane, BatchSeq: int64(batchNo), BatchID: batchID, SelectedRecords: end - start, SelectedErrors: errorsSelected,
			NativeMemoryBytes: 256 << 20, NativeSpillBytes: 256 << 20,
		}
		summary, err := (app.ProcessConversionRunner{BinaryPath: binaryPath}).Run(ctx, conversionRequest, func(bundle engine.ConvertedBundle) error {
			key := bundle.EventDay + "/" + string(bundle.Kind)
			ids, exists := expectedPartitions[key]
			if !exists || bundle.RowCount != int64(len(ids)) || bundle.IdentitySHA256 != nativeIdentityHash(ids) {
				return fmt.Errorf("partition identity/count mismatch: %s", key)
			}
			delete(expectedPartitions, key)
			file := bundle.Analytics
			parquetBytes += file.Evidence.Bytes
			if parquetBytes > 2<<30 || len(files) >= query.MaxPlanFiles {
				return fmt.Errorf("native oracle retained-data budget exceeded")
			}
			files = append(files, model.CatalogFile{FileID: fmt.Sprintf("%08x-0000-4000-8000-000000000000", len(files)+1), ObjectKey: file.Path, Bytes: file.Evidence.Bytes, SHA256: file.Evidence.SHA256, RowCount: file.RowCount})
			return os.Remove(bundle.Payload.Path)
		})
		convertTime += time.Since(convertStarted)
		if err != nil || summary.SelectedRecordCount != end-start || summary.DuckDBVersion != "v2.0.0-dev84020" || summary.SelectedIdentitySHA256 != nativeIdentityHash(expectedIDs) || len(expectedPartitions) != 0 {
			t.Fatalf("convert batch=%d summary=%+v err=%v", batchNo, summary, err)
		}
		for _, path := range []string{stage.Name(), journal.Name()} {
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
		}
		if batchNo%10 == 0 || end == rows {
			t.Logf("native progress rows=%d/%d analytics_bytes=%d elapsed=%s", end, rows, parquetBytes, time.Since(started).Round(time.Millisecond))
		}
	}
	for _, test := range nativeCountCases(rows) {
		t.Run(test.name, func(t *testing.T) {
			plan := nativeFilterPlan(t, test.filter, test.start, test.end, rows)
			operation, err := query.BuildAggregateOperation(query.AggregateOperationSpec{Plan: plan, Metrics: []query.AggregateMetric{{Name: "count", Op: "count"}}})
			if err != nil {
				t.Fatal(err)
			}
			result := nativeExecute(t, ctx, binaryPath, root, files, operation)
			if len(result) != 1 || string(result[0]["m0_valid"]) != strconv.FormatInt(test.want, 10) || string(result[0]["m0_excluded"]) != "0" {
				t.Fatalf("native count=%s want=%d", result, test.want)
			}
		})
	}
	// Two keyset pages: independent top-K includes exact equal-time tie breaking.
	var cursor *query.CursorTuple
	for page := 0; page < 2; page++ {
		operation, err := query.BuildRowOperation(query.RowOperationSpec{Plan: nativeFilterPlan(t, "true", nativeEpoch-3601, nativeEpoch+int64(rows), rows), Sort: "event_desc", Limit: 100, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		result := nativeExecute(t, ctx, binaryPath, root, files, operation)
		if len(result) != 101 {
			t.Fatalf("page %d rows=%d", page, len(result))
		}
		for index, value := range result {
			want := top[page*100+index]
			var id, kind string
			var eventUS int64
			var remainder int
			if json.Unmarshal(value["record_id"], &id) != nil || json.Unmarshal(value["kind"], &kind) != nil || json.Unmarshal(value["event_time_us"], &eventUS) != nil || json.Unmarshal(value["event_time_ns_remainder"], &remainder) != nil || id != want.ID || eventUS != want.EventUS || kind != want.Kind || remainder != 123 {
				t.Fatalf("page %d row %d got=%s want=%+v", page, index, value, want)
			}
		}
		last := top[page*100+99]
		ns := 123
		cursor = &query.CursorTuple{EventUS: &last.EventUS, EventNS: &ns, RecordID: last.ID}
	}
	if t.Failed() {
		t.Fatal("native oracle result comparisons failed")
	}
	runtime.ReadMemStats(&allocatedAfter)
	for _, line := range strings.Split(nativeCgroup(t, "memory.events"), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && (fields[0] == "oom" || fields[0] == "oom_kill") && fields[1] != "0" {
			t.Fatalf("cgroup OOM: %s", line)
		}
	}
	t.Logf("native oracle PASS version=native-fixture-v1 rows=%d actual_envelope_sha256=%x analytics_files=%d analytics_bytes=%d journal_bytes=%d normalize_ms=%d conversion_ms=%d elapsed_ms=%d supervisor_allocated_bytes=%d cgroup_peak_bytes=%s s3_requests=0 network_bytes=0", rows, digest.Sum(nil), len(files), parquetBytes, journalBytes, normalizeTime.Milliseconds(), convertTime.Milliseconds(), time.Since(started).Milliseconds(), allocatedAfter.TotalAlloc-allocatedBefore.TotalAlloc, nativeCgroup(t, "memory.peak"))
}

type nativeRow struct {
	ID, Kind string
	EventUS  int64
}

func nativeIdentityHash(ids []string) string {
	sort.Strings(ids)
	digest := sha256.New()
	for _, id := range ids {
		io.WriteString(digest, id)
		io.WriteString(digest, "\n")
	}
	return fmt.Sprintf("%x", digest.Sum(nil))
}

func nativeExpectedRow(index, start int, acceptance string) nativeRow {
	item, ordinal := 0, index-start-(index-start+20)/21
	kind := "log"
	if index%21 == 0 {
		kind, item, ordinal = "error", 1+(index-start)/21, 0
	}
	// Independent contract encoding, not ingest.recordID or normalized output.
	var encoded bytes.Buffer
	encoded.WriteString("eventglass-record-id-v1\x00")
	for _, part := range []string{"1", acceptance, strconv.Itoa(item), strconv.Itoa(ordinal)} {
		_ = binary.Write(&encoded, binary.BigEndian, uint64(len(part)))
		encoded.WriteString(part)
	}
	second := nativeEpoch + int64(index/2)
	if index%97 == 0 {
		second -= 3600
	}
	return nativeRow{ID: fmt.Sprintf("%x", sha256.Sum256(encoded.Bytes())), Kind: kind, EventUS: second * 1_000_000}
}

func nativeEnvelope(t *testing.T, start, end int) []byte {
	t.Helper()
	logs, events := []any{}, []any{}
	for index := start; index < end; index++ {
		second := nativeEpoch + int64(index/2)
		if index%97 == 0 {
			second -= 3600
		}
		payload := map[string]any{"timestamp": json.Number(fmt.Sprintf("%d.000000123", second))}
		if index%21 == 0 {
			payload["message"] = fmt.Sprintf("native row %d", index)
			if index%17 != 0 {
				payload["event_id"] = fmt.Sprintf("%032x", index+1)
			}
			events = append(events, payload)
		} else {
			payload["body"] = fmt.Sprintf("native row %d", index)
			attrs := map[string]any{}
			if index%29 != 0 {
				var value any
				if index%31 != 0 {
					number := strconv.Itoa(index%1000 - 500)
					switch index % 3 {
					case 0:
						value = json.Number(number)
					case 1:
						value = json.Number(number + ".25")
					case 2:
						value = number
					}
				}
				attrs["v"] = value
			}
			if index%23 == 0 {
				attrs["a.b"] = "literal"
			} else {
				attrs["a"] = map[string]any{"b": "nested object"}
			}
			payload["attributes"] = attrs
			logs = append(logs, payload)
		}
	}
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	if err := encoder.Encode(map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Encode(map[string]any{"type": "log", "item_count": len(logs)}); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Encode(map[string]any{"items": logs}); err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if err := encoder.Encode(map[string]any{"type": "event"}); err != nil {
			t.Fatal(err)
		}
		if err := encoder.Encode(event); err != nil {
			t.Fatal(err)
		}
	}
	return body.Bytes()
}

func nativeRetryOracle(t *testing.T, envelope sdk.Envelope, options ingest.NormalizeOptions, first model.NormalizedRequest) {
	t.Helper()
	retry, err := ingest.NormalizeEnvelope(envelope, options)
	if err != nil || !reflect.DeepEqual(first, retry) {
		t.Fatal("same-acceptance retry changed canonical data")
	}
	options.AcceptanceID = "00000000-0000-4000-9000-000000000001"
	duplicate, err := ingest.NormalizeEnvelope(envelope, options)
	if err != nil {
		t.Fatal(err)
	}
	for index, record := range first.Records {
		other := duplicate.Records[index]
		if record.RecordID == other.RecordID {
			t.Fatal("new acceptance reused occurrence ID")
		}
		left, eligible, err := ingest.DedupePayloadSHA256(record)
		right, otherEligible, otherErr := ingest.DedupePayloadSHA256(other)
		wantEligible := record.Kind == model.KindError && record.SourceEventID != nil
		if err != nil || otherErr != nil || eligible != wantEligible || otherEligible != eligible || left != right {
			t.Fatal("duplicate payload key changed across acceptances")
		}
	}
}

type nativeCountCase struct {
	name, filter     string
	start, end, want int64
}

func nativeCountCases(rows int) []nativeCountCase {
	allStart, allEnd := nativeEpoch-3601, nativeEpoch+int64(rows)
	cases := []nativeCountCase{
		{"all", "true", allStart, allEnd, int64(rows)},
		{"errors", `kind == "error"`, allStart, allEnd, 0},
		{"integer", `iattr("attributes", "/v") > 100`, allStart, allEnd, 0},
		{"double", `dattr("attributes", "/v") > 100.0`, allStart, allEnd, 0},
		{"string", `sattr("attributes", "/v") == "101"`, allStart, allEnd, 0},
		{"null", `is_null("attributes", "/v")`, allStart, allEnd, 0},
		{"missing_logs", `kind == "log" && !exists("attributes", "/v")`, allStart, allEnd, 0},
		{"literal_dot", `exists("attributes", "/a.b")`, allStart, allEnd, 0},
		{"not_nested_pointer", `exists("attributes", "/a/b")`, allStart, allEnd, 0},
		{"half_open_time", "true", nativeEpoch + 123, nativeEpoch + 456, 0},
	}
	// Arithmetic Go oracle does not parse the filter or inspect generated/normalized data.
	for index := 0; index < rows; index++ {
		second := nativeEpoch + int64(index/2)
		if index%97 == 0 {
			second -= 3600
		}
		if second >= cases[9].start && second < cases[9].end {
			cases[9].want++
		}
		if index%21 == 0 {
			cases[1].want++
			continue
		}
		if index%23 == 0 {
			cases[7].want++
		}
		if index%29 == 0 {
			cases[6].want++
			continue
		}
		if index%31 == 0 {
			cases[5].want++
			continue
		}
		value := index%1000 - 500
		if index%3 == 0 && value > 100 {
			cases[2].want++
		}
		if index%3 == 1 && float64(value)+0.25 > 100 {
			cases[3].want++
		}
		if index%3 == 2 && value == 101 {
			cases[4].want++
		}
	}
	return cases
}

func nativeFilterPlan(t *testing.T, filter string, start, end int64, rows int) query.CompiledPlan {
	t.Helper()
	node, err := query.ParseCEL(filter)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := query.CanonicalFilter(node)
	if err != nil {
		t.Fatal(err)
	}
	var cuts [model.LaneCount]int64
	for index := range cuts {
		cuts[index] = int64(rows)
	}
	plan, err := query.BuildPlan(model.DatasetSpec{TenantID: 1, ProjectIDs: []int64{1}, Kinds: []model.Kind{model.KindError, model.KindLog}, TimeBasis: model.QueryTimeEvent, StartUS: start * 1_000_000, EndUS: end * 1_000_000, Filter: canonical}, model.SnapshotScope{LaneCuts: cuts, RetentionFloorUS: nativeEpoch * 1_000_000}, node)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func nativeExecute(t *testing.T, ctx context.Context, binaryPath, root string, files []model.CatalogFile, operation engine.QueryOperation) []map[string]json.RawMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	directory, err := os.MkdirTemp(root, "query-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(directory)
	encoded, err := query.CanonicalOperation(operation)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := query.BuildExecutionPlan(query.PlanScope{QueryID: "00000000-0000-4000-8000-000000000101", TenantID: 1, SnapshotID: "00000000-0000-4000-8000-000000000102", Generation: 1, OperationHash: fmt.Sprintf("%x", sha256.Sum256(encoded)), Operation: encoded, DeadlineUS: time.Now().Add(10 * time.Minute).UnixMicro()}, files)
	if err != nil {
		t.Fatal(err)
	}
	outputs := map[model.QueryTaskKey]string{}
	started := time.Now()
	var final string
	var outputBytes int64
	for index, task := range plan.Tasks {
		var manifest query.TaskManifest
		if err := json.Unmarshal(task.Manifest, &manifest); err != nil {
			t.Fatal(err)
		}
		var inputs []string
		for _, file := range manifest.Files {
			inputs = append(inputs, file.ObjectKey)
		}
		for _, input := range task.InputOrdinal {
			path, ok := outputs[input.Producer]
			if !ok {
				t.Fatal("missing producer")
			}
			inputs = append(inputs, path)
		}
		final = filepath.Join(directory, fmt.Sprintf("%06d.parquet", index))
		summary, err := (app.ProcessQueryRunner{BinaryPath: binaryPath}).Run(ctx, engine.QueryRequest{Version: 1, QueryID: "native-oracle", Task: task.Key, Operation: operation, InputPaths: inputs, OutputPath: final, SpillDirectory: final + ".spill", NativeMemoryBytes: 192 << 20, NativeSpillBytes: 256 << 20})
		if err != nil {
			t.Fatalf("task=%+v: %v", task.Key, err)
		}
		outputBytes += summary.Evidence.Bytes
		if outputBytes > 64<<20 || summary.Rows > 101 || operation.Kind == "aggregate" && summary.Rows != 1 {
			t.Fatalf("fixture query output budget: rows=%d bytes=%d", summary.Rows, outputBytes)
		}
		outputs[task.Key] = final
	}
	jsonPath := filepath.Join(directory, "result.jsonl")
	_, err = (app.ProcessQueryExportRunner{BinaryPath: binaryPath}).Export(ctx, engine.QueryExportRequest{Version: 1, InputPath: final, OutputPath: jsonPath})
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(jsonPath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, engine.MaxPublicQueryResultBytes+1))
	var result []map[string]json.RawMessage
	for {
		var row map[string]json.RawMessage
		err := decoder.Decode(&row)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, row)
		if len(result) > 202 {
			t.Fatal("unexpectedly large fixture result")
		}
	}
	t.Logf("native query kind=%s scans=%d reducers=%d rows=%d elapsed_ms=%d", operation.Kind, plan.ScanCount, plan.ReducerCount, len(result), time.Since(started).Milliseconds())
	return result
}

func nativeCgroup(t *testing.T, name string) string {
	t.Helper()
	encoded, err := os.ReadFile(filepath.Join("/sys/fs/cgroup", name))
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(encoded))
}
