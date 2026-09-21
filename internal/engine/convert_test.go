package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

func writeConversionStage(t testing.TB, directory string, records []StageRecord) string {
	t.Helper()
	path := filepath.Join(directory, "selected.jsonl")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(file)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func BenchmarkConvertSelectedBatch(b *testing.B) {
	for _, count := range []int{1, 100} {
		b.Run(fmt.Sprintf("records=%d", count), func(b *testing.B) {
			root := b.TempDir()
			records := make([]StageRecord, count)
			for index := range records {
				records[index] = conversionRecord("a", model.KindLog, 1_700_000_000_000_000+int64(index), index)
				records[index].Record.RecordID = fmt.Sprintf("%064x", index+1)
			}
			request := ConversionRequest{
				Version: ConversionProtocolVersion, StagePath: writeConversionStage(b, root, records),
				OutputDirectory: filepath.Join(root, "output"), SpillDirectory: filepath.Join(root, "spill"),
				TenantID: 1, LaneID: 3, BatchSeq: 4, BatchID: records[0].BatchID, SelectedRecords: count,
				NativeMemoryBytes: 256 << 20, NativeSpillBytes: 128 << 20,
			}
			b.ReportAllocs()
			for b.Loop() {
				summary, err := Convert(context.Background(), request, func(ConvertedBundle) error { return nil })
				if err != nil || summary.SelectedRecordCount != count || summary.BundleCount != 1 {
					b.Fatalf("summary=%+v err=%v", summary, err)
				}
				if err := os.RemoveAll(request.OutputDirectory); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkConversionPhaseCosts(b *testing.B) {
	root := b.TempDir()
	record := conversionRecord("a", model.KindLog, 1_700_000_000_000_000, 0)
	request := ConversionRequest{
		Version: ConversionProtocolVersion, StagePath: writeConversionStage(b, root, []StageRecord{record}),
		OutputDirectory: filepath.Join(root, "output"), SpillDirectory: filepath.Join(root, "spill"),
		TenantID: 1, LaneID: 3, BatchSeq: 4, BatchID: record.BatchID, SelectedRecords: 1,
		NativeMemoryBytes: 256 << 20, NativeSpillBytes: 128 << 20,
	}
	ctx := context.Background()
	var opening, configuring, appending, materializing, writing, closing time.Duration
	for b.Loop() {
		if err := prepareConversionDirectories(request); err != nil {
			b.Fatal(err)
		}
		started := time.Now()
		db, err := Open(ctx, "")
		opening += time.Since(started)
		if err != nil {
			b.Fatal(err)
		}
		started = time.Now()
		if err := configureConversionDB(ctx, db, request); err != nil {
			b.Fatal(err)
		}
		configuring += time.Since(started)
		started = time.Now()
		if count, _, err := appendStage(ctx, db, request); err != nil || count != 1 {
			b.Fatalf("count=%d err=%v", count, err)
		}
		appending += time.Since(started)
		started = time.Now()
		if err := materializeStage(ctx, db); err != nil {
			b.Fatal(err)
		}
		materializing += time.Since(started)
		started = time.Now()
		if bundle, _, err := writePartition(ctx, db, request.OutputDirectory, 0, partition{day: "2023-11-14", kind: model.KindLog}); err != nil || bundle.RowCount != 1 {
			b.Fatalf("rows=%d err=%v", bundle.RowCount, err)
		}
		writing += time.Since(started)
		started = time.Now()
		if err := db.Close(); err != nil {
			b.Fatal(err)
		}
		closing += time.Since(started)
		for _, path := range []string{request.OutputDirectory, request.SpillDirectory} {
			if err := os.RemoveAll(path); err != nil {
				b.Fatal(err)
			}
		}
	}
	for name, elapsed := range map[string]time.Duration{"open": opening, "configure": configuring, "append": appending, "materialize": materializing, "write_inspect": writing, "close": closing} {
		b.ReportMetric(float64(elapsed.Nanoseconds())/float64(b.N), name+"-ns/op")
	}
}

func conversionRecord(idCharacter string, kind model.Kind, eventTimeUS int64, ordinal int) StageRecord {
	integer := "12345678901234567890123456789012345678"
	record := model.Record{
		TenantID: 1, ProjectID: 10, RecordID: strings.Repeat(idCharacter, 64), AcceptanceID: "00000000-0000-4000-8000-000000000111",
		ItemOrdinal: ordinal, RecordOrdinal: 0, Kind: kind, EventTimeUS: eventTimeUS, EventTimeNSRemainder: 999,
		ArrivalTimeUS: eventTimeUS + 5, Message: "message", Level: "error", Raw: json.RawMessage(`{"message":"message","secret":"[Filtered]"}`),
		Attrs:        []model.Attribute{{Namespace: "attributes", Path: "/large", ValueType: "integer", IntegerValue: &integer}},
		SearchValues: []string{"message"}, Warnings: []string{"fixture"}, SchemaVersion: model.SchemaVersion,
		NormalizerVersion: model.NormalizerVersion, ScrubVersion: model.ScrubVersion,
	}
	staged := StageRecord{
		Version: ConversionProtocolVersion, GlobalOrdinal: ordinal, BatchID: "00000000-0000-4000-8000-000000000222",
		LaneID: 3, BatchSeq: 4, ReceivedTimeUS: eventTimeUS + 10, GroupingVersion: 1, Record: record,
	}
	if kind == model.KindError {
		staged.IssueID = strings.Repeat("f", 64)
		staged.FingerprintSHA256 = staged.IssueID
		staged.IssueTitle = "message"
	}
	return staged
}

func TestConvertBulkAppendPairedSchemaIdentityAndBounds(t *testing.T) {
	root := t.TempDir()
	records := []StageRecord{
		conversionRecord("a", model.KindError, 1_700_000_000_000_000, 0),
		conversionRecord("b", model.KindError, 1_700_000_001_000_000, 1),
		conversionRecord("c", model.KindLog, 1_700_086_400_000_000, 2),
	}
	request := ConversionRequest{
		Version: ConversionProtocolVersion, StagePath: writeConversionStage(t, root, records),
		OutputDirectory: filepath.Join(root, "output"), SpillDirectory: filepath.Join(root, "spill"),
		TenantID: 1, LaneID: 3, BatchSeq: 4, BatchID: records[0].BatchID, SelectedRecords: 3, SelectedErrors: 2,
		NativeMemoryBytes: 64 << 20, NativeSpillBytes: 128 << 20,
	}
	var bundles []ConvertedBundle
	summary, err := Convert(context.Background(), request, func(bundle ConvertedBundle) error {
		bundles = append(bundles, bundle)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(summary.DuckDBVersion, "v2.0.0-dev84020") || summary.SelectedRecordCount != 3 || summary.SelectedErrorCount != 2 || summary.BundleCount != 2 || len(bundles) != 2 {
		t.Fatalf("summary=%#v bundles=%d", summary, len(bundles))
	}
	for _, bundle := range bundles {
		if bundle.RowCount <= 0 || bundle.Analytics.RowCount != bundle.RowCount || bundle.Payload.RowCount != bundle.RowCount || bundle.Analytics.Evidence.Bytes > MaxBundleFileBytes || bundle.Payload.Evidence.Bytes > MaxBundleFileBytes {
			t.Fatalf("bundle=%#v", bundle)
		}
		if err := storage.VerifyFile(bundle.Analytics.Path, bundle.Analytics.Evidence); err != nil {
			t.Fatal(err)
		}
		if err := storage.VerifyFile(bundle.Payload.Path, bundle.Payload.Evidence); err != nil {
			t.Fatal(err)
		}
	}
	db, err := Open(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var decimal, compression string
	if err := db.QueryRow(`SELECT attrs[1].integer_value::VARCHAR FROM read_parquet(?) WHERE record_id=?`, bundles[0].Analytics.Path, strings.Repeat("a", 64)).Scan(&decimal); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT min(compression) FROM parquet_metadata(?)`, bundles[0].Analytics.Path).Scan(&compression); err != nil {
		t.Fatal(err)
	}
	if decimal != "12345678901234567890123456789012345678" || compression != "ZSTD" {
		t.Fatalf("decimal=%q compression=%q", decimal, compression)
	}
	var raw, metadata string
	if err := db.QueryRow(`SELECT raw_json,canonical_metadata_json FROM read_parquet(?) WHERE record_id=?`, bundles[0].Payload.Path, strings.Repeat("a", 64)).Scan(&raw, &metadata); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(raw, `"[Filtered]"`) || strings.Contains(metadata, `"raw"`) || strings.Contains(metadata, `"envelope_sdk_json"`) {
		t.Fatalf("raw=%s metadata=%s", raw, metadata)
	}
	if _, err := os.Stat(request.SpillDirectory); !os.IsNotExist(err) {
		t.Fatalf("spill directory survived: %v", err)
	}
}

func TestConvertTypedStageColumnsPreserveNullsAndIntegerPrecision(t *testing.T) {
	root := t.TempDir()
	empty, named := "", "검색 'service'"
	services := []*string{nil, &empty, &named}
	records := make([]StageRecord, len(services))
	for index := range records {
		records[index] = conversionRecord(string("abc"[index]), model.KindLog, 1_700_000_000_000_000+int64(index), index)
		records[index].Record.TenantID = 9_007_199_254_740_993
		records[index].Record.ProjectID = 9_007_199_254_740_994 + int64(index)
		records[index].Record.Service = services[index]
		records[index].BatchSeq = 9_007_199_254_741_000
	}
	request := ConversionRequest{
		Version: ConversionProtocolVersion, StagePath: writeConversionStage(t, root, records),
		OutputDirectory: filepath.Join(root, "output"), SpillDirectory: filepath.Join(root, "spill"),
		TenantID: records[0].Record.TenantID, LaneID: 3, BatchSeq: records[0].BatchSeq, BatchID: records[0].BatchID, SelectedRecords: len(records),
	}
	var bundle ConvertedBundle
	if _, err := Convert(context.Background(), request, func(got ConvertedBundle) error { bundle = got; return nil }); err != nil {
		t.Fatal(err)
	}
	db, err := Open(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT tenant_id,project_id,record_id,batch_seq,kind,event_time_us,received_time_us,service FROM read_parquet(?) ORDER BY record_id`, bundle.Analytics.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	for index, expected := range []string{"BIGINT", "BIGINT", "VARCHAR", "BIGINT", "VARCHAR", "BIGINT", "BIGINT", "VARCHAR"} {
		if got := columns[index].DatabaseTypeName(); got != expected {
			t.Fatalf("column %d type=%s want=%s", index, got, expected)
		}
	}
	count := 0
	for rows.Next() {
		var tenant, project, sequence, event, received int64
		var id, kind string
		var service *string
		if err := rows.Scan(&tenant, &project, &id, &sequence, &kind, &event, &received, &service); err != nil {
			t.Fatal(err)
		}
		if count >= len(records) {
			t.Fatal("unexpected extra row")
		}
		want := records[count]
		if tenant != want.Record.TenantID || project != want.Record.ProjectID || id != want.Record.RecordID || sequence != want.BatchSeq || kind != string(want.Record.Kind) || event != want.Record.EventTimeUS || received != want.ReceivedTimeUS ||
			(service == nil) != (want.Record.Service == nil) || service != nil && *service != *want.Record.Service {
			t.Fatalf("typed stage projection differs at row %d", count)
		}
		count++
	}
	if err := rows.Err(); err != nil || count != len(records) {
		t.Fatalf("rows=%d err=%v", count, err)
	}
}

func TestConvertAttributeTypeReusePreservesValuesAndNulls(t *testing.T) {
	root := t.TempDir()
	text, integer, unit := "검색 'quoted'", "-99999999999999999999999999999999999999", "millisecond"
	fraction, flag := 0.125, false
	records := []StageRecord{
		conversionRecord("a", model.KindLog, 1_700_000_000_000_000, 0),
		conversionRecord("b", model.KindLog, 1_700_000_000_000_001, 1),
		conversionRecord("c", model.KindLog, 1_700_000_000_000_002, 2),
	}
	records[0].Record.Attrs = []model.Attribute{
		{Namespace: "attributes", Path: "/a", ValueType: "string", StringValue: &text},
		{Namespace: "attributes", Path: "/b", ValueType: "integer", IntegerValue: &integer},
		{Namespace: "attributes", Path: "/c", ValueType: "double", DoubleValue: &fraction, Unit: &unit},
		{Namespace: "attributes", Path: "/d", ValueType: "boolean", BooleanValue: &flag},
		{Namespace: "attributes", Path: "/e", ValueType: "json", JSONValue: json.RawMessage(`{"nested":[1,true,null]}`)},
		{Namespace: "attributes", Path: "/f", ValueType: "null"},
	}
	records[1].Record.Attrs = nil
	records[2].Record.Attrs = []model.Attribute{}
	request := ConversionRequest{
		Version: ConversionProtocolVersion, StagePath: writeConversionStage(t, root, records),
		OutputDirectory: filepath.Join(root, "output"), SpillDirectory: filepath.Join(root, "spill"),
		TenantID: 1, LaneID: 3, BatchSeq: 4, BatchID: records[0].BatchID, SelectedRecords: len(records),
	}
	var bundle ConvertedBundle
	if _, err := Convert(context.Background(), request, func(got ConvertedBundle) error { bundle = got; return nil }); err != nil {
		t.Fatal(err)
	}
	// A fresh connection has no conversion-local type alias. Read the persisted
	// structure, not its in-memory type name, including exact DECIMAL(38,0).
	db, err := Open(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var decimalType, doubleType, boolType string
	if err := db.QueryRow(`SELECT typeof(attrs[1].integer_value),typeof(attrs[1].double_value),typeof(attrs[1].boolean_value) FROM read_parquet(?) LIMIT 1`, bundle.Analytics.Path).Scan(&decimalType, &doubleType, &boolType); err != nil {
		t.Fatal(err)
	}
	if decimalType != "DECIMAL(38,0)" || doubleType != "DOUBLE" || boolType != "BOOLEAN" {
		t.Fatalf("attribute types changed: %s/%s/%s", decimalType, doubleType, boolType)
	}
	rows, err := db.Query(`SELECT a.namespace,a.path,a.value_type,a.string_value,a.integer_value::VARCHAR,a.double_value,a.boolean_value,a.json_value,a.unit
		FROM (SELECT unnest(attrs) AS a FROM read_parquet(?)) ORDER BY a.path`, bundle.Analytics.Path)
	if err != nil {
		t.Fatal(err)
	}
	index := 0
	for rows.Next() {
		var got model.Attribute
		var raw *string
		if err := rows.Scan(&got.Namespace, &got.Path, &got.ValueType, &got.StringValue, &got.IntegerValue, &got.DoubleValue, &got.BooleanValue, &raw, &got.Unit); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if raw != nil {
			got.JSONValue = json.RawMessage(*raw)
		}
		if index >= len(records[0].Record.Attrs) || !reflect.DeepEqual(got, records[0].Record.Attrs[index]) {
			rows.Close()
			t.Fatalf("attribute %d differs: %+v", index, got)
		}
		index++
	}
	err = rows.Err()
	rows.Close()
	if err != nil || index != len(records[0].Record.Attrs) {
		t.Fatalf("attributes=%d err=%v", index, err)
	}
	var empty int
	if err := db.QueryRow(`SELECT count(*) FROM read_parquet(?) WHERE array_length(attrs)=0`, bundle.Analytics.Path).Scan(&empty); err != nil || empty != 2 {
		t.Fatalf("NULL/empty arrays: count=%d err=%v", empty, err)
	}
}

func TestConvertCancellationRemovesPartialOutputsAndSpill(t *testing.T) {
	root := t.TempDir()
	records := make([]StageRecord, 3000)
	for index := range records {
		id := strings.Repeat("0", 56) + strings.Repeat("a", 8)
		id = id[:56] + strings.ToLower(strings.Repeat(string("0123456789abcdef"[index%16]), 8))
		records[index] = conversionRecord("a", model.KindLog, 1_700_000_000_000_000+int64(index), index)
		records[index].Record.RecordID = id
	}
	// Preserve uniqueness without relying on a huge fixture in memory.
	stagePath := filepath.Join(root, "selected.jsonl")
	stage, err := os.Create(stagePath)
	if err != nil {
		t.Fatal(err)
	}
	writer := bufio.NewWriter(stage)
	for index := range records {
		records[index].Record.RecordID = strings.Repeat("0", 56) + strings.ToLower(hex8(index))
		encoded, _ := json.Marshal(records[index])
		writer.Write(encoded)
		writer.WriteByte('\n')
	}
	writer.Flush()
	stage.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := ConversionRequest{
		Version: 1, StagePath: stagePath, OutputDirectory: filepath.Join(root, "output"), SpillDirectory: filepath.Join(root, "spill"),
		TenantID: 1, LaneID: 3, BatchSeq: 4, BatchID: records[0].BatchID, SelectedRecords: len(records), NativeMemoryBytes: 64 << 20, NativeSpillBytes: 128 << 20,
	}
	if _, err := Convert(ctx, request, func(ConvertedBundle) error { return nil }); err == nil {
		t.Fatal("canceled conversion succeeded")
	}
	entries, err := os.ReadDir(request.OutputDirectory)
	if err != nil || len(entries) != 0 {
		t.Fatalf("partial outputs=%v err=%v", entries, err)
	}
	if _, err := os.Stat(request.SpillDirectory); !os.IsNotExist(err) {
		t.Fatalf("spill directory survived cancellation: %v", err)
	}
}

func TestConvertMaximumCanonicalRecordStaysBelowHardFileTarget(t *testing.T) {
	root := t.TempDir()
	payload := make([]byte, (1<<20)-8192)
	for index := range payload {
		payload[index] = "abcdefghijklmnopqrstuvwxyz012345"[index%32]
	}
	raw, err := json.Marshal(map[string]string{"body": string(payload)})
	if err != nil {
		t.Fatal(err)
	}
	staged := conversionRecord("d", model.KindLog, 1_700_000_000_000_000, 0)
	staged.Record.Raw = raw
	staged.Record.Message = "large"
	request := ConversionRequest{
		Version: 1, StagePath: writeConversionStage(t, root, []StageRecord{staged}), OutputDirectory: filepath.Join(root, "output"), SpillDirectory: filepath.Join(root, "spill"),
		TenantID: 1, LaneID: 3, BatchSeq: 4, BatchID: staged.BatchID, SelectedRecords: 1,
	}
	var bundle ConvertedBundle
	summary, err := Convert(context.Background(), request, func(value ConvertedBundle) error { bundle = value; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if summary.BundleCount != 1 || bundle.Analytics.Evidence.Bytes <= 0 || bundle.Payload.Evidence.Bytes <= 0 || bundle.Analytics.Evidence.Bytes > MaxBundleFileBytes || bundle.Payload.Evidence.Bytes > MaxBundleFileBytes {
		t.Fatalf("summary=%#v analytics=%d payload=%d", summary, bundle.Analytics.Evidence.Bytes, bundle.Payload.Evidence.Bytes)
	}
}

func TestConvertWideDateSelectionStreamsPartitionsInOrder(t *testing.T) {
	root := t.TempDir()
	const dayCount = 32
	records := make([]StageRecord, dayCount)
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	for index := range records {
		records[index] = conversionRecord("e", model.KindLog, start.AddDate(0, 0, index).UnixMicro(), index)
		records[index].Record.RecordID = strings.Repeat("0", 56) + hex8(index+1)
	}
	request := ConversionRequest{
		Version: 1, StagePath: writeConversionStage(t, root, records), OutputDirectory: filepath.Join(root, "output"), SpillDirectory: filepath.Join(root, "spill"),
		TenantID: 1, LaneID: 3, BatchSeq: 4, BatchID: records[0].BatchID, SelectedRecords: dayCount,
	}
	emitted := 0
	summary, err := Convert(context.Background(), request, func(bundle ConvertedBundle) error {
		expectedDay := start.AddDate(0, 0, emitted).Format(time.DateOnly)
		if bundle.Index != emitted || bundle.EventDay != expectedDay || bundle.RowCount != 1 {
			t.Fatalf("bundle=%#v expected day=%s", bundle, expectedDay)
		}
		emitted++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if emitted != dayCount || summary.BundleCount != dayCount || summary.SelectedRecordCount != dayCount {
		t.Fatalf("emitted=%d summary=%#v", emitted, summary)
	}
}

func hex8(value int) string {
	const digits = "0123456789abcdef"
	result := make([]byte, 8)
	for index := len(result) - 1; index >= 0; index-- {
		result[index] = digits[value&15]
		value >>= 4
	}
	return string(result)
}
