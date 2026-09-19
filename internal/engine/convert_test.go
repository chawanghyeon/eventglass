package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

func writeConversionStage(t *testing.T, directory string, records []StageRecord) string {
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
