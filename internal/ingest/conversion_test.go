package ingest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/storage"
	"github.com/klauspost/compress/zstd"
)

type countingConversionRunner struct{ calls int }

func (runner *countingConversionRunner) Run(context.Context, engine.ConversionRequest, func(engine.ConvertedBundle) error) (engine.ConversionSummary, error) {
	runner.calls++
	return engine.ConversionSummary{}, nil
}

func conversionFixture(t *testing.T, classes []model.ReceiptClass) ConversionInput {
	t.Helper()
	acceptanceID := "00000000-0000-4000-8000-000000000111"
	laneID, err := model.LaneForAcceptance(acceptanceID)
	if err != nil {
		t.Fatal(err)
	}
	records := []model.Record{
		{
			TenantID: 17, ProjectID: 29, RecordID: strings.Repeat("a", 64), AcceptanceID: acceptanceID,
			ItemOrdinal: 0, RecordOrdinal: 0, Kind: model.KindError, EventTimeUS: 1_700_000_000_000_000,
			Message: "boom", Raw: json.RawMessage(`{"message":"boom"}`), Attrs: []model.Attribute{}, SearchValues: []string{"boom"}, Warnings: []string{},
			SchemaVersion: model.SchemaVersion, NormalizerVersion: model.NormalizerVersion, ScrubVersion: model.ScrubVersion,
		},
		{
			TenantID: 17, ProjectID: 29, RecordID: strings.Repeat("b", 64), AcceptanceID: acceptanceID,
			ItemOrdinal: 1, RecordOrdinal: 0, Kind: model.KindLog, EventTimeUS: 1_700_086_400_000_000,
			Message: "log", Raw: json.RawMessage(`{"body":"log"}`), Attrs: []model.Attribute{}, SearchValues: []string{"log"}, Warnings: []string{},
			SchemaVersion: model.SchemaVersion, NormalizerVersion: model.NormalizerVersion, ScrubVersion: model.ScrubVersion,
		},
	}
	batchID := "00000000-0000-4000-8000-000000000222"
	journalPath := filepath.Join(t.TempDir(), "journal.zst")
	journal, err := os.OpenFile(journalPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	info, err := storage.WriteJournal(journal, model.JournalBatch{BatchID: batchID, TenantID: 17, LaneID: laneID, Requests: []model.NormalizedRequest{{
		TenantID: 17, ProjectID: 29, AcceptanceID: acceptanceID, Records: records,
	}}})
	if closeErr := journal.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	selection, err := model.NewReceiptSelection(classes)
	if err != nil {
		t.Fatal(err)
	}
	selectionSHA, err := selection.SHA256(len(records))
	if err != nil {
		t.Fatal(err)
	}
	index := info.Index.Requests[0]
	return ConversionInput{
		JournalPath: journalPath, Journal: info, TenantID: 17, LaneID: laneID, BatchSeq: 7, BatchID: batchID, TaskDir: filepath.Join(t.TempDir(), "task"),
		Receipts: []ConversionReceipt{{GroupingVersion: 1, Receipt: control.ReceiptResult{
			AcceptanceID: acceptanceID, TenantID: 17, ProjectID: 29, LaneID: laneID, BatchSeq: 7,
			ContentSHA256: index.ContentSHA256, Selection: selection, SelectionSHA256: selectionSHA,
			OrdinalFirst: index.OrdinalFirst, OrdinalLast: index.OrdinalLast, AcceptedCount: selectionAcceptedCount(classes), ReceivedTimeUS: 1_800_000_000_000_000,
		}}},
	}
}

func selectionAcceptedCount(classes []model.ReceiptClass) int {
	total := 0
	for _, class := range classes {
		if class == model.ReceiptAccepted {
			total++
		}
	}
	return total
}

func TestStageConversionUsesDurableSelectionAndPersistsErrorSummary(t *testing.T) {
	input := conversionFixture(t, []model.ReceiptClass{model.ReceiptAccepted, model.ReceiptDuplicate})
	artifacts, err := StageConversion(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	defer artifacts.Cleanup()
	if artifacts.SelectedRecordCount != 1 || artifacts.SelectedErrorCount != 1 || len(artifacts.OccurrenceSHA256) != 64 || artifacts.Request.SelectedRecords != 1 {
		t.Fatalf("artifacts=%#v", artifacts)
	}
	stageBytes, err := os.ReadFile(artifacts.Request.StagePath)
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(stageBytes), []byte{'\n'})
	if len(lines) != 1 || !bytes.Contains(lines[0], []byte(strings.Repeat("a", 64))) || bytes.Contains(lines[0], []byte(strings.Repeat("b", 64))) {
		t.Fatalf("stage=%s", stageBytes)
	}
	occurrenceBytes, err := os.ReadFile(artifacts.OccurrencePath)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(occurrenceBytes)
	if hex.EncodeToString(digest[:]) != artifacts.OccurrenceSHA256 || len(bytes.Split(bytes.TrimSpace(occurrenceBytes), []byte{'\n'})) != 2 {
		t.Fatalf("occurrences=%s", occurrenceBytes)
	}
	replayed := 0
	if err := ReplayOccurrenceSummaries(artifacts.OccurrencePath, artifacts.OccurrenceBytes, artifacts.OccurrenceSHA256, func(summary model.IssueOccurrenceSummary) error {
		replayed++
		if summary.RecordID != strings.Repeat("a", 64) || summary.ReleaseJSON != nil {
			t.Fatalf("summary=%#v", summary)
		}
		return nil
	}); err != nil || replayed != 1 {
		t.Fatalf("replayed=%d err=%v", replayed, err)
	}
}

func TestCorruptFinalJournalLineNeverStartsChildAndDiscardsStage(t *testing.T) {
	input := conversionFixture(t, []model.ReceiptClass{model.ReceiptAccepted, model.ReceiptAccepted})
	compressed, err := os.ReadFile(input.JournalPath)
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := zstd.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := io.ReadAll(decoder)
	decoder.Close()
	if err != nil {
		t.Fatal(err)
	}
	plain = bytes.Replace(plain, []byte(`{"type":"end","value":1}`), []byte(`{"type":"end","value":2}`), 1)
	var rebuilt bytes.Buffer
	encoder, err := zstd.NewWriter(&rebuilt, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := encoder.Write(plain); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(input.JournalPath, rebuilt.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(rebuilt.Bytes())
	input.Journal.Bytes = int64(rebuilt.Len())
	input.Journal.SHA256 = hex.EncodeToString(digest[:])
	runner := &countingConversionRunner{}
	_, artifacts, err := (ConversionWorker{Runner: runner}).Execute(context.Background(), input, func(engine.ConvertedBundle) error { return nil })
	if err == nil || artifacts != nil || runner.calls != 0 {
		t.Fatalf("artifacts=%v calls=%d err=%v", artifacts, runner.calls, err)
	}
	entries, readErr := os.ReadDir(input.TaskDir)
	if readErr != nil || len(entries) != 0 {
		t.Fatalf("staged residue=%v err=%v", entries, readErr)
	}
}

func TestZeroSelectedDoesNotStartChildOrCreateParquet(t *testing.T) {
	input := conversionFixture(t, []model.ReceiptClass{model.ReceiptDuplicate, model.ReceiptConflict})
	runner := &countingConversionRunner{}
	summary, artifacts, err := (ConversionWorker{Runner: runner}).Execute(context.Background(), input, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer artifacts.Cleanup()
	if runner.calls != 0 || summary.BundleCount != 0 || summary.SelectedIdentitySHA256 != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" { // pragma: allowlist secret -- public empty SHA-256
		t.Fatalf("summary=%#v calls=%d", summary, runner.calls)
	}
	entries, err := os.ReadDir(artifacts.Request.OutputDirectory)
	if err != nil || len(entries) != 0 {
		t.Fatalf("zero output files=%v err=%v", entries, err)
	}
}

func TestEmptyReceiptStagesAnEmptyPublication(t *testing.T) {
	acceptanceID := "00000000-0000-4000-8000-000000000111"
	laneID, _ := model.LaneForAcceptance(acceptanceID)
	batchID := "00000000-0000-4000-8000-000000000222"
	journalPath := filepath.Join(t.TempDir(), "empty.zst")
	journal, err := os.Create(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	info, err := storage.WriteJournal(journal, model.JournalBatch{BatchID: batchID, TenantID: 17, LaneID: laneID, Requests: []model.NormalizedRequest{{TenantID: 17, ProjectID: 29, AcceptanceID: acceptanceID}}})
	if closeErr := journal.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	selection, _ := model.NewReceiptSelection([]model.ReceiptClass{})
	selectionSHA, _ := selection.SHA256(0)
	input := ConversionInput{
		JournalPath: journalPath, Journal: info, TenantID: 17, LaneID: laneID, BatchSeq: 1, BatchID: batchID, TaskDir: filepath.Join(t.TempDir(), "task"),
		Receipts: []ConversionReceipt{{GroupingVersion: 1, Receipt: control.ReceiptResult{
			AcceptanceID: acceptanceID, TenantID: 17, ProjectID: 29, LaneID: laneID, BatchSeq: 1,
			ContentSHA256: info.Index.Requests[0].ContentSHA256, Selection: selection, SelectionSHA256: selectionSHA,
			OrdinalFirst: 0, OrdinalLast: -1,
		}}},
	}
	artifacts, err := StageConversion(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	defer artifacts.Cleanup()
	if artifacts.SelectedRecordCount != 0 || artifacts.SelectedErrorCount != 0 {
		t.Fatalf("artifacts=%#v", artifacts)
	}
}

func TestStageConversionAcceptsTenThousandDistinctEventDays(t *testing.T) {
	const count = 10_000
	acceptanceID := "00000000-0000-4000-8000-000000000111"
	laneID, err := model.LaneForAcceptance(acceptanceID)
	if err != nil {
		t.Fatal(err)
	}
	records := make([]model.Record, count)
	classes := make([]model.ReceiptClass, count)
	for index := range records {
		records[index] = model.Record{
			TenantID: 17, ProjectID: 29, RecordID: fmt.Sprintf("%064x", index+1), AcceptanceID: acceptanceID,
			ItemOrdinal: index, Kind: model.KindLog, EventTimeUS: 946_684_800_000_000 + int64(index)*86_400_000_000,
			Message: "x", Raw: json.RawMessage(`{"body":"x"}`), Attrs: []model.Attribute{}, SearchValues: []string{"x"}, Warnings: []string{},
			SchemaVersion: model.SchemaVersion, NormalizerVersion: model.NormalizerVersion, ScrubVersion: model.ScrubVersion,
		}
		classes[index] = model.ReceiptAccepted
	}
	batchID := "00000000-0000-4000-8000-000000000222"
	journalPath := filepath.Join(t.TempDir(), "wide.zst")
	journal, err := os.Create(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	info, err := storage.WriteJournal(journal, model.JournalBatch{BatchID: batchID, TenantID: 17, LaneID: laneID, Requests: []model.NormalizedRequest{{TenantID: 17, ProjectID: 29, AcceptanceID: acceptanceID, Records: records}}})
	if closeErr := journal.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	selection, err := model.NewReceiptSelection(classes)
	if err != nil {
		t.Fatal(err)
	}
	selectionSHA, _ := selection.SHA256(count)
	requestIndex := info.Index.Requests[0]
	input := ConversionInput{
		JournalPath: journalPath, Journal: info, TenantID: 17, LaneID: laneID, BatchSeq: 1, BatchID: batchID, TaskDir: filepath.Join(t.TempDir(), "task"),
		Receipts: []ConversionReceipt{{GroupingVersion: 1, Receipt: control.ReceiptResult{
			AcceptanceID: acceptanceID, TenantID: 17, ProjectID: 29, LaneID: laneID, BatchSeq: 1,
			ContentSHA256: requestIndex.ContentSHA256, Selection: selection, SelectionSHA256: selectionSHA,
			OrdinalFirst: 0, OrdinalLast: count - 1, AcceptedCount: count, ReceivedTimeUS: 1,
		}}},
	}
	artifacts, err := StageConversion(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	defer artifacts.Cleanup()
	stage, err := os.ReadFile(artifacts.Request.StagePath)
	if err != nil {
		t.Fatal(err)
	}
	if artifacts.SelectedRecordCount != count || artifacts.SelectedErrorCount != 0 || bytes.Count(stage, []byte{'\n'}) != count {
		t.Fatalf("selected=%d errors=%d lines=%d", artifacts.SelectedRecordCount, artifacts.SelectedErrorCount, bytes.Count(stage, []byte{'\n'}))
	}
}
