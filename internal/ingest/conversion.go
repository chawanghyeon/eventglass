package ingest

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/issues"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/resource"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

const occurrenceEncodingVersion = 1
const maxConversionStageBytes = 64 << 20

type conversionStageWriter struct {
	writer    io.Writer
	remaining int64
}

func (writer *conversionStageWriter) Write(data []byte) (int, error) {
	if int64(len(data)) > writer.remaining {
		return 0, resource.ErrLimited
	}
	n, err := writer.writer.Write(data)
	writer.remaining -= int64(n)
	return n, err
}

type ConversionReceipt struct {
	Receipt         control.ReceiptResult
	GroupingVersion int
}

type ConversionInput struct {
	JournalPath string
	Journal     storage.JournalInfo
	TenantID    int64
	LaneID      int
	BatchSeq    int64
	BatchID     string
	Receipts    []ConversionReceipt
	TaskDir     string
}

type ConversionArtifacts struct {
	Request             engine.ConversionRequest
	OccurrencePath      string
	OccurrenceBytes     int64
	OccurrenceSHA256    string
	SelectedRecordCount int
	SelectedErrorCount  int
	ReceiptSetSHA256    string
	JournalSHA256       string
	removeOnCleanup     []string
}

func (artifacts *ConversionArtifacts) Cleanup() error {
	var result error
	for _, path := range artifacts.removeOnCleanup {
		result = errors.Join(result, os.RemoveAll(path))
	}
	artifacts.removeOnCleanup = nil
	return result
}

type ConversionRunner interface {
	Run(context.Context, engine.ConversionRequest, func(engine.ConvertedBundle) error) (engine.ConversionSummary, error)
}

type ConversionWorker struct {
	Runner ConversionRunner
}

func (worker ConversionWorker) Execute(ctx context.Context, input ConversionInput, emit func(engine.ConvertedBundle) error) (engine.ConversionSummary, *ConversionArtifacts, error) {
	artifacts, err := StageConversion(ctx, input)
	if err != nil {
		return engine.ConversionSummary{}, nil, err
	}
	if artifacts.SelectedRecordCount == 0 {
		empty := sha256.Sum256(nil)
		return engine.ConversionSummary{
			SelectedIdentitySHA256: hex.EncodeToString(empty[:]),
		}, artifacts, nil
	}
	if worker.Runner == nil || emit == nil {
		_ = artifacts.Cleanup()
		return engine.ConversionSummary{}, nil, errors.New("native conversion runner and bundle emitter are required")
	}
	summary, err := worker.Runner.Run(ctx, artifacts.Request, emit)
	if err != nil {
		_ = artifacts.Cleanup()
		return engine.ConversionSummary{}, nil, err
	}
	if summary.SelectedRecordCount != artifacts.SelectedRecordCount || summary.SelectedErrorCount != artifacts.SelectedErrorCount {
		_ = artifacts.Cleanup()
		return engine.ConversionSummary{}, nil, errors.New("native conversion summary differs from verified selection")
	}
	return summary, artifacts, nil
}

func StageConversion(ctx context.Context, input ConversionInput) (_ *ConversionArtifacts, retErr error) {
	if input.TenantID <= 0 || input.LaneID < 0 || input.LaneID >= model.LaneCount || input.BatchSeq <= 0 || input.BatchID == "" || input.JournalPath == "" || input.TaskDir == "" || len(input.Receipts) == 0 {
		return nil, errors.New("invalid conversion staging input")
	}
	if err := storage.EnsurePrivateDirectory(input.TaskDir); err != nil {
		return nil, err
	}
	if _, err := model.LaneForAcceptance(input.BatchID); err != nil {
		return nil, errors.New("invalid conversion batch ID")
	}
	receipts, receiptHash, expectedAccepted, err := validateConversionReceipts(input)
	if err != nil {
		return nil, err
	}
	journal, err := os.Open(input.JournalPath)
	if err != nil {
		return nil, err
	}
	defer journal.Close()
	stage, err := os.CreateTemp(input.TaskDir, "selected-*.jsonl")
	if err != nil {
		return nil, err
	}
	stagePath := stage.Name()
	defer func() {
		_ = stage.Close()
		if retErr != nil {
			_ = os.Remove(stagePath)
		}
	}()
	stageBuffer := bufio.NewWriterSize(&conversionStageWriter{writer: stage, remaining: maxConversionStageBytes}, 64<<10)
	stageEncoder := json.NewEncoder(stageBuffer)
	summaries := make([]model.IssueOccurrenceSummary, 0)
	selected, selectedErrors := 0, 0
	index, err := storage.ReplayJournal(journal, input.Journal, func(meta storage.JournalRequest, globalOrdinal int, record model.Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		receipt, ok := receipts[meta.AcceptanceID]
		if !ok || meta.ProjectID != receipt.Receipt.ProjectID {
			return errors.New("journal request is absent from durable receipt set")
		}
		local := globalOrdinal - meta.First
		if !selectionContains(receipt.Receipt.Selection.Accepted, local) {
			return nil
		}
		staged := engine.StageRecord{
			Version: engine.ConversionProtocolVersion, GlobalOrdinal: globalOrdinal, BatchID: input.BatchID,
			LaneID: input.LaneID, BatchSeq: input.BatchSeq, ReceivedTimeUS: receipt.Receipt.ReceivedTimeUS,
			GroupingVersion: receipt.GroupingVersion, Record: record,
		}
		if record.Kind == model.KindError {
			if receipt.GroupingVersion != issues.GroupingVersion {
				return errors.New("unsupported receipt grouping version")
			}
			group, err := issues.GroupRecord(record)
			if err != nil {
				return err
			}
			staged.IssueID, staged.FingerprintSHA256, staged.IssueTitle = group.IssueID, group.FingerprintSHA, group.Title
			titleJSON, _ := json.Marshal(group.Title)
			var releaseJSON *string
			if record.Release != nil {
				encoded, _ := json.Marshal(*record.Release)
				value := string(encoded)
				releaseJSON = &value
			}
			summaries = append(summaries, model.IssueOccurrenceSummary{
				RecordID: record.RecordID, ProjectID: record.ProjectID, AcceptanceID: record.AcceptanceID,
				LaneID: input.LaneID, BatchSeq: input.BatchSeq, Ordinal: globalOrdinal,
				EventTimeUS: record.EventTimeUS, EventNS: record.EventTimeNSRemainder,
				ReceivedTimeUS: receipt.Receipt.ReceivedTimeUS, ReleaseJSON: releaseJSON,
				IssueID: group.IssueID, GroupingVersion: receipt.GroupingVersion,
				FingerprintSHA256: group.FingerprintSHA, TitleJSON: string(titleJSON),
			})
			selectedErrors++
		}
		if err := stageEncoder.Encode(staged); err != nil {
			return err
		}
		selected++
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := validateReplayAgainstReceipts(index, input, receipts); err != nil {
		return nil, err
	}
	if selected != expectedAccepted {
		return nil, errors.New("selected record count differs from durable receipts")
	}
	if err := stageBuffer.Flush(); err != nil {
		return nil, err
	}
	if err := stage.Sync(); err != nil {
		return nil, err
	}
	if err := stage.Close(); err != nil {
		return nil, err
	}
	occurrencePath, occurrenceBytes, occurrenceSHA, err := writeOccurrenceSummaries(input.TaskDir, summaries)
	if err != nil {
		return nil, err
	}
	outputDirectory, err := os.MkdirTemp(input.TaskDir, "output-")
	if err != nil {
		_ = os.Remove(occurrencePath)
		return nil, err
	}
	spillDirectory := filepath.Join(input.TaskDir, "spill-"+filepath.Base(outputDirectory))
	artifacts := &ConversionArtifacts{
		Request: engine.ConversionRequest{
			Version: engine.ConversionProtocolVersion, StagePath: stagePath, OutputDirectory: outputDirectory, SpillDirectory: spillDirectory,
			TenantID: input.TenantID, LaneID: input.LaneID, BatchSeq: input.BatchSeq, BatchID: input.BatchID,
			SelectedRecords: selected, SelectedErrors: selectedErrors,
		},
		OccurrencePath: occurrencePath, OccurrenceBytes: occurrenceBytes, OccurrenceSHA256: occurrenceSHA,
		SelectedRecordCount: selected, SelectedErrorCount: selectedErrors, ReceiptSetSHA256: receiptHash, JournalSHA256: input.Journal.SHA256,
		removeOnCleanup: []string{stagePath, occurrencePath, outputDirectory, spillDirectory},
	}
	return artifacts, nil
}

func validateConversionReceipts(input ConversionInput) (map[string]ConversionReceipt, string, int, error) {
	receipts := make(map[string]ConversionReceipt, len(input.Receipts))
	hash := sha256.New()
	accepted := 0
	ordered := append([]ConversionReceipt(nil), input.Receipts...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Receipt.AcceptanceID < ordered[j].Receipt.AcceptanceID })
	for _, item := range ordered {
		receipt := item.Receipt
		recordCount := receipt.OrdinalLast - receipt.OrdinalFirst + 1
		digest, err := receipt.Selection.SHA256(recordCount)
		lane, idErr := model.LaneForAcceptance(receipt.AcceptanceID)
		if err != nil || idErr != nil || lane != input.LaneID || digest != receipt.SelectionSHA256 || !validWorkerSHA(receipt.ContentSHA256) || receipt.TenantID != input.TenantID || receipt.LaneID != input.LaneID || receipt.BatchSeq != input.BatchSeq || receipt.ProjectID <= 0 || receipt.OrdinalFirst < 0 || item.GroupingVersion <= 0 || receipts[receipt.AcceptanceID].Receipt.AcceptanceID != "" {
			return nil, "", 0, errors.New("invalid durable conversion receipt")
		}
		count := selectionCount(receipt.Selection.Accepted)
		if count != receipt.AcceptedCount {
			return nil, "", 0, errors.New("receipt accepted count differs from selection")
		}
		receipts[receipt.AcceptanceID] = item
		accepted += count
		encoded, _ := json.Marshal(struct {
			AcceptanceID string `json:"acceptance_id"`
			SelectionSHA string `json:"selection_sha256"`
		}{receipt.AcceptanceID, receipt.SelectionSHA256})
		_, _ = hash.Write(encoded)
		_, _ = hash.Write([]byte{'\n'})
	}
	return receipts, hex.EncodeToString(hash.Sum(nil)), accepted, nil
}

func validWorkerSHA(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func validateReplayAgainstReceipts(index storage.JournalIndex, input ConversionInput, receipts map[string]ConversionReceipt) error {
	if index.Header.TenantID != input.TenantID || index.Header.LaneID != input.LaneID || index.Header.BatchID != input.BatchID || len(index.Requests) != len(receipts) {
		return errors.New("replayed journal scope differs from durable batch")
	}
	for _, request := range index.Requests {
		receipt, ok := receipts[request.AcceptanceID]
		if !ok || request.ProjectID != receipt.Receipt.ProjectID || request.OrdinalFirst != receipt.Receipt.OrdinalFirst || request.OrdinalLast != receipt.Receipt.OrdinalLast || request.ContentSHA256 != receipt.Receipt.ContentSHA256 {
			return errors.New("replayed journal index differs from durable receipt")
		}
	}
	return nil
}

func selectionContains(ranges []model.ReceiptRange, position int) bool {
	index := sort.Search(len(ranges), func(index int) bool { return ranges[index][1] >= position })
	return index < len(ranges) && ranges[index][0] <= position
}

func selectionCount(ranges []model.ReceiptRange) int {
	total := 0
	for _, span := range ranges {
		total += span[1] - span[0] + 1
	}
	return total
}

func writeOccurrenceSummaries(directory string, summaries []model.IssueOccurrenceSummary) (string, int64, string, error) {
	sort.Slice(summaries, func(i, j int) bool { return summaries[i].RecordID < summaries[j].RecordID })
	file, err := os.CreateTemp(directory, "occurrences-*.jsonl")
	if err != nil {
		return "", 0, "", err
	}
	path := file.Name()
	fail := func(cause error) (string, int64, string, error) {
		_ = file.Close()
		_ = os.Remove(path)
		return "", 0, "", cause
	}
	hash := sha256.New()
	written := int64(0)
	writeLine := func(value any) error {
		encoded, err := json.Marshal(value)
		if err != nil {
			return err
		}
		encoded = append(encoded, '\n')
		count, err := io.MultiWriter(file, hash).Write(encoded)
		written += int64(count)
		return err
	}
	if err := writeLine(struct {
		Version int `json:"version"`
	}{occurrenceEncodingVersion}); err != nil {
		return fail(err)
	}
	previous := ""
	for _, summary := range summaries {
		if summary.RecordID <= previous {
			return fail(errors.New("duplicate or unordered Issue occurrence summary"))
		}
		if err := writeLine(summary); err != nil {
			return fail(err)
		}
		previous = summary.RecordID
	}
	if err := file.Sync(); err != nil {
		return fail(err)
	}
	if err := file.Close(); err != nil {
		return fail(err)
	}
	return path, written, hex.EncodeToString(hash.Sum(nil)), nil
}

func ReplayOccurrenceSummaries(path string, expectedBytes int64, expectedSHA string, consume func(model.IssueOccurrenceSummary) error) error {
	if expectedBytes <= 0 || expectedBytes > model.MaxManifestBytes || !validWorkerSHA(expectedSHA) || consume == nil {
		return errors.New("invalid Issue occurrence summary manifest")
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	hash := sha256.New()
	count, err := io.Copy(hash, io.LimitReader(file, expectedBytes+1))
	if err != nil || count != expectedBytes || hex.EncodeToString(hash.Sum(nil)) != expectedSHA {
		return errors.Join(errors.New("Issue occurrence summary checksum mismatch"), err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64<<10), (1<<20)+(16<<10))
	if !scanner.Scan() {
		return errors.New("Issue occurrence summary header missing")
	}
	var header struct {
		Version int `json:"version"`
	}
	if err := strictConversionJSON(scanner.Bytes(), &header); err != nil || header.Version != occurrenceEncodingVersion {
		return errors.Join(errors.New("unsupported Issue occurrence summary version"), err)
	}
	previous := ""
	for scanner.Scan() {
		var summary model.IssueOccurrenceSummary
		if err := strictConversionJSON(scanner.Bytes(), &summary); err != nil {
			return err
		}
		var title string
		titleErr := json.Unmarshal([]byte(summary.TitleJSON), &title)
		releaseValid := true
		if summary.ReleaseJSON != nil {
			var release string
			releaseValid = json.Unmarshal([]byte(*summary.ReleaseJSON), &release) == nil
		}
		if summary.RecordID <= previous || !validWorkerSHA(summary.RecordID) || summary.ProjectID <= 0 || summary.AcceptanceID == "" || summary.LaneID < 0 || summary.LaneID >= model.LaneCount || summary.BatchSeq <= 0 || summary.Ordinal < 0 || summary.EventNS > 999 || !validWorkerSHA(summary.IssueID) || summary.IssueID != summary.FingerprintSHA256 || summary.GroupingVersion <= 0 || titleErr != nil || len([]rune(title)) > 512 || !releaseValid {
			return errors.New("invalid Issue occurrence summary")
		}
		if err := consume(summary); err != nil {
			return err
		}
		previous = summary.RecordID
	}
	return scanner.Err()
}

func strictConversionJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("trailing conversion JSON")
	}
	return nil
}
