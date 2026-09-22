package ingest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/resource"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

type workflowControlFixture struct {
	work          control.ConversionWork
	registrations []control.OutputIntentRegistration
	uploaded      []control.IntentAuthority
	prepared      *control.PrepareCommand
}

func (fixture *workflowControlFixture) LoadConversion(_ context.Context, _ control.JobAuthority) (control.ConversionWork, error) {
	return fixture.work, nil
}
func (fixture *workflowControlFixture) RegisterOutputIntent(_ context.Context, value control.OutputIntentRegistration) error {
	fixture.registrations = append(fixture.registrations, value)
	return nil
}
func (fixture *workflowControlFixture) MarkOutputIntentUploaded(_ context.Context, _ control.JobAuthority, _ int64, value control.IntentAuthority) error {
	fixture.uploaded = append(fixture.uploaded, value)
	return nil
}
func (fixture *workflowControlFixture) Prepare(_ context.Context, value control.PrepareCommand) error {
	fixture.prepared = &value
	return nil
}

type workflowStoreFixture struct {
	journal []byte
	objects map[string][]byte
}

func (fixture *workflowStoreFixture) DownloadToFile(_ context.Context, _ string, path string, size int64, checksum string, maxBytes int64) error {
	if size > maxBytes || int64(len(fixture.journal)) != size || digestBytes(fixture.journal) != checksum {
		return io.ErrUnexpectedEOF
	}
	return os.WriteFile(path, fixture.journal, 0o600)
}
func (fixture *workflowStoreFixture) PutStream(_ context.Context, key string, body io.ReadSeeker, size int64, checksum string) (storage.ObjectInfo, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return storage.ObjectInfo{}, err
	}
	if int64(len(data)) != size || digestBytes(data) != checksum {
		return storage.ObjectInfo{}, io.ErrUnexpectedEOF
	}
	fixture.objects[key] = data
	return storage.ObjectInfo{Key: key, Size: size, SHA256: checksum}, nil
}
func (fixture *workflowStoreFixture) VerifyObject(_ context.Context, key string, size int64, checksum string) error {
	data, ok := fixture.objects[key]
	if !ok || int64(len(data)) != size || digestBytes(data) != checksum {
		return io.ErrUnexpectedEOF
	}
	return nil
}

type workflowRunnerFixture struct{}

func (workflowRunnerFixture) Run(_ context.Context, request engine.ConversionRequest, emit func(engine.ConvertedBundle) error) (engine.ConversionSummary, error) {
	makeFile := func(name, body string) engine.ConvertedFile {
		path := filepath.Join(request.OutputDirectory, name)
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			panic(err)
		}
		evidence, err := storage.InspectFile(path)
		if err != nil {
			panic(err)
		}
		return engine.ConvertedFile{Path: path, Evidence: evidence, RowCount: 1, MinEventTimeUS: 10, MaxEventTimeUS: 10, MinReceivedTimeUS: 20, MaxReceivedTimeUS: 20, MinBatchSeq: request.BatchSeq, MaxBatchSeq: request.BatchSeq}
	}
	recordID := strings.Repeat("a", 64)
	identity := digestBytes([]byte(recordID + "\n"))
	bundle := engine.ConvertedBundle{
		Index: 0, EventDay: "2026-01-01", Kind: model.KindError, RowCount: 1, IdentitySHA256: identity, ProjectIDs: []int64{29},
		Analytics: makeFile("analytics.parquet", "analytics"), Payload: makeFile("payload.parquet", "payload"),
	}
	if err := emit(bundle); err != nil {
		return engine.ConversionSummary{}, err
	}
	return engine.ConversionSummary{DuckDBVersion: "fixture", SelectedRecordCount: 1, SelectedErrorCount: 1, BundleCount: 1, SelectedIdentitySHA256: identity}, nil
}

func TestDurableConversionWorkflowUploadsPairsAndPreparesPagedManifest(t *testing.T) {
	input := conversionFixture(t, []model.ReceiptClass{model.ReceiptAccepted, model.ReceiptDuplicate})
	journal, err := os.ReadFile(input.JournalPath)
	if err != nil {
		t.Fatal(err)
	}
	job := control.ConversionJob{Authority: control.JobAuthority{
		InstallationID: "00000000-0000-4000-8000-000000000001", StorageGeneration: 1,
		JobID: "00000000-0000-4000-8000-000000000002", Owner: "worker", Fence: 3,
	}, TenantID: input.TenantID, LaneID: input.LaneID, BatchSeq: input.BatchSeq}
	controlFixture := &workflowControlFixture{work: control.ConversionWork{
		TenantID: input.TenantID, LaneID: input.LaneID, BatchSeq: input.BatchSeq, BatchID: input.BatchID,
		JournalObjectKey: "journal", JournalBytes: int64(len(journal)), JournalSHA256: digestBytes(journal),
		Receipts: []control.DurableConversionReceipt{{Receipt: input.Receipts[0].Receipt, GroupingVersion: input.Receipts[0].GroupingVersion}},
	}}
	storeFixture := &workflowStoreFixture{journal: journal, objects: make(map[string][]byte)}
	workflow := DurableConversionWorkflow{
		Control: controlFixture, Store: storeFixture, Runner: workflowRunnerFixture{}, InstallationID: job.Authority.InstallationID, ScratchDir: filepath.Join(t.TempDir(), "worker"), Disk: resource.NewBudget(4 << 30),
	}
	if err := workflow.ConvertAndPrepare(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if len(controlFixture.registrations) != 2 || len(controlFixture.uploaded) != 2 || len(storeFixture.objects) != 2 || controlFixture.prepared == nil {
		t.Fatalf("registrations=%d uploaded=%d objects=%d prepared=%v", len(controlFixture.registrations), len(controlFixture.uploaded), len(storeFixture.objects), controlFixture.prepared != nil)
	}
	prepared := controlFixture.prepared
	if prepared.Root.Header.BundleCount != 1 || prepared.Root.Header.SelectedRecordCount != 1 || prepared.Root.Header.SelectedErrorCount != 1 || len(prepared.Parts) != 1 {
		t.Fatalf("prepared root=%#v parts=%d", prepared.Root.Header, len(prepared.Parts))
	}
	for _, registration := range controlFixture.registrations {
		if registration.Authority != job.Authority || registration.Intent.Owner != job.Authority.Owner || registration.Intent.Fence != job.Authority.Fence || !bytes.Contains([]byte(registration.ObjectKey), []byte(job.Authority.JobID)) {
			t.Fatalf("registration=%#v", registration)
		}
	}
}

func digestBytes(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}
