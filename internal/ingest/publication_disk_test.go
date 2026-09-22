package ingest

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/resource"
)

func TestConversionRejectsMissingDiskBudgetBeforeCreatingScratch(t *testing.T) {
	scratch := filepath.Join(t.TempDir(), "conversion")
	installationID := "00000000-0000-4000-8000-000000000001"
	workflow := DurableConversionWorkflow{
		Control: &workflowControlFixture{}, Store: &workflowStoreFixture{}, Runner: workflowRunnerFixture{},
		InstallationID: installationID, ScratchDir: scratch,
	}
	err := workflow.ConvertAndPrepare(context.Background(), control.ConversionJob{
		Authority: control.JobAuthority{InstallationID: installationID},
	})
	if err == nil {
		t.Fatal("conversion without shared disk admission succeeded")
	}
	if _, err := os.Stat(scratch); !os.IsNotExist(err) {
		t.Fatalf("conversion created scratch without a shared disk budget: %v", err)
	}
}

func TestConversionCancellationKeepsDiskUntilRunnerReturns(t *testing.T) {
	input := conversionFixture(t, []model.ReceiptClass{model.ReceiptAccepted, model.ReceiptDuplicate})
	journal, err := os.ReadFile(input.JournalPath)
	if err != nil {
		t.Fatal(err)
	}
	installationID := "00000000-0000-4000-8000-000000000001"
	controlFixture := &workflowControlFixture{work: control.ConversionWork{
		TenantID: input.TenantID, LaneID: input.LaneID, BatchSeq: input.BatchSeq, BatchID: input.BatchID,
		JournalObjectKey: "journal", JournalBytes: int64(len(journal)), JournalSHA256: digestBytes(journal),
		Receipts: []control.DurableConversionReceipt{{Receipt: input.Receipts[0].Receipt, GroupingVersion: input.Receipts[0].GroupingVersion}},
	}}
	budget := resource.NewBudget(conversionDiskReservation)
	scratch := filepath.Join(t.TempDir(), "worker")
	entered := make(chan engine.ConversionRequest, 1)
	release := make(chan struct{})
	releaseRunner := sync.OnceFunc(func() { close(release) })
	defer releaseRunner()
	workflow := DurableConversionWorkflow{Control: controlFixture,
		Store:          &workflowStoreFixture{journal: journal, objects: make(map[string][]byte)},
		Runner:         blockedConversionRunner{entered: entered, release: release},
		InstallationID: installationID, ScratchDir: scratch, Disk: budget}
	job := control.ConversionJob{Authority: control.JobAuthority{InstallationID: installationID}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		done <- workflow.ConvertAndPrepare(ctx, job)
	}()
	defer func() {
		cancel()
		releaseRunner()
		<-joined // Fatal assertions must not remove a live runner's files.
	}()
	var request engine.ConversionRequest
	select {
	case request = <-entered:
	case err := <-done:
		t.Fatalf("runner not reached: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	cancel()
	if budget.Used() != conversionDiskReservation {
		t.Fatalf("live runner lost disk: %d", budget.Used())
	}
	if _, err := os.Stat(request.StagePath); err != nil {
		t.Fatalf("live runner lost stage: %v", err)
	}
	select {
	case err := <-done:
		t.Fatalf("returned before runner joined: %v", err)
	default:
	}
	releaseRunner()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not join")
	}
	entries, err := os.ReadDir(scratch)
	if err != nil || len(entries) != 0 || budget.Used() != 0 || controlFixture.prepared != nil {
		t.Fatalf("cancel cleanup: entries=%v err=%v used=%d prepared=%v", entries, err, budget.Used(), controlFixture.prepared)
	}
}

type blockedConversionRunner struct {
	entered chan<- engine.ConversionRequest
	release <-chan struct{}
}

func (runner blockedConversionRunner) Run(ctx context.Context, request engine.ConversionRequest, _ func(engine.ConvertedBundle) error) (engine.ConversionSummary, error) {
	runner.entered <- request
	<-runner.release
	return engine.ConversionSummary{}, ctx.Err()
}

func TestConversionDiskExhaustionDoesNotDownloadOrPrepare(t *testing.T) {
	installationID := "00000000-0000-4000-8000-000000000001"
	controlFixture := &workflowControlFixture{}
	budget := resource.NewBudget(conversionDiskReservation)
	other, err := budget.Acquire(1)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Release()
	scratch := filepath.Join(t.TempDir(), "worker")
	workflow := DurableConversionWorkflow{Control: controlFixture, Store: &workflowStoreFixture{},
		Runner: workflowRunnerFixture{}, InstallationID: installationID, ScratchDir: scratch, Disk: budget}
	err = workflow.ConvertAndPrepare(context.Background(), control.ConversionJob{Authority: control.JobAuthority{InstallationID: installationID}})
	if !errors.Is(err, resource.ErrLimited) || budget.Used() != 1 || controlFixture.prepared != nil {
		t.Fatalf("admission err=%v used=%d prepared=%v", err, budget.Used(), controlFixture.prepared)
	}
	entries, err := os.ReadDir(scratch)
	if err != nil || len(entries) != 0 {
		t.Fatalf("rejected work created task files: %v %v", entries, err)
	}
}

func TestConversionStageWriterExactBound(t *testing.T) {
	var output bytes.Buffer
	writer := conversionStageWriter{writer: &output, remaining: 4}
	if n, err := writer.Write([]byte("abcd")); n != 4 || err != nil {
		t.Fatalf("exact bound: %d %v", n, err)
	}
	if n, err := writer.Write([]byte("e")); n != 0 || !errors.Is(err, resource.ErrLimited) || output.String() != "abcd" {
		t.Fatalf("overflow wrote bytes: %d %v %q", n, err, output.String())
	}
}
