package ingest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/model"
)

type recordingProcessor struct {
	mu      sync.Mutex
	batches [][]Command
	started chan []Command
	release <-chan struct{}
}

func (processor *recordingProcessor) Process(_ context.Context, commands []Command) []CommandResult {
	copyCommands := append([]Command(nil), commands...)
	processor.mu.Lock()
	processor.batches = append(processor.batches, copyCommands)
	processor.mu.Unlock()
	if processor.started != nil {
		processor.started <- copyCommands
	}
	if processor.release != nil {
		<-processor.release
	}
	results := make([]CommandResult, len(commands))
	for index, command := range commands {
		results[index].Receipt = control.ReceiptResult{AcceptanceID: command.Request.AcceptanceID}
	}
	return results
}

func batcherCommand(t *testing.T, lane int, sequence int, records int, tenant int64) Command {
	t.Helper()
	acceptanceID := ""
	for candidate := sequence * 100; ; candidate++ {
		value := fmt.Sprintf("00000000-0000-4000-8000-%012x", candidate)
		got, err := model.LaneForAcceptance(value)
		if err == nil && got == lane {
			acceptanceID = value
			break
		}
	}
	request := model.NormalizedRequest{TenantID: tenant, ProjectID: tenant*10 + 1, AcceptanceID: acceptanceID}
	for index := 0; index < records; index++ {
		request.Records = append(request.Records, model.Record{Message: "payload"})
	}
	return Command{Request: request, Authorization: Authorization{TenantID: tenant, ProjectID: request.ProjectID}}
}

func TestBatcherFlushesByTimeAndThresholds(t *testing.T) {
	processor := &recordingProcessor{started: make(chan []Command, 4)}
	batcher, err := NewBatcher(BatcherConfig{Processor: processor, MaxWait: 20 * time.Millisecond, MaxBatchRequests: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = batcher.Drain(context.Background()) })

	results := make(chan error, 3)
	for index := 0; index < 3; index++ {
		command := batcherCommand(t, 3, index+1, 1, 1)
		go func() {
			_, err := batcher.Accept(context.Background(), command)
			results <- err
		}()
	}
	first := <-processor.started
	second := <-processor.started
	if len(first) != 2 || len(second) != 1 {
		t.Fatalf("request threshold batches=%d,%d", len(first), len(second))
	}
	for range 3 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}

	recordProcessor := &recordingProcessor{started: make(chan []Command, 2)}
	recordBatcher, err := NewBatcher(BatcherConfig{Processor: recordProcessor, MaxWait: time.Second, MaxBatchRecords: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recordBatcher.Drain(context.Background()) })
	command := batcherCommand(t, 4, 20, 3, 1)
	done := make(chan error, 1)
	go func() { _, err := recordBatcher.Accept(context.Background(), command); done <- err }()
	if got := <-recordProcessor.started; len(got) != 1 {
		t.Fatalf("dedicated oversized request count=%d", len(got))
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestDefaultBatchThresholdContract(t *testing.T) {
	config := BatcherConfig{MaxBatchBytes: DefaultBatchBytes, MaxBatchRecords: DefaultBatchRecords, MaxBatchRequests: DefaultBatchRequests}
	request := batcherCommand(t, 1, 91, 1, 1)
	entry := &batchEntry{command: request, bytes: 1}
	for _, test := range []struct {
		batch *pendingBatch
		want  bool
	}{
		{&pendingBatch{bytes: DefaultBatchBytes - 1}, false},
		{&pendingBatch{bytes: DefaultBatchBytes}, true},
		{&pendingBatch{records: DefaultBatchRecords - 1}, false},
		{&pendingBatch{records: DefaultBatchRecords}, true},
		{&pendingBatch{entries: make([]*batchEntry, DefaultBatchRequests-1)}, false},
		{&pendingBatch{entries: make([]*batchEntry, DefaultBatchRequests)}, true},
	} {
		if got := wouldExceedBatch(test.batch, entry, config); got != test.want {
			t.Fatalf("batch=%+v got=%v want=%v", test.batch, got, test.want)
		}
	}
}

func TestBatcherCancellationBeforeAndAfterSeal(t *testing.T) {
	processor := &recordingProcessor{started: make(chan []Command, 1)}
	batcher, err := NewBatcher(BatcherConfig{Processor: processor, MaxWait: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := batcher.Accept(ctx, batcherCommand(t, 5, 1, 1, 1)); done <- err }()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-seal cancellation=%v", err)
	}
	select {
	case batch := <-processor.started:
		t.Fatalf("canceled command was sealed: %#v", batch)
	case <-time.After(40 * time.Millisecond):
	}
	if err := batcher.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}

	release := make(chan struct{})
	sealedProcessor := &recordingProcessor{started: make(chan []Command, 1), release: release}
	sealedBatcher, err := NewBatcher(BatcherConfig{Processor: sealedProcessor, MaxBatchRequests: 1})
	if err != nil {
		t.Fatal(err)
	}
	sealedCtx, sealedCancel := context.WithCancel(context.Background())
	sealedDone := make(chan error, 1)
	go func() { _, err := sealedBatcher.Accept(sealedCtx, batcherCommand(t, 6, 2, 1, 1)); sealedDone <- err }()
	<-sealedProcessor.started
	sealedCancel()
	select {
	case err := <-sealedDone:
		t.Fatalf("sealed caller released ownership early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-sealedDone; err != nil {
		t.Fatalf("sealed workflow failed after caller cancellation: %v", err)
	}
	if err := sealedBatcher.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
}
