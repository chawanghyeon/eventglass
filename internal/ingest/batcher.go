package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/model"
)

const (
	DefaultBatchBytes      = 4 << 20
	DefaultBatchRecords    = 1_000
	DefaultBatchRequests   = 1_000
	DefaultBatchWait       = 100 * time.Millisecond
	DefaultWaitingRequests = 1_024
	DefaultUploadWorkflows = 2
)

type BatchProcessor interface {
	Process(context.Context, []Command) []CommandResult
}

type BatcherConfig struct {
	Processor          BatchProcessor
	MaxBatchBytes      int
	MaxBatchRecords    int
	MaxBatchRequests   int
	MaxWait            time.Duration
	MaxWaitingRequests int
	MaxUploadWorkflows int
}

type Batcher struct {
	config     BatcherConfig
	input      chan *batchEntry
	completed  chan processedBatch
	drain      chan chan struct{}
	slots      chan struct{}
	rootCtx    context.Context
	cancel     context.CancelFunc
	done       chan struct{}
	workflowWG sync.WaitGroup
}

type batchScope struct {
	tenant int64
	lane   int
}

type batchEntry struct {
	ctx      context.Context
	command  Command
	bytes    int
	enqueued time.Time
	result   chan CommandResult
}

type pendingBatch struct {
	scope   batchScope
	entries []*batchEntry
	bytes   int
	records int
	oldest  time.Time
}

type sealedBatch struct {
	tenant  int64
	entries []*batchEntry
}

type processedBatch struct {
	batch   sealedBatch
	results []CommandResult
}

func NewBatcher(config BatcherConfig) (*Batcher, error) {
	if config.Processor == nil {
		return nil, errors.New("batch processor is required")
	}
	if config.MaxBatchBytes <= 0 {
		config.MaxBatchBytes = DefaultBatchBytes
	}
	if config.MaxBatchRecords <= 0 {
		config.MaxBatchRecords = DefaultBatchRecords
	}
	if config.MaxBatchRequests <= 0 {
		config.MaxBatchRequests = DefaultBatchRequests
	}
	if config.MaxWait <= 0 {
		config.MaxWait = DefaultBatchWait
	}
	if config.MaxWaitingRequests <= 0 {
		config.MaxWaitingRequests = DefaultWaitingRequests
	}
	if config.MaxUploadWorkflows <= 0 {
		config.MaxUploadWorkflows = DefaultUploadWorkflows
	}
	rootCtx, cancel := context.WithCancel(context.Background())
	batcher := &Batcher{
		config: config, input: make(chan *batchEntry), completed: make(chan processedBatch, config.MaxUploadWorkflows),
		drain: make(chan chan struct{}), slots: make(chan struct{}, config.MaxWaitingRequests), rootCtx: rootCtx, cancel: cancel, done: make(chan struct{}),
	}
	go batcher.loop()
	return batcher, nil
}

func (batcher *Batcher) Accept(ctx context.Context, command Command) (control.ReceiptResult, error) {
	encoded, err := json.Marshal(command.Request)
	if err != nil {
		return control.ReceiptResult{}, err
	}
	entry := &batchEntry{ctx: ctx, command: command, bytes: len(encoded), enqueued: time.Now(), result: make(chan CommandResult, 1)}
	select {
	case batcher.slots <- struct{}{}:
	case <-ctx.Done():
		return control.ReceiptResult{}, ctx.Err()
	case <-batcher.done:
		return control.ReceiptResult{}, ErrDraining
	}
	select {
	case batcher.input <- entry:
	case <-ctx.Done():
		<-batcher.slots
		return control.ReceiptResult{}, ctx.Err()
	case <-batcher.done:
		<-batcher.slots
		return control.ReceiptResult{}, ErrDraining
	}
	result := <-entry.result
	return result.Receipt, result.Err
}

func (batcher *Batcher) Drain(ctx context.Context) error {
	ack := make(chan struct{})
	select {
	case batcher.drain <- ack:
	case <-batcher.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-ack:
	case <-ctx.Done():
		batcher.cancel()
		return ctx.Err()
	}
	select {
	case <-batcher.done:
		return nil
	case <-ctx.Done():
		batcher.cancel()
		return ctx.Err()
	}
}

func (batcher *Batcher) loop() {
	defer close(batcher.done)
	pending := make(map[batchScope]*pendingBatch)
	ready := make(map[int64][]sealedBatch)
	var tenants []int64
	active, draining := 0, false
	var drainAck chan struct{}
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	for {
		batcher.dispatchReady(ready, &tenants, &active)
		if draining && len(pending) == 0 && len(tenants) == 0 && active == 0 {
			if drainAck != nil {
				close(drainAck)
			}
			batcher.workflowWG.Wait()
			return
		}
		resetBatchTimer(timer, pending, batcher.config.MaxWait)
		select {
		case entry := <-batcher.input:
			if draining {
				batcher.finishEntry(entry, CommandResult{Err: ErrDraining})
				continue
			}
			batcher.addEntry(entry, pending, ready, &tenants)
		case completed := <-batcher.completed:
			active--
			for index, entry := range completed.batch.entries {
				result := CommandResult{Err: errors.New("batch processor result count mismatch")}
				if index < len(completed.results) {
					result = completed.results[index]
				}
				batcher.finishEntry(entry, result)
			}
		case <-timer.C:
			now := time.Now()
			for scope, batch := range pending {
				batcher.removeCanceled(batch)
				if len(batch.entries) == 0 {
					delete(pending, scope)
					continue
				}
				if !batch.oldest.Add(batcher.config.MaxWait).After(now) {
					delete(pending, scope)
					enqueueReady(ready, &tenants, sealedBatch{tenant: scope.tenant, entries: batch.entries})
				}
			}
		case ack := <-batcher.drain:
			if !draining {
				draining, drainAck = true, ack
				for scope, batch := range pending {
					batcher.removeCanceled(batch)
					delete(pending, scope)
					if len(batch.entries) != 0 {
						enqueueReady(ready, &tenants, sealedBatch{tenant: scope.tenant, entries: batch.entries})
					}
				}
			} else {
				close(ack)
			}
		case <-batcher.rootCtx.Done():
			for _, batch := range pending {
				for _, entry := range batch.entries {
					batcher.finishEntry(entry, CommandResult{Err: ErrDraining})
				}
			}
			for _, batches := range ready {
				for _, batch := range batches {
					for _, entry := range batch.entries {
						batcher.finishEntry(entry, CommandResult{Err: ErrDraining})
					}
				}
			}
			batcher.workflowWG.Wait()
			return
		}
	}
}

func (batcher *Batcher) addEntry(entry *batchEntry, pending map[batchScope]*pendingBatch, ready map[int64][]sealedBatch, tenants *[]int64) {
	if entry.ctx.Err() != nil {
		batcher.finishEntry(entry, CommandResult{Err: entry.ctx.Err()})
		return
	}
	lane, err := model.LaneForAcceptance(entry.command.Request.AcceptanceID)
	if err != nil {
		batcher.finishEntry(entry, CommandResult{Err: err})
		return
	}
	scope := batchScope{entry.command.Request.TenantID, lane}
	batch := pending[scope]
	if batch == nil {
		batch = &pendingBatch{scope: scope, oldest: entry.enqueued}
		pending[scope] = batch
	}
	records := len(entry.command.Request.Records)
	if len(batch.entries) > 0 && wouldExceedBatch(batch, entry, batcher.config) {
		delete(pending, scope)
		batcher.removeCanceled(batch)
		if len(batch.entries) != 0 {
			enqueueReady(ready, tenants, sealedBatch{tenant: scope.tenant, entries: batch.entries})
		}
		batch = &pendingBatch{scope: scope, oldest: entry.enqueued}
		pending[scope] = batch
	}
	batch.entries = append(batch.entries, entry)
	batch.bytes += entry.bytes
	batch.records += records
	if entry.bytes > batcher.config.MaxBatchBytes || records > batcher.config.MaxBatchRecords || len(batch.entries) >= batcher.config.MaxBatchRequests {
		delete(pending, scope)
		enqueueReady(ready, tenants, sealedBatch{tenant: scope.tenant, entries: batch.entries})
	}
}

func wouldExceedBatch(batch *pendingBatch, entry *batchEntry, config BatcherConfig) bool {
	return batch.bytes+entry.bytes > config.MaxBatchBytes ||
		batch.records+len(entry.command.Request.Records) > config.MaxBatchRecords ||
		len(batch.entries)+1 > config.MaxBatchRequests
}

func (batcher *Batcher) dispatchReady(ready map[int64][]sealedBatch, tenants *[]int64, active *int) {
	for *active < batcher.config.MaxUploadWorkflows && len(*tenants) > 0 {
		tenant := (*tenants)[0]
		*tenants = (*tenants)[1:]
		queue := ready[tenant]
		batch := queue[0]
		if len(queue) == 1 {
			delete(ready, tenant)
		} else {
			ready[tenant] = queue[1:]
			*tenants = append(*tenants, tenant)
		}
		(*active)++
		batcher.workflowWG.Add(1)
		go func() {
			defer batcher.workflowWG.Done()
			commands := make([]Command, len(batch.entries))
			for index, entry := range batch.entries {
				commands[index] = entry.command
			}
			results := batcher.config.Processor.Process(batcher.rootCtx, commands)
			batcher.completed <- processedBatch{batch: batch, results: results}
		}()
	}
}

func enqueueReady(ready map[int64][]sealedBatch, tenants *[]int64, batch sealedBatch) {
	if len(ready[batch.tenant]) == 0 {
		*tenants = append(*tenants, batch.tenant)
	}
	ready[batch.tenant] = append(ready[batch.tenant], batch)
}

func (batcher *Batcher) removeCanceled(batch *pendingBatch) {
	kept := batch.entries[:0]
	batch.bytes, batch.records = 0, 0
	for _, entry := range batch.entries {
		if entry.ctx.Err() != nil {
			batcher.finishEntry(entry, CommandResult{Err: entry.ctx.Err()})
			continue
		}
		kept = append(kept, entry)
		batch.bytes += entry.bytes
		batch.records += len(entry.command.Request.Records)
	}
	batch.entries = kept
	if len(kept) != 0 {
		batch.oldest = kept[0].enqueued
		for _, entry := range kept[1:] {
			if entry.enqueued.Before(batch.oldest) {
				batch.oldest = entry.enqueued
			}
		}
	}
}

func (batcher *Batcher) finishEntry(entry *batchEntry, result CommandResult) {
	entry.result <- result
	<-batcher.slots
}

func resetBatchTimer(timer *time.Timer, pending map[batchScope]*pendingBatch, wait time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	var next time.Time
	for _, batch := range pending {
		deadline := batch.oldest.Add(wait)
		if next.IsZero() || deadline.Before(next) {
			next = deadline
		}
	}
	if !next.IsZero() {
		delay := time.Until(next)
		if delay < 0 {
			delay = 0
		}
		timer.Reset(delay)
	}
}
