package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/resource"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

const (
	DefaultWorkflowTimeout = 30 * time.Second
	MaxUploadAttempts      = 3
)

type JournalObjectStore interface {
	PutStream(context.Context, string, io.ReadSeeker, int64, string) (storage.ObjectInfo, error)
}

type IngestController interface {
	RegisterJournalIntent(context.Context, control.JournalIntentRegistration) error
	MarkJournalIntentUploaded(context.Context, string, int64, int64, control.IntentAuthority) error
	Accept(context.Context, control.VerifiedBatch) ([]control.ReceiptResult, error)
}

type WorkflowConfig struct {
	Control           IngestController
	Store             JournalObjectStore
	InstallationID    string
	StorageGeneration int64
	ProcessID         string
	TempDir           string
	SpoolBudget       *resource.Budget
	Timeout           time.Duration
}

type Workflow struct {
	config WorkflowConfig
}

type CommandResult struct {
	Receipt control.ReceiptResult
	Err     error
}

func NewWorkflow(config WorkflowConfig) (*Workflow, error) {
	if config.Control == nil || config.Store == nil || config.InstallationID == "" || config.StorageGeneration <= 0 || config.ProcessID == "" || config.TempDir == "" || config.SpoolBudget == nil {
		return nil, errors.New("complete workflow configuration is required")
	}
	if config.Timeout <= 0 {
		config.Timeout = DefaultWorkflowTimeout
	}
	if err := os.MkdirAll(config.TempDir, 0o700); err != nil {
		return nil, err
	}
	info, err := os.Stat(config.TempDir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("workflow temporary directory must be private")
	}
	return &Workflow{config: config}, nil
}

func (workflow *Workflow) Process(parent context.Context, commands []Command) []CommandResult {
	results := make([]CommandResult, len(commands))
	if len(commands) == 0 {
		return results
	}
	ctx, cancel := context.WithTimeout(parent, workflow.config.Timeout)
	defer cancel()
	workflow.processSubset(ctx, commands, makeIndex(commands), results)
	return results
}

func (workflow *Workflow) processSubset(ctx context.Context, commands []Command, original []int, results []CommandResult) {
	if len(commands) == 0 {
		return
	}
	var lastErr error
	for attempt := 0; attempt < MaxUploadAttempts; attempt++ {
		verified, err := workflow.uploadAttempt(ctx, commands)
		if err != nil {
			lastErr = err
			if ctx.Err() != nil {
				break
			}
			continue
		}
		receipts, err := workflow.config.Control.Accept(ctx, verified)
		if err == nil {
			if len(receipts) != len(commands) {
				err = errors.New("durable Accept result count mismatch")
				for _, index := range original {
					results[index].Err = err
				}
				return
			}
			for index := range commands {
				results[original[index]].Receipt = receipts[index]
			}
			return
		}
		var subset *control.ExistingSubsetError
		if errors.As(err, &subset) {
			existing := make(map[string]control.ReceiptResult, len(subset.Existing))
			for _, receipt := range subset.Existing {
				existing[receipt.AcceptanceID] = receipt
			}
			remaining, remainingOriginal := filterExisting(commands, original, existing, results)
			workflow.processSubset(ctx, remaining, remainingOriginal, results)
			return
		}
		var rejected *control.AuthorizationRejectError
		if errors.As(err, &rejected) {
			causes := make(map[string]error, len(rejected.Rejected))
			for _, rejection := range rejected.Rejected {
				causes[rejection.AcceptanceID] = rejection.Cause
			}
			remaining, remainingOriginal := filterRejected(commands, original, causes, results)
			workflow.processSubset(ctx, remaining, remainingOriginal, results)
			return
		}
		for _, index := range original {
			results[index].Err = mapControlError(err)
		}
		return
	}
	if lastErr == nil {
		lastErr = ctx.Err()
	}
	for _, index := range original {
		results[index].Err = lastErr
	}
}

func (workflow *Workflow) uploadAttempt(ctx context.Context, commands []Command) (control.VerifiedBatch, error) {
	tenantID, laneID, err := commandScope(commands)
	if err != nil {
		return control.VerifiedBatch{}, err
	}
	batchID, err := NewAcceptanceID()
	if err != nil {
		return control.VerifiedBatch{}, err
	}
	jobID, err := NewAcceptanceID()
	if err != nil {
		return control.VerifiedBatch{}, err
	}
	intentID, err := NewAcceptanceID()
	if err != nil {
		return control.VerifiedBatch{}, err
	}
	permit, err := workflow.config.SpoolBudget.Acquire(storage.MaxJournalBytes)
	if err != nil {
		if errors.Is(err, resource.ErrDraining) {
			return control.VerifiedBatch{}, ErrDraining
		}
		return control.VerifiedBatch{}, ErrAdmissionLimited
	}
	defer permit.Release()
	spool, err := os.CreateTemp(workflow.config.TempDir, "journal-*.zst")
	if err != nil {
		return control.VerifiedBatch{}, err
	}
	spoolPath := spool.Name()
	defer func() {
		_ = spool.Close()
		_ = os.Remove(spoolPath)
	}()
	requests := make([]model.NormalizedRequest, len(commands))
	for index := range commands {
		requests[index] = commands[index].Request
	}
	journalInfo, err := storage.WriteJournal(spool, model.JournalBatch{BatchID: batchID, TenantID: tenantID, LaneID: laneID, Requests: requests})
	if err != nil {
		return control.VerifiedBatch{}, err
	}
	if err := spool.Close(); err != nil {
		return control.VerifiedBatch{}, err
	}
	spool, err = os.Open(spoolPath)
	if err != nil {
		return control.VerifiedBatch{}, err
	}
	authority := control.IntentAuthority{IntentID: intentID, Owner: workflow.config.ProcessID, Fence: 1, Bytes: journalInfo.Bytes, SHA256: journalInfo.SHA256}
	objectKey := journalObjectKey(workflow.config.InstallationID, tenantID, laneID, batchID)
	registration := control.JournalIntentRegistration{
		InstallationID: workflow.config.InstallationID, StorageGeneration: workflow.config.StorageGeneration,
		TenantID: tenantID, ObjectKey: objectKey, Authority: authority,
	}
	if err := workflow.config.Control.RegisterJournalIntent(ctx, registration); err != nil {
		return control.VerifiedBatch{}, err
	}
	if _, err := workflow.config.Store.PutStream(ctx, objectKey, spool, journalInfo.Bytes, journalInfo.SHA256); err != nil {
		return control.VerifiedBatch{}, err
	}
	if err := workflow.config.Control.MarkJournalIntentUploaded(ctx, workflow.config.InstallationID, workflow.config.StorageGeneration, tenantID, authority); err != nil {
		return control.VerifiedBatch{}, err
	}
	verifiedRequests, err := verifiedRequests(commands, journalInfo.Index.Requests)
	if err != nil {
		return control.VerifiedBatch{}, err
	}
	return control.VerifiedBatch{
		InstallationID: workflow.config.InstallationID, StorageGeneration: workflow.config.StorageGeneration,
		TenantID: tenantID, LaneID: laneID, BatchID: batchID, JobID: jobID, Journal: authority, Requests: verifiedRequests,
	}, nil
}

func verifiedRequests(commands []Command, indexes []model.JournalRequestIndex) ([]control.VerifiedRequest, error) {
	if len(commands) != len(indexes) {
		return nil, errors.New("journal index request count mismatch")
	}
	requests := make([]control.VerifiedRequest, len(commands))
	for index, command := range commands {
		journalIndex := indexes[index]
		if journalIndex.AcceptanceID != command.Request.AcceptanceID || journalIndex.ProjectID != command.Request.ProjectID || journalIndex.RecordCount != len(command.Request.Records) {
			return nil, errors.New("journal index scope mismatch")
		}
		candidates := make([]control.Candidate, len(command.Request.Records))
		for position, record := range command.Request.Records {
			candidate := control.Candidate{Position: position, RecordID: record.RecordID, Kind: record.Kind}
			if digest, eligible, err := DedupePayloadSHA256(record); err != nil {
				return nil, err
			} else if eligible {
				candidate.SourceEventID = *record.SourceEventID
				candidate.DedupeSHA256 = digest
			}
			candidates[position] = candidate
		}
		requests[index] = control.VerifiedRequest{
			Index: journalIndex, Candidates: candidates,
			Authorization: control.AuthorizationSnapshot{
				TenantRevision: command.Authorization.TenantRevision, ProjectRevision: command.Authorization.ProjectRevision,
				KeyRevision: command.Authorization.KeyRevision, ScrubRevision: command.Authorization.ScrubRevision,
				ConfigRevision: command.Authorization.ConfigRevision, KeyHash: command.Authorization.KeyHash,
			},
		}
	}
	return requests, nil
}

func commandScope(commands []Command) (int64, int, error) {
	if len(commands) == 0 {
		return 0, 0, errors.New("empty workflow batch")
	}
	tenantID := commands[0].Request.TenantID
	laneID, err := model.LaneForAcceptance(commands[0].Request.AcceptanceID)
	if err != nil {
		return 0, 0, err
	}
	for _, command := range commands {
		lane, err := model.LaneForAcceptance(command.Request.AcceptanceID)
		if err != nil || command.Request.TenantID != tenantID || lane != laneID || command.Authorization.TenantID != tenantID || command.Authorization.ProjectID != command.Request.ProjectID {
			return 0, 0, errors.New("workflow batch scope mismatch")
		}
	}
	return tenantID, laneID, nil
}

func journalObjectKey(installationID string, tenantID int64, laneID int, batchID string) string {
	return strings.Join([]string{"v1", installationID, "journals", strconv.FormatInt(tenantID, 10), strconv.Itoa(laneID), batchID + ".jsonl.zst"}, "/")
}

func makeIndex(commands []Command) []int {
	indexes := make([]int, len(commands))
	for index := range indexes {
		indexes[index] = index
	}
	return indexes
}

func filterExisting(commands []Command, original []int, existing map[string]control.ReceiptResult, results []CommandResult) ([]Command, []int) {
	remaining := make([]Command, 0, len(commands))
	remainingOriginal := make([]int, 0, len(commands))
	for index, command := range commands {
		if receipt, ok := existing[command.Request.AcceptanceID]; ok {
			results[original[index]].Receipt = receipt
		} else {
			remaining = append(remaining, command)
			remainingOriginal = append(remainingOriginal, original[index])
		}
	}
	return remaining, remainingOriginal
}

func filterRejected(commands []Command, original []int, causes map[string]error, results []CommandResult) ([]Command, []int) {
	remaining := make([]Command, 0, len(commands))
	remainingOriginal := make([]int, 0, len(commands))
	for index, command := range commands {
		if cause, ok := causes[command.Request.AcceptanceID]; ok {
			results[original[index]].Err = mapControlError(cause)
		} else {
			remaining = append(remaining, command)
			remainingOriginal = append(remainingOriginal, original[index])
		}
	}
	return remaining, remainingOriginal
}

func mapControlError(err error) error {
	switch {
	case errors.Is(err, control.ErrTenantDisabled), errors.Is(err, control.ErrProjectDisabled):
		return ErrProjectDisabled
	case errors.Is(err, control.ErrKeyRevoked):
		return ErrKeyRevoked
	case errors.Is(err, control.ErrAuthorizationStale):
		return ErrScrubChanged
	default:
		return fmt.Errorf("durable Accept: %w", err)
	}
}
