package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/resource"
	"github.com/chawanghyeon/eventglass/internal/sdk"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

type workflowStore struct {
	mu       sync.Mutex
	keys     []string
	failures int
}

func (store *workflowStore) PutStream(_ context.Context, key string, body io.ReadSeeker, size int64, checksum string) (storage.ObjectInfo, error) {
	store.mu.Lock()
	store.keys = append(store.keys, key)
	if store.failures > 0 {
		store.failures--
		store.mu.Unlock()
		return storage.ObjectInfo{}, errors.New("injected upload failure")
	}
	store.mu.Unlock()
	if _, err := body.Seek(0, io.SeekStart); err != nil {
		return storage.ObjectInfo{}, err
	}
	hash := sha256.New()
	n, err := io.Copy(hash, body)
	if err != nil || n != size || hex.EncodeToString(hash.Sum(nil)) != checksum {
		return storage.ObjectInfo{}, errors.New("workflow passed invalid spool")
	}
	return storage.ObjectInfo{Key: key, Size: size, SHA256: checksum}, nil
}

type workflowControl struct {
	mu         sync.Mutex
	registered []control.JournalIntentRegistration
	marked     []control.IntentAuthority
	accepted   []control.VerifiedBatch
	markErr    error
	accept     func(control.VerifiedBatch) ([]control.ReceiptResult, error)
}

func (database *workflowControl) RegisterJournalIntent(_ context.Context, registration control.JournalIntentRegistration) error {
	database.mu.Lock()
	defer database.mu.Unlock()
	database.registered = append(database.registered, registration)
	return nil
}

func (database *workflowControl) MarkJournalIntentUploaded(_ context.Context, _ string, _ int64, _ int64, authority control.IntentAuthority) error {
	database.mu.Lock()
	defer database.mu.Unlock()
	database.marked = append(database.marked, authority)
	return database.markErr
}

func (database *workflowControl) Accept(_ context.Context, batch control.VerifiedBatch) ([]control.ReceiptResult, error) {
	database.mu.Lock()
	database.accepted = append(database.accepted, batch)
	database.mu.Unlock()
	if database.accept != nil {
		return database.accept(batch)
	}
	results := make([]control.ReceiptResult, len(batch.Requests))
	for index, request := range batch.Requests {
		results[index].AcceptanceID = request.Index.AcceptanceID
	}
	return results, nil
}

func workflowCommand(t *testing.T, acceptanceID string, projectID int64) Command {
	t.Helper()
	request, err := NormalizeEnvelope(sdk.Envelope{Items: []sdk.Item{{Ordinal: 0, Type: "event", Value: map[string]any{
		"event_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "message": "workflow",
	}}}}, NormalizeOptions{TenantID: 1, ProjectID: projectID, AcceptanceID: acceptanceID, ArrivalTime: time.Unix(1, 0)})
	if err != nil {
		t.Fatal(err)
	}
	return Command{Request: request, Authorization: Authorization{
		TenantID: 1, ProjectID: projectID, KeyHash: sha256.Sum256([]byte("key")), TenantRevision: 1,
		ProjectRevision: 1, KeyRevision: 1, ScrubRevision: 1, ConfigRevision: 1,
	}}
}

func workflowFixture(t *testing.T, database *workflowControl, store *workflowStore) *Workflow {
	t.Helper()
	workflow, err := NewWorkflow(WorkflowConfig{
		Control: database, Store: store, InstallationID: "00000000-0000-4000-8000-000000000001", StorageGeneration: 1,
		ProcessID: "00000000-0000-4000-8000-000000000002", TempDir: filepath.Join(t.TempDir(), "spool"), SpoolBudget: resource.NewBudget(2 * storage.MaxJournalBytes),
	})
	if err != nil {
		t.Fatal(err)
	}
	return workflow
}

func TestWorkflowRetriesUploadWithFreshIdentity(t *testing.T) {
	store := &workflowStore{failures: 2}
	database := &workflowControl{}
	workflow := workflowFixture(t, database, store)
	command := workflowCommand(t, "00000000-0000-4000-8000-000000000004", 10)
	results := workflow.Process(context.Background(), []Command{command})
	if len(results) != 1 || results[0].Err != nil || results[0].Receipt.AcceptanceID != command.Request.AcceptanceID {
		t.Fatalf("results=%#v", results)
	}
	if len(store.keys) != 3 || store.keys[0] == store.keys[1] || store.keys[1] == store.keys[2] || len(database.registered) != 3 || len(database.marked) != 1 || len(database.accepted) != 1 {
		t.Fatalf("keys=%#v registrations=%d marked=%d accepted=%d", store.keys, len(database.registered), len(database.marked), len(database.accepted))
	}
}

func TestWorkflowRebatchesExistingAndRejectedRequests(t *testing.T) {
	store := &workflowStore{}
	database := &workflowControl{}
	workflow := workflowFixture(t, database, store)
	commands := []Command{
		workflowCommand(t, "00000000-0000-4000-8000-000000000000", 10),
		workflowCommand(t, "00000000-0000-4000-8000-000000000004", 11),
		workflowCommand(t, "00000000-0000-4000-8000-000000000011", 12),
	}
	call := 0
	database.accept = func(batch control.VerifiedBatch) ([]control.ReceiptResult, error) {
		call++
		switch call {
		case 1:
			return nil, &control.AuthorizationRejectError{Rejected: []control.AuthorizationRejection{{AcceptanceID: commands[2].Request.AcceptanceID, Cause: control.ErrKeyRevoked}}}
		case 2:
			return nil, &control.ExistingSubsetError{
				Existing: []control.ReceiptResult{{AcceptanceID: commands[0].Request.AcceptanceID}},
				Missing:  []string{commands[1].Request.AcceptanceID},
			}
		default:
			return []control.ReceiptResult{{AcceptanceID: batch.Requests[0].Index.AcceptanceID}}, nil
		}
	}
	results := workflow.Process(context.Background(), commands)
	if results[0].Err != nil || results[1].Err != nil || !errors.Is(results[2].Err, ErrKeyRevoked) || results[0].Receipt.AcceptanceID != commands[0].Request.AcceptanceID || results[1].Receipt.AcceptanceID != commands[1].Request.AcceptanceID {
		t.Fatalf("results=%#v", results)
	}
	if len(database.accepted) != 3 || len(database.accepted[0].Requests) != 3 || len(database.accepted[1].Requests) != 2 || len(database.accepted[2].Requests) != 1 {
		t.Fatalf("rebatches=%#v", database.accepted)
	}
}

func TestWorkflowNeverAcceptsUnmarkedLateUpload(t *testing.T) {
	store := &workflowStore{}
	database := &workflowControl{markErr: control.ErrIntentStale}
	workflow := workflowFixture(t, database, store)
	results := workflow.Process(context.Background(), []Command{workflowCommand(t, "00000000-0000-4000-8000-000000000004", 10)})
	if len(results) != 1 || results[0].Err == nil || len(store.keys) != MaxUploadAttempts || len(database.accepted) != 0 {
		t.Fatalf("results=%#v keys=%d accepts=%d", results, len(store.keys), len(database.accepted))
	}
	for index := 1; index < len(store.keys); index++ {
		if store.keys[index] == store.keys[index-1] {
			t.Fatal("late upload retry reused an object key")
		}
	}
}

func TestWorkflowRejectsIncompleteDurableResults(t *testing.T) {
	store := &workflowStore{}
	database := &workflowControl{accept: func(control.VerifiedBatch) ([]control.ReceiptResult, error) {
		return nil, nil
	}}
	workflow := workflowFixture(t, database, store)
	results := workflow.Process(context.Background(), []Command{workflowCommand(t, "00000000-0000-4000-8000-000000000004", 10)})
	if len(results) != 1 || results[0].Err == nil || results[0].Receipt.AcceptanceID != "" {
		t.Fatalf("incomplete durable result was acknowledged: %#v", results)
	}
}

var _ JournalObjectStore = (*workflowStore)(nil)
var _ IngestController = (*workflowControl)(nil)
