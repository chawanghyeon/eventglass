package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/api"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/ingest"
	"github.com/chawanghyeon/eventglass/internal/resource"
	"github.com/chawanghyeon/eventglass/internal/sdk"
	"github.com/chawanghyeon/eventglass/internal/storage"
	"github.com/jackc/pgx/v5/pgxpool"
)

func integrationStore(t *testing.T, prefix string) *storage.S3Store {
	t.Helper()
	environment := requiredEnvironment(t, "EVENTGLASS_S3_ENDPOINT", "EVENTGLASS_S3_BUCKET", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")
	store, err := storage.NewS3Store(context.Background(), storage.S3Config{
		Endpoint: environment["EVENTGLASS_S3_ENDPOINT"], Region: "us-east-1", Bucket: environment["EVENTGLASS_S3_BUCKET"], Prefix: prefix,
		AccessKeyID: environment["AWS_ACCESS_KEY_ID"], SecretAccessKey: environment["AWS_SECRET_ACCESS_KEY"], PathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func integrationCommand(t *testing.T, fixture *acceptFixture, projectID int64, auth control.AuthorizationSnapshot, acceptanceID, message string) ingest.Command {
	t.Helper()
	request, err := ingest.NormalizeEnvelope(sdk.Envelope{Items: []sdk.Item{{Ordinal: 0, Type: "event", Value: map[string]any{
		"event_id": "dddddddddddddddddddddddddddddddd", "message": message,
	}}}}, ingest.NormalizeOptions{TenantID: fixture.tenantID, ProjectID: projectID, AcceptanceID: acceptanceID, ArrivalTime: time.Unix(1, 0)})
	if err != nil {
		t.Fatal(err)
	}
	return ingest.Command{Request: request, Authorization: ingest.Authorization{
		TenantID: fixture.tenantID, ProjectID: projectID, KeyHash: auth.KeyHash, TenantRevision: auth.TenantRevision,
		ProjectRevision: auth.ProjectRevision, KeyRevision: auth.KeyRevision, ScrubRevision: auth.ScrubRevision, ConfigRevision: auth.ConfigRevision,
	}}
}

func integrationWorkflow(t *testing.T, fixture *acceptFixture, controller ingest.IngestController, store *storage.S3Store, name string) *ingest.Workflow {
	t.Helper()
	workflow, err := ingest.NewWorkflow(ingest.WorkflowConfig{
		Control: controller, Store: store, InstallationID: acceptInstallationID, StorageGeneration: 1,
		ProcessID: fixture.uuidForLane(0), TempDir: filepath.Join(t.TempDir(), "spool"), SpoolBudget: resource.NewBudget(2 * storage.MaxJournalBytes),
	})
	if err != nil {
		t.Fatalf("workflow %s: %v", name, err)
	}
	return workflow
}

func TestDurableWorkflowVerifiedUploadDuplicateAndUnsupportedOnly(t *testing.T) {
	fixture := setupAcceptFixture(t, 601)
	operations, err := control.NewIngestOperations(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(context.Background(), `UPDATE projects SET default_service='dynamic',allowed_origins='["https://dynamic.invalid"]'::jsonb WHERE project_id=$1`, fixture.projectID); err != nil {
		t.Fatal(err)
	}
	resolved, err := operations.LoadProjectAuthorization(context.Background(), fixture.tenantID, fixture.projectID, fixture.keyHash)
	if err != nil || resolved.DefaultService != "dynamic" || len(resolved.AllowedOrigins) != 1 || resolved.AllowedOrigins[0] != "https://dynamic.invalid" || resolved.Snapshot.KeyHash != fixture.keyHash {
		t.Fatalf("dynamic authorization=%#v err=%v", resolved, err)
	}
	origins, err := operations.LoadProjectOrigins(context.Background(), fixture.tenantID, fixture.projectID)
	if err != nil || len(origins) != 1 || origins[0] != "https://dynamic.invalid" {
		t.Fatalf("dynamic origins=%#v err=%v", origins, err)
	}
	store := integrationStore(t, "workflow-601")
	workflow := integrationWorkflow(t, fixture, operations, store, "durable")
	lane := 8
	seed := integrationCommand(t, fixture, fixture.projectID, fixture.auth, fixture.uuidForLane(lane), "same-payload")
	seedResults := workflow.Process(context.Background(), []ingest.Command{seed})
	if len(seedResults) != 1 || seedResults[0].Err != nil || seedResults[0].Receipt.AcceptedCount != 1 {
		t.Fatalf("seed=%#v", seedResults)
	}
	duplicate := integrationCommand(t, fixture, fixture.projectID, fixture.auth, fixture.uuidForLane(lane), "same-payload")
	unsupportedRequest, err := ingest.NormalizeEnvelope(sdk.Envelope{Items: []sdk.Item{{Ordinal: 0, Type: "attachment", Payload: []byte("five!")}}}, ingest.NormalizeOptions{
		TenantID: fixture.tenantID, ProjectID: fixture.projectID, AcceptanceID: fixture.uuidForLane(lane), ArrivalTime: time.Unix(2, 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	unsupported := ingest.Command{Request: unsupportedRequest, Authorization: ingest.Authorization{
		TenantID: fixture.tenantID, ProjectID: fixture.projectID, KeyHash: fixture.auth.KeyHash, TenantRevision: 1, ProjectRevision: 1, KeyRevision: 1, ScrubRevision: 1, ConfigRevision: 1,
	}}
	results := workflow.Process(context.Background(), []ingest.Command{duplicate, unsupported})
	if len(results) != 2 || results[0].Err != nil || results[1].Err != nil || results[0].Receipt.DuplicateCount != 1 || results[0].Receipt.AcceptedCount != 0 || results[1].Receipt.UnsupportedCount != 1 || results[1].Receipt.AcceptedCount != 0 {
		t.Fatalf("results=%#v", results)
	}
	var receipts, batches, jobs, referenced int
	if err := fixture.pool.QueryRow(context.Background(), `SELECT
		(SELECT count(*) FROM receipts WHERE tenant_id=$1),
		(SELECT count(*) FROM ingest_batches WHERE tenant_id=$1),
		(SELECT count(*) FROM jobs WHERE tenant_id=$1),
		(SELECT count(*) FROM object_intents WHERE tenant_id=$1 AND state='referenced')`, fixture.tenantID).Scan(&receipts, &batches, &jobs, &referenced); err != nil {
		t.Fatal(err)
	}
	if receipts != 3 || batches != 2 || jobs != 2 || referenced != 2 {
		t.Fatalf("receipts=%d batches=%d jobs=%d referenced=%d", receipts, batches, jobs, referenced)
	}
	objects, err := store.List(context.Background(), "v1")
	if err != nil || len(objects) != 2 {
		t.Fatalf("objects=%#v err=%v", objects, err)
	}
}

type revokingController struct {
	operations *control.IngestOperations
	pool       *pgxpool.Pool
	projectID  int64
	once       sync.Once
}

func (controller *revokingController) RegisterJournalIntent(ctx context.Context, registration control.JournalIntentRegistration) error {
	return controller.operations.RegisterJournalIntent(ctx, registration)
}

func (controller *revokingController) MarkJournalIntentUploaded(ctx context.Context, installationID string, generation, tenantID int64, authority control.IntentAuthority) error {
	return controller.operations.MarkJournalIntentUploaded(ctx, installationID, generation, tenantID, authority)
}

func (controller *revokingController) Accept(ctx context.Context, batch control.VerifiedBatch) ([]control.ReceiptResult, error) {
	var revokeErr error
	controller.once.Do(func() {
		_, revokeErr = controller.pool.Exec(ctx, `UPDATE projects SET state='disabled',auth_revision=auth_revision+1 WHERE project_id=$1`, controller.projectID)
	})
	if revokeErr != nil {
		return nil, revokeErr
	}
	return controller.operations.Accept(ctx, batch)
}

func TestDurableWorkflowRebatchesAfterProjectRevocation(t *testing.T) {
	fixture := setupAcceptFixture(t, 602)
	secondProject := fixture.projectID + 1
	secondKey := sha256.Sum256([]byte("second-key"))
	secondKeyID := fixture.uuidForLane(0)
	if _, err := fixture.pool.Exec(context.Background(), `INSERT INTO projects(tenant_id,project_id,scrub_revision) VALUES($1,$2,1)`, fixture.tenantID, secondProject); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(context.Background(), `INSERT INTO project_keys(tenant_id,project_id,key_hash,key_id,key_prefix) VALUES($1,$2,$3,$4,$5)`,
		fixture.tenantID, secondProject, secondKey[:], secondKeyID, fmt.Sprintf("%x", secondKey[:4])); err != nil {
		t.Fatal(err)
	}
	operations, err := control.NewIngestOperations(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	controller := &revokingController{operations: operations, pool: fixture.pool, projectID: secondProject}
	store := integrationStore(t, "workflow-602")
	workflow := integrationWorkflow(t, fixture, controller, store, "revoke")
	lane := 9
	valid := integrationCommand(t, fixture, fixture.projectID, fixture.auth, fixture.uuidForLane(lane), "valid")
	secondAuth := control.AuthorizationSnapshot{TenantRevision: 1, ProjectRevision: 1, KeyRevision: 1, ScrubRevision: 1, ConfigRevision: 1, KeyHash: secondKey}
	revoked := integrationCommand(t, fixture, secondProject, secondAuth, fixture.uuidForLane(lane), "revoked")
	results := workflow.Process(context.Background(), []ingest.Command{valid, revoked})
	if len(results) != 2 || results[0].Err != nil || results[0].Receipt.AcceptanceID != valid.Request.AcceptanceID || !errors.Is(results[1].Err, ingest.ErrProjectDisabled) {
		t.Fatalf("results=%#v", results)
	}
	var validReceipts, revokedReceipts, uploaded, referenced int
	if err := fixture.pool.QueryRow(context.Background(), `SELECT
		(SELECT count(*) FROM receipts WHERE acceptance_id=$1),
		(SELECT count(*) FROM receipts WHERE acceptance_id=$2),
		(SELECT count(*) FROM object_intents WHERE tenant_id=$3 AND state='uploaded'),
		(SELECT count(*) FROM object_intents WHERE tenant_id=$3 AND state='referenced')`,
		valid.Request.AcceptanceID, revoked.Request.AcceptanceID, fixture.tenantID).Scan(&validReceipts, &revokedReceipts, &uploaded, &referenced); err != nil {
		t.Fatal(err)
	}
	if validReceipts != 1 || revokedReceipts != 0 || uploaded != 1 || referenced != 1 {
		t.Fatalf("valid=%d revoked=%d uploaded=%d referenced=%d", validReceipts, revokedReceipts, uploaded, referenced)
	}
}

func TestHTTPDynamicAuthorizationReturnsOnlyDurableReceipt(t *testing.T) {
	fixture := setupAcceptFixture(t, 603)
	if _, err := fixture.pool.Exec(context.Background(), `UPDATE projects SET default_service='http-dynamic',allowed_origins='["https://sdk.invalid"]'::jsonb WHERE project_id=$1`, fixture.projectID); err != nil {
		t.Fatal(err)
	}
	operations, err := control.NewIngestOperations(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	store := integrationStore(t, "workflow-603")
	workflow := integrationWorkflow(t, fixture, operations, store, "http")
	batcher, err := ingest.NewBatcher(ingest.BatcherConfig{Processor: workflow, MaxWait: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := batcher.Drain(ctx); err != nil {
			t.Error(err)
		}
	})
	handler, err := api.NewIngestHandler(api.Config{
		TenantID: fixture.tenantID, ProjectID: fixture.projectID, Sink: batcher,
		ResolveAuthorization: operations.LoadProjectAuthorization, ResolveOrigins: operations.LoadProjectOrigins,
	})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("{}\n{\"type\":\"event\"}\n{\"event_id\":\"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee\",\"message\":\"durable http\"}")
	request := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/%d/envelope/?sentry_key=key-%d", fixture.projectID, fixture.tenantID), bytes.NewReader(body))
	request.Header.Set("Origin", "https://sdk.invalid")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	receiptID := response.Header().Get("X-Eventglass-Receipt")
	if response.Code != http.StatusOK || receiptID == "" {
		t.Fatalf("status=%d headers=%#v body=%s", response.Code, response.Header(), response.Body.String())
	}
	var receipts, referenced int
	if err := fixture.pool.QueryRow(context.Background(), `SELECT
		(SELECT count(*) FROM receipts WHERE acceptance_id=$1),
		(SELECT count(*) FROM object_intents WHERE tenant_id=$2 AND state='referenced')`, receiptID, fixture.tenantID).Scan(&receipts, &referenced); err != nil {
		t.Fatal(err)
	}
	if receipts != 1 || referenced != 1 {
		t.Fatalf("HTTP ACK was not durable: receipts=%d referenced=%d", receipts, referenced)
	}
}

var _ ingest.IngestController = (*revokingController)(nil)
