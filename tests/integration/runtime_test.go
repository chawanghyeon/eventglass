package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/app"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

func TestRuntimeSchemaAndConversionJobLeaseFencing(t *testing.T) {
	fixture := setupAcceptFixture(t, 604)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := control.VerifyRuntimeSchema(ctx, fixture.pool); err != nil {
		t.Fatal(err)
	}
	installation, err := control.LoadRuntimeInstallation(ctx, fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	if installation.InstallationID != acceptInstallationID || installation.StorageGeneration != 1 || installation.LaneCount != 16 {
		t.Fatalf("installation=%#v", installation)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE jobs SET state='completed',owner=NULL,lease_until=NULL WHERE state IN ('queued','running')`); err != nil {
		t.Fatal(err)
	}

	lane := 10
	request := fixture.request(fixture.uuidForLane(lane), "runtime-job", "", "")
	batch := fixture.batch(t, lane, "runtime-job", []control.VerifiedRequest{request})
	if _, err := control.Accept(ctx, fixture.pool, batch); err != nil {
		t.Fatal(err)
	}
	first, err := control.ClaimConversionJob(ctx, fixture.pool, acceptInstallationID, 1, "worker-a", time.Minute)
	if err != nil || first == nil || first.Authority.Fence != 1 || first.Attempt != 1 {
		t.Fatalf("first claim=%#v err=%v", first, err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE job_id=$1`, first.Authority.JobID); err != nil {
		t.Fatal(err)
	}
	second, err := control.ClaimConversionJob(ctx, fixture.pool, acceptInstallationID, 1, "worker-b", time.Minute)
	if err != nil || second == nil || second.Authority.JobID != first.Authority.JobID || second.Authority.Fence != 2 || second.Attempt != 2 {
		t.Fatalf("reclaimed=%#v err=%v", second, err)
	}
	if _, err := control.HeartbeatConversionJob(ctx, fixture.pool, first.Authority, time.Minute); !errors.Is(err, control.ErrJobFenceStale) {
		t.Fatalf("expired authority heartbeat=%v", err)
	}
	if _, err := control.HeartbeatConversionJob(ctx, fixture.pool, second.Authority, time.Minute); err != nil {
		t.Fatalf("current authority heartbeat=%v", err)
	}
}

func TestRuntimeStartsDurableIngressAndDrains(t *testing.T) {
	fixture := setupAcceptFixture(t, 605)
	environment := requiredEnvironment(t, "EVENTGLASS_DATABASE_URL", "EVENTGLASS_S3_ENDPOINT", "EVENTGLASS_S3_BUCKET")
	s3Config := storage.S3Config{
		Endpoint: environment["EVENTGLASS_S3_ENDPOINT"], Region: "us-east-1", Bucket: environment["EVENTGLASS_S3_BUCKET"],
		Prefix: "runtime-605", PathStyle: true,
	}
	identity, err := app.StorageIdentity(s3Config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(context.Background(), `UPDATE installations SET storage_identity=$1 WHERE singleton`, identity); err != nil {
		t.Fatal(err)
	}
	config := app.Config{
		DatabaseURL: environment["EVENTGLASS_DATABASE_URL"], HTTPAddr: "127.0.0.1:0", PublicURL: "http://127.0.0.1",
		ScratchDir: filepath.Join(t.TempDir(), "runtime"), Roles: map[app.Role]bool{app.RoleAPI: true}, S3: s3Config, DrainTimeout: time.Second,
	}
	if _, err := app.NewRuntime(context.Background(), config); err == nil {
		t.Fatal("runtime started without the installation marker")
	}
	store := integrationStore(t, s3Config.Prefix)
	marker, markerKey, _, err := app.InstallationMarker(acceptInstallationID, identity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(context.Background(), markerKey, marker); err != nil {
		t.Fatal(err)
	}
	runtime, err := app.NewRuntime(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	runContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(runContext) }()
	select {
	case <-runtime.Started():
	case <-time.After(5 * time.Second):
		t.Fatal("runtime did not start")
	}
	client := &http.Client{Timeout: 5 * time.Second}
	baseURL := "http://" + runtime.Addr()
	response, err := client.Get(baseURL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("ready status=%d", response.StatusCode)
	}
	body := []byte("{}\n{\"type\":\"event\"}\n{\"event_id\":\"ffffffffffffffffffffffffffffffff\",\"message\":\"runtime durable\"}")
	request, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/%d/envelope/?sentry_key=key-%d", baseURL, fixture.projectID, fixture.tenantID), bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	response, err = client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	receiptID := response.Header.Get("X-Eventglass-Receipt")
	responseBody, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || receiptID == "" {
		t.Fatalf("ingest status=%d receipt=%q body=%s", response.StatusCode, receiptID, responseBody)
	}
	var receipts, referenced int
	if err := fixture.pool.QueryRow(context.Background(), `SELECT
		(SELECT count(*) FROM receipts WHERE acceptance_id=$1),
		(SELECT count(*) FROM object_intents WHERE tenant_id=$2 AND state='referenced')`, receiptID, fixture.tenantID).Scan(&receipts, &referenced); err != nil {
		t.Fatal(err)
	}
	if receipts != 1 || referenced != 1 {
		t.Fatalf("runtime ACK was not durable: receipts=%d referenced=%d", receipts, referenced)
	}
	if err := store.Delete(context.Background(), []string{markerKey}); err != nil {
		t.Fatal(err)
	}
	response, err = client.Get(baseURL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readiness ignored missing storage marker: %d", response.StatusCode)
	}
	cancel()
	if err := <-runResult; err != nil {
		t.Fatalf("runtime drain=%v", err)
	}
}
