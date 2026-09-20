package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
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

func TestSchedulerRoleAdvancesPersistedRetentionFloor(t *testing.T) {
	fixture := setupAcceptFixture(t, 608)
	environment := requiredEnvironment(t, "EVENTGLASS_DATABASE_URL", "EVENTGLASS_S3_ENDPOINT", "EVENTGLASS_S3_BUCKET")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	s3Config := storage.S3Config{
		Endpoint: environment["EVENTGLASS_S3_ENDPOINT"], Region: "us-east-1", Bucket: environment["EVENTGLASS_S3_BUCKET"],
		Prefix: "runtime-scheduler-608", PathStyle: true,
	}
	identity, err := app.StorageIdentity(s3Config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE installations SET storage_identity=$1,retention_floor_us=0,retention_tick_at=clock_timestamp()-interval '10 minutes' WHERE singleton`, identity); err != nil {
		t.Fatal(err)
	}
	store := integrationStore(t, s3Config.Prefix)
	marker, markerKey, _, err := app.InstallationMarker(acceptInstallationID, identity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, markerKey, marker); err != nil {
		t.Fatal(err)
	}
	runtime, err := app.NewRuntime(ctx, app.Config{
		DatabaseURL: fixture.pool.Config().ConnString(), HTTPAddr: "127.0.0.1:0", PublicURL: "http://127.0.0.1",
		ScratchDir: filepath.Join(t.TempDir(), "runtime-scheduler"), InsecureCookie: true,
		Roles: map[app.Role]bool{app.RoleScheduler: true}, S3: s3Config, DrainTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	runContext, stop := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- runtime.Run(runContext) }()
	select {
	case <-runtime.Started():
	case <-ctx.Done():
		t.Fatal("scheduler runtime did not start")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		var floor int64
		var age float64
		if err := fixture.pool.QueryRow(ctx, `SELECT retention_floor_us,extract(epoch FROM (clock_timestamp()-retention_tick_at)) FROM installations WHERE singleton`).Scan(&floor, &age); err != nil {
			t.Fatal(err)
		}
		if floor > 0 && age < 5 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("scheduler did not advance floor: floor=%d age=%f", floor, age)
		}
		time.Sleep(25 * time.Millisecond)
	}
	stop()
	if err := <-result; err != nil {
		t.Fatalf("scheduler runtime drain=%v", err)
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
		DatabaseURL: fixture.pool.Config().ConnString(), HTTPAddr: "127.0.0.1:0", PublicURL: "http://127.0.0.1",
		ScratchDir: filepath.Join(t.TempDir(), "runtime"), AuthHashKeyFile: authHashKeyFile(t), TokenKeyFile: authHashKeyFile(t), AlertEncryptionKeyFile: authHashKeyFile(t), InsecureCookie: true,
		Roles: map[app.Role]bool{app.RoleAPI: true}, S3: s3Config, DrainTimeout: time.Second,
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

func TestRuntimeExposesOnlySetupSurfaceUntilInitializationCompletes(t *testing.T) {
	fixture := setupAcceptFixture(t, 607)
	environment := requiredEnvironment(t, "EVENTGLASS_DATABASE_URL", "EVENTGLASS_S3_ENDPOINT", "EVENTGLASS_S3_BUCKET")
	s3Config := storage.S3Config{
		Endpoint: environment["EVENTGLASS_S3_ENDPOINT"], Region: "us-east-1", Bucket: environment["EVENTGLASS_S3_BUCKET"],
		Prefix: "runtime-setup-607", PathStyle: true,
	}
	identity, err := app.StorageIdentity(s3Config)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap := bytes.Repeat([]byte{0x61}, 32)
	bootstrapHash := sha256.Sum256(bootstrap)
	if _, err := fixture.pool.Exec(context.Background(), `UPDATE installations SET storage_identity=$1,setup_state='uninitialized',
		setup_attempt=NULL,setup_request_fingerprint=NULL,setup_marker_key=NULL,setup_marker_sha256=NULL,setup_owner=NULL,
		setup_lease_until=NULL,bootstrap_token_hash=$2,setup_completed_at=NULL WHERE singleton`, identity, bootstrapHash[:]); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = fixture.pool.Exec(context.Background(), `UPDATE installations SET setup_state='ready',bootstrap_token_hash=NULL,
			setup_completed_at=clock_timestamp() WHERE singleton`)
	})
	bootstrapPath := filepath.Join(t.TempDir(), "bootstrap-token")
	if err := os.WriteFile(bootstrapPath, []byte(fmt.Sprintf("%x\n", bootstrap)), 0o600); err != nil {
		t.Fatal(err)
	}
	config := app.Config{
		DatabaseURL: fixture.pool.Config().ConnString(), HTTPAddr: "127.0.0.1:0", PublicURL: "http://127.0.0.1",
		ScratchDir: filepath.Join(t.TempDir(), "runtime-setup"), BootstrapTokenFile: bootstrapPath,
		AuthHashKeyFile: authHashKeyFile(t), TokenKeyFile: authHashKeyFile(t), AlertEncryptionKeyFile: authHashKeyFile(t), InsecureCookie: true, Roles: map[app.Role]bool{app.RoleAPI: true},
		S3: s3Config, DrainTimeout: time.Second,
	}
	runtime, err := app.NewRuntime(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	runContext, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(runContext) }()
	select {
	case <-runtime.Started():
	case <-time.After(5 * time.Second):
		t.Fatal("setup runtime did not start")
	}
	client := &http.Client{Timeout: 5 * time.Second}
	baseURL := "http://" + runtime.Addr()
	for path, expected := range map[string]int{"/readyz": http.StatusServiceUnavailable, "/v1/setup": http.StatusOK} {
		response, err := client.Get(baseURL + path)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if response.StatusCode != expected {
			t.Fatalf("%s status=%d want=%d", path, response.StatusCode, expected)
		}
	}
	ingestRequest, err := http.NewRequest(http.MethodPost, baseURL+"/api/1/envelope/?sentry_key=blocked", bytes.NewReader([]byte("{}\n")))
	if err != nil {
		t.Fatal(err)
	}
	ingestResponse, err := client.Do(ingestRequest)
	if err != nil {
		t.Fatal(err)
	}
	ingestResponse.Body.Close()
	if ingestResponse.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("uninitialized ingress status=%d", ingestResponse.StatusCode)
	}
	cancel()
	if err := <-runResult; err != nil {
		t.Fatalf("setup runtime drain=%v", err)
	}
}

func TestRuntimeWorkerPublishesEmptyAckedBatch(t *testing.T) {
	fixture := setupAcceptFixture(t, 606)
	environment := requiredEnvironment(t, "EVENTGLASS_DATABASE_URL", "EVENTGLASS_S3_ENDPOINT", "EVENTGLASS_S3_BUCKET")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := fixture.pool.Exec(ctx, `UPDATE jobs SET state='completed',owner=NULL,lease_until=NULL WHERE state IN ('queued','running')`); err != nil {
		t.Fatal(err)
	}
	s3Config := storage.S3Config{
		Endpoint: environment["EVENTGLASS_S3_ENDPOINT"], Region: "us-east-1", Bucket: environment["EVENTGLASS_S3_BUCKET"],
		Prefix: "runtime-worker-606", PathStyle: true,
	}
	identity, err := app.StorageIdentity(s3Config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE installations SET storage_identity=$1 WHERE singleton`, identity); err != nil {
		t.Fatal(err)
	}
	store := integrationStore(t, s3Config.Prefix)
	marker, markerKey, _, err := app.InstallationMarker(acceptInstallationID, identity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, markerKey, marker); err != nil {
		t.Fatal(err)
	}
	config := app.Config{
		DatabaseURL: fixture.pool.Config().ConnString(), HTTPAddr: "127.0.0.1:0", PublicURL: "http://127.0.0.1",
		ScratchDir: filepath.Join(t.TempDir(), "runtime-worker"), AuthHashKeyFile: authHashKeyFile(t), TokenKeyFile: authHashKeyFile(t), AlertEncryptionKeyFile: authHashKeyFile(t), InsecureCookie: true,
		Roles: map[app.Role]bool{app.RoleAPI: true, app.RoleWorker: true}, S3: s3Config, DrainTimeout: 3 * time.Second,
	}
	runtime, err := app.NewRuntime(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	runContext, stop := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(runContext) }()
	select {
	case <-runtime.Started():
	case <-time.After(5 * time.Second):
		t.Fatal("worker runtime did not start")
	}
	body := []byte("{}\n{\"type\":\"attachment\",\"length\":4}\n\x00\n\xff\x01")
	request, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/api/%d/envelope/?sentry_key=key-%d", runtime.Addr(), fixture.projectID, fixture.tenantID), bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	receiptID := response.Header.Get("X-Eventglass-Receipt")
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || receiptID == "" {
		t.Fatalf("empty ingest status=%d receipt=%q", response.StatusCode, receiptID)
	}
	var laneID int
	var batchSeq int64
	if err := fixture.pool.QueryRow(ctx, `SELECT lane_id,batch_seq FROM receipts WHERE acceptance_id=$1`, receiptID).Scan(&laneID, &batchSeq); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		var publishedSeq, generation int64
		var bundles int
		if err := fixture.pool.QueryRow(ctx, `SELECT published_seq,catalog_generation,(SELECT count(*) FROM bundles WHERE tenant_id=$1) FROM lanes WHERE tenant_id=$1 AND lane_id=$2`, fixture.tenantID, laneID).Scan(&publishedSeq, &generation, &bundles); err != nil {
			t.Fatal(err)
		}
		if publishedSeq == batchSeq {
			if generation != 1 || bundles != 0 {
				t.Fatalf("empty publication generation=%d bundles=%d", generation, bundles)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("worker did not publish batch %d", batchSeq)
		}
		time.Sleep(50 * time.Millisecond)
	}
	stop()
	if err := <-runResult; err != nil {
		t.Fatalf("worker runtime drain=%v", err)
	}
}

func authHashKeyFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "auth-hash-key")
	if err := os.WriteFile(path, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
