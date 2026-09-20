package crash_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/app"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/ingest"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/resource"
	"github.com/chawanghyeon/eventglass/internal/sdk"
	"github.com/chawanghyeon/eventglass/internal/storage"
	"github.com/jackc/pgx/v5/pgxpool"
)

const crashInstallationID = "00000000-0000-4000-8000-00000000c001"

type crashFixture struct {
	pool      *pgxpool.Pool
	store     *storage.S3Store
	s3        storage.S3Config
	tenantID  int64
	projectID int64
	keyHash   [32]byte
	keyFiles  [3]string
}

type barrierMessage struct {
	Name string `json:"name"`
	Addr string `json:"addr,omitempty"`
}

func requireCrashEnvironment(t *testing.T) map[string]string {
	t.Helper()
	if os.Getenv("EVENTGLASS_CRASH_REQUIRED") != "1" {
		t.Skip("run through ./scripts/check crash")
	}
	names := []string{"EVENTGLASS_DATABASE_URL", "EVENTGLASS_S3_ENDPOINT", "EVENTGLASS_S3_BUCKET", "EVENTGLASS_TEST_S3_PREFIX", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"}
	result := make(map[string]string, len(names))
	for _, name := range names {
		result[name] = os.Getenv(name)
		if result[name] == "" {
			t.Fatalf("%s is required by the crash gate", name)
		}
	}
	return result
}

func setupCrashFixture(t *testing.T, base int64) *crashFixture {
	t.Helper()
	environment := requireCrashEnvironment(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	if err := control.ApplyMigrations(ctx, environment["EVENTGLASS_DATABASE_URL"]); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, environment["EVENTGLASS_DATABASE_URL"])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s3Config := storage.S3Config{
		Endpoint: environment["EVENTGLASS_S3_ENDPOINT"], Region: "us-east-1", Bucket: environment["EVENTGLASS_S3_BUCKET"],
		Prefix: environment["EVENTGLASS_TEST_S3_PREFIX"], AccessKeyID: environment["AWS_ACCESS_KEY_ID"], SecretAccessKey: environment["AWS_SECRET_ACCESS_KEY"], PathStyle: true,
	}
	store, err := storage.NewS3Store(ctx, s3Config)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := app.StorageIdentity(s3Config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO installations(singleton,installation_id,storage_generation,schema_version,storage_identity)
		VALUES(true,$1,1,1,$2) ON CONFLICT(singleton) DO UPDATE SET installation_id=excluded.installation_id,storage_generation=1,schema_version=1,storage_identity=excluded.storage_identity`, crashInstallationID, identity); err != nil {
		t.Fatal(err)
	}
	marker, markerKey, _, err := app.InstallationMarker(crashInstallationID, identity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, markerKey, marker); err != nil {
		t.Fatal(err)
	}
	fixture := &crashFixture{pool: pool, store: store, s3: s3Config, tenantID: base, projectID: base*10 + 1, keyHash: sha256.Sum256([]byte(fmt.Sprintf("key-%d", base)))}
	// Stable across this fixture's restarts, outside the scratch directory that
	// the crash oracle intentionally removes. Never reuse production secrets.
	keyDirectory := t.TempDir()
	for index := range fixture.keyFiles {
		key := make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			t.Fatal(err)
		}
		fixture.keyFiles[index] = filepath.Join(keyDirectory, fmt.Sprintf("key-%d", index))
		if err := os.WriteFile(fixture.keyFiles[index], []byte(hex.EncodeToString(key)), 0600); err != nil {
			t.Fatal(err)
		}
	}
	keyID := crashUUID(base, 1)
	if _, err := pool.Exec(ctx, `INSERT INTO tenants(tenant_id) VALUES($1)`, fixture.tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO projects(tenant_id,project_id,scrub_revision) VALUES($1,$2,1)`, fixture.tenantID, fixture.projectID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO project_keys(tenant_id,project_id,key_hash,key_id,key_prefix) VALUES($1,$2,$3,$4,$5)`,
		fixture.tenantID, fixture.projectID, fixture.keyHash[:], keyID, hex.EncodeToString(fixture.keyHash[:4])); err != nil {
		t.Fatal(err)
	}
	for lane := 0; lane < model.LaneCount; lane++ {
		if _, err := pool.Exec(ctx, `INSERT INTO lanes(tenant_id,lane_id) VALUES($1,$2)`, fixture.tenantID, lane); err != nil {
			t.Fatal(err)
		}
	}
	return fixture
}

func crashUUID(base int64, sequence int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012x", base*1000+int64(sequence))
}

func crashCommand(t *testing.T, fixture *crashFixture, acceptanceID string) ingest.Command {
	t.Helper()
	request, err := ingest.NormalizeEnvelope(sdk.Envelope{Items: []sdk.Item{{Ordinal: 0, Type: "event", Value: map[string]any{
		"event_id": fmt.Sprintf("%032x", fixture.tenantID), "message": "crash durability",
	}}}}, ingest.NormalizeOptions{TenantID: fixture.tenantID, ProjectID: fixture.projectID, AcceptanceID: acceptanceID, ArrivalTime: time.Unix(1, 0)})
	if err != nil {
		t.Fatal(err)
	}
	return ingest.Command{Request: request, Authorization: ingest.Authorization{
		TenantID: fixture.tenantID, ProjectID: fixture.projectID, KeyHash: fixture.keyHash,
		TenantRevision: 1, ProjectRevision: 1, KeyRevision: 1, ScrubRevision: 1, ConfigRevision: 1,
	}}
}

func replacementWorkflow(t *testing.T, fixture *crashFixture, scratch string) *ingest.Workflow {
	t.Helper()
	operations, err := control.NewIngestOperations(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	workflow, err := ingest.NewWorkflow(ingest.WorkflowConfig{
		Control: operations, Store: fixture.store, InstallationID: crashInstallationID, StorageGeneration: 1,
		ProcessID: crashUUID(fixture.tenantID, 99), TempDir: scratch, SpoolBudget: resource.NewBudget(2 * storage.MaxJournalBytes),
	})
	if err != nil {
		t.Fatal(err)
	}
	return workflow
}

func childEnvironment(fixture *crashFixture, mode, acceptanceID, scratch string) []string {
	return append(os.Environ(),
		"EVENTGLASS_CRASH_HELPER=1", "EVENTGLASS_CRASH_MODE="+mode,
		"EVENTGLASS_CRASH_TENANT="+strconv.FormatInt(fixture.tenantID, 10), "EVENTGLASS_CRASH_PROJECT="+strconv.FormatInt(fixture.projectID, 10),
		"EVENTGLASS_CRASH_ACCEPTANCE="+acceptanceID, "EVENTGLASS_SCRATCH_DIR="+scratch,
		"EVENTGLASS_ROLES=api", "EVENTGLASS_HTTP_ADDR=127.0.0.1:0", "EVENTGLASS_PUBLIC_URL=http://127.0.0.1",
		"EVENTGLASS_AUTH_HASH_KEY_FILE="+fixture.keyFiles[0], "EVENTGLASS_TOKEN_KEY_FILE="+fixture.keyFiles[1],
		"EVENTGLASS_ALERT_ENCRYPTION_KEY_FILE="+fixture.keyFiles[2], "EVENTGLASS_INSECURE_COOKIE=true",
		"EVENTGLASS_S3_REGION="+fixture.s3.Region, "EVENTGLASS_S3_PREFIX="+fixture.s3.Prefix, "EVENTGLASS_S3_PATH_STYLE=true",
	)
}

func startCrashChild(t *testing.T, fixture *crashFixture, mode, acceptanceID, scratch string) (*exec.Cmd, barrierMessage) {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	executable, err := os.Executable()
	if err != nil {
		reader.Close()
		writer.Close()
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestCrashHelperProcess$")
	command.Env = childEnvironment(fixture, mode, acceptanceID, scratch)
	command.ExtraFiles = []*os.File{writer}
	command.Stdout, command.Stderr = &output, &output
	if err := command.Start(); err != nil {
		reader.Close()
		writer.Close()
		t.Fatal(err)
	}
	writer.Close()
	answer := make(chan struct {
		message barrierMessage
		err     error
	}, 1)
	go func() {
		var message barrierMessage
		err := json.NewDecoder(reader).Decode(&message)
		_ = reader.Close()
		answer <- struct {
			message barrierMessage
			err     error
		}{message, err}
	}()
	select {
	case result := <-answer:
		if result.err != nil {
			_ = command.Process.Kill()
			_ = command.Wait()
			t.Fatalf("child barrier: %v; output=%s", result.err, output.String())
		}
		return command, result.message
	case <-time.After(15 * time.Second):
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatalf("child barrier timeout; output=%s", output.String())
		return nil, barrierMessage{}
	}
}

func killChild(t *testing.T, command *exec.Cmd) {
	t.Helper()
	if err := command.Process.Signal(syscall.SIGKILL); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("SIGKILLed child exited successfully")
	}
}

func TestCrashBarriersAroundUploadAndAccept(t *testing.T) {
	for index, mode := range []string{"before-put", "after-put", "after-accept"} {
		t.Run(mode, func(t *testing.T) {
			fixture := setupCrashFixture(t, 710+int64(index))
			acceptanceID := crashUUID(fixture.tenantID, 10)
			scratch := filepath.Join(t.TempDir(), "killed")
			command, barrier := startCrashChild(t, fixture, mode, acceptanceID, scratch)
			if barrier.Name != mode {
				t.Fatalf("barrier=%#v", barrier)
			}
			killChild(t, command)
			if err := os.RemoveAll(scratch); err != nil {
				t.Fatal(err)
			}
			var pending, uploaded, referenced, receipts int
			if err := fixture.pool.QueryRow(context.Background(), `SELECT
				count(*) FILTER (WHERE state='pending'),count(*) FILTER (WHERE state='uploaded'),count(*) FILTER (WHERE state='referenced'),
				(SELECT count(*) FROM receipts WHERE acceptance_id=$1)
				FROM object_intents WHERE tenant_id=$2`, acceptanceID, fixture.tenantID).Scan(&pending, &uploaded, &referenced, &receipts); err != nil {
				t.Fatal(err)
			}
			var objectKey, expectedSHA string
			var expectedBytes int64
			if err := fixture.pool.QueryRow(context.Background(), `SELECT object_key,expected_bytes,expected_sha256 FROM object_intents WHERE tenant_id=$1`, fixture.tenantID).
				Scan(&objectKey, &expectedBytes, &expectedSHA); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "before-put":
				if pending != 1 || uploaded != 0 || referenced != 0 || receipts != 0 {
					t.Fatalf("D01 pending=%d uploaded=%d referenced=%d receipts=%d", pending, uploaded, referenced, receipts)
				}
				if _, err := fixture.store.Head(context.Background(), objectKey); err == nil {
					t.Fatal("D01 object exists before PUT")
				}
			case "after-put":
				if pending != 0 || uploaded != 1 || referenced != 0 || receipts != 0 {
					t.Fatalf("D02 pending=%d uploaded=%d referenced=%d receipts=%d", pending, uploaded, referenced, receipts)
				}
				object, err := fixture.store.Head(context.Background(), objectKey)
				if err != nil || object.Size != expectedBytes || object.SHA256 != expectedSHA {
					t.Fatalf("D02 uploaded object=%#v err=%v", object, err)
				}
			case "after-accept":
				if pending != 0 || uploaded != 0 || referenced != 1 || receipts != 1 {
					t.Fatalf("D04 pending=%d uploaded=%d referenced=%d receipts=%d", pending, uploaded, referenced, receipts)
				}
				workflow := replacementWorkflow(t, fixture, filepath.Join(t.TempDir(), "replacement"))
				results := workflow.Process(context.Background(), []ingest.Command{crashCommand(t, fixture, acceptanceID)})
				if len(results) != 1 || results[0].Err != nil || results[0].Receipt.AcceptanceID != acceptanceID {
					t.Fatalf("D04 replacement=%#v", results)
				}
				var batches, jobs int
				if err := fixture.pool.QueryRow(context.Background(), `SELECT
					(SELECT count(*) FROM ingest_batches WHERE tenant_id=$1),(SELECT count(*) FROM jobs WHERE tenant_id=$1)`, fixture.tenantID).Scan(&batches, &jobs); err != nil {
					t.Fatal(err)
				}
				if batches != 1 || jobs != 1 {
					t.Fatalf("D04 duplicated durable work: batches=%d jobs=%d", batches, jobs)
				}
			}
		})
	}
}

func TestACKOracleSurvivesSIGKILLRestartAndEmptyScratch(t *testing.T) {
	fixture := setupCrashFixture(t, 720)
	scratch := filepath.Join(t.TempDir(), "first-process")
	command, barrier := startCrashChild(t, fixture, "http", "", scratch)
	if barrier.Name != "listening" || barrier.Addr == "" {
		t.Fatalf("startup barrier=%#v", barrier)
	}
	body := []byte("{}\n{\"type\":\"event\"}\n{\"event_id\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"message\":\"parent oracle\"}")
	request, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/api/%d/envelope/?sentry_key=key-%d", barrier.Addr, fixture.projectID, fixture.tenantID), bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 10 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	receiptID := response.Header.Get("X-Eventglass-Receipt")
	responseBody, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || receiptID == "" {
		t.Fatalf("ACK status=%d receipt=%q body=%s", response.StatusCode, receiptID, responseBody)
	}
	oracle := control.ReceiptResult{AcceptanceID: receiptID, AcceptedCount: 1}
	killChild(t, command)
	if err := os.RemoveAll(scratch); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(scratch); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("killed process scratch still exists: %v", err)
	}
	restartScratch := filepath.Join(t.TempDir(), "replacement-process")
	restarted, restartBarrier := startCrashChild(t, fixture, "http", "", restartScratch)
	if restartBarrier.Name != "listening" {
		t.Fatalf("restart barrier=%#v", restartBarrier)
	}
	t.Cleanup(func() {
		if restarted.ProcessState == nil {
			_ = restarted.Process.Kill()
			_ = restarted.Wait()
		}
	})
	readyResponse, err := client.Get("http://" + restartBarrier.Addr + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	readyResponse.Body.Close()
	if readyResponse.StatusCode != http.StatusOK {
		t.Fatalf("restart readiness=%d", readyResponse.StatusCode)
	}

	var restored control.ReceiptResult
	var objectKey, expectedSHA string
	var expectedBytes int64
	err = fixture.pool.QueryRow(context.Background(), `SELECT r.acceptance_id::text,r.accepted_count,r.duplicate_count,r.conflict_count,r.unsupported_count,
		oi.object_key,oi.expected_bytes,oi.expected_sha256
		FROM receipts r JOIN ingest_batches b USING(tenant_id,lane_id,batch_seq)
		JOIN object_intents oi ON oi.intent_id=b.journal_intent_id WHERE r.acceptance_id=$1`, receiptID).Scan(
		&restored.AcceptanceID, &restored.AcceptedCount, &restored.DuplicateCount, &restored.ConflictCount, &restored.UnsupportedCount,
		&objectKey, &expectedBytes, &expectedSHA)
	if err != nil {
		t.Fatal(err)
	}
	if restored.AcceptanceID != oracle.AcceptanceID || restored.AcceptedCount != oracle.AcceptedCount || restored.DuplicateCount != 0 || restored.ConflictCount != 0 || restored.UnsupportedCount != 0 {
		t.Fatalf("D05 oracle=%#v restored=%#v", oracle, restored)
	}
	object, err := fixture.store.Head(context.Background(), objectKey)
	if err != nil || object.Size != expectedBytes || object.SHA256 != expectedSHA {
		t.Fatalf("restored journal head=%#v err=%v", object, err)
	}
	journalBytes, err := fixture.store.ReadRange(context.Background(), objectKey, 0, expectedBytes)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := storage.ReplayJournal(bytes.NewReader(journalBytes), storage.JournalInfo{Bytes: expectedBytes, SHA256: expectedSHA}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed.Requests) != 1 || replayed.Requests[0].AcceptanceID != oracle.AcceptanceID {
		t.Fatalf("D05 replayed index=%#v", replayed)
	}
	t.Logf("D05 restored ACK=%s journal_bytes=%d after SIGKILL and empty scratch", oracle.AcceptanceID, expectedBytes)
	killChild(t, restarted)
}

func TestPreparedAndPublishedOutputsSurviveWorkerSIGKILL(t *testing.T) {
	for index, mode := range []string{"after-prepare", "after-publish"} {
		t.Run(mode, func(t *testing.T) {
			fixture := setupCrashFixture(t, 730+int64(index))
			if _, err := fixture.pool.Exec(context.Background(), `UPDATE jobs SET state='completed',owner=NULL,lease_until=NULL WHERE state IN ('queued','running')`); err != nil {
				t.Fatal(err)
			}
			acceptanceID := crashUUID(fixture.tenantID, 10)
			request, err := ingest.NormalizeEnvelope(sdk.Envelope{Items: []sdk.Item{{Ordinal: 0, Type: "attachment", Payload: []byte("empty")}}}, ingest.NormalizeOptions{
				TenantID: fixture.tenantID, ProjectID: fixture.projectID, AcceptanceID: acceptanceID, ArrivalTime: time.Unix(2, 0),
			})
			if err != nil {
				t.Fatal(err)
			}
			workflow := replacementWorkflow(t, fixture, filepath.Join(t.TempDir(), "producer"))
			results := workflow.Process(context.Background(), []ingest.Command{{Request: request, Authorization: ingest.Authorization{
				TenantID: fixture.tenantID, ProjectID: fixture.projectID, KeyHash: fixture.keyHash,
				TenantRevision: 1, ProjectRevision: 1, KeyRevision: 1, ScrubRevision: 1, ConfigRevision: 1,
			}}})
			if len(results) != 1 || results[0].Err != nil || results[0].Receipt.AcceptedCount != 0 {
				t.Fatalf("empty ACK=%#v", results)
			}
			scratch := filepath.Join(t.TempDir(), "publication-worker")
			child, barrier := startCrashChild(t, fixture, mode, acceptanceID, scratch)
			if barrier.Name != mode {
				t.Fatalf("barrier=%#v", barrier)
			}
			killChild(t, child)
			if err := os.RemoveAll(scratch); err != nil {
				t.Fatal(err)
			}
			var jobID, outputID, manifestSHA, state string
			var laneID int
			var batchSeq, fence int64
			if err := fixture.pool.QueryRow(context.Background(), `SELECT j.job_id::text,j.lane_id,j.batch_seq,j.fence,j.state,j.prepared_output_id::text,o.manifest_sha256
				FROM jobs j JOIN job_outputs o ON o.output_id=j.prepared_output_id WHERE j.tenant_id=$1`, fixture.tenantID).Scan(&jobID, &laneID, &batchSeq, &fence, &state, &outputID, &manifestSHA); err != nil {
				t.Fatal(err)
			}
			if mode == "after-prepare" {
				if state != "prepared" {
					t.Fatalf("D09 job state=%s", state)
				}
				publication, err := control.ClaimPublicationJob(context.Background(), fixture.pool, crashInstallationID, 1, fixture.tenantID, laneID, "replacement-publisher", time.Minute)
				if err != nil || publication == nil {
					t.Fatalf("D09 replacement claim=%#v err=%v", publication, err)
				}
				published, err := control.Publish(context.Background(), fixture.pool, control.PublishCommand{Authority: publication.Authority, TenantID: fixture.tenantID, LaneID: laneID, BatchSeq: batchSeq, OutputID: outputID, ManifestSHA: manifestSHA})
				if err != nil || published.CatalogGeneration != 1 {
					t.Fatalf("D09 replacement Publish=%#v err=%v", published, err)
				}
			} else {
				if state != "completed" {
					t.Fatalf("D10 job state=%s", state)
				}
				authority := control.JobAuthority{InstallationID: crashInstallationID, StorageGeneration: 1, JobID: jobID, Owner: "crash-publication", Fence: fence}
				published, err := control.Publish(context.Background(), fixture.pool, control.PublishCommand{Authority: authority, TenantID: fixture.tenantID, LaneID: laneID, BatchSeq: batchSeq, OutputID: outputID, ManifestSHA: manifestSHA})
				if err != nil || !published.AlreadyPublished {
					t.Fatalf("D10 retry=%#v err=%v", published, err)
				}
			}
			var publishedSeq, generation, outputs int64
			if err := fixture.pool.QueryRow(context.Background(), `SELECT published_seq,catalog_generation,(SELECT count(*) FROM job_outputs WHERE job_id=$3 AND state='published')
				FROM lanes WHERE tenant_id=$1 AND lane_id=$2`, fixture.tenantID, laneID, jobID).Scan(&publishedSeq, &generation, &outputs); err != nil {
				t.Fatal(err)
			}
			if publishedSeq != batchSeq || generation != 1 || outputs != 1 {
				t.Fatalf("%s published=%d generation=%d outputs=%d", mode, publishedSeq, generation, outputs)
			}
		})
	}
}

type blockingStore struct {
	base    *storage.S3Store
	mode    string
	barrier func(string)
}

func (store *blockingStore) PutStream(ctx context.Context, key string, body io.ReadSeeker, size int64, checksum string) (storage.ObjectInfo, error) {
	if store.mode == "before-put" {
		store.barrier(store.mode)
	}
	return store.base.PutStream(ctx, key, body, size, checksum)
}

type blockingController struct {
	base    *control.IngestOperations
	mode    string
	barrier func(string)
}

func (controller *blockingController) RegisterJournalIntent(ctx context.Context, registration control.JournalIntentRegistration) error {
	return controller.base.RegisterJournalIntent(ctx, registration)
}

func (controller *blockingController) MarkJournalIntentUploaded(ctx context.Context, installationID string, generation, tenantID int64, authority control.IntentAuthority) error {
	err := controller.base.MarkJournalIntentUploaded(ctx, installationID, generation, tenantID, authority)
	if err == nil && controller.mode == "after-put" {
		controller.barrier(controller.mode)
	}
	return err
}

func (controller *blockingController) Accept(ctx context.Context, batch control.VerifiedBatch) ([]control.ReceiptResult, error) {
	results, err := controller.base.Accept(ctx, batch)
	if err == nil && controller.mode == "after-accept" {
		controller.barrier(controller.mode)
	}
	return results, err
}

func TestCrashHelperProcess(t *testing.T) {
	if os.Getenv("EVENTGLASS_CRASH_HELPER") != "1" {
		return
	}
	barrierFile := os.NewFile(3, "eventglass-crash-barrier")
	if barrierFile == nil {
		t.Fatal("missing inherited barrier FD")
	}
	defer barrierFile.Close()
	emit := func(name, addr string) {
		if err := json.NewEncoder(barrierFile).Encode(barrierMessage{Name: name, Addr: addr}); err != nil {
			panic(err)
		}
	}
	mode := os.Getenv("EVENTGLASS_CRASH_MODE")
	if mode == "http" {
		config, err := app.LoadConfigFromEnv(os.LookupEnv)
		if err != nil {
			t.Fatal(err)
		}
		runtime, err := app.NewRuntime(context.Background(), config)
		if err != nil {
			t.Fatal(err)
		}
		result := make(chan error, 1)
		go func() { result <- runtime.Run(context.Background()) }()
		<-runtime.Started()
		emit("listening", runtime.Addr())
		if err := <-result; err != nil {
			t.Fatal(err)
		}
		return
	}
	tenantID, err := strconv.ParseInt(os.Getenv("EVENTGLASS_CRASH_TENANT"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	projectID, err := strconv.ParseInt(os.Getenv("EVENTGLASS_CRASH_PROJECT"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	databaseURL := os.Getenv("EVENTGLASS_DATABASE_URL")
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if mode == "after-prepare" || mode == "after-publish" {
		publicationHelper(t, pool, mode, os.Getenv("EVENTGLASS_SCRATCH_DIR"), emit)
		return
	}
	operations, err := control.NewIngestOperations(pool)
	if err != nil {
		t.Fatal(err)
	}
	s3Config := storage.S3Config{
		Endpoint: os.Getenv("EVENTGLASS_S3_ENDPOINT"), Region: os.Getenv("EVENTGLASS_S3_REGION"), Bucket: os.Getenv("EVENTGLASS_S3_BUCKET"),
		Prefix: os.Getenv("EVENTGLASS_S3_PREFIX"), AccessKeyID: os.Getenv("AWS_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"), PathStyle: true,
	}
	store, err := storage.NewS3Store(context.Background(), s3Config)
	if err != nil {
		t.Fatal(err)
	}
	block := func(name string) {
		emit(name, "")
		select {}
	}
	controller := &blockingController{base: operations, mode: mode, barrier: block}
	objectStore := &blockingStore{base: store, mode: mode, barrier: block}
	workflow, err := ingest.NewWorkflow(ingest.WorkflowConfig{
		Control: controller, Store: objectStore, InstallationID: crashInstallationID, StorageGeneration: 1,
		ProcessID: crashUUID(tenantID, 98), TempDir: os.Getenv("EVENTGLASS_SCRATCH_DIR"), SpoolBudget: resource.NewBudget(2 * storage.MaxJournalBytes),
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture := &crashFixture{tenantID: tenantID, projectID: projectID, keyHash: sha256.Sum256([]byte(fmt.Sprintf("key-%d", tenantID)))}
	results := workflow.Process(context.Background(), []ingest.Command{crashCommand(t, fixture, os.Getenv("EVENTGLASS_CRASH_ACCEPTANCE"))})
	t.Fatalf("helper unexpectedly crossed barrier: %#v", results)
}

func publicationHelper(t *testing.T, pool *pgxpool.Pool, mode, scratch string, emit func(string, string)) {
	t.Helper()
	job, err := control.ClaimConversionJob(context.Background(), pool, crashInstallationID, 1, "crash-publication", time.Minute)
	if err != nil || job == nil {
		t.Fatalf("claim conversion=%#v err=%v", job, err)
	}
	work, err := control.LoadConversionWork(context.Background(), pool, job.Authority)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(scratch, 0o700); err != nil {
		t.Fatal(err)
	}
	occurrencePath := filepath.Join(scratch, "occurrences.jsonl")
	occurrence := []byte("{\"version\":1}\n")
	if err := os.WriteFile(occurrencePath, occurrence, 0o600); err != nil {
		t.Fatal(err)
	}
	occurrenceDigest := sha256.Sum256(occurrence)
	receiptHash := sha256.New()
	groupingVersion := 0
	for _, item := range work.Receipts {
		groupingVersion = item.GroupingVersion
		encoded, _ := json.Marshal(struct {
			AcceptanceID string `json:"acceptance_id"`
			SelectionSHA string `json:"selection_sha256"`
		}{item.Receipt.AcceptanceID, item.Receipt.SelectionSHA256})
		receiptHash.Write(encoded)
		receiptHash.Write([]byte{'\n'})
	}
	emptyIdentity := sha256.Sum256(nil)
	outputID := crashUUID(work.TenantID, 700)
	root := model.OutputManifestRoot{Version: 1, Header: model.OutputManifestHeader{
		Version: 1, OutputID: outputID, JobID: job.Authority.JobID, TenantID: work.TenantID, LaneID: work.LaneID, BatchSeq: work.BatchSeq,
		JournalSHA256: work.JournalSHA256, ReceiptSetSHA256: hex.EncodeToString(receiptHash.Sum(nil)), SelectedIdentitySHA256: hex.EncodeToString(emptyIdentity[:]),
		OccurrenceSummarySHA256: hex.EncodeToString(occurrenceDigest[:]), GroupingVersion: groupingVersion,
	}, Parts: []model.OutputManifestPartRef{}}
	if err := control.Prepare(context.Background(), pool, control.PrepareCommand{
		Authority: job.Authority, TenantID: work.TenantID, LaneID: work.LaneID, BatchSeq: work.BatchSeq, Root: root,
		Parts: []control.PreparedPartInput{}, OccurrencePath: occurrencePath, OccurrenceBytes: int64(len(occurrence)), OccurrenceSHA: root.Header.OccurrenceSummarySHA256,
	}); err != nil {
		t.Fatal(err)
	}
	if mode == "after-prepare" {
		emit(mode, "")
		select {}
	}
	publication, err := control.ClaimPublicationJob(context.Background(), pool, crashInstallationID, 1, work.TenantID, work.LaneID, "crash-publication", time.Minute)
	if err != nil || publication == nil {
		t.Fatalf("claim publication=%#v err=%v", publication, err)
	}
	if _, err := control.Publish(context.Background(), pool, control.PublishCommand{
		Authority: publication.Authority, TenantID: work.TenantID, LaneID: work.LaneID, BatchSeq: work.BatchSeq, OutputID: publication.OutputID, ManifestSHA: publication.ManifestSHA,
	}); err != nil {
		t.Fatal(err)
	}
	emit(mode, "")
	select {}
}

var _ ingest.JournalObjectStore = (*blockingStore)(nil)
var _ ingest.IngestController = (*blockingController)(nil)
