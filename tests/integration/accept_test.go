package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

const acceptInstallationID = "00000000-0000-4000-8000-00000000a001"

type acceptFixture struct {
	pool      *pgxpool.Pool
	tenantID  int64
	projectID int64
	keyHash   [32]byte
	auth      control.AuthorizationSnapshot
	sequence  int
}

func setupAcceptFixture(t *testing.T, base int64) *acceptFixture {
	t.Helper()
	environment := requiredEnvironment(t, "EVENTGLASS_DATABASE_URL")
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
	fixture := &acceptFixture{pool: pool, tenantID: base, projectID: base*10 + 1, keyHash: sha256.Sum256([]byte(fmt.Sprintf("key-%d", base)))}
	fixture.auth = control.AuthorizationSnapshot{TenantRevision: 1, ProjectRevision: 1, KeyRevision: 1, ScrubRevision: 1, ConfigRevision: 1, KeyHash: fixture.keyHash}
	if _, err := pool.Exec(ctx, `INSERT INTO installations(singleton,installation_id,storage_generation,schema_version,storage_identity)
		VALUES(true,$1,1,1,'integration-storage') ON CONFLICT(singleton) DO NOTHING`, acceptInstallationID); err != nil {
		t.Fatal(err)
	}
	keyID := fixture.uuidForLane(0)
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

func (fixture *acceptFixture) uuidForLane(lane int) string {
	for {
		fixture.sequence++
		candidate := fmt.Sprintf("00000000-0000-4000-8000-%012x", fixture.tenantID*10000+int64(fixture.sequence))
		got, err := model.LaneForAcceptance(candidate)
		if err == nil && got == lane {
			return candidate
		}
	}
}

func fixtureSHA(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func (fixture *acceptFixture) request(acceptanceID, label, sourceID, payloadHash string, outcomes ...model.Outcome) control.VerifiedRequest {
	candidates := []control.Candidate{{Position: 0, RecordID: fixtureSHA("record:" + label), Kind: model.KindError}}
	if sourceID != "" {
		candidates[0].SourceEventID = sourceID
		candidates[0].DedupeSHA256 = payloadHash
	}
	return control.VerifiedRequest{
		Index: model.JournalRequestIndex{
			AcceptanceID: acceptanceID, ProjectID: fixture.projectID, OrdinalFirst: 0, OrdinalLast: 0, RecordCount: 1,
			ContentSHA256: fixtureSHA("content:" + label), Outcomes: outcomes,
		},
		Candidates: candidates, Authorization: fixture.auth,
	}
}

func (fixture *acceptFixture) batch(t *testing.T, lane int, label string, requests []control.VerifiedRequest) control.VerifiedBatch {
	t.Helper()
	position := 0
	for index := range requests {
		requests[index].Index.OrdinalFirst = position
		requests[index].Index.OrdinalLast = position + len(requests[index].Candidates) - 1
		requests[index].Index.RecordCount = len(requests[index].Candidates)
		position += len(requests[index].Candidates)
	}
	batch := control.VerifiedBatch{
		InstallationID: acceptInstallationID, StorageGeneration: 1, TenantID: fixture.tenantID, LaneID: lane,
		BatchID: fixture.uuidForLane(0), JobID: fixture.uuidForLane(0),
		Journal:  control.IntentAuthority{IntentID: fixture.uuidForLane(0), Owner: "integration-owner", Fence: 1, Bytes: 100, SHA256: fixtureSHA("journal:" + label)},
		Requests: requests,
	}
	registration := control.JournalIntentRegistration{
		InstallationID: acceptInstallationID, StorageGeneration: 1, TenantID: fixture.tenantID,
		ObjectKey: "v1/integration/" + label + "/" + batch.Journal.IntentID, Authority: batch.Journal,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := control.RegisterJournalIntent(ctx, fixture.pool, registration); err != nil {
		t.Fatal(err)
	}
	if err := control.MarkJournalIntentUploaded(ctx, fixture.pool, acceptInstallationID, 1, fixture.tenantID, batch.Journal); err != nil {
		t.Fatal(err)
	}
	return batch
}

func TestAcceptConcurrentCrossLaneDedupeAndIdempotency(t *testing.T) {
	fixture := setupAcceptFixture(t, 501)
	sourceID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	firstID := fixture.uuidForLane(0)
	secondID := fixture.uuidForLane(1)
	first := fixture.batch(t, 0, "concurrent-a", []control.VerifiedRequest{fixture.request(firstID, "a", sourceID, fixtureSHA("payload-a"))})
	second := fixture.batch(t, 1, "concurrent-b", []control.VerifiedRequest{fixture.request(secondID, "b", sourceID, fixtureSHA("payload-b"))})

	type answer struct {
		results []control.ReceiptResult
		err     error
		hash    string
	}
	answers := make(chan answer, 2)
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(2)
	for _, batch := range []control.VerifiedBatch{first, second} {
		go func(batch control.VerifiedBatch) {
			ready.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			results, err := control.Accept(ctx, fixture.pool, batch)
			answers <- answer{results: results, err: err, hash: batch.Requests[0].Candidates[0].DedupeSHA256}
		}(batch)
	}
	ready.Wait()
	close(start)
	accepted, conflicts := 0, 0
	acceptedHash := ""
	for range 2 {
		answer := <-answers
		if answer.err != nil || len(answer.results) != 1 {
			t.Fatalf("concurrent Accept: results=%#v err=%v", answer.results, answer.err)
		}
		accepted += answer.results[0].AcceptedCount
		conflicts += answer.results[0].ConflictCount
		if answer.results[0].AcceptedCount == 1 {
			acceptedHash = answer.hash
		}
	}
	if accepted != 1 || conflicts != 1 {
		t.Fatalf("accepted=%d conflicts=%d", accepted, conflicts)
	}
	ctx := context.Background()
	var dedupeRows, batches, jobs int
	if err := fixture.pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM event_dedupe WHERE tenant_id=$1),
		(SELECT count(*) FROM ingest_batches WHERE tenant_id=$1),
		(SELECT count(*) FROM jobs WHERE tenant_id=$1)`, fixture.tenantID).Scan(&dedupeRows, &batches, &jobs); err != nil {
		t.Fatal(err)
	}
	if dedupeRows != 1 || batches != 2 || jobs != 2 {
		t.Fatalf("dedupe=%d batches=%d jobs=%d", dedupeRows, batches, jobs)
	}
	duplicateID := fixture.uuidForLane(2)
	duplicate := fixture.batch(t, 2, "duplicate", []control.VerifiedRequest{fixture.request(duplicateID, "duplicate", sourceID, acceptedHash)})
	duplicateResult, err := control.Accept(ctx, fixture.pool, duplicate)
	if err != nil || duplicateResult[0].DuplicateCount != 1 || duplicateResult[0].AcceptedCount != 0 {
		t.Fatalf("same source payload was not duplicate: %#v %v", duplicateResult, err)
	}
	retry, err := control.Accept(ctx, fixture.pool, first)
	if err != nil || len(retry) != 1 {
		t.Fatalf("idempotent retry: %#v %v", retry, err)
	}
	changed := first
	changed.Requests = append([]control.VerifiedRequest(nil), first.Requests...)
	changed.Requests[0].Index.ContentSHA256 = fixtureSHA("different-content")
	if _, err := control.Accept(ctx, fixture.pool, changed); !errors.Is(err, control.ErrIdempotencyConflict) {
		t.Fatalf("content conflict returned %v", err)
	}
}

func TestAcceptRejectsStaleAuthorityWithoutWrites(t *testing.T) {
	tests := []struct {
		name    string
		mutate  string
		wantErr error
	}{
		{"tenant revision", `UPDATE tenants SET auth_revision=2 WHERE tenant_id=$1`, control.ErrAuthorizationStale},
		{"tenant disabled", `UPDATE tenants SET state='disabled',auth_revision=2 WHERE tenant_id=$1`, control.ErrTenantDisabled},
		{"project revision", `UPDATE projects SET auth_revision=2 WHERE tenant_id=$1`, control.ErrAuthorizationStale},
		{"project disabled", `UPDATE projects SET state='disabled',auth_revision=2 WHERE tenant_id=$1`, control.ErrProjectDisabled},
		{"key revision", `UPDATE project_keys SET revision=2 WHERE tenant_id=$1`, control.ErrAuthorizationStale},
		{"key revoked", `UPDATE project_keys SET state='revoked',revision=2 WHERE tenant_id=$1`, control.ErrKeyRevoked},
		{"scrub revision", `UPDATE projects SET scrub_revision=2 WHERE tenant_id=$1`, control.ErrAuthorizationStale},
		{"config revision", `UPDATE projects SET config_revision=2 WHERE tenant_id=$1`, control.ErrAuthorizationStale},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := setupAcceptFixture(t, int64(510+index))
			lane := 6
			acceptanceID := fixture.uuidForLane(lane)
			batch := fixture.batch(t, lane, "stale-"+test.name, []control.VerifiedRequest{fixture.request(acceptanceID, "stale", "", "")})
			if _, err := fixture.pool.Exec(context.Background(), test.mutate, fixture.tenantID); err != nil {
				t.Fatal(err)
			}
			if _, err := control.Accept(context.Background(), fixture.pool, batch); !errors.Is(err, test.wantErr) {
				t.Fatalf("got %v want %v", err, test.wantErr)
			}
			var seq, rows int64
			var intentState string
			if err := fixture.pool.QueryRow(context.Background(), `SELECT l.accepted_seq,
				(SELECT count(*) FROM receipts r WHERE r.tenant_id=l.tenant_id),
				(SELECT state FROM object_intents WHERE intent_id=$3)
				FROM lanes l WHERE l.tenant_id=$1 AND l.lane_id=$2`, fixture.tenantID, lane, batch.Journal.IntentID).Scan(&seq, &rows, &intentState); err != nil {
				t.Fatal(err)
			}
			if seq != 0 || rows != 0 || intentState != "uploaded" {
				t.Fatalf("stale Accept mutated seq=%d rows=%d intent=%s", seq, rows, intentState)
			}
		})
	}
}

func TestAcceptRollbackEmptyMissingSourceExpiryAndSubset(t *testing.T) {
	fixture := setupAcceptFixture(t, 502)
	ctx := context.Background()
	lane := 2
	acceptanceID := fixture.uuidForLane(lane)
	overflow := fixture.request(acceptanceID, "rollback", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", fixtureSHA("rollback-payload"),
		model.Outcome{ItemOrdinal: 0, Category: "error", Reason: "overflow", Quantity: math.MaxInt64},
		model.Outcome{ItemOrdinal: 0, Category: "error", Reason: "overflow", Quantity: 1})
	batch := fixture.batch(t, lane, "rollback", []control.VerifiedRequest{overflow})
	if _, err := control.Accept(ctx, fixture.pool, batch); err == nil {
		t.Fatal("overflowing outcome accepted")
	}
	var acceptedSeq, batches int64
	if err := fixture.pool.QueryRow(ctx, `SELECT accepted_seq,(SELECT count(*) FROM ingest_batches WHERE tenant_id=$1 AND lane_id=$2) FROM lanes WHERE tenant_id=$1 AND lane_id=$2`, fixture.tenantID, lane).Scan(&acceptedSeq, &batches); err != nil {
		t.Fatal(err)
	}
	if acceptedSeq != 0 || batches != 0 {
		t.Fatalf("rollback left seq=%d batches=%d", acceptedSeq, batches)
	}
	batch.Requests[0].Index.Outcomes = nil
	if _, err := fixture.pool.Exec(ctx, `CREATE FUNCTION eventglass_test_fail_receipt_502() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF NEW.tenant_id=502 THEN RAISE EXCEPTION 'injected receipt failure'; END IF; RETURN NEW; END $$;
		CREATE TRIGGER eventglass_test_fail_receipt_502 BEFORE INSERT ON receipts FOR EACH ROW EXECUTE FUNCTION eventglass_test_fail_receipt_502()`); err != nil {
		t.Fatal(err)
	}
	if _, err := control.Accept(ctx, fixture.pool, batch); err == nil {
		t.Fatal("injected receipt failure did not abort Accept")
	}
	if err := fixture.pool.QueryRow(ctx, `SELECT accepted_seq,(SELECT count(*) FROM event_dedupe WHERE tenant_id=$1) FROM lanes WHERE tenant_id=$1 AND lane_id=$2`, fixture.tenantID, lane).Scan(&acceptedSeq, &batches); err != nil {
		t.Fatal(err)
	}
	if acceptedSeq != 0 || batches != 0 {
		t.Fatalf("failed SQL transaction left seq=%d dedupe=%d", acceptedSeq, batches)
	}
	if _, err := fixture.pool.Exec(ctx, `DROP TRIGGER eventglass_test_fail_receipt_502 ON receipts; DROP FUNCTION eventglass_test_fail_receipt_502()`); err != nil {
		t.Fatal(err)
	}
	result, err := control.Accept(ctx, fixture.pool, batch)
	if err != nil || result[0].BatchSeq != 1 {
		t.Fatalf("valid retry did not fill seq 1: %#v %v", result, err)
	}

	expiryID := fixture.uuidForLane(3)
	expired := fixture.batch(t, 3, "expiry", []control.VerifiedRequest{fixture.request(expiryID, "expiry", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", fixtureSHA("new-after-expiry"))})
	if _, err := fixture.pool.Exec(ctx, `UPDATE event_dedupe SET expires_at=clock_timestamp()-interval '1 second' WHERE tenant_id=$1`, fixture.tenantID); err != nil {
		t.Fatal(err)
	}
	expiryResult, err := control.Accept(ctx, fixture.pool, expired)
	if err != nil || expiryResult[0].AcceptedCount != 1 {
		t.Fatalf("expired source was not replaced: %#v %v", expiryResult, err)
	}

	emptyID := fixture.uuidForLane(4)
	missingID := fixture.uuidForLane(4)
	empty := control.VerifiedRequest{Index: model.JournalRequestIndex{
		AcceptanceID: emptyID, ProjectID: fixture.projectID, OrdinalLast: -1, ContentSHA256: fixtureSHA("empty"),
		Outcomes:         []model.Outcome{{ItemOrdinal: 0, Category: "attachment", Reason: "unsupported", Quantity: 1}},
		UnsupportedItems: []model.UnsupportedItem{{ItemOrdinal: 0, Type: "attachment", Bytes: 5}},
	}, Authorization: fixture.auth}
	missing := fixture.request(missingID, "missing-source", "", "")
	emptyBatch := fixture.batch(t, 4, "empty-missing", []control.VerifiedRequest{empty, missing})
	emptyResults, err := control.Accept(ctx, fixture.pool, emptyBatch)
	if err != nil || len(emptyResults) != 2 || emptyResults[0].AcceptedCount != 0 || emptyResults[0].UnsupportedCount != 1 || emptyResults[1].AcceptedCount != 1 || emptyResults[0].Selection.Version != 1 {
		t.Fatalf("empty/missing-source results=%#v err=%v", emptyResults, err)
	}
	var outcomeRows, unsupportedRows int
	if err := fixture.pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM sdk_outcomes WHERE acceptance_id=$1),
		(SELECT count(*) FROM receipt_unsupported WHERE acceptance_id=$1)`, emptyID).Scan(&outcomeRows, &unsupportedRows); err != nil || outcomeRows != 1 || unsupportedRows != 1 {
		t.Fatalf("empty diagnostics outcomes=%d unsupported=%d err=%v", outcomeRows, unsupportedRows, err)
	}

	repeatedSource := "cccccccccccccccccccccccccccccccc"
	repeatedHash := fixtureSHA("repeated")
	repeatedRequests := []control.VerifiedRequest{
		fixture.request(fixture.uuidForLane(7), "repeated-first", repeatedSource, repeatedHash),
		fixture.request(fixture.uuidForLane(7), "repeated-duplicate", repeatedSource, repeatedHash),
		fixture.request(fixture.uuidForLane(7), "repeated-conflict", repeatedSource, fixtureSHA("repeated-different")),
	}
	repeatedBatch := fixture.batch(t, 7, "repeated-source", repeatedRequests)
	repeatedResults, err := control.Accept(ctx, fixture.pool, repeatedBatch)
	if err != nil || repeatedResults[0].AcceptedCount != 1 || repeatedResults[1].DuplicateCount != 1 || repeatedResults[2].ConflictCount != 1 {
		t.Fatalf("repeated source selection=%#v err=%v", repeatedResults, err)
	}

	partialID := fixture.uuidForLane(5)
	partialFirst := fixture.request(partialID, "partial-first", "", "")
	partialBatch := fixture.batch(t, 5, "partial-first", []control.VerifiedRequest{partialFirst})
	if _, err := control.Accept(ctx, fixture.pool, partialBatch); err != nil {
		t.Fatal(err)
	}
	newID := fixture.uuidForLane(5)
	partialMixed := fixture.batch(t, 5, "partial-mixed", []control.VerifiedRequest{partialFirst, fixture.request(newID, "partial-new", "", "")})
	_, err = control.Accept(ctx, fixture.pool, partialMixed)
	var subset *control.ExistingSubsetError
	if !errors.As(err, &subset) || len(subset.Existing) != 1 || len(subset.Missing) != 1 || subset.Missing[0] != newID {
		t.Fatalf("partial existing result=%#v err=%v", subset, err)
	}
	var state string
	if err := fixture.pool.QueryRow(ctx, `SELECT state FROM object_intents WHERE intent_id=$1`, partialMixed.Journal.IntentID).Scan(&state); err != nil || state != "uploaded" {
		t.Fatalf("partial batch intent state=%q err=%v", state, err)
	}
}
