package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
	"github.com/jackc/pgx/v5"
)

func TestSnapshotAdmissionRenewalRevocationAndRetentionClock(t *testing.T) {
	fixture := setupAcceptFixture(t, 960)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	operations, tokenHash := setupQueryPrincipal(t, fixture)
	command := snapshotCommand(t, fixture, tokenHash, 1)
	snapshot, err := operations.CreateSnapshot(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.RetentionFloorUS != 77 || snapshot.Lanes[15].CutSeq != 5 || snapshot.Lanes[15].CatalogGeneration != 7 || snapshot.PrincipalHash != model.QueryPrincipalHash(snapshot.UserID, tokenHash) {
		t.Fatalf("snapshot=%#v", snapshot)
	}
	if renewed, err := operations.RenewSnapshot(ctx, tokenHash, fixture.tenantID, snapshot.SnapshotID, snapshot.DatasetSHA256); err != nil || renewed.ExpiresAtUS <= snapshot.ExpiresAtUS {
		t.Fatalf("renewed=%#v err=%v", renewed, err)
	}

	for index := 2; index <= control.MaxUserSnapshots; index++ {
		if _, err := operations.CreateSnapshot(ctx, snapshotCommand(t, fixture, tokenHash, index)); err != nil {
			t.Fatalf("snapshot %d: %v", index, err)
		}
	}
	if _, err := operations.CreateSnapshot(ctx, snapshotCommand(t, fixture, tokenHash, 99)); !errors.Is(err, control.ErrSnapshotLimit) {
		t.Fatalf("user cap=%v", err)
	}
	otherTokenHash := sha256.Sum256([]byte("same user different browser session"))
	otherCSRFHash := sha256.Sum256([]byte("same user different browser csrf"))
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO sessions(token_hash,user_id,csrf_hash,credential_revision,storage_generation,expires_at)
		VALUES($1,$2,$3,1,1,clock_timestamp()+interval '1 hour')`, otherTokenHash[:], snapshot.UserID, otherCSRFHash[:]); err != nil {
		t.Fatal(err)
	}
	secondSnapshot := snapshotCommand(t, fixture, tokenHash, 2)
	if _, err := operations.RenewSnapshot(ctx, otherTokenHash, fixture.tenantID, secondSnapshot.SnapshotID, secondSnapshot.DatasetSHA256); !errors.Is(err, control.ErrSnapshotMismatch) {
		t.Fatalf("different session reused snapshot: %v", err)
	}
	if err := operations.ReleaseSnapshot(ctx, tokenHash, fixture.tenantID, snapshot.SnapshotID); err != nil {
		t.Fatal(err)
	}
	if err := operations.ReleaseSnapshot(ctx, tokenHash, fixture.tenantID, snapshot.SnapshotID); err != nil {
		t.Fatalf("idempotent release: %v", err)
	}
	if _, err := operations.CreateSnapshot(ctx, snapshotCommand(t, fixture, tokenHash, 100)); err != nil {
		t.Fatalf("released slot not reclaimed: %v", err)
	}

	active := snapshotCommand(t, fixture, tokenHash, 101)
	if err := operations.ReleaseSnapshot(ctx, tokenHash, fixture.tenantID, snapshotUUID(fixture.tenantID, 2)); err != nil {
		t.Fatal(err)
	}
	created, err := operations.CreateSnapshot(ctx, active)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE users SET auth_revision=auth_revision+1 WHERE user_id=$1`, created.UserID); err != nil {
		t.Fatal(err)
	}
	if _, err := operations.RenewSnapshot(ctx, tokenHash, fixture.tenantID, created.SnapshotID, created.DatasetSHA256); !errors.Is(err, control.ErrForbidden) {
		t.Fatalf("revoked scope renewed: %v", err)
	}

	const futureFloor = int64(9_000_000_000_000_000)
	if _, err := fixture.pool.Exec(ctx, `UPDATE installations SET retention_floor_us=$1`, futureFloor); err != nil {
		t.Fatal(err)
	}
	floor, _, err := operations.AdvanceRetentionFloor(ctx)
	if err != nil || floor != futureFloor {
		t.Fatalf("retention floor regressed: floor=%d err=%v", floor, err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE installations SET retention_tick_at=clock_timestamp()-interval '121 seconds' WHERE singleton`); err != nil {
		t.Fatal(err)
	}
	if _, err := operations.CreateSnapshot(ctx, snapshotCommand(t, fixture, tokenHash, 102)); !errors.Is(err, control.ErrRetentionClockStale) {
		t.Fatalf("stale retention clock accepted: %v", err)
	}
}

func TestConcurrentSnapshotAdmissionIsSharedAcrossOperations(t *testing.T) {
	fixture := setupAcceptFixture(t, 962)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	first, tokenHash := setupQueryPrincipal(t, fixture)
	second, err := control.NewQueryOperations(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errorsSeen := make(chan error, 8)
	var ready sync.WaitGroup
	ready.Add(8)
	for ordinal := 1; ordinal <= 8; ordinal++ {
		operation := first
		if ordinal%2 == 0 {
			operation = second
		}
		command := snapshotCommand(t, fixture, tokenHash, ordinal)
		go func() {
			ready.Done()
			<-start
			_, err := operation.CreateSnapshot(ctx, command)
			errorsSeen <- err
		}()
	}
	ready.Wait()
	close(start)
	succeeded, limited := 0, 0
	for range 8 {
		err := <-errorsSeen
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, control.ErrSnapshotLimit):
			limited++
		default:
			t.Fatalf("unexpected concurrent admission error: %v", err)
		}
	}
	if succeeded != control.MaxUserSnapshots || limited != 8-control.MaxUserSnapshots {
		t.Fatalf("succeeded=%d limited=%d", succeeded, limited)
	}
	for userOrdinal := 1; userOrdinal <= 7; userOrdinal++ {
		userOperations, userToken := addQueryPrincipal(t, fixture, userOrdinal)
		for slot := 0; slot < control.MaxUserSnapshots; slot++ {
			ordinal := 100 + userOrdinal*10 + slot
			if _, err := userOperations.CreateSnapshot(ctx, snapshotCommand(t, fixture, userToken, ordinal)); err != nil {
				t.Fatalf("tenant fill user=%d slot=%d: %v", userOrdinal, slot, err)
			}
		}
	}
	overflowOperations, overflowToken := addQueryPrincipal(t, fixture, 8)
	if _, err := overflowOperations.CreateSnapshot(ctx, snapshotCommand(t, fixture, overflowToken, 999)); !errors.Is(err, control.ErrSnapshotLimit) {
		t.Fatalf("tenant snapshot cap=%v", err)
	}
}

func TestSnapshotRetriesReaderLaneRaceAndCatalogUsesCapturedGeneration(t *testing.T) {
	fixture := setupAcceptFixture(t, 961)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	operations, tokenHash := setupQueryPrincipal(t, fixture)
	insertCatalogFixture(t, fixture, 8)

	locker, err := fixture.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Rollback(ctx)
	if _, err := locker.Exec(ctx, `SELECT lane_id FROM lanes WHERE tenant_id=$1 AND lane_id=0 FOR UPDATE`, fixture.tenantID); err != nil {
		t.Fatal(err)
	}
	type answer struct {
		snapshot model.QuerySnapshot
		err      error
	}
	answers := make(chan answer, 1)
	started := make(chan struct{})
	createCommand := snapshotCommand(t, fixture, tokenHash, 1)
	go func() {
		close(started)
		result, err := operations.CreateSnapshot(ctx, createCommand)
		answers <- answer{result, err}
	}()
	<-started
	select {
	case answer := <-answers:
		t.Fatalf("snapshot crossed locked lane before catalog swap: %#v", answer)
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := locker.Exec(ctx, `UPDATE lanes SET catalog_generation=8 WHERE tenant_id=$1`, fixture.tenantID); err != nil {
		t.Fatal(err)
	}
	if err := locker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	got := <-answers
	if got.err != nil || got.snapshot.Lanes[0].CatalogGeneration != 8 {
		t.Fatalf("snapshot did not retry at swapped generation: %#v", got)
	}

	command := control.CatalogCommand{
		SessionTokenHash: tokenHash, TenantID: fixture.tenantID, SnapshotID: got.snapshot.SnapshotID,
		DatasetSHA256: got.snapshot.DatasetSHA256, DatasetBytes: got.snapshot.DatasetBytes,
		TimeBasis: model.QueryTimeEvent, StartUS: 100, EndUS: 200, Kinds: []model.Kind{model.KindError}, Limit: 256,
	}
	files, err := operations.CatalogPage(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].ObjectKey != "v1/query/analytics.parquet" || len(files[0].BlockSHA256) != 1 {
		t.Fatalf("catalog=%#v", files)
	}
	command.StartUS, command.EndUS = 300, 400
	files, err = operations.CatalogPage(ctx, command)
	if err != nil || len(files) != 0 {
		t.Fatalf("authorized empty catalog=%#v err=%v", files, err)
	}
}

func setupQueryPrincipal(t *testing.T, fixture *acceptFixture) (*control.QueryOperations, [32]byte) {
	t.Helper()
	ctx := context.Background()
	userID := fixture.tenantID*100 + 1
	tokenHash := sha256.Sum256([]byte(fmt.Sprintf("query-session-%d", fixture.tenantID)))
	csrfHash := sha256.Sum256([]byte(fmt.Sprintf("query-csrf-%d", fixture.tenantID)))
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO users(user_id,email_normalized,password_phc)
		VALUES($1,$2,'$argon2id$v=19$m=65536,t=3,p=2$ZXZlbnRnbGFzcy1kdW1teQ$3fS7jbTBnU4p8kMPSY1XANFKxf3f6OUt0OXbKsCB9BA')`, userID, fmt.Sprintf("query-%d@example.invalid", fixture.tenantID)); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'admin')`, fixture.tenantID, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO sessions(token_hash,user_id,csrf_hash,credential_revision,storage_generation,expires_at)
		VALUES($1,$2,$3,1,1,clock_timestamp()+interval '2 hours')`, tokenHash[:], userID, csrfHash[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE installations SET retention_floor_us=77,retention_tick_at=clock_timestamp()`); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE lanes SET accepted_seq=5,published_seq=5,catalog_generation=7 WHERE tenant_id=$1`, fixture.tenantID); err != nil {
		t.Fatal(err)
	}
	operations, err := control.NewQueryOperations(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	return operations, tokenHash
}

func addQueryPrincipal(t *testing.T, fixture *acceptFixture, ordinal int) (*control.QueryOperations, [32]byte) {
	t.Helper()
	ctx := context.Background()
	userID := fixture.tenantID*100 + 100 + int64(ordinal)
	tokenHash := sha256.Sum256([]byte(fmt.Sprintf("query-extra-session-%d-%d", fixture.tenantID, ordinal)))
	csrfHash := sha256.Sum256([]byte(fmt.Sprintf("query-extra-csrf-%d-%d", fixture.tenantID, ordinal)))
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO users(user_id,email_normalized,password_phc)
		VALUES($1,$2,'$argon2id$v=19$m=65536,t=3,p=2$ZXZlbnRnbGFzcy1kdW1teQ$3fS7jbTBnU4p8kMPSY1XANFKxf3f6OUt0OXbKsCB9BA')`, userID, fmt.Sprintf("query-extra-%d-%d@example.invalid", fixture.tenantID, ordinal)); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'admin')`, fixture.tenantID, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO sessions(token_hash,user_id,csrf_hash,credential_revision,storage_generation,expires_at)
		VALUES($1,$2,$3,1,1,clock_timestamp()+interval '2 hours')`, tokenHash[:], userID, csrfHash[:]); err != nil {
		t.Fatal(err)
	}
	operations, err := control.NewQueryOperations(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	return operations, tokenHash
}

func snapshotCommand(t *testing.T, fixture *acceptFixture, tokenHash [32]byte, ordinal int) control.CreateSnapshotCommand {
	t.Helper()
	filter, err := query.CanonicalFilter(&query.Node{Op: "constant", Constant: true})
	if err != nil {
		t.Fatal(err)
	}
	spec := model.DatasetSpec{TenantID: fixture.tenantID, ProjectIDs: []int64{fixture.projectID}, Kinds: []model.Kind{model.KindError}, TimeBasis: model.QueryTimeEvent, StartUS: 100, EndUS: 200, Filter: filter}
	digest, encoded, err := query.DatasetHash(spec)
	if err != nil {
		t.Fatal(err)
	}
	return control.CreateSnapshotCommand{SnapshotID: snapshotUUID(fixture.tenantID, ordinal), SessionTokenHash: tokenHash, TenantID: fixture.tenantID, ProjectIDs: spec.ProjectIDs, DatasetSHA256: digest, DatasetBytes: encoded}
}

func snapshotUUID(tenantID int64, ordinal int) string {
	return fmt.Sprintf("10000000-0000-4000-8000-%012x", tenantID*1000+int64(ordinal))
}

func insertCatalogFixture(t *testing.T, fixture *acceptFixture, generation int64) {
	t.Helper()
	ctx := context.Background()
	intentID := snapshotUUID(fixture.tenantID, 500)
	payloadIntentID := snapshotUUID(fixture.tenantID, 503)
	bundleID := snapshotUUID(fixture.tenantID, 501)
	fileID := snapshotUUID(fixture.tenantID, 502)
	payloadFileID := snapshotUUID(fixture.tenantID, 504)
	checksum := hex.EncodeToString(sha256.New().Sum(nil))
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO object_intents(intent_id,installation_id,tenant_id,storage_generation,object_key,kind,state,owner,fence,expires_at,expected_bytes,expected_sha256,uploaded_bytes,uploaded_sha256)
		VALUES($1,$2,$3,1,'v1/query/analytics.parquet','analytics','referenced','fixture',1,clock_timestamp()+interval '1 hour',4,$4,4,$4)`, intentID, acceptInstallationID, fixture.tenantID, checksum); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO object_intents(intent_id,installation_id,tenant_id,storage_generation,object_key,kind,state,owner,fence,expires_at,expected_bytes,expected_sha256,uploaded_bytes,uploaded_sha256)
		VALUES($1,$2,$3,1,'v1/query/payload.parquet','payload','referenced','fixture',1,clock_timestamp()+interval '1 hour',4,$4,4,$4)`, payloadIntentID, acceptInstallationID, fixture.tenantID, checksum); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO bundles(bundle_id,tenant_id,lane_id,schema_version,grouping_version,event_day,kind,input_seq_min,input_seq_max,row_count,identity_sha256,valid_from_generation)
		VALUES($1,$2,0,1,1,'2026-01-01','error',1,5,1,$3,$4)`, bundleID, fixture.tenantID, checksum, generation); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO files(file_id,tenant_id,bundle_id,intent_id,role,bytes,full_sha256,row_count,min_event_time_us,max_event_time_us,min_received_time_us,max_received_time_us,min_batch_seq,max_batch_seq)
		VALUES($1,$2,$3,$4,'analytics',4,$5,1,110,110,110,110,1,5)`, fileID, fixture.tenantID, bundleID, intentID, checksum); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO files(file_id,tenant_id,bundle_id,intent_id,role,bytes,full_sha256,row_count,min_event_time_us,max_event_time_us,min_received_time_us,max_received_time_us,min_batch_seq,max_batch_seq)
		VALUES($1,$2,$3,$4,'payload',4,$5,1,110,110,110,110,1,5)`, payloadFileID, fixture.tenantID, bundleID, payloadIntentID, checksum); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO file_blocks(file_id,block_index,sha256) VALUES($1,0,$2)`, fileID, checksum); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO file_blocks(file_id,block_index,sha256) VALUES($1,0,$2)`, payloadFileID, checksum); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO bundle_projects(tenant_id,bundle_id,project_id) VALUES($1,$2,$3)`, fixture.tenantID, bundleID, fixture.projectID); err != nil {
		t.Fatal(err)
	}
}
