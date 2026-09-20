package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/google/uuid"
)

func TestRetentionMixedRewriteAndPinnedSnapshot(t *testing.T) {
	fixture := setupAcceptFixture(t, 1710)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	queryOps, token := setupQueryPrincipal(t, fixture)
	if _, err := fixture.pool.Exec(ctx, `UPDATE installations SET retention_floor_us=1500000,retention_tick_at=clock_timestamp()`); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE lanes SET catalog_generation=10 WHERE tenant_id=$1 AND lane_id=0`, fixture.tenantID); err != nil {
		t.Fatal(err)
	}
	bundleID := insertMaintenanceBundle(t, ctx, fixture, 10, 1, "mixed-retention")
	if _, err := fixture.pool.Exec(ctx, `UPDATE bundles SET row_count=2,input_seq_max=2 WHERE bundle_id=$1`, bundleID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE files SET row_count=2,min_received_time_us=1000000,max_received_time_us=2000000,max_batch_seq=2 WHERE bundle_id=$1`, bundleID); err != nil {
		t.Fatal(err)
	}
	snapshotInput := snapshotCommand(t, fixture, token, 1710)
	snapshot, err := queryOps.CreateSnapshot(ctx, snapshotInput)
	if err != nil {
		t.Fatal(err)
	}
	operations, _ := control.NewMaintenanceOperations(fixture.pool)
	attestTestGC(t, ctx, fixture)
	candidate, err := operations.FindRetentionCandidateForTenant(ctx, fixture.tenantID)
	if err != nil || candidate.BundleID != bundleID || candidate.FullyExpired {
		t.Fatalf("candidate=%#v err=%v", candidate, err)
	}
	task, err := operations.ReserveRetention(ctx, control.ReserveRetentionCommand{InstallationID: acceptInstallationID, StorageGeneration: 1, TaskID: uuid.NewString(), TenantID: fixture.tenantID, LaneID: 0, BundleID: bundleID})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := operations.ClaimRetention(ctx, acceptInstallationID, "retention-worker", time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("claim=%#v err=%v", claimed, err)
	}
	manifest := prepareRetentionOutput(t, ctx, operations, *claimed, fixture.projectID)
	if _, err := fixture.pool.Exec(ctx, `UPDATE object_intents SET expires_at=clock_timestamp()-interval '9 days',retired_at=clock_timestamp()-interval '9 days' WHERE maintenance_task_id=$1`, claimed.Authority.TaskID); err != nil {
		t.Fatal(err)
	}
	if objects, err := operations.ClaimGCObjectsForTenant(ctx, fixture.tenantID, 10); err != nil || len(objects) != 0 {
		t.Fatalf("prepared outputs were collectible: %#v err=%v", objects, err)
	}
	prepared, err := operations.ClaimRetention(ctx, acceptInstallationID, "retention-swap", time.Minute)
	if err != nil || prepared == nil || !prepared.Prepared {
		t.Fatalf("prepared=%#v err=%v", prepared, err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE maintenance_tasks SET lease_until=clock_timestamp()-interval '1 second' WHERE task_id=$1`, prepared.Authority.TaskID); err != nil {
		t.Fatal(err)
	}
	if _, err := operations.SwapRetention(ctx, *prepared); !errors.Is(err, control.ErrMaintenanceFence) {
		t.Fatalf("expired swap: %v", err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE maintenance_tasks SET lease_until=clock_timestamp()+interval '1 minute' WHERE task_id=$1`, prepared.Authority.TaskID); err != nil {
		t.Fatal(err)
	}
	result, err := operations.SwapRetention(ctx, *prepared)
	if err != nil || result.CatalogGeneration != 11 {
		t.Fatalf("swap=%#v err=%v", result, err)
	}
	files, err := queryOps.CatalogPage(ctx, control.CatalogCommand{SessionTokenHash: token, TenantID: fixture.tenantID, SnapshotID: snapshot.SnapshotID, DatasetSHA256: snapshotInput.DatasetSHA256, DatasetBytes: snapshotInput.DatasetBytes, TimeBasis: model.QueryTimeEvent, StartUS: 0, EndUS: 3_000_000, Kinds: []model.Kind{model.KindLog}, Limit: 10})
	if err != nil || len(files) != 1 || files[0].BundleID != bundleID {
		t.Fatalf("pinned files=%#v err=%v", files, err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE bundles SET retired_at=clock_timestamp()-interval '9 days' WHERE bundle_id=$1`, bundleID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE object_intents SET retired_at=clock_timestamp()-interval '9 days',protect_until=NULL WHERE intent_id IN (SELECT intent_id FROM files WHERE bundle_id=$1)`, bundleID); err != nil {
		t.Fatal(err)
	}
	if objects, err := operations.ClaimGCObjectsForTenant(ctx, fixture.tenantID, 10); err != nil || len(objects) != 0 {
		t.Fatalf("pinned/current objects were collectible: %#v err=%v", objects, err)
	}
	var current string
	if err := fixture.pool.QueryRow(ctx, `SELECT bundle_id::text FROM bundles WHERE tenant_id=$1 AND lane_id=0 AND valid_to_generation IS NULL`, fixture.tenantID).Scan(&current); err != nil {
		t.Fatal(err)
	}
	if current != manifest.BundleID {
		t.Fatalf("current=%s want=%s", current, manifest.BundleID)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE installations SET retention_floor_us=3000000`); err != nil {
		t.Fatal(err)
	}
	full, err := operations.FindRetentionCandidateForTenant(ctx, fixture.tenantID)
	if err != nil || !full.FullyExpired || full.BundleID != manifest.BundleID {
		t.Fatalf("full=%#v err=%v", full, err)
	}
	fullTask, err := operations.ReserveRetention(ctx, control.ReserveRetentionCommand{InstallationID: acceptInstallationID, StorageGeneration: 1, TaskID: uuid.NewString(), TenantID: fixture.tenantID, LaneID: 0, BundleID: manifest.BundleID})
	if err != nil {
		t.Fatal(err)
	}
	_ = fullTask
	fullClaim, err := operations.ClaimRetention(ctx, acceptInstallationID, "full-retention", time.Minute)
	if err != nil || fullClaim == nil {
		t.Fatalf("full claim=%#v err=%v", fullClaim, err)
	}
	if _, err := operations.SwapRetention(ctx, *fullClaim); err != nil {
		t.Fatal(err)
	}
	var currentCount int
	if err := fixture.pool.QueryRow(ctx, `SELECT count(*) FROM bundles WHERE tenant_id=$1 AND lane_id=0 AND valid_to_generation IS NULL`, fixture.tenantID).Scan(&currentCount); err != nil || currentCount != 0 {
		t.Fatalf("current=%d err=%v", currentCount, err)
	}
	_ = task
}

func prepareRetentionOutput(t *testing.T, ctx context.Context, operations *control.MaintenanceOperations, task control.RetentionTask, projectID int64) model.BundleManifest {
	t.Helper()
	bundle := model.BundleManifest{BundleID: uuid.NewString(), EventDay: "2026-09-20", Kind: model.KindLog, InputSeqMin: 2, InputSeqMax: 2, RowCount: 1, IdentitySHA256: fixtureSHA("retained"), ProjectIDs: []int64{projectID}}
	for index, role := range []string{"analytics", "payload"} {
		checksum := fixtureSHA("retained:" + role)
		intent := control.IntentAuthority{IntentID: uuid.NewString(), Owner: task.Authority.Owner, Fence: task.Authority.Fence, Bytes: 100, SHA256: checksum}
		if err := operations.RegisterMaintenanceIntent(ctx, control.MaintenanceIntentRegistration{Authority: task.Authority, Role: role, ObjectKey: "v1/integration/retained/" + intent.IntentID, Intent: intent}); err != nil {
			t.Fatal(err)
		}
		if err := operations.MarkMaintenanceIntentUploaded(ctx, task.Authority, intent); err != nil {
			t.Fatal(err)
		}
		file := model.FileManifest{FileID: uuid.NewString(), IntentID: intent.IntentID, Role: role, Bytes: 100, SHA256: checksum, RowCount: 1, MinEventTimeUS: 1_000_000, MaxEventTimeUS: 1_000_000, MinReceivedTimeUS: 2_000_000, MaxReceivedTimeUS: 2_000_000, MinBatchSeq: 2, MaxBatchSeq: 2, Blocks: []model.FileBlockManifest{{Index: 0, SHA256: fixtureSHA("retained:block:" + role)}}}
		if index == 0 {
			bundle.Analytics = file
		} else {
			bundle.Payload = file
		}
	}
	if err := operations.PrepareRetention(ctx, task, bundle); err != nil {
		t.Fatal(err)
	}
	return bundle
}

func TestGCProtectionJournalGraceAndLatePutResweep(t *testing.T) {
	fixture := setupAcceptFixture(t, 1711)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	store := integrationStore(t, "retention-gc-1711")
	operations, _ := control.NewMaintenanceOperations(fixture.pool)
	if _, err := operations.ClaimGCObjectsForTenant(ctx, fixture.tenantID, 10); !errors.Is(err, control.ErrBackupInterlock) {
		t.Fatalf("unknown backup health: %v", err)
	}
	attestTestGC(t, ctx, fixture)
	data := []byte("late-put-tombstone")
	info, err := store.Put(ctx, "orphan.bin", data)
	if err != nil {
		t.Fatal(err)
	}
	intentID := uuid.NewString()
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO object_intents(intent_id,installation_id,tenant_id,storage_generation,object_key,kind,state,owner,fence,expires_at,expected_bytes,expected_sha256,uploaded_bytes,uploaded_sha256) VALUES($1,$2,$3,1,'orphan.bin','temporary','uploaded','gc-fixture',1,clock_timestamp()-interval '9 days',$4,$5,$4,$5)`, intentID, acceptInstallationID, fixture.tenantID, info.Size, info.SHA256); err != nil {
		t.Fatal(err)
	}
	backupID := uuid.NewString()
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO backup_sets(backup_id,external_tool_id,installation_id,storage_generation,base_start,base_end,earliest_recoverable_time,latest_recoverable_time,state,protected_until,verified_at) VALUES($1,$2,$3,1,clock_timestamp()-interval '1 day',clock_timestamp(),clock_timestamp()-interval '8 days',clock_timestamp(),'verified',clock_timestamp()+interval '1 day',clock_timestamp())`, backupID, "gc-protect-1711", acceptInstallationID); err != nil {
		t.Fatal(err)
	}
	objects, err := operations.ClaimGCObjectsForTenant(ctx, fixture.tenantID, 10)
	if err != nil || len(objects) != 0 {
		t.Fatalf("protected objects=%#v err=%v", objects, err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE backup_sets SET protected_until=clock_timestamp()-interval '1 second',state='expired' WHERE backup_id=$1`, backupID); err != nil {
		t.Fatal(err)
	}
	objects, err = operations.ClaimGCObjectsForTenant(ctx, fixture.tenantID, 10)
	if err != nil || len(objects) != 1 {
		t.Fatalf("gc objects=%#v err=%v", objects, err)
	}
	if err := store.Delete(ctx, []string{objects[0].ObjectKey}); err != nil {
		t.Fatal(err)
	}
	if err := operations.ConfirmGCObjects(ctx, objects); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Head(ctx, "orphan.bin"); err == nil {
		t.Fatal("orphan survived GC")
	}
	if _, err := store.Put(ctx, "orphan.bin", data); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE object_intents SET gc_confirmed_at=clock_timestamp()-interval '2 hours' WHERE intent_id=$1`, intentID); err != nil {
		t.Fatal(err)
	}
	objects, err = operations.ClaimGCObjectsForTenant(ctx, fixture.tenantID, 10)
	if err != nil || len(objects) != 1 || !objects[0].Resweep {
		t.Fatalf("resweep objects=%#v err=%v", objects, err)
	}
	if err := store.Delete(ctx, []string{objects[0].ObjectKey}); err != nil {
		t.Fatal(err)
	}
	stale := append([]control.GCObject(nil), objects...)
	stale[0].Attempt--
	if err := operations.ConfirmGCObjects(ctx, stale); !errors.Is(err, control.ErrMaintenanceFence) {
		t.Fatalf("stale GC confirmation: %v", err)
	}
	if err := operations.ConfirmGCObjects(ctx, objects); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Head(ctx, "orphan.bin"); err == nil {
		t.Fatal("late PUT survived tombstone resweep")
	}

	requestID := fixture.uuidForLane(0)
	batch := fixture.batch(t, 0, "retire-journal", []control.VerifiedRequest{fixture.request(requestID, "retire-journal", "", "")})
	if _, err := control.Accept(ctx, fixture.pool, batch); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE receipts SET received_time_us=floor(extract(epoch FROM (clock_timestamp()-interval '40 days'))*1000000)::bigint WHERE tenant_id=$1 AND lane_id=0 AND batch_seq=1`, fixture.tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE jobs SET state='completed' WHERE tenant_id=$1 AND lane_id=0 AND batch_seq=1`, fixture.tenantID); err != nil {
		t.Fatal(err)
	}
	// A real converter leaves a producer FK even after its output is published.
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO object_intents(intent_id,installation_id,tenant_id,storage_generation,object_key,kind,state,owner,fence,expires_at,conversion_job_id,producer_generation,producer_fence,expected_bytes,expected_sha256)
		VALUES($1,$2,$3,1,$4,'analytics','pending','completed-producer',1,clock_timestamp(),$5,1,1,1,repeat('a',64))`, uuid.NewString(), acceptInstallationID, fixture.tenantID, "v1/integration/producer/"+uuid.NewString(), batch.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE ingest_batches SET state='published' WHERE tenant_id=$1 AND lane_id=0 AND batch_seq=1`, fixture.tenantID); err != nil {
		t.Fatal(err)
	}
	secondBackup := uuid.NewString()
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO backup_sets(backup_id,external_tool_id,installation_id,storage_generation,base_start,base_end,earliest_recoverable_time,latest_recoverable_time,state,protected_until,verified_at) VALUES($1,$2,$3,1,clock_timestamp()-interval '1 day',clock_timestamp(),clock_timestamp()-interval '8 days',clock_timestamp(),'verified',clock_timestamp()+interval '1 day',clock_timestamp())`, secondBackup, "journal-protect-1711", acceptInstallationID); err != nil {
		t.Fatal(err)
	}
	if progressed, err := operations.RunRetentionCleanupTenant(ctx, fixture.tenantID); err != nil || progressed {
		t.Fatalf("backup-protected cleanup=%v err=%v", progressed, err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE backup_sets SET protected_until=clock_timestamp()-interval '1 second',state='expired' WHERE backup_id=$1`, secondBackup); err != nil {
		t.Fatal(err)
	}
	progressed, err := operations.RunRetentionCleanupTenant(ctx, fixture.tenantID)
	if err != nil || !progressed {
		t.Fatalf("cleanup=%v err=%v", progressed, err)
	}
	var state string
	var journal *string
	var protected bool
	if err := fixture.pool.QueryRow(ctx, `SELECT ib.recovery_state,ib.journal_intent_id::text,oi.protect_until>=clock_timestamp()+interval '7 days 23 hours' FROM ingest_batches ib JOIN ingest_batch_retirement_summaries s USING(tenant_id,lane_id,batch_seq) JOIN object_intents oi ON oi.tenant_id=ib.tenant_id AND oi.intent_id=$2 WHERE ib.tenant_id=$1 AND ib.lane_id=0 AND ib.batch_seq=1`, fixture.tenantID, batch.Journal.IntentID).Scan(&state, &journal, &protected); err != nil {
		t.Fatal(err)
	}
	if state != "retired" || journal != nil || !protected {
		t.Fatalf("state=%s journal=%v protected=%v", state, journal, protected)
	}
}

// Test-only attestation. Production remains frozen until M4 verifies a
// coordinated PG/WAL/object horizon; absence of backups is not verification.
func attestTestGC(t *testing.T, ctx context.Context, fixture *acceptFixture) {
	t.Helper()
	if _, err := fixture.pool.Exec(ctx, `UPDATE installations SET gc_safe_before=clock_timestamp()-interval '8 days',gc_verified_until=clock_timestamp()+interval '1 hour' WHERE singleton`); err != nil {
		t.Fatal(err)
	}
}
