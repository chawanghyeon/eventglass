package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/google/uuid"
)

func TestCompactionReservationRecoveryAndGenerationSwap(t *testing.T) {
	fixture := setupAcceptFixture(t, 1701)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	queryOperations, tokenHash := setupQueryPrincipal(t, fixture)
	if _, err := fixture.pool.Exec(ctx, `UPDATE lanes SET accepted_seq=9,published_seq=9,catalog_generation=10 WHERE tenant_id=$1 AND lane_id=0`, fixture.tenantID); err != nil {
		t.Fatal(err)
	}
	bundles := make([]string, 8)
	for index := range bundles {
		bundles[index] = insertMaintenanceBundle(t, ctx, fixture, 10, int64(index+1), fmt.Sprintf("seed-%d", index))
	}
	snapshotInput := snapshotCommand(t, fixture, tokenHash, 1701)
	snapshot, err := queryOperations.CreateSnapshot(ctx, snapshotInput)
	if err != nil || snapshot.Lanes[0].CatalogGeneration != 10 {
		t.Fatalf("snapshot=%#v err=%v", snapshot, err)
	}
	operations, err := control.NewMaintenanceOperations(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := operations.FindCompactionCandidate(ctx)
	if err != nil || candidate.TenantID != fixture.tenantID || candidate.LaneID != 0 || len(candidate.BundleIDs) != 8 {
		t.Fatalf("candidate=%#v err=%v", candidate, err)
	}
	taskID := uuid.NewString()
	reserved, err := operations.ReserveCompaction(ctx, control.ReserveCompactionCommand{
		InstallationID: acceptInstallationID, StorageGeneration: 1, TaskID: taskID,
		TenantID: fixture.tenantID, LaneID: 0, BundleIDs: bundles[:2],
	})
	if err != nil {
		t.Fatal(err)
	}
	if reserved.Partition.EventDay != "2026-09-20" || reserved.Partition.Kind != model.KindLog {
		t.Fatalf("partition=%#v", reserved.Partition)
	}

	claimed, err := operations.ClaimCompaction(ctx, acceptInstallationID, "first-worker", time.Minute)
	if err != nil || claimed == nil || claimed.Prepared {
		t.Fatalf("first claim=%#v err=%v", claimed, err)
	}
	work, err := operations.LoadCompaction(ctx, claimed.Authority)
	if err != nil || len(work.Inputs) != 2 || !sameMaintenanceBundleSet(work.Inputs, bundles[:2]) {
		t.Fatalf("work=%#v err=%v", work, err)
	}
	// A worker crash before Prepare expires only its lease. The original catalog
	// remains current and a newly fenced worker can safely redo the rewrite.
	if _, err := fixture.pool.Exec(ctx, `UPDATE maintenance_tasks SET lease_until=clock_timestamp()-interval '1 second' WHERE task_id=$1`, taskID); err != nil {
		t.Fatal(err)
	}
	claimed, err = operations.ClaimCompaction(ctx, acceptInstallationID, "replacement-worker", time.Minute)
	if err != nil || claimed == nil || claimed.Authority.Fence != 2 {
		t.Fatalf("replacement claim=%#v err=%v", claimed, err)
	}
	assertCurrentMaintenanceBundles(t, ctx, fixture, bundles[:2], true)

	manifest := prepareMaintenanceOutput(t, ctx, operations, claimed.Authority, fixture.projectID)
	// Publication may advance the lane generation while the rewrite is prepared.
	// Swap must CAS only its exact reservation, not the whole old generation.
	unrelated := insertMaintenanceBundle(t, ctx, fixture, 11, 9, "concurrent-publication")
	if _, err := fixture.pool.Exec(ctx, `UPDATE lanes SET catalog_generation=11 WHERE tenant_id=$1 AND lane_id=0`, fixture.tenantID); err != nil {
		t.Fatal(err)
	}
	prepared, err := operations.ClaimCompaction(ctx, acceptInstallationID, "swap-worker", time.Minute)
	if err != nil || prepared == nil || !prepared.Prepared || prepared.Authority.Fence != 3 {
		t.Fatalf("prepared claim=%#v err=%v", prepared, err)
	}
	result, err := operations.SwapCompaction(ctx, prepared.Authority)
	if err != nil || result.CatalogGeneration != 12 || result.AlreadyCompleted {
		t.Fatalf("swap=%#v err=%v", result, err)
	}
	assertCurrentMaintenanceBundles(t, ctx, fixture, bundles[:2], false)
	assertCurrentMaintenanceBundles(t, ctx, fixture, append(bundles[2:], unrelated, manifest.BundleID), true)

	var oldCount, currentCount, publishedSeq int64
	if err := fixture.pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM bundles WHERE tenant_id=$1 AND lane_id=0 AND valid_from_generation<=10 AND (valid_to_generation IS NULL OR valid_to_generation>10)),
		(SELECT count(*) FROM bundles WHERE tenant_id=$1 AND lane_id=0 AND valid_from_generation<=12 AND (valid_to_generation IS NULL OR valid_to_generation>12)),
		(SELECT published_seq FROM lanes WHERE tenant_id=$1 AND lane_id=0)`, fixture.tenantID).Scan(&oldCount, &currentCount, &publishedSeq); err != nil {
		t.Fatal(err)
	}
	if oldCount != 8 || currentCount != 8 || publishedSeq != 9 {
		t.Fatalf("old reader=%d current=%d published_seq=%d", oldCount, currentCount, publishedSeq)
	}
	files, err := queryOperations.CatalogPage(ctx, control.CatalogCommand{
		SessionTokenHash: tokenHash, TenantID: fixture.tenantID, SnapshotID: snapshot.SnapshotID,
		DatasetSHA256: snapshotInput.DatasetSHA256, DatasetBytes: snapshotInput.DatasetBytes,
		TimeBasis: model.QueryTimeEvent, StartUS: 0, EndUS: 2_000_000, Kinds: []model.Kind{model.KindLog}, Limit: 32,
	})
	if err != nil || !sameCatalogBundleSet(files, bundles) {
		t.Fatalf("pinned catalog=%#v err=%v", files, err)
	}
	// Retrying after a crash immediately after commit is idempotent.
	replayed, err := operations.SwapCompaction(ctx, prepared.Authority)
	if err != nil || !replayed.AlreadyCompleted || replayed.CatalogGeneration != 12 {
		t.Fatalf("replayed swap=%#v err=%v", replayed, err)
	}
}

func TestCompactionCandidateFillsTinyPartitionInsteadOfStoppingAtMinimum(t *testing.T) {
	fixture := setupAcceptFixture(t, 1702)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := fixture.pool.Exec(ctx, `UPDATE lanes SET accepted_seq=12,published_seq=12,catalog_generation=1 WHERE tenant_id=$1 AND lane_id=0`, fixture.tenantID); err != nil {
		t.Fatal(err)
	}
	for index := range 12 {
		insertMaintenanceBundle(t, ctx, fixture, 1, int64(index+1), fmt.Sprintf("tiny-%d", index))
	}
	operations, err := control.NewMaintenanceOperations(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := operations.FindCompactionCandidate(ctx)
	if err != nil || candidate.TenantID != fixture.tenantID || candidate.LaneID != 0 || len(candidate.BundleIDs) != 12 {
		t.Fatalf("candidate=%#v err=%v", candidate, err)
	}
}

func sameMaintenanceBundleSet(inputs []control.CompactionWorkInput, expected []string) bool {
	seen := make(map[string]bool, len(inputs))
	for _, input := range inputs {
		seen[input.BundleID] = true
	}
	for _, bundleID := range expected {
		if !seen[bundleID] {
			return false
		}
	}
	return len(seen) == len(expected)
}

func sameCatalogBundleSet(files []model.CatalogFile, expected []string) bool {
	seen := make(map[string]bool, len(files))
	for _, file := range files {
		seen[file.BundleID] = true
	}
	for _, bundleID := range expected {
		if !seen[bundleID] {
			return false
		}
	}
	return len(seen) == len(expected)
}

func insertMaintenanceBundle(t *testing.T, ctx context.Context, fixture *acceptFixture, generation, sequence int64, label string) string {
	t.Helper()
	bundleID := uuid.NewString()
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO bundles(bundle_id,tenant_id,lane_id,schema_version,grouping_version,event_day,kind,input_seq_min,input_seq_max,row_count,identity_sha256,valid_from_generation)
		VALUES($1,$2,0,1,1,'2026-09-20','log',$3,$3,1,$4,$5)`, bundleID, fixture.tenantID, sequence, fixtureSHA(label+":identity"), generation); err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"analytics", "payload"} {
		intentID := uuid.NewString()
		checksum := fixtureSHA(label + ":" + role)
		if _, err := fixture.pool.Exec(ctx, `INSERT INTO object_intents(intent_id,installation_id,tenant_id,storage_generation,object_key,kind,state,owner,fence,expires_at,expected_bytes,expected_sha256,uploaded_bytes,uploaded_sha256)
			VALUES($1,$2,$3,1,$4,$5,'referenced','maintenance-fixture',1,clock_timestamp()+interval '1 day',100,$6,100,$6)`,
			intentID, acceptInstallationID, fixture.tenantID, "v1/integration/maintenance/"+intentID, role, checksum); err != nil {
			t.Fatal(err)
		}
		fileID := uuid.NewString()
		if _, err := fixture.pool.Exec(ctx, `INSERT INTO files(file_id,tenant_id,bundle_id,intent_id,role,bytes,full_sha256,row_count,min_event_time_us,max_event_time_us,min_received_time_us,max_received_time_us,min_batch_seq,max_batch_seq)
			VALUES($1,$2,$3,$4,$5,100,$6,1,1000000,1000000,1000000,1000000,$7,$7)`,
			fileID, fixture.tenantID, bundleID, intentID, role, checksum, sequence); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.pool.Exec(ctx, `INSERT INTO file_blocks(file_id,block_index,sha256) VALUES($1,0,$2)`, fileID, fixtureSHA(label+":block:"+role)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO bundle_projects(tenant_id,bundle_id,project_id) VALUES($1,$2,$3)`, fixture.tenantID, bundleID, fixture.projectID); err != nil {
		t.Fatal(err)
	}
	return bundleID
}

func prepareMaintenanceOutput(t *testing.T, ctx context.Context, operations *control.MaintenanceOperations, authority control.MaintenanceAuthority, projectID int64) model.BundleManifest {
	t.Helper()
	bundle := model.BundleManifest{
		BundleID: uuid.NewString(), EventDay: "2026-09-20", Kind: model.KindLog,
		InputSeqMin: 1, InputSeqMax: 2, RowCount: 2, IdentitySHA256: fixtureSHA("compacted-identity"), ProjectIDs: []int64{projectID},
	}
	files := []*model.FileManifest{&bundle.Analytics, &bundle.Payload}
	for index, role := range []string{"analytics", "payload"} {
		checksum := fixtureSHA("compacted:" + role)
		intent := control.IntentAuthority{IntentID: uuid.NewString(), Owner: authority.Owner, Fence: authority.Fence, Bytes: 200, SHA256: checksum}
		if err := operations.RegisterMaintenanceIntent(ctx, control.MaintenanceIntentRegistration{
			Authority: authority, Role: role, ObjectKey: fmt.Sprintf("v1/integration/maintenance/%s/%s", authority.TaskID, role), Intent: intent,
		}); err != nil {
			t.Fatal(err)
		}
		if err := operations.MarkMaintenanceIntentUploaded(ctx, authority, intent); err != nil {
			t.Fatal(err)
		}
		*files[index] = model.FileManifest{
			FileID: uuid.NewString(), IntentID: intent.IntentID, Role: role, Bytes: 200, SHA256: checksum, RowCount: 2,
			MinEventTimeUS: 1_000_000, MaxEventTimeUS: 1_000_000, MinReceivedTimeUS: 1_000_000, MaxReceivedTimeUS: 1_000_000,
			MinBatchSeq: 1, MaxBatchSeq: 2, Blocks: []model.FileBlockManifest{{Index: 0, SHA256: fixtureSHA("compacted:block:" + role)}},
		}
	}
	if err := operations.PrepareCompaction(ctx, authority, bundle); err != nil {
		t.Fatal(err)
	}
	return bundle
}

func assertCurrentMaintenanceBundles(t *testing.T, ctx context.Context, fixture *acceptFixture, bundleIDs []string, current bool) {
	t.Helper()
	for _, bundleID := range bundleIDs {
		var actual bool
		if err := fixture.pool.QueryRow(ctx, `SELECT valid_to_generation IS NULL FROM bundles WHERE tenant_id=$1 AND bundle_id=$2`, fixture.tenantID, bundleID).Scan(&actual); err != nil {
			t.Fatal(err)
		}
		if actual != current {
			t.Fatalf("bundle %s current=%v want=%v", bundleID, actual, current)
		}
	}
}
