package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/google/uuid"
)

// Pressure can appear after the scheduler reserved inputs. A worker must check
// it again, including prepared swaps and recovery of an expired lease. Denying
// admission must not consume a fence/attempt or release the input reservation.
func TestMaintenanceClaimRechecksForegroundPressure(t *testing.T) {
	for _, kind := range []string{"compact", "retain"} {
		for _, phase := range []string{"queued", "prepared", "expired"} {
			for _, pressure := range []string{"ingest", "query"} {
				t.Run(kind+"/"+phase+"/"+pressure, func(t *testing.T) {
					fixture := setupAcceptFixture(t, 1760)
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					operations, err := control.NewMaintenanceOperations(fixture.pool)
					if err != nil {
						t.Fatal(err)
					}
					taskID, claim := reservePressureMaintenance(t, ctx, fixture, operations, kind)
					var priorFence int64
					if phase != "queued" {
						first := claim("initial")
						if first == nil {
							t.Fatal("unpressured initial claim returned no work")
						}
						priorFence = first.Authority.Fence
						if phase == "prepared" {
							if kind == "compact" {
								prepareMaintenanceOutput(t, ctx, operations, first.Authority, fixture.projectID)
							} else {
								prepareRetentionOutput(t, ctx, operations, control.RetentionTask{CompactionTask: *first, RetentionFloor: 1_500_000}, fixture.projectID)
							}
						} else if _, err := fixture.pool.Exec(ctx, `UPDATE maintenance_tasks SET lease_until=clock_timestamp()-interval '1 second' WHERE task_id=$1`, taskID); err != nil {
							t.Fatal(err)
						}
					}
					clearPressure := insertMaintenancePressure(t, ctx, fixture, pressure)
					if task := claim("pressured"); task != nil {
						t.Fatalf("claimed reserved %s work while %s was overdue: %#v", phase, pressure, task)
					}
					var fence int64
					var attempt, reserved int
					if err := fixture.pool.QueryRow(ctx, `SELECT fence,attempt,(SELECT count(*) FROM bundles WHERE reserved_by=$1) FROM maintenance_tasks WHERE task_id=$1`, taskID).Scan(&fence, &attempt, &reserved); err != nil {
						t.Fatal(err)
					}
					if fence != priorFence || int64(attempt) != priorFence || reserved == 0 {
						t.Fatalf("denied claim changed authority/reservation: fence=%d attempt=%d reserved=%d", fence, attempt, reserved)
					}
					clearPressure()
					resumed := claim("resumed")
					if resumed == nil || resumed.Authority.TaskID != taskID || resumed.Authority.Fence != priorFence+1 || resumed.Prepared != (phase == "prepared") {
						t.Fatalf("pressure cleared but original work did not resume correctly: %#v", resumed)
					}
				})
			}
		}
	}
}

func reservePressureMaintenance(t *testing.T, ctx context.Context, fixture *acceptFixture, operations *control.MaintenanceOperations, kind string) (string, func(string) *control.CompactionTask) {
	t.Helper()
	if _, err := fixture.pool.Exec(ctx, `UPDATE lanes SET accepted_seq=2,published_seq=2,catalog_generation=1 WHERE tenant_id=$1 AND lane_id=0`, fixture.tenantID); err != nil {
		t.Fatal(err)
	}
	taskID := uuid.NewString()
	first := insertMaintenanceBundle(t, ctx, fixture, 1, 1, "pressure-first")
	if kind == "compact" {
		second := insertMaintenanceBundle(t, ctx, fixture, 1, 2, "pressure-second")
		_, err := operations.ReserveCompaction(ctx, control.ReserveCompactionCommand{InstallationID: acceptInstallationID, StorageGeneration: 1, TaskID: taskID, TenantID: fixture.tenantID, LaneID: 0, BundleIDs: []string{first, second}})
		if err != nil {
			t.Fatal(err)
		}
		return taskID, func(owner string) *control.CompactionTask {
			t.Helper()
			task, err := operations.ClaimCompaction(ctx, acceptInstallationID, owner, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			return task
		}
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE installations SET retention_floor_us=1500000,retention_tick_at=clock_timestamp()`); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE bundles SET row_count=2,input_seq_max=2 WHERE bundle_id=$1`, first); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE files SET row_count=2,max_received_time_us=2000000,max_batch_seq=2 WHERE bundle_id=$1`, first); err != nil {
		t.Fatal(err)
	}
	_, err := operations.ReserveRetention(ctx, control.ReserveRetentionCommand{InstallationID: acceptInstallationID, StorageGeneration: 1, TaskID: taskID, TenantID: fixture.tenantID, LaneID: 0, BundleID: first})
	if err != nil {
		t.Fatal(err)
	}
	return taskID, func(owner string) *control.CompactionTask {
		t.Helper()
		task, err := operations.ClaimRetention(ctx, acceptInstallationID, owner, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if task == nil {
			return nil
		}
		return &task.CompactionTask
	}
}

func insertMaintenancePressure(t *testing.T, ctx context.Context, fixture *acceptFixture, kind string) func() {
	t.Helper()
	if kind == "ingest" {
		request := fixture.request(fixture.uuidForLane(1), "pressure-ingest", "", "")
		batch := fixture.batch(t, 1, "pressure-ingest", []control.VerifiedRequest{request})
		if _, err := control.Accept(ctx, fixture.pool, batch); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.pool.Exec(ctx, `UPDATE jobs SET created_at=clock_timestamp()-interval '6 seconds' WHERE job_id=$1`, batch.JobID); err != nil {
			t.Fatal(err)
		}
		return func() {
			t.Helper()
			if _, err := fixture.pool.Exec(ctx, `UPDATE jobs SET state='failed' WHERE job_id=$1`, batch.JobID); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Add authority without resetting the existing lane generations or the
	// retention floor captured by the maintenance reservation.
	operations, token := addQueryPrincipal(t, fixture, 1)
	snapshot, err := operations.CreateSnapshot(ctx, snapshotCommand(t, fixture, token, 1760))
	if err != nil {
		t.Fatal(err)
	}
	operation := []byte(`{"kind":"search"}`)
	digest := sha256.Sum256(operation)
	queryID := uuid.NewString()
	_, err = operations.CreateQuery(ctx, control.CreateQueryCommand{QueryID: queryID, SessionTokenHash: token, TenantID: fixture.tenantID, SnapshotID: snapshot.SnapshotID, OperationKind: "search", OperationHash: hex.EncodeToString(digest[:]), OperationBytes: operation, Owner: "pressure-coordinator", Timeout: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE query_jobs SET created_at=clock_timestamp()-interval '1 second' WHERE query_id=$1`, queryID); err != nil {
		t.Fatal(err)
	}
	return func() {
		t.Helper()
		if err := operations.CancelQuery(ctx, token, fixture.tenantID, queryID); err != nil {
			t.Fatal(fmt.Errorf("clear query pressure: %w", err))
		}
	}
}
