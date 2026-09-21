package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/google/uuid"
)

func TestMaintenanceCandidatesSkipLaneWithActiveWork(t *testing.T) {
	for _, phase := range []string{"queued", "running", "prepared"} {
		t.Run(phase, func(t *testing.T) {
			fixture := setupAcceptFixture(t, 1761)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if _, err := fixture.pool.Exec(ctx, `UPDATE lanes SET accepted_seq=10,published_seq=10,catalog_generation=1 WHERE tenant_id=$1 AND lane_id IN (0,1)`, fixture.tenantID); err != nil {
				t.Fatal(err)
			}
			var occupied []string
			for index := range 10 {
				occupied = append(occupied, insertMaintenanceBundle(t, ctx, fixture, 1, int64(index+1), fmt.Sprintf("occupied-%d", index)))
			}
			for index := range 8 {
				id := insertMaintenanceBundle(t, ctx, fixture, 1, int64(index+1), fmt.Sprintf("available-%d", index))
				if _, err := fixture.pool.Exec(ctx, `UPDATE bundles SET lane_id=1 WHERE bundle_id=$1`, id); err != nil {
					t.Fatal(err)
				}
			}
			operations, err := control.NewMaintenanceOperations(fixture.pool)
			if err != nil {
				t.Fatal(err)
			}
			_, err = operations.ReserveCompaction(ctx, control.ReserveCompactionCommand{InstallationID: acceptInstallationID, StorageGeneration: 1, TaskID: uuid.NewString(), TenantID: fixture.tenantID, LaneID: 0, BundleIDs: occupied[:2]})
			if err != nil {
				t.Fatal(err)
			}
			if phase != "queued" {
				task, err := operations.ClaimCompaction(ctx, acceptInstallationID, "occupied-worker", time.Minute)
				if err != nil || task == nil {
					t.Fatalf("claim=%v err=%v", task, err)
				}
				if phase == "prepared" {
					prepareMaintenanceOutput(t, ctx, operations, task.Authority, fixture.projectID)
				}
			}
			candidate, err := operations.FindCompactionCandidate(ctx)
			if err != nil || candidate.LaneID != 1 || len(candidate.BundleIDs) != 8 {
				t.Errorf("active %s lane hid eligible work: candidate=%+v err=%v", phase, candidate, err)
			}
			if _, err := fixture.pool.Exec(ctx, `UPDATE installations SET retention_floor_us=1500000,retention_tick_at=clock_timestamp()`); err != nil {
				t.Fatal(err)
			}
			retention, err := operations.FindRetentionCandidate(ctx)
			if err != nil || retention.LaneID != 1 {
				t.Errorf("active %s lane hid retention: candidate=%+v err=%v", phase, retention, err)
			}
			retention, err = operations.FindRetentionCandidateForTenant(ctx, fixture.tenantID)
			if err != nil || retention.LaneID != 1 {
				t.Errorf("active %s lane hid tenant retention: candidate=%+v err=%v", phase, retention, err)
			}
			if t.Failed() {
				return
			}
			_, err = operations.ReserveCompaction(ctx, control.ReserveCompactionCommand{InstallationID: acceptInstallationID, StorageGeneration: 1, TaskID: uuid.NewString(), TenantID: fixture.tenantID, LaneID: candidate.LaneID, BundleIDs: candidate.BundleIDs})
			if err != nil {
				t.Fatalf("selected candidate was not reservable: %v", err)
			}
		})
	}
}
