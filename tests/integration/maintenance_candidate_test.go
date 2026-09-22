package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/google/uuid"
)

func TestCompactionCandidatePrioritizesBoundedFileReduction(t *testing.T) {
	for _, test := range []struct {
		counts     [2]int
		largeFirst bool
		sameLane   bool
	}{{[2]int{8, 64}, false, false}, {[2]int{8, 129}, false, false}, {[2]int{7, 8}, false, false}, {[2]int{64, 16}, true, false}, {[2]int{8, 64}, false, true}} {
		counts := test.counts
		t.Run(fmt.Sprintf("%d-%d-same-lane-%t", counts[0], counts[1], test.sameLane), func(t *testing.T) {
			fixture := setupAcceptFixture(t, 1762)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			for lane, count := range counts {
				physicalLane := lane
				kind := "log"
				if test.sameLane {
					physicalLane = 0
					if lane == 0 {
						kind = "error"
					}
				}
				if _, err := fixture.pool.Exec(ctx, `UPDATE lanes SET accepted_seq=GREATEST(accepted_seq,$3),published_seq=GREATEST(published_seq,$3),catalog_generation=1 WHERE tenant_id=$1 AND lane_id=$2`, fixture.tenantID, physicalLane, count); err != nil {
					t.Fatal(err)
				}
				for index := range count {
					id := insertMaintenanceBundle(t, ctx, fixture, 1, int64(index+1), fmt.Sprintf("lane-%d-%d", lane, index))
					if _, err := fixture.pool.Exec(ctx, `UPDATE bundles SET lane_id=$2,kind=$3 WHERE bundle_id=$1`, id, physicalLane, kind); err != nil {
						t.Fatal(err)
					}
					if test.largeFirst && lane == 0 {
						if _, err := fixture.pool.Exec(ctx, `UPDATE files SET bytes=2097152 WHERE bundle_id=$1`, id); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			operations, err := control.NewMaintenanceOperations(fixture.pool)
			if err != nil {
				t.Fatal(err)
			}
			candidate, err := operations.FindCompactionCandidate(ctx)
			expectedLane := 1
			if test.sameLane {
				expectedLane = 0
			}
			if err != nil || candidate.LaneID != expectedLane || candidate.Partition.Kind != "log" || len(candidate.BundleIDs) != min(counts[1], control.MaxCompactionInputs) {
				t.Fatalf("lexical lane/kind displaced larger bounded GET reduction: lane=%d kind=%s inputs=%d err=%v", candidate.LaneID, candidate.Partition.Kind, len(candidate.BundleIDs), err)
			}
			_, err = operations.ReserveCompaction(ctx, control.ReserveCompactionCommand{InstallationID: acceptInstallationID, StorageGeneration: 1,
				TaskID: uuid.NewString(), TenantID: fixture.tenantID, LaneID: candidate.LaneID, BundleIDs: candidate.BundleIDs})
			if err != nil {
				t.Fatalf("selected candidate is not reservable: %v", err)
			}
			// The active lane cannot be selected again, even if unreserved
			// inputs remain above the minimum after a bounded reservation.
			next, err := operations.FindCompactionCandidate(ctx)
			expected := counts[0]
			if test.largeFirst {
				expected = 8
			}
			if counts[0] < 8 || test.sameLane {
				if !errors.Is(err, control.ErrMaintenanceNoWork) {
					t.Fatalf("small quiet partition: %+v %v", next, err)
				}
			} else if err != nil || next.LaneID != 0 || len(next.BundleIDs) != expected {
				t.Fatalf("active lane hid other eligible partition: %+v %v", next, err)
			}
			canceled, stop := context.WithCancel(ctx)
			stop()
			if _, err := operations.FindCompactionCandidate(canceled); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel=%v", err)
			}
		})
	}
}

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
