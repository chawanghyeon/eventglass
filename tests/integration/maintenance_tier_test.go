package integration

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/google/uuid"
)

// Real PostgreSQL selection/reservation evidence, not native compression or S3
// bytes. One old large pair must not be rewritten on every tiny-input arrival.
func TestCompactionCandidateKeepsDifferentSizeCohortsSeparate(t *testing.T) {
	for _, test := range []struct {
		name                   string
		smallCount, largeCount int
		wantSmall              bool
		wantCount              int
	}{
		{"large-quiet-small-ready", 8, 1, true, 8},
		{"equal-count-less-rewrite", 8, 8, true, 8},
		{"large-cohort-progresses", 7, 8, false, 8},
		{"quiet-cohorts-do-not-churn", 7, 7, false, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := setupAcceptFixture(t, 1771)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if _, err := fixture.pool.Exec(ctx, `UPDATE lanes SET accepted_seq=32,published_seq=32,catalog_generation=2 WHERE tenant_id=$1 AND lane_id=0`, fixture.tenantID); err != nil {
				t.Fatal(err)
			}
			large := seedCompactionSizeCohort(t, ctx, fixture, 0, 1, test.largeCount, 5<<20, "large")
			small := seedCompactionSizeCohort(t, ctx, fixture, 0, 2, test.smallCount, 4<<10, "small")
			operations, err := control.NewMaintenanceOperations(fixture.pool)
			if err != nil {
				t.Fatal(err)
			}
			candidate, err := operations.FindCompactionCandidate(ctx)
			if test.wantCount == 0 {
				if !errors.Is(err, control.ErrMaintenanceNoWork) {
					t.Fatalf("quiet mixed sizes churn: candidate=%+v err=%v", candidate, err)
				}
				return
			}
			expected := large
			if test.wantSmall {
				expected = small
			}
			var rewriteBytes int64
			if err == nil {
				if err := fixture.pool.QueryRow(ctx, `SELECT sum(bytes) FROM files WHERE tenant_id=$1 AND bundle_id=ANY($2::uuid[])`, fixture.tenantID, candidate.BundleIDs).Scan(&rewriteBytes); err != nil {
					t.Fatal(err)
				}
			}
			t.Logf("catalog selection inputs=%d rewrite_bytes=%d; not actual S3/native rewrite bytes", len(candidate.BundleIDs), rewriteBytes)
			slices.Sort(expected)
			actual := slices.Clone(candidate.BundleIDs)
			slices.Sort(actual)
			if err != nil || !slices.Equal(actual, expected) || len(actual) != test.wantCount {
				t.Fatalf("mixed-size rewrite: actual=%v expected=%v bytes=%d err=%v", actual, expected, rewriteBytes, err)
			}
			_, err = operations.ReserveCompaction(ctx, control.ReserveCompactionCommand{InstallationID: acceptInstallationID, StorageGeneration: 1,
				TaskID: uuid.NewString(), TenantID: fixture.tenantID, LaneID: 0, BundleIDs: candidate.BundleIDs})
			if err != nil {
				t.Fatalf("cohort not reservable: %v", err)
			}
			if _, err := operations.FindCompactionCandidate(ctx); !errors.Is(err, control.ErrMaintenanceNoWork) {
				t.Fatalf("active lane admitted another tier: %v", err)
			}
		})
	}
}

func TestCompactionCandidateSizeTierBoundaries(t *testing.T) {
	for _, threshold := range []int64{16 << 10, 64 << 10, 256 << 10, 1 << 20, 4 << 20} {
		t.Run(fmt.Sprint(threshold), func(t *testing.T) {
			fixture := setupAcceptFixture(t, 1772)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if _, err := fixture.pool.Exec(ctx, `UPDATE lanes SET accepted_seq=32,published_seq=32,catalog_generation=2 WHERE tenant_id=$1 AND lane_id=0`, fixture.tenantID); err != nil {
				t.Fatal(err)
			}
			seedCompactionSizeCohort(t, ctx, fixture, 0, 1, 7, threshold/2-1, "below")
			expected := seedCompactionSizeCohort(t, ctx, fixture, 0, 2, 8, threshold/2, "at")
			operations, err := control.NewMaintenanceOperations(fixture.pool)
			if err != nil {
				t.Fatal(err)
			}
			candidate, err := operations.FindCompactionCandidate(ctx)
			slices.Sort(expected)
			actual := slices.Clone(candidate.BundleIDs)
			slices.Sort(actual)
			if err != nil || !slices.Equal(actual, expected) {
				t.Fatalf("tier boundary=%d selected=%v expected=%v err=%v", threshold, actual, expected, err)
			}
		})
	}
}

func TestCompactionCandidateRanksReadyCohortNotMixedPartitionCount(t *testing.T) {
	fixture := setupAcceptFixture(t, 1773)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := fixture.pool.Exec(ctx, `UPDATE lanes SET accepted_seq=32,published_seq=32,catalog_generation=2 WHERE tenant_id=$1 AND lane_id IN (0,1)`, fixture.tenantID); err != nil {
		t.Fatal(err)
	}
	seedCompactionSizeCohort(t, ctx, fixture, 0, 1, 7, 4<<10, "mixed-small")
	seedCompactionSizeCohort(t, ctx, fixture, 0, 1, 7, 1<<20, "mixed-large")
	seedCompactionSizeCohort(t, ctx, fixture, 1, 1, 8, 4<<10, "ready")
	operations, err := control.NewMaintenanceOperations(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := operations.FindCompactionCandidate(ctx)
	if err != nil || candidate.LaneID != 1 || len(candidate.BundleIDs) != 8 {
		t.Fatalf("mixed partition inflated expected reduction: %+v %v", candidate, err)
	}
}

func seedCompactionSizeCohort(t *testing.T, ctx context.Context, fixture *acceptFixture, lane int, generation int64, count int, fileBytes int64, label string) []string {
	t.Helper()
	ids := make([]string, count)
	for index := range ids {
		ids[index] = insertMaintenanceBundle(t, ctx, fixture, generation, int64(index+1), fmt.Sprintf("%s-%d", label, index))
		if _, err := fixture.pool.Exec(ctx, `UPDATE bundles SET lane_id=$2 WHERE bundle_id=$1`, ids[index], lane); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.pool.Exec(ctx, `UPDATE files SET bytes=$2 WHERE bundle_id=$1`, ids[index], fileBytes); err != nil {
			t.Fatal(err)
		}
	}
	return ids
}
