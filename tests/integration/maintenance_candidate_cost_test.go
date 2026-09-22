package integration

import (
	"context"
	"fmt"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Catalog-only cost evidence: real PostgreSQL, no native rewrite or S3. The
// separate durable workflow/load gates establish those costs and correctness.
// Both revisions use the same 8/32/128-bundle logical catalog, private schema,
// two files per bundle, limits and warmup. UUIDs are fresh, not byte-identical.
func TestCompactionCandidateCostAndRoundTrips(t *testing.T) {
	fixture := setupAcceptFixture(t, 1763)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for lane, count := range []int{8, 32, 128} {
		if _, err := fixture.pool.Exec(ctx, `UPDATE lanes SET accepted_seq=$3,published_seq=$3,catalog_generation=1 WHERE tenant_id=$1 AND lane_id=$2`, fixture.tenantID, lane, count); err != nil {
			t.Fatal(err)
		}
		for index := range count {
			id := insertMaintenanceBundle(t, ctx, fixture, 1, int64(index+1), fmt.Sprintf("cost-%d-%d", lane, index))
			if _, err := fixture.pool.Exec(ctx, `UPDATE bundles SET lane_id=$2 WHERE bundle_id=$1`, id, lane); err != nil {
				t.Fatal(err)
			}
		}
	}
	counter := &maintenanceQueryCounter{}
	config := fixture.pool.Config()
	config.MaxConns, config.MinConns = 1, 1
	config.ConnConfig.Tracer = counter
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	operations, err := control.NewMaintenanceOperations(pool)
	if err != nil {
		t.Fatal(err)
	}
	for sample := -1; sample < 5; sample++ {
		// Fixed pause outside timing keeps fixture and preceding sample work
		// out of a fresh CPU quota interval in the matched bounded runner.
		time.Sleep(150 * time.Millisecond)
		counter.queries.Store(0)
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		started := time.Now()
		const repeats = 10
		var candidate control.CompactionCandidate
		for range repeats {
			candidate, err = operations.FindCompactionCandidate(ctx)
			if err != nil {
				t.Fatal(err)
			}
		}
		elapsed := time.Since(started)
		runtime.ReadMemStats(&after)
		queries := counter.queries.Load()
		if queries != 2*repeats || len(candidate.BundleIDs) < 8 || len(candidate.BundleIDs) > control.MaxCompactionInputs {
			t.Fatalf("unbounded selection: queries=%d files=%d", queries, len(candidate.BundleIDs))
		}
		if sample >= 0 {
			t.Logf("candidate sample=%d catalog_bundles=168 selected_lane=%d selected_inputs=%d elapsed_ns_per_op=%d queries_per_op=%d go_alloc_bytes_per_op=%d go_allocs_per_op=%d s3_requests=0 s3_bytes=0", sample, candidate.LaneID, len(candidate.BundleIDs), elapsed.Nanoseconds()/repeats, queries/repeats, (after.TotalAlloc-before.TotalAlloc)/repeats, (after.Mallocs-before.Mallocs)/repeats)
		}
	}
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		t.Fatal(err)
	}
	rss := usage.Maxrss
	if runtime.GOOS == "linux" {
		rss *= 1024
	}
	t.Logf("candidate process_max_rss_bytes=%d (includes fixture/setup)", rss)
}
