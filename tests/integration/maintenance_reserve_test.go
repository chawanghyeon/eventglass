package integration

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestCompactionReservationQueryCountIndependentOfInputs(t *testing.T) {
	t.Cleanup(func() {
		var usage syscall.Rusage
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
			t.Fatal(err)
		}
		bytes := usage.Maxrss
		if runtime.GOOS == "linux" {
			bytes *= 1024
		}
		t.Logf("reservation process max_rss_bytes=%d (includes fixture setup)", bytes)
	})
	for _, count := range []int{2, 8, 32, control.MaxCompactionInputs} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			fixture := setupAcceptFixture(t, 1780)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			ids := make([]string, count)
			identities := make(map[string]string, count)
			for index := range ids {
				label := fmt.Sprintf("reserve-%d", index)
				ids[index] = insertMaintenanceBundle(t, ctx, fixture, 1, int64(index+1), label)
				identities[ids[index]] = fixtureSHA(label + ":identity")
			}
			// Independently preserve the original sorted, newline-framed JSON hash.
			sorted := slices.Clone(ids)
			slices.Sort(sorted)
			digest := sha256.New()
			for _, id := range sorted {
				fmt.Fprintf(digest, "{\"bundle_id\":\"%s\",\"valid_from\":1,\"identity\":\"%s\"}\n", id, identities[id])
			}
			wantIdentity := fmt.Sprintf("%x", digest.Sum(nil))
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
			var maximumQueries int64
			for sample := -1; sample < 5; sample++ { // One unmeasured connection/statement warmup.
				// Do not carry fixture/reset CPU throttling into a short sample.
				// This pause is outside timing and identical for both revisions.
				if sample >= 0 {
					time.Sleep(150 * time.Millisecond)
				}
				command := control.ReserveCompactionCommand{InstallationID: acceptInstallationID, StorageGeneration: 1,
					TaskID: uuid.NewString(), TenantID: fixture.tenantID, LaneID: 0, BundleIDs: ids}
				counter.queries.Store(0)
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				started := time.Now()
				task, err := operations.ReserveCompaction(ctx, command)
				elapsed := time.Since(started)
				runtime.ReadMemStats(&after)
				queries := counter.queries.Load()
				if err != nil || task.Authority.TaskID != command.TaskID || task.Partition.EventDay != "2026-09-20" {
					t.Fatalf("reservation=%+v err=%v", task, err)
				}
				var identity string
				var reserved, inputs, mismatches int
				if err := fixture.pool.QueryRow(ctx, `SELECT input_identity,
					(SELECT count(*) FROM bundles WHERE reserved_by=$1),
					(SELECT count(*) FROM maintenance_inputs WHERE task_id=$1),
					(SELECT count(*) FROM maintenance_inputs mi JOIN bundles b USING(tenant_id,bundle_id)
					 WHERE mi.task_id=$1 AND (mi.expected_valid_from_generation<>b.valid_from_generation OR mi.expected_identity_sha256<>b.identity_sha256))
					FROM maintenance_tasks WHERE task_id=$1`, command.TaskID).Scan(&identity, &reserved, &inputs, &mismatches); err != nil {
					t.Fatal(err)
				}
				if identity != wantIdentity || reserved != count || inputs != count || mismatches != 0 {
					t.Fatalf("reservation identity=%s want=%s reserved=%d inputs=%d mismatches=%d", identity, wantIdentity, reserved, inputs, mismatches)
				}
				if sample >= 0 {
					maximumQueries = max(maximumQueries, queries)
					t.Logf("maintenance reservation inputs=%d sample=%d elapsed_ns=%d queries=%d go_alloc_bytes=%d go_allocs=%d s3_requests=0 network_s3_bytes=0", count, sample, elapsed.Nanoseconds(), queries, after.TotalAlloc-before.TotalAlloc, after.Mallocs-before.Mallocs)
				}
				// Reset only this disposable schema's measured reservation, outside timing.
				if _, err := fixture.pool.Exec(ctx, `UPDATE bundles SET reserved_by=NULL WHERE reserved_by=$1`, command.TaskID); err != nil {
					t.Fatal(err)
				}
				if _, err := fixture.pool.Exec(ctx, `DELETE FROM maintenance_tasks WHERE task_id=$1`, command.TaskID); err != nil {
					t.Fatal(err)
				}
			}
			if maximumQueries > 10 {
				t.Errorf("input count introduced reservation round trips: inputs=%d queries=%d want<=10", count, maximumQueries)
			}
		})
	}
}

func TestCompactionReservationRejectsPartialInputsAtomically(t *testing.T) {
	for _, failure := range []string{"missing", "other-tenant", "other-lane", "closed", "partition", "bytes", "generation", "duplicate-alias"} {
		t.Run(failure, func(t *testing.T) {
			fixture := setupAcceptFixture(t, 1781)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			ids := []string{insertMaintenanceBundle(t, ctx, fixture, 1, 1, "first"), insertMaintenanceBundle(t, ctx, fixture, 1, 2, "second")}
			command := control.ReserveCompactionCommand{InstallationID: acceptInstallationID, StorageGeneration: 1,
				TaskID: uuid.NewString(), TenantID: fixture.tenantID, LaneID: 0, BundleIDs: ids}
			var statement string
			switch failure {
			case "missing":
				command.BundleIDs = []string{ids[0], uuid.NewString()}
			case "other-tenant":
				command.TenantID++
			case "other-lane":
				statement = `UPDATE bundles SET lane_id=1 WHERE bundle_id=$1`
			case "closed":
				statement = `UPDATE bundles SET valid_to_generation=2,retired_at=clock_timestamp() WHERE bundle_id=$1`
			case "partition":
				statement = `UPDATE bundles SET event_day='2026-09-21' WHERE bundle_id=$1`
			case "bytes":
				statement = `UPDATE files SET bytes=134217729 WHERE bundle_id=$1`
			case "generation":
				command.StorageGeneration++
			case "duplicate-alias":
				command.BundleIDs = []string{ids[0], strings.ToUpper(ids[0])}
			}
			if statement != "" {
				if _, err := fixture.pool.Exec(ctx, statement, ids[1]); err != nil {
					t.Fatal(err)
				}
			}
			operations, _ := control.NewMaintenanceOperations(fixture.pool)
			if _, err := operations.ReserveCompaction(ctx, command); err == nil {
				t.Fatal("invalid reservation succeeded")
			}
			assertNoMaintenanceReservation(t, ctx, fixture)
		})
	}
}

func assertNoMaintenanceReservation(t *testing.T, ctx context.Context, fixture *acceptFixture) {
	t.Helper()
	var count int
	if err := fixture.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM maintenance_tasks)+(SELECT count(*) FROM maintenance_inputs)+(SELECT count(*) FROM bundles WHERE reserved_by IS NOT NULL)`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial reservation remains=%d err=%v", count, err)
	}
}

func TestCompactionReservationCancellationAndConcurrentRetry(t *testing.T) {
	fixture := setupAcceptFixture(t, 1782)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ids := []string{insertMaintenanceBundle(t, ctx, fixture, 1, 1, "first"), insertMaintenanceBundle(t, ctx, fixture, 1, 2, "second")}
	operations, _ := control.NewMaintenanceOperations(fixture.pool)
	command := control.ReserveCompactionCommand{InstallationID: acceptInstallationID, StorageGeneration: 1,
		TaskID: uuid.NewString(), TenantID: fixture.tenantID, LaneID: 0, BundleIDs: ids}
	blocker, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(context.Background())
	if _, err := blocker.Exec(ctx, `SELECT bundle_id FROM bundles WHERE bundle_id=$1 FOR UPDATE`, ids[1]); err != nil {
		t.Fatal(err)
	}
	waitCtx, stop := context.WithTimeout(ctx, 150*time.Millisecond)
	defer stop()
	if _, err := operations.ReserveCompaction(waitCtx, command); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("locked input cancellation: %v", err)
	}
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	assertNoMaintenanceReservation(t, ctx, fixture)
	// Retry after the joined cancellation, racing two complete reservations.
	start := make(chan struct{})
	answers := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			candidate := command
			candidate.TaskID = uuid.NewString()
			_, err := operations.ReserveCompaction(ctx, candidate)
			answers <- err
		}()
	}
	close(start)
	success, busy := 0, 0
	for range 2 {
		err := <-answers
		if err == nil {
			success++
		} else if errors.Is(err, control.ErrMaintenanceBusy) {
			busy++
		} else {
			t.Errorf("concurrent reservation: %v", err)
		}
	}
	if success != 1 || busy != 1 {
		t.Fatalf("reservation success=%d busy=%d", success, busy)
	}
	var tasks, inputs, reserved int
	if err := fixture.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM maintenance_tasks),(SELECT count(*) FROM maintenance_inputs),(SELECT count(*) FROM bundles WHERE reserved_by IS NOT NULL)`).Scan(&tasks, &inputs, &reserved); err != nil || tasks != 1 || inputs != 2 || reserved != 2 {
		t.Fatalf("concurrent reservation tasks=%d inputs=%d reserved=%d err=%v", tasks, inputs, reserved, err)
	}
}
