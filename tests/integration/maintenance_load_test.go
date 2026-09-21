package integration

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRetentionLoadsItsSingleReservedBundle(t *testing.T) {
	fixture := setupAcceptFixture(t, 1770)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	operations, err := control.NewMaintenanceOperations(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	_, claim := reservePressureMaintenance(t, ctx, fixture, operations, "retain")
	task := claim("retention-loader")
	if task == nil {
		t.Fatal("retention claim returned no work")
	}
	if _, err := operations.LoadCompaction(ctx, task.Authority); err == nil {
		t.Fatal("single retention input relaxed compaction's two-input minimum")
	}
	work, err := operations.LoadRetention(ctx, control.RetentionTask{CompactionTask: *task, RetentionFloor: 1_500_000})
	if err != nil || len(work.Inputs) != 1 || work.FullyExpired {
		t.Fatalf("single reserved retention input: work=%+v err=%v", work, err)
	}
	input := work.Inputs[0]
	if input.Analytics.FileID == "" || input.Payload.FileID == "" || len(input.ProjectIDs) != 1 || input.ProjectIDs[0] != fixture.projectID {
		t.Fatalf("incomplete retention pair: %+v", input)
	}
}

type maintenanceQueryCounter struct{ queries atomic.Int64 }

func (counter *maintenanceQueryCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	counter.queries.Add(1)
	return ctx
}
func (*maintenanceQueryCounter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestCompactionLoadQueryCountIndependentOfInputs(t *testing.T) {
	for _, count := range []int{2, control.MaxCompactionInputs} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			fixture := setupAcceptFixture(t, 1771)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if _, err := fixture.pool.Exec(ctx, `UPDATE lanes SET accepted_seq=$2,published_seq=$2,catalog_generation=1 WHERE tenant_id=$1 AND lane_id=0`, fixture.tenantID, count); err != nil {
				t.Fatal(err)
			}
			secondProject := fixture.projectID + 1
			if _, err := fixture.pool.Exec(ctx, `INSERT INTO projects(tenant_id,project_id,scrub_revision) VALUES($1,$2,1)`, fixture.tenantID, secondProject); err != nil {
				t.Fatal(err)
			}
			ids := make([]string, count)
			for index := range ids {
				ids[index] = insertMaintenanceBundle(t, ctx, fixture, 1, int64(index+1), fmt.Sprintf("load-%d", index))
				if index%2 == 1 {
					if _, err := fixture.pool.Exec(ctx, `INSERT INTO bundle_projects(tenant_id,bundle_id,project_id) VALUES($1,$2,$3)`, fixture.tenantID, ids[index], secondProject); err != nil {
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
			_, err = operations.ReserveCompaction(ctx, control.ReserveCompactionCommand{InstallationID: acceptInstallationID, StorageGeneration: 1, TaskID: uuid.NewString(), TenantID: fixture.tenantID, LaneID: 0, BundleIDs: ids})
			if err != nil {
				t.Fatal(err)
			}
			task, err := operations.ClaimCompaction(ctx, acceptInstallationID, "bulk-loader", time.Minute)
			if err != nil || task == nil {
				t.Fatalf("claim=%+v err=%v", task, err)
			}
			if _, err := operations.LoadRetention(ctx, control.RetentionTask{CompactionTask: *task, RetentionFloor: 1_500_000}); err == nil {
				t.Fatal("retention accepted multiple reserved compaction inputs")
			}
			// Warm the same real connection/statements before five paired samples.
			if _, err := operations.LoadCompaction(ctx, task.Authority); err != nil {
				t.Fatal(err)
			}
			var maximumQueries int64
			for sample := range 5 {
				counter.queries.Store(0)
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				started := time.Now()
				work, err := operations.LoadCompaction(ctx, task.Authority)
				elapsed := time.Since(started)
				runtime.ReadMemStats(&after)
				queries := counter.queries.Load()
				maximumQueries = max(maximumQueries, queries)
				if err != nil || len(work.Inputs) != count || !sameMaintenanceBundleSet(work.Inputs, ids) {
					t.Fatalf("load=%+v err=%v", work, err)
				}
				for _, input := range work.Inputs {
					projects := 1 + int((input.InputSeqMin-1)%2)
					if len(input.ProjectIDs) != projects || input.ProjectIDs[0] != fixture.projectID || projects == 2 && input.ProjectIDs[1] != secondProject || input.Analytics.RowCount != 1 || input.Payload.RowCount != 1 || input.Analytics.FileID == input.Payload.FileID {
						t.Fatalf("bulk metadata lost pair/project association: %+v", input)
					}
				}
				t.Logf("maintenance metadata inputs=%d sample=%d elapsed_ns=%d queries=%d go_alloc_bytes=%d go_allocs=%d s3_requests=0 network_s3_bytes=0", count, sample, elapsed.Nanoseconds(), queries, after.TotalAlloc-before.TotalAlloc, after.Mallocs-before.Mallocs)
			}
			if maximumQueries > 6 {
				t.Errorf("input count introduced database round trips: inputs=%d queries=%d want<=6", count, maximumQueries)
			}
			stale := task.Authority
			stale.Fence++
			if _, err := operations.LoadCompaction(ctx, stale); !errors.Is(err, control.ErrMaintenanceFence) {
				t.Fatalf("stale authority loaded inputs: %v", err)
			}
			canceled, stop := context.WithCancel(ctx)
			stop()
			if _, err := operations.LoadCompaction(canceled, task.Authority); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled loader: %v", err)
			}
			if _, err := fixture.pool.Exec(ctx, `UPDATE object_intents SET state='uploaded' WHERE intent_id=(SELECT intent_id FROM files WHERE bundle_id=$1 AND role='payload')`, ids[0]); err != nil {
				t.Fatal(err)
			}
			if _, err := operations.LoadCompaction(ctx, task.Authority); err == nil {
				t.Fatal("unreferenced payload was accepted as a complete pair")
			}
		})
	}
}
