package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
)

// Real PostgreSQL admission/claim/completion transitions with metadata-only
// plans. Native/S3 execution is verified separately, not simulated by these IDs.
func TestQueryClaimsSkipSaturatedQueryWithoutRelaxingCap(t *testing.T) {
	fixture := setupAcceptFixture(t, 1801)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	operations, token := setupQueryPrincipal(t, fixture)
	full := sealSchedulingQuery(t, ctx, fixture, operations, token, 10, 5)
	ready := sealSchedulingQuery(t, ctx, fixture, operations, token, 20, 1)
	for index := range control.MaxRunningQueryTasks {
		task, err := operations.ClaimQueryTaskForQuery(ctx, acceptInstallationID, 1, fmt.Sprintf("occupied-%d", index), full)
		if err != nil || task == nil || task.Authority.QueryID != full {
			t.Fatalf("occupy=%+v error=%v", task, err)
		}
	}
	if task, err := operations.ClaimQueryTaskForQuery(ctx, acceptInstallationID, 1, "full-helper", full); err != nil || task != nil {
		t.Fatalf("targeted helper exceeded cap or stole another query: %+v %v", task, err)
	}
	task, err := operations.ClaimQueryTask(ctx, acceptInstallationID, 1, "available-worker")
	if err != nil || task == nil || task.Authority.QueryID != ready {
		t.Fatalf("saturated older query hid runnable query: task=%+v expected=%s error=%v", task, ready, err)
	}
	var running int
	if err := fixture.pool.QueryRow(ctx, `SELECT count(*) FROM query_tasks WHERE query_id=$1 AND state='running'`, full).Scan(&running); err != nil || running != control.MaxRunningQueryTasks {
		t.Fatalf("original query cap changed: %d %v", running, err)
	}
}

func TestQueryClaimsRotateTenantAfterCompletedTask(t *testing.T) {
	fixture := setupAcceptFixture(t, 1802)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	second := addSchedulingTenant(t, ctx, fixture, 1803)
	operations, token := setupQueryPrincipal(t, fixture)
	otherOps, otherToken := setupQueryPrincipal(t, second)
	firstQuery := sealSchedulingQuery(t, ctx, fixture, operations, token, 10, 2)
	secondQuery := sealSchedulingQuery(t, ctx, second, otherOps, otherToken, 10, 1)
	first, err := operations.ClaimQueryTask(ctx, acceptInstallationID, 1, "single-worker")
	if err != nil || first == nil || first.Authority.QueryID != firstQuery {
		t.Fatalf("first=%+v error=%v", first, err)
	}
	completeQueryTaskFixture(t, ctx, operations, first, 81)
	if _, err := operations.ClaimQueryTaskAfterTenant(ctx, acceptInstallationID, 1, "single-worker", -1); err == nil {
		t.Fatal("negative query cursor admitted")
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := operations.ClaimQueryTaskAfterTenant(canceled, acceptInstallationID, 1, "single-worker", first.Authority.TenantID); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled cursor claim=%v", err)
	}
	if _, err := operations.ClaimQueryTaskAfterTenant(ctx, acceptInstallationID, 2, "single-worker", first.Authority.TenantID); !errors.Is(err, control.ErrJobFenceStale) {
		t.Fatalf("cursor bypassed installation generation: %v", err)
	}
	next, err := operations.ClaimQueryTaskAfterTenant(ctx, acceptInstallationID, 1, "single-worker", first.Authority.TenantID)
	if err != nil || next == nil || next.Authority.QueryID != secondQuery {
		t.Fatalf("finished task lost the other tenant's turn: missing=%t expected=%s error=%v", next == nil, secondQuery, err)
	}
	completeQueryTaskFixture(t, ctx, operations, next, 82)
	wrapped, err := operations.ClaimQueryTaskAfterTenant(ctx, acceptInstallationID, 1, "single-worker", math.MaxInt64)
	if err != nil || wrapped == nil || wrapped.Authority.QueryID != firstQuery {
		t.Fatalf("maximum cursor did not wrap to remaining work: missing=%t error=%v", wrapped == nil, err)
	}
}

func TestQueryClaimsConcurrentPerQueryCapAndProgress(t *testing.T) {
	fixture := setupAcceptFixture(t, 1804)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	operations, token := setupQueryPrincipal(t, fixture)
	first := sealSchedulingQuery(t, ctx, fixture, operations, token, 10, 5)
	second := sealSchedulingQuery(t, ctx, fixture, operations, token, 20, 5)
	type answer struct {
		task *control.QueryTask
		err  error
	}
	start, replies := make(chan struct{}), make(chan answer, 12)
	var ready sync.WaitGroup
	ready.Add(cap(replies))
	for index := range cap(replies) {
		go func() {
			ready.Done()
			<-start
			task, err := operations.ClaimQueryTaskAfterTenant(ctx, acceptInstallationID, 1, fmt.Sprintf("racing-%d", index), fixture.tenantID)
			replies <- answer{task, err}
		}()
	}
	ready.Wait()
	close(start)
	var tasks []*control.QueryTask
	var claimErrors []error
	for range cap(replies) { // Join every caller before any fatal assertion/cleanup.
		reply := <-replies
		if reply.err != nil {
			claimErrors = append(claimErrors, reply.err)
		}
		if reply.task != nil {
			tasks = append(tasks, reply.task)
		}
	}
	if len(claimErrors) != 0 {
		t.Fatalf("concurrent claims=%v", claimErrors)
	}
	// SKIP LOCKED may defer a raced query to the next sweep. Once those
	// transactions have joined, fill all remaining admitted slots, boundedly.
	for range 2*control.MaxRunningQueryTasks + 1 {
		task, err := operations.ClaimQueryTask(ctx, acceptInstallationID, 1, "next-sweep")
		if err != nil {
			t.Fatal(err)
		}
		if task == nil {
			break
		}
		tasks = append(tasks, task)
	}
	if len(tasks) != 2*control.MaxRunningQueryTasks {
		t.Fatalf("full older query hid progress or cap exceeded: claims=%d", len(tasks))
	}
	for _, queryID := range []string{first, second} {
		var running int
		if err := fixture.pool.QueryRow(ctx, `SELECT count(*) FROM query_tasks WHERE query_id=$1 AND state='running'`, queryID).Scan(&running); err != nil || running != control.MaxRunningQueryTasks {
			t.Fatalf("query=%s running=%d error=%v", queryID, running, err)
		}
	}
	completed := tasks[0]
	completeQueryTaskFixture(t, ctx, operations, completed, 83)
	next, err := operations.ClaimQueryTaskForQuery(ctx, acceptInstallationID, 1, "replacement-slot", completed.Authority.QueryID)
	if err != nil || next == nil || next.Authority.Key == completed.Authority.Key {
		t.Fatalf("completed slot did not admit its queued successor: missing=%t error=%v", next == nil, err)
	}
}

func sealSchedulingQuery(t *testing.T, ctx context.Context, fixture *acceptFixture, operations *control.QueryOperations, token [32]byte, ordinal, scans int) string {
	t.Helper()
	snapshot, err := operations.CreateSnapshot(ctx, snapshotCommand(t, fixture, token, ordinal))
	if err != nil {
		t.Fatal(err)
	}
	operation := []byte(`{"kind":"search"}`)
	digest := sha256.Sum256(operation)
	operationHash := hex.EncodeToString(digest[:])
	queryID := snapshotUUID(fixture.tenantID, ordinal+500)
	job, err := operations.CreateQuery(ctx, control.CreateQueryCommand{
		QueryID: queryID, SessionTokenHash: token, TenantID: fixture.tenantID, SnapshotID: snapshot.SnapshotID,
		OperationKind: "search", OperationHash: operationHash, OperationBytes: operation, Owner: "scheduling-coordinator", Timeout: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	files := make([]model.CatalogFile, scans)
	for index := range files {
		files[index] = model.CatalogFile{FileID: snapshotUUID(fixture.tenantID, 1000+ordinal*10+index), ObjectKey: fmt.Sprintf("v1/scheduling/%d/%d.parquet", ordinal, index), Bytes: 64 << 20, SHA256: fmt.Sprintf("%064x", index+1), RowCount: 1}
	}
	plan, err := query.BuildExecutionPlan(query.PlanScope{QueryID: queryID, TenantID: fixture.tenantID, SnapshotID: snapshot.SnapshotID, Generation: 1,
		OperationHash: operationHash, Operation: operation, DeadlineUS: job.Deadline.UnixMicro()}, files)
	if err != nil || plan.ScanCount != scans {
		t.Fatalf("plan scans=%d expected=%d error=%v", plan.ScanCount, scans, err)
	}
	if err := operations.SealQueryPlan(ctx, control.SealQueryPlanCommand{Authority: job.Authority, PlanSHA256: plan.SHA256, Tasks: plan.Tasks, ScanCount: plan.ScanCount, ManifestBytes: plan.ManifestBytes}); err != nil {
		t.Fatal(err)
	}
	return queryID
}
