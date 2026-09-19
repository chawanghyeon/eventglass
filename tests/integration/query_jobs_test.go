package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
)

func TestQueryPlanTasksWinningAttemptsAndFixedReduction(t *testing.T) {
	fixture := setupAcceptFixture(t, 970)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	operations, tokenHash := setupQueryPrincipal(t, fixture)
	snapshot, err := operations.CreateSnapshot(ctx, snapshotCommand(t, fixture, tokenHash, 1))
	if err != nil {
		t.Fatal(err)
	}
	operation := []byte(`{"kind":"search"}`)
	operationDigest := sha256.Sum256(operation)
	operationHash := hex.EncodeToString(operationDigest[:])
	queryID := snapshotUUID(fixture.tenantID, 500)
	job, err := operations.CreateQuery(ctx, control.CreateQueryCommand{
		QueryID: queryID, SessionTokenHash: tokenHash, TenantID: fixture.tenantID, SnapshotID: snapshot.SnapshotID,
		OperationKind: "search", OperationHash: operationHash, OperationBytes: operation, Owner: "coordinator-a", Timeout: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	files := make([]model.CatalogFile, 9)
	for index := range files {
		files[index] = model.CatalogFile{
			FileID: fmt.Sprintf("00000000-0000-4000-8000-%012d", index+1), ObjectKey: fmt.Sprintf("v1/files/%d.parquet", index+1),
			Bytes: 8 << 20, SHA256: fmt.Sprintf("%064x", index+1), RowCount: 1,
		}
	}
	plan, err := query.BuildExecutionPlan(query.PlanScope{
		QueryID: queryID, TenantID: fixture.tenantID, SnapshotID: snapshot.SnapshotID, Generation: 1,
		OperationHash: operationHash, Operation: operation, DeadlineUS: job.Deadline.UnixMicro(),
	}, files)
	if err != nil || plan.ScanCount != 2 || plan.ReducerCount != 1 {
		t.Fatalf("plan=%#v err=%v", plan, err)
	}
	if err := operations.SealQueryPlan(ctx, control.SealQueryPlanCommand{
		Authority: job.Authority, PlanSHA256: plan.SHA256, Tasks: plan.Tasks, ScanCount: plan.ScanCount, ManifestBytes: plan.ManifestBytes,
	}); err != nil {
		t.Fatal(err)
	}
	first, err := operations.ClaimQueryTask(ctx, acceptInstallationID, 1, "worker-a")
	if err != nil || first == nil || first.Authority.Key.Stage != model.QueryTaskScan {
		t.Fatalf("first=%#v err=%v", first, err)
	}
	completeQueryTaskFixture(t, ctx, operations, first, 1)
	second, err := operations.ClaimQueryTask(ctx, acceptInstallationID, 1, "worker-b")
	if err != nil || second == nil || second.Authority.Key.Stage != model.QueryTaskScan || second.Authority.Key.PartitionID == first.Authority.Key.PartitionID {
		t.Fatalf("second=%#v err=%v", second, err)
	}
	completeQueryTaskFixture(t, ctx, operations, second, 2)
	reducer, err := operations.ClaimQueryTask(ctx, acceptInstallationID, 1, "worker-c")
	if err != nil || reducer == nil || reducer.Authority.Key.Stage != model.QueryTaskReduce || len(reducer.Inputs) != 2 {
		t.Fatalf("reducer=%#v err=%v", reducer, err)
	}
	completion := completeQueryTaskFixture(t, ctx, operations, reducer, 3)
	if err := operations.CompleteQueryTask(ctx, completion); err != nil {
		t.Fatalf("same winning response was not idempotent: %v", err)
	}
	var committedBytes int64
	if err := fixture.pool.QueryRow(ctx, `SELECT committed_bytes FROM query_level_budgets WHERE query_id=$1 AND level=1`, queryID).Scan(&committedBytes); err != nil {
		t.Fatal(err)
	}
	if committedBytes != completion.Bytes {
		t.Fatalf("idempotent completion counted bytes twice: got=%d want=%d", committedBytes, completion.Bytes)
	}
	var state string
	var resultBytes int64
	if err := fixture.pool.QueryRow(ctx, `SELECT state,result_bytes FROM query_jobs WHERE query_id=$1`, queryID).Scan(&state, &resultBytes); err != nil {
		t.Fatal(err)
	}
	if state != "succeeded" || resultBytes != completion.Bytes {
		t.Fatalf("state=%s bytes=%d", state, resultBytes)
	}
}

func TestQueryLevelBudgetIsSharedAcrossWinningTasks(t *testing.T) {
	fixture := setupAcceptFixture(t, 973)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	operations, tokenHash := setupQueryPrincipal(t, fixture)
	snapshot, err := operations.CreateSnapshot(ctx, snapshotCommand(t, fixture, tokenHash, 1))
	if err != nil {
		t.Fatal(err)
	}
	operation := []byte(`{"kind":"search"}`)
	digest := sha256.Sum256(operation)
	queryID := snapshotUUID(fixture.tenantID, 800)
	job, err := operations.CreateQuery(ctx, control.CreateQueryCommand{
		QueryID: queryID, SessionTokenHash: tokenHash, TenantID: fixture.tenantID, SnapshotID: snapshot.SnapshotID,
		OperationKind: "search", OperationHash: hex.EncodeToString(digest[:]), OperationBytes: operation,
		Owner: "coordinator", Timeout: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	files := []model.CatalogFile{
		{FileID: snapshotUUID(fixture.tenantID, 801), ObjectKey: "v1/files/budget-a.parquet", Bytes: 64 << 20, SHA256: fmt.Sprintf("%064x", 801), RowCount: 1},
		{FileID: snapshotUUID(fixture.tenantID, 802), ObjectKey: "v1/files/budget-b.parquet", Bytes: 64 << 20, SHA256: fmt.Sprintf("%064x", 802), RowCount: 1},
	}
	plan, err := query.BuildExecutionPlan(query.PlanScope{
		QueryID: queryID, TenantID: fixture.tenantID, SnapshotID: snapshot.SnapshotID, Generation: 1,
		OperationHash: hex.EncodeToString(digest[:]), Operation: operation, DeadlineUS: job.Deadline.UnixMicro(),
	}, files)
	if err != nil || plan.ScanCount != 2 {
		t.Fatalf("plan=%#v err=%v", plan, err)
	}
	if err := operations.SealQueryPlan(ctx, control.SealQueryPlanCommand{
		Authority: job.Authority, PlanSHA256: plan.SHA256, Tasks: plan.Tasks, ScanCount: plan.ScanCount, ManifestBytes: plan.ManifestBytes,
	}); err != nil {
		t.Fatal(err)
	}
	first, err := operations.ClaimQueryTask(ctx, acceptInstallationID, 1, "budget-worker-a")
	if err != nil || first == nil {
		t.Fatalf("first=%#v err=%v", first, err)
	}
	completeQueryTaskBytes(t, ctx, operations, first, 1, 40<<20)
	second, err := operations.ClaimQueryTask(ctx, acceptInstallationID, 1, "budget-worker-b")
	if err != nil || second == nil {
		t.Fatalf("second=%#v err=%v", second, err)
	}
	command := uploadedQueryTaskBytes(t, ctx, operations, second, 2, 25<<20)
	if err := operations.CompleteQueryTask(ctx, command); !errors.Is(err, control.ErrQueryLimitExceeded) {
		t.Fatalf("shared level budget accepted 65MiB: %v", err)
	}
	var committed int64
	if err := fixture.pool.QueryRow(ctx, `SELECT committed_bytes FROM query_level_budgets WHERE query_id=$1 AND level=0`, queryID).Scan(&committed); err != nil {
		t.Fatal(err)
	}
	if committed != 40<<20 {
		t.Fatalf("failed completion changed budget: %d", committed)
	}
}

func TestQueryCoordinatorTakeoverCancellationAndLateResultFence(t *testing.T) {
	fixture := setupAcceptFixture(t, 971)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	operations, tokenHash := setupQueryPrincipal(t, fixture)
	snapshot, err := operations.CreateSnapshot(ctx, snapshotCommand(t, fixture, tokenHash, 1))
	if err != nil {
		t.Fatal(err)
	}
	operation := []byte(`{"kind":"search"}`)
	digest := sha256.Sum256(operation)
	queryID := snapshotUUID(fixture.tenantID, 600)
	job, err := operations.CreateQuery(ctx, control.CreateQueryCommand{
		QueryID: queryID, SessionTokenHash: tokenHash, TenantID: fixture.tenantID, SnapshotID: snapshot.SnapshotID,
		OperationKind: "search", OperationHash: hex.EncodeToString(digest[:]), OperationBytes: operation,
		Owner: "dead-coordinator", Timeout: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE query_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE query_id=$1`, queryID); err != nil {
		t.Fatal(err)
	}
	takeover, err := operations.ClaimQueryCoordinator(ctx, acceptInstallationID, 1, "new-coordinator")
	if err != nil || takeover == nil || takeover.Authority.Fence != 2 {
		t.Fatalf("takeover=%#v err=%v", takeover, err)
	}
	plan, err := query.BuildExecutionPlan(query.PlanScope{
		QueryID: queryID, TenantID: fixture.tenantID, SnapshotID: snapshot.SnapshotID, Generation: 1,
		OperationHash: hex.EncodeToString(digest[:]), Operation: operation, DeadlineUS: job.Deadline.UnixMicro(),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	oldSeal := control.SealQueryPlanCommand{Authority: job.Authority, PlanSHA256: plan.SHA256, Tasks: plan.Tasks, ScanCount: plan.ScanCount, ManifestBytes: plan.ManifestBytes}
	if err := operations.SealQueryPlan(ctx, oldSeal); !errors.Is(err, control.ErrQueryFenceStale) {
		t.Fatalf("old coordinator sealed: %v", err)
	}
	oldSeal.Authority = takeover.Authority
	if err := operations.SealQueryPlan(ctx, oldSeal); err != nil {
		t.Fatal(err)
	}
	task, err := operations.ClaimQueryTask(ctx, acceptInstallationID, 1, "worker")
	if err != nil || task == nil || len(task.Inputs) != 0 {
		t.Fatalf("empty reducer=%#v err=%v", task, err)
	}
	completion := registerUploadedQueryIntentFixture(t, ctx, operations, task, 10)
	if err := operations.CancelQuery(ctx, tokenHash, fixture.tenantID, queryID); err != nil {
		t.Fatal(err)
	}
	if err := operations.CompleteQueryTask(ctx, completion); !errors.Is(err, control.ErrQueryTerminal) {
		t.Fatalf("late result accepted: %v", err)
	}
}

func TestConcurrentQueryAdmissionUsesSharedPostgresCap(t *testing.T) {
	fixture := setupAcceptFixture(t, 972)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	first, tokenHash := setupQueryPrincipal(t, fixture)
	second, err := control.NewQueryOperations(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := first.CreateSnapshot(ctx, snapshotCommand(t, fixture, tokenHash, 1))
	if err != nil {
		t.Fatal(err)
	}
	operation := []byte(`{"kind":"search"}`)
	digest := sha256.Sum256(operation)
	start := make(chan struct{})
	results := make(chan error, 6)
	var ready sync.WaitGroup
	ready.Add(6)
	for index := 0; index < 6; index++ {
		operations := first
		if index%2 == 1 {
			operations = second
		}
		command := control.CreateQueryCommand{
			QueryID: snapshotUUID(fixture.tenantID, 700+index), SessionTokenHash: tokenHash, TenantID: fixture.tenantID,
			SnapshotID: snapshot.SnapshotID, OperationKind: "search", OperationHash: hex.EncodeToString(digest[:]),
			OperationBytes: operation, Owner: fmt.Sprintf("api-%d", index), Timeout: time.Minute,
		}
		go func() {
			ready.Done()
			<-start
			_, err := operations.CreateQuery(ctx, command)
			results <- err
		}()
	}
	ready.Wait()
	close(start)
	succeeded, limited := 0, 0
	for range 6 {
		err := <-results
		if err == nil {
			succeeded++
		} else if errors.Is(err, control.ErrQueryLimitExceeded) {
			limited++
		} else {
			t.Fatalf("admission=%v", err)
		}
	}
	if succeeded != control.MaxUserQueries || limited != 6-control.MaxUserQueries {
		t.Fatalf("succeeded=%d limited=%d", succeeded, limited)
	}
}

func completeQueryTaskFixture(t *testing.T, ctx context.Context, operations *control.QueryOperations, task *control.QueryTask, ordinal int) control.CompleteQueryTaskCommand {
	t.Helper()
	command := registerUploadedQueryIntentFixture(t, ctx, operations, task, ordinal)
	if err := operations.CompleteQueryTask(ctx, command); err != nil {
		t.Fatal(err)
	}
	return command
}

func registerUploadedQueryIntentFixture(t *testing.T, ctx context.Context, operations *control.QueryOperations, task *control.QueryTask, ordinal int) control.CompleteQueryTaskCommand {
	t.Helper()
	bytes := []byte(fmt.Sprintf("query-result-%d", ordinal))
	return uploadedQueryTaskBytes(t, ctx, operations, task, ordinal, int64(len(bytes)))
}

func completeQueryTaskBytes(t *testing.T, ctx context.Context, operations *control.QueryOperations, task *control.QueryTask, ordinal int, byteCount int64) {
	t.Helper()
	command := uploadedQueryTaskBytes(t, ctx, operations, task, ordinal, byteCount)
	if err := operations.CompleteQueryTask(ctx, command); err != nil {
		t.Fatal(err)
	}
}

func uploadedQueryTaskBytes(t *testing.T, ctx context.Context, operations *control.QueryOperations, task *control.QueryTask, ordinal int, byteCount int64) control.CompleteQueryTaskCommand {
	t.Helper()
	bytes := []byte(fmt.Sprintf("query-result-%d-%d", ordinal, byteCount))
	digest := sha256.Sum256(bytes)
	checksum := hex.EncodeToString(digest[:])
	intent := control.IntentAuthority{
		IntentID: fmt.Sprintf("00000000-0000-4000-9000-%012d", task.Authority.TenantID*100+int64(ordinal)), Owner: task.Authority.Owner,
		Fence: task.Authority.Fence, Bytes: byteCount, SHA256: checksum,
	}
	if err := operations.RegisterQueryIntent(ctx, control.QueryIntentRegistration{
		Authority: task.Authority, ObjectKey: fmt.Sprintf("v1/query/%s/%s-%d-%d", task.Authority.QueryID, task.Authority.Key.Stage, task.Authority.Key.Level, task.Authority.Key.PartitionID), Intent: intent,
	}); err != nil {
		t.Fatal(err)
	}
	if err := operations.MarkQueryIntentUploaded(ctx, task.Authority, intent); err != nil {
		t.Fatal(err)
	}
	return control.CompleteQueryTaskCommand{Authority: task.Authority, IntentID: intent.IntentID, SHA256: checksum, Rows: 1, Bytes: byteCount}
}
