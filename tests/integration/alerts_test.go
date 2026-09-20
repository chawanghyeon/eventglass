package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/alerts"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
	"github.com/jackc/pgx/v5/pgxpool"
)

type alertFixture struct {
	pool                  *pgxpool.Pool
	tenant, project, user int64
	operations            *control.AlertOperations
	destination           string
	sequence              int64
}

func setupAlertFixture(t *testing.T, base int64) *alertFixture {
	t.Helper()
	env := requiredEnvironment(t, "EVENTGLASS_DATABASE_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	if err := control.ApplyMigrations(ctx, env["EVENTGLASS_DATABASE_URL"]); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, env["EVENTGLASS_DATABASE_URL"])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	f := &alertFixture{pool: pool, tenant: base, project: base*10 + 1, user: base*10 + 2, sequence: base * 100}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `UPDATE deliveries SET state='canceled',owner=NULL,lease_until=NULL WHERE tenant_id=$1 AND state IN ('queued','running')`, f.tenant)
		_, _ = pool.Exec(context.Background(), `DELETE FROM audit_events WHERE tenant_id=$1`, f.tenant)
	})
	f.operations, _ = control.NewAlertOperations(pool)
	_, err = pool.Exec(ctx, `INSERT INTO installations(singleton,installation_id,storage_generation,schema_version,storage_identity) VALUES(true,'00000000-0000-4000-8000-00000000f001',1,1,'alert-integration') ON CONFLICT(singleton) DO NOTHING`)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO tenants(tenant_id) VALUES($1)`, []any{f.tenant}},
		{`INSERT INTO projects(tenant_id,project_id,scrub_revision) VALUES($1,$2,1)`, []any{f.tenant, f.project}},
		{`INSERT INTO users(user_id,email_normalized,password_phc) VALUES($1,$2,repeat('x',32))`, []any{f.user, fmt.Sprintf("alert-%d@example.invalid", base)}},
		{`INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'admin')`, []any{f.tenant, f.user}},
	} {
		if _, err = pool.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	for lane := 0; lane < model.LaneCount; lane++ {
		if _, err := pool.Exec(ctx, `INSERT INTO lanes(tenant_id,lane_id) VALUES($1,$2)`, f.tenant, lane); err != nil {
			t.Fatal(err)
		}
	}
	destination := f.uuid()
	value, err := f.operations.CreateDestination(ctx, control.CreateDestinationCommand{TenantID: f.tenant, ActorUserID: f.user, DestinationID: destination, Name: "integration", URL: "https://example.invalid/hook", RequestID: f.uuid(), AuditID: f.uuid()})
	if err != nil {
		t.Fatal(err)
	}
	f.destination = value.DestinationID
	return f
}
func (f *alertFixture) uuid() string {
	f.sequence++
	return fmt.Sprintf("00000000-0000-4000-8000-%012x", f.sequence)
}
func testRule(t *testing.T, kind alerts.Kind, raw string) alerts.Rule {
	t.Helper()
	value, err := alerts.ValidateRule(kind, []byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestAlertThresholdBarrierBacklogCompletionRevisionAndRetention(t *testing.T) {
	f := setupAlertFixture(t, 1301)
	ctx := context.Background()
	rule := testRule(t, alerts.KindThreshold, `{"expression":"true","kinds":["error"],"window_seconds":60,"metric":"count","operator":"ge","threshold":"1"}`)
	created, err := f.operations.CreateAlert(ctx, control.CreateAlertCommand{TenantID: f.tenant, ProjectID: f.project, ActorUserID: f.user, AlertID: f.uuid(), Name: "threshold", DestinationID: f.destination, Kind: "threshold", RuleBytes: rule.Bytes, RuleSHA256: rule.SHA256, CooldownSeconds: 60, RequestID: f.uuid(), AuditID: f.uuid()})
	if err != nil {
		t.Fatal(err)
	}
	// Put the first scheduled window in the past without changing its ordering
	// semantics; production obtains this value from the database clock at create.
	var end int64
	if err := f.pool.QueryRow(ctx, `SELECT (floor(extract(epoch FROM clock_timestamp())/60)*60*1000000)::bigint FROM installations WHERE singleton`).Scan(&end); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `UPDATE alerts SET first_window_end_us=$3 WHERE tenant_id=$1 AND alert_id=$2`, f.tenant, created.AlertID, end); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `UPDATE lanes SET accepted_seq=CASE WHEN lane_id=0 THEN 7 ELSE 0 END,published_seq=CASE WHEN lane_id=0 THEN 6 ELSE 0 END WHERE tenant_id=$1`, f.tenant); err != nil {
		t.Fatal(err)
	}
	reserved, err := f.operations.ReserveThresholdWindow(ctx, f.tenant, created.AlertID, f.uuid())
	if err != nil || reserved == nil {
		t.Fatalf("reserve=%+v err=%v", reserved, err)
	}
	if reserved.Cut[0] != 7 {
		t.Fatalf("cut=%v", reserved.Cut)
	}
	var barrier int64
	if err := f.pool.QueryRow(ctx, `SELECT last_received_time_us FROM lanes WHERE tenant_id=$1 AND lane_id=0`, f.tenant).Scan(&barrier); err != nil || barrier < end {
		t.Fatalf("barrier=%d err=%v", barrier, err)
	}
	waiting, err := f.operations.PromoteThresholdEvaluation(ctx, f.tenant, reserved.EvaluationID)
	if err != nil || waiting.State != "waiting" {
		t.Fatalf("pending publication became zero: %+v %v", waiting, err)
	}
	if _, err := f.pool.Exec(ctx, `UPDATE lanes SET published_seq=accepted_seq WHERE tenant_id=$1`, f.tenant); err != nil {
		t.Fatal(err)
	}
	queued, err := f.operations.PromoteThresholdEvaluation(ctx, f.tenant, reserved.EvaluationID)
	if err != nil || queued.State != "queued" {
		t.Fatalf("promote=%+v %v", queued, err)
	}
	filter := &query.Node{Op: "constant", Constant: true}
	canonical, _ := query.CanonicalFilter(filter)
	spec := model.DatasetSpec{TenantID: f.tenant, ProjectIDs: []int64{f.project}, Kinds: []model.Kind{model.KindError}, TimeBasis: model.QueryTimeReceived, StartUS: reserved.WindowStartUS, EndUS: reserved.WindowEndUS, Filter: canonical}
	datasetHash, datasetBytes, err := query.DatasetHash(spec)
	if err != nil {
		t.Fatal(err)
	}
	queryOps, _ := control.NewQueryOperations(f.pool)
	snapshot, err := queryOps.CreateAlertSnapshot(ctx, control.CreateAlertSnapshotCommand{TenantID: f.tenant, AlertID: created.AlertID, EvaluationID: reserved.EvaluationID, SnapshotID: f.uuid(), DatasetSHA256: datasetHash, DatasetBytes: datasetBytes})
	if err != nil {
		t.Fatal(err)
	}
	page, err := queryOps.CatalogPageForAlert(ctx, control.AlertCatalogCommand{AlertID: created.AlertID, CatalogCommand: control.CatalogCommand{TenantID: f.tenant, SnapshotID: snapshot.SnapshotID, DatasetSHA256: datasetHash, DatasetBytes: datasetBytes, TimeBasis: model.QueryTimeReceived, StartUS: spec.StartUS, EndUS: spec.EndUS, Kinds: spec.Kinds, Limit: 256}})
	if err != nil || len(page) != 0 {
		t.Fatalf("alert catalog=%+v err=%v", page, err)
	}
	var cuts [model.LaneCount]int64
	for lane := range snapshot.Lanes {
		cuts[lane] = snapshot.Lanes[lane].CutSeq
	}
	compiled, err := query.BuildPlan(spec, model.SnapshotScope{LaneCuts: cuts}, filter)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := query.BuildAggregateOperation(query.AggregateOperationSpec{Plan: compiled, Metrics: []query.AggregateMetric{{Name: "count", Op: "count"}}, Top: 1, Order: query.AggregateOrder{Metric: "count", Direction: "desc"}})
	if err != nil {
		t.Fatal(err)
	}
	operationBytes, err := query.CanonicalOperation(operation)
	if err != nil {
		t.Fatal(err)
	}
	operationDigest := sha256.Sum256(operationBytes)
	job, err := queryOps.CreateAlertQuery(ctx, control.CreateAlertQueryCommand{QueryID: f.uuid(), TenantAlertID: created.AlertID, TenantID: f.tenant, SnapshotID: snapshot.SnapshotID, OperationHash: hex.EncodeToString(operationDigest[:]), OperationBytes: operationBytes, Owner: "planner", Timeout: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := query.BuildExecutionPlan(query.PlanScope{QueryID: job.Authority.QueryID, TenantID: f.tenant, SnapshotID: snapshot.SnapshotID, Generation: 1, OperationHash: hex.EncodeToString(operationDigest[:]), Operation: operationBytes, DeadlineUS: job.Deadline.UnixMicro()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := queryOps.SealQueryPlan(ctx, control.SealQueryPlanCommand{Authority: job.Authority, PlanSHA256: plan.SHA256, Tasks: plan.Tasks, ScanCount: plan.ScanCount, ManifestBytes: plan.ManifestBytes}); err != nil {
		t.Fatal(err)
	}
	var installationID string
	if err := f.pool.QueryRow(ctx, `SELECT installation_id::text FROM installations WHERE singleton`).Scan(&installationID); err != nil {
		t.Fatal(err)
	}
	task, err := queryOps.ClaimQueryTaskForQuery(ctx, installationID, 1, "alert-worker", job.Authority.QueryID)
	if err != nil || task == nil {
		t.Fatalf("alert task=%+v err=%v", task, err)
	}
	claimed, err := f.operations.ClaimThresholdEvaluation(ctx, f.tenant, reserved.EvaluationID, "test-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	fired, err := f.operations.CompleteThresholdEvaluation(ctx, control.CompleteThresholdCommand{TenantID: f.tenant, EvaluationID: claimed.EvaluationID, Owner: claimed.Owner, Fence: claimed.Fence, ObservedCount: 1, DeliveryID: f.uuid(), BodyBytes: []byte(`{"version":1}`)})
	if err != nil || !fired {
		t.Fatalf("complete fired=%v err=%v", fired, err)
	}
	var deliveries int
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM deliveries WHERE tenant_id=$1 AND alert_id=$2`, f.tenant, created.AlertID).Scan(&deliveries); err != nil || deliveries != 1 {
		t.Fatalf("deliveries=%d err=%v", deliveries, err)
	}
	// A revision cancels pending work and resets window progress.
	if _, err := f.operations.UpdateAlert(ctx, control.UpdateAlertCommand{TenantID: f.tenant, ProjectID: f.project, ActorUserID: f.user, ExpectedRevision: created.Revision, AlertID: created.AlertID, Enabled: ptr(false), RequestID: f.uuid(), AuditID: f.uuid()}); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := f.pool.QueryRow(ctx, `SELECT state FROM deliveries WHERE tenant_id=$1 AND alert_id=$2`, f.tenant, created.AlertID).Scan(&state); err != nil || state != "canceled" {
		t.Fatalf("delivery state=%s err=%v", state, err)
	}
	// A window whose beginning is below the durable floor fails explicitly.
	second, err := f.operations.CreateAlert(ctx, control.CreateAlertCommand{TenantID: f.tenant, ProjectID: f.project, ActorUserID: f.user, AlertID: f.uuid(), Name: "expired", DestinationID: f.destination, Kind: "threshold", RuleBytes: rule.Bytes, RuleSHA256: rule.SHA256, CooldownSeconds: 0, RequestID: f.uuid(), AuditID: f.uuid()})
	if err != nil {
		t.Fatal(err)
	}
	firstPage, err := f.operations.ListAlertPage(ctx, f.tenant, f.project, f.user, 1, "")
	if err != nil || len(firstPage) != 2 || firstPage[0].AlertID != created.AlertID {
		t.Fatalf("first alert page=%+v err=%v", firstPage, err)
	}
	secondPage, err := f.operations.ListAlertPage(ctx, f.tenant, f.project, f.user, 1, firstPage[0].AlertID)
	if err != nil || len(secondPage) != 1 || secondPage[0].AlertID != second.AlertID {
		t.Fatalf("second alert page=%+v err=%v", secondPage, err)
	}
	loaded, err := f.operations.GetAlert(ctx, f.tenant, f.project, f.user, second.AlertID)
	if err != nil || loaded.Revision != second.Revision {
		t.Fatalf("loaded alert=%+v err=%v", loaded, err)
	}
	if _, err := f.pool.Exec(ctx, `UPDATE alerts SET first_window_end_us=$3 WHERE tenant_id=$1 AND alert_id=$2`, f.tenant, second.AlertID, end); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `UPDATE installations SET retention_floor_us=$1 WHERE singleton`, end); err != nil {
		t.Fatal(err)
	}
	expired, err := f.operations.ReserveThresholdWindow(ctx, f.tenant, second.AlertID, f.uuid())
	if err != nil || expired == nil {
		t.Fatalf("expired reserve=%+v %v", expired, err)
	}
	failed, err := f.operations.PromoteThresholdEvaluation(ctx, f.tenant, expired.EvaluationID)
	if err != nil || failed.State != "failed" || failed.ErrorCode == nil || *failed.ErrorCode != "retention_expired" {
		t.Fatalf("expired=%+v err=%v", failed, err)
	}
}

func TestIssueAlertTransitionCreatesExactlyOneDelivery(t *testing.T) {
	f := setupAlertFixture(t, 1302)
	ctx := context.Background()
	rule := testRule(t, alerts.KindIssue, `{"events":["created","regressed"]}`)
	created, err := f.operations.CreateAlert(ctx, control.CreateAlertCommand{TenantID: f.tenant, ProjectID: f.project, ActorUserID: f.user, AlertID: f.uuid(), Name: "issues", DestinationID: f.destination, Kind: "issue", RuleBytes: rule.Bytes, RuleSHA256: rule.SHA256, CooldownSeconds: 0, RequestID: f.uuid(), AuditID: f.uuid()})
	if err != nil {
		t.Fatal(err)
	}
	intent, batch, acceptance, transition := f.uuid(), f.uuid(), f.uuid(), f.uuid()
	record, issue := hashText("record"), hashText("issue")
	tx, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO object_intents(intent_id,installation_id,tenant_id,storage_generation,object_key,kind,state,owner,expires_at,expected_bytes,expected_sha256) VALUES($1,'00000000-0000-4000-8000-00000000f001',$2,1,$3,'journal','referenced','test',clock_timestamp()+interval '1 hour',1,repeat('a',64))`, []any{intent, f.tenant, "v1/alert-test/" + intent}},
		{`INSERT INTO ingest_batches(tenant_id,lane_id,batch_seq,batch_id,journal_intent_id,request_count,received_time_us,record_count,accepted_count,duplicate_count,conflict_count,journal_sha256,state) VALUES($1,0,1,$2,$3,1,100,1,1,0,0,repeat('b',64),'published')`, []any{f.tenant, batch, intent}},
		{`INSERT INTO receipts(acceptance_id,tenant_id,project_id,lane_id,batch_seq,content_sha256,request_index,selection_json,selection_sha256,ordinal_first,ordinal_last,accepted_count,duplicate_count,conflict_count,unsupported_count,received_time_us) VALUES($1,$2,$3,0,1,repeat('c',64),0,'[0]',repeat('d',64),0,0,1,0,0,0,100)`, []any{acceptance, f.tenant, f.project}},
		{`INSERT INTO issues(tenant_id,project_id,issue_id,grouping_version,fingerprint_sha256,status,revision,occurrence_count,first_event_time_us,first_event_time_ns,first_record_id,last_event_time_us,last_event_time_ns,last_record_id,last_received_time_us,title_json) VALUES($1,$2,$3,1,$3,'unresolved',1,1,10,0,$4,10,0,$4,100,'"boom"')`, []any{f.tenant, f.project, issue, record}},
		{`INSERT INTO issue_occurrences(record_id,tenant_id,project_id,issue_id,acceptance_id,lane_id,batch_seq,ordinal,event_time_us,event_time_ns,received_time_us) VALUES($1,$2,$3,$4,$5,0,1,0,10,0,100)`, []any{record, f.tenant, f.project, issue, acceptance}},
		{`INSERT INTO issue_transitions(transition_id,tenant_id,project_id,issue_id,issue_revision,type,received_time_us,record_id) VALUES($1,$2,$3,$4,1,'created',100,$5)`, []any{transition, f.tenant, f.project, issue, record}},
		{`UPDATE lanes SET accepted_seq=1,published_seq=1 WHERE tenant_id=$1 AND lane_id=0`, []any{f.tenant}},
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	pending, err := f.operations.ListPendingIssueTransitions(ctx, f.tenant, created.AlertID, 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	delivery := f.uuid()
	command := control.CommitIssueTransitionCommand{TenantID: f.tenant, AlertID: created.AlertID, TransitionID: transition, DeliveryID: delivery, ExpectedRevision: created.Revision, BodyBytes: []byte(`{"version":1}`)}
	first, err := f.operations.CommitIssueTransition(ctx, command)
	if err != nil || first != "sent" {
		t.Fatalf("first=%s err=%v", first, err)
	}
	second, err := f.operations.CommitIssueTransition(ctx, command)
	if err != nil || second != "sent" {
		t.Fatalf("retry=%s err=%v", second, err)
	}
	var ledgers, deliveries int
	if err := f.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM issue_alert_evaluations WHERE alert_id=$1),(SELECT count(*) FROM deliveries WHERE alert_id=$1)`, created.AlertID).Scan(&ledgers, &deliveries); err != nil || ledgers != 1 || deliveries != 1 {
		t.Fatalf("ledger=%d delivery=%d err=%v", ledgers, deliveries, err)
	}
}

func ptr(value bool) *bool { return &value }
func hashText(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
