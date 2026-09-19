package integration

import (
	"context"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/issues"
)

func TestIssueResolveCutBacklogNewAcceptIgnoreAndConcurrentRegression(t *testing.T) {
	fixture := setupAcceptFixture(t, 904)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	if _, err := fixture.pool.Exec(ctx, `UPDATE jobs SET state='completed',owner=NULL,lease_until=NULL WHERE state IN ('queued','running')`); err != nil {
		t.Fatal(err)
	}
	issueID := fixtureSHA("p4-lifecycle-issue")
	prepare := func(lane int, label string) control.ReceiptResult {
		acceptanceID := fixture.uuidForLane(lane)
		batch := fixture.batch(t, lane, label, []control.VerifiedRequest{fixture.request(acceptanceID, label, "", "")})
		receipts, err := control.Accept(ctx, fixture.pool, batch)
		if err != nil {
			t.Fatal(err)
		}
		job, err := control.ClaimConversionJob(ctx, fixture.pool, acceptInstallationID, 1, "lifecycle-converter-"+label, time.Minute)
		if err != nil || job == nil {
			t.Fatalf("conversion %s=%#v err=%v", label, job, err)
		}
		output := prepareFixtureOutput(t, fixture, batch, receipts[0], job, issueID)
		if err := control.Prepare(ctx, fixture.pool, output.command); err != nil {
			t.Fatal(err)
		}
		return receipts[0]
	}
	publish := func(receipt control.ReceiptResult, owner string) control.PublishCommand {
		job, err := control.ClaimPublicationJob(ctx, fixture.pool, acceptInstallationID, 1, fixture.tenantID, receipt.LaneID, owner, time.Minute)
		if err != nil || job == nil || job.BatchSeq != receipt.BatchSeq {
			t.Fatalf("publication=%#v receipt=%#v err=%v", job, receipt, err)
		}
		command := publishCommand(*job, fixture.tenantID, receipt.LaneID)
		if _, err := control.Publish(ctx, fixture.pool, command); err != nil {
			t.Fatal(err)
		}
		return command
	}

	first := prepare(0, "lifecycle-first")
	publish(first, "lifecycle-publisher-first")
	backlog := prepare(0, "lifecycle-backlog")
	resolved, err := control.ChangeIssueStatus(ctx, fixture.pool, control.IssueStatusCommand{
		TenantID: fixture.tenantID, ProjectID: fixture.projectID, IssueID: issueID, ExpectedRevision: 1, Action: control.IssueResolve,
	})
	if err != nil || !resolved.Changed || resolved.Status != issues.StatusResolved || resolved.Revision != 2 {
		t.Fatalf("resolve=%#v err=%v", resolved, err)
	}
	publish(backlog, "lifecycle-publisher-backlog")
	assertIssueState(t, ctx, fixture, issueID, issues.StatusResolved, 2, 2, 0)

	newOccurrence := prepare(0, "lifecycle-new")
	publish(newOccurrence, "lifecycle-publisher-new")
	assertIssueState(t, ctx, fixture, issueID, issues.StatusUnresolved, 3, 3, 1)
	ignored, err := control.ChangeIssueStatus(ctx, fixture.pool, control.IssueStatusCommand{
		TenantID: fixture.tenantID, ProjectID: fixture.projectID, IssueID: issueID, ExpectedRevision: 3, Action: control.IssueIgnore,
	})
	if err != nil || ignored.Status != issues.StatusIgnored || ignored.Revision != 4 {
		t.Fatalf("ignore=%#v err=%v", ignored, err)
	}
	ignoredOccurrence := prepare(0, "lifecycle-ignored")
	publish(ignoredOccurrence, "lifecycle-publisher-ignored")
	assertIssueState(t, ctx, fixture, issueID, issues.StatusIgnored, 4, 4, 1)

	reopened, err := control.ChangeIssueStatus(ctx, fixture.pool, control.IssueStatusCommand{
		TenantID: fixture.tenantID, ProjectID: fixture.projectID, IssueID: issueID, ExpectedRevision: 4, Action: control.IssueReopen,
	})
	if err != nil || reopened.Status != issues.StatusUnresolved || reopened.Revision != 5 {
		t.Fatalf("reopen=%#v err=%v", reopened, err)
	}
	resolved, err = control.ChangeIssueStatus(ctx, fixture.pool, control.IssueStatusCommand{
		TenantID: fixture.tenantID, ProjectID: fixture.projectID, IssueID: issueID, ExpectedRevision: 5, Action: control.IssueResolve,
	})
	if err != nil || resolved.Revision != 6 {
		t.Fatalf("second resolve=%#v err=%v", resolved, err)
	}
	left := prepare(2, "lifecycle-left")
	right := prepare(11, "lifecycle-right")
	commands := []control.PublishCommand{}
	for index, receipt := range []control.ReceiptResult{left, right} {
		job, err := control.ClaimPublicationJob(ctx, fixture.pool, acceptInstallationID, 1, fixture.tenantID, receipt.LaneID, "lifecycle-concurrent-"+string(rune('a'+index)), time.Minute)
		if err != nil || job == nil {
			t.Fatalf("concurrent claim=%#v err=%v", job, err)
		}
		commands = append(commands, publishCommand(*job, fixture.tenantID, receipt.LaneID))
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, command := range commands {
		go func(command control.PublishCommand) {
			<-start
			_, err := control.Publish(context.Background(), fixture.pool, command)
			results <- err
		}(command)
	}
	close(start)
	for range commands {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	assertIssueState(t, ctx, fixture, issueID, issues.StatusUnresolved, 7, 6, 2)
	if _, err := control.ChangeIssueStatus(ctx, fixture.pool, control.IssueStatusCommand{
		TenantID: fixture.tenantID, ProjectID: fixture.projectID, IssueID: issueID, ExpectedRevision: 6, Action: control.IssueIgnore,
	}); err != control.ErrIssueRevisionStale {
		t.Fatalf("stale revision=%v", err)
	}
}

func assertIssueState(t *testing.T, ctx context.Context, fixture *acceptFixture, issueID string, status issues.Status, revision, count, regressions int64) {
	t.Helper()
	var gotStatus string
	var gotRevision, gotCount, gotRegressions int64
	if err := fixture.pool.QueryRow(ctx, `SELECT status,revision,occurrence_count,
		(SELECT count(*) FROM issue_transitions WHERE issue_id=$3 AND type='regressed')
		FROM issues WHERE tenant_id=$1 AND project_id=$2 AND issue_id=$3`, fixture.tenantID, fixture.projectID, issueID).Scan(&gotStatus, &gotRevision, &gotCount, &gotRegressions); err != nil {
		t.Fatal(err)
	}
	if gotStatus != string(status) || gotRevision != revision || gotCount != count || gotRegressions != regressions {
		t.Fatalf("status=%s revision=%d count=%d regressions=%d", gotStatus, gotRevision, gotCount, gotRegressions)
	}
}
