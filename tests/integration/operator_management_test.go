package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/model"
)

func TestOperatorProjectIssueOccurrenceAndSDKOutcomeManagement(t *testing.T) {
	fixture := setupAcceptFixture(t, 1601)
	ctx := context.Background()
	userID := fixture.tenantID*10 + 2
	viewerID := fixture.tenantID*10 + 3
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO users(user_id,email_normalized,password_phc) VALUES($1,$2,repeat('x',32))`, userID, fmt.Sprintf("operator-%d@example.invalid", fixture.tenantID)); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'admin')`, fixture.tenantID, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO users(user_id,email_normalized,password_phc) VALUES($1,$2,repeat('x',32))`, viewerID, fmt.Sprintf("viewer-%d@example.invalid", fixture.tenantID)); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO memberships(tenant_id,user_id,role) VALUES($1,$2,'member')`, fixture.tenantID, viewerID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO project_grants(tenant_id,project_id,user_id,role) VALUES($1,$2,$3,'viewer')`, fixture.tenantID, fixture.projectID, viewerID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = fixture.pool.Exec(context.Background(), `DELETE FROM audit_events WHERE tenant_id=$1`, fixture.tenantID)
		_, _ = fixture.pool.Exec(context.Background(), `DELETE FROM issue_transitions WHERE tenant_id=$1`, fixture.tenantID)
		_, _ = fixture.pool.Exec(context.Background(), `DELETE FROM issue_occurrences WHERE tenant_id=$1`, fixture.tenantID)
		_, _ = fixture.pool.Exec(context.Background(), `DELETE FROM issues WHERE tenant_id=$1`, fixture.tenantID)
		_, _ = fixture.pool.Exec(context.Background(), `DELETE FROM sdk_outcomes o USING receipts r WHERE o.acceptance_id=r.acceptance_id AND r.tenant_id=$1`, fixture.tenantID)
		_, _ = fixture.pool.Exec(context.Background(), `DELETE FROM project_grants WHERE tenant_id=$1 AND user_id=$2`, fixture.tenantID, viewerID)
		_, _ = fixture.pool.Exec(context.Background(), `DELETE FROM memberships WHERE tenant_id=$1 AND user_id IN ($2,$3)`, fixture.tenantID, userID, viewerID)
		_, _ = fixture.pool.Exec(context.Background(), `DELETE FROM users WHERE user_id IN ($1,$2)`, userID, viewerID)
	})
	auth, err := control.NewAuthOperations(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	projects, err := auth.ListProjectPage(ctx, control.ProjectPageCommand{TenantID: fixture.tenantID, ActorUserID: userID, Limit: 100})
	if err != nil || len(projects) != 1 || projects[0].ProjectID != fixture.projectID || len(projects[0].ScrubRules) == 0 {
		t.Fatalf("projects=%+v err=%v", projects, err)
	}
	viewerProjects, err := auth.ListProjectPage(ctx, control.ProjectPageCommand{TenantID: fixture.tenantID, ActorUserID: viewerID, Limit: 100})
	if err != nil || len(viewerProjects) != 1 || viewerProjects[0].ProjectID != fixture.projectID || viewerProjects[0].ScrubRules != nil {
		t.Fatalf("viewer projects=%+v err=%v", viewerProjects, err)
	}
	if _, err := auth.CreateProject(ctx, control.CreateProjectCommand{TenantID: fixture.tenantID, ActorUserID: viewerID, Name: "Forbidden", RequestID: fixture.uuidForLane(0), AuditID: fixture.uuidForLane(0)}); !errors.Is(err, control.ErrForbidden) {
		t.Fatalf("viewer create project err=%v", err)
	}
	created, err := auth.CreateProject(ctx, control.CreateProjectCommand{TenantID: fixture.tenantID, ActorUserID: userID, Name: "Browser", DefaultService: "web", AllowedOrigins: []string{"https://app.example.invalid"}, RequestID: fixture.uuidForLane(0), AuditID: fixture.uuidForLane(0)})
	if err != nil || created.Revision != 1 || len(created.AllowedOrigins) != 1 {
		t.Fatalf("created=%+v err=%v", created, err)
	}
	disabled := "disabled"
	updatedName := "Browser UI"
	updated, err := auth.UpdateProject(ctx, control.UpdateProjectCommand{TenantID: fixture.tenantID, ProjectID: created.ProjectID, ActorUserID: userID, ExpectedRevision: created.Revision, Name: &updatedName, State: &disabled, RequestID: fixture.uuidForLane(0), AuditID: fixture.uuidForLane(0)})
	if err != nil || updated.State != disabled || updated.Revision != 2 || updated.AuthRevision != 2 {
		t.Fatalf("updated=%+v err=%v", updated, err)
	}

	acceptanceID := fixture.uuidForLane(0)
	request := fixture.request(acceptanceID, "operator", "", "", model.Outcome{ItemOrdinal: 0, Category: "error", Reason: "queue_overflow", Quantity: 7, Approximate: true})
	batch := fixture.batch(t, 0, "operator", []control.VerifiedRequest{request})
	receipts, err := control.Accept(ctx, fixture.pool, batch)
	if err != nil || len(receipts) != 1 {
		t.Fatalf("accept=%+v err=%v", receipts, err)
	}
	recordID := request.Candidates[0].RecordID
	issueID := fixtureSHA("operator-issue")
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO issues(tenant_id,project_id,issue_id,grouping_version,fingerprint_sha256,status,revision,occurrence_count,
		first_event_time_us,first_event_time_ns,first_record_id,last_event_time_us,last_event_time_ns,last_record_id,last_received_time_us,title_json)
		VALUES($1,$2,$3,1,$3,'unresolved',1,1,100,7,$4,100,7,$4,200,'"<script>safe issue</script>"')`, fixture.tenantID, fixture.projectID, issueID, recordID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO issue_occurrences(record_id,tenant_id,project_id,issue_id,acceptance_id,lane_id,batch_seq,ordinal,event_time_us,event_time_ns,received_time_us)
		VALUES($4,$1,$2,$3,$5,$6,$7,0,100,7,200)`, fixture.tenantID, fixture.projectID, issueID, recordID, acceptanceID, receipts[0].LaneID, receipts[0].BatchSeq); err != nil {
		t.Fatal(err)
	}
	issues, err := auth.ListIssuePage(ctx, control.IssuePageCommand{TenantID: fixture.tenantID, ActorUserID: userID, ProjectIDs: []int64{fixture.projectID}, Limit: 100})
	if err != nil || len(issues) != 1 || issues[0].Title != "<script>safe issue</script>" {
		t.Fatalf("issues=%+v err=%v", issues, err)
	}
	occurrences, err := auth.ListOccurrencePage(ctx, control.OccurrencePageCommand{TenantID: fixture.tenantID, ProjectID: fixture.projectID, ActorUserID: userID, IssueID: issueID, Limit: 100})
	if err != nil || len(occurrences) != 1 || occurrences[0].RecordID != recordID {
		t.Fatalf("occurrences=%+v err=%v", occurrences, err)
	}
	operationID := fixture.uuidForLane(0)
	changed, err := auth.ChangeIssueStatus(ctx, control.IssueStatusCommand{TenantID: fixture.tenantID, ProjectID: fixture.projectID, IssueID: issueID, ExpectedRevision: 1, Action: control.IssueIgnore, ActorUserID: &userID, RequestID: fixture.uuidForLane(0), AuditID: fixture.uuidForLane(0), OperationID: operationID})
	if err != nil || changed.Status != "ignored" || changed.Revision != 2 {
		t.Fatalf("changed=%+v err=%v", changed, err)
	}
	replayed, err := auth.ChangeIssueStatus(ctx, control.IssueStatusCommand{TenantID: fixture.tenantID, ProjectID: fixture.projectID, IssueID: issueID, ExpectedRevision: 1, Action: control.IssueIgnore, ActorUserID: &userID, RequestID: fixture.uuidForLane(0), AuditID: fixture.uuidForLane(0), OperationID: operationID})
	if err != nil || replayed.Status != "ignored" || replayed.Revision != 2 {
		t.Fatalf("replayed=%+v err=%v", replayed, err)
	}
	if _, err := auth.ChangeIssueStatus(ctx, control.IssueStatusCommand{TenantID: fixture.tenantID, ProjectID: fixture.projectID, IssueID: issueID, ExpectedRevision: 2, Action: control.IssueResolve, ActorUserID: &viewerID, RequestID: fixture.uuidForLane(0), AuditID: fixture.uuidForLane(0), OperationID: fixture.uuidForLane(0)}); !errors.Is(err, control.ErrForbidden) {
		t.Fatalf("viewer issue transition err=%v", err)
	}
	outcomes, err := auth.ListSDKOutcomePage(ctx, control.SDKOutcomePageCommand{TenantID: fixture.tenantID, ActorUserID: userID, StartUS: receipts[0].ReceivedTimeUS - 1, EndUS: receipts[0].ReceivedTimeUS + 1, Limit: 100})
	if err != nil || len(outcomes) != 1 || outcomes[0].Count != 7 || outcomes[0].Category != "error" || !outcomes[0].Approximate {
		t.Fatalf("outcomes=%+v err=%v", outcomes, err)
	}
}
