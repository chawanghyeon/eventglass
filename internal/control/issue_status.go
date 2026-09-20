package control

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/chawanghyeon/eventglass/internal/issues"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrIssueRevisionStale = errors.New("Issue revision is stale")

type IssueAction string

const (
	IssueResolve IssueAction = "resolve"
	IssueIgnore  IssueAction = "ignore"
	IssueReopen  IssueAction = "reopen"
)

type IssueStatusCommand struct {
	TenantID         int64
	ProjectID        int64
	IssueID          string
	ExpectedRevision int64
	Action           IssueAction
	ActorUserID      *int64
	RequestID        string
	AuditID          string
	OperationID      string
}

type IssueStatusResult struct {
	Status   issues.Status
	Revision int64
	Changed  bool
}

func (operations *AuthOperations) ChangeIssueStatus(ctx context.Context, command IssueStatusCommand) (IssueStatusResult, error) {
	if operations == nil {
		return IssueStatusResult{}, errors.New("Issue operations are required")
	}
	return ChangeIssueStatus(ctx, operations.pool, command)
}

func ChangeIssueStatus(ctx context.Context, pool *pgxpool.Pool, command IssueStatusCommand) (IssueStatusResult, error) {
	if pool == nil || command.TenantID <= 0 || command.ProjectID <= 0 || !validSHA(command.IssueID) || command.ExpectedRevision <= 0 || (command.Action != IssueResolve && command.Action != IssueIgnore && command.Action != IssueReopen) {
		return IssueStatusResult{}, errors.New("invalid Issue status command")
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return IssueStatusResult{}, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = on"); err != nil {
		return IssueStatusResult{}, err
	}
	if command.ActorUserID != nil {
		if *command.ActorUserID <= 0 || command.RequestID == "" || command.AuditID == "" {
			return IssueStatusResult{}, errors.New("invalid Issue actor")
		}
		if err := requireTenantRole(ctx, tx, command.TenantID, *command.ActorUserID, false, command.ProjectID, true); err != nil {
			return IssueStatusResult{}, err
		}
		if command.OperationID != "" {
			var status issues.Status
			var revision int64
			err := tx.QueryRow(ctx, `SELECT i.status,i.revision FROM issue_transitions t JOIN issues i
				ON i.tenant_id=t.tenant_id AND i.project_id=t.project_id AND i.issue_id=t.issue_id
				WHERE t.tenant_id=$1 AND t.actor_user_id=$2 AND t.operation_id=$3 AND t.project_id=$4 AND t.issue_id=$5`,
				command.TenantID, *command.ActorUserID, command.OperationID, command.ProjectID, command.IssueID).Scan(&status, &revision)
			if err == nil {
				return IssueStatusResult{Status: status, Revision: revision}, tx.Commit(ctx)
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				return IssueStatusResult{}, err
			}
		}
	} else if _, err := tx.Exec(ctx, `SELECT project_id FROM projects WHERE tenant_id=$1 AND project_id=$2 FOR SHARE`, command.TenantID, command.ProjectID); err != nil {
		return IssueStatusResult{}, err
	}
	var cut []int64
	if command.Action == IssueResolve {
		cut, err = lockAcceptedCut(ctx, tx, command.TenantID)
		if err != nil {
			return IssueStatusResult{}, err
		}
	}
	state, err := loadIssueForStatus(ctx, tx, command)
	if err != nil {
		return IssueStatusResult{}, err
	}
	if state.Revision != command.ExpectedRevision {
		return IssueStatusResult{}, ErrIssueRevisionStale
	}
	var transition *issues.TransitionType
	switch command.Action {
	case IssueResolve:
		state, transition, err = issues.Resolve(state, cut)
	case IssueIgnore:
		state, transition, err = issues.Ignore(state)
	case IssueReopen:
		state, transition, err = issues.Reopen(state)
	}
	if err != nil {
		return IssueStatusResult{}, err
	}
	if transition == nil {
		if err := tx.Commit(ctx); err != nil {
			return IssueStatusResult{}, err
		}
		return IssueStatusResult{Status: state.Status, Revision: state.Revision}, nil
	}
	resolvedCut, err := encodeResolvedCut(state.ResolvedCut)
	if err != nil {
		return IssueStatusResult{}, err
	}
	result, err := tx.Exec(ctx, `UPDATE issues SET status=$4,revision=$5,resolved_cut=$6 WHERE tenant_id=$1 AND project_id=$2 AND issue_id=$3 AND revision=$7`, command.TenantID, command.ProjectID, command.IssueID, state.Status, state.Revision, resolvedCut, command.ExpectedRevision)
	if err != nil || result.RowsAffected() != 1 {
		return IssueStatusResult{}, errors.Join(ErrIssueRevisionStale, err)
	}
	var receivedUS int64
	if err := tx.QueryRow(ctx, `SELECT floor(extract(epoch FROM clock_timestamp())*1000000)::bigint`).Scan(&receivedUS); err != nil {
		return IssueStatusResult{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO issue_transitions(transition_id,tenant_id,project_id,issue_id,issue_revision,type,received_time_us,actor_user_id,operation_id)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, deterministicTransitionID(command.IssueID, state.Revision), command.TenantID, command.ProjectID, command.IssueID, state.Revision, *transition, receivedUS, command.ActorUserID, nullableText(command.OperationID)); err != nil {
		return IssueStatusResult{}, err
	}
	if command.ActorUserID != nil {
		if _, err := tx.Exec(ctx, `INSERT INTO audit_events(tenant_id,audit_id,actor_user_id,action,target_type,target_id,target_revision,request_id,operation_id)
			VALUES($1,$2,$3,'issue_status_changed','issue',$4,$5,$6,$7)`, command.TenantID, command.AuditID, *command.ActorUserID, command.IssueID, state.Revision, command.RequestID, nullableText(command.OperationID)); err != nil {
			return IssueStatusResult{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return IssueStatusResult{}, err
	}
	return IssueStatusResult{Status: state.Status, Revision: state.Revision, Changed: true}, nil
}

func lockAcceptedCut(ctx context.Context, tx pgx.Tx, tenantID int64) ([]int64, error) {
	rows, err := tx.Query(ctx, `SELECT lane_id,accepted_seq FROM lanes WHERE tenant_id=$1 ORDER BY lane_id FOR UPDATE`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cut := make([]int64, model.LaneCount)
	count := 0
	for rows.Next() {
		var lane int
		if err := rows.Scan(&lane, &cut[count]); err != nil || lane != count {
			return nil, errors.Join(errors.New("tenant lane topology is incomplete"), err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if count != model.LaneCount {
		return nil, errors.New("tenant lane topology is incomplete")
	}
	return cut, nil
}

func loadIssueForStatus(ctx context.Context, tx pgx.Tx, command IssueStatusCommand) (issues.State, error) {
	var state issues.State
	var firstRelease, lastRelease *string
	var titleJSON string
	var resolvedJSON []byte
	err := tx.QueryRow(ctx, `SELECT status,revision,occurrence_count,first_event_time_us,first_event_time_ns,first_record_id,first_release_json,
		last_event_time_us,last_event_time_ns,last_record_id,last_release_json,last_received_time_us,title_json,resolved_cut
		FROM issues WHERE tenant_id=$1 AND project_id=$2 AND issue_id=$3 FOR UPDATE`, command.TenantID, command.ProjectID, command.IssueID).Scan(
		&state.Status, &state.Revision, &state.OccurrenceCount, &state.First.TimeUS, &state.First.NS, &state.First.RecordID, &firstRelease,
		&state.Last.TimeUS, &state.Last.NS, &state.Last.RecordID, &lastRelease, &state.LastReceivedUS, &titleJSON, &resolvedJSON)
	if err != nil {
		return state, err
	}
	state.First.ReleaseJSON, state.Last.ReleaseJSON = firstRelease, lastRelease
	if err := json.Unmarshal([]byte(titleJSON), &state.Title); err != nil {
		return state, err
	}
	if len(resolvedJSON) != 0 {
		if err := json.Unmarshal(resolvedJSON, &state.ResolvedCut); err != nil {
			return state, err
		}
	}
	return state, nil
}
