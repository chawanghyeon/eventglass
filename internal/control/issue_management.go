package control

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	"github.com/jackc/pgx/v5"
)

type ManagedIssuePoint struct {
	EventUS  int64
	NS       int
	RecordID string
	Release  *string
}

type ManagedIssue struct {
	TenantID, ProjectID, Revision, OccurrenceCount int64
	IssueID, Status, Title                         string
	GroupingVersion                                int
	First, Last                                    ManagedIssuePoint
	LastReceivedUS, DetailRetentionFloorUS         int64
}

type IssuePageCommand struct {
	TenantID, ActorUserID int64
	ProjectIDs            []int64
	Status                string
	Limit                 int
	BeforeReceivedUS      *int64
	BeforeIssueID         string
}

func (operations *AuthOperations) ListIssuePage(ctx context.Context, command IssuePageCommand) ([]ManagedIssue, error) {
	if command.TenantID <= 0 || command.ActorUserID <= 0 || len(command.ProjectIDs) == 0 || len(command.ProjectIDs) > 1000 || command.Limit < 1 || command.Limit > 1000 || command.Status != "" && command.Status != "unresolved" && command.Status != "resolved" && command.Status != "ignored" || (command.BeforeReceivedUS == nil) != (command.BeforeIssueID == "") || command.BeforeIssueID != "" && !validSHA(command.BeforeIssueID) {
		return nil, errors.New("invalid Issue page")
	}
	projects := append([]int64(nil), command.ProjectIDs...)
	sort.Slice(projects, func(i, j int) bool { return projects[i] < projects[j] })
	for index, projectID := range projects {
		if projectID <= 0 || index > 0 && projects[index-1] == projectID {
			return nil, errors.New("invalid Issue projects")
		}
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	for _, projectID := range projects {
		if _, err := tenantAccess(ctx, tx, command.TenantID, command.ActorUserID, projectID, false); err != nil {
			return nil, err
		}
	}
	rows, err := tx.Query(ctx, `SELECT x.tenant_id,x.project_id,x.issue_id,x.status,x.revision,x.title_json,x.grouping_version,x.occurrence_count,
		x.first_event_time_us,x.first_event_time_ns,x.first_record_id,x.first_release_json,
		x.last_event_time_us,x.last_event_time_ns,x.last_record_id,x.last_release_json,x.last_received_time_us,i.retention_floor_us
		FROM issues x CROSS JOIN installations i WHERE i.singleton AND x.tenant_id=$1 AND x.project_id=ANY($2)
		AND ($3='' OR x.status=$3) AND ($4::bigint IS NULL OR (x.last_received_time_us,x.issue_id)<($4,NULLIF($5,'')))
		ORDER BY x.last_received_time_us DESC,x.issue_id DESC LIMIT $6`, command.TenantID, projects, command.Status, command.BeforeReceivedUS, command.BeforeIssueID, command.Limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]ManagedIssue, 0, command.Limit+1)
	for rows.Next() {
		value, err := scanManagedIssue(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

func (operations *AuthOperations) GetIssue(ctx context.Context, tenantID, projectID, actorUserID int64, issueID string) (ManagedIssue, error) {
	if tenantID <= 0 || projectID <= 0 || actorUserID <= 0 || !validSHA(issueID) {
		return ManagedIssue{}, errors.New("invalid Issue lookup")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return ManagedIssue{}, err
	}
	defer tx.Rollback(ctx)
	if _, err := tenantAccess(ctx, tx, tenantID, actorUserID, projectID, false); err != nil {
		return ManagedIssue{}, err
	}
	row := tx.QueryRow(ctx, `SELECT x.tenant_id,x.project_id,x.issue_id,x.status,x.revision,x.title_json,x.grouping_version,x.occurrence_count,
		x.first_event_time_us,x.first_event_time_ns,x.first_record_id,x.first_release_json,
		x.last_event_time_us,x.last_event_time_ns,x.last_record_id,x.last_release_json,x.last_received_time_us,i.retention_floor_us
		FROM issues x CROSS JOIN installations i WHERE i.singleton AND x.tenant_id=$1 AND x.project_id=$2 AND x.issue_id=$3`, tenantID, projectID, issueID)
	value, err := scanManagedIssue(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return ManagedIssue{}, ErrForbidden
	}
	if err != nil {
		return ManagedIssue{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ManagedIssue{}, err
	}
	return value, nil
}

type issueScanner interface{ Scan(...any) error }

func scanManagedIssue(scanner issueScanner) (ManagedIssue, error) {
	var value ManagedIssue
	var titleJSON string
	var firstReleaseJSON, lastReleaseJSON *string
	err := scanner.Scan(&value.TenantID, &value.ProjectID, &value.IssueID, &value.Status, &value.Revision, &titleJSON, &value.GroupingVersion, &value.OccurrenceCount,
		&value.First.EventUS, &value.First.NS, &value.First.RecordID, &firstReleaseJSON,
		&value.Last.EventUS, &value.Last.NS, &value.Last.RecordID, &lastReleaseJSON, &value.LastReceivedUS, &value.DetailRetentionFloorUS)
	if err != nil {
		return value, err
	}
	if err := json.Unmarshal([]byte(titleJSON), &value.Title); err != nil {
		return value, err
	}
	if firstReleaseJSON != nil {
		if err := json.Unmarshal([]byte(*firstReleaseJSON), &value.First.Release); err != nil {
			return value, err
		}
	}
	if lastReleaseJSON != nil {
		if err := json.Unmarshal([]byte(*lastReleaseJSON), &value.Last.Release); err != nil {
			return value, err
		}
	}
	return value, nil
}

type ManagedOccurrence struct {
	RecordID            string
	ProjectID           int64
	EventUS, ReceivedUS int64
	NS                  int
	Release             *string
	DetailAvailable     bool
}

type OccurrencePageCommand struct {
	TenantID, ProjectID, ActorUserID int64
	IssueID                          string
	Limit                            int
	BeforeEventUS                    *int64
	BeforeNS                         *int
	BeforeRecordID                   string
}

func (operations *AuthOperations) ListOccurrencePage(ctx context.Context, command OccurrencePageCommand) ([]ManagedOccurrence, error) {
	if command.TenantID <= 0 || command.ProjectID <= 0 || command.ActorUserID <= 0 || !validSHA(command.IssueID) || command.Limit < 1 || command.Limit > 1000 || (command.BeforeEventUS == nil) != (command.BeforeNS == nil) || (command.BeforeEventUS == nil) != (command.BeforeRecordID == "") || command.BeforeRecordID != "" && !validSHA(command.BeforeRecordID) {
		return nil, errors.New("invalid occurrence page")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err := tenantAccess(ctx, tx, command.TenantID, command.ActorUserID, command.ProjectID, false); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `SELECT o.record_id,o.project_id,o.event_time_us,o.event_time_ns,o.received_time_us,o.release_json,
		o.received_time_us>=i.retention_floor_us FROM issue_occurrences o CROSS JOIN installations i
		WHERE i.singleton AND o.tenant_id=$1 AND o.project_id=$2 AND o.issue_id=$3
		AND ($4::bigint IS NULL OR (o.event_time_us,o.event_time_ns,o.record_id)<($4,$5,NULLIF($6,'')))
		ORDER BY o.event_time_us DESC,o.event_time_ns DESC,o.record_id DESC LIMIT $7`, command.TenantID, command.ProjectID, command.IssueID, command.BeforeEventUS, command.BeforeNS, command.BeforeRecordID, command.Limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]ManagedOccurrence, 0, command.Limit+1)
	for rows.Next() {
		var value ManagedOccurrence
		var releaseJSON *string
		if err := rows.Scan(&value.RecordID, &value.ProjectID, &value.EventUS, &value.NS, &value.ReceivedUS, &releaseJSON, &value.DetailAvailable); err != nil {
			return nil, err
		}
		if releaseJSON != nil {
			if err := json.Unmarshal([]byte(*releaseJSON), &value.Release); err != nil {
				return nil, err
			}
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}
