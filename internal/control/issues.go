package control

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/chawanghyeon/eventglass/internal/issues"
	"github.com/jackc/pgx/v5"
)

type durableOccurrence struct {
	recordID          string
	projectID         int64
	acceptanceID      string
	laneID            int
	batchSeq          int64
	ordinal           int
	eventTimeUS       int64
	eventNS           uint16
	receivedTimeUS    int64
	releaseJSON       *string
	issueID           string
	groupingVersion   int
	fingerprintSHA256 string
	titleJSON         string
}

func publishIssueOccurrences(ctx context.Context, tx pgx.Tx, command PublishCommand) error {
	rows, err := tx.Query(ctx, `SELECT DISTINCT issue_id FROM job_output_occurrences WHERE output_id=$1 ORDER BY issue_id`, command.OutputID)
	if err != nil {
		return err
	}
	issueIDs := make([]string, 0)
	for rows.Next() {
		var issueID string
		if err := rows.Scan(&issueID); err != nil {
			rows.Close()
			return err
		}
		issueIDs = append(issueIDs, issueID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, issueID := range issueIDs {
		var afterTime int64
		var afterNS uint16
		afterRecord := ""
		firstPage := true
		for {
			query := `SELECT record_id,project_id,acceptance_id::text,lane_id,batch_seq,ordinal,event_time_us,event_time_ns,received_time_us,release_json,issue_id,grouping_version,fingerprint_sha256,title_json
				FROM job_output_occurrences WHERE output_id=$1 AND issue_id=$2 ORDER BY event_time_us,event_time_ns,record_id LIMIT 512`
			arguments := []any{command.OutputID, issueID}
			if !firstPage {
				query = `SELECT record_id,project_id,acceptance_id::text,lane_id,batch_seq,ordinal,event_time_us,event_time_ns,received_time_us,release_json,issue_id,grouping_version,fingerprint_sha256,title_json
					FROM job_output_occurrences WHERE output_id=$1 AND issue_id=$2 AND (event_time_us,event_time_ns,record_id)>($3,$4,$5)
					ORDER BY event_time_us,event_time_ns,record_id LIMIT 512`
				arguments = append(arguments, afterTime, afterNS, afterRecord)
			}
			pageRows, err := tx.Query(ctx, query, arguments...)
			if err != nil {
				return err
			}
			group := make([]durableOccurrence, 0, 512)
			for pageRows.Next() {
				var occurrence durableOccurrence
				if err := pageRows.Scan(&occurrence.recordID, &occurrence.projectID, &occurrence.acceptanceID, &occurrence.laneID, &occurrence.batchSeq, &occurrence.ordinal, &occurrence.eventTimeUS, &occurrence.eventNS, &occurrence.receivedTimeUS, &occurrence.releaseJSON, &occurrence.issueID, &occurrence.groupingVersion, &occurrence.fingerprintSHA256, &occurrence.titleJSON); err != nil {
					pageRows.Close()
					return err
				}
				group = append(group, occurrence)
			}
			if err := pageRows.Err(); err != nil {
				pageRows.Close()
				return err
			}
			pageRows.Close()
			if len(group) == 0 {
				break
			}
			if err := publishIssueGroup(ctx, tx, command.TenantID, group); err != nil {
				return err
			}
			last := group[len(group)-1]
			afterTime, afterNS, afterRecord = last.eventTimeUS, last.eventNS, last.recordID
			firstPage = false
			if len(group) < 512 {
				break
			}
		}
	}
	return nil
}

func publishIssueGroup(ctx context.Context, tx pgx.Tx, tenantID int64, occurrences []durableOccurrence) error {
	first := occurrences[0]
	for _, occurrence := range occurrences {
		if occurrence.issueID != first.issueID || occurrence.projectID != first.projectID || occurrence.groupingVersion != first.groupingVersion || occurrence.fingerprintSHA256 != first.fingerprintSHA256 {
			return errors.New("prepared Issue group is inconsistent")
		}
	}
	state, found, err := loadIssueState(ctx, tx, tenantID, first.projectID, first.issueID, first.groupingVersion, first.fingerprintSHA256)
	if err != nil {
		return err
	}
	start := 0
	if !found {
		title, err := decodeIssueTitle(first.titleJSON)
		if err != nil {
			return err
		}
		created, transition, err := issues.ApplyUniqueOccurrence(nil, issueOccurrence(first), title)
		if err != nil {
			return err
		}
		result, err := tx.Exec(ctx, `INSERT INTO issues(tenant_id,project_id,issue_id,grouping_version,fingerprint_sha256,status,revision,occurrence_count,
			first_event_time_us,first_event_time_ns,first_record_id,first_release_json,last_event_time_us,last_event_time_ns,last_record_id,last_release_json,last_received_time_us,title_json,resolved_cut)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,NULL) ON CONFLICT DO NOTHING`,
			tenantID, first.projectID, first.issueID, first.groupingVersion, first.fingerprintSHA256, created.Status, created.Revision, created.OccurrenceCount,
			created.First.TimeUS, created.First.NS, created.First.RecordID, created.First.ReleaseJSON, created.Last.TimeUS, created.Last.NS, created.Last.RecordID, created.Last.ReleaseJSON, created.LastReceivedUS, first.titleJSON)
		if err != nil {
			return err
		}
		if result.RowsAffected() == 1 {
			if inserted, err := insertIssueOccurrence(ctx, tx, tenantID, first); err != nil || !inserted {
				return errors.Join(errors.New("new Issue occurrence was not unique"), err)
			}
			if transition == nil {
				return errors.New("new Issue lacks created transition")
			}
			if err := insertIssueTransition(ctx, tx, tenantID, first, created.Revision, *transition); err != nil {
				return err
			}
			state, found, start = created, true, 1
		} else {
			state, found, err = loadIssueState(ctx, tx, tenantID, first.projectID, first.issueID, first.groupingVersion, first.fingerprintSHA256)
			if err != nil || !found {
				return errors.Join(errors.New("concurrent Issue creation disappeared"), err)
			}
		}
	}
	for _, occurrence := range occurrences[start:] {
		inserted, err := insertIssueOccurrence(ctx, tx, tenantID, occurrence)
		if err != nil {
			return err
		}
		if !inserted {
			continue
		}
		title, err := decodeIssueTitle(occurrence.titleJSON)
		if err != nil {
			return err
		}
		next, transition, err := issues.ApplyUniqueOccurrence(&state, issueOccurrence(occurrence), title)
		if err != nil {
			return err
		}
		state = next
		if transition != nil {
			if err := insertIssueTransition(ctx, tx, tenantID, occurrence, state.Revision, *transition); err != nil {
				return err
			}
		}
	}
	if !found {
		return errors.New("Issue state was not established")
	}
	resolvedCut, err := encodeResolvedCut(state.ResolvedCut)
	if err != nil {
		return err
	}
	titleJSON, _ := json.Marshal(state.Title)
	_, err = tx.Exec(ctx, `UPDATE issues SET status=$4,revision=$5,occurrence_count=$6,
		first_event_time_us=$7,first_event_time_ns=$8,first_record_id=$9,first_release_json=$10,
		last_event_time_us=$11,last_event_time_ns=$12,last_record_id=$13,last_release_json=$14,last_received_time_us=$15,title_json=$16,resolved_cut=$17
		WHERE tenant_id=$1 AND project_id=$2 AND issue_id=$3`, tenantID, first.projectID, first.issueID, state.Status, state.Revision, state.OccurrenceCount,
		state.First.TimeUS, state.First.NS, state.First.RecordID, state.First.ReleaseJSON, state.Last.TimeUS, state.Last.NS, state.Last.RecordID, state.Last.ReleaseJSON, state.LastReceivedUS, string(titleJSON), resolvedCut)
	return err
}

func loadIssueState(ctx context.Context, tx pgx.Tx, tenantID, projectID int64, issueID string, groupingVersion int, fingerprint string) (issues.State, bool, error) {
	var state issues.State
	var firstRelease, lastRelease *string
	var titleJSON string
	var resolvedJSON []byte
	err := tx.QueryRow(ctx, `SELECT status,revision,occurrence_count,first_event_time_us,first_event_time_ns,first_record_id,first_release_json,
		last_event_time_us,last_event_time_ns,last_record_id,last_release_json,last_received_time_us,title_json,resolved_cut
		FROM issues WHERE tenant_id=$1 AND project_id=$2 AND issue_id=$3 AND grouping_version=$4 AND fingerprint_sha256=$5 FOR UPDATE`, tenantID, projectID, issueID, groupingVersion, fingerprint).Scan(
		&state.Status, &state.Revision, &state.OccurrenceCount, &state.First.TimeUS, &state.First.NS, &state.First.RecordID, &firstRelease,
		&state.Last.TimeUS, &state.Last.NS, &state.Last.RecordID, &lastRelease, &state.LastReceivedUS, &titleJSON, &resolvedJSON)
	if errors.Is(err, pgx.ErrNoRows) {
		return issues.State{}, false, nil
	}
	if err != nil {
		return state, false, err
	}
	state.First.ReleaseJSON, state.Last.ReleaseJSON = firstRelease, lastRelease
	if err := json.Unmarshal([]byte(titleJSON), &state.Title); err != nil {
		return state, false, err
	}
	if len(resolvedJSON) != 0 {
		if err := json.Unmarshal(resolvedJSON, &state.ResolvedCut); err != nil {
			return state, false, err
		}
	}
	return state, true, nil
}

func insertIssueOccurrence(ctx context.Context, tx pgx.Tx, tenantID int64, occurrence durableOccurrence) (bool, error) {
	result, err := tx.Exec(ctx, `INSERT INTO issue_occurrences(record_id,tenant_id,project_id,issue_id,acceptance_id,lane_id,batch_seq,ordinal,event_time_us,event_time_ns,received_time_us,release_json)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT(record_id) DO NOTHING`, occurrence.recordID, tenantID, occurrence.projectID, occurrence.issueID, occurrence.acceptanceID, occurrence.laneID, occurrence.batchSeq, occurrence.ordinal, occurrence.eventTimeUS, occurrence.eventNS, occurrence.receivedTimeUS, occurrence.releaseJSON)
	return result.RowsAffected() == 1, err
}

func insertIssueTransition(ctx context.Context, tx pgx.Tx, tenantID int64, occurrence durableOccurrence, revision int64, transition issues.TransitionType) error {
	_, err := tx.Exec(ctx, `INSERT INTO issue_transitions(transition_id,tenant_id,project_id,issue_id,issue_revision,type,received_time_us,record_id)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, deterministicTransitionID(occurrence.issueID, revision), tenantID, occurrence.projectID, occurrence.issueID, revision, transition, occurrence.receivedTimeUS, occurrence.recordID)
	return err
}

func issueOccurrence(value durableOccurrence) issues.Occurrence {
	return issues.Occurrence{RecordID: value.recordID, LaneID: value.laneID, BatchSeq: value.batchSeq, EventTimeUS: value.eventTimeUS, EventNS: value.eventNS, ReceivedUS: value.receivedTimeUS, ReleaseJSON: value.releaseJSON}
}

func decodeIssueTitle(encoded string) (string, error) {
	var title string
	if err := json.Unmarshal([]byte(encoded), &title); err != nil {
		return "", err
	}
	return title, nil
}

func encodeResolvedCut(cut []int64) (*string, error) {
	if cut == nil {
		return nil, nil
	}
	encoded, err := json.Marshal(cut)
	value := string(encoded)
	return &value, err
}
