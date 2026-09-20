package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/jackc/pgx/v5"
)

var ErrAlertLeaseLost = errors.New("alert evaluation lease lost")

type ThresholdEvaluation struct {
	TenantID                   int64
	AlertID                    string
	AlertRevision              int64
	EvaluationID               string
	WindowStartUS, WindowEndUS int64
	Cut                        [model.LaneCount]int64
	State                      string
	Fence                      int64
	Attempt                    int
	Owner                      string
	LeaseUntil                 *time.Time
	SnapshotID                 *string
	ObservedCount              *int64
	Fired                      *bool
	ErrorCode                  *string
}

func (operations *AlertOperations) ListOpenThresholdEvaluations(ctx context.Context, tenantID int64, alertID string, limit int) ([]ThresholdEvaluation, error) {
	if limit < 1 || limit > 10 {
		return nil, errors.New("invalid evaluation limit")
	}
	rows, err := operations.pool.Query(ctx, `SELECT tenant_id,alert_id::text,alert_revision,evaluation_id::text,window_start_us,window_end_us,cut,state,fence,attempt,coalesce(owner,''),lease_until,snapshot_id::text,observed_count,fired,error_code FROM alert_evaluations WHERE tenant_id=$1 AND alert_id=$2 AND state IN ('waiting','queued','running') ORDER BY window_end_us LIMIT $3`, tenantID, alertID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []ThresholdEvaluation{}
	for rows.Next() {
		var value ThresholdEvaluation
		var cut []int64
		if err := rows.Scan(&value.TenantID, &value.AlertID, &value.AlertRevision, &value.EvaluationID, &value.WindowStartUS, &value.WindowEndUS, &cut, &value.State, &value.Fence, &value.Attempt, &value.Owner, &value.LeaseUntil, &value.SnapshotID, &value.ObservedCount, &value.Fired, &value.ErrorCode); err != nil {
			return nil, err
		}
		if len(cut) != model.LaneCount {
			return nil, errors.New("stored evaluation cut is invalid")
		}
		copy(value.Cut[:], cut)
		result = append(result, value)
	}
	return result, rows.Err()
}

func (operations *AlertOperations) FailThresholdEvaluation(ctx context.Context, tenantID int64, evaluationID, owner string, fence int64, code string) error {
	if code == "" || len(code) > 64 {
		return errors.New("invalid evaluation failure")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var snapshotID *string
	err = tx.QueryRow(ctx, `UPDATE alert_evaluations SET state='failed',owner=NULL,lease_until=NULL,error_code=$5,updated_at=clock_timestamp() WHERE tenant_id=$1 AND evaluation_id=$2 AND state='running' AND owner=$3 AND fence=$4 AND lease_until>clock_timestamp() RETURNING snapshot_id::text`, tenantID, evaluationID, owner, fence, code).Scan(&snapshotID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrAlertLeaseLost
	}
	if err != nil {
		return err
	}
	if snapshotID != nil {
		if _, err := tx.Exec(ctx, `UPDATE query_snapshots SET state='released',expires_at=LEAST(expires_at,clock_timestamp()) WHERE tenant_id=$1 AND snapshot_id=$2`, tenantID, *snapshotID); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// ReserveThresholdWindow serializes with Accept on all lane rows, captures an
// accepted cut, and raises the received-time barrier before any later Accept.
func (operations *AlertOperations) ReserveThresholdWindow(ctx context.Context, tenantID int64, alertID, evaluationID string) (*ThresholdEvaluation, error) {
	if tenantID <= 0 || alertID == "" || evaluationID == "" {
		return nil, errors.New("invalid threshold reservation")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var projectID int64
	err = tx.QueryRow(ctx, `SELECT project_id FROM alerts WHERE tenant_id=$1 AND alert_id=$2 AND kind='threshold'`, tenantID, alertID).Scan(&projectID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrForbidden
	}
	if err != nil {
		return nil, err
	}
	var paused bool
	if err := tx.QueryRow(ctx, `SELECT alerts_paused FROM installations WHERE singleton FOR SHARE`).Scan(&paused); err != nil {
		return nil, err
	}
	var projectState string
	if err := tx.QueryRow(ctx, `SELECT state FROM projects WHERE tenant_id=$1 AND project_id=$2 FOR SHARE`, tenantID, projectID).Scan(&projectState); err != nil {
		return nil, err
	}
	if projectState != "active" {
		return nil, ErrForbidden
	}
	rows, err := tx.Query(ctx, `SELECT lane_id,accepted_seq,last_received_time_us FROM lanes WHERE tenant_id=$1 ORDER BY lane_id FOR UPDATE`, tenantID)
	if err != nil {
		return nil, err
	}
	var cut [model.LaneCount]int64
	var lastReceived [model.LaneCount]int64
	index := 0
	for rows.Next() {
		var lane int
		if index >= model.LaneCount || rows.Scan(&lane, &cut[index], &lastReceived[index]) != nil || lane != index {
			rows.Close()
			return nil, errors.New("tenant lane topology is invalid")
		}
		index++
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	if index != model.LaneCount {
		return nil, errors.New("tenant lane topology is incomplete")
	}
	var revision int64
	var enabled bool
	var ruleBytes []byte
	var firstEnd int64
	var lastCompleted *int64
	if err := tx.QueryRow(ctx, `SELECT revision,enabled,rule_bytes,first_window_end_us,last_completed_end_us FROM alerts WHERE tenant_id=$1 AND alert_id=$2 AND kind='threshold' FOR UPDATE`, tenantID, alertID).Scan(&revision, &enabled, &ruleBytes, &firstEnd, &lastCompleted); err != nil {
		return nil, err
	}
	if !enabled || paused {
		return nil, nil
	}
	var rule struct {
		WindowSeconds int `json:"window_seconds"`
	}
	if json.Unmarshal(ruleBytes, &rule) != nil || rule.WindowSeconds <= 0 {
		return nil, errors.New("stored threshold rule is invalid")
	}
	var maxReserved *int64
	var pending int
	if err := tx.QueryRow(ctx, `SELECT max(window_end_us),count(*) FILTER (WHERE state IN ('waiting','queued','running')) FROM alert_evaluations WHERE tenant_id=$1 AND alert_id=$2 AND alert_revision=$3`, tenantID, alertID, revision).Scan(&maxReserved, &pending); err != nil {
		return nil, err
	}
	if pending >= 10 {
		return nil, nil
	}
	end := firstEnd
	if lastCompleted != nil {
		end = *lastCompleted + 60_000_000
	}
	if maxReserved != nil && *maxReserved >= end {
		end = *maxReserved + 60_000_000
	}
	var nowUS int64
	if err := tx.QueryRow(ctx, `SELECT floor(extract(epoch FROM clock_timestamp())*1000000)::bigint`).Scan(&nowUS); err != nil {
		return nil, err
	}
	if end > nowUS {
		return nil, nil
	}
	for lane := 0; lane < model.LaneCount; lane++ {
		if lastReceived[lane] < end {
			if _, err := tx.Exec(ctx, `UPDATE lanes SET last_received_time_us=$3 WHERE tenant_id=$1 AND lane_id=$2`, tenantID, lane, end); err != nil {
				return nil, err
			}
		}
	}
	result := &ThresholdEvaluation{TenantID: tenantID, AlertID: alertID, AlertRevision: revision, EvaluationID: evaluationID, WindowEndUS: end, WindowStartUS: end - int64(rule.WindowSeconds)*1_000_000, Cut: cut, State: "waiting"}
	_, err = tx.Exec(ctx, `INSERT INTO alert_evaluations(tenant_id,alert_id,alert_revision,evaluation_id,window_start_us,window_end_us,cut,state) VALUES($1,$2,$3,$4,$5,$6,$7,'waiting') ON CONFLICT(alert_id,alert_revision,window_end_us) DO NOTHING`, tenantID, alertID, revision, evaluationID, result.WindowStartUS, end, cut[:])
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

// PromoteThresholdEvaluation distinguishes publication backlog from an empty
// result. It queues only after every lane reaches the reserved accepted cut.
func (operations *AlertOperations) PromoteThresholdEvaluation(ctx context.Context, tenantID int64, evaluationID string) (*ThresholdEvaluation, error) {
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	observed, err := loadThresholdEvaluation(ctx, tx, tenantID, evaluationID, false)
	if err != nil {
		return nil, err
	}
	var projectID int64
	if err := tx.QueryRow(ctx, `SELECT project_id FROM alerts WHERE tenant_id=$1 AND alert_id=$2`, tenantID, observed.AlertID).Scan(&projectID); err != nil {
		return nil, err
	}
	var retentionFloor int64
	if err := tx.QueryRow(ctx, `SELECT retention_floor_us FROM installations WHERE singleton FOR SHARE`).Scan(&retentionFloor); err != nil {
		return nil, err
	}
	var projectState string
	if err := tx.QueryRow(ctx, `SELECT state FROM projects WHERE tenant_id=$1 AND project_id=$2 FOR SHARE`, tenantID, projectID).Scan(&projectState); err != nil {
		return nil, err
	}
	if projectState != "active" {
		return nil, ErrForbidden
	}
	rows, err := tx.Query(ctx, `SELECT lane_id,published_seq FROM lanes WHERE tenant_id=$1 ORDER BY lane_id FOR SHARE`, tenantID)
	if err != nil {
		return nil, err
	}
	var published [model.LaneCount]int64
	index := 0
	for rows.Next() {
		var lane int
		if index >= model.LaneCount || rows.Scan(&lane, &published[index]) != nil || lane != index {
			rows.Close()
			return nil, errors.New("tenant lane topology is invalid")
		}
		index++
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	if index != model.LaneCount {
		return nil, errors.New("tenant lane topology is incomplete")
	}
	var currentRevision int64
	var enabled bool
	if err := tx.QueryRow(ctx, `SELECT revision,enabled FROM alerts WHERE tenant_id=$1 AND alert_id=$2 FOR SHARE`, tenantID, observed.AlertID).Scan(&currentRevision, &enabled); err != nil {
		return nil, err
	}
	value, err := loadThresholdEvaluation(ctx, tx, tenantID, evaluationID, true)
	if err != nil {
		return nil, err
	}
	if value.State != "waiting" {
		return &value, tx.Commit(ctx)
	}
	if currentRevision != value.AlertRevision || !enabled {
		if _, err := tx.Exec(ctx, `UPDATE alert_evaluations SET state='canceled',error_code='alert_revised',updated_at=clock_timestamp() WHERE tenant_id=$1 AND evaluation_id=$2`, tenantID, evaluationID); err != nil {
			return nil, err
		}
		value.State = "canceled"
		code := "alert_revised"
		value.ErrorCode = &code
		return &value, tx.Commit(ctx)
	}
	if retentionFloor > value.WindowStartUS {
		if _, err := tx.Exec(ctx, `UPDATE alert_evaluations SET state='failed',error_code='retention_expired',updated_at=clock_timestamp() WHERE tenant_id=$1 AND evaluation_id=$2`, tenantID, evaluationID); err != nil {
			return nil, err
		}
		value.State = "failed"
		code := "retention_expired"
		value.ErrorCode = &code
		return &value, tx.Commit(ctx)
	}
	ready := true
	for lane := range published {
		ready = ready && published[lane] >= value.Cut[lane]
	}
	if !ready {
		return &value, tx.Commit(ctx)
	}
	if _, err := tx.Exec(ctx, `UPDATE alert_evaluations SET state='queued',updated_at=clock_timestamp() WHERE tenant_id=$1 AND evaluation_id=$2 AND state='waiting'`, tenantID, evaluationID); err != nil {
		return nil, err
	}
	value.State = "queued"
	return &value, tx.Commit(ctx)
}

func (operations *AlertOperations) ClaimThresholdEvaluation(ctx context.Context, tenantID int64, evaluationID, owner string, lease time.Duration) (ThresholdEvaluation, error) {
	if owner == "" || lease <= 0 || lease > time.Minute {
		return ThresholdEvaluation{}, errors.New("invalid threshold claim")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return ThresholdEvaluation{}, err
	}
	defer tx.Rollback(ctx)
	value, err := loadThresholdEvaluation(ctx, tx, tenantID, evaluationID, true)
	if err != nil {
		return value, err
	}
	var databaseNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
		return value, err
	}
	if value.State != "queued" && !(value.State == "running" && value.LeaseUntil != nil && !value.LeaseUntil.After(databaseNow)) {
		return value, ErrAlertLeaseLost
	}
	err = tx.QueryRow(ctx, `UPDATE alert_evaluations SET state='running',owner=$3,fence=fence+1,attempt=attempt+1,lease_until=clock_timestamp()+($4::bigint*interval '1 microsecond'),updated_at=clock_timestamp() WHERE tenant_id=$1 AND evaluation_id=$2 RETURNING fence,attempt,lease_until`, tenantID, evaluationID, owner, lease.Microseconds()).Scan(&value.Fence, &value.Attempt, &value.LeaseUntil)
	if err != nil {
		return value, err
	}
	value.State, value.Owner = "running", owner
	if err := tx.Commit(ctx); err != nil {
		return value, err
	}
	return value, nil
}

func (operations *AlertOperations) HeartbeatThresholdEvaluation(ctx context.Context, tenantID int64, evaluationID, owner string, fence int64, lease time.Duration) error {
	if tenantID <= 0 || evaluationID == "" || owner == "" || fence <= 0 || lease <= 0 || lease > time.Minute {
		return errors.New("invalid threshold heartbeat")
	}
	result, err := operations.pool.Exec(ctx, `UPDATE alert_evaluations SET lease_until=clock_timestamp()+($5::bigint*interval '1 microsecond'),updated_at=clock_timestamp() WHERE tenant_id=$1 AND evaluation_id=$2 AND state='running' AND owner=$3 AND fence=$4 AND lease_until>clock_timestamp()`, tenantID, evaluationID, owner, fence, lease.Microseconds())
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return ErrAlertLeaseLost
	}
	return nil
}

type CompleteThresholdCommand struct {
	TenantID                                    int64
	EvaluationID, Owner, DeliveryID, PublicLink string
	Fence                                       int64
	ObservedCount                               int64
	BodyBytes                                   []byte
}

func (operations *AlertOperations) CompleteThresholdEvaluation(ctx context.Context, command CompleteThresholdCommand) (bool, error) {
	if command.TenantID <= 0 || command.EvaluationID == "" || command.Owner == "" || command.Fence <= 0 || command.ObservedCount < 0 {
		return false, errors.New("invalid threshold completion")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var alertID string
	if err := tx.QueryRow(ctx, `SELECT alert_id::text FROM alert_evaluations WHERE tenant_id=$1 AND evaluation_id=$2`, command.TenantID, command.EvaluationID).Scan(&alertID); err != nil {
		return false, err
	}
	var revision, projectID, firstWindowEnd int64
	var enabled bool
	var ruleBytes []byte
	var cooldown int
	var previousCompleted, previousFired *int64
	var destination AlertDestination
	err = tx.QueryRow(ctx, `SELECT a.revision,a.project_id,a.enabled,a.rule_bytes,a.cooldown_seconds,a.first_window_end_us,a.last_completed_end_us,a.last_fired_end_us,
		d.destination_id::text,d.url,d.revision,coalesce(d.secret_ciphertext,''::bytea),coalesce(d.encryption_key_id,''),d.enabled
		FROM alerts a JOIN alert_destinations d ON d.tenant_id=a.tenant_id AND d.destination_id=a.destination_id WHERE a.tenant_id=$1 AND a.alert_id=$2 FOR UPDATE OF a,d`, command.TenantID, alertID).Scan(&revision, &projectID, &enabled, &ruleBytes, &cooldown, &firstWindowEnd, &previousCompleted, &previousFired, &destination.DestinationID, &destination.URL, &destination.Revision, &destination.Secret, &destination.EncryptionKeyID, &destination.Enabled)
	if err != nil {
		return false, err
	}
	value, err := loadThresholdEvaluation(ctx, tx, command.TenantID, command.EvaluationID, true)
	if err != nil {
		return false, err
	}
	var leaseLive bool
	if err := tx.QueryRow(ctx, `SELECT lease_until>clock_timestamp() FROM alert_evaluations WHERE tenant_id=$1 AND evaluation_id=$2`, command.TenantID, command.EvaluationID).Scan(&leaseLive); err != nil {
		return false, err
	}
	if value.State != "running" || value.Owner != command.Owner || value.Fence != command.Fence || value.LeaseUntil == nil || !leaseLive {
		return false, ErrAlertLeaseLost
	}
	if revision != value.AlertRevision || !enabled {
		_, err = tx.Exec(ctx, `UPDATE alert_evaluations SET state='canceled',owner=NULL,lease_until=NULL,error_code='alert_revised',updated_at=clock_timestamp() WHERE tenant_id=$1 AND evaluation_id=$2`, command.TenantID, command.EvaluationID)
		if err == nil {
			err = tx.Commit(ctx)
		}
		return false, err
	}
	var rule struct{ Operator, Threshold string }
	if json.Unmarshal(ruleBytes, &rule) != nil {
		return false, errors.New("stored threshold rule is invalid")
	}
	threshold, err := strconv.ParseInt(rule.Threshold, 10, 64)
	if err != nil {
		return false, err
	}
	matched := compareThreshold(command.ObservedCount, rule.Operator, threshold)
	expected := firstWindowEnd
	if previousCompleted != nil {
		expected = *previousCompleted + 60_000_000
	}
	if expected != value.WindowEndUS {
		return false, errors.New("threshold evaluation completed out of order")
	}
	fired := matched && (previousFired == nil || value.WindowEndUS-*previousFired >= int64(cooldown)*1_000_000) && destination.Enabled
	if fired {
		if command.DeliveryID == "" || len(command.BodyBytes) < 2 || len(command.BodyBytes) > 64<<10 {
			return false, errors.New("threshold delivery body is required")
		}
		digest := sha256.Sum256(command.BodyBytes)
		dedupe := "threshold:" + value.AlertID + ":" + strconv.FormatInt(value.AlertRevision, 10) + ":" + strconv.FormatInt(value.WindowEndUS, 10)
		_, err = tx.Exec(ctx, `INSERT INTO deliveries(tenant_id,project_id,delivery_id,alert_id,alert_revision,destination_id,destination_revision,destination_url,destination_secret_ciphertext,destination_encryption_key_id,dedupe_key,body_bytes,body_sha256)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, command.TenantID, projectID, command.DeliveryID, value.AlertID, value.AlertRevision, destination.DestinationID, destination.Revision, destination.URL, nullableBytes(destination.Secret), nullableText(destination.EncryptionKeyID), dedupe, command.BodyBytes, hex.EncodeToString(digest[:]))
		if err != nil {
			return false, err
		}
	}
	_, err = tx.Exec(ctx, `UPDATE alert_evaluations SET state='succeeded',owner=NULL,lease_until=NULL,observed_count=$3,fired=$4,error_code=NULL,updated_at=clock_timestamp() WHERE tenant_id=$1 AND evaluation_id=$2`, command.TenantID, command.EvaluationID, command.ObservedCount, fired)
	if err != nil {
		return false, err
	}
	_, err = tx.Exec(ctx, `UPDATE alerts SET last_completed_end_us=$3,last_fired_end_us=CASE WHEN $4 THEN $3 ELSE last_fired_end_us END,updated_at=clock_timestamp() WHERE tenant_id=$1 AND alert_id=$2`, command.TenantID, value.AlertID, value.WindowEndUS, fired)
	if err != nil {
		return false, err
	}
	if value.SnapshotID != nil {
		if _, err := tx.Exec(ctx, `UPDATE query_snapshots SET state='released',expires_at=LEAST(expires_at,clock_timestamp()) WHERE tenant_id=$1 AND snapshot_id=$2`, command.TenantID, *value.SnapshotID); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return fired, nil
}

type PendingIssueTransition struct {
	TenantID, ProjectID                int64
	AlertID                            string
	AlertRevision                      int64
	TransitionID, IssueID, Type, Title string
	IssueRevision, ReceivedUS          int64
}

func (operations *AlertOperations) ListPendingIssueTransitions(ctx context.Context, tenantID int64, alertID string, limit int) ([]PendingIssueTransition, error) {
	if limit < 1 || limit > 100 {
		return nil, errors.New("invalid issue transition limit")
	}
	rows, err := operations.pool.Query(ctx, `SELECT a.tenant_id,a.project_id,a.alert_id::text,a.revision,t.transition_id::text,t.issue_id,t.type,t.issue_revision,t.received_time_us,i.title_json
	FROM alerts a JOIN issue_transitions t ON t.tenant_id=a.tenant_id AND t.project_id=a.project_id JOIN issue_occurrences o ON o.record_id=t.record_id JOIN issues i ON i.tenant_id=t.tenant_id AND i.project_id=t.project_id AND i.issue_id=t.issue_id
	WHERE a.tenant_id=$1 AND a.alert_id=$2 AND a.kind='issue' AND t.type IN ('created','regressed') AND (convert_from(a.rule_bytes,'UTF8')::jsonb->'events') ? t.type AND o.batch_seq>a.enabled_from_public_cut[o.lane_id+1]
	AND NOT EXISTS(SELECT 1 FROM issue_alert_evaluations e WHERE e.alert_id=a.alert_id AND e.alert_revision=a.revision AND e.transition_id=t.transition_id)
	ORDER BY o.lane_id,o.batch_seq,t.transition_id LIMIT $3`, tenantID, alertID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []PendingIssueTransition{}
	for rows.Next() {
		var value PendingIssueTransition
		var titleJSON string
		if err := rows.Scan(&value.TenantID, &value.ProjectID, &value.AlertID, &value.AlertRevision, &value.TransitionID, &value.IssueID, &value.Type, &value.IssueRevision, &value.ReceivedUS, &titleJSON); err != nil {
			return nil, err
		}
		if json.Unmarshal([]byte(titleJSON), &value.Title) != nil {
			return nil, errors.New("stored Issue title is invalid")
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

type CommitIssueTransitionCommand struct {
	TenantID                          int64
	AlertID, TransitionID, DeliveryID string
	ExpectedRevision                  int64
	BodyBytes                         []byte
}

func (operations *AlertOperations) CommitIssueTransition(ctx context.Context, command CommitIssueTransitionCommand) (string, error) {
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	var projectID, revision int64
	var enabled, destinationEnabled bool
	var ruleBytes []byte
	var cooldown int
	var cut []int64
	var previousFired *int64
	var destination AlertDestination
	err = tx.QueryRow(ctx, `SELECT a.project_id,a.revision,a.enabled,a.rule_bytes,a.cooldown_seconds,a.enabled_from_public_cut,a.last_issue_fired_at_us,
		d.destination_id::text,d.url,d.revision,coalesce(d.secret_ciphertext,''::bytea),coalesce(d.encryption_key_id,''),d.enabled FROM alerts a JOIN alert_destinations d ON d.tenant_id=a.tenant_id AND d.destination_id=a.destination_id
		WHERE a.tenant_id=$1 AND a.alert_id=$2 AND a.kind='issue' FOR UPDATE OF a,d`, command.TenantID, command.AlertID).Scan(&projectID, &revision, &enabled, &ruleBytes, &cooldown, &cut, &previousFired, &destination.DestinationID, &destination.URL, &destination.Revision, &destination.Secret, &destination.EncryptionKeyID, &destinationEnabled)
	if err != nil {
		return "", err
	}
	if revision != command.ExpectedRevision {
		return "", ErrRevisionConflict
	}
	var existingDecision string
	if err := tx.QueryRow(ctx, `SELECT decision FROM issue_alert_evaluations WHERE alert_id=$1 AND alert_revision=$2 AND transition_id=$3`, command.AlertID, revision, command.TransitionID).Scan(&existingDecision); err == nil {
		if err := tx.Commit(ctx); err != nil {
			return "", err
		}
		return existingDecision, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}
	var transition PendingIssueTransition
	var lane int
	var batchSeq int64
	var titleJSON string
	err = tx.QueryRow(ctx, `SELECT t.transition_id::text,t.issue_id,t.type,t.issue_revision,t.received_time_us,o.lane_id,o.batch_seq,i.title_json FROM issue_transitions t JOIN issue_occurrences o ON o.record_id=t.record_id JOIN issues i ON i.tenant_id=t.tenant_id AND i.project_id=t.project_id AND i.issue_id=t.issue_id WHERE t.tenant_id=$1 AND t.project_id=$2 AND t.transition_id=$3 FOR SHARE OF t,o,i`, command.TenantID, projectID, command.TransitionID).Scan(&transition.TransitionID, &transition.IssueID, &transition.Type, &transition.IssueRevision, &transition.ReceivedUS, &lane, &batchSeq, &titleJSON)
	if err != nil {
		return "", err
	}
	if json.Unmarshal([]byte(titleJSON), &transition.Title) != nil {
		return "", errors.New("stored Issue title is invalid")
	}
	var rule struct {
		Events []string `json:"events"`
	}
	if json.Unmarshal(ruleBytes, &rule) != nil {
		return "", errors.New("stored issue rule is invalid")
	}
	eligible := false
	for _, event := range rule.Events {
		eligible = eligible || event == transition.Type
	}
	if len(cut) != model.LaneCount || !eligible || lane < 0 || lane >= model.LaneCount || batchSeq <= cut[lane] {
		return "", ErrForbidden
	}
	var nowUS, previousEvaluation int64
	if err := tx.QueryRow(ctx, `SELECT floor(extract(epoch FROM clock_timestamp())*1000000)::bigint,coalesce(max(evaluated_at_us),0) FROM issue_alert_evaluations WHERE alert_id=$1 AND alert_revision=$2`, command.AlertID, revision).Scan(&nowUS, &previousEvaluation); err != nil {
		return "", err
	}
	evaluated := max(nowUS, previousEvaluation)
	decision := "disabled"
	if enabled && destinationEnabled {
		decision = "cooldown"
		if previousFired == nil || evaluated-*previousFired >= int64(cooldown)*1_000_000 {
			decision = "sent"
		}
	}
	var delivery any
	if decision == "sent" {
		if command.DeliveryID == "" || len(command.BodyBytes) < 2 || len(command.BodyBytes) > 64<<10 {
			return "", errors.New("issue delivery body is required")
		}
		digest := sha256.Sum256(command.BodyBytes)
		dedupe := "issue:" + command.AlertID + ":" + strconv.FormatInt(revision, 10) + ":" + transition.IssueID + ":" + strconv.FormatInt(transition.IssueRevision, 10)
		_, err = tx.Exec(ctx, `INSERT INTO deliveries(tenant_id,project_id,delivery_id,alert_id,alert_revision,destination_id,destination_revision,destination_url,destination_secret_ciphertext,destination_encryption_key_id,dedupe_key,body_bytes,body_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, command.TenantID, projectID, command.DeliveryID, command.AlertID, revision, destination.DestinationID, destination.Revision, destination.URL, nullableBytes(destination.Secret), nullableText(destination.EncryptionKeyID), dedupe, command.BodyBytes, hex.EncodeToString(digest[:]))
		if err != nil {
			return "", err
		}
		delivery = command.DeliveryID
		if _, err := tx.Exec(ctx, `UPDATE alerts SET last_issue_fired_at_us=$3,updated_at=clock_timestamp() WHERE tenant_id=$1 AND alert_id=$2`, command.TenantID, command.AlertID, evaluated); err != nil {
			return "", err
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO issue_alert_evaluations(tenant_id,alert_id,alert_revision,transition_id,decision,delivery_id,evaluated_at_us) VALUES($1,$2,$3,$4,$5,$6,$7)`, command.TenantID, command.AlertID, revision, command.TransitionID, decision, delivery, evaluated)
	if err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return decision, nil
}

func loadThresholdEvaluation(ctx context.Context, tx pgx.Tx, tenantID int64, evaluationID string, lock bool) (ThresholdEvaluation, error) {
	var value ThresholdEvaluation
	var cut []int64
	query := `SELECT tenant_id,alert_id::text,alert_revision,evaluation_id::text,window_start_us,window_end_us,cut,state,fence,attempt,coalesce(owner,''),lease_until,snapshot_id::text,observed_count,fired,error_code FROM alert_evaluations WHERE tenant_id=$1 AND evaluation_id=$2`
	if lock {
		query += ` FOR UPDATE`
	}
	err := tx.QueryRow(ctx, query, tenantID, evaluationID).Scan(&value.TenantID, &value.AlertID, &value.AlertRevision, &value.EvaluationID, &value.WindowStartUS, &value.WindowEndUS, &cut, &value.State, &value.Fence, &value.Attempt, &value.Owner, &value.LeaseUntil, &value.SnapshotID, &value.ObservedCount, &value.Fired, &value.ErrorCode)
	if err != nil {
		return value, err
	}
	if len(cut) != model.LaneCount {
		return value, errors.New("stored evaluation cut is invalid")
	}
	copy(value.Cut[:], cut)
	return value, nil
}
func compareThreshold(observed int64, operator string, threshold int64) bool {
	switch operator {
	case "gt":
		return observed > threshold
	case "ge":
		return observed >= threshold
	case "lt":
		return observed < threshold
	case "le":
		return observed <= threshold
	}
	return false
}
