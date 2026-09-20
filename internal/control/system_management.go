package control

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

type SystemLane struct {
	LaneID                    int
	AcceptedSeq, PublishedSeq int64
	OldestPendingReceivedUS   *int64
	ErrorCode                 *string
}

type SystemCounters struct {
	Since                                                                                  time.Time
	AcceptedRequests, AcceptedRecords, DuplicateRecords, ConflictRecords, PublishedRecords int64
	ReportedDrops, UnsupportedItems                                                        int64
}

type SystemBackup struct {
	State                        string
	LastSuccessAt, LastRestoreAt *time.Time
	WALAgeSeconds                *int64
}

type SystemStatus struct {
	InstallationID                      string
	Generation                          int64
	RecoveryState                       string
	AlertsPaused                        bool
	RetentionDays                       int
	RetentionRevision, RetentionFloorUS int64
	NowUS                               int64
	GCSafeBefore, GCVerifiedUntil       *time.Time
	Lanes                               []SystemLane
	Counters                            SystemCounters
	Backup                              SystemBackup
}

func (operations *AuthOperations) ReadSystemStatus(ctx context.Context, tenantID, actorUserID int64) (SystemStatus, error) {
	if tenantID <= 0 || actorUserID <= 0 {
		return SystemStatus{}, errors.New("invalid system status request")
	}
	tx, err := operations.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return SystemStatus{}, err
	}
	defer tx.Rollback(ctx)
	if err := requireTenantRole(ctx, tx, tenantID, actorUserID, true, 0, false); err != nil {
		return SystemStatus{}, err
	}
	var result SystemStatus
	if err := tx.QueryRow(ctx, `SELECT installation_id::text,storage_generation,recovery_state,alerts_paused,retention_days,retention_revision,retention_floor_us,gc_safe_before,gc_verified_until FROM installations WHERE singleton`).Scan(
		&result.InstallationID, &result.Generation, &result.RecoveryState, &result.AlertsPaused, &result.RetentionDays, &result.RetentionRevision, &result.RetentionFloorUS, &result.GCSafeBefore, &result.GCVerifiedUntil); err != nil {
		return SystemStatus{}, err
	}
	rows, err := tx.Query(ctx, `SELECT l.lane_id,l.accepted_seq,l.published_seq,
		(SELECT min(b.received_time_us) FROM ingest_batches b WHERE b.tenant_id=l.tenant_id AND b.lane_id=l.lane_id AND b.state<>'published'),
		(SELECT j.last_error_code FROM jobs j WHERE j.tenant_id=l.tenant_id AND j.lane_id=l.lane_id AND j.state='failed' ORDER BY j.updated_at DESC LIMIT 1)
		FROM lanes l WHERE l.tenant_id=$1 ORDER BY l.lane_id`, tenantID)
	if err != nil {
		return SystemStatus{}, err
	}
	for rows.Next() {
		var lane SystemLane
		if err := rows.Scan(&lane.LaneID, &lane.AcceptedSeq, &lane.PublishedSeq, &lane.OldestPendingReceivedUS, &lane.ErrorCode); err != nil {
			rows.Close()
			return SystemStatus{}, err
		}
		result.Lanes = append(result.Lanes, lane)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return SystemStatus{}, err
	}
	rows.Close()
	if len(result.Lanes) != 16 {
		return SystemStatus{}, errors.New("tenant lane topology is incomplete")
	}
	if err := tx.QueryRow(ctx, `SELECT coalesce(min(created_at),clock_timestamp()),count(*),coalesce(sum(accepted_count),0),coalesce(sum(duplicate_count),0),coalesce(sum(conflict_count),0),floor(extract(epoch FROM clock_timestamp())*1000000)::bigint FROM receipts WHERE tenant_id=$1`, tenantID).Scan(
		&result.Counters.Since, &result.Counters.AcceptedRequests, &result.Counters.AcceptedRecords, &result.Counters.DuplicateRecords, &result.Counters.ConflictRecords, &result.NowUS); err != nil {
		return SystemStatus{}, err
	}
	if err := tx.QueryRow(ctx, `SELECT coalesce(sum(accepted_count),0) FROM ingest_batches WHERE tenant_id=$1 AND state='published'`, tenantID).Scan(&result.Counters.PublishedRecords); err != nil {
		return SystemStatus{}, err
	}
	if err := tx.QueryRow(ctx, `SELECT
		coalesce((SELECT sum(o.quantity) FROM sdk_outcomes o JOIN receipts r ON r.acceptance_id=o.acceptance_id WHERE r.tenant_id=$1),0),
		coalesce((SELECT count(*) FROM receipt_unsupported u JOIN receipts r ON r.acceptance_id=u.acceptance_id WHERE r.tenant_id=$1),0)`, tenantID).Scan(&result.Counters.ReportedDrops, &result.Counters.UnsupportedItems); err != nil {
		return SystemStatus{}, err
	}
	result.Backup.State = "missing"
	var backupState string
	err = tx.QueryRow(ctx, `SELECT state,verified_at,greatest(0,extract(epoch FROM clock_timestamp()-latest_recoverable_time)::bigint) FROM backup_sets WHERE installation_id=$1 ORDER BY base_end DESC,backup_id DESC LIMIT 1`, result.InstallationID).Scan(&backupState, &result.Backup.LastSuccessAt, &result.Backup.WALAgeSeconds)
	if err == nil {
		result.Backup.State = backupState
		if backupState == "verified" && (result.GCVerifiedUntil == nil || result.GCVerifiedUntil.Before(time.Now())) {
			result.Backup.State = "stale"
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return SystemStatus{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return SystemStatus{}, err
	}
	return result, nil
}

type ChangeRetentionCommand struct {
	TenantID, ActorUserID, ExpectedRevision int64
	Days                                    int
	RequestID, AuditID                      string
}

func (operations *AuthOperations) ChangeRetentionPolicyAuthorized(ctx context.Context, command ChangeRetentionCommand) (SystemStatus, error) {
	if command.TenantID <= 0 || command.ActorUserID <= 0 || command.ExpectedRevision <= 0 || command.Days < 1 || command.Days > 3650 || command.RequestID == "" || command.AuditID == "" {
		return SystemStatus{}, errors.New("invalid retention policy change")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return SystemStatus{}, err
	}
	defer tx.Rollback(ctx)
	var result SystemStatus
	var nowUS int64
	if err := tx.QueryRow(ctx, `SELECT installation_id::text,storage_generation,recovery_state,alerts_paused,retention_days,retention_revision,retention_floor_us,floor(extract(epoch FROM clock_timestamp())*1000000)::bigint FROM installations WHERE singleton FOR UPDATE`).Scan(
		&result.InstallationID, &result.Generation, &result.RecoveryState, &result.AlertsPaused, &result.RetentionDays, &result.RetentionRevision, &result.RetentionFloorUS, &nowUS); err != nil {
		return SystemStatus{}, err
	}
	if err := requireTenantRole(ctx, tx, command.TenantID, command.ActorUserID, true, 0, false); err != nil {
		return SystemStatus{}, err
	}
	var installationAdmin bool
	if err := tx.QueryRow(ctx, `SELECT is_installation_admin FROM users WHERE user_id=$1 FOR UPDATE`, command.ActorUserID).Scan(&installationAdmin); err != nil || !installationAdmin {
		return SystemStatus{}, errors.Join(ErrForbidden, err)
	}
	if result.RetentionRevision != command.ExpectedRevision {
		return SystemStatus{}, ErrRevisionConflict
	}
	newFloor, err := RetentionFloorAfterChange(nowUS, result.RetentionFloorUS, result.RetentionDays, command.Days)
	if err != nil {
		return SystemStatus{}, err
	}
	result.RetentionDays, result.RetentionRevision, result.RetentionFloorUS = command.Days, result.RetentionRevision+1, newFloor
	if _, err := tx.Exec(ctx, `UPDATE installations SET retention_days=$1,dedupe_retention_days=GREATEST(dedupe_retention_days,$1),retention_revision=$2,retention_floor_us=$3,retention_tick_at=clock_timestamp() WHERE singleton`, result.RetentionDays, result.RetentionRevision, result.RetentionFloorUS); err != nil {
		return SystemStatus{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO audit_events(tenant_id,audit_id,actor_user_id,action,target_type,target_id,target_revision,request_id) VALUES($1,$2,$3,'retention_updated','installation',$4,$5,$6)`, command.TenantID, command.AuditID, command.ActorUserID, result.InstallationID, result.RetentionRevision, command.RequestID); err != nil {
		return SystemStatus{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return SystemStatus{}, err
	}
	return result, nil
}
