package control

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const RecoveryVerificationFreshness = 24 * time.Hour

var (
	ErrRecoveryGeneration = errors.New("recovery generation changed")
	ErrRecoveryReport     = errors.New("recovery verification report is invalid")
)

type BackupRegistration struct {
	BackupID, ExternalToolID, InstallationID string
	StorageGeneration                        int64
	BaseStart, BaseEnd                       time.Time
	EarliestRecoverable, LatestRecoverable   time.Time
	ProtectedUntil                           time.Time
}

type RecoveryObject struct {
	IntentID, ObjectKey, SHA256 string
	Bytes                       int64
}

type RecoveryInventory struct {
	BackupID, ExternalToolID, InstallationID string
	StorageGeneration                        int64
	EarliestRecoverable, LatestRecoverable   time.Time
	Objects                                  []RecoveryObject
}

type RecoveryVerification struct {
	VerificationID, BackupID, InstallationID   string
	SourceGeneration                           int64
	ReportSHA256, InventorySHA256, RecoveryLSN string
	ReferencedObjects, ReferencedBytes         int64
}

type LaneRepairState struct {
	TenantID          int64   `json:"tenant_id"`
	LaneID            int     `json:"lane_id"`
	AcceptedSeq       int64   `json:"accepted_seq"`
	PublishedSeq      int64   `json:"published_seq"`
	CatalogGeneration int64   `json:"catalog_generation"`
	OldestPendingSeq  *int64  `json:"oldest_pending_seq,omitempty"`
	FailedJobID       *string `json:"failed_job_id,omitempty"`
	FailedErrorCode   *string `json:"failed_error_code,omitempty"`
	FailedFence       *int64  `json:"failed_fence,omitempty"`
}

func (operations *MaintenanceOperations) InspectRecoveryLane(ctx context.Context, tenantID int64, laneID int) (LaneRepairState, error) {
	if tenantID <= 0 || laneID < 0 || laneID >= 16 {
		return LaneRepairState{}, errors.New("invalid repair scope")
	}
	var result LaneRepairState
	result.TenantID, result.LaneID = tenantID, laneID
	err := operations.pool.QueryRow(ctx, `SELECT l.accepted_seq,l.published_seq,l.catalog_generation,
		(SELECT min(batch_seq) FROM ingest_batches WHERE tenant_id=l.tenant_id AND lane_id=l.lane_id AND state<>'published'),
		(SELECT job_id::text FROM jobs WHERE tenant_id=l.tenant_id AND lane_id=l.lane_id AND state='failed' ORDER BY batch_seq LIMIT 1),
		(SELECT last_error_code FROM jobs WHERE tenant_id=l.tenant_id AND lane_id=l.lane_id AND state='failed' ORDER BY batch_seq LIMIT 1),
		(SELECT fence FROM jobs WHERE tenant_id=l.tenant_id AND lane_id=l.lane_id AND state='failed' ORDER BY batch_seq LIMIT 1)
		FROM lanes l WHERE l.tenant_id=$1 AND l.lane_id=$2`, tenantID, laneID).Scan(&result.AcceptedSeq, &result.PublishedSeq, &result.CatalogGeneration, &result.OldestPendingSeq, &result.FailedJobID, &result.FailedErrorCode, &result.FailedFence)
	return result, err
}

func (operations *MaintenanceOperations) RegisterBackup(ctx context.Context, value BackupRegistration) error {
	if uuid.Validate(value.BackupID) != nil || uuid.Validate(value.InstallationID) != nil || value.ExternalToolID == "" || len(value.ExternalToolID) > 256 || value.StorageGeneration <= 0 || value.BaseEnd.Before(value.BaseStart) || value.LatestRecoverable.Before(value.EarliestRecoverable) || !value.ProtectedUntil.After(time.Now()) {
		return errors.New("invalid backup registration")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var currentID string
	var generation int64
	if err := tx.QueryRow(ctx, `SELECT installation_id::text,storage_generation FROM installations WHERE singleton FOR SHARE`).Scan(&currentID, &generation); err != nil {
		return err
	}
	if currentID != value.InstallationID || generation != value.StorageGeneration {
		return ErrRecoveryGeneration
	}
	_, err = tx.Exec(ctx, `INSERT INTO backup_sets(backup_id,external_tool_id,installation_id,storage_generation,base_start,base_end,earliest_recoverable_time,latest_recoverable_time,state,protected_until)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,'pending',$9)`, value.BackupID, value.ExternalToolID, value.InstallationID, value.StorageGeneration, value.BaseStart, value.BaseEnd, value.EarliestRecoverable, value.LatestRecoverable, value.ProtectedUntil)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// BeginRecoveryVerification is run only against an isolated restored database.
// It makes every subsequent accidental runtime start fail closed before object IO.
func (operations *MaintenanceOperations) BeginRecoveryVerification(ctx context.Context, backupID string, expectedGeneration int64) (RecoveryInventory, error) {
	if uuid.Validate(backupID) != nil || expectedGeneration <= 0 {
		return RecoveryInventory{}, errors.New("invalid recovery verification")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return RecoveryInventory{}, err
	}
	defer tx.Rollback(ctx)
	var result RecoveryInventory
	if err := tx.QueryRow(ctx, `SELECT bs.backup_id::text,bs.external_tool_id,bs.installation_id::text,bs.storage_generation,bs.earliest_recoverable_time,bs.latest_recoverable_time
		FROM backup_sets bs JOIN installations i ON i.singleton AND i.installation_id=bs.installation_id
		WHERE bs.backup_id=$1 AND bs.state IN ('pending','verified') FOR UPDATE OF bs,i`, backupID).Scan(&result.BackupID, &result.ExternalToolID, &result.InstallationID, &result.StorageGeneration, &result.EarliestRecoverable, &result.LatestRecoverable); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RecoveryInventory{}, ErrRecoveryReport
		}
		return RecoveryInventory{}, err
	}
	if result.StorageGeneration != expectedGeneration {
		return RecoveryInventory{}, ErrRecoveryGeneration
	}
	if _, err := tx.Exec(ctx, `UPDATE installations SET recovery_state='restoring',alerts_paused=true,gc_safe_before=NULL,gc_verified_until=NULL WHERE singleton AND storage_generation=$1`, expectedGeneration); err != nil {
		return RecoveryInventory{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE sessions SET revoked_at=COALESCE(revoked_at,clock_timestamp()) WHERE revoked_at IS NULL`); err != nil {
		return RecoveryInventory{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE query_snapshots SET state='released' WHERE state='active'`); err != nil {
		return RecoveryInventory{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE query_tasks SET state='canceled',owner=NULL,lease_until=NULL,error_code='restore_generation_changed' WHERE state IN ('queued','running')`); err != nil {
		return RecoveryInventory{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE query_jobs SET state='canceled',coordinator_owner=NULL,lease_until=NULL,error_code='restore_generation_changed',updated_at=clock_timestamp() WHERE state IN ('planning','queued','running')`); err != nil {
		return RecoveryInventory{}, err
	}
	rows, err := tx.Query(ctx, `SELECT intent_id::text,object_key,expected_bytes,expected_sha256 FROM object_intents
		WHERE state IN ('uploaded','referenced') ORDER BY object_key,intent_id`)
	if err != nil {
		return RecoveryInventory{}, err
	}
	for rows.Next() {
		var object RecoveryObject
		if err := rows.Scan(&object.IntentID, &object.ObjectKey, &object.Bytes, &object.SHA256); err != nil {
			rows.Close()
			return RecoveryInventory{}, err
		}
		result.Objects = append(result.Objects, object)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return RecoveryInventory{}, err
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return RecoveryInventory{}, err
	}
	return result, nil
}

func (operations *MaintenanceOperations) FailRecoveryVerification(ctx context.Context, backupID, code string) error {
	if uuid.Validate(backupID) != nil || code == "" || len(code) > 64 {
		return errors.New("invalid recovery failure")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `UPDATE backup_sets SET state='failed' WHERE backup_id=$1 AND state IN ('pending','verified')`, backupID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE installations SET recovery_state='verification_required',alerts_paused=true,gc_safe_before=NULL,gc_verified_until=NULL WHERE singleton`); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (operations *MaintenanceOperations) RecordRecoveryVerification(ctx context.Context, value RecoveryVerification) error {
	if uuid.Validate(value.VerificationID) != nil || uuid.Validate(value.BackupID) != nil || uuid.Validate(value.InstallationID) != nil || value.SourceGeneration <= 0 || !validHexDigest(value.ReportSHA256) || !validHexDigest(value.InventorySHA256) || value.RecoveryLSN == "" || len(value.RecoveryLSN) > 64 || value.ReferencedObjects < 0 || value.ReferencedBytes < 0 {
		return errors.New("invalid recovery verification result")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var installationID string
	var generation int64
	var recovery string
	if err := tx.QueryRow(ctx, `SELECT installation_id::text,storage_generation,recovery_state FROM installations WHERE singleton FOR UPDATE`).Scan(&installationID, &generation, &recovery); err != nil {
		return err
	}
	if installationID != value.InstallationID || generation != value.SourceGeneration || recovery != "restoring" {
		return ErrRecoveryGeneration
	}
	result, err := tx.Exec(ctx, `UPDATE backup_sets SET state='verified',verified_at=clock_timestamp()
		WHERE backup_id=$1 AND installation_id=$2 AND storage_generation=$3 AND state IN ('pending','verified')`, value.BackupID, value.InstallationID, value.SourceGeneration)
	if err != nil || result.RowsAffected() != 1 {
		return errors.Join(ErrRecoveryReport, err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO recovery_verifications(verification_id,backup_id,installation_id,source_generation,report_sha256,inventory_sha256,referenced_objects,referenced_bytes,recovery_lsn,state)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'verified')`, value.VerificationID, value.BackupID, value.InstallationID, value.SourceGeneration, value.ReportSHA256, value.InventorySHA256, value.ReferencedObjects, value.ReferencedBytes, value.RecoveryLSN)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE installations SET recovery_state='verification_required',alerts_paused=true WHERE singleton`); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// RecordBackupRehearsal imports a signed report produced by an isolated restore.
// It refreshes only the backup/GC horizon and never changes runtime generation.
func (operations *MaintenanceOperations) RecordBackupRehearsal(ctx context.Context, value RecoveryVerification, verifiedAt time.Time) error {
	age := time.Since(verifiedAt)
	if uuid.Validate(value.VerificationID) != nil || uuid.Validate(value.BackupID) != nil || uuid.Validate(value.InstallationID) != nil || value.SourceGeneration <= 0 || !validHexDigest(value.ReportSHA256) || !validHexDigest(value.InventorySHA256) || value.RecoveryLSN == "" || len(value.RecoveryLSN) > 64 || value.ReferencedObjects < 0 || value.ReferencedBytes < 0 || age < 0 || age > RecoveryVerificationFreshness {
		return errors.New("invalid backup rehearsal")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var installationID, recovery string
	var generation int64
	if err := tx.QueryRow(ctx, `SELECT installation_id::text,storage_generation,recovery_state FROM installations WHERE singleton FOR UPDATE`).Scan(&installationID, &generation, &recovery); err != nil {
		return err
	}
	if installationID != value.InstallationID || generation != value.SourceGeneration || recovery != "ready" {
		return ErrRecoveryGeneration
	}
	var safeBefore time.Time
	result, err := tx.Exec(ctx, `UPDATE backup_sets SET state='verified',verified_at=$2 WHERE backup_id=$1 AND installation_id=$3 AND storage_generation=$4 AND state IN ('pending','verified')`, value.BackupID, verifiedAt, value.InstallationID, value.SourceGeneration)
	if err != nil || result.RowsAffected() != 1 {
		return errors.Join(ErrRecoveryReport, err)
	}
	if err := tx.QueryRow(ctx, `SELECT earliest_recoverable_time FROM backup_sets WHERE backup_id=$1`, value.BackupID).Scan(&safeBefore); err != nil {
		return err
	}
	result, err = tx.Exec(ctx, `INSERT INTO recovery_verifications(verification_id,backup_id,installation_id,source_generation,report_sha256,inventory_sha256,referenced_objects,referenced_bytes,recovery_lsn,state,verified_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'verified',$10) ON CONFLICT(verification_id) DO NOTHING`, value.VerificationID, value.BackupID, value.InstallationID, value.SourceGeneration, value.ReportSHA256, value.InventorySHA256, value.ReferencedObjects, value.ReferencedBytes, value.RecoveryLSN, verifiedAt)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		var matches bool
		err = tx.QueryRow(ctx, `SELECT backup_id=$2 AND installation_id=$3 AND source_generation=$4 AND report_sha256=$5 AND inventory_sha256=$6 AND referenced_objects=$7 AND referenced_bytes=$8 AND recovery_lsn=$9 AND state='verified'
			FROM recovery_verifications WHERE verification_id=$1`, value.VerificationID, value.BackupID, value.InstallationID, value.SourceGeneration, value.ReportSHA256, value.InventorySHA256, value.ReferencedObjects, value.ReferencedBytes, value.RecoveryLSN).Scan(&matches)
		if err != nil || !matches {
			return errors.Join(ErrRecoveryReport, err)
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE installations SET gc_safe_before=$1,gc_verified_until=$2 WHERE singleton`, safeBefore, verifiedAt.Add(RecoveryVerificationFreshness)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (operations *MaintenanceOperations) ActivateRecovery(ctx context.Context, verificationID, reportSHA string, expectedGeneration int64) (int64, error) {
	if uuid.Validate(verificationID) != nil || !validHexDigest(reportSHA) || expectedGeneration <= 0 {
		return 0, errors.New("invalid recovery activation")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	var sourceGeneration int64
	var state string
	var verifiedAt time.Time
	var safeBefore time.Time
	if err := tx.QueryRow(ctx, `SELECT rv.source_generation,rv.state,rv.verified_at,bs.earliest_recoverable_time
		FROM recovery_verifications rv JOIN backup_sets bs ON bs.backup_id=rv.backup_id
		WHERE rv.verification_id=$1 AND rv.report_sha256=$2 FOR UPDATE OF rv,bs`, verificationID, reportSHA).Scan(&sourceGeneration, &state, &verifiedAt, &safeBefore); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrRecoveryReport
		}
		return 0, err
	}
	if sourceGeneration != expectedGeneration || state != "verified" || time.Since(verifiedAt) < 0 || time.Since(verifiedAt) > RecoveryVerificationFreshness {
		return 0, ErrRecoveryReport
	}
	var currentGeneration int64
	var recovery string
	if err := tx.QueryRow(ctx, `SELECT storage_generation,recovery_state FROM installations WHERE singleton FOR UPDATE`).Scan(&currentGeneration, &recovery); err != nil {
		return 0, err
	}
	if currentGeneration != expectedGeneration || recovery != "verification_required" {
		return 0, ErrRecoveryGeneration
	}
	newGeneration := currentGeneration + 1
	if _, err := tx.Exec(ctx, `UPDATE job_outputs o SET state='discarded' FROM jobs j WHERE j.prepared_output_id=o.output_id AND j.state<>'completed' AND o.state='prepared'`); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `UPDATE jobs SET state='queued',storage_generation=$1,prepared_output_id=NULL,owner=NULL,lease_until=NULL,fence=fence+1,retry_at=clock_timestamp(),last_error_code=NULL,updated_at=clock_timestamp() WHERE state<>'completed'`, newGeneration); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `UPDATE bundles SET reserved_by=NULL WHERE reserved_by IN (SELECT task_id FROM maintenance_tasks WHERE state IN ('queued','running','prepared'))`); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `UPDATE maintenance_tasks SET state='failed',owner=NULL,lease_until=NULL,output_manifest=NULL,output_sha256=NULL,last_error_code='restore_generation_changed',updated_at=clock_timestamp() WHERE state IN ('queued','running','prepared')`); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `UPDATE alert_evaluations SET state='failed',owner=NULL,lease_until=NULL,error_code='restore_generation_changed',updated_at=clock_timestamp() WHERE state IN ('waiting','queued','running')`); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `UPDATE deliveries SET state='queued',owner=NULL,lease_until=NULL,fence=fence+1,retry_at=clock_timestamp(),error_code='restore_retry',updated_at=clock_timestamp() WHERE state='running'`); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `UPDATE recovery_verifications SET state='activated',activated_at=clock_timestamp() WHERE verification_id=$1`, verificationID); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `UPDATE installations SET storage_generation=$1,recovery_state='ready',alerts_paused=true,last_restore_at=clock_timestamp(),gc_safe_before=$2,gc_verified_until=clock_timestamp()+interval '24 hours' WHERE singleton`, newGeneration, safeBefore); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return newGeneration, nil
}

func validHexDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, char := range value {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f')) {
			return false
		}
	}
	return true
}
