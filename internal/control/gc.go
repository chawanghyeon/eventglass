package control

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const ObjectRetirementGrace = 8 * 24 * time.Hour

type GCObject struct {
	IntentID  string
	ObjectKey string
	Attempt   int
	Resweep   bool
}

func (operations *MaintenanceOperations) ClaimGCObjects(ctx context.Context, limit int) ([]GCObject, error) {
	return operations.claimGCObjects(ctx, 0, limit)
}

func (operations *MaintenanceOperations) ClaimGCObjectsForTenant(ctx context.Context, tenantID int64, limit int) ([]GCObject, error) {
	if tenantID <= 0 {
		return nil, errors.New("invalid GC tenant")
	}
	return operations.claimGCObjects(ctx, tenantID, limit)
}

func (operations *MaintenanceOperations) claimGCObjects(ctx context.Context, tenantID int64, limit int) ([]GCObject, error) {
	if limit <= 0 || limit > 100 {
		return nil, errors.New("invalid GC claim limit")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT singleton FROM installations WHERE singleton FOR SHARE`); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE object_intents oi SET retired_at=source.retired_at
		FROM (SELECT f.intent_id,min(b.retired_at) retired_at FROM files f JOIN bundles b ON b.tenant_id=f.tenant_id AND b.bundle_id=f.bundle_id WHERE b.valid_to_generation IS NOT NULL GROUP BY f.intent_id) source
		WHERE oi.intent_id=source.intent_id AND oi.retired_at IS NULL`); err != nil {
		return nil, err
	}
	laneRows, err := tx.Query(ctx, `SELECT l.tenant_id,l.lane_id FROM lanes l WHERE l.tenant_id IN (
		SELECT tenant_id FROM (
			SELECT DISTINCT oi.tenant_id FROM object_intents oi
			WHERE ($1::bigint=0 OR oi.tenant_id=$1) AND (
				oi.state='deleting' OR (oi.state='deleted' AND oi.gc_confirmed_at<clock_timestamp()-interval '1 hour')
				OR (oi.state IN ('pending','uploaded','referenced') AND COALESCE(oi.retired_at,oi.expires_at)<clock_timestamp()-interval '8 days'))
			ORDER BY oi.tenant_id LIMIT 100
		) candidates
	) ORDER BY l.tenant_id,l.lane_id FOR UPDATE OF l`, tenantID)
	if err != nil {
		return nil, err
	}
	lockedTenants := make([]int64, 0, 100)
	var previous int64
	for laneRows.Next() {
		var lockedTenant int64
		var lane int
		if err := laneRows.Scan(&lockedTenant, &lane); err != nil {
			laneRows.Close()
			return nil, err
		}
		if lockedTenant != previous {
			lockedTenants = append(lockedTenants, lockedTenant)
			previous = lockedTenant
		}
	}
	if err := laneRows.Err(); err != nil {
		laneRows.Close()
		return nil, err
	}
	laneRows.Close()
	if len(lockedTenants) == 0 {
		return nil, tx.Commit(ctx)
	}
	rows, err := tx.Query(ctx, `SELECT oi.intent_id::text,oi.object_key,oi.gc_attempt,oi.state='deleted'
		FROM object_intents oi
		WHERE oi.tenant_id=ANY($2::bigint[]) AND (
			oi.state='deleting'
			OR (oi.state='deleted' AND oi.gc_confirmed_at<clock_timestamp()-interval '1 hour')
			OR (oi.state IN ('pending','uploaded','referenced')
				AND COALESCE(oi.retired_at,oi.expires_at)<clock_timestamp()-interval '8 days'
				AND (oi.protect_until IS NULL OR oi.protect_until<=clock_timestamp())
				AND NOT EXISTS(SELECT 1 FROM ingest_batches ib WHERE ib.journal_intent_id=oi.intent_id AND ib.recovery_state='live')
				AND NOT EXISTS(SELECT 1 FROM files f JOIN bundles b ON b.tenant_id=f.tenant_id AND b.bundle_id=f.bundle_id WHERE f.intent_id=oi.intent_id AND b.valid_to_generation IS NULL)
				AND NOT EXISTS(SELECT 1 FROM files f JOIN bundles b ON b.tenant_id=f.tenant_id AND b.bundle_id=f.bundle_id JOIN snapshot_lanes sl ON sl.tenant_id=b.tenant_id AND sl.lane_id=b.lane_id JOIN query_snapshots s ON s.tenant_id=sl.tenant_id AND s.snapshot_id=sl.snapshot_id WHERE f.intent_id=oi.intent_id AND s.state='active' AND s.expires_at>clock_timestamp() AND b.valid_from_generation<=sl.catalog_generation AND (b.valid_to_generation IS NULL OR sl.catalog_generation<b.valid_to_generation))
				AND NOT EXISTS(SELECT 1 FROM jobs j WHERE j.job_id=oi.conversion_job_id AND (
					j.state='prepared' OR (j.state='running' AND (oi.state='referenced' OR (j.storage_generation=oi.producer_generation AND j.fence=oi.producer_fence)))))
				AND NOT EXISTS(SELECT 1 FROM query_jobs q WHERE q.query_id=oi.query_id AND q.expires_at>clock_timestamp())
				AND NOT EXISTS(SELECT 1 FROM maintenance_tasks m WHERE m.task_id=oi.maintenance_task_id AND (
					m.state='prepared' OR (m.state='running' AND (oi.state='referenced' OR (m.storage_generation=oi.producer_generation AND m.fence=oi.producer_fence)))))
				AND NOT EXISTS(SELECT 1 FROM backup_sets bs WHERE bs.state='verified' AND bs.protected_until>clock_timestamp())
			)
		) ORDER BY CASE oi.state WHEN 'deleting' THEN 0 WHEN 'deleted' THEN 1 ELSE 2 END,oi.intent_id
		LIMIT $1 FOR UPDATE OF oi SKIP LOCKED`, limit, lockedTenants)
	if err != nil {
		return nil, err
	}
	var objects []GCObject
	for rows.Next() {
		var object GCObject
		if err := rows.Scan(&object.IntentID, &object.ObjectKey, &object.Attempt, &object.Resweep); err != nil {
			rows.Close()
			return nil, err
		}
		objects = append(objects, object)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	for index := range objects {
		result, err := tx.Exec(ctx, `UPDATE object_intents SET state='deleting',gc_marked_at=COALESCE(gc_marked_at,clock_timestamp()),gc_confirmed_at=NULL,gc_attempt=gc_attempt+1,updated_at=clock_timestamp() WHERE intent_id=$1 AND state IN ('pending','uploaded','referenced','deleting','deleted')`, objects[index].IntentID)
		if err != nil || result.RowsAffected() != 1 {
			return nil, errors.Join(ErrMaintenanceFence, err)
		}
		objects[index].Attempt++
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return objects, nil
}

func (operations *MaintenanceOperations) ConfirmGCObjects(ctx context.Context, intentIDs []string) error {
	if len(intentIDs) == 0 || len(intentIDs) > 100 {
		return errors.New("invalid GC confirmation")
	}
	for _, id := range intentIDs {
		if uuid.Validate(id) != nil {
			return errors.New("invalid GC intent")
		}
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	result, err := tx.Exec(ctx, `UPDATE object_intents SET state='deleted',gc_confirmed_at=clock_timestamp(),updated_at=clock_timestamp() WHERE intent_id=ANY($1::uuid[]) AND state='deleting'`, intentIDs)
	if err != nil {
		return err
	}
	if result.RowsAffected() != int64(len(intentIDs)) {
		return ErrMaintenanceFence
	}
	return tx.Commit(ctx)
}

// RunRetentionCleanup releases expired detail and one recoverable journal in a
// bounded transaction. It never deletes lane/batch ledgers or lifetime Issues.
func (operations *MaintenanceOperations) RunRetentionCleanup(ctx context.Context) (bool, error) {
	var tenantID int64
	err := operations.pool.QueryRow(ctx, `SELECT tenant_id FROM (
		SELECT o.tenant_id FROM issue_occurrences o JOIN installations i ON i.singleton
		WHERE o.received_time_us<i.retention_floor_us
		AND NOT EXISTS(SELECT 1 FROM query_snapshots s WHERE s.tenant_id=o.tenant_id AND s.state='active' AND s.expires_at>clock_timestamp() AND s.retention_floor_us<=o.received_time_us)
		UNION ALL
		SELECT d.tenant_id FROM event_dedupe d JOIN projects p ON p.tenant_id=d.tenant_id AND p.project_id=d.project_id
		WHERE d.expires_at<=clock_timestamp() AND d.created_at+make_interval(days=>p.retention_days+7)<=clock_timestamp()
		UNION ALL
		SELECT ib.tenant_id FROM ingest_batches ib WHERE ib.state='published' AND ib.recovery_state='live'
		AND NOT EXISTS(SELECT 1 FROM receipts r JOIN projects p ON p.tenant_id=r.tenant_id AND p.project_id=r.project_id WHERE r.tenant_id=ib.tenant_id AND r.lane_id=ib.lane_id AND r.batch_seq=ib.batch_seq AND r.received_time_us>=floor(extract(epoch FROM (clock_timestamp()-(p.retention_days+7)*interval '1 day'))*1000000)::bigint)
		AND NOT EXISTS(SELECT 1 FROM event_dedupe d JOIN receipts r ON r.acceptance_id=d.receipt_acceptance_id WHERE r.tenant_id=ib.tenant_id AND r.lane_id=ib.lane_id AND r.batch_seq=ib.batch_seq)
		AND NOT EXISTS(SELECT 1 FROM issue_occurrences o WHERE o.tenant_id=ib.tenant_id AND o.lane_id=ib.lane_id AND o.batch_seq=ib.batch_seq)
		AND NOT EXISTS(SELECT 1 FROM backup_sets bs WHERE bs.state='verified' AND bs.protected_until>clock_timestamp())
	) candidates ORDER BY tenant_id LIMIT 1`).Scan(&tenantID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return operations.runRetentionCleanup(ctx, tenantID)
}

func (operations *MaintenanceOperations) RunRetentionCleanupTenant(ctx context.Context, tenantID int64) (bool, error) {
	if tenantID <= 0 {
		return false, errors.New("invalid cleanup tenant")
	}
	return operations.runRetentionCleanup(ctx, tenantID)
}

func (operations *MaintenanceOperations) runRetentionCleanup(ctx context.Context, onlyTenant int64) (bool, error) {
	if onlyTenant <= 0 {
		return false, errors.New("invalid cleanup tenant")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT singleton FROM installations WHERE singleton FOR SHARE`); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `SELECT lane_id FROM lanes WHERE tenant_id=$1 ORDER BY lane_id FOR UPDATE`, onlyTenant); err != nil {
		return false, err
	}
	var floor int64
	if err := tx.QueryRow(ctx, `SELECT retention_floor_us FROM installations WHERE singleton FOR SHARE`).Scan(&floor); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `WITH expired AS (
		SELECT o.record_id FROM issue_occurrences o WHERE o.received_time_us<$1 AND ($2::bigint=0 OR o.tenant_id=$2)
		AND NOT EXISTS(SELECT 1 FROM query_snapshots s WHERE s.tenant_id=o.tenant_id AND s.state='active' AND s.expires_at>clock_timestamp() AND s.retention_floor_us<=o.received_time_us)
		ORDER BY o.record_id LIMIT 1000)
		UPDATE issue_transitions t SET record_id=NULL FROM expired e WHERE t.record_id=e.record_id`, floor, onlyTenant); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `WITH expired AS (
		SELECT o.record_id FROM issue_occurrences o WHERE o.received_time_us<$1 AND ($2::bigint=0 OR o.tenant_id=$2)
		AND NOT EXISTS(SELECT 1 FROM query_snapshots s WHERE s.tenant_id=o.tenant_id AND s.state='active' AND s.expires_at>clock_timestamp() AND s.retention_floor_us<=o.received_time_us)
		ORDER BY o.record_id LIMIT 1000)
		DELETE FROM issue_occurrences o USING expired e WHERE o.record_id=e.record_id`, floor, onlyTenant); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM event_dedupe d WHERE d.ctid IN (
		SELECT d2.ctid FROM event_dedupe d2 JOIN projects p ON p.tenant_id=d2.tenant_id AND p.project_id=d2.project_id
		WHERE d2.expires_at<=clock_timestamp()
		AND d2.created_at+make_interval(days=>p.retention_days+7)<=clock_timestamp()
		AND ($1::bigint=0 OR d2.tenant_id=$1) ORDER BY d2.expires_at LIMIT 1000)`, onlyTenant); err != nil {
		return false, err
	}
	var tenantID, batchSeq int64
	var laneID int
	err = tx.QueryRow(ctx, `SELECT ib.tenant_id,ib.lane_id,ib.batch_seq FROM ingest_batches ib
		WHERE ib.state='published' AND ib.recovery_state='live'
		AND ($1::bigint=0 OR ib.tenant_id=$1)
		AND NOT EXISTS(SELECT 1 FROM receipts r JOIN projects p ON p.tenant_id=r.tenant_id AND p.project_id=r.project_id WHERE r.tenant_id=ib.tenant_id AND r.lane_id=ib.lane_id AND r.batch_seq=ib.batch_seq AND r.received_time_us>=floor(extract(epoch FROM (clock_timestamp()-(p.retention_days+7)*interval '1 day'))*1000000)::bigint)
		AND NOT EXISTS(SELECT 1 FROM event_dedupe d JOIN receipts r ON r.acceptance_id=d.receipt_acceptance_id WHERE r.tenant_id=ib.tenant_id AND r.lane_id=ib.lane_id AND r.batch_seq=ib.batch_seq)
		AND NOT EXISTS(SELECT 1 FROM issue_occurrences o WHERE o.tenant_id=ib.tenant_id AND o.lane_id=ib.lane_id AND o.batch_seq=ib.batch_seq)
		AND NOT EXISTS(SELECT 1 FROM backup_sets bs WHERE bs.state='verified' AND bs.protected_until>clock_timestamp())
		ORDER BY ib.tenant_id,ib.lane_id,ib.batch_seq LIMIT 1 FOR UPDATE OF ib SKIP LOCKED`, onlyTenant).Scan(&tenantID, &laneID, &batchSeq)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, tx.Commit(ctx)
	}
	if err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO ingest_batch_retirement_summaries(tenant_id,lane_id,batch_seq,batch_id,request_count,record_count,accepted_count,duplicate_count,conflict_count,journal_sha256,receipt_set_sha256)
		SELECT ib.tenant_id,ib.lane_id,ib.batch_seq,ib.batch_id,ib.request_count,ib.record_count,ib.accepted_count,ib.duplicate_count,ib.conflict_count,ib.journal_sha256,
		encode(sha256(convert_to(COALESCE(string_agg(r.acceptance_id::text||':'||r.selection_sha256,E'\n' ORDER BY r.acceptance_id),''),'UTF8')),'hex')
		FROM ingest_batches ib LEFT JOIN receipts r ON r.tenant_id=ib.tenant_id AND r.lane_id=ib.lane_id AND r.batch_seq=ib.batch_seq WHERE ib.tenant_id=$1 AND ib.lane_id=$2 AND ib.batch_seq=$3 GROUP BY ib.tenant_id,ib.lane_id,ib.batch_seq`, tenantID, laneID, batchSeq); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM job_output_occurrences WHERE tenant_id=$1 AND lane_id=$2 AND batch_seq=$3`, tenantID, laneID, batchSeq); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `UPDATE jobs SET prepared_output_id=NULL WHERE tenant_id=$1 AND lane_id=$2 AND batch_seq=$3 AND state='completed'`, tenantID, laneID, batchSeq); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM job_outputs WHERE tenant_id=$1 AND job_id IN (SELECT job_id FROM jobs WHERE tenant_id=$1 AND lane_id=$2 AND batch_seq=$3 AND state='completed')`, tenantID, laneID, batchSeq); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jobs WHERE tenant_id=$1 AND lane_id=$2 AND batch_seq=$3 AND state='completed'`, tenantID, laneID, batchSeq); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM receipts WHERE tenant_id=$1 AND lane_id=$2 AND batch_seq=$3`, tenantID, laneID, batchSeq); err != nil {
		return false, err
	}
	result, err := tx.Exec(ctx, `UPDATE object_intents oi SET retired_at=clock_timestamp(),protect_until=GREATEST(COALESCE(protect_until,'-infinity'::timestamptz),clock_timestamp()+interval '8 days'),updated_at=clock_timestamp() FROM ingest_batches ib WHERE ib.tenant_id=$1 AND ib.lane_id=$2 AND ib.batch_seq=$3 AND oi.intent_id=ib.journal_intent_id`, tenantID, laneID, batchSeq)
	if err != nil || result.RowsAffected() != 1 {
		return false, errors.Join(errors.New("journal retirement failed"), err)
	}
	result, err = tx.Exec(ctx, `UPDATE ingest_batches SET recovery_state='retired',journal_intent_id=NULL,journal_retired_at=clock_timestamp() WHERE tenant_id=$1 AND lane_id=$2 AND batch_seq=$3 AND state='published' AND recovery_state='live'`, tenantID, laneID, batchSeq)
	if err != nil || result.RowsAffected() != 1 {
		return false, errors.Join(errors.New("batch retirement failed"), err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}
