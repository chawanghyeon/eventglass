package control

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const MaxTenantAlertSnapshots = 8

type CreateAlertSnapshotCommand struct {
	TenantID                          int64
	AlertID, EvaluationID, SnapshotID string
	DatasetSHA256                     string
	DatasetBytes                      []byte
}

func (operations *QueryOperations) CreateAlertSnapshot(ctx context.Context, command CreateAlertSnapshotCommand) (model.QuerySnapshot, error) {
	if command.TenantID <= 0 || uuid.Validate(command.AlertID) != nil || uuid.Validate(command.EvaluationID) != nil || uuid.Validate(command.SnapshotID) != nil || !validSHA(command.DatasetSHA256) || len(command.DatasetBytes) < 2 || len(command.DatasetBytes) > 32768 {
		return model.QuerySnapshot{}, errors.New("invalid alert snapshot")
	}
	connection, err := operations.pool.Acquire(ctx)
	if err != nil {
		return model.QuerySnapshot{}, err
	}
	if _, err := connection.Exec(ctx, `SELECT pg_advisory_lock($1::bigint)`, -command.TenantID); err != nil {
		connection.Release()
		return model.QuerySnapshot{}, err
	}
	defer releaseTenantAdmission(connection, -command.TenantID)
	tx, err := connection.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return model.QuerySnapshot{}, err
	}
	defer tx.Rollback(ctx)
	var result model.QuerySnapshot
	var alertRevision, projectID, tenantRevision, projectRevision, generation, retentionFloor int64
	var enabled, paused bool
	var evaluationRevision int64
	var evaluationState string
	var cut []int64
	var recovery, projectState string
	var now time.Time
	err = tx.QueryRow(ctx, `SELECT project_id FROM alerts WHERE tenant_id=$1 AND alert_id=$2`, command.TenantID, command.AlertID).Scan(&projectID)
	if err != nil {
		return result, err
	}
	if err := tx.QueryRow(ctx, `SELECT storage_generation,retention_floor_us,alerts_paused,recovery_state,clock_timestamp() FROM installations WHERE singleton FOR SHARE`).Scan(&generation, &retentionFloor, &paused, &recovery, &now); err != nil {
		return result, err
	}
	if err := tx.QueryRow(ctx, `SELECT t.auth_revision,p.auth_revision,p.state FROM tenants t JOIN projects p ON p.tenant_id=t.tenant_id WHERE t.tenant_id=$1 AND p.project_id=$2 FOR SHARE OF t,p`, command.TenantID, projectID).Scan(&tenantRevision, &projectRevision, &projectState); err != nil {
		return result, err
	}
	if paused || recovery != "ready" || projectState != "active" {
		return result, ErrForbidden
	}
	var tenantCount int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM query_snapshots WHERE tenant_id=$1 AND principal_kind='alert' AND state='active' AND expires_at>clock_timestamp()`, command.TenantID).Scan(&tenantCount); err != nil {
		return result, err
	}
	if tenantCount >= MaxTenantAlertSnapshots {
		return result, ErrSnapshotLimit
	}
	rows, err := tx.Query(ctx, `SELECT lane_id,published_seq,catalog_generation FROM lanes WHERE tenant_id=$1 ORDER BY lane_id FOR SHARE`, command.TenantID)
	if err != nil {
		return result, err
	}
	var published [model.LaneCount]int64
	index := 0
	for rows.Next() {
		if index >= model.LaneCount {
			rows.Close()
			return result, errors.New("tenant lane topology is invalid")
		}
		if err := rows.Scan(&result.Lanes[index].LaneID, &published[index], &result.Lanes[index].CatalogGeneration); err != nil {
			rows.Close()
			return result, err
		}
		if result.Lanes[index].LaneID != index {
			rows.Close()
			return result, errors.New("tenant lane topology is invalid")
		}
		index++
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return result, err
	}
	rows.Close()
	if index != model.LaneCount {
		return result, errors.New("tenant lane topology is incomplete")
	}
	err = tx.QueryRow(ctx, `SELECT a.revision,a.enabled,e.alert_revision,e.state,e.cut
		FROM alerts a JOIN alert_evaluations e ON e.tenant_id=a.tenant_id AND e.alert_id=a.alert_id
		WHERE a.tenant_id=$1 AND a.alert_id=$2 AND e.evaluation_id=$3 FOR UPDATE OF a,e`, command.TenantID, command.AlertID, command.EvaluationID).Scan(&alertRevision, &enabled, &evaluationRevision, &evaluationState, &cut)
	if err != nil {
		return result, err
	}
	if !enabled || evaluationRevision != alertRevision || evaluationState != "queued" || len(cut) != model.LaneCount {
		return result, ErrForbidden
	}
	for lane := range result.Lanes {
		result.Lanes[lane].CutSeq = cut[lane]
		if published[lane] < cut[lane] {
			return result, errors.New("alert evaluation cut is not published")
		}
	}
	expires := now.Add(QueryMaximumTimeout)
	maxUntil := expires
	result = model.QuerySnapshot{SnapshotID: command.SnapshotID, TenantID: command.TenantID, PrincipalHash: command.AlertID, AuthRevision: alertRevision, TenantAuthRevision: tenantRevision, StorageGeneration: generation, DatasetSHA256: command.DatasetSHA256, DatasetBytes: append([]byte(nil), command.DatasetBytes...), RetentionFloorUS: retentionFloor, ProjectIDs: []int64{projectID}, ProjectRevisions: []int64{projectRevision}, Lanes: result.Lanes, ExpiresAtUS: expires.UnixMicro(), MaxUntilUS: maxUntil.UnixMicro()}
	_, err = tx.Exec(ctx, `INSERT INTO query_snapshots(snapshot_id,tenant_id,user_id,principal_kind,principal_ref,auth_revision,tenant_auth_revision,storage_generation,dataset_hash,dataset_bytes,retention_floor_us,created_at,expires_at,max_until) VALUES($1,$2,NULL,'alert',$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, command.SnapshotID, command.TenantID, command.AlertID, alertRevision, tenantRevision, generation, command.DatasetSHA256, command.DatasetBytes, retentionFloor, now, expires, maxUntil)
	if err != nil {
		return result, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO snapshot_projects(snapshot_id,tenant_id,project_id,project_auth_revision) VALUES($1,$2,$3,$4)`, command.SnapshotID, command.TenantID, projectID, projectRevision); err != nil {
		return result, err
	}
	for _, lane := range result.Lanes {
		if _, err := tx.Exec(ctx, `INSERT INTO snapshot_lanes(snapshot_id,tenant_id,lane_id,cut_seq,catalog_generation) VALUES($1,$2,$3,$4,$5)`, command.SnapshotID, command.TenantID, lane.LaneID, lane.CutSeq, lane.CatalogGeneration); err != nil {
			return result, err
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE alert_evaluations SET snapshot_id=$3,updated_at=clock_timestamp() WHERE tenant_id=$1 AND evaluation_id=$2 AND state='queued'`, command.TenantID, command.EvaluationID, command.SnapshotID); err != nil {
		return result, err
	}
	if err := tx.Commit(ctx); err != nil {
		return result, err
	}
	return result, nil
}

type AlertCatalogCommand struct {
	AlertID string
	CatalogCommand
}

func (operations *QueryOperations) CatalogPageForAlert(ctx context.Context, command AlertCatalogCommand) ([]model.CatalogFile, error) {
	if uuid.Validate(command.AlertID) != nil || command.SessionTokenHash != ([32]byte{}) {
		return nil, errors.New("invalid alert catalog")
	}
	if err := validateCatalogScope(command.CatalogCommand, false); err != nil {
		return nil, err
	}
	tx, err := operations.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	snapshot, err := loadAlertSnapshot(ctx, tx, command.TenantID, command.SnapshotID, command.AlertID)
	if err != nil {
		return nil, err
	}
	if snapshot.DatasetSHA256 != command.DatasetSHA256 || !bytes.Equal(snapshot.DatasetBytes, command.DatasetBytes) {
		return nil, ErrSnapshotMismatch
	}
	result, err := catalogPageRows(ctx, tx, command.CatalogCommand, snapshot.RetentionFloorUS)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

func loadAlertSnapshot(ctx context.Context, tx pgx.Tx, tenantID int64, snapshotID, alertID string) (model.QuerySnapshot, error) {
	var result model.QuerySnapshot
	var expires, maxUntil, now time.Time
	var enabled, paused bool
	var currentRevision, tenantRevision, generation int64
	var recovery string
	err := tx.QueryRow(ctx, `SELECT s.snapshot_id::text,s.tenant_id,s.principal_ref,s.auth_revision,s.tenant_auth_revision,s.storage_generation,s.dataset_hash,s.dataset_bytes,s.retention_floor_us,s.expires_at,s.max_until,a.revision,a.enabled,t.auth_revision,i.storage_generation,i.alerts_paused,i.recovery_state,clock_timestamp()
		FROM query_snapshots s JOIN alerts a ON a.tenant_id=s.tenant_id AND a.alert_id::text=s.principal_ref JOIN tenants t ON t.tenant_id=s.tenant_id JOIN installations i ON i.singleton WHERE s.tenant_id=$1 AND s.snapshot_id=$2 AND s.principal_kind='alert' AND s.principal_ref=$3 AND s.state='active' FOR SHARE OF s,a,t,i`, tenantID, snapshotID, alertID).Scan(&result.SnapshotID, &result.TenantID, &result.PrincipalHash, &result.AuthRevision, &result.TenantAuthRevision, &result.StorageGeneration, &result.DatasetSHA256, &result.DatasetBytes, &result.RetentionFloorUS, &expires, &maxUntil, &currentRevision, &enabled, &tenantRevision, &generation, &paused, &recovery, &now)
	if err != nil {
		return result, errors.Join(ErrSnapshotExpired, err)
	}
	if !enabled || paused || recovery != "ready" || !expires.After(now) || currentRevision != result.AuthRevision || tenantRevision != result.TenantAuthRevision || generation != result.StorageGeneration {
		return result, ErrForbidden
	}
	result.ExpiresAtUS, result.MaxUntilUS = expires.UnixMicro(), maxUntil.UnixMicro()
	rows, err := tx.Query(ctx, `SELECT sp.project_id,sp.project_auth_revision,p.auth_revision,p.state FROM snapshot_projects sp JOIN projects p ON p.tenant_id=sp.tenant_id AND p.project_id=sp.project_id WHERE sp.tenant_id=$1 AND sp.snapshot_id=$2 FOR SHARE OF p`, tenantID, snapshotID)
	if err != nil {
		return result, err
	}
	for rows.Next() {
		var captured, current int64
		var state string
		var project int64
		if err := rows.Scan(&project, &captured, &current, &state); err != nil {
			rows.Close()
			return result, err
		}
		if captured != current || state != "active" {
			rows.Close()
			return result, ErrForbidden
		}
		result.ProjectIDs = append(result.ProjectIDs, project)
		result.ProjectRevisions = append(result.ProjectRevisions, captured)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return result, err
	}
	rows.Close()
	if len(result.ProjectIDs) != 1 {
		return result, ErrForbidden
	}
	return result, nil
}

func validateCatalogScope(command CatalogCommand, requireToken bool) error {
	token := command.SessionTokenHash
	if !requireToken {
		command.SessionTokenHash = [32]byte{1}
	}
	err := validateCatalogCommand(command)
	command.SessionTokenHash = token
	return err
}

type CreateAlertQueryCommand struct {
	QueryID, TenantAlertID, SnapshotID, OperationHash, Owner string
	TenantID                                                 int64
	OperationBytes                                           []byte
	Timeout                                                  time.Duration
}

func (operations *QueryOperations) CreateAlertQuery(ctx context.Context, command CreateAlertQueryCommand) (QueryJob, error) {
	if command.TenantID <= 0 || uuid.Validate(command.QueryID) != nil || uuid.Validate(command.TenantAlertID) != nil || uuid.Validate(command.SnapshotID) != nil || !validSHA(command.OperationHash) || len(command.OperationBytes) < 2 || len(command.OperationBytes) > 65536 || command.Owner == "" || command.Timeout <= 0 || command.Timeout > QueryMaximumTimeout {
		return QueryJob{}, errors.New("invalid alert query")
	}
	connection, err := operations.pool.Acquire(ctx)
	if err != nil {
		return QueryJob{}, err
	}
	if _, err := connection.Exec(ctx, `SELECT pg_advisory_lock($1::bigint)`, command.TenantID); err != nil {
		connection.Release()
		return QueryJob{}, err
	}
	defer releaseTenantAdmission(connection, command.TenantID)
	tx, err := connection.Begin(ctx)
	if err != nil {
		return QueryJob{}, err
	}
	defer tx.Rollback(ctx)
	snapshot, err := loadAlertSnapshot(ctx, tx, command.TenantID, command.SnapshotID, command.TenantAlertID)
	if err != nil {
		return QueryJob{}, err
	}
	var active, queued int
	if err := tx.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE state IN ('planning','queued')) FROM query_jobs WHERE tenant_id=$1 AND state IN ('planning','queued','running') AND deadline>clock_timestamp()`, command.TenantID).Scan(&active, &queued); err != nil {
		return QueryJob{}, err
	}
	if active >= MaxTenantQueries || queued >= MaxQueuedQueries {
		return QueryJob{}, ErrQueryLimitExceeded
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return QueryJob{}, err
	}
	deadline := now.Add(command.Timeout)
	maxUntil := time.UnixMicro(snapshot.MaxUntilUS)
	if deadline.After(maxUntil) {
		deadline = maxUntil
	}
	expires := time.UnixMicro(snapshot.ExpiresAtUS)
	lease := now.Add(QueryCoordinatorLease)
	if lease.After(deadline) {
		lease = deadline
	}
	_, err = tx.Exec(ctx, `INSERT INTO query_jobs(query_id,tenant_id,user_id,principal_ref,snapshot_id,operation_kind,operation_hash,operation_bytes,state,deadline,coordinator_owner,coordinator_fence,lease_until,expires_at) VALUES($1,$2,NULL,$3,$4,'alert',$5,$6,'planning',$7,$8,1,$9,$10)`, command.QueryID, command.TenantID, command.TenantAlertID, command.SnapshotID, command.OperationHash, command.OperationBytes, deadline, command.Owner, lease, expires)
	if err != nil {
		return QueryJob{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return QueryJob{}, err
	}
	return QueryJob{Authority: QueryCoordinatorAuthority{QueryID: command.QueryID, TenantID: command.TenantID, Owner: command.Owner, Fence: 1, StorageGeneration: snapshot.StorageGeneration}, SnapshotID: command.SnapshotID, Deadline: deadline, ExpiresAt: expires, State: "planning"}, nil
}

func (operations *QueryOperations) GetAlertQueryStatus(ctx context.Context, tenantID int64, alertID, queryID string) (QueryStatus, error) {
	if tenantID <= 0 || uuid.Validate(alertID) != nil || uuid.Validate(queryID) != nil {
		return QueryStatus{}, ErrQueryNotFound
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return QueryStatus{}, err
	}
	defer tx.Rollback(ctx)
	var result QueryStatus
	var principal string
	var userID *int64
	var errorCode, intentID, resultSHA, objectKey *string
	var resultBytes *int64
	var now time.Time
	err = tx.QueryRow(ctx, `SELECT q.query_id::text,q.tenant_id,q.snapshot_id::text,q.operation_kind,q.state,q.error_code,q.deadline,q.expires_at,q.created_at,q.updated_at,q.plan_file_count,q.plan_scan_count,q.plan_bytes,q.plan_input_bytes,q.cache_bytes,q.operation_hash,q.operation_bytes,s.dataset_hash,q.user_id,q.principal_ref,q.result_intent_id::text,q.result_sha256,q.result_bytes,clock_timestamp() FROM query_jobs q JOIN query_snapshots s ON s.tenant_id=q.tenant_id AND s.snapshot_id=q.snapshot_id WHERE q.tenant_id=$1 AND q.query_id=$2 FOR SHARE OF q`, tenantID, queryID).Scan(&result.QueryID, &result.TenantID, &result.SnapshotID, &result.OperationKind, &result.State, &errorCode, &result.Deadline, &result.ExpiresAt, &result.CreatedAt, &result.UpdatedAt, &result.PlanFiles, &result.PlanScans, &result.PlanBytes, &result.PlanInputBytes, &result.CacheBytes, &result.OperationHash, &result.Operation, &result.DatasetHash, &userID, &principal, &intentID, &resultSHA, &resultBytes, &now)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, ErrQueryNotFound
	}
	if err != nil {
		return result, err
	}
	if userID != nil || principal != alertID {
		return result, ErrQueryNotFound
	}
	if !result.ExpiresAt.After(now) {
		return result, ErrQueryGone
	}
	if _, err := loadAlertSnapshot(ctx, tx, tenantID, result.SnapshotID, alertID); err != nil {
		return result, err
	}
	if errorCode != nil {
		result.ErrorCode = *errorCode
	}
	if result.State == "succeeded" {
		if intentID == nil || resultSHA == nil || resultBytes == nil || *resultBytes <= 0 {
			return result, errors.New("succeeded alert query result is incomplete")
		}
		if err := tx.QueryRow(ctx, `SELECT object_key FROM object_intents WHERE tenant_id=$1 AND intent_id=$2 AND state='referenced'`, tenantID, *intentID).Scan(&objectKey); err != nil || objectKey == nil {
			return result, errors.Join(errors.New("succeeded query artifact is unavailable"), err)
		}
		result.Result = &QueryResultArtifact{IntentID: *intentID, ObjectKey: *objectKey, Bytes: *resultBytes, SHA256: *resultSHA}
	}
	if err := tx.Commit(ctx); err != nil {
		return result, err
	}
	return result, nil
}

func (operations *QueryOperations) FindAlertQuery(ctx context.Context, tenantID int64, alertID, snapshotID string) (*QueryStatus, error) {
	var queryID string
	err := operations.pool.QueryRow(ctx, `SELECT query_id::text FROM query_jobs WHERE tenant_id=$1 AND principal_ref=$2 AND snapshot_id=$3 AND user_id IS NULL`, tenantID, alertID, snapshotID).Scan(&queryID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	status, err := operations.GetAlertQueryStatus(ctx, tenantID, alertID, queryID)
	return &status, err
}
