package control

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var (
	ErrQueryNotFound = errors.New("query not found")
	ErrQueryGone     = errors.New("query result expired")
)

type QueryResultArtifact struct {
	IntentID  string
	ObjectKey string
	Bytes     int64
	SHA256    string
}

type QueryStatus struct {
	QueryID        string
	TenantID       int64
	SnapshotID     string
	OperationKind  string
	State          string
	ErrorCode      string
	Deadline       time.Time
	ExpiresAt      time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
	PlanFiles      int
	PlanScans      int
	PlanBytes      int64
	PlanInputBytes int64
	OperationHash  string
	Operation      []byte
	DatasetHash    string
	Result         *QueryResultArtifact
}

func (operations *QueryOperations) GetQueryStatus(ctx context.Context, tokenHash [32]byte, tenantID int64, queryID string) (QueryStatus, error) {
	if tokenHash == ([32]byte{}) || tenantID <= 0 || uuid.Validate(queryID) != nil {
		return QueryStatus{}, ErrQueryNotFound
	}
	tx, err := operations.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return QueryStatus{}, err
	}
	defer tx.Rollback(ctx)
	authority, err := lockQueryAuthority(ctx, tx, tokenHash, tenantID, false, false)
	if err != nil {
		return QueryStatus{}, err
	}
	var result QueryStatus
	var userID int64
	var principal string
	var errorCode *string
	var intentID, objectKey, resultSHA *string
	var resultBytes *int64
	var now time.Time
	err = tx.QueryRow(ctx, `SELECT q.query_id::text,q.tenant_id,q.snapshot_id::text,q.operation_kind,q.state,q.error_code,
		q.deadline,q.expires_at,q.created_at,q.updated_at,q.plan_file_count,q.plan_scan_count,q.plan_bytes,q.plan_input_bytes,
		q.operation_hash,q.operation_bytes,s.dataset_hash,
		q.user_id,q.principal_ref,q.result_intent_id::text,oi.object_key,q.result_sha256,q.result_bytes,clock_timestamp()
		FROM query_jobs q JOIN query_snapshots s ON s.tenant_id=q.tenant_id AND s.snapshot_id=q.snapshot_id
		LEFT JOIN object_intents oi ON oi.tenant_id=q.tenant_id AND oi.intent_id=q.result_intent_id
		WHERE q.tenant_id=$1 AND q.query_id=$2 FOR SHARE OF q`, tenantID, queryID).Scan(
		&result.QueryID, &result.TenantID, &result.SnapshotID, &result.OperationKind, &result.State, &errorCode,
		&result.Deadline, &result.ExpiresAt, &result.CreatedAt, &result.UpdatedAt, &result.PlanFiles, &result.PlanScans, &result.PlanBytes, &result.PlanInputBytes,
		&result.OperationHash, &result.Operation, &result.DatasetHash,
		&userID, &principal, &intentID, &objectKey, &resultSHA, &resultBytes, &now)
	if errors.Is(err, pgx.ErrNoRows) {
		return QueryStatus{}, ErrQueryNotFound
	}
	if err != nil {
		return QueryStatus{}, err
	}
	if userID != authority.userID || principal != authority.principalHash {
		return QueryStatus{}, ErrQueryNotFound
	}
	if !result.ExpiresAt.After(now) {
		return QueryStatus{}, ErrQueryGone
	}
	if _, err := lockDurableQueryScope(ctx, tx, tenantID, result.SnapshotID, authority.generation); err != nil {
		return QueryStatus{}, err
	}
	if errorCode != nil {
		result.ErrorCode = *errorCode
	}
	if result.State == "succeeded" {
		if intentID == nil || objectKey == nil || resultSHA == nil || resultBytes == nil || *resultBytes <= 0 {
			return QueryStatus{}, errors.New("succeeded query result is incomplete")
		}
		result.Result = &QueryResultArtifact{IntentID: *intentID, ObjectKey: *objectKey, Bytes: *resultBytes, SHA256: *resultSHA}
	}
	if err := tx.Commit(ctx); err != nil {
		return QueryStatus{}, err
	}
	return result, nil
}
