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

type QueryPlanContext struct {
	Authority     QueryCoordinatorAuthority
	Snapshot      model.QuerySnapshot
	OperationKind string
	OperationHash string
	Operation     []byte
	Deadline      time.Time
}

func (operations *QueryOperations) LoadQueryPlanContext(ctx context.Context, authority QueryCoordinatorAuthority) (QueryPlanContext, error) {
	if err := validateCoordinatorAuthority(authority); err != nil {
		return QueryPlanContext{}, err
	}
	tx, err := operations.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return QueryPlanContext{}, err
	}
	defer tx.Rollback(ctx)
	var result QueryPlanContext
	result.Authority = authority
	if err := tx.QueryRow(ctx, `SELECT snapshot_id::text FROM query_jobs WHERE tenant_id=$1 AND query_id=$2`, authority.TenantID, authority.QueryID).Scan(&result.Snapshot.SnapshotID); err != nil {
		return QueryPlanContext{}, err
	}
	scope, err := lockDurableQueryScope(ctx, tx, authority.TenantID, result.Snapshot.SnapshotID, authority.StorageGeneration)
	if err != nil {
		return QueryPlanContext{}, err
	}
	var owner, state string
	var snapshotID string
	var fence int64
	var leaseUntil, now time.Time
	err = tx.QueryRow(ctx, `SELECT snapshot_id::text,operation_kind,operation_hash,operation_bytes,deadline,
		coordinator_owner,coordinator_fence,lease_until,state,clock_timestamp()
		FROM query_jobs WHERE tenant_id=$1 AND query_id=$2 FOR SHARE`, authority.TenantID, authority.QueryID).Scan(
		&snapshotID, &result.OperationKind, &result.OperationHash, &result.Operation, &result.Deadline,
		&owner, &fence, &leaseUntil, &state, &now)
	if err != nil {
		return QueryPlanContext{}, err
	}
	if state != "planning" || snapshotID != result.Snapshot.SnapshotID || owner != authority.Owner || fence != authority.Fence || !leaseUntil.After(now) || !result.Deadline.After(now) {
		return QueryPlanContext{}, ErrQueryFenceStale
	}
	result.Snapshot.TenantID, result.Snapshot.UserID = scope.TenantID, scope.UserID
	result.Snapshot.PrincipalHash, result.Snapshot.StorageGeneration = scope.PrincipalHash, scope.StorageGeneration
	result.Snapshot.AuthRevision, result.Snapshot.TenantAuthRevision = scope.AuthRevision, scope.TenantRevision
	result.Snapshot.ExpiresAtUS, result.Snapshot.MaxUntilUS = scope.ExpiresAt.UnixMicro(), scope.MaxUntil.UnixMicro()
	result.Snapshot.ProjectIDs = append([]int64(nil), scope.ProjectIDs...)
	result.Snapshot.ProjectRevisions = append([]int64(nil), scope.ProjectRevisions...)
	if err := tx.QueryRow(ctx, `SELECT dataset_hash,dataset_bytes,retention_floor_us FROM query_snapshots
		WHERE tenant_id=$1 AND snapshot_id=$2 FOR SHARE`, authority.TenantID, result.Snapshot.SnapshotID).Scan(
		&result.Snapshot.DatasetSHA256, &result.Snapshot.DatasetBytes, &result.Snapshot.RetentionFloorUS); err != nil {
		return QueryPlanContext{}, err
	}
	rows, err := tx.Query(ctx, `SELECT lane_id,cut_seq,catalog_generation FROM snapshot_lanes
		WHERE tenant_id=$1 AND snapshot_id=$2 ORDER BY lane_id`, authority.TenantID, result.Snapshot.SnapshotID)
	if err != nil {
		return QueryPlanContext{}, err
	}
	index := 0
	for rows.Next() {
		if index >= model.LaneCount || rows.Scan(&result.Snapshot.Lanes[index].LaneID, &result.Snapshot.Lanes[index].CutSeq, &result.Snapshot.Lanes[index].CatalogGeneration) != nil || result.Snapshot.Lanes[index].LaneID != index {
			rows.Close()
			return QueryPlanContext{}, errors.New("snapshot lane vector is invalid")
		}
		index++
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return QueryPlanContext{}, err
	}
	rows.Close()
	if index != model.LaneCount {
		return QueryPlanContext{}, errors.New("snapshot lane vector is incomplete")
	}
	if err := tx.Commit(ctx); err != nil {
		return QueryPlanContext{}, err
	}
	return result, nil
}

func (operations *QueryOperations) CatalogPageForQuery(ctx context.Context, authority QueryCoordinatorAuthority, command CatalogCommand) ([]model.CatalogFile, error) {
	if err := validateCoordinatorAuthority(authority); err != nil || command.TenantID != authority.TenantID || command.SessionTokenHash != ([32]byte{}) || uuid.Validate(command.SnapshotID) != nil || !validSHA(command.DatasetSHA256) || len(command.DatasetBytes) < 2 || len(command.DatasetBytes) > 32768 || command.StartUS >= command.EndUS || command.Limit < 1 || command.Limit > MaxCatalogPageFiles {
		return nil, errors.New("invalid durable catalog request")
	}
	if command.TimeBasis != model.QueryTimeEvent && command.TimeBasis != model.QueryTimeReceived || command.AfterFileID != "" && uuid.Validate(command.AfterFileID) != nil {
		return nil, errors.New("invalid durable catalog scope")
	}
	seenKinds := map[model.Kind]bool{}
	for _, kind := range command.Kinds {
		if kind != model.KindError && kind != model.KindLog && kind != model.KindTransaction || seenKinds[kind] {
			return nil, errors.New("invalid durable catalog kind scope")
		}
		seenKinds[kind] = true
	}
	tx, err := operations.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	scope, err := lockDurableQueryScope(ctx, tx, authority.TenantID, command.SnapshotID, authority.StorageGeneration)
	if err != nil {
		return nil, err
	}
	var owner, state, datasetHash string
	var fence int64
	var leaseUntil, deadline, now time.Time
	var datasetBytes []byte
	var retentionFloor int64
	if err := tx.QueryRow(ctx, `SELECT q.coordinator_owner,q.coordinator_fence,q.lease_until,q.deadline,q.state,
		s.dataset_hash,s.dataset_bytes,s.retention_floor_us,clock_timestamp()
		FROM query_jobs q JOIN query_snapshots s ON s.tenant_id=q.tenant_id AND s.snapshot_id=q.snapshot_id
		WHERE q.tenant_id=$1 AND q.query_id=$2 AND q.snapshot_id=$3 FOR SHARE OF q,s`, authority.TenantID, authority.QueryID, command.SnapshotID).Scan(
		&owner, &fence, &leaseUntil, &deadline, &state, &datasetHash, &datasetBytes, &retentionFloor, &now); err != nil {
		return nil, err
	}
	if state != "planning" || owner != authority.Owner || fence != authority.Fence || !leaseUntil.After(now) || !deadline.After(now) || datasetHash != command.DatasetSHA256 || !bytes.Equal(datasetBytes, command.DatasetBytes) || scope.SnapshotID != command.SnapshotID {
		return nil, ErrQueryFenceStale
	}
	result, err := catalogPageRows(ctx, tx, command, retentionFloor)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

func (operations *QueryOperations) HeartbeatQueryCoordinator(ctx context.Context, authority QueryCoordinatorAuthority) (time.Time, error) {
	if err := validateCoordinatorAuthority(authority); err != nil {
		return time.Time{}, err
	}
	var lease time.Time
	err := operations.pool.QueryRow(ctx, `UPDATE query_jobs SET lease_until=LEAST(deadline,clock_timestamp()+interval '60 seconds'),updated_at=clock_timestamp()
		WHERE tenant_id=$1 AND query_id=$2 AND state='planning' AND coordinator_owner=$3 AND coordinator_fence=$4
		AND lease_until>clock_timestamp() AND deadline>clock_timestamp() RETURNING lease_until`, authority.TenantID, authority.QueryID, authority.Owner, authority.Fence).Scan(&lease)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, ErrQueryFenceStale
	}
	return lease, err
}

func (operations *QueryOperations) FailQueryPlanning(ctx context.Context, authority QueryCoordinatorAuthority, errorCode string) error {
	if err := validateCoordinatorAuthority(authority); err != nil || errorCode == "" || len(errorCode) > 64 {
		return ErrQueryFenceStale
	}
	result, err := operations.pool.Exec(ctx, `UPDATE query_jobs SET state='failed',error_code=$5,coordinator_owner=NULL,
		lease_until=NULL,updated_at=clock_timestamp() WHERE tenant_id=$1 AND query_id=$2 AND state='planning'
		AND coordinator_owner=$3 AND coordinator_fence=$4`, authority.TenantID, authority.QueryID, authority.Owner, authority.Fence, errorCode)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return ErrQueryFenceStale
	}
	return nil
}

func validateCoordinatorAuthority(authority QueryCoordinatorAuthority) error {
	if uuid.Validate(authority.QueryID) != nil || authority.TenantID <= 0 || authority.Owner == "" || len(authority.Owner) > 128 || authority.Fence <= 0 || authority.StorageGeneration <= 0 {
		return ErrQueryFenceStale
	}
	return nil
}
