package control

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (operations *QueryOperations) CancelQuery(ctx context.Context, tokenHash [32]byte, tenantID int64, queryID string) error {
	if tokenHash == ([32]byte{}) || tenantID <= 0 || uuid.Validate(queryID) != nil {
		return ErrQueryFenceStale
	}
	tx, err := operations.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	authority, err := lockQueryAuthority(ctx, tx, tokenHash, tenantID, false, false)
	if err != nil {
		return err
	}
	var userID int64
	var principal, state string
	if err := tx.QueryRow(ctx, `SELECT user_id,principal_ref,state FROM query_jobs
		WHERE tenant_id=$1 AND query_id=$2 FOR UPDATE`, tenantID, queryID).Scan(&userID, &principal, &state); err != nil {
		return errors.Join(ErrQueryFenceStale, err)
	}
	if userID != authority.userID || principal != authority.principalHash {
		return ErrQueryNotFound
	}
	if state == "canceled" {
		return tx.Commit(ctx)
	}
	if state == "succeeded" || state == "failed" {
		return ErrQueryTerminal
	}
	if _, err := tx.Exec(ctx, `UPDATE query_jobs SET state='canceled',coordinator_owner=NULL,lease_until=NULL,
		error_code='canceled',updated_at=clock_timestamp() WHERE tenant_id=$1 AND query_id=$2`, tenantID, queryID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE query_tasks SET state='canceled',owner=NULL,lease_until=NULL,error_code='canceled'
		WHERE tenant_id=$1 AND query_id=$2 AND state IN ('queued','running')`, tenantID, queryID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
