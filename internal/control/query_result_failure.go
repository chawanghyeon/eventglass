package control

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// FailSucceededQueryResult converts a result that cannot be safely retrieved or
// decoded into an explicit terminal failure. Authorization deliberately matches
// status lookup so another session cannot probe or mutate the query.
func (operations *QueryOperations) FailSucceededQueryResult(ctx context.Context, tokenHash [32]byte, tenantID int64, queryID, errorCode string) error {
	if tokenHash == ([32]byte{}) || tenantID <= 0 || uuid.Validate(queryID) != nil || errorCode == "" || len(errorCode) > 64 {
		return ErrQueryNotFound
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
		WHERE tenant_id=$1 AND query_id=$2 FOR UPDATE`, tenantID, queryID).Scan(&userID, &principal, &state); errors.Is(err, pgx.ErrNoRows) {
		return ErrQueryNotFound
	} else if err != nil {
		return err
	}
	if userID != authority.userID || principal != authority.principalHash {
		return ErrQueryNotFound
	}
	if state == "failed" {
		return tx.Commit(ctx)
	}
	if state != "succeeded" {
		return ErrQueryTerminal
	}
	if _, err := tx.Exec(ctx, `UPDATE query_jobs SET state='failed',error_code=$3,updated_at=clock_timestamp()
		WHERE tenant_id=$1 AND query_id=$2 AND state='succeeded'`, tenantID, queryID, errorCode); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
