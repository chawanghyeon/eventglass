package control

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

func (operations *QueryOperations) RecordKnownExpired(ctx context.Context, tokenHash [32]byte, tenantID, projectID int64, recordID string) (bool, error) {
	if tokenHash == ([32]byte{}) || tenantID <= 0 || projectID <= 0 || !validSHA(recordID) {
		return false, ErrQueryNotFound
	}
	tx, err := operations.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	authority, err := lockQueryAuthority(ctx, tx, tokenHash, tenantID, false, false)
	if err != nil {
		return false, err
	}
	if _, err := authorizeSnapshotProjects(ctx, tx, authority, tenantID, []int64{projectID}, nil); err != nil {
		return false, err
	}
	var receivedUS int64
	err = tx.QueryRow(ctx, `SELECT received_time_us FROM issue_occurrences
		WHERE tenant_id=$1 AND project_id=$2 AND record_id=$3`, tenantID, projectID, recordID).Scan(&receivedUS)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, tx.Commit(ctx)
	}
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return receivedUS < authority.retentionFloorUS, nil
}
