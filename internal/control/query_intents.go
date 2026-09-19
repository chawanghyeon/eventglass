package control

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type QueryIntentRegistration struct {
	Authority QueryTaskAuthority
	ObjectKey string
	Intent    IntentAuthority
}

func (operations *QueryOperations) RegisterQueryIntent(ctx context.Context, registration QueryIntentRegistration) error {
	authority, intent := registration.Authority, registration.Intent
	if err := validateQueryTaskAuthority(authority); err != nil || !validQueryObjectKey(registration.ObjectKey) || uuid.Validate(intent.IntentID) != nil || intent.Owner != authority.Owner || intent.Fence != authority.Fence || intent.Bytes <= 0 || intent.Bytes > 64<<20 || !validSHA(intent.SHA256) {
		return ErrQueryFenceStale
	}
	tx, err := operations.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := lockRuntimeGeneration(ctx, tx, authority.InstallationID, authority.StorageGeneration); err != nil {
		return err
	}
	var expiresAt time.Time
	if err := tx.QueryRow(ctx, `SELECT q.expires_at FROM query_jobs q JOIN query_tasks t ON t.tenant_id=q.tenant_id AND t.query_id=q.query_id
		WHERE t.tenant_id=$1 AND t.query_id=$2 AND t.stage=$3 AND t.level=$4 AND t.partition_id=$5
		AND q.state='running' AND q.deadline>clock_timestamp() AND t.state='running' AND t.owner=$6 AND t.fence=$7
		AND t.attempt=$8 AND t.lease_until>clock_timestamp() FOR SHARE OF q,t`, authority.TenantID, authority.QueryID,
		authority.Key.Stage, authority.Key.Level, authority.Key.PartitionID, authority.Owner, authority.Fence, authority.Attempt).Scan(&expiresAt); err != nil {
		return errors.Join(ErrQueryFenceStale, err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO object_intents(intent_id,installation_id,tenant_id,storage_generation,object_key,kind,state,
		owner,fence,expires_at,expected_bytes,expected_sha256,query_id,query_stage,query_level,query_partition_id,producer_generation,producer_fence)
		VALUES($1,$2,$3,$4,$5,'temporary','pending',$6,$7,$8,$9,$10,$11,$12,$13,$14,$4,$7)`,
		intent.IntentID, authority.InstallationID, authority.TenantID, authority.StorageGeneration, registration.ObjectKey,
		authority.Owner, authority.Fence, expiresAt, intent.Bytes, intent.SHA256, authority.QueryID,
		authority.Key.Stage, authority.Key.Level, authority.Key.PartitionID)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (operations *QueryOperations) MarkQueryIntentUploaded(ctx context.Context, authority QueryTaskAuthority, intent IntentAuthority) error {
	if err := validateQueryTaskAuthority(authority); err != nil || uuid.Validate(intent.IntentID) != nil || intent.Owner != authority.Owner || intent.Fence != authority.Fence || intent.Bytes <= 0 || intent.Bytes > 64<<20 || !validSHA(intent.SHA256) {
		return ErrQueryFenceStale
	}
	result, err := operations.pool.Exec(ctx, `UPDATE object_intents oi SET state='uploaded',uploaded_bytes=$10,uploaded_sha256=$11,updated_at=clock_timestamp()
		WHERE oi.intent_id=$1 AND oi.installation_id=$2 AND oi.storage_generation=$3 AND oi.tenant_id=$4
		AND oi.query_id=$5 AND oi.query_stage=$6 AND oi.query_level=$7 AND oi.query_partition_id=$8
		AND oi.owner=$9 AND oi.fence=$12 AND oi.producer_generation=$3 AND oi.producer_fence=$12
		AND oi.state='pending' AND oi.expires_at>clock_timestamp() AND oi.expected_bytes=$10 AND oi.expected_sha256=$11
		AND EXISTS (SELECT 1 FROM query_tasks t JOIN query_jobs q ON q.tenant_id=t.tenant_id AND q.query_id=t.query_id
			WHERE t.tenant_id=$4 AND t.query_id=$5 AND t.stage=$6 AND t.level=$7 AND t.partition_id=$8
			AND t.state='running' AND t.owner=$9 AND t.fence=$12 AND t.attempt=$13 AND t.lease_until>clock_timestamp()
			AND q.state='running' AND q.deadline>clock_timestamp())`, intent.IntentID, authority.InstallationID,
		authority.StorageGeneration, authority.TenantID, authority.QueryID, authority.Key.Stage, authority.Key.Level,
		authority.Key.PartitionID, authority.Owner, intent.Bytes, intent.SHA256, authority.Fence, authority.Attempt)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return ErrQueryFenceStale
	}
	return nil
}

func validQueryObjectKey(value string) bool {
	return safeObjectKey(value) && strings.HasPrefix(value, "v1/query/")
}
