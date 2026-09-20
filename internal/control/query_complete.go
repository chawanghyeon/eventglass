package control

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (operations *QueryOperations) CompleteQueryTask(ctx context.Context, command CompleteQueryTaskCommand) error {
	if err := validateQueryTaskAuthority(command.Authority); err != nil || uuid.Validate(command.IntentID) != nil || !validSHA(command.SHA256) || command.Rows < 0 || command.Rows > 20000 || command.Bytes <= 0 || command.Bytes > 64<<20 || command.CacheBytes < 0 || command.CacheBytes > 4<<30 || !validBlockManifest(command.Bytes, command.BlockSHA256) {
		return ErrQueryFenceStale
	}
	var existingIntent, existingSHA *string
	var existingRows, existingBytes *int64
	err := operations.pool.QueryRow(ctx, `SELECT result_intent_id::text,result_sha256,result_rows,result_bytes
		FROM query_tasks WHERE tenant_id=$1 AND query_id=$2 AND stage=$3 AND level=$4 AND partition_id=$5 AND state='succeeded'`,
		command.Authority.TenantID, command.Authority.QueryID, command.Authority.Key.Stage, command.Authority.Key.Level,
		command.Authority.Key.PartitionID).Scan(&existingIntent, &existingSHA, &existingRows, &existingBytes)
	if err == nil {
		if existingIntent != nil && existingSHA != nil && existingRows != nil && existingBytes != nil &&
			*existingIntent == command.IntentID && *existingSHA == command.SHA256 && *existingRows == command.Rows && *existingBytes == command.Bytes {
			var blocks []string
			if err := operations.pool.QueryRow(ctx, `SELECT COALESCE(array_agg(sha256 ORDER BY block_index),ARRAY[]::text[]) FROM query_task_blocks WHERE tenant_id=$1 AND query_id=$2 AND stage=$3 AND level=$4 AND partition_id=$5`,
				command.Authority.TenantID, command.Authority.QueryID, command.Authority.Key.Stage, command.Authority.Key.Level, command.Authority.Key.PartitionID).Scan(&blocks); err == nil && slices.Equal(blocks, command.BlockSHA256) {
				return nil
			}
		}
		return ErrQueryFenceStale
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	tx, err := operations.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var snapshotID string
	if err := tx.QueryRow(ctx, `SELECT snapshot_id::text FROM query_jobs WHERE tenant_id=$1 AND query_id=$2`, command.Authority.TenantID, command.Authority.QueryID).Scan(&snapshotID); err != nil {
		return errors.Join(ErrQueryFenceStale, err)
	}
	if _, err := lockDurableQueryScope(ctx, tx, command.Authority.TenantID, snapshotID, command.Authority.StorageGeneration); err != nil {
		return err
	}
	var queryState string
	var deadline, now time.Time
	if err := tx.QueryRow(ctx, `SELECT state,deadline,clock_timestamp() FROM query_jobs WHERE tenant_id=$1 AND query_id=$2 FOR UPDATE`,
		command.Authority.TenantID, command.Authority.QueryID).Scan(&queryState, &deadline, &now); err != nil {
		return err
	}
	if queryState != "running" || !deadline.After(now) {
		return ErrQueryTerminal
	}
	var taskState, owner string
	var fence int64
	var attempt int
	var leaseUntil time.Time
	if err := tx.QueryRow(ctx, `SELECT state,owner,fence,attempt,lease_until FROM query_tasks
		WHERE tenant_id=$1 AND query_id=$2 AND stage=$3 AND level=$4 AND partition_id=$5 FOR UPDATE`,
		command.Authority.TenantID, command.Authority.QueryID, command.Authority.Key.Stage, command.Authority.Key.Level,
		command.Authority.Key.PartitionID).Scan(&taskState, &owner, &fence, &attempt, &leaseUntil); err != nil {
		return err
	}
	if taskState != "running" || owner != command.Authority.Owner || fence != command.Authority.Fence || attempt != command.Authority.Attempt || !leaseUntil.After(now) {
		return ErrQueryFenceStale
	}
	var intentBytes int64
	var intentSHA, intentState string
	if err := tx.QueryRow(ctx, `SELECT state,uploaded_bytes,uploaded_sha256 FROM object_intents
		WHERE tenant_id=$1 AND intent_id=$2 AND query_id=$3 AND query_stage=$4 AND query_level=$5 AND query_partition_id=$6
		AND producer_generation=$7 AND producer_fence=$8 AND owner=$9 AND fence=$8 FOR UPDATE`,
		command.Authority.TenantID, command.IntentID, command.Authority.QueryID, command.Authority.Key.Stage,
		command.Authority.Key.Level, command.Authority.Key.PartitionID, command.Authority.StorageGeneration,
		command.Authority.Fence, command.Authority.Owner).Scan(&intentState, &intentBytes, &intentSHA); err != nil {
		return errors.Join(ErrQueryFenceStale, err)
	}
	if intentState != "uploaded" || intentBytes != command.Bytes || intentSHA != command.SHA256 {
		return ErrQueryFenceStale
	}
	budgetResult, err := tx.Exec(ctx, `UPDATE query_level_budgets SET committed_bytes=committed_bytes+$4
		WHERE tenant_id=$1 AND query_id=$2 AND level=$3 AND committed_bytes+$4<=reserved_bytes`,
		command.Authority.TenantID, command.Authority.QueryID, command.Authority.Key.Level, command.Bytes)
	if err != nil {
		return err
	}
	if budgetResult.RowsAffected() != 1 {
		return ErrQueryLimitExceeded
	}
	if _, err := tx.Exec(ctx, `UPDATE object_intents SET state='referenced',updated_at=clock_timestamp()
		WHERE tenant_id=$1 AND intent_id=$2 AND state='uploaded'`, command.Authority.TenantID, command.IntentID); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE query_tasks SET state='succeeded',owner=NULL,lease_until=NULL,
		result_intent_id=$6,result_sha256=$7,result_rows=$8,result_bytes=$9
		WHERE tenant_id=$1 AND query_id=$2 AND stage=$3 AND level=$4 AND partition_id=$5
		AND state='running' AND fence=$10 AND attempt=$11`, command.Authority.TenantID, command.Authority.QueryID,
		command.Authority.Key.Stage, command.Authority.Key.Level, command.Authority.Key.PartitionID,
		command.IntentID, command.SHA256, command.Rows, command.Bytes, command.Authority.Fence, command.Authority.Attempt)
	if err != nil || result.RowsAffected() != 1 {
		return errors.Join(ErrQueryFenceStale, err)
	}
	for index, checksum := range command.BlockSHA256 {
		if _, err := tx.Exec(ctx, `INSERT INTO query_task_blocks(tenant_id,query_id,stage,level,partition_id,block_index,sha256) VALUES($1,$2,$3,$4,$5,$6,$7)`,
			command.Authority.TenantID, command.Authority.QueryID, command.Authority.Key.Stage, command.Authority.Key.Level, command.Authority.Key.PartitionID, index, checksum); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE query_jobs SET cache_bytes=cache_bytes+$3 WHERE tenant_id=$1 AND query_id=$2`, command.Authority.TenantID, command.Authority.QueryID, command.CacheBytes); err != nil {
		return err
	}
	var hasConsumer bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM query_task_inputs WHERE tenant_id=$1 AND query_id=$2
		AND producer_stage=$3 AND producer_level=$4 AND producer_partition_id=$5)`, command.Authority.TenantID,
		command.Authority.QueryID, command.Authority.Key.Stage, command.Authority.Key.Level, command.Authority.Key.PartitionID).Scan(&hasConsumer); err != nil {
		return err
	}
	if !hasConsumer {
		jobResult, err := tx.Exec(ctx, `UPDATE query_jobs SET state='succeeded',result_intent_id=$3,result_sha256=$4,
			result_bytes=$5,coordinator_owner=NULL,lease_until=NULL,updated_at=clock_timestamp()
			WHERE tenant_id=$1 AND query_id=$2 AND state='running' AND deadline>clock_timestamp()`,
			command.Authority.TenantID, command.Authority.QueryID, command.IntentID, command.SHA256, command.Bytes)
		if err != nil || jobResult.RowsAffected() != 1 {
			return errors.Join(ErrQueryTerminal, err)
		}
	}
	return tx.Commit(ctx)
}

func validBlockManifest(size int64, blocks []string) bool {
	if len(blocks) != int((size+model.FileBlockBytes-1)/model.FileBlockBytes) {
		return false
	}
	for _, checksum := range blocks {
		if !validSHA(checksum) {
			return false
		}
	}
	return true
}

func (operations *QueryOperations) FailQueryTask(ctx context.Context, authority QueryTaskAuthority, errorCode string, transient bool) error {
	if err := validateQueryTaskAuthority(authority); err != nil || errorCode == "" || len(errorCode) > 64 {
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
	var queryState string
	var deadline, now time.Time
	if err := tx.QueryRow(ctx, `SELECT state,deadline,clock_timestamp() FROM query_jobs WHERE tenant_id=$1 AND query_id=$2 FOR UPDATE`, authority.TenantID, authority.QueryID).Scan(&queryState, &deadline, &now); err != nil {
		return err
	}
	if queryState != "running" || !deadline.After(now) {
		return ErrQueryTerminal
	}
	nextState := "failed"
	if transient && authority.Attempt < 3 {
		nextState = "queued"
	}
	result, err := tx.Exec(ctx, `UPDATE query_tasks SET state=$9,owner=NULL,lease_until=NULL,error_code=$10,
		retry_at=CASE WHEN $9='queued' THEN clock_timestamp()+($11::bigint*interval '1 millisecond') ELSE retry_at END
		WHERE tenant_id=$1 AND query_id=$2 AND stage=$3 AND level=$4 AND partition_id=$5
		AND state='running' AND owner=$6 AND fence=$7 AND attempt=$8`, authority.TenantID, authority.QueryID,
		authority.Key.Stage, authority.Key.Level, authority.Key.PartitionID, authority.Owner, authority.Fence, authority.Attempt,
		nextState, errorCode, int64(authority.Attempt*100))
	if err != nil || result.RowsAffected() != 1 {
		return errors.Join(ErrQueryFenceStale, err)
	}
	if nextState == "failed" {
		if _, err := tx.Exec(ctx, `UPDATE query_jobs SET state='failed',error_code=$3,coordinator_owner=NULL,lease_until=NULL,updated_at=clock_timestamp()
			WHERE tenant_id=$1 AND query_id=$2`, authority.TenantID, authority.QueryID, errorCode); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE query_tasks SET state='canceled',owner=NULL,lease_until=NULL
			WHERE tenant_id=$1 AND query_id=$2 AND state='queued'`, authority.TenantID, authority.QueryID); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
