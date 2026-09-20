package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/jackc/pgx/v5"
)

type CompactionSwapResult struct {
	CatalogGeneration int64
	AlreadyCompleted  bool
}

func (operations *MaintenanceOperations) SwapCompaction(ctx context.Context, authority MaintenanceAuthority) (CompactionSwapResult, error) {
	if err := validateMaintenanceAuthority(authority); err != nil {
		return CompactionSwapResult{}, err
	}
	tx, err := operations.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return CompactionSwapResult{}, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = on"); err != nil {
		return CompactionSwapResult{}, err
	}
	if err := lockRuntimeGeneration(ctx, tx, authority.InstallationID, authority.StorageGeneration); err != nil {
		return CompactionSwapResult{}, err
	}
	if _, err := tx.Exec(ctx, `SELECT tenant_id FROM tenants WHERE tenant_id=$1 FOR SHARE`, authority.TenantID); err != nil {
		return CompactionSwapResult{}, err
	}
	var generation int64
	if err := tx.QueryRow(ctx, `SELECT catalog_generation FROM lanes WHERE tenant_id=$1 AND lane_id=$2 FOR UPDATE`, authority.TenantID, authority.LaneID).Scan(&generation); err != nil {
		return CompactionSwapResult{}, err
	}
	var state, owner string
	var fence, storageGeneration int64
	var encoded []byte
	var manifestSHA *string
	err = tx.QueryRow(ctx, `SELECT state,COALESCE(owner,''),fence,storage_generation,output_manifest,output_sha256 FROM maintenance_tasks WHERE task_id=$1 AND tenant_id=$2 AND lane_id=$3 FOR UPDATE`, authority.TaskID, authority.TenantID, authority.LaneID).Scan(&state, &owner, &fence, &storageGeneration, &encoded, &manifestSHA)
	if err != nil {
		return CompactionSwapResult{}, err
	}
	if state == "completed" {
		if err := tx.Commit(ctx); err != nil {
			return CompactionSwapResult{}, err
		}
		return CompactionSwapResult{CatalogGeneration: generation, AlreadyCompleted: true}, nil
	}
	if state != "running" || owner != authority.Owner || fence != authority.Fence || storageGeneration != authority.StorageGeneration || manifestSHA == nil || len(encoded) == 0 {
		return CompactionSwapResult{}, ErrMaintenanceFence
	}
	if err := lockRunningMaintenance(ctx, tx, authority); err != nil {
		return CompactionSwapResult{}, err
	}
	digest := sha256.Sum256(encoded)
	if hex.EncodeToString(digest[:]) != *manifestSHA {
		return CompactionSwapResult{}, errors.New("compaction output manifest checksum mismatch")
	}
	var output model.BundleManifest
	if err := strictControlJSON(encoded, &output); err != nil {
		return CompactionSwapResult{}, err
	}
	work, err := loadCompactionExpectations(ctx, tx, authority.TaskID)
	if err != nil {
		return CompactionSwapResult{}, err
	}
	if err := validateCompactionOutput(output, work); err != nil {
		return CompactionSwapResult{}, err
	}
	if generation == math.MaxInt64 {
		return CompactionSwapResult{}, errors.New("catalog generation exhausted")
	}
	newGeneration := generation + 1
	for _, input := range work.Inputs {
		result, err := tx.Exec(ctx, `UPDATE bundles SET valid_to_generation=$4,retired_at=clock_timestamp(),reserved_by=NULL
			WHERE tenant_id=$1 AND lane_id=$2 AND bundle_id=$3 AND valid_to_generation IS NULL AND reserved_by=$5 AND valid_from_generation=$6 AND identity_sha256=$7`,
			authority.TenantID, authority.LaneID, input.BundleID, newGeneration, authority.TaskID, input.ValidFromGeneration, input.IdentitySHA256)
		if err != nil || result.RowsAffected() != 1 {
			return CompactionSwapResult{}, errors.Join(ErrMaintenanceBusy, err)
		}
	}
	if err := insertCompactionBundle(ctx, tx, authority, work.Task.Partition, output, newGeneration); err != nil {
		return CompactionSwapResult{}, err
	}
	result, err := tx.Exec(ctx, `UPDATE lanes SET catalog_generation=$3 WHERE tenant_id=$1 AND lane_id=$2 AND catalog_generation=$4`, authority.TenantID, authority.LaneID, newGeneration, generation)
	if err != nil || result.RowsAffected() != 1 {
		return CompactionSwapResult{}, errors.Join(errors.New("compaction lane generation update failed"), err)
	}
	result, err = tx.Exec(ctx, `UPDATE maintenance_tasks SET state='completed',owner=NULL,lease_until=NULL,updated_at=clock_timestamp() WHERE task_id=$1 AND state='running' AND owner=$2 AND fence=$3`, authority.TaskID, authority.Owner, authority.Fence)
	if err != nil || result.RowsAffected() != 1 {
		return CompactionSwapResult{}, errors.Join(ErrMaintenanceFence, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return CompactionSwapResult{}, err
	}
	return CompactionSwapResult{CatalogGeneration: newGeneration}, nil
}

func insertCompactionBundle(ctx context.Context, tx pgx.Tx, authority MaintenanceAuthority, partition CompactionPartition, bundle model.BundleManifest, generation int64) error {
	_, err := tx.Exec(ctx, `INSERT INTO bundles(bundle_id,tenant_id,lane_id,schema_version,grouping_version,event_day,kind,input_seq_min,input_seq_max,row_count,identity_sha256,valid_from_generation)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, bundle.BundleID, authority.TenantID, authority.LaneID, partition.SchemaVersion, partition.GroupingVersion, partition.EventDay, partition.Kind, bundle.InputSeqMin, bundle.InputSeqMax, bundle.RowCount, bundle.IdentitySHA256, generation)
	if err != nil {
		return err
	}
	for _, file := range []model.FileManifest{bundle.Analytics, bundle.Payload} {
		var state, taskID string
		var bytes int64
		var checksum string
		if err := tx.QueryRow(ctx, `SELECT state,maintenance_task_id::text,expected_bytes,expected_sha256 FROM object_intents WHERE tenant_id=$1 AND intent_id=$2 FOR UPDATE`, authority.TenantID, file.IntentID).Scan(&state, &taskID, &bytes, &checksum); err != nil {
			return err
		}
		if state != "referenced" || taskID != authority.TaskID || bytes != file.Bytes || checksum != file.SHA256 {
			return ErrIntentStale
		}
		if _, err := tx.Exec(ctx, `INSERT INTO files(file_id,tenant_id,bundle_id,intent_id,role,bytes,full_sha256,row_count,min_event_time_us,max_event_time_us,min_received_time_us,max_received_time_us,min_batch_seq,max_batch_seq)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`, file.FileID, authority.TenantID, bundle.BundleID, file.IntentID, file.Role, file.Bytes, file.SHA256, file.RowCount, file.MinEventTimeUS, file.MaxEventTimeUS, file.MinReceivedTimeUS, file.MaxReceivedTimeUS, file.MinBatchSeq, file.MaxBatchSeq); err != nil {
			return err
		}
		for _, block := range file.Blocks {
			if _, err := tx.Exec(ctx, `INSERT INTO file_blocks(file_id,block_index,sha256) VALUES($1,$2,$3)`, file.FileID, block.Index, block.SHA256); err != nil {
				return err
			}
		}
	}
	for _, projectID := range bundle.ProjectIDs {
		if _, err := tx.Exec(ctx, `INSERT INTO bundle_projects(tenant_id,bundle_id,project_id) VALUES($1,$2,$3)`, authority.TenantID, bundle.BundleID, projectID); err != nil {
			return err
		}
	}
	return nil
}

func canonicalCompactionManifest(bundle model.BundleManifest) ([]byte, string, error) {
	encoded, err := json.Marshal(bundle)
	if err != nil {
		return nil, "", err
	}
	digest := sha256.Sum256(encoded)
	return encoded, hex.EncodeToString(digest[:]), nil
}
