package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/jackc/pgx/v5"
)

func (operations *MaintenanceOperations) SwapRetention(ctx context.Context, task RetentionTask) (CompactionSwapResult, error) {
	authority := task.Authority
	if err := validateMaintenanceAuthority(authority); err != nil || task.RetentionFloor < 0 {
		return CompactionSwapResult{}, errors.New("invalid retention swap")
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
	var generation int64
	if err := tx.QueryRow(ctx, `SELECT catalog_generation FROM lanes WHERE tenant_id=$1 AND lane_id=$2 FOR UPDATE`, authority.TenantID, authority.LaneID).Scan(&generation); err != nil {
		return CompactionSwapResult{}, err
	}
	var state, owner string
	var fence, storageGeneration, floor int64
	var encoded []byte
	var manifestSHA *string
	if err := tx.QueryRow(ctx, `SELECT state,COALESCE(owner,''),fence,storage_generation,retention_floor_us,output_manifest,output_sha256 FROM maintenance_tasks WHERE task_id=$1 AND tenant_id=$2 AND lane_id=$3 AND kind='retain' FOR UPDATE`, authority.TaskID, authority.TenantID, authority.LaneID).Scan(&state, &owner, &fence, &storageGeneration, &floor, &encoded, &manifestSHA); err != nil {
		return CompactionSwapResult{}, err
	}
	if state == "completed" {
		if err := tx.Commit(ctx); err != nil {
			return CompactionSwapResult{}, err
		}
		return CompactionSwapResult{CatalogGeneration: generation, AlreadyCompleted: true}, nil
	}
	if state != "running" || owner != authority.Owner || fence != authority.Fence || storageGeneration != authority.StorageGeneration || floor != task.RetentionFloor {
		return CompactionSwapResult{}, ErrMaintenanceFence
	}
	if err := lockRunningMaintenance(ctx, tx, authority); err != nil {
		return CompactionSwapResult{}, err
	}
	work, err := loadCompactionExpectations(ctx, tx, authority.TaskID)
	if err != nil {
		return CompactionSwapResult{}, err
	}
	var fullyExpired bool
	if err := tx.QueryRow(ctx, `SELECT bool_and(f.max_received_time_us<$2)
		FROM maintenance_inputs mi JOIN files f ON f.tenant_id=mi.tenant_id AND f.bundle_id=mi.bundle_id AND f.role='analytics'
		WHERE mi.task_id=$1`, authority.TaskID, floor).Scan(&fullyExpired); err != nil {
		return CompactionSwapResult{}, err
	}
	var output *model.BundleManifest
	if !fullyExpired {
		if manifestSHA == nil || len(encoded) == 0 {
			return CompactionSwapResult{}, ErrMaintenanceFence
		}
		digest := sha256.Sum256(encoded)
		if hex.EncodeToString(digest[:]) != *manifestSHA {
			return CompactionSwapResult{}, errors.New("retention output manifest checksum mismatch")
		}
		var manifest model.BundleManifest
		if err := strictControlJSON(encoded, &manifest); err != nil {
			return CompactionSwapResult{}, err
		}
		if err := validateRetentionOutput(manifest, work, floor); err != nil {
			return CompactionSwapResult{}, err
		}
		output = &manifest
	} else if manifestSHA != nil || len(encoded) != 0 {
		return CompactionSwapResult{}, errors.New("fully expired retention task has output")
	}
	if generation == math.MaxInt64 {
		return CompactionSwapResult{}, errors.New("catalog generation exhausted")
	}
	newGeneration := generation + 1
	for _, input := range work.Inputs {
		result, err := tx.Exec(ctx, `UPDATE bundles SET valid_to_generation=$4,retired_at=clock_timestamp(),reserved_by=NULL WHERE tenant_id=$1 AND lane_id=$2 AND bundle_id=$3 AND valid_to_generation IS NULL AND reserved_by=$5 AND valid_from_generation=$6 AND identity_sha256=$7`, authority.TenantID, authority.LaneID, input.BundleID, newGeneration, authority.TaskID, input.ValidFromGeneration, input.IdentitySHA256)
		if err != nil || result.RowsAffected() != 1 {
			return CompactionSwapResult{}, errors.Join(ErrMaintenanceBusy, err)
		}
	}
	if output != nil {
		if err := insertCompactionBundle(ctx, tx, authority, work.Task.Partition, *output, newGeneration); err != nil {
			return CompactionSwapResult{}, err
		}
	}
	result, err := tx.Exec(ctx, `UPDATE lanes SET catalog_generation=$3 WHERE tenant_id=$1 AND lane_id=$2 AND catalog_generation=$4`, authority.TenantID, authority.LaneID, newGeneration, generation)
	if err != nil || result.RowsAffected() != 1 {
		return CompactionSwapResult{}, errors.Join(errors.New("retention lane generation update failed"), err)
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
