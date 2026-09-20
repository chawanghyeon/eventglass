package control

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

func RetentionFloorAfterChange(nowUS, oldFloorUS int64, oldDays, newDays int) (int64, error) {
	if oldDays < 1 || oldDays > 3650 || newDays < 1 || newDays > 3650 {
		return 0, errors.New("invalid retention days")
	}
	dayUS := int64(24 * time.Hour / time.Microsecond)
	result := oldFloorUS
	for _, days := range []int{oldDays, newDays} {
		cutoff := nowUS - int64(days)*dayUS
		if cutoff > result {
			result = cutoff
		}
	}
	return result, nil
}

func (operations *MaintenanceOperations) ChangeRetentionPolicy(ctx context.Context, installationID string, storageGeneration int64, expectedRevision int64, days int) (int64, int64, error) {
	if uuid.Validate(installationID) != nil || storageGeneration <= 0 || expectedRevision <= 0 || days < 1 || days > 3650 {
		return 0, 0, errors.New("invalid retention policy change")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback(ctx)
	var currentID string
	var generation, revision, floor, nowUS int64
	var oldDays int
	if err := tx.QueryRow(ctx, `SELECT installation_id::text,storage_generation,retention_revision,retention_days,retention_floor_us,floor(extract(epoch FROM clock_timestamp())*1000000)::bigint FROM installations WHERE singleton FOR UPDATE`).Scan(&currentID, &generation, &revision, &oldDays, &floor, &nowUS); err != nil {
		return 0, 0, err
	}
	if currentID != installationID || generation != storageGeneration {
		return 0, 0, ErrStorageGeneration
	}
	if revision != expectedRevision {
		return 0, 0, ErrRevisionConflict
	}
	newFloor, err := RetentionFloorAfterChange(nowUS, floor, oldDays, days)
	if err != nil {
		return 0, 0, err
	}
	revision++
	if _, err := tx.Exec(ctx, `UPDATE installations SET retention_days=$1,retention_revision=$2,retention_floor_us=$3,retention_tick_at=clock_timestamp() WHERE singleton`, days, revision, newFloor); err != nil {
		return 0, 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, err
	}
	return revision, newFloor, nil
}
