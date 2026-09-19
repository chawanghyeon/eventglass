package control

import (
	"bytes"
	"context"
	"errors"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const MaxCatalogPageFiles = 256

type CatalogCommand struct {
	SessionTokenHash [32]byte
	TenantID         int64
	SnapshotID       string
	DatasetSHA256    string
	DatasetBytes     []byte
	TimeBasis        model.QueryTimeBasis
	StartUS          int64
	EndUS            int64
	Kinds            []model.Kind
	AfterFileID      string
	Limit            int
}

func (operations *QueryOperations) CatalogPage(ctx context.Context, command CatalogCommand) ([]model.CatalogFile, error) {
	if err := validateCatalogCommand(command); err != nil {
		return nil, err
	}
	var lastErr error
	for attempt := 0; attempt <= SnapshotRetryLimit; attempt++ {
		result, err := operations.catalogPageOnce(ctx, command)
		if err == nil || !retryableSnapshotError(err) {
			return result, err
		}
		lastErr = err
		if err := waitSnapshotRetry(ctx, attempt); err != nil {
			return nil, err
		}
	}
	return nil, lastErr
}

func (operations *QueryOperations) catalogPageOnce(ctx context.Context, command CatalogCommand) ([]model.CatalogFile, error) {
	tx, err := operations.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	authority, err := lockQueryAuthority(ctx, tx, command.SessionTokenHash, command.TenantID, false, false)
	if err != nil {
		return nil, err
	}
	snapshot, projectRevisions, err := loadSnapshotForUpdate(ctx, tx, command.TenantID, command.SnapshotID)
	if err != nil {
		return nil, err
	}
	if snapshot.UserID != authority.userID || snapshot.PrincipalHash != authority.principalHash || snapshot.DatasetSHA256 != command.DatasetSHA256 || !bytes.Equal(snapshot.DatasetBytes, command.DatasetBytes) {
		return nil, ErrSnapshotMismatch
	}
	if snapshot.StorageGeneration != authority.generation {
		return nil, ErrStorageGeneration
	}
	if snapshot.AuthRevision != authority.authRevision || snapshot.TenantAuthRevision != authority.tenantAuthRevision {
		return nil, ErrForbidden
	}
	if snapshot.ExpiresAtUS <= authority.now.UnixMicro() {
		return nil, ErrSnapshotExpired
	}
	if _, err := authorizeSnapshotProjects(ctx, tx, authority, command.TenantID, snapshot.ProjectIDs, projectRevisions); err != nil {
		return nil, err
	}
	kinds := command.Kinds
	if len(kinds) == 0 {
		kinds = []model.Kind{model.KindError, model.KindLog, model.KindTransaction}
	}
	kindValues := make([]string, len(kinds))
	for index, kind := range kinds {
		kindValues[index] = string(kind)
	}
	timeMax, timeMin := "f.max_event_time_us", "f.min_event_time_us"
	if command.TimeBasis == model.QueryTimeReceived {
		timeMax, timeMin = "f.max_received_time_us", "f.min_received_time_us"
	}
	var after any
	if command.AfterFileID != "" {
		after = command.AfterFileID
	}
	statement := `SELECT f.file_id::text,b.bundle_id::text,oi.object_key,f.bytes,f.full_sha256,f.row_count,
		f.min_event_time_us,f.max_event_time_us,f.min_received_time_us,f.max_received_time_us,
		f.min_batch_seq,f.max_batch_seq,b.lane_id,b.kind,
		COALESCE(array_agg(fb.sha256 ORDER BY fb.block_index) FILTER (WHERE fb.file_id IS NOT NULL),ARRAY[]::text[]),
		COALESCE(min(fb.block_index),-1),COALESCE(max(fb.block_index),-1),count(fb.file_id)
		FROM snapshot_lanes sl
		JOIN bundles b ON b.tenant_id=sl.tenant_id AND b.lane_id=sl.lane_id
		JOIN files f ON f.tenant_id=b.tenant_id AND f.bundle_id=b.bundle_id AND f.role='analytics'
		JOIN object_intents oi ON oi.tenant_id=f.tenant_id AND oi.intent_id=f.intent_id AND oi.state='referenced'
		LEFT JOIN file_blocks fb ON fb.file_id=f.file_id
		WHERE sl.tenant_id=$1 AND sl.snapshot_id=$2
		AND b.valid_from_generation<=sl.catalog_generation AND (b.valid_to_generation IS NULL OR sl.catalog_generation<b.valid_to_generation)
		AND b.input_seq_max<=sl.cut_seq AND b.kind=ANY($3::text[])
		AND ` + timeMax + `>=$4 AND ` + timeMin + `<$5 AND f.max_received_time_us>=$6
		AND ($7::uuid IS NULL OR f.file_id>$7::uuid)
		AND EXISTS (SELECT 1 FROM bundle_projects bp JOIN snapshot_projects sp
			ON sp.tenant_id=bp.tenant_id AND sp.project_id=bp.project_id AND sp.snapshot_id=$2
			WHERE bp.tenant_id=b.tenant_id AND bp.bundle_id=b.bundle_id)
		GROUP BY f.file_id,b.bundle_id,oi.object_key,b.lane_id,b.kind
		ORDER BY f.file_id LIMIT $8`
	rows, err := tx.Query(ctx, statement, command.TenantID, command.SnapshotID, kindValues, command.StartUS, command.EndUS, snapshot.RetentionFloorUS, after, command.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]model.CatalogFile, 0, command.Limit)
	for rows.Next() {
		var file model.CatalogFile
		var firstBlock, lastBlock, blockCount int
		if err := rows.Scan(&file.FileID, &file.BundleID, &file.ObjectKey, &file.Bytes, &file.SHA256, &file.RowCount,
			&file.MinEventTimeUS, &file.MaxEventTimeUS, &file.MinReceivedTimeUS, &file.MaxReceivedTimeUS,
			&file.MinBatchSeq, &file.MaxBatchSeq, &file.LaneID, &file.Kind, &file.BlockSHA256, &firstBlock, &lastBlock, &blockCount); err != nil {
			return nil, err
		}
		if blockCount < 1 || firstBlock != 0 || lastBlock != blockCount-1 || len(file.BlockSHA256) != blockCount {
			return nil, errors.New("catalog file block manifest is not contiguous")
		}
		for _, checksum := range file.BlockSHA256 {
			if !validSHA(checksum) {
				return nil, errors.New("catalog file block checksum is invalid")
			}
		}
		result = append(result, file)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

func validateCatalogCommand(command CatalogCommand) error {
	if command.SessionTokenHash == ([32]byte{}) || command.TenantID <= 0 || uuid.Validate(command.SnapshotID) != nil || !validSHA(command.DatasetSHA256) || len(command.DatasetBytes) < 2 || len(command.DatasetBytes) > 32768 || command.StartUS >= command.EndUS || command.Limit < 1 || command.Limit > MaxCatalogPageFiles {
		return errors.New("invalid catalog command")
	}
	if command.TimeBasis != model.QueryTimeEvent && command.TimeBasis != model.QueryTimeReceived {
		return errors.New("invalid catalog time basis")
	}
	if command.AfterFileID != "" && uuid.Validate(command.AfterFileID) != nil {
		return errors.New("invalid catalog cursor")
	}
	seen := map[model.Kind]bool{}
	for _, kind := range command.Kinds {
		if kind != model.KindError && kind != model.KindLog && kind != model.KindTransaction || seen[kind] {
			return errors.New("invalid catalog kind scope")
		}
		seen[kind] = true
	}
	return nil
}
