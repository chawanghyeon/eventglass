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
	// Internal conservative pruning hint; mandatory snapshot/row scope remains.
	MinimumBatchSeq [model.LaneCount]int64
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
	result, err := catalogPageRows(ctx, tx, command, snapshot.RetentionFloorUS)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

func catalogPageRows(ctx context.Context, tx pgx.Tx, command CatalogCommand, retentionFloorUS int64) ([]model.CatalogFile, error) {
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
	statement := `WITH page AS MATERIALIZED (
		SELECT f.file_id,b.bundle_id,oi.object_key,f.bytes,f.full_sha256,f.row_count,
			f.min_event_time_us,f.max_event_time_us,f.min_received_time_us,f.max_received_time_us,
			f.min_batch_seq,f.max_batch_seq,b.lane_id,b.kind,pf.file_id AS payload_file_id,
			poi.object_key AS payload_object_key,pf.bytes AS payload_bytes,pf.full_sha256 AS payload_sha256,
			NOT EXISTS (SELECT 1 FROM bundle_projects bp WHERE bp.tenant_id=b.tenant_id AND bp.bundle_id=b.bundle_id
				AND NOT EXISTS (SELECT 1 FROM snapshot_projects sp WHERE sp.tenant_id=bp.tenant_id AND sp.project_id=bp.project_id AND sp.snapshot_id=$2)) AS all_projects_selected
		FROM snapshot_lanes sl
		JOIN bundles b ON b.tenant_id=sl.tenant_id AND b.lane_id=sl.lane_id
		JOIN files f ON f.tenant_id=b.tenant_id AND f.bundle_id=b.bundle_id AND f.role='analytics'
		JOIN object_intents oi ON oi.tenant_id=f.tenant_id AND oi.intent_id=f.intent_id AND oi.state='referenced'
		JOIN files pf ON pf.tenant_id=b.tenant_id AND pf.bundle_id=b.bundle_id AND pf.role='payload'
		JOIN object_intents poi ON poi.tenant_id=pf.tenant_id AND poi.intent_id=pf.intent_id AND poi.state='referenced'
		WHERE sl.tenant_id=$1 AND sl.snapshot_id=$2
		AND b.valid_from_generation<=sl.catalog_generation AND (b.valid_to_generation IS NULL OR sl.catalog_generation<b.valid_to_generation)
		AND b.input_seq_max<=sl.cut_seq AND b.kind=ANY($3::text[])
		AND ` + timeMax + `>=$4 AND ` + timeMin + `<$5 AND f.max_received_time_us>=$6
		AND ($7::uuid IS NULL OR f.file_id>$7::uuid)
		AND f.max_batch_seq >= ($9::bigint[])[sl.lane_id+1]
		AND EXISTS (SELECT 1 FROM bundle_projects bp JOIN snapshot_projects sp
			ON sp.tenant_id=bp.tenant_id AND sp.project_id=bp.project_id AND sp.snapshot_id=$2
			WHERE bp.tenant_id=b.tenant_id AND bp.bundle_id=b.bundle_id)
		ORDER BY f.file_id LIMIT $8
	)
	SELECT p.file_id::text,p.bundle_id::text,p.object_key,p.bytes,p.full_sha256,p.row_count,
		p.min_event_time_us,p.max_event_time_us,p.min_received_time_us,p.max_received_time_us,
		p.min_batch_seq,p.max_batch_seq,p.lane_id,p.kind,
		COALESCE(analytics_blocks.sha256,ARRAY[]::text[]),COALESCE(analytics_blocks.first_block,-1),
		COALESCE(analytics_blocks.last_block,-1),COALESCE(analytics_blocks.block_count,0),
		p.payload_file_id::text,p.payload_object_key,p.payload_bytes,p.payload_sha256,
		COALESCE(payload_blocks.sha256,ARRAY[]::text[]),COALESCE(payload_blocks.first_block,-1),
		COALESCE(payload_blocks.last_block,-1),COALESCE(payload_blocks.block_count,0),p.all_projects_selected
	FROM page p
	LEFT JOIN LATERAL (
		SELECT array_agg(fb.sha256::text ORDER BY fb.block_index) AS sha256,
			min(fb.block_index) AS first_block,max(fb.block_index) AS last_block,count(*) AS block_count
		FROM file_blocks fb WHERE fb.file_id=p.file_id
	) analytics_blocks ON true
	LEFT JOIN LATERAL (
		SELECT array_agg(pfb.sha256::text ORDER BY pfb.block_index) AS sha256,
			min(pfb.block_index) AS first_block,max(pfb.block_index) AS last_block,count(*) AS block_count
		FROM file_blocks pfb WHERE pfb.file_id=p.payload_file_id
	) payload_blocks ON true
	ORDER BY p.file_id`
	rows, err := tx.Query(ctx, statement, command.TenantID, command.SnapshotID, kindValues, command.StartUS, command.EndUS, retentionFloorUS, after, command.Limit, command.MinimumBatchSeq[:])
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]model.CatalogFile, 0, command.Limit)
	for rows.Next() {
		var file model.CatalogFile
		var firstBlock, lastBlock, blockCount, payloadFirstBlock, payloadLastBlock, payloadBlockCount int
		if err := rows.Scan(&file.FileID, &file.BundleID, &file.ObjectKey, &file.Bytes, &file.SHA256, &file.RowCount,
			&file.MinEventTimeUS, &file.MaxEventTimeUS, &file.MinReceivedTimeUS, &file.MaxReceivedTimeUS,
			&file.MinBatchSeq, &file.MaxBatchSeq, &file.LaneID, &file.Kind, &file.BlockSHA256, &firstBlock, &lastBlock, &blockCount,
			&file.PayloadFileID, &file.PayloadObjectKey, &file.PayloadBytes, &file.PayloadSHA256,
			&file.PayloadBlockSHA256, &payloadFirstBlock, &payloadLastBlock, &payloadBlockCount, &file.AllProjectsSelected); err != nil {
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
		if uuid.Validate(file.PayloadFileID) != nil || file.PayloadObjectKey == "" || file.PayloadBytes <= 0 || !validSHA(file.PayloadSHA256) {
			return nil, errors.New("catalog payload file manifest is invalid")
		}
		if payloadBlockCount < 1 || payloadFirstBlock != 0 || payloadLastBlock != payloadBlockCount-1 || len(file.PayloadBlockSHA256) != payloadBlockCount {
			return nil, errors.New("catalog payload block manifest is not contiguous")
		}
		for _, checksum := range file.PayloadBlockSHA256 {
			if !validSHA(checksum) {
				return nil, errors.New("catalog payload block checksum is invalid")
			}
		}
		result = append(result, file)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func validateCatalogCommand(command CatalogCommand) error {
	for _, seq := range command.MinimumBatchSeq {
		if seq < 0 {
			return errors.New("invalid catalog sequence bound")
		}
	}
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
