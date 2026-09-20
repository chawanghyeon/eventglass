package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type MaintenanceIntentRegistration struct {
	Authority       MaintenanceAuthority
	Role, ObjectKey string
	Intent          IntentAuthority
}

func (operations *MaintenanceOperations) LoadCompaction(ctx context.Context, authority MaintenanceAuthority) (CompactionWork, error) {
	if err := validateMaintenanceAuthority(authority); err != nil {
		return CompactionWork{}, err
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return CompactionWork{}, err
	}
	defer tx.Rollback(ctx)
	if err := lockRunningMaintenance(ctx, tx, authority); err != nil {
		return CompactionWork{}, err
	}
	work := CompactionWork{Task: CompactionTask{Authority: authority}}
	rows, err := tx.Query(ctx, `SELECT b.bundle_id::text,b.schema_version,b.grouping_version,b.event_day::text,b.kind,b.valid_from_generation,b.identity_sha256,b.row_count,b.input_seq_min,b.input_seq_max
		FROM maintenance_inputs mi JOIN bundles b ON b.tenant_id=mi.tenant_id AND b.bundle_id=mi.bundle_id
		WHERE mi.task_id=$1 ORDER BY b.bundle_id`, authority.TaskID)
	if err != nil {
		return work, err
	}
	for rows.Next() {
		var input CompactionWorkInput
		var partition CompactionPartition
		if err := rows.Scan(&input.BundleID, &partition.SchemaVersion, &partition.GroupingVersion, &partition.EventDay, &partition.Kind, &input.ValidFromGeneration, &input.IdentitySHA256, &input.RowCount, &input.InputSeqMin, &input.InputSeqMax); err != nil {
			rows.Close()
			return work, err
		}
		if len(work.Inputs) == 0 {
			work.Task.Partition = partition
		} else if partition != work.Task.Partition {
			rows.Close()
			return work, errors.New("reserved compaction partition changed")
		}
		work.Inputs = append(work.Inputs, input)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return work, err
	}
	rows.Close()
	for index := range work.Inputs {
		projects, err := tx.Query(ctx, `SELECT project_id FROM bundle_projects WHERE tenant_id=$1 AND bundle_id=$2 ORDER BY project_id`, authority.TenantID, work.Inputs[index].BundleID)
		if err != nil {
			return work, err
		}
		for projects.Next() {
			var id int64
			if err := projects.Scan(&id); err != nil {
				projects.Close()
				return work, err
			}
			work.Inputs[index].ProjectIDs = append(work.Inputs[index].ProjectIDs, id)
		}
		if err := projects.Err(); err != nil {
			projects.Close()
			return work, err
		}
		projects.Close()
		files, err := tx.Query(ctx, `SELECT f.file_id::text,f.intent_id::text,oi.object_key,f.role,f.bytes,f.full_sha256,f.row_count,f.min_event_time_us,f.max_event_time_us,f.min_received_time_us,f.max_received_time_us,f.min_batch_seq,f.max_batch_seq
			FROM files f JOIN object_intents oi ON oi.tenant_id=f.tenant_id AND oi.intent_id=f.intent_id AND oi.state='referenced'
			WHERE f.tenant_id=$1 AND f.bundle_id=$2 ORDER BY f.role`, authority.TenantID, work.Inputs[index].BundleID)
		if err != nil {
			return work, err
		}
		for files.Next() {
			var file CompactionFile
			if err := files.Scan(&file.FileID, &file.IntentID, &file.ObjectKey, &file.Role, &file.Bytes, &file.SHA256, &file.RowCount, &file.MinEventTimeUS, &file.MaxEventTimeUS, &file.MinReceivedTimeUS, &file.MaxReceivedTimeUS, &file.MinBatchSeq, &file.MaxBatchSeq); err != nil {
				files.Close()
				return work, err
			}
			if file.Role == "analytics" {
				work.Inputs[index].Analytics = file
			} else {
				work.Inputs[index].Payload = file
			}
		}
		if err := files.Err(); err != nil {
			files.Close()
			return work, err
		}
		files.Close()
		if work.Inputs[index].Analytics.FileID == "" || work.Inputs[index].Payload.FileID == "" {
			return work, errors.New("reserved compaction pair is incomplete")
		}
	}
	if len(work.Inputs) < 2 {
		return work, errors.New("compaction inputs are incomplete")
	}
	if err := tx.Commit(ctx); err != nil {
		return work, err
	}
	return work, nil
}

func (operations *MaintenanceOperations) RegisterMaintenanceIntent(ctx context.Context, registration MaintenanceIntentRegistration) error {
	if err := validateMaintenanceAuthority(registration.Authority); err != nil || registration.Role != "analytics" && registration.Role != "payload" || !safeObjectKey(registration.ObjectKey) || uuid.Validate(registration.Intent.IntentID) != nil || registration.Intent.Owner != registration.Authority.Owner || registration.Intent.Fence != registration.Authority.Fence || registration.Intent.Bytes <= 0 || !validSHA(registration.Intent.SHA256) {
		return errors.New("invalid maintenance intent")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := lockRuntimeGeneration(ctx, tx, registration.Authority.InstallationID, registration.Authority.StorageGeneration); err != nil {
		return err
	}
	if err := lockRunningMaintenance(ctx, tx, registration.Authority); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO object_intents(intent_id,installation_id,tenant_id,storage_generation,object_key,kind,state,owner,fence,expires_at,expected_bytes,expected_sha256,maintenance_task_id,producer_generation,producer_fence)
		VALUES($1,$2,$3,$4,$5,$6,'pending',$7,$8,clock_timestamp()+interval '10 minutes',$9,$10,$11,$4,$8)`, registration.Intent.IntentID, registration.Authority.InstallationID, registration.Authority.TenantID, registration.Authority.StorageGeneration, registration.ObjectKey, registration.Role, registration.Authority.Owner, registration.Authority.Fence, registration.Intent.Bytes, registration.Intent.SHA256, registration.Authority.TaskID)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (operations *MaintenanceOperations) MarkMaintenanceIntentUploaded(ctx context.Context, authority MaintenanceAuthority, intent IntentAuthority) error {
	if err := validateMaintenanceAuthority(authority); err != nil || intent.Owner != authority.Owner || intent.Fence != authority.Fence || intent.Bytes <= 0 || !validSHA(intent.SHA256) {
		return errors.New("invalid maintenance upload")
	}
	result, err := operations.pool.Exec(ctx, `UPDATE object_intents SET state='uploaded',uploaded_bytes=$8,uploaded_sha256=$9,updated_at=clock_timestamp()
		WHERE intent_id=$1 AND installation_id=$2 AND tenant_id=$3 AND storage_generation=$4 AND maintenance_task_id=$5 AND owner=$6 AND fence=$7 AND state='pending' AND expires_at>clock_timestamp() AND expected_bytes=$8 AND expected_sha256=$9
		AND EXISTS(SELECT 1 FROM maintenance_tasks WHERE task_id=$5 AND state='running' AND owner=$6 AND fence=$7 AND lease_until>clock_timestamp())`, intent.IntentID, authority.InstallationID, authority.TenantID, authority.StorageGeneration, authority.TaskID, authority.Owner, authority.Fence, intent.Bytes, intent.SHA256)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return ErrMaintenanceFence
	}
	return nil
}

func (operations *MaintenanceOperations) PrepareCompaction(ctx context.Context, authority MaintenanceAuthority, bundle model.BundleManifest) error {
	if err := validateMaintenanceAuthority(authority); err != nil {
		return err
	}
	encoded, err := json.Marshal(bundle)
	if err != nil || len(encoded) > 1<<20 {
		return errors.Join(errors.New("invalid compaction output manifest"), err)
	}
	digest := sha256.Sum256(encoded)
	checksum := hex.EncodeToString(digest[:])
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := lockRuntimeGeneration(ctx, tx, authority.InstallationID, authority.StorageGeneration); err != nil {
		return err
	}
	if err := lockRunningMaintenance(ctx, tx, authority); err != nil {
		return err
	}
	work, err := loadCompactionExpectations(ctx, tx, authority.TaskID)
	if err != nil {
		return err
	}
	if err := validateCompactionOutput(bundle, work); err != nil {
		return err
	}
	for _, file := range []model.FileManifest{bundle.Analytics, bundle.Payload} {
		result, err := tx.Exec(ctx, `UPDATE object_intents SET state='referenced',expires_at=clock_timestamp()+interval '24 hours',updated_at=clock_timestamp()
			WHERE intent_id=$1 AND tenant_id=$2 AND maintenance_task_id=$3 AND state='uploaded' AND owner=$4 AND fence=$5 AND uploaded_bytes=expected_bytes AND uploaded_sha256=expected_sha256 AND expected_bytes=$6 AND expected_sha256=$7`, file.IntentID, authority.TenantID, authority.TaskID, authority.Owner, authority.Fence, file.Bytes, file.SHA256)
		if err != nil || result.RowsAffected() != 1 {
			return errors.Join(ErrMaintenanceFence, err)
		}
	}
	result, err := tx.Exec(ctx, `UPDATE maintenance_tasks SET state='prepared',owner=NULL,lease_until=NULL,output_manifest=$4,output_sha256=$5,updated_at=clock_timestamp() WHERE task_id=$1 AND state='running' AND owner=$2 AND fence=$3`, authority.TaskID, authority.Owner, authority.Fence, encoded, checksum)
	if err != nil || result.RowsAffected() != 1 {
		return errors.Join(ErrMaintenanceFence, err)
	}
	return tx.Commit(ctx)
}

func validateMaintenanceAuthority(authority MaintenanceAuthority) error {
	if uuid.Validate(authority.InstallationID) != nil || authority.StorageGeneration <= 0 || uuid.Validate(authority.TaskID) != nil || authority.TenantID <= 0 || authority.LaneID < 0 || authority.LaneID >= model.LaneCount || authority.Owner == "" || authority.Fence <= 0 {
		return errors.New("invalid maintenance authority")
	}
	return nil
}
func lockRunningMaintenance(ctx context.Context, tx pgx.Tx, authority MaintenanceAuthority) error {
	var tenant int64
	var lane int
	var generation, fence int64
	var owner string
	err := tx.QueryRow(ctx, `SELECT tenant_id,lane_id,storage_generation,fence,owner FROM maintenance_tasks WHERE task_id=$1 AND state='running' AND lease_until>clock_timestamp() FOR UPDATE`, authority.TaskID).Scan(&tenant, &lane, &generation, &fence, &owner)
	if err != nil || tenant != authority.TenantID || lane != authority.LaneID || generation != authority.StorageGeneration || fence != authority.Fence || owner != authority.Owner {
		return errors.Join(ErrMaintenanceFence, err)
	}
	return nil
}

func loadCompactionExpectations(ctx context.Context, tx pgx.Tx, taskID string) (CompactionWork, error) {
	var work CompactionWork
	rows, err := tx.Query(ctx, `SELECT b.bundle_id::text,b.schema_version,b.grouping_version,b.event_day::text,b.kind,b.valid_from_generation,b.identity_sha256,b.row_count,b.input_seq_min,b.input_seq_max FROM maintenance_inputs mi JOIN bundles b ON b.tenant_id=mi.tenant_id AND b.bundle_id=mi.bundle_id WHERE mi.task_id=$1 ORDER BY b.bundle_id`, taskID)
	if err != nil {
		return work, err
	}
	for rows.Next() {
		var input CompactionWorkInput
		var p CompactionPartition
		if err := rows.Scan(&input.BundleID, &p.SchemaVersion, &p.GroupingVersion, &p.EventDay, &p.Kind, &input.ValidFromGeneration, &input.IdentitySHA256, &input.RowCount, &input.InputSeqMin, &input.InputSeqMax); err != nil {
			return work, err
		}
		if len(work.Inputs) == 0 {
			work.Task.Partition = p
		} else if p != work.Task.Partition {
			return work, errors.New("compaction partition changed")
		}
		work.Inputs = append(work.Inputs, input)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return work, err
	}
	rows.Close()
	for index := range work.Inputs {
		projects, err := tx.Query(ctx, `SELECT project_id FROM bundle_projects WHERE bundle_id=$1 ORDER BY project_id`, work.Inputs[index].BundleID)
		if err != nil {
			return work, err
		}
		for projects.Next() {
			var id int64
			if err := projects.Scan(&id); err != nil {
				projects.Close()
				return work, err
			}
			work.Inputs[index].ProjectIDs = append(work.Inputs[index].ProjectIDs, id)
		}
		if err := projects.Err(); err != nil {
			projects.Close()
			return work, err
		}
		projects.Close()
	}
	return work, nil
}

func validateCompactionOutput(bundle model.BundleManifest, work CompactionWork) error {
	day, dayErr := time.Parse(time.DateOnly, bundle.EventDay)
	if uuid.Validate(bundle.BundleID) != nil || dayErr != nil || day.Format(time.DateOnly) != bundle.EventDay || bundle.EventDay != work.Task.Partition.EventDay || bundle.Kind != work.Task.Partition.Kind || bundle.RowCount <= 0 || !validSHA(bundle.IdentitySHA256) {
		return errors.New("compaction output scope is invalid")
	}
	var rows, minSeq, maxSeq int64
	projects := map[int64]bool{}
	for i, input := range work.Inputs {
		rows += input.RowCount
		if i == 0 || input.InputSeqMin < minSeq {
			minSeq = input.InputSeqMin
		}
		if input.InputSeqMax > maxSeq {
			maxSeq = input.InputSeqMax
		}
		for _, id := range input.ProjectIDs {
			projects[id] = true
		}
	}
	expected := make([]int64, 0, len(projects))
	for id := range projects {
		expected = append(expected, id)
	}
	sort.Slice(expected, func(i, j int) bool { return expected[i] < expected[j] })
	if rows != bundle.RowCount || minSeq != bundle.InputSeqMin || maxSeq != bundle.InputSeqMax || len(expected) != len(bundle.ProjectIDs) {
		return errors.New("compaction output totals differ from inputs")
	}
	for i := range expected {
		if expected[i] != bundle.ProjectIDs[i] {
			return errors.New("compaction output projects differ from inputs")
		}
	}
	files := []model.FileManifest{bundle.Analytics, bundle.Payload}
	for index, file := range files {
		role := []string{"analytics", "payload"}[index]
		if file.Role != role || uuid.Validate(file.FileID) != nil || uuid.Validate(file.IntentID) != nil || file.RowCount != rows || file.Bytes <= 0 || file.Bytes > 128<<20 || !validSHA(file.SHA256) || file.MinEventTimeUS > file.MaxEventTimeUS || file.MinReceivedTimeUS > file.MaxReceivedTimeUS || file.MinBatchSeq != minSeq || file.MaxBatchSeq != maxSeq || len(file.Blocks) != int((file.Bytes+model.FileBlockBytes-1)/model.FileBlockBytes) {
			return errors.New("compaction output file is invalid")
		}
		for blockIndex, block := range file.Blocks {
			if block.Index != blockIndex || !validSHA(block.SHA256) {
				return errors.New("compaction output file blocks are invalid")
			}
		}
	}
	if bundle.Analytics.FileID == bundle.Payload.FileID || bundle.Analytics.IntentID == bundle.Payload.IntentID || bundle.Analytics.MinEventTimeUS != bundle.Payload.MinEventTimeUS || bundle.Analytics.MaxEventTimeUS != bundle.Payload.MaxEventTimeUS || bundle.Analytics.MinReceivedTimeUS != bundle.Payload.MinReceivedTimeUS || bundle.Analytics.MaxReceivedTimeUS != bundle.Payload.MaxReceivedTimeUS {
		return errors.New("compaction output pair metadata differs")
	}
	return nil
}
