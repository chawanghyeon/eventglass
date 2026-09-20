package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type RetentionCandidate struct {
	TenantID       int64
	LaneID         int
	BundleID       string
	RetentionFloor int64
	FullyExpired   bool
	Partition      CompactionPartition
}

type ReserveRetentionCommand struct {
	InstallationID    string
	StorageGeneration int64
	TaskID            string
	TenantID          int64
	LaneID            int
	BundleID          string
}

type RetentionTask struct {
	CompactionTask
	RetentionFloor int64
}

type RetentionWork struct {
	CompactionWork
	RetentionFloor int64
	FullyExpired   bool
}

func (operations *MaintenanceOperations) FindRetentionCandidate(ctx context.Context) (RetentionCandidate, error) {
	return operations.findRetentionCandidate(ctx, 0)
}

func (operations *MaintenanceOperations) FindRetentionCandidateForTenant(ctx context.Context, tenantID int64) (RetentionCandidate, error) {
	if tenantID <= 0 {
		return RetentionCandidate{}, errors.New("invalid retention tenant")
	}
	return operations.findRetentionCandidate(ctx, tenantID)
}

func (operations *MaintenanceOperations) findRetentionCandidate(ctx context.Context, tenantID int64) (RetentionCandidate, error) {
	var candidate RetentionCandidate
	err := operations.pool.QueryRow(ctx, `SELECT b.tenant_id,b.lane_id,b.bundle_id::text,b.schema_version,b.grouping_version,b.event_day::text,b.kind,i.retention_floor_us,
		f.max_received_time_us<i.retention_floor_us
		FROM installations i JOIN bundles b ON b.valid_to_generation IS NULL AND b.reserved_by IS NULL
		JOIN files f ON f.tenant_id=b.tenant_id AND f.bundle_id=b.bundle_id AND f.role='analytics'
		WHERE i.singleton AND f.min_received_time_us<i.retention_floor_us AND ($1::bigint=0 OR b.tenant_id=$1)
		AND NOT EXISTS(SELECT 1 FROM jobs WHERE state IN ('queued','running') AND created_at<clock_timestamp()-interval '5 seconds' AND ($1::bigint=0 OR tenant_id=b.tenant_id))
		AND NOT EXISTS(SELECT 1 FROM query_jobs WHERE state IN ('planning','queued','running') AND created_at<clock_timestamp()-interval '500 milliseconds' AND ($1::bigint=0 OR tenant_id=b.tenant_id))
		ORDER BY (f.max_received_time_us<i.retention_floor_us) DESC,f.min_received_time_us,b.tenant_id,b.lane_id,b.bundle_id LIMIT 1`, tenantID).Scan(
		&candidate.TenantID, &candidate.LaneID, &candidate.BundleID,
		&candidate.Partition.SchemaVersion, &candidate.Partition.GroupingVersion, &candidate.Partition.EventDay,
		&candidate.Partition.Kind, &candidate.RetentionFloor, &candidate.FullyExpired)
	if errors.Is(err, pgx.ErrNoRows) {
		return RetentionCandidate{}, ErrMaintenanceNoWork
	}
	return candidate, err
}

func (operations *MaintenanceOperations) ReserveRetention(ctx context.Context, command ReserveRetentionCommand) (RetentionTask, error) {
	if uuid.Validate(command.InstallationID) != nil || command.StorageGeneration <= 0 || uuid.Validate(command.TaskID) != nil || command.TenantID <= 0 || command.LaneID < 0 || command.LaneID >= model.LaneCount || uuid.Validate(command.BundleID) != nil {
		return RetentionTask{}, errors.New("invalid retention reservation")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return RetentionTask{}, err
	}
	defer tx.Rollback(ctx)
	if err := lockRuntimeGeneration(ctx, tx, command.InstallationID, command.StorageGeneration); err != nil {
		return RetentionTask{}, err
	}
	var floor int64
	if err := tx.QueryRow(ctx, `SELECT retention_floor_us FROM installations WHERE singleton`).Scan(&floor); err != nil {
		return RetentionTask{}, err
	}
	if _, err := tx.Exec(ctx, `SELECT lane_id FROM lanes WHERE tenant_id=$1 AND lane_id=$2 FOR UPDATE`, command.TenantID, command.LaneID); err != nil {
		return RetentionTask{}, err
	}
	partition, identity, err := lockCompactionInputs(ctx, tx, command.TenantID, command.LaneID, []string{command.BundleID})
	if err != nil {
		return RetentionTask{}, err
	}
	var minimum int64
	if err := tx.QueryRow(ctx, `SELECT min_received_time_us FROM files WHERE tenant_id=$1 AND bundle_id=$2 AND role='analytics'`, command.TenantID, command.BundleID).Scan(&minimum); err != nil {
		return RetentionTask{}, err
	}
	if minimum >= floor {
		return RetentionTask{}, ErrMaintenanceNoWork
	}
	if _, err := tx.Exec(ctx, `INSERT INTO maintenance_tasks(task_id,tenant_id,lane_id,kind,input_identity,state,storage_generation,retention_floor_us)
		VALUES($1,$2,$3,'retain',$4,'queued',$5,$6)`, command.TaskID, command.TenantID, command.LaneID, identity, command.StorageGeneration, floor); err != nil {
		if isUniqueViolation(err) {
			return RetentionTask{}, ErrMaintenanceBusy
		}
		return RetentionTask{}, err
	}
	result, err := tx.Exec(ctx, `UPDATE bundles SET reserved_by=$1 WHERE tenant_id=$2 AND lane_id=$3 AND bundle_id=$4 AND valid_to_generation IS NULL AND reserved_by IS NULL`, command.TaskID, command.TenantID, command.LaneID, command.BundleID)
	if err != nil || result.RowsAffected() != 1 {
		return RetentionTask{}, errors.Join(ErrMaintenanceBusy, err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO maintenance_inputs(task_id,tenant_id,bundle_id,expected_valid_from_generation,expected_identity_sha256)
		SELECT $1,tenant_id,bundle_id,valid_from_generation,identity_sha256 FROM bundles WHERE tenant_id=$2 AND bundle_id=$3`, command.TaskID, command.TenantID, command.BundleID); err != nil {
		return RetentionTask{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return RetentionTask{}, err
	}
	return RetentionTask{CompactionTask: CompactionTask{Authority: MaintenanceAuthority{InstallationID: command.InstallationID, StorageGeneration: command.StorageGeneration, TaskID: command.TaskID, TenantID: command.TenantID, LaneID: command.LaneID}, Partition: partition}, RetentionFloor: floor}, nil
}

func (operations *MaintenanceOperations) ClaimRetention(ctx context.Context, installationID, owner string, lease time.Duration) (*RetentionTask, error) {
	if uuid.Validate(installationID) != nil || owner == "" || lease <= 0 || lease > 5*time.Minute {
		return nil, errors.New("invalid retention claim")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var task RetentionTask
	var state string
	err = tx.QueryRow(ctx, `SELECT m.task_id::text,m.tenant_id,m.lane_id,m.storage_generation,m.fence,m.state,m.retention_floor_us,
		b.schema_version,b.grouping_version,b.event_day::text,b.kind
		FROM maintenance_tasks m JOIN maintenance_inputs mi ON mi.task_id=m.task_id AND mi.tenant_id=m.tenant_id
		JOIN bundles b ON b.tenant_id=mi.tenant_id AND b.bundle_id=mi.bundle_id
		WHERE m.kind='retain' AND m.retry_at<=clock_timestamp() AND (m.state IN ('queued','prepared') OR (m.state='running' AND m.lease_until<=clock_timestamp()))
		ORDER BY m.created_at,m.task_id LIMIT 1 FOR UPDATE OF m SKIP LOCKED`).Scan(
		&task.Authority.TaskID, &task.Authority.TenantID, &task.Authority.LaneID, &task.Authority.StorageGeneration,
		&task.Authority.Fence, &state, &task.RetentionFloor, &task.Partition.SchemaVersion,
		&task.Partition.GroupingVersion, &task.Partition.EventDay, &task.Partition.Kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := lockRuntimeGeneration(ctx, tx, installationID, task.Authority.StorageGeneration); err != nil {
		return nil, err
	}
	task.Authority.InstallationID, task.Authority.Owner = installationID, owner
	task.Authority.Fence++
	task.Prepared = state == "prepared"
	result, err := tx.Exec(ctx, `UPDATE maintenance_tasks SET state='running',owner=$2,fence=$3,attempt=attempt+1,lease_until=clock_timestamp()+$4*interval '1 microsecond',updated_at=clock_timestamp() WHERE task_id=$1 AND fence=$5`, task.Authority.TaskID, owner, task.Authority.Fence, lease.Microseconds(), task.Authority.Fence-1)
	if err != nil || result.RowsAffected() != 1 {
		return nil, errors.Join(ErrMaintenanceFence, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &task, nil
}

func (operations *MaintenanceOperations) LoadRetention(ctx context.Context, task RetentionTask) (RetentionWork, error) {
	work, err := operations.LoadCompaction(ctx, task.Authority)
	if err != nil {
		return RetentionWork{}, err
	}
	result := RetentionWork{CompactionWork: work, RetentionFloor: task.RetentionFloor, FullyExpired: true}
	for _, input := range work.Inputs {
		if input.Analytics.MaxReceivedTimeUS >= task.RetentionFloor {
			result.FullyExpired = false
		}
	}
	return result, nil
}

func (operations *MaintenanceOperations) PrepareRetention(ctx context.Context, task RetentionTask, bundle model.BundleManifest) error {
	encoded, err := json.Marshal(bundle)
	if err != nil || len(encoded) > 1<<20 {
		return errors.Join(errors.New("invalid retention output manifest"), err)
	}
	digest := sha256.Sum256(encoded)
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := lockRuntimeGeneration(ctx, tx, task.Authority.InstallationID, task.Authority.StorageGeneration); err != nil {
		return err
	}
	if err := lockRunningMaintenance(ctx, tx, task.Authority); err != nil {
		return err
	}
	work, err := loadCompactionExpectations(ctx, tx, task.Authority.TaskID)
	if err != nil {
		return err
	}
	if err := validateRetentionOutput(bundle, work, task.RetentionFloor); err != nil {
		return err
	}
	for _, file := range []model.FileManifest{bundle.Analytics, bundle.Payload} {
		result, err := tx.Exec(ctx, `UPDATE object_intents SET state='referenced',expires_at=clock_timestamp()+interval '24 hours',updated_at=clock_timestamp() WHERE intent_id=$1 AND tenant_id=$2 AND maintenance_task_id=$3 AND state='uploaded' AND owner=$4 AND fence=$5 AND uploaded_bytes=expected_bytes AND uploaded_sha256=expected_sha256 AND expected_bytes=$6 AND expected_sha256=$7`, file.IntentID, task.Authority.TenantID, task.Authority.TaskID, task.Authority.Owner, task.Authority.Fence, file.Bytes, file.SHA256)
		if err != nil || result.RowsAffected() != 1 {
			return errors.Join(ErrMaintenanceFence, err)
		}
	}
	result, err := tx.Exec(ctx, `UPDATE maintenance_tasks SET state='prepared',owner=NULL,lease_until=NULL,output_manifest=$4,output_sha256=$5,updated_at=clock_timestamp() WHERE task_id=$1 AND state='running' AND owner=$2 AND fence=$3`, task.Authority.TaskID, task.Authority.Owner, task.Authority.Fence, encoded, hex.EncodeToString(digest[:]))
	if err != nil || result.RowsAffected() != 1 {
		return errors.Join(ErrMaintenanceFence, err)
	}
	return tx.Commit(ctx)
}

func validateRetentionOutput(bundle model.BundleManifest, work CompactionWork, floor int64) error {
	if uuid.Validate(bundle.BundleID) != nil || bundle.EventDay != work.Task.Partition.EventDay || bundle.Kind != work.Task.Partition.Kind || bundle.RowCount <= 0 || !validSHA(bundle.IdentitySHA256) {
		return errors.New("retention output scope is invalid")
	}
	var inputRows, minSeq, maxSeq int64
	projects := map[int64]bool{}
	for index, input := range work.Inputs {
		inputRows += input.RowCount
		if index == 0 || input.InputSeqMin < minSeq {
			minSeq = input.InputSeqMin
		}
		if input.InputSeqMax > maxSeq {
			maxSeq = input.InputSeqMax
		}
		for _, projectID := range input.ProjectIDs {
			projects[projectID] = true
		}
	}
	if bundle.RowCount > inputRows || bundle.InputSeqMin < minSeq || bundle.InputSeqMax > maxSeq || bundle.InputSeqMin > bundle.InputSeqMax {
		return errors.New("retention output exceeds its inputs")
	}
	previous := int64(0)
	for _, projectID := range bundle.ProjectIDs {
		if projectID <= previous || !projects[projectID] {
			return errors.New("retention output projects are invalid")
		}
		previous = projectID
	}
	for index, file := range []model.FileManifest{bundle.Analytics, bundle.Payload} {
		role := []string{"analytics", "payload"}[index]
		if file.Role != role || uuid.Validate(file.FileID) != nil || uuid.Validate(file.IntentID) != nil || file.RowCount != bundle.RowCount || file.Bytes <= 0 || file.Bytes > 128<<20 || !validSHA(file.SHA256) || file.MinReceivedTimeUS < floor || file.MinReceivedTimeUS > file.MaxReceivedTimeUS || file.MinEventTimeUS > file.MaxEventTimeUS || file.MinBatchSeq != bundle.InputSeqMin || file.MaxBatchSeq != bundle.InputSeqMax || len(file.Blocks) != int((file.Bytes+model.FileBlockBytes-1)/model.FileBlockBytes) {
			return errors.New("retention output file is invalid")
		}
		for blockIndex, block := range file.Blocks {
			if block.Index != blockIndex || !validSHA(block.SHA256) {
				return errors.New("retention output blocks are invalid")
			}
		}
	}
	if bundle.Analytics.FileID == bundle.Payload.FileID || bundle.Analytics.IntentID == bundle.Payload.IntentID {
		return errors.New("retention output pair identities collide")
	}
	if bundle.Analytics.MinEventTimeUS != bundle.Payload.MinEventTimeUS || bundle.Analytics.MaxEventTimeUS != bundle.Payload.MaxEventTimeUS || bundle.Analytics.MinReceivedTimeUS != bundle.Payload.MinReceivedTimeUS || bundle.Analytics.MaxReceivedTimeUS != bundle.Payload.MaxReceivedTimeUS || bundle.Analytics.MinBatchSeq != bundle.Payload.MinBatchSeq || bundle.Analytics.MaxBatchSeq != bundle.Payload.MaxBatchSeq {
		return errors.New("retention output pair metadata differs")
	}
	return nil
}
