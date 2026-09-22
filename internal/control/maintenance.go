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
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	MaxCompactionInputs     = 128
	MaxCompactionInputBytes = int64(256 << 20)
	TargetCompactionBytes   = int64(32 << 20)
	SmallCompactionFile     = int64(8 << 20)
)

// Recheck at claim time as well as candidate selection: foreground pressure
// may begin after reservation, before a prepared swap, or during lease recovery.
// This is admission only; running work retains its fencing/heartbeat contract.
const maintenancePressureSQL = `(
	EXISTS(SELECT 1 FROM jobs WHERE state IN ('queued','running') AND created_at < clock_timestamp()-interval '5 seconds')
	OR EXISTS(SELECT 1 FROM query_jobs WHERE state IN ('planning','queued','running') AND created_at < clock_timestamp()-interval '500 milliseconds'))`

var (
	ErrMaintenanceBusy   = errors.New("maintenance lane is busy")
	ErrMaintenanceFence  = errors.New("maintenance fence is stale")
	ErrMaintenanceNoWork = errors.New("no maintenance work is available")
)

type MaintenanceOperations struct{ pool *pgxpool.Pool }

func NewMaintenanceOperations(pool *pgxpool.Pool) (*MaintenanceOperations, error) {
	if pool == nil {
		return nil, errors.New("maintenance database pool is required")
	}
	return &MaintenanceOperations{pool: pool}, nil
}

type MaintenanceAuthority struct {
	InstallationID    string
	StorageGeneration int64
	TaskID            string
	TenantID          int64
	LaneID            int
	Owner             string
	Fence             int64
}

type CompactionPartition struct {
	SchemaVersion, GroupingVersion int
	EventDay                       string
	Kind                           model.Kind
}

type CompactionCandidate struct {
	TenantID  int64
	LaneID    int
	Partition CompactionPartition
	BundleIDs []string
}

type ReserveCompactionCommand struct {
	InstallationID    string
	StorageGeneration int64
	TaskID            string
	TenantID          int64
	LaneID            int
	BundleIDs         []string
}

type CompactionTask struct {
	Authority MaintenanceAuthority
	Partition CompactionPartition
	Prepared  bool
}

type CompactionFile struct {
	FileID, IntentID, ObjectKey, Role, SHA256 string
	Bytes, RowCount                           int64
	MinEventTimeUS, MaxEventTimeUS            int64
	MinReceivedTimeUS, MaxReceivedTimeUS      int64
	MinBatchSeq, MaxBatchSeq                  int64
	Blocks                                    []model.FileBlockManifest
}

type CompactionWorkInput struct {
	BundleID, IdentitySHA256 string
	ValidFromGeneration      int64
	RowCount                 int64
	InputSeqMin, InputSeqMax int64
	ProjectIDs               []int64
	Analytics, Payload       CompactionFile
}

type CompactionWork struct {
	Task      CompactionTask
	Inputs    []CompactionWorkInput
	Output    *model.BundleManifest
	OutputSHA string
}

func (operations *MaintenanceOperations) FindCompactionCandidate(ctx context.Context) (CompactionCandidate, error) {
	var pressured bool
	if err := operations.pool.QueryRow(ctx, `SELECT `+maintenancePressureSQL).Scan(&pressured); err != nil {
		return CompactionCandidate{}, err
	}
	if pressured {
		return CompactionCandidate{}, ErrMaintenanceNoWork
	}
	// Compare the same bounded prefix that a task may actually reserve, not
	// lexical tenant/lane/kind order or an unbounded partition's total count.
	// Otherwise a steady stream of errors can indefinitely displace a larger
	// log compaction in the same lane. Rank expected file reduction first and
	// rewrite bytes second; retain the old deterministic order for exact ties.
	// PostgreSQL selects one partition; at most 128 rows cross into this process.
	rows, err := operations.pool.Query(ctx, `WITH eligible AS MATERIALIZED (
		SELECT b.tenant_id,b.lane_id,b.schema_version,b.grouping_version,b.event_day,b.kind,b.bundle_id,b.valid_from_generation,sum(f.bytes) AS bytes
		FROM bundles b JOIN files f ON f.tenant_id=b.tenant_id AND f.bundle_id=b.bundle_id
		WHERE b.valid_to_generation IS NULL AND b.reserved_by IS NULL
		AND NOT EXISTS(SELECT 1 FROM maintenance_tasks m WHERE m.tenant_id=b.tenant_id AND m.lane_id=b.lane_id AND m.state IN ('queued','running','prepared'))
		GROUP BY b.bundle_id HAVING max(f.bytes)<$1
	), ordered AS (
		SELECT *,row_number() OVER candidate_order AS ordinal,
		COALESCE(sum(bytes) OVER (candidate_order ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS preceding_bytes
		FROM eligible WINDOW candidate_order AS (PARTITION BY tenant_id,lane_id,schema_version,grouping_version,event_day,kind ORDER BY valid_from_generation,bundle_id)
	), bounded AS MATERIALIZED (
		SELECT * FROM ordered WHERE ordinal<=$2 AND (ordinal<=8 OR preceding_bytes<$3) AND preceding_bytes+bytes<=$4
	), preferred AS (
		SELECT tenant_id,lane_id,schema_version,grouping_version,event_day,kind FROM bounded
		GROUP BY tenant_id,lane_id,schema_version,grouping_version,event_day,kind HAVING count(*)>=8
		ORDER BY count(*) DESC,sum(bytes),tenant_id,lane_id,schema_version,grouping_version,event_day,kind LIMIT 1
	)
	SELECT b.tenant_id,b.lane_id,b.schema_version,b.grouping_version,b.event_day::text,b.kind,b.bundle_id::text,b.bytes
	FROM bounded b JOIN preferred USING(tenant_id,lane_id,schema_version,grouping_version,event_day,kind)
	ORDER BY b.ordinal LIMIT $2`, SmallCompactionFile, MaxCompactionInputs, TargetCompactionBytes, MaxCompactionInputBytes)
	if err != nil {
		return CompactionCandidate{}, err
	}
	defer rows.Close()
	type groupKey struct {
		tenant                 int64
		lane, schema, grouping int
		day, kind              string
	}
	var selected CompactionCandidate
	var current groupKey
	var bytes int64
	for rows.Next() {
		var key groupKey
		var bundleID string
		var bundleBytes int64
		if err := rows.Scan(&key.tenant, &key.lane, &key.schema, &key.grouping, &key.day, &key.kind, &bundleID, &bundleBytes); err != nil {
			return CompactionCandidate{}, err
		}
		if len(selected.BundleIDs) == 0 || key != current {
			if len(selected.BundleIDs) >= 8 {
				return selected, nil
			}
			current, bytes = key, 0
			selected = CompactionCandidate{TenantID: key.tenant, LaneID: key.lane, Partition: CompactionPartition{SchemaVersion: key.schema, GroupingVersion: key.grouping, EventDay: key.day, Kind: model.Kind(key.kind)}}
		}
		if len(selected.BundleIDs) < MaxCompactionInputs && bundleBytes <= MaxCompactionInputBytes-bytes {
			selected.BundleIDs = append(selected.BundleIDs, bundleID)
			bytes += bundleBytes
		}
		if len(selected.BundleIDs) >= 8 && (len(selected.BundleIDs) == MaxCompactionInputs || bytes >= TargetCompactionBytes) {
			return selected, nil
		}
	}
	if err := rows.Err(); err != nil {
		return CompactionCandidate{}, err
	}
	if len(selected.BundleIDs) >= 8 {
		return selected, nil
	}
	return CompactionCandidate{}, ErrMaintenanceNoWork
}

func (operations *MaintenanceOperations) ReserveCompaction(ctx context.Context, command ReserveCompactionCommand) (CompactionTask, error) {
	if err := validateReserveCompaction(command); err != nil {
		return CompactionTask{}, err
	}
	bundleIDs := append([]string(nil), command.BundleIDs...)
	sort.Strings(bundleIDs)
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return CompactionTask{}, err
	}
	defer tx.Rollback(ctx)
	if err := lockRuntimeGeneration(ctx, tx, command.InstallationID, command.StorageGeneration); err != nil {
		return CompactionTask{}, err
	}
	if _, err := tx.Exec(ctx, `SELECT tenant_id FROM tenants WHERE tenant_id=$1 FOR SHARE`, command.TenantID); err != nil {
		return CompactionTask{}, err
	}
	if _, err := tx.Exec(ctx, `SELECT lane_id FROM lanes WHERE tenant_id=$1 AND lane_id=$2 FOR UPDATE`, command.TenantID, command.LaneID); err != nil {
		return CompactionTask{}, err
	}
	partition, identity, err := lockCompactionInputs(ctx, tx, command.TenantID, command.LaneID, bundleIDs)
	if err != nil {
		return CompactionTask{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO maintenance_tasks(task_id,tenant_id,lane_id,kind,input_identity,state,storage_generation)
		VALUES($1,$2,$3,'compact',$4,'queued',$5)`, command.TaskID, command.TenantID, command.LaneID, identity, command.StorageGeneration)
	if err != nil {
		if isUniqueViolation(err) {
			return CompactionTask{}, ErrMaintenanceBusy
		}
		return CompactionTask{}, err
	}
	result, err := tx.Exec(ctx, `UPDATE bundles SET reserved_by=$1
		WHERE tenant_id=$2 AND lane_id=$3 AND bundle_id=ANY($4::uuid[]) AND valid_to_generation IS NULL AND reserved_by IS NULL`, command.TaskID, command.TenantID, command.LaneID, bundleIDs)
	if err != nil || result.RowsAffected() != int64(len(bundleIDs)) {
		return CompactionTask{}, errors.Join(ErrMaintenanceBusy, err)
	}
	result, err = tx.Exec(ctx, `INSERT INTO maintenance_inputs(task_id,tenant_id,bundle_id,expected_valid_from_generation,expected_identity_sha256)
		SELECT $1,tenant_id,bundle_id,valid_from_generation,identity_sha256 FROM bundles
		WHERE tenant_id=$2 AND lane_id=$3 AND bundle_id=ANY($4::uuid[]) AND reserved_by=$1`, command.TaskID, command.TenantID, command.LaneID, bundleIDs)
	if err != nil || result.RowsAffected() != int64(len(bundleIDs)) {
		return CompactionTask{}, errors.Join(ErrMaintenanceBusy, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return CompactionTask{}, err
	}
	return CompactionTask{Authority: MaintenanceAuthority{InstallationID: command.InstallationID, StorageGeneration: command.StorageGeneration, TaskID: command.TaskID, TenantID: command.TenantID, LaneID: command.LaneID}, Partition: partition}, nil
}

func validateReserveCompaction(command ReserveCompactionCommand) error {
	if uuid.Validate(command.InstallationID) != nil || command.StorageGeneration <= 0 || uuid.Validate(command.TaskID) != nil || command.TenantID <= 0 || command.LaneID < 0 || command.LaneID >= model.LaneCount || len(command.BundleIDs) < 2 || len(command.BundleIDs) > MaxCompactionInputs {
		return errors.New("invalid compaction reservation")
	}
	seen := map[string]bool{}
	for _, bundleID := range command.BundleIDs {
		if uuid.Validate(bundleID) != nil || seen[bundleID] {
			return errors.New("invalid compaction bundle identity")
		}
		seen[bundleID] = true
	}
	return nil
}

func lockCompactionInputs(ctx context.Context, tx pgx.Tx, tenantID int64, laneID int, bundleIDs []string) (CompactionPartition, string, error) {
	var partition CompactionPartition
	// Callers already hold the lane lock. Lock all requested current bundles in
	// the same caller-sorted order as before, with a bounded aggregate of their
	// immutable paired file sizes. Ordinality preserves the exact old hash input
	// (including request spelling) and detects a missing/foreign/reserved input.
	rows, err := tx.Query(ctx, `SELECT requested.ordinal,b.schema_version,b.grouping_version,b.event_day::text,b.kind,
		b.valid_from_generation,b.identity_sha256,COALESCE(f.bytes,0)
		FROM unnest($3::uuid[]) WITH ORDINALITY requested(bundle_id,ordinal)
		JOIN bundles b ON b.bundle_id=requested.bundle_id AND b.tenant_id=$1 AND b.lane_id=$2
		LEFT JOIN (SELECT bundle_id,sum(bytes) AS bytes FROM files WHERE tenant_id=$1 AND bundle_id=ANY($3::uuid[]) GROUP BY bundle_id) f ON f.bundle_id=b.bundle_id
		WHERE b.valid_to_generation IS NULL AND b.reserved_by IS NULL
		ORDER BY requested.ordinal FOR UPDATE OF b`, tenantID, laneID, bundleIDs)
	if err != nil {
		return partition, "", errors.Join(ErrMaintenanceBusy, err)
	}
	defer rows.Close()
	hash := sha256.New()
	var total int64
	index := 0
	for rows.Next() {
		var candidate CompactionPartition
		var ordinal, validFrom, bytes int64
		var identity string
		if err := rows.Scan(&ordinal, &candidate.SchemaVersion, &candidate.GroupingVersion, &candidate.EventDay, &candidate.Kind, &validFrom, &identity, &bytes); err != nil {
			return partition, "", errors.Join(ErrMaintenanceBusy, err)
		}
		if index >= len(bundleIDs) || ordinal != int64(index+1) {
			return partition, "", ErrMaintenanceBusy
		}
		if index == 0 {
			partition = candidate
		} else if candidate != partition {
			return partition, "", errors.New("compaction inputs cross a physical partition")
		}
		if bytes <= 0 || total > MaxCompactionInputBytes-bytes {
			return partition, "", errors.New("compaction input bytes exceed limit")
		}
		total += bytes
		encoded, _ := json.Marshal(struct {
			BundleID  string `json:"bundle_id"`
			ValidFrom int64  `json:"valid_from"`
			Identity  string `json:"identity"`
		}{bundleIDs[index], validFrom, identity})
		hash.Write(encoded)
		hash.Write([]byte{'\n'})
		index++
	}
	if err := rows.Err(); err != nil {
		return partition, "", errors.Join(ErrMaintenanceBusy, err)
	}
	if index != len(bundleIDs) {
		return partition, "", ErrMaintenanceBusy
	}
	return partition, hex.EncodeToString(hash.Sum(nil)), nil
}

func (operations *MaintenanceOperations) ClaimCompaction(ctx context.Context, installationID, owner string, lease time.Duration) (*CompactionTask, error) {
	if uuid.Validate(installationID) != nil || owner == "" || lease <= 0 || lease > 5*time.Minute {
		return nil, errors.New("invalid compaction claim")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var task CompactionTask
	var state string
	err = tx.QueryRow(ctx, `SELECT m.task_id::text,m.tenant_id,m.lane_id,m.storage_generation,m.fence,m.state,
		b.schema_version,b.grouping_version,b.event_day::text,b.kind
		FROM maintenance_tasks m JOIN maintenance_inputs mi ON mi.task_id=m.task_id AND mi.tenant_id=m.tenant_id JOIN bundles b ON b.tenant_id=mi.tenant_id AND b.bundle_id=mi.bundle_id
		WHERE m.kind='compact' AND m.retry_at<=clock_timestamp() AND (m.state IN ('queued','prepared') OR (m.state='running' AND m.lease_until<=clock_timestamp()))
		AND NOT `+maintenancePressureSQL+`
		ORDER BY m.created_at,m.task_id LIMIT 1 FOR UPDATE OF m SKIP LOCKED`,
	).Scan(&task.Authority.TaskID, &task.Authority.TenantID, &task.Authority.LaneID, &task.Authority.StorageGeneration, &task.Authority.Fence, &state, &task.Partition.SchemaVersion, &task.Partition.GroupingVersion, &task.Partition.EventDay, &task.Partition.Kind)
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
	result, err := tx.Exec(ctx, `UPDATE maintenance_tasks SET state='running',owner=$2,fence=$3,attempt=attempt+1,lease_until=clock_timestamp()+$4*interval '1 microsecond',updated_at=clock_timestamp()
		WHERE task_id=$1 AND fence=$5`, task.Authority.TaskID, owner, task.Authority.Fence, lease.Microseconds(), task.Authority.Fence-1)
	if err != nil || result.RowsAffected() != 1 {
		return nil, errors.Join(ErrMaintenanceFence, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &task, nil
}

func (operations *MaintenanceOperations) HeartbeatCompaction(ctx context.Context, authority MaintenanceAuthority, lease time.Duration) error {
	if err := validateMaintenanceAuthority(authority); err != nil || lease <= 0 || lease > 5*time.Minute {
		return errors.New("invalid compaction heartbeat")
	}
	result, err := operations.pool.Exec(ctx, `UPDATE maintenance_tasks
		SET lease_until=clock_timestamp()+$4*interval '1 microsecond',updated_at=clock_timestamp()
		WHERE task_id=$1 AND state='running' AND owner=$2 AND fence=$3 AND lease_until>clock_timestamp()`,
		authority.TaskID, authority.Owner, authority.Fence, lease.Microseconds())
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return ErrMaintenanceFence
	}
	return nil
}

func (operations *MaintenanceOperations) FailCompaction(ctx context.Context, authority MaintenanceAuthority, errorCode string, retry bool) error {
	if err := validateMaintenanceAuthority(authority); err != nil || errorCode == "" || len(errorCode) > 64 {
		return errors.New("invalid compaction failure")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := lockRunningMaintenance(ctx, tx, authority); err != nil {
		return err
	}
	if retry {
		result, err := tx.Exec(ctx, `UPDATE maintenance_tasks
			SET state=CASE WHEN output_manifest IS NULL THEN 'queued' ELSE 'prepared' END,
				owner=NULL,lease_until=NULL,retry_at=clock_timestamp()+interval '1 second',last_error_code=$4,updated_at=clock_timestamp()
			WHERE task_id=$1 AND owner=$2 AND fence=$3 AND state='running'`, authority.TaskID, authority.Owner, authority.Fence, errorCode)
		if err != nil || result.RowsAffected() != 1 {
			return errors.Join(ErrMaintenanceFence, err)
		}
	} else {
		result, err := tx.Exec(ctx, `UPDATE maintenance_tasks
			SET state='failed',owner=NULL,lease_until=NULL,output_manifest=NULL,output_sha256=NULL,last_error_code=$4,updated_at=clock_timestamp()
			WHERE task_id=$1 AND owner=$2 AND fence=$3 AND state='running'`, authority.TaskID, authority.Owner, authority.Fence, errorCode)
		if err != nil || result.RowsAffected() != 1 {
			return errors.Join(ErrMaintenanceFence, err)
		}
		if _, err := tx.Exec(ctx, `UPDATE bundles SET reserved_by=NULL WHERE reserved_by=$1`, authority.TaskID); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func isUniqueViolation(err error) bool {
	var pgErr interface{ SQLState() string }
	return errors.As(err, &pgErr) && pgErr.SQLState() == "23505"
}
