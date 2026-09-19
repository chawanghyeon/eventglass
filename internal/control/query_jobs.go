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

const (
	MaxUserQueries        = 2
	MaxTenantQueries      = 8
	MaxQueuedQueries      = 32
	QueryCoordinatorLease = 60 * time.Second
	QueryTaskLease        = 60 * time.Second
	QueryMaximumTimeout   = 5 * time.Minute
	QuerySyncTimeout      = 30 * time.Second
	MaxQueryTasks         = 4681
	MaxQueryPlanBytes     = 16 << 20
)

var (
	ErrQueryLimitExceeded = errors.New("query limit exceeded")
	ErrQueryFenceStale    = errors.New("query authority is stale")
	ErrQueryTerminal      = errors.New("query is terminal")
)

type QueryCoordinatorAuthority struct {
	QueryID           string
	TenantID          int64
	Owner             string
	Fence             int64
	StorageGeneration int64
}

type QueryJob struct {
	Authority  QueryCoordinatorAuthority
	SnapshotID string
	UserID     int64
	Deadline   time.Time
	ExpiresAt  time.Time
	State      string
}

type CreateQueryCommand struct {
	QueryID          string
	SessionTokenHash [32]byte
	TenantID         int64
	SnapshotID       string
	OperationKind    string
	OperationHash    string
	OperationBytes   []byte
	Owner            string
	Timeout          time.Duration
}

type SealQueryPlanCommand struct {
	Authority     QueryCoordinatorAuthority
	PlanSHA256    string
	Tasks         []model.QueryPlannedTask
	ScanCount     int
	ManifestBytes int
}

func (operations *QueryOperations) CreateQuery(ctx context.Context, command CreateQueryCommand) (QueryJob, error) {
	if err := validateCreateQuery(command); err != nil {
		return QueryJob{}, err
	}
	connection, err := operations.pool.Acquire(ctx)
	if err != nil {
		return QueryJob{}, err
	}
	if _, err := connection.Exec(ctx, `SELECT pg_advisory_lock($1::bigint)`, command.TenantID); err != nil {
		connection.Release()
		return QueryJob{}, err
	}
	defer releaseTenantAdmission(connection, command.TenantID)
	tx, err := connection.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return QueryJob{}, err
	}
	defer tx.Rollback(ctx)
	authority, err := lockQueryAuthority(ctx, tx, command.SessionTokenHash, command.TenantID, false, false)
	if err != nil {
		return QueryJob{}, err
	}
	snapshot, revisions, err := loadSnapshotForUpdate(ctx, tx, command.TenantID, command.SnapshotID)
	if err != nil {
		return QueryJob{}, err
	}
	if snapshot.UserID != authority.userID || snapshot.PrincipalHash != authority.principalHash || snapshot.StorageGeneration != authority.generation || snapshot.AuthRevision != authority.authRevision || snapshot.TenantAuthRevision != authority.tenantAuthRevision {
		return QueryJob{}, ErrForbidden
	}
	if _, err := authorizeSnapshotProjects(ctx, tx, authority, command.TenantID, snapshot.ProjectIDs, revisions); err != nil {
		return QueryJob{}, err
	}
	if snapshot.ExpiresAtUS <= authority.now.UnixMicro() {
		return QueryJob{}, ErrSnapshotExpired
	}
	var userCount, tenantCount, queuedCount int
	if err := tx.QueryRow(ctx, `SELECT count(*) FILTER (WHERE user_id=$2),count(*),
		count(*) FILTER (WHERE state IN ('planning','queued')) FROM query_jobs
		WHERE tenant_id=$1 AND state IN ('planning','queued','running') AND deadline>clock_timestamp()`, command.TenantID, authority.userID).Scan(&userCount, &tenantCount, &queuedCount); err != nil {
		return QueryJob{}, err
	}
	if userCount >= MaxUserQueries || tenantCount >= MaxTenantQueries || queuedCount >= MaxQueuedQueries {
		return QueryJob{}, ErrQueryLimitExceeded
	}
	deadline := authority.now.Add(command.Timeout)
	if maxUntil := time.UnixMicro(snapshot.MaxUntilUS); deadline.After(maxUntil) {
		deadline = maxUntil
	}
	expiresAt := time.UnixMicro(snapshot.ExpiresAtUS)
	leaseUntil := authority.now.Add(QueryCoordinatorLease)
	if leaseUntil.After(deadline) {
		leaseUntil = deadline
	}
	_, err = tx.Exec(ctx, `INSERT INTO query_jobs(query_id,tenant_id,user_id,principal_ref,snapshot_id,operation_kind,operation_hash,
		operation_bytes,state,deadline,coordinator_owner,coordinator_fence,lease_until,expires_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,'planning',$9,$10,1,$11,$12)`, command.QueryID, command.TenantID,
		authority.userID, authority.principalHash, command.SnapshotID, command.OperationKind, command.OperationHash,
		command.OperationBytes, deadline, command.Owner, leaseUntil, expiresAt)
	if err != nil {
		return QueryJob{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return QueryJob{}, err
	}
	return QueryJob{
		Authority:  QueryCoordinatorAuthority{QueryID: command.QueryID, TenantID: command.TenantID, Owner: command.Owner, Fence: 1, StorageGeneration: authority.generation},
		SnapshotID: command.SnapshotID, UserID: authority.userID, Deadline: deadline, ExpiresAt: expiresAt, State: "planning",
	}, nil
}

func (operations *QueryOperations) SealQueryPlan(ctx context.Context, command SealQueryPlanCommand) error {
	if err := validateSealPlan(command); err != nil {
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
	var state string
	var owner string
	var fence int64
	var deadline, now time.Time
	var sealed *string
	if err := tx.QueryRow(ctx, `SELECT state,coordinator_owner,coordinator_fence,deadline,sealed_plan_sha,clock_timestamp()
		FROM query_jobs WHERE tenant_id=$1 AND query_id=$2 FOR UPDATE`, command.Authority.TenantID, command.Authority.QueryID).Scan(&state, &owner, &fence, &deadline, &sealed, &now); err != nil {
		return err
	}
	if sealed != nil {
		if *sealed == command.PlanSHA256 {
			return tx.Commit(ctx)
		}
		return ErrQueryFenceStale
	}
	if state != "planning" || owner != command.Authority.Owner || fence != command.Authority.Fence || !deadline.After(now) {
		return ErrQueryFenceStale
	}
	if _, err := tx.Exec(ctx, `DELETE FROM query_tasks WHERE tenant_id=$1 AND query_id=$2`, command.Authority.TenantID, command.Authority.QueryID); err != nil {
		return err
	}
	levels := map[int]bool{}
	for _, task := range command.Tasks {
		if _, err := tx.Exec(ctx, `INSERT INTO query_tasks(query_id,tenant_id,stage,level,partition_id,state,manifest_json)
			VALUES($1,$2,$3,$4,$5,'queued',$6)`, command.Authority.QueryID, command.Authority.TenantID,
			task.Key.Stage, task.Key.Level, task.Key.PartitionID, task.Manifest); err != nil {
			return err
		}
		levels[task.Key.Level] = true
	}
	for _, task := range command.Tasks {
		for _, input := range task.InputOrdinal {
			if input.Consumer != task.Key {
				return errors.New("query task input consumer mismatch")
			}
			if _, err := tx.Exec(ctx, `INSERT INTO query_task_inputs(query_id,tenant_id,consumer_stage,consumer_level,consumer_partition_id,
				ordinal,producer_stage,producer_level,producer_partition_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
				command.Authority.QueryID, command.Authority.TenantID, input.Consumer.Stage, input.Consumer.Level, input.Consumer.PartitionID,
				input.Ordinal, input.Producer.Stage, input.Producer.Level, input.Producer.PartitionID); err != nil {
				return err
			}
		}
	}
	for level := range levels {
		if _, err := tx.Exec(ctx, `INSERT INTO query_level_budgets(query_id,tenant_id,level,reserved_bytes)
			VALUES($1,$2,$3,67108864)`, command.Authority.QueryID, command.Authority.TenantID, level); err != nil {
			return err
		}
	}
	result, err := tx.Exec(ctx, `UPDATE query_jobs SET state='queued',sealed_plan_sha=$5,plan_file_count=$6,plan_scan_count=$7,
		plan_bytes=$8,coordinator_owner=NULL,lease_until=NULL,updated_at=clock_timestamp()
		WHERE tenant_id=$1 AND query_id=$2 AND state='planning' AND coordinator_owner=$3 AND coordinator_fence=$4`,
		command.Authority.TenantID, command.Authority.QueryID, command.Authority.Owner, command.Authority.Fence,
		command.PlanSHA256, planFileCount(command.Tasks), command.ScanCount, command.ManifestBytes)
	if err != nil || result.RowsAffected() != 1 {
		return errors.Join(ErrQueryFenceStale, err)
	}
	return tx.Commit(ctx)
}

func validateCreateQuery(command CreateQueryCommand) error {
	if uuid.Validate(command.QueryID) != nil || uuid.Validate(command.SnapshotID) != nil || command.SessionTokenHash == ([32]byte{}) || command.TenantID <= 0 || command.Owner == "" || len(command.Owner) > 128 || !validSHA(command.OperationHash) || len(command.OperationBytes) < 2 || len(command.OperationBytes) > 65536 || !json.Valid(command.OperationBytes) || command.Timeout <= 0 || command.Timeout > QueryMaximumTimeout {
		return errors.New("invalid query creation command")
	}
	switch command.OperationKind {
	case "search", "aggregate", "detail", "related":
	default:
		return errors.New("unsupported query operation kind")
	}
	digest := sha256.Sum256(command.OperationBytes)
	if hex.EncodeToString(digest[:]) != command.OperationHash {
		return errors.New("query operation hash differs from bytes")
	}
	return nil
}

func validateSealPlan(command SealQueryPlanCommand) error {
	if uuid.Validate(command.Authority.QueryID) != nil || command.Authority.TenantID <= 0 || command.Authority.Owner == "" || command.Authority.Fence <= 0 || command.Authority.StorageGeneration <= 0 || !validSHA(command.PlanSHA256) || len(command.Tasks) < 1 || len(command.Tasks) > MaxQueryTasks || command.ScanCount < 0 || command.ScanCount > 4096 || command.ManifestBytes < 2 || command.ManifestBytes > MaxQueryPlanBytes {
		return errors.New("invalid query plan seal")
	}
	seen := make(map[model.QueryTaskKey]bool, len(command.Tasks))
	actualBytes, actualScans := 0, 0
	for _, task := range command.Tasks {
		if seen[task.Key] || task.Key.PartitionID < 0 || task.Key.Stage == model.QueryTaskScan && task.Key.Level != 0 || task.Key.Stage == model.QueryTaskReduce && task.Key.Level <= 0 || task.Key.Stage != model.QueryTaskScan && task.Key.Stage != model.QueryTaskReduce || len(task.Manifest) < 2 || len(task.Manifest) > 1<<20 || !json.Valid(task.Manifest) {
			return errors.New("invalid planned query task")
		}
		seen[task.Key] = true
		actualBytes += len(task.Manifest)
		if task.Key.Stage == model.QueryTaskScan {
			actualScans++
		}
	}
	if actualBytes != command.ManifestBytes || actualScans != command.ScanCount {
		return errors.New("query plan counters differ from tasks")
	}
	for _, task := range command.Tasks {
		if len(task.InputOrdinal) > 8 {
			return errors.New("query reducer fan-in exceeds eight")
		}
		for ordinal, input := range task.InputOrdinal {
			if input.Ordinal != ordinal || input.Consumer != task.Key || !seen[input.Producer] || input.Producer.Level >= input.Consumer.Level {
				return errors.New("invalid query task dependency")
			}
		}
	}
	return nil
}

func planFileCount(tasks []model.QueryPlannedTask) int {
	total := 0
	for _, task := range tasks {
		if task.Key.Stage != model.QueryTaskScan {
			continue
		}
		var manifest struct {
			Files []json.RawMessage `json:"files"`
		}
		if json.Unmarshal(task.Manifest, &manifest) == nil {
			total += len(manifest.Files)
		}
	}
	return total
}
