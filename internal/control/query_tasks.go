package control

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const MaxRunningQueryTasks = 4

type QueryTaskAuthority struct {
	InstallationID    string
	StorageGeneration int64
	TenantID          int64
	QueryID           string
	Key               model.QueryTaskKey
	Owner             string
	Fence             int64
	Attempt           int
}

type QueryInputArtifact struct {
	Ordinal     int
	Producer    model.QueryTaskKey
	IntentID    string
	ObjectKey   string
	Bytes       int64
	SHA256      string
	BlockSHA256 []string
}

type QueryTask struct {
	Authority  QueryTaskAuthority
	Manifest   []byte
	Inputs     []QueryInputArtifact
	Deadline   time.Time
	LeaseUntil time.Time
}

type CompleteQueryTaskCommand struct {
	Authority   QueryTaskAuthority
	IntentID    string
	SHA256      string
	Rows        int64
	Bytes       int64
	BlockSHA256 []string
	CacheBytes  int64
}

func (operations *QueryOperations) ClaimQueryCoordinator(ctx context.Context, installationID string, generation int64, owner string) (*QueryJob, error) {
	if installationID == "" || generation <= 0 || owner == "" || len(owner) > 128 {
		return nil, errors.New("invalid query coordinator claim")
	}
	tx, err := operations.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if err := lockRuntimeGeneration(ctx, tx, installationID, generation); err != nil {
		return nil, err
	}
	var job QueryJob
	var previousFence int64
	err = tx.QueryRow(ctx, `SELECT query_id::text,tenant_id,user_id,snapshot_id::text,deadline,expires_at,coordinator_fence
		FROM query_jobs WHERE state='planning' AND deadline>clock_timestamp()
		AND (lease_until IS NULL OR lease_until<=clock_timestamp())
		ORDER BY created_at,query_id FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(
		&job.Authority.QueryID, &job.Authority.TenantID, &job.UserID, &job.SnapshotID, &job.Deadline, &job.ExpiresAt, &previousFence)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if previousFence == math.MaxInt64 {
		return nil, errors.New("query coordinator fence overflow")
	}
	job.Authority.Owner, job.Authority.Fence, job.Authority.StorageGeneration = owner, previousFence+1, generation
	job.State = "planning"
	result, err := tx.Exec(ctx, `UPDATE query_jobs SET coordinator_owner=$3,coordinator_fence=$4,
		lease_until=LEAST(deadline,clock_timestamp()+interval '60 seconds'),updated_at=clock_timestamp()
		WHERE tenant_id=$1 AND query_id=$2 AND state='planning' AND coordinator_fence=$5`,
		job.Authority.TenantID, job.Authority.QueryID, owner, job.Authority.Fence, previousFence)
	if err != nil || result.RowsAffected() != 1 {
		return nil, errors.Join(ErrQueryFenceStale, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &job, nil
}

func (operations *QueryOperations) ClaimQueryTask(ctx context.Context, installationID string, generation int64, owner string) (*QueryTask, error) {
	return operations.claimQueryTask(ctx, installationID, generation, owner, "")
}

// ClaimQueryTaskForQuery uses the same durable scheduler and fencing path as a
// background worker, but restricts admission to one already-authorized query.
// It lets an interactive request help its own query without stealing unrelated
// tenant work.
func (operations *QueryOperations) ClaimQueryTaskForQuery(ctx context.Context, installationID string, generation int64, owner, queryID string) (*QueryTask, error) {
	if uuid.Validate(queryID) != nil {
		return nil, errors.New("invalid target query")
	}
	return operations.claimQueryTask(ctx, installationID, generation, owner, queryID)
}

func (operations *QueryOperations) claimQueryTask(ctx context.Context, installationID string, generation int64, owner, targetQueryID string) (*QueryTask, error) {
	if installationID == "" || generation <= 0 || owner == "" || len(owner) > 128 {
		return nil, errors.New("invalid query task claim")
	}
	var target any
	if targetQueryID != "" {
		target = targetQueryID
	}
	tx, err := operations.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if err := lockRuntimeGeneration(ctx, tx, installationID, generation); err != nil {
		return nil, err
	}
	var queryID, snapshotID string
	var tenantID int64
	var deadline time.Time
	err = tx.QueryRow(ctx, `SELECT q.query_id::text,q.tenant_id,q.snapshot_id::text,q.deadline
		FROM query_jobs q WHERE q.state IN ('queued','running') AND q.deadline>clock_timestamp()
		AND ($1::uuid IS NULL OR q.query_id=$1::uuid)
		AND EXISTS (SELECT 1 FROM query_tasks t WHERE t.query_id=q.query_id AND t.tenant_id=q.tenant_id
			AND t.state='queued' AND t.retry_at<=clock_timestamp() AND t.attempt<3
			AND NOT EXISTS (SELECT 1 FROM query_task_inputs i
				JOIN query_tasks p ON p.tenant_id=i.tenant_id AND p.query_id=i.query_id
				AND p.stage=i.producer_stage AND p.level=i.producer_level AND p.partition_id=i.producer_partition_id
				WHERE i.tenant_id=t.tenant_id AND i.query_id=t.query_id AND i.consumer_stage=t.stage
				AND i.consumer_level=t.level AND i.consumer_partition_id=t.partition_id AND p.state<>'succeeded'))
		ORDER BY (SELECT count(*) FROM query_tasks active WHERE active.tenant_id=q.tenant_id AND active.state='running' AND active.lease_until>clock_timestamp()),q.deadline,q.query_id
		FOR UPDATE OF q SKIP LOCKED LIMIT 1`, target).Scan(&queryID, &tenantID, &snapshotID, &deadline)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var running int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM query_tasks WHERE tenant_id=$1 AND query_id=$2 AND state='running'`, tenantID, queryID).Scan(&running); err != nil {
		return nil, err
	}
	if running >= MaxRunningQueryTasks {
		return nil, nil
	}
	var task QueryTask
	task.Authority = QueryTaskAuthority{InstallationID: installationID, StorageGeneration: generation, TenantID: tenantID, QueryID: queryID, Owner: owner}
	var previousFence int64
	err = tx.QueryRow(ctx, `SELECT stage,level,partition_id,attempt,fence,manifest_json
		FROM query_tasks t WHERE tenant_id=$1 AND query_id=$2 AND state='queued' AND retry_at<=clock_timestamp() AND attempt<3
		AND NOT EXISTS (SELECT 1 FROM query_task_inputs i
			JOIN query_tasks p ON p.tenant_id=i.tenant_id AND p.query_id=i.query_id
			AND p.stage=i.producer_stage AND p.level=i.producer_level AND p.partition_id=i.producer_partition_id
			WHERE i.tenant_id=t.tenant_id AND i.query_id=t.query_id AND i.consumer_stage=t.stage
			AND i.consumer_level=t.level AND i.consumer_partition_id=t.partition_id AND p.state<>'succeeded')
		ORDER BY level,partition_id FOR UPDATE SKIP LOCKED LIMIT 1`, tenantID, queryID).Scan(
		&task.Authority.Key.Stage, &task.Authority.Key.Level, &task.Authority.Key.PartitionID, &task.Authority.Attempt, &previousFence, &task.Manifest)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if previousFence == math.MaxInt64 || task.Authority.Attempt >= 3 {
		return nil, ErrQueryLimitExceeded
	}
	task.Authority.Fence, task.Authority.Attempt = previousFence+1, task.Authority.Attempt+1
	err = tx.QueryRow(ctx, `UPDATE query_tasks SET state='running',owner=$7,fence=$8,attempt=$9,
		lease_until=LEAST($10,clock_timestamp()+interval '60 seconds')
		WHERE tenant_id=$1 AND query_id=$2 AND stage=$3 AND level=$4 AND partition_id=$5 AND state='queued' AND fence=$6
		RETURNING lease_until`, tenantID, queryID, task.Authority.Key.Stage, task.Authority.Key.Level,
		task.Authority.Key.PartitionID, previousFence, owner, task.Authority.Fence, task.Authority.Attempt, deadline).Scan(&task.LeaseUntil)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE query_jobs SET state='running',updated_at=clock_timestamp()
		WHERE tenant_id=$1 AND query_id=$2 AND state='queued'`, tenantID, queryID); err != nil {
		return nil, err
	}
	inputRows, err := tx.Query(ctx, `SELECT i.ordinal,i.producer_stage,i.producer_level,i.producer_partition_id,
		p.result_intent_id::text,oi.object_key,p.result_bytes,p.result_sha256,
		COALESCE((SELECT array_agg(qb.sha256 ORDER BY qb.block_index) FROM query_task_blocks qb
			WHERE qb.tenant_id=p.tenant_id AND qb.query_id=p.query_id AND qb.stage=p.stage AND qb.level=p.level AND qb.partition_id=p.partition_id),ARRAY[]::text[])
		FROM query_task_inputs i JOIN query_tasks p ON p.tenant_id=i.tenant_id AND p.query_id=i.query_id
		AND p.stage=i.producer_stage AND p.level=i.producer_level AND p.partition_id=i.producer_partition_id
		JOIN object_intents oi ON oi.tenant_id=p.tenant_id AND oi.intent_id=p.result_intent_id AND oi.state='referenced'
		WHERE i.tenant_id=$1 AND i.query_id=$2 AND i.consumer_stage=$3 AND i.consumer_level=$4 AND i.consumer_partition_id=$5
		ORDER BY i.ordinal`, tenantID, queryID, task.Authority.Key.Stage, task.Authority.Key.Level, task.Authority.Key.PartitionID)
	if err != nil {
		return nil, err
	}
	for inputRows.Next() {
		var input QueryInputArtifact
		if err := inputRows.Scan(&input.Ordinal, &input.Producer.Stage, &input.Producer.Level, &input.Producer.PartitionID,
			&input.IntentID, &input.ObjectKey, &input.Bytes, &input.SHA256, &input.BlockSHA256); err != nil {
			inputRows.Close()
			return nil, err
		}
		task.Inputs = append(task.Inputs, input)
	}
	if err := inputRows.Err(); err != nil {
		inputRows.Close()
		return nil, err
	}
	inputRows.Close()
	task.Deadline = deadline
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &task, nil
}

func (operations *QueryOperations) HeartbeatQueryTask(ctx context.Context, authority QueryTaskAuthority) (time.Time, error) {
	if err := validateQueryTaskAuthority(authority); err != nil {
		return time.Time{}, err
	}
	tx, err := operations.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return time.Time{}, err
	}
	defer tx.Rollback(ctx)
	if err := lockRuntimeGeneration(ctx, tx, authority.InstallationID, authority.StorageGeneration); err != nil {
		return time.Time{}, err
	}
	var leaseUntil time.Time
	err = tx.QueryRow(ctx, `UPDATE query_tasks t SET lease_until=LEAST(q.deadline,clock_timestamp()+interval '60 seconds')
		FROM query_jobs q WHERE t.tenant_id=$1 AND t.query_id=$2 AND t.stage=$3 AND t.level=$4 AND t.partition_id=$5
		AND t.owner=$6 AND t.fence=$7 AND t.attempt=$8 AND t.state='running' AND t.lease_until>clock_timestamp()
		AND q.tenant_id=t.tenant_id AND q.query_id=t.query_id AND q.state='running' AND q.deadline>clock_timestamp()
		RETURNING t.lease_until`, authority.TenantID, authority.QueryID, authority.Key.Stage, authority.Key.Level,
		authority.Key.PartitionID, authority.Owner, authority.Fence, authority.Attempt).Scan(&leaseUntil)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, ErrQueryFenceStale
	}
	if err != nil {
		return time.Time{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return time.Time{}, err
	}
	return leaseUntil, nil
}

func validateQueryTaskAuthority(authority QueryTaskAuthority) error {
	if authority.InstallationID == "" || authority.StorageGeneration <= 0 || authority.TenantID <= 0 || uuid.Validate(authority.QueryID) != nil || authority.Owner == "" || authority.Fence <= 0 || authority.Attempt < 1 || authority.Attempt > 3 || authority.Key.PartitionID < 0 || authority.Key.Stage == model.QueryTaskScan && authority.Key.Level != 0 || authority.Key.Stage == model.QueryTaskReduce && authority.Key.Level <= 0 {
		return ErrQueryFenceStale
	}
	return nil
}
