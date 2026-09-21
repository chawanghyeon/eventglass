package control

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrJobFenceStale = errors.New("job lease authority is stale")

type JobAuthority struct {
	InstallationID    string
	StorageGeneration int64
	JobID             string
	Owner             string
	Fence             int64
}

type ConversionJob struct {
	Authority  JobAuthority
	TenantID   int64
	LaneID     int
	BatchSeq   int64
	Attempt    int
	LeaseUntil time.Time
}

type PublicationJob struct {
	Authority   JobAuthority
	TenantID    int64
	LaneID      int
	BatchSeq    int64
	OutputID    string
	ManifestSHA string
	Attempt     int
	LeaseUntil  time.Time
}

func ClaimConversionJob(ctx context.Context, pool *pgxpool.Pool, installationID string, generation int64, owner string, lease time.Duration) (*ConversionJob, error) {
	if pool == nil || installationID == "" || generation <= 0 || owner == "" || lease <= 0 || lease > 5*time.Minute {
		return nil, errors.New("invalid conversion job claim")
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = on"); err != nil {
		return nil, err
	}
	if err := lockRuntimeGeneration(ctx, tx, installationID, generation); err != nil {
		return nil, err
	}
	leaseMicroseconds := lease.Microseconds()
	var job ConversionJob
	err = tx.QueryRow(ctx, `WITH candidate AS (
		SELECT j.job_id FROM jobs j
		WHERE j.kind='convert' AND j.storage_generation=$1 AND j.prepared_output_id IS NULL AND j.fence<$2 AND j.attempt<$3
		  AND ((state='queued' AND retry_at<=clock_timestamp()) OR (state='running' AND lease_until<=clock_timestamp()))
		ORDER BY (SELECT count(*) FROM jobs active WHERE active.tenant_id=j.tenant_id AND active.state='running' AND active.lease_until>clock_timestamp()),j.retry_at,j.created_at,j.job_id
		FOR UPDATE OF j SKIP LOCKED LIMIT 1
	)
	UPDATE jobs j SET state='running',attempt=j.attempt+1,fence=j.fence+1,owner=$4,
		lease_until=clock_timestamp()+($5::bigint * interval '1 microsecond'),updated_at=clock_timestamp()
	FROM candidate WHERE j.job_id=candidate.job_id
	RETURNING j.job_id::text,j.tenant_id,j.lane_id,j.batch_seq,j.attempt,j.fence,j.lease_until`,
		generation, int64(math.MaxInt64), int32(math.MaxInt32), owner, leaseMicroseconds).Scan(
		&job.Authority.JobID, &job.TenantID, &job.LaneID, &job.BatchSeq, &job.Attempt, &job.Authority.Fence, &job.LeaseUntil)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	job.Authority.InstallationID = installationID
	job.Authority.StorageGeneration = generation
	job.Authority.Owner = owner
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &job, nil
}

func HeartbeatConversionJob(ctx context.Context, pool *pgxpool.Pool, authority JobAuthority, lease time.Duration) (time.Time, error) {
	if pool == nil || authority.InstallationID == "" || authority.StorageGeneration <= 0 || authority.JobID == "" || authority.Owner == "" || authority.Fence <= 0 || lease <= 0 || lease > 5*time.Minute {
		return time.Time{}, ErrJobFenceStale
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return time.Time{}, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = on"); err != nil {
		return time.Time{}, err
	}
	if err := lockRuntimeGeneration(ctx, tx, authority.InstallationID, authority.StorageGeneration); err != nil {
		return time.Time{}, err
	}
	var leaseUntil time.Time
	err = tx.QueryRow(ctx, `UPDATE jobs SET lease_until=clock_timestamp()+($5::bigint * interval '1 microsecond'),updated_at=clock_timestamp()
		WHERE job_id=$1 AND storage_generation=$2 AND state='running' AND owner=$3 AND fence=$4 AND lease_until>clock_timestamp()
		RETURNING lease_until`, authority.JobID, authority.StorageGeneration, authority.Owner, authority.Fence, lease.Microseconds()).Scan(&leaseUntil)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, ErrJobFenceStale
	}
	if err != nil {
		return time.Time{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return time.Time{}, err
	}
	return leaseUntil, nil
}

// ClaimPublication locks one explicit lane before its next prepared job. This
// preserves lane -> job -> intent lock order and never starts a native child.
func ClaimPublicationJob(ctx context.Context, pool *pgxpool.Pool, installationID string, generation, tenantID int64, laneID int, owner string, lease time.Duration) (*PublicationJob, error) {
	if pool == nil || installationID == "" || generation <= 0 || tenantID <= 0 || laneID < 0 || laneID >= 16 || owner == "" || lease <= 0 || lease > 5*time.Minute {
		return nil, errors.New("invalid publication job claim")
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = on"); err != nil {
		return nil, err
	}
	if err := lockRuntimeGeneration(ctx, tx, installationID, generation); err != nil {
		return nil, err
	}
	var publishedSeq int64
	if err := tx.QueryRow(ctx, `SELECT published_seq FROM lanes WHERE tenant_id=$1 AND lane_id=$2 FOR UPDATE`, tenantID, laneID).Scan(&publishedSeq); err != nil {
		return nil, err
	}
	var job PublicationJob
	err = tx.QueryRow(ctx, `SELECT j.job_id::text,j.batch_seq,j.prepared_output_id::text,o.manifest_sha256,j.attempt,j.fence
		FROM jobs j JOIN job_outputs o ON o.output_id=j.prepared_output_id AND o.tenant_id=j.tenant_id
		WHERE j.tenant_id=$1 AND j.lane_id=$2 AND j.batch_seq=$3 AND j.storage_generation=$4 AND j.prepared_output_id IS NOT NULL
		AND ((j.state='prepared') OR (j.state='running' AND j.lease_until<=clock_timestamp())) AND o.state='prepared'
		FOR UPDATE OF j,o`, tenantID, laneID, publishedSeq+1, generation).Scan(
		&job.Authority.JobID, &job.BatchSeq, &job.OutputID, &job.ManifestSHA, &job.Attempt, &job.Authority.Fence)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if job.Authority.Fence == math.MaxInt64 || job.Attempt == math.MaxInt32 {
		return nil, errors.New("publication job authority overflow")
	}
	rows, err := tx.Query(ctx, `SELECT intent_id FROM object_intents WHERE tenant_id=$1 AND conversion_job_id=$2 AND state='referenced' AND kind IN ('analytics','payload') ORDER BY intent_id FOR UPDATE`, tenantID, job.Authority.JobID)
	if err != nil {
		return nil, err
	}
	var intentIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		intentIDs = append(intentIDs, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	newFence := job.Authority.Fence + 1
	if len(intentIDs) != 0 {
		result, err := tx.Exec(ctx, `UPDATE object_intents SET owner=$3,fence=$4,producer_generation=$5,producer_fence=$4,updated_at=clock_timestamp()
			WHERE tenant_id=$1 AND intent_id=ANY($2::uuid[]) AND state='referenced' AND storage_generation=$5`, tenantID, intentIDs, owner, newFence, generation)
		if err != nil {
			return nil, err
		}
		if result.RowsAffected() != int64(len(intentIDs)) {
			return nil, ErrIntentStale
		}
	}
	err = tx.QueryRow(ctx, `UPDATE jobs SET state='running',attempt=attempt+1,fence=$2,owner=$3,
		lease_until=clock_timestamp()+($4::bigint*interval '1 microsecond'),updated_at=clock_timestamp()
		WHERE job_id=$1 RETURNING attempt,lease_until`, job.Authority.JobID, newFence, owner, lease.Microseconds()).Scan(&job.Attempt, &job.LeaseUntil)
	if err != nil {
		return nil, err
	}
	job.Authority = JobAuthority{InstallationID: installationID, StorageGeneration: generation, JobID: job.Authority.JobID, Owner: owner, Fence: newFence}
	job.TenantID, job.LaneID = tenantID, laneID
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &job, nil
}

func lockRuntimeGeneration(ctx context.Context, tx pgx.Tx, installationID string, generation int64) error {
	var currentID string
	var currentGeneration int64
	if err := tx.QueryRow(ctx, `SELECT installation_id::text,storage_generation FROM installations WHERE singleton FOR SHARE`).Scan(&currentID, &currentGeneration); err != nil {
		return err
	}
	if currentID != installationID || currentGeneration != generation {
		return fmt.Errorf("%w: installation generation changed", ErrJobFenceStale)
	}
	return nil
}
