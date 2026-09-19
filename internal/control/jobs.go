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
		SELECT job_id FROM jobs
		WHERE kind='convert' AND storage_generation=$1 AND fence<$2 AND attempt<$3
		  AND ((state='queued' AND retry_at<=clock_timestamp()) OR (state='running' AND lease_until<=clock_timestamp()))
		ORDER BY retry_at,created_at,job_id FOR UPDATE SKIP LOCKED LIMIT 1
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
