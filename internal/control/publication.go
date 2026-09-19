package control

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DurableConversionReceipt struct {
	Receipt         ReceiptResult
	GroupingVersion int
}

type ConversionWork struct {
	TenantID         int64
	LaneID           int
	BatchSeq         int64
	BatchID          string
	JournalObjectKey string
	JournalBytes     int64
	JournalSHA256    string
	Receipts         []DurableConversionReceipt
}

type PublicationOperations struct{ pool *pgxpool.Pool }

type PublicationObject struct {
	ObjectKey string
	Bytes     int64
	SHA256    string
}

type PublicationLane struct {
	TenantID int64
	LaneID   int
}

func NewPublicationOperations(pool *pgxpool.Pool) (*PublicationOperations, error) {
	if pool == nil {
		return nil, errors.New("PostgreSQL pool is required")
	}
	return &PublicationOperations{pool: pool}, nil
}

func (operations *PublicationOperations) ClaimConversion(ctx context.Context, installationID string, generation int64, owner string, lease time.Duration) (*ConversionJob, error) {
	return ClaimConversionJob(ctx, operations.pool, installationID, generation, owner, lease)
}

func (operations *PublicationOperations) Heartbeat(ctx context.Context, authority JobAuthority, lease time.Duration) (time.Time, error) {
	return HeartbeatConversionJob(ctx, operations.pool, authority, lease)
}

func (operations *PublicationOperations) LoadConversion(ctx context.Context, authority JobAuthority) (ConversionWork, error) {
	return LoadConversionWork(ctx, operations.pool, authority)
}

func (operations *PublicationOperations) RegisterOutputIntent(ctx context.Context, registration OutputIntentRegistration) error {
	return RegisterOutputIntent(ctx, operations.pool, registration)
}

func (operations *PublicationOperations) MarkOutputIntentUploaded(ctx context.Context, authority JobAuthority, tenantID int64, intent IntentAuthority) error {
	return MarkOutputIntentUploaded(ctx, operations.pool, authority, tenantID, intent)
}

func (operations *PublicationOperations) Prepare(ctx context.Context, command PrepareCommand) error {
	return Prepare(ctx, operations.pool, command)
}

func (operations *PublicationOperations) ClaimPublication(ctx context.Context, installationID string, generation, tenantID int64, laneID int, owner string, lease time.Duration) (*PublicationJob, error) {
	return ClaimPublicationJob(ctx, operations.pool, installationID, generation, tenantID, laneID, owner, lease)
}

func (operations *PublicationOperations) Publish(ctx context.Context, command PublishCommand) (PublishResult, error) {
	return Publish(ctx, operations.pool, command)
}

func (operations *PublicationOperations) PublicationLanes(ctx context.Context, generation int64, limit int) ([]PublicationLane, error) {
	if generation <= 0 || limit <= 0 || limit > model.LaneCount*1024 {
		return nil, errors.New("invalid publication lane scan")
	}
	rows, err := operations.pool.Query(ctx, `SELECT l.tenant_id,l.lane_id FROM lanes l JOIN jobs j
		ON j.tenant_id=l.tenant_id AND j.lane_id=l.lane_id AND j.batch_seq=l.published_seq+1
		WHERE j.storage_generation=$1 AND j.prepared_output_id IS NOT NULL
		AND (j.state='prepared' OR (j.state='running' AND j.lease_until<=clock_timestamp()))
		ORDER BY j.updated_at,l.tenant_id,l.lane_id LIMIT $2`, generation, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]PublicationLane, 0)
	for rows.Next() {
		var lane PublicationLane
		if err := rows.Scan(&lane.TenantID, &lane.LaneID); err != nil {
			return nil, err
		}
		result = append(result, lane)
	}
	return result, rows.Err()
}

func (operations *PublicationOperations) LoadPublicationObjects(ctx context.Context, authority JobAuthority, outputID string) ([]PublicationObject, error) {
	return LoadPublicationObjects(ctx, operations.pool, authority, outputID)
}

func LoadConversionWork(ctx context.Context, pool *pgxpool.Pool, authority JobAuthority) (ConversionWork, error) {
	if pool == nil || authority.InstallationID == "" || authority.StorageGeneration <= 0 || authority.JobID == "" || authority.Owner == "" || authority.Fence <= 0 {
		return ConversionWork{}, ErrJobFenceStale
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ConversionWork{}, err
	}
	defer tx.Rollback(ctx)
	if err := lockRuntimeGeneration(ctx, tx, authority.InstallationID, authority.StorageGeneration); err != nil {
		return ConversionWork{}, err
	}
	var work ConversionWork
	var live bool
	err = tx.QueryRow(ctx, `SELECT j.tenant_id,j.lane_id,j.batch_seq,b.batch_id::text,oi.object_key,oi.expected_bytes,oi.expected_sha256,
		COALESCE(j.lease_until>clock_timestamp(),false)
		FROM jobs j JOIN ingest_batches b ON b.tenant_id=j.tenant_id AND b.lane_id=j.lane_id AND b.batch_seq=j.batch_seq
		JOIN object_intents oi ON oi.tenant_id=b.tenant_id AND oi.intent_id=b.journal_intent_id
		WHERE j.job_id=$1 AND j.storage_generation=$2 AND j.state='running' AND j.owner=$3 AND j.fence=$4 AND j.prepared_output_id IS NULL
		AND b.state='accepted' AND oi.state='referenced'`, authority.JobID, authority.StorageGeneration, authority.Owner, authority.Fence).Scan(
		&work.TenantID, &work.LaneID, &work.BatchSeq, &work.BatchID, &work.JournalObjectKey, &work.JournalBytes, &work.JournalSHA256, &live)
	if err != nil || !live {
		return ConversionWork{}, errors.Join(ErrJobFenceStale, err)
	}
	rows, err := tx.Query(ctx, `SELECT acceptance_id::text,tenant_id,project_id,lane_id,batch_seq,content_sha256,selection_json,selection_sha256,
		ordinal_first,ordinal_last,accepted_count,duplicate_count,conflict_count,unsupported_count,received_time_us,grouping_version
		FROM receipts WHERE tenant_id=$1 AND lane_id=$2 AND batch_seq=$3 ORDER BY request_index`, work.TenantID, work.LaneID, work.BatchSeq)
	if err != nil {
		return ConversionWork{}, err
	}
	for rows.Next() {
		var item DurableConversionReceipt
		var selectionJSON []byte
		receipt := &item.Receipt
		if err := rows.Scan(&receipt.AcceptanceID, &receipt.TenantID, &receipt.ProjectID, &receipt.LaneID, &receipt.BatchSeq, &receipt.ContentSHA256, &selectionJSON, &receipt.SelectionSHA256,
			&receipt.OrdinalFirst, &receipt.OrdinalLast, &receipt.AcceptedCount, &receipt.DuplicateCount, &receipt.ConflictCount, &receipt.UnsupportedCount, &receipt.ReceivedTimeUS, &item.GroupingVersion); err != nil {
			rows.Close()
			return ConversionWork{}, err
		}
		if err := json.Unmarshal(selectionJSON, &receipt.Selection); err != nil {
			rows.Close()
			return ConversionWork{}, err
		}
		work.Receipts = append(work.Receipts, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ConversionWork{}, err
	}
	rows.Close()
	if len(work.Receipts) == 0 {
		return ConversionWork{}, errors.New("conversion job has no durable receipts")
	}
	if err := tx.Commit(ctx); err != nil {
		return ConversionWork{}, err
	}
	return work, nil
}

// LoadPublicationObjects returns the immutable prepared references for
// verification outside the Publish transaction.
func LoadPublicationObjects(ctx context.Context, pool *pgxpool.Pool, authority JobAuthority, outputID string) ([]PublicationObject, error) {
	if pool == nil || authority.InstallationID == "" || authority.StorageGeneration <= 0 || authority.JobID == "" || authority.Owner == "" || authority.Fence <= 0 || outputID == "" {
		return nil, ErrJobFenceStale
	}
	var live bool
	err := pool.QueryRow(ctx, `SELECT COALESCE(lease_until>clock_timestamp(),false) FROM jobs
		WHERE job_id=$1 AND storage_generation=$2 AND state='running' AND owner=$3 AND fence=$4 AND prepared_output_id=$5
		AND EXISTS (SELECT 1 FROM installations WHERE singleton AND installation_id=$6 AND storage_generation=$2)`,
		authority.JobID, authority.StorageGeneration, authority.Owner, authority.Fence, outputID, authority.InstallationID).Scan(&live)
	if err != nil || !live {
		return nil, errors.Join(ErrJobFenceStale, err)
	}
	rows, err := pool.Query(ctx, `SELECT object_key,expected_bytes,expected_sha256 FROM object_intents
		WHERE conversion_job_id=$1 AND storage_generation=$2 AND state='referenced' AND owner=$3 AND fence=$4 AND kind IN ('analytics','payload')
		ORDER BY intent_id`, authority.JobID, authority.StorageGeneration, authority.Owner, authority.Fence)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	objects := make([]PublicationObject, 0)
	for rows.Next() {
		var object PublicationObject
		if err := rows.Scan(&object.ObjectKey, &object.Bytes, &object.SHA256); err != nil {
			return nil, err
		}
		objects = append(objects, object)
	}
	return objects, rows.Err()
}
