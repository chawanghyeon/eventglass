package control

import (
	"context"
	"errors"
	"strings"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

type OutputIntentRegistration struct {
	Authority JobAuthority
	TenantID  int64
	Role      string
	ObjectKey string
	Intent    IntentAuthority
}

type JournalIntentRegistration struct {
	InstallationID    string
	StorageGeneration int64
	TenantID          int64
	ObjectKey         string
	Authority         IntentAuthority
}

func RegisterJournalIntent(ctx context.Context, pool *pgxpool.Pool, registration JournalIntentRegistration) error {
	if pool == nil || registration.TenantID <= 0 || registration.StorageGeneration <= 0 || registration.ObjectKey == "" || strings.HasPrefix(registration.ObjectKey, "/") || strings.Contains(registration.ObjectKey, "..") || strings.Contains(registration.ObjectKey, `\`) {
		return ErrInvalidVerifiedBatch
	}
	if _, err := model.LaneForAcceptance(registration.Authority.IntentID); err != nil || registration.Authority.Owner == "" || registration.Authority.Fence != 1 || registration.Authority.Bytes <= 0 || !validSHA(registration.Authority.SHA256) {
		return ErrInvalidVerifiedBatch
	}
	command, err := pool.Exec(ctx, `INSERT INTO object_intents(intent_id,installation_id,tenant_id,storage_generation,object_key,kind,state,owner,fence,expires_at,expected_bytes,expected_sha256)
		SELECT $1,$2,$3,$4,$5,'journal','pending',$6,$7,clock_timestamp()+interval '10 minutes',$8,$9
		FROM installations i JOIN tenants t ON t.tenant_id=$3
		WHERE i.singleton AND i.installation_id=$2 AND i.storage_generation=$4 AND t.state='active'`,
		registration.Authority.IntentID, registration.InstallationID, registration.TenantID, registration.StorageGeneration, registration.ObjectKey,
		registration.Authority.Owner, registration.Authority.Fence, registration.Authority.Bytes, registration.Authority.SHA256)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return ErrAuthorizationStale
	}
	return nil
}

// MarkJournalIntentUploaded records only a checksum-verified upload. The caller
// performs provider verification before invoking this SQL-only transition.
func MarkJournalIntentUploaded(ctx context.Context, pool *pgxpool.Pool, installationID string, generation, tenantID int64, authority IntentAuthority) error {
	if pool == nil || generation <= 0 || tenantID <= 0 || authority.Owner == "" || authority.Fence <= 0 || authority.Bytes <= 0 || !validSHA(authority.SHA256) {
		return ErrInvalidVerifiedBatch
	}
	command, err := pool.Exec(ctx, `UPDATE object_intents SET state='uploaded',uploaded_bytes=$7,uploaded_sha256=$8,updated_at=clock_timestamp()
		WHERE intent_id=$1 AND installation_id=$2 AND storage_generation=$3 AND tenant_id=$4 AND owner=$5 AND fence=$6
		AND state='pending' AND expires_at>clock_timestamp() AND expected_bytes=$7 AND expected_sha256=$8`,
		authority.IntentID, installationID, generation, tenantID, authority.Owner, authority.Fence, authority.Bytes, authority.SHA256)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return ErrIntentStale
	}
	return nil
}

// RegisterOutputIntent commits ownership before the caller uploads bytes. Every
// retry uses a fresh intent and object key; a stale conversion fence cannot
// later attach its bytes to a prepared output.
func RegisterOutputIntent(ctx context.Context, pool *pgxpool.Pool, registration OutputIntentRegistration) error {
	authority := registration.Authority
	intent := registration.Intent
	if pool == nil || registration.TenantID <= 0 || (registration.Role != "analytics" && registration.Role != "payload") || !safeObjectKey(registration.ObjectKey) || authority.InstallationID == "" || authority.StorageGeneration <= 0 || authority.JobID == "" || authority.Owner == "" || authority.Fence <= 0 || intent.IntentID == "" || intent.Owner != authority.Owner || intent.Fence != authority.Fence || intent.Bytes <= 0 || !validSHA(intent.SHA256) {
		return ErrInvalidVerifiedBatch
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := lockRuntimeGeneration(ctx, tx, authority.InstallationID, authority.StorageGeneration); err != nil {
		return err
	}
	var tenantID int64
	err = tx.QueryRow(ctx, `SELECT tenant_id FROM jobs WHERE job_id=$1 AND storage_generation=$2 AND state='running' AND owner=$3 AND fence=$4 AND lease_until>clock_timestamp() FOR SHARE`,
		authority.JobID, authority.StorageGeneration, authority.Owner, authority.Fence).Scan(&tenantID)
	if err != nil || tenantID != registration.TenantID {
		return errors.Join(ErrJobFenceStale, err)
	}
	command, err := tx.Exec(ctx, `INSERT INTO object_intents(intent_id,installation_id,tenant_id,storage_generation,object_key,kind,state,owner,fence,expires_at,expected_bytes,expected_sha256,conversion_job_id,producer_generation,producer_fence)
		VALUES($1,$2,$3,$4,$5,$6,'pending',$7,$8,clock_timestamp()+interval '10 minutes',$9,$10,$11,$4,$8)`,
		intent.IntentID, authority.InstallationID, registration.TenantID, authority.StorageGeneration, registration.ObjectKey, registration.Role,
		authority.Owner, authority.Fence, intent.Bytes, intent.SHA256, authority.JobID)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return ErrIntentStale
	}
	return tx.Commit(ctx)
}

func MarkOutputIntentUploaded(ctx context.Context, pool *pgxpool.Pool, authority JobAuthority, tenantID int64, intent IntentAuthority) error {
	if pool == nil || tenantID <= 0 || authority.InstallationID == "" || authority.StorageGeneration <= 0 || authority.JobID == "" || authority.Owner == "" || authority.Fence <= 0 || intent.Owner != authority.Owner || intent.Fence != authority.Fence || intent.Bytes <= 0 || !validSHA(intent.SHA256) {
		return ErrInvalidVerifiedBatch
	}
	command, err := pool.Exec(ctx, `UPDATE object_intents SET state='uploaded',uploaded_bytes=$9,uploaded_sha256=$10,updated_at=clock_timestamp()
		WHERE intent_id=$1 AND installation_id=$2 AND storage_generation=$3 AND tenant_id=$4 AND conversion_job_id=$5
		AND producer_generation=$3 AND producer_fence=$6 AND owner=$7 AND fence=$8 AND state='pending'
		AND expires_at>clock_timestamp() AND expected_bytes=$9 AND expected_sha256=$10
		AND EXISTS (SELECT 1 FROM jobs WHERE job_id=$5 AND storage_generation=$3 AND state='running' AND owner=$7 AND fence=$6 AND lease_until>clock_timestamp())`,
		intent.IntentID, authority.InstallationID, authority.StorageGeneration, tenantID, authority.JobID, authority.Fence, authority.Owner, intent.Fence, intent.Bytes, intent.SHA256)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return ErrIntentStale
	}
	return nil
}

func safeObjectKey(value string) bool {
	return value != "" && !strings.HasPrefix(value, "/") && !strings.Contains(value, "..") && !strings.Contains(value, `\`)
}
