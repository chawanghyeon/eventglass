package control

import (
	"context"
	"strings"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

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
