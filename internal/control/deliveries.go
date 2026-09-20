package control

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var ErrDeliveryLeaseLost = errors.New("delivery lease lost")

type DeliveryAuthority struct {
	InstallationID    string
	StorageGeneration int64
	TenantID          int64
	DeliveryID        string
	Owner             string
	Fence             int64
}

type Delivery struct {
	Authority                     DeliveryAuthority
	ProjectID                     int64
	AlertID                       string
	AlertRevision, Revision       int64
	DestinationID                 string
	DestinationRevision           int64
	DestinationURL                string
	DestinationSecretCiphertext   []byte
	DestinationEncryptionKeyID    string
	BodyBytes                     []byte
	BodySHA256                    string
	State                         string
	Attempt                       int
	RetryAt, CreatedAt, UpdatedAt time.Time
	LeaseUntil                    *time.Time
	LastStatus                    *int
	ErrorCode                     *string
}

func (operations *AlertOperations) ClaimDelivery(ctx context.Context, installationID string, generation int64, owner string, lease time.Duration) (*Delivery, error) {
	if uuid.Validate(installationID) != nil || generation <= 0 || owner == "" || lease <= 0 || lease > time.Minute {
		return nil, errors.New("invalid delivery claim")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var currentID string
	var currentGeneration int64
	var paused bool
	if err := tx.QueryRow(ctx, `SELECT installation_id::text,storage_generation,alerts_paused FROM installations WHERE singleton FOR SHARE`).Scan(&currentID, &currentGeneration, &paused); err != nil {
		return nil, err
	}
	if currentID != installationID || currentGeneration != generation {
		return nil, ErrStorageGeneration
	}
	if paused {
		return nil, tx.Commit(ctx)
	}
	if _, err := tx.Exec(ctx, `UPDATE deliveries SET state='failed',owner=NULL,lease_until=NULL,error_code='attempts_exhausted',revision=revision+1,updated_at=clock_timestamp()
		WHERE attempt>=12 AND (state='queued' OR (state='running' AND lease_until<=clock_timestamp()))`); err != nil {
		return nil, err
	}
	var result Delivery
	var secret []byte
	var keyID string
	err = tx.QueryRow(ctx, `SELECT tenant_id,delivery_id::text FROM deliveries
		WHERE attempt<12 AND ((state='queued' AND retry_at<=clock_timestamp()) OR (state='running' AND lease_until<=clock_timestamp()))
		ORDER BY retry_at,created_at,delivery_id FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&result.Authority.TenantID, &result.Authority.DeliveryID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, tx.Commit(ctx)
	}
	if err != nil {
		return nil, err
	}
	err = tx.QueryRow(ctx, `UPDATE deliveries SET state='running',owner=$3,fence=fence+1,attempt=attempt+1,lease_until=clock_timestamp()+($4::bigint*interval '1 microsecond'),revision=revision+1,updated_at=clock_timestamp()
		WHERE tenant_id=$1 AND delivery_id=$2 RETURNING project_id,alert_id::text,alert_revision,revision,destination_id::text,destination_revision,destination_url,
		coalesce(destination_secret_ciphertext,''::bytea),coalesce(destination_encryption_key_id,''),body_bytes,body_sha256,state,attempt,retry_at,lease_until,last_status,error_code,created_at,updated_at,fence`,
		result.Authority.TenantID, result.Authority.DeliveryID, owner, lease.Microseconds()).Scan(&result.ProjectID, &result.AlertID, &result.AlertRevision, &result.Revision, &result.DestinationID, &result.DestinationRevision, &result.DestinationURL, &secret, &keyID, &result.BodyBytes, &result.BodySHA256, &result.State, &result.Attempt, &result.RetryAt, &result.LeaseUntil, &result.LastStatus, &result.ErrorCode, &result.CreatedAt, &result.UpdatedAt, &result.Authority.Fence)
	if err != nil {
		return nil, err
	}
	result.DestinationSecretCiphertext = append([]byte(nil), secret...)
	result.DestinationEncryptionKeyID = keyID
	result.Authority.InstallationID, result.Authority.StorageGeneration, result.Authority.Owner = installationID, generation, owner
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &result, nil
}

func (operations *AlertOperations) HeartbeatDelivery(ctx context.Context, authority DeliveryAuthority, lease time.Duration) error {
	if err := validateDeliveryAuthority(authority); err != nil || lease <= 0 || lease > time.Minute {
		return errors.New("invalid delivery heartbeat")
	}
	result, err := operations.pool.Exec(ctx, `UPDATE deliveries d SET lease_until=clock_timestamp()+($7::bigint*interval '1 microsecond'),updated_at=clock_timestamp()
		FROM installations i WHERE i.singleton AND i.installation_id=$1 AND i.storage_generation=$2 AND NOT i.alerts_paused
		AND d.tenant_id=$3 AND d.delivery_id=$4 AND d.state='running' AND d.owner=$5 AND d.fence=$6 AND d.lease_until>clock_timestamp()`, authority.InstallationID, authority.StorageGeneration, authority.TenantID, authority.DeliveryID, authority.Owner, authority.Fence, lease.Microseconds())
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return ErrDeliveryLeaseLost
	}
	return nil
}

func (operations *AlertOperations) CompleteDelivery(ctx context.Context, authority DeliveryAuthority, status int) error {
	if err := validateDeliveryAuthority(authority); err != nil || status < 200 || status > 299 {
		return errors.New("invalid delivery completion")
	}
	return operations.finishDelivery(ctx, authority, "succeeded", status, "", 0)
}

func (operations *AlertOperations) FailDelivery(ctx context.Context, authority DeliveryAuthority, status int, code string, retry bool, delay time.Duration) error {
	if err := validateDeliveryAuthority(authority); err != nil || status != 0 && (status < 100 || status > 599) || code == "" || len(code) > 64 || delay < 0 || delay > time.Hour {
		return errors.New("invalid delivery failure")
	}
	state := "failed"
	if retry {
		state = "queued"
	}
	return operations.finishDelivery(ctx, authority, state, status, code, delay)
}

func (operations *AlertOperations) finishDelivery(ctx context.Context, authority DeliveryAuthority, requestedState string, status int, code string, delay time.Duration) error {
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var currentID string
	var generation int64
	var paused bool
	if err := tx.QueryRow(ctx, `SELECT installation_id::text,storage_generation,alerts_paused FROM installations WHERE singleton FOR SHARE`).Scan(&currentID, &generation, &paused); err != nil {
		return err
	}
	if currentID != authority.InstallationID || generation != authority.StorageGeneration {
		return ErrStorageGeneration
	}
	state := requestedState
	if paused && state == "queued" {
		state, code = "failed", "alerts_paused"
	}
	var storedAttempt int
	if err := tx.QueryRow(ctx, `SELECT attempt FROM deliveries WHERE tenant_id=$1 AND delivery_id=$2 AND state='running' AND owner=$3 AND fence=$4 AND lease_until>clock_timestamp() FOR UPDATE`, authority.TenantID, authority.DeliveryID, authority.Owner, authority.Fence).Scan(&storedAttempt); errors.Is(err, pgx.ErrNoRows) {
		return ErrDeliveryLeaseLost
	} else if err != nil {
		return err
	}
	if state == "queued" && storedAttempt >= 12 {
		state, code = "failed", "attempts_exhausted"
	}
	var statusValue any
	if status != 0 {
		statusValue = status
	}
	_, err = tx.Exec(ctx, `UPDATE deliveries SET state=$5,owner=NULL,lease_until=NULL,last_status=$6,error_code=$7,
		retry_at=CASE WHEN $5='queued' THEN clock_timestamp()+($8::bigint*interval '1 microsecond') ELSE retry_at END,
		revision=revision+1,updated_at=clock_timestamp() WHERE tenant_id=$1 AND delivery_id=$2 AND owner=$3 AND fence=$4`, authority.TenantID, authority.DeliveryID, authority.Owner, authority.Fence, state, statusValue, nullableText(code), delay.Microseconds())
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func validateDeliveryAuthority(authority DeliveryAuthority) error {
	if uuid.Validate(authority.InstallationID) != nil || authority.StorageGeneration <= 0 || authority.TenantID <= 0 || uuid.Validate(authority.DeliveryID) != nil || authority.Owner == "" || authority.Fence <= 0 {
		return errors.New("invalid delivery authority")
	}
	return nil
}
