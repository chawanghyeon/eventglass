package control

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type DeliveryPageCommand struct {
	TenantID, ProjectID, ActorUserID int64
	AlertID, State                   string
	Limit                            int
	BeforeCreatedAt                  *time.Time
	BeforeDeliveryID                 string
}

func (operations *AlertOperations) ListDeliveryPage(ctx context.Context, command DeliveryPageCommand) ([]Delivery, error) {
	if command.TenantID <= 0 || command.ProjectID <= 0 || command.ActorUserID <= 0 || command.Limit < 1 || command.Limit > 1000 || command.AlertID != "" && uuid.Validate(command.AlertID) != nil || command.State != "" && !validDeliveryState(command.State) || (command.BeforeCreatedAt == nil) != (command.BeforeDeliveryID == "") || command.BeforeDeliveryID != "" && uuid.Validate(command.BeforeDeliveryID) != nil {
		return nil, errors.New("invalid delivery page")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err := tenantAccess(ctx, tx, command.TenantID, command.ActorUserID, command.ProjectID, false); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `SELECT tenant_id,project_id,delivery_id::text,alert_id::text,alert_revision,revision,destination_id::text,destination_revision,state,attempt,retry_at,last_status,error_code,created_at,updated_at
		FROM deliveries WHERE tenant_id=$1 AND project_id=$2 AND ($3='' OR alert_id=NULLIF($3,'')::uuid) AND ($4='' OR state=$4)
		AND ($5::timestamptz IS NULL OR (created_at,delivery_id)<($5,NULLIF($6,'')::uuid)) ORDER BY created_at DESC,delivery_id DESC LIMIT $7`,
		command.TenantID, command.ProjectID, command.AlertID, command.State, command.BeforeCreatedAt, command.BeforeDeliveryID, command.Limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]Delivery, 0, command.Limit+1)
	for rows.Next() {
		var value Delivery
		if err := rows.Scan(&value.Authority.TenantID, &value.ProjectID, &value.Authority.DeliveryID, &value.AlertID, &value.AlertRevision, &value.Revision, &value.DestinationID, &value.DestinationRevision, &value.State, &value.Attempt, &value.RetryAt, &value.LastStatus, &value.ErrorCode, &value.CreatedAt, &value.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

type RetryDeliveryCommand struct {
	TenantID, ProjectID, ActorUserID, ExpectedRevision int64
	DeliveryID                                         string
}

func (operations *AlertOperations) RetryDelivery(ctx context.Context, command RetryDeliveryCommand) (Delivery, error) {
	if command.TenantID <= 0 || command.ProjectID <= 0 || command.ActorUserID <= 0 || command.ExpectedRevision <= 0 || uuid.Validate(command.DeliveryID) != nil {
		return Delivery{}, errors.New("invalid delivery retry")
	}
	tx, err := operations.pool.Begin(ctx)
	if err != nil {
		return Delivery{}, err
	}
	defer tx.Rollback(ctx)
	if err := requireTenantRole(ctx, tx, command.TenantID, command.ActorUserID, false, command.ProjectID, true); err != nil {
		return Delivery{}, err
	}
	var result Delivery
	var destinationEnabled bool
	err = tx.QueryRow(ctx, `SELECT d.tenant_id,d.project_id,d.delivery_id::text,d.alert_id::text,d.alert_revision,d.revision,d.destination_id::text,d.destination_revision,d.state,d.attempt,d.retry_at,d.last_status,d.error_code,d.created_at,d.updated_at,ad.enabled
		FROM deliveries d JOIN alert_destinations ad ON ad.tenant_id=d.tenant_id AND ad.destination_id=d.destination_id
		WHERE d.tenant_id=$1 AND d.project_id=$2 AND d.delivery_id=$3 FOR UPDATE OF d FOR SHARE OF ad`, command.TenantID, command.ProjectID, command.DeliveryID).Scan(&result.Authority.TenantID, &result.ProjectID, &result.Authority.DeliveryID, &result.AlertID, &result.AlertRevision, &result.Revision, &result.DestinationID, &result.DestinationRevision, &result.State, &result.Attempt, &result.RetryAt, &result.LastStatus, &result.ErrorCode, &result.CreatedAt, &result.UpdatedAt, &destinationEnabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return Delivery{}, ErrForbidden
	}
	if err != nil {
		return Delivery{}, err
	}
	if result.Revision != command.ExpectedRevision {
		return Delivery{}, ErrRevisionConflict
	}
	if result.State != "failed" || !destinationEnabled {
		return Delivery{}, ErrForbidden
	}
	err = tx.QueryRow(ctx, `UPDATE deliveries SET state='queued',attempt=0,retry_at=clock_timestamp(),last_status=NULL,error_code=NULL,owner=NULL,lease_until=NULL,revision=revision+1,updated_at=clock_timestamp()
		WHERE tenant_id=$1 AND delivery_id=$2 RETURNING revision,state,attempt,retry_at,last_status,error_code,updated_at`, command.TenantID, command.DeliveryID).Scan(&result.Revision, &result.State, &result.Attempt, &result.RetryAt, &result.LastStatus, &result.ErrorCode, &result.UpdatedAt)
	if err != nil {
		return Delivery{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Delivery{}, err
	}
	return result, nil
}

func validDeliveryState(state string) bool {
	return state == "queued" || state == "running" || state == "succeeded" || state == "failed" || state == "canceled"
}
