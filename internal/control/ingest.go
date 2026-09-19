package control

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// IngestOperations binds the explicit ingest transactions to one PostgreSQL
// pool without exposing the driver outside the control package.
type IngestOperations struct {
	pool *pgxpool.Pool
}

func NewIngestOperations(pool *pgxpool.Pool) (*IngestOperations, error) {
	if pool == nil {
		return nil, ErrAuthorizationStale
	}
	return &IngestOperations{pool: pool}, nil
}

func (operations *IngestOperations) RegisterJournalIntent(ctx context.Context, registration JournalIntentRegistration) error {
	return RegisterJournalIntent(ctx, operations.pool, registration)
}

func (operations *IngestOperations) MarkJournalIntentUploaded(ctx context.Context, installationID string, generation, tenantID int64, authority IntentAuthority) error {
	return MarkJournalIntentUploaded(ctx, operations.pool, installationID, generation, tenantID, authority)
}

func (operations *IngestOperations) Accept(ctx context.Context, batch VerifiedBatch) ([]ReceiptResult, error) {
	return Accept(ctx, operations.pool, batch)
}

func (operations *IngestOperations) LoadProjectAuthorization(ctx context.Context, tenantID, projectID int64, keyHash [32]byte) (ProjectAuthorization, error) {
	return LoadProjectAuthorization(ctx, operations.pool, tenantID, projectID, keyHash)
}

func (operations *IngestOperations) LoadProjectOrigins(ctx context.Context, tenantID, projectID int64) ([]string, error) {
	return LoadProjectOrigins(ctx, operations.pool, tenantID, projectID)
}

func (operations *IngestOperations) LoadProjectTenant(ctx context.Context, projectID int64) (int64, error) {
	return LoadProjectTenant(ctx, operations.pool, projectID)
}
