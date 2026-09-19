package control

import (
	"context"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"
)

// IngestOperations binds the explicit ingest transactions to one PostgreSQL
// pool without exposing the driver outside the control package.
type IngestOperations struct {
	pool                *pgxpool.Pool
	intentRegistrations atomic.Uint64
	intentUploadedMarks atomic.Uint64
	acceptCalls         atomic.Uint64
	acceptTransactions  atomic.Uint64
}

type IngestOperationCounts struct {
	IntentRegistrations uint64
	IntentUploadedMarks uint64
	AcceptCalls         uint64
	AcceptTransactions  uint64
	PGWriteTransactions uint64
}

func (operations *IngestOperations) OperationCounts() IngestOperationCounts {
	registrations := operations.intentRegistrations.Load()
	marks := operations.intentUploadedMarks.Load()
	acceptTransactions := operations.acceptTransactions.Load()
	return IngestOperationCounts{
		IntentRegistrations: registrations, IntentUploadedMarks: marks,
		AcceptCalls: operations.acceptCalls.Load(), AcceptTransactions: acceptTransactions,
		PGWriteTransactions: registrations + marks + acceptTransactions,
	}
}

func NewIngestOperations(pool *pgxpool.Pool) (*IngestOperations, error) {
	if pool == nil {
		return nil, ErrAuthorizationStale
	}
	return &IngestOperations{pool: pool}, nil
}

func (operations *IngestOperations) RegisterJournalIntent(ctx context.Context, registration JournalIntentRegistration) error {
	operations.intentRegistrations.Add(1)
	return RegisterJournalIntent(ctx, operations.pool, registration)
}

func (operations *IngestOperations) MarkJournalIntentUploaded(ctx context.Context, installationID string, generation, tenantID int64, authority IntentAuthority) error {
	operations.intentUploadedMarks.Add(1)
	return MarkJournalIntentUploaded(ctx, operations.pool, installationID, generation, tenantID, authority)
}

func (operations *IngestOperations) Accept(ctx context.Context, batch VerifiedBatch) ([]ReceiptResult, error) {
	operations.acceptCalls.Add(1)
	return accept(ctx, operations.pool, batch, func() { operations.acceptTransactions.Add(1) })
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
