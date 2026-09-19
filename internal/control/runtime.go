package control

import (
	"context"
	"errors"
	"fmt"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const EmptyGlobalScrubPolicySHA = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" // pragma: allowlist secret

type RuntimeInstallation struct {
	InstallationID            string
	StorageGeneration         int64
	StorageIdentity           string
	SchemaVersion             int
	TopologyVersion           int
	LaneCount                 int
	GlobalScrubPolicySHA      string
	GlobalScrubPolicyRevision int64
}

// RuntimeDatabase owns the PostgreSQL driver pool. App assembly configures its
// bound but receives only named control operations, never driver primitives.
type RuntimeDatabase struct {
	pool *pgxpool.Pool
}

func OpenRuntimeDatabase(ctx context.Context, databaseURL string, maxConnections int32) (*RuntimeDatabase, error) {
	if databaseURL == "" || maxConnections <= 0 {
		return nil, errors.New("database URL and positive pool bound are required")
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	config.MaxConns = maxConnections
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	return &RuntimeDatabase{pool: pool}, nil
}

func (database *RuntimeDatabase) Ping(ctx context.Context) error { return database.pool.Ping(ctx) }
func (database *RuntimeDatabase) Close()                         { database.pool.Close() }
func (database *RuntimeDatabase) VerifySchema(ctx context.Context) error {
	return VerifyRuntimeSchema(ctx, database.pool)
}
func (database *RuntimeDatabase) LoadInstallation(ctx context.Context) (RuntimeInstallation, error) {
	return LoadRuntimeInstallation(ctx, database.pool)
}
func (database *RuntimeDatabase) IngestOperations() (*IngestOperations, error) {
	return NewIngestOperations(database.pool)
}

// VerifyRuntimeSchema is read-only. Runtime never races migrations into a live
// deployment: its ledger must exactly match this binary's embedded manifest.
func VerifyRuntimeSchema(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return errors.New("PostgreSQL pool is required")
	}
	manifest, err := MigrationManifest()
	if err != nil {
		return err
	}
	rows, err := pool.Query(ctx, "SELECT version,sha256 FROM schema_migrations ORDER BY version")
	if err != nil {
		return fmt.Errorf("read runtime migration ledger: %w", err)
	}
	defer rows.Close()
	applied := make(map[int]string, len(manifest))
	for rows.Next() {
		var version int
		var checksum string
		if err := rows.Scan(&version, &checksum); err != nil {
			return err
		}
		applied[version] = checksum
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := validateMigrationLedger(manifest, applied); err != nil {
		return err
	}
	if len(applied) != len(manifest) {
		return fmt.Errorf("runtime schema is incomplete: database=%d binary=%d", len(applied), len(manifest))
	}
	return nil
}

func LoadRuntimeInstallation(ctx context.Context, pool *pgxpool.Pool) (RuntimeInstallation, error) {
	if pool == nil {
		return RuntimeInstallation{}, errors.New("PostgreSQL pool is required")
	}
	var installation RuntimeInstallation
	err := pool.QueryRow(ctx, `SELECT installation_id::text,storage_generation,storage_identity,schema_version,
		topology_version,lane_count,global_scrub_policy_sha,global_scrub_policy_revision
		FROM installations WHERE singleton`).Scan(
		&installation.InstallationID, &installation.StorageGeneration, &installation.StorageIdentity,
		&installation.SchemaVersion, &installation.TopologyVersion, &installation.LaneCount,
		&installation.GlobalScrubPolicySHA, &installation.GlobalScrubPolicyRevision)
	if errors.Is(err, pgx.ErrNoRows) {
		return RuntimeInstallation{}, errors.New("installation is not initialized")
	}
	if err != nil {
		return RuntimeInstallation{}, err
	}
	if installation.StorageGeneration <= 0 || installation.SchemaVersion != model.SchemaVersion || installation.TopologyVersion != 1 || installation.LaneCount != model.LaneCount || installation.GlobalScrubPolicyRevision <= 0 {
		return RuntimeInstallation{}, errors.New("installation format is not supported by this binary")
	}
	return installation, nil
}

func LoadProjectTenant(ctx context.Context, pool *pgxpool.Pool, projectID int64) (int64, error) {
	if pool == nil || projectID <= 0 {
		return 0, ErrProjectDisabled
	}
	var tenantID int64
	var tenantState, projectState string
	err := pool.QueryRow(ctx, `SELECT p.tenant_id,t.state,p.state FROM projects p
		JOIN tenants t ON t.tenant_id=p.tenant_id WHERE p.project_id=$1`, projectID).
		Scan(&tenantID, &tenantState, &projectState)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrProjectDisabled
	}
	if err != nil {
		return 0, err
	}
	if tenantState != "active" || projectState != "active" {
		return 0, ErrProjectDisabled
	}
	return tenantID, nil
}
