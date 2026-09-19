package control

import (
	"context"
	"errors"
	"time"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	SnapshotTTL             = 15 * time.Minute
	SnapshotMaximumLifetime = time.Hour
	SnapshotTickMaximumAge  = 120 * time.Second
	MaxUserSnapshots        = 4
	MaxTenantSnapshots      = 32
	SnapshotRetryLimit      = 3
)

var (
	ErrSnapshotLimit       = errors.New("snapshot admission limit exceeded")
	ErrSnapshotExpired     = errors.New("snapshot expired")
	ErrSnapshotMismatch    = errors.New("snapshot scope does not match")
	ErrRetentionClockStale = errors.New("retention clock is stale")
	ErrStorageGeneration   = errors.New("storage generation changed")
)

type QueryOperations struct{ pool *pgxpool.Pool }

type CreateSnapshotCommand struct {
	SnapshotID       string
	SessionTokenHash [32]byte
	TenantID         int64
	ProjectIDs       []int64
	DatasetSHA256    string
	DatasetBytes     []byte
}

type queryAuthority struct {
	userID             int64
	authRevision       int64
	tenantAuthRevision int64
	role               string
	generation         int64
	retentionFloorUS   int64
	now                time.Time
	principalHash      string
}

func NewQueryOperations(pool *pgxpool.Pool) (*QueryOperations, error) {
	if pool == nil {
		return nil, errors.New("PostgreSQL pool is required")
	}
	return &QueryOperations{pool: pool}, nil
}

func (operations *QueryOperations) AdvanceRetentionFloor(ctx context.Context) (int64, time.Time, error) {
	var floor int64
	var tick time.Time
	err := operations.pool.QueryRow(ctx, `UPDATE installations SET
		retention_floor_us=GREATEST(retention_floor_us,floor(extract(epoch FROM (clock_timestamp()-retention_days*interval '1 day'))*1000000)::bigint),
		retention_tick_at=clock_timestamp()
		WHERE singleton AND setup_state='ready' AND recovery_state='ready'
		RETURNING retention_floor_us,retention_tick_at`).Scan(&floor, &tick)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, time.Time{}, ErrStorageGeneration
	}
	return floor, tick, err
}

func (operations *QueryOperations) CreateSnapshot(ctx context.Context, command CreateSnapshotCommand) (model.QuerySnapshot, error) {
	if err := validateCreateSnapshot(command); err != nil {
		return model.QuerySnapshot{}, err
	}
	var lastErr error
	for attempt := 0; attempt <= SnapshotRetryLimit; attempt++ {
		result, err := operations.createSnapshotOnce(ctx, command)
		if err == nil || !retryableSnapshotError(err) {
			return result, err
		}
		lastErr = err
		if err := waitSnapshotRetry(ctx, attempt); err != nil {
			return model.QuerySnapshot{}, err
		}
	}
	return model.QuerySnapshot{}, lastErr
}

func (operations *QueryOperations) createSnapshotOnce(ctx context.Context, command CreateSnapshotCommand) (model.QuerySnapshot, error) {
	connection, err := operations.pool.Acquire(ctx)
	if err != nil {
		return model.QuerySnapshot{}, err
	}
	admissionKey := -command.TenantID
	if _, err := connection.Exec(ctx, `SELECT pg_advisory_lock($1::bigint)`, admissionKey); err != nil {
		connection.Release()
		return model.QuerySnapshot{}, err
	}
	defer releaseTenantAdmission(connection, admissionKey)

	tx, err := connection.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return model.QuerySnapshot{}, err
	}
	defer tx.Rollback(ctx)
	authority, err := lockQueryAuthority(ctx, tx, command.SessionTokenHash, command.TenantID, true, true)
	if err != nil {
		return model.QuerySnapshot{}, err
	}
	projectRevisions, err := authorizeSnapshotProjects(ctx, tx, authority, command.TenantID, command.ProjectIDs, nil)
	if err != nil {
		return model.QuerySnapshot{}, err
	}
	var userCount, tenantCount int
	if err := tx.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE user_id=$2),count(*)
		FROM query_snapshots WHERE tenant_id=$1 AND principal_kind='user' AND state='active' AND expires_at>clock_timestamp()`,
		command.TenantID, authority.userID).Scan(&userCount, &tenantCount); err != nil {
		return model.QuerySnapshot{}, err
	}
	if userCount >= MaxUserSnapshots || tenantCount >= MaxTenantSnapshots {
		return model.QuerySnapshot{}, ErrSnapshotLimit
	}
	lanes, err := lockSnapshotLanes(ctx, tx, command.TenantID)
	if err != nil {
		return model.QuerySnapshot{}, err
	}
	expiresAt := authority.now.Add(SnapshotTTL)
	maxUntil := authority.now.Add(SnapshotMaximumLifetime)
	_, err = tx.Exec(ctx, `INSERT INTO query_snapshots(snapshot_id,tenant_id,user_id,principal_kind,principal_ref,auth_revision,
		tenant_auth_revision,storage_generation,dataset_hash,dataset_bytes,retention_floor_us,created_at,expires_at,max_until)
		VALUES($1,$2,$3,'user',$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		command.SnapshotID, command.TenantID, authority.userID, authority.principalHash, authority.authRevision,
		authority.tenantAuthRevision, authority.generation, command.DatasetSHA256, command.DatasetBytes,
		authority.retentionFloorUS, authority.now, expiresAt, maxUntil)
	if err != nil {
		return model.QuerySnapshot{}, err
	}
	for index, projectID := range command.ProjectIDs {
		if _, err := tx.Exec(ctx, `INSERT INTO snapshot_projects(snapshot_id,tenant_id,project_id,project_auth_revision) VALUES($1,$2,$3,$4)`,
			command.SnapshotID, command.TenantID, projectID, projectRevisions[index]); err != nil {
			return model.QuerySnapshot{}, err
		}
	}
	for _, lane := range lanes {
		if _, err := tx.Exec(ctx, `INSERT INTO snapshot_lanes(snapshot_id,tenant_id,lane_id,cut_seq,catalog_generation) VALUES($1,$2,$3,$4,$5)`,
			command.SnapshotID, command.TenantID, lane.LaneID, lane.CutSeq, lane.CatalogGeneration); err != nil {
			return model.QuerySnapshot{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return model.QuerySnapshot{}, err
	}
	return snapshotResult(command, authority, projectRevisions, lanes, expiresAt, maxUntil), nil
}

func releaseTenantAdmission(connection *pgxpool.Conn, key int64) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var unlocked bool
	if err := connection.QueryRow(ctx, `SELECT pg_advisory_unlock($1::bigint)`, key).Scan(&unlocked); err != nil || !unlocked {
		// Never return a session carrying a lock to the pool. Closing a hijacked
		// connection lets PostgreSQL release the session-level lock atomically.
		_ = connection.Hijack().Close(context.Background())
		return
	}
	connection.Release()
}

func (operations *QueryOperations) RenewSnapshot(ctx context.Context, tokenHash [32]byte, tenantID int64, snapshotID, datasetHash string) (model.QuerySnapshot, error) {
	if tenantID <= 0 || uuid.Validate(snapshotID) != nil || !validSHA(datasetHash) {
		return model.QuerySnapshot{}, ErrSnapshotMismatch
	}
	var lastErr error
	for attempt := 0; attempt <= SnapshotRetryLimit; attempt++ {
		result, err := operations.renewSnapshotOnce(ctx, tokenHash, tenantID, snapshotID, datasetHash)
		if err == nil || !retryableSnapshotError(err) {
			return result, err
		}
		lastErr = err
		if err := waitSnapshotRetry(ctx, attempt); err != nil {
			return model.QuerySnapshot{}, err
		}
	}
	return model.QuerySnapshot{}, lastErr
}

func (operations *QueryOperations) renewSnapshotOnce(ctx context.Context, tokenHash [32]byte, tenantID int64, snapshotID, datasetHash string) (model.QuerySnapshot, error) {
	tx, err := operations.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return model.QuerySnapshot{}, err
	}
	defer tx.Rollback(ctx)
	authority, err := lockQueryAuthority(ctx, tx, tokenHash, tenantID, false, false)
	if err != nil {
		return model.QuerySnapshot{}, err
	}
	snapshot, projectRevisions, err := loadSnapshotForUpdate(ctx, tx, tenantID, snapshotID)
	if err != nil {
		return model.QuerySnapshot{}, err
	}
	if snapshot.UserID != authority.userID || snapshot.PrincipalHash != authority.principalHash || snapshot.DatasetSHA256 != datasetHash {
		return model.QuerySnapshot{}, ErrSnapshotMismatch
	}
	if snapshot.StorageGeneration != authority.generation {
		return model.QuerySnapshot{}, ErrStorageGeneration
	}
	if snapshot.AuthRevision != authority.authRevision || snapshot.TenantAuthRevision != authority.tenantAuthRevision {
		return model.QuerySnapshot{}, ErrForbidden
	}
	if _, err := authorizeSnapshotProjects(ctx, tx, authority, tenantID, snapshot.ProjectIDs, projectRevisions); err != nil {
		return model.QuerySnapshot{}, err
	}
	if snapshot.ExpiresAtUS <= authority.now.UnixMicro() || snapshot.MaxUntilUS <= authority.now.UnixMicro() {
		return model.QuerySnapshot{}, ErrSnapshotExpired
	}
	renewed := authority.now.Add(SnapshotTTL)
	if renewed.UnixMicro() > snapshot.MaxUntilUS {
		renewed = time.UnixMicro(snapshot.MaxUntilUS)
	}
	if _, err := tx.Exec(ctx, `UPDATE query_snapshots SET expires_at=$3 WHERE tenant_id=$1 AND snapshot_id=$2 AND state='active'`, tenantID, snapshotID, renewed); err != nil {
		return model.QuerySnapshot{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return model.QuerySnapshot{}, err
	}
	snapshot.ExpiresAtUS = renewed.UnixMicro()
	return snapshot, nil
}

func (operations *QueryOperations) ReleaseSnapshot(ctx context.Context, tokenHash [32]byte, tenantID int64, snapshotID string) error {
	if tenantID <= 0 || uuid.Validate(snapshotID) != nil {
		return ErrSnapshotMismatch
	}
	tx, err := operations.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	authority, err := lockQueryAuthority(ctx, tx, tokenHash, tenantID, false, false)
	if err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE query_snapshots SET state='released',expires_at=LEAST(expires_at,clock_timestamp())
		WHERE tenant_id=$1 AND snapshot_id=$2 AND user_id=$3 AND principal_ref=$4`, tenantID, snapshotID, authority.userID, authority.principalHash)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return ErrSnapshotMismatch
	}
	return tx.Commit(ctx)
}

func lockQueryAuthority(ctx context.Context, tx pgx.Tx, tokenHash [32]byte, tenantID int64, requireFreshTick, admission bool) (queryAuthority, error) {
	var result queryAuthority
	var setupState, recoveryState string
	var tick time.Time
	if err := tx.QueryRow(ctx, `SELECT storage_generation,retention_floor_us,retention_tick_at,clock_timestamp(),setup_state,recovery_state
		FROM installations WHERE singleton FOR SHARE`).Scan(&result.generation, &result.retentionFloorUS, &tick, &result.now, &setupState, &recoveryState); err != nil {
		return result, err
	}
	if setupState != "ready" || recoveryState != "ready" {
		return result, ErrUnauthenticated
	}
	if requireFreshTick && result.now.Sub(tick) > SnapshotTickMaximumAge {
		return result, ErrRetentionClockStale
	}
	var tenantState string
	tenantStatement := `SELECT state,auth_revision FROM tenants WHERE tenant_id=$1 FOR SHARE`
	if admission {
		// The no-op row version is an admission epoch. A concurrent REPEATABLE
		// READ allocator must retry after waiting, so its cap count cannot come
		// from a snapshot taken before the preceding insert committed.
		tenantStatement = `UPDATE tenants SET state=state WHERE tenant_id=$1 RETURNING state,auth_revision`
	}
	if err := tx.QueryRow(ctx, tenantStatement, tenantID).Scan(&tenantState, &result.tenantAuthRevision); err != nil || tenantState != "active" {
		return result, errors.Join(ErrForbidden, err)
	}
	var userState string
	userStatement := `SELECT u.user_id,u.auth_revision,u.state,m.role
		FROM sessions s JOIN users u ON u.user_id=s.user_id JOIN memberships m ON m.user_id=u.user_id AND m.tenant_id=$2
		WHERE s.token_hash=$1 AND s.revoked_at IS NULL AND s.expires_at>clock_timestamp()
		AND s.credential_revision=u.credential_revision AND s.storage_generation=$3
		FOR SHARE OF s,m,u`
	if admission {
		userStatement = `SELECT u.user_id,u.auth_revision,u.state,m.role
			FROM sessions s JOIN users u ON u.user_id=s.user_id JOIN memberships m ON m.user_id=u.user_id AND m.tenant_id=$2
			WHERE s.token_hash=$1 AND s.revoked_at IS NULL AND s.expires_at>clock_timestamp()
			AND s.credential_revision=u.credential_revision AND s.storage_generation=$3
			FOR UPDATE OF u FOR SHARE OF s,m`
	}
	err := tx.QueryRow(ctx, userStatement, tokenHash[:], tenantID, result.generation).Scan(&result.userID, &result.authRevision, &userState, &result.role)
	if err != nil || userState != "active" {
		return result, errors.Join(ErrUnauthenticated, err)
	}
	result.principalHash = model.QueryPrincipalHash(result.userID, tokenHash)
	return result, nil
}

func authorizeSnapshotProjects(ctx context.Context, tx pgx.Tx, authority queryAuthority, tenantID int64, projectIDs, expected []int64) ([]int64, error) {
	rows, err := tx.Query(ctx, `SELECT project_id,state,auth_revision FROM projects
		WHERE tenant_id=$1 AND project_id=ANY($2::bigint[]) ORDER BY project_id FOR SHARE`, tenantID, projectIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	revisions := make([]int64, 0, len(projectIDs))
	index := 0
	for rows.Next() {
		var projectID, revision int64
		var state string
		if err := rows.Scan(&projectID, &state, &revision); err != nil {
			return nil, err
		}
		if index >= len(projectIDs) || projectID != projectIDs[index] || state != "active" {
			return nil, ErrForbidden
		}
		if expected != nil && (index >= len(expected) || revision != expected[index]) {
			return nil, ErrForbidden
		}
		revisions = append(revisions, revision)
		index++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	if index != len(projectIDs) || expected != nil && len(expected) != index {
		return nil, ErrForbidden
	}
	if authority.role != "admin" {
		grantRows, err := tx.Query(ctx, `SELECT project_id FROM project_grants
			WHERE tenant_id=$1 AND user_id=$2 AND project_id=ANY($3::bigint[]) ORDER BY project_id FOR SHARE`, tenantID, authority.userID, projectIDs)
		if err != nil {
			return nil, err
		}
		granted := 0
		for grantRows.Next() {
			var projectID int64
			if err := grantRows.Scan(&projectID); err != nil || granted >= len(projectIDs) || projectID != projectIDs[granted] {
				grantRows.Close()
				return nil, ErrForbidden
			}
			granted++
		}
		if err := grantRows.Err(); err != nil {
			grantRows.Close()
			return nil, err
		}
		grantRows.Close()
		if granted != len(projectIDs) {
			return nil, ErrForbidden
		}
	}
	return revisions, nil
}

func lockSnapshotLanes(ctx context.Context, tx pgx.Tx, tenantID int64) ([model.LaneCount]model.SnapshotLane, error) {
	var result [model.LaneCount]model.SnapshotLane
	rows, err := tx.Query(ctx, `SELECT lane_id,published_seq,catalog_generation FROM lanes WHERE tenant_id=$1 ORDER BY lane_id FOR SHARE`, tenantID)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	index := 0
	for rows.Next() {
		if index >= model.LaneCount {
			return result, errors.New("tenant lane topology is invalid")
		}
		if err := rows.Scan(&result[index].LaneID, &result[index].CutSeq, &result[index].CatalogGeneration); err != nil {
			return result, err
		}
		if result[index].LaneID != index {
			return result, errors.New("tenant lane topology is invalid")
		}
		index++
	}
	if err := rows.Err(); err != nil {
		return result, err
	}
	if index != model.LaneCount {
		return result, errors.New("tenant lane topology is incomplete")
	}
	return result, nil
}

func loadSnapshotForUpdate(ctx context.Context, tx pgx.Tx, tenantID int64, snapshotID string) (model.QuerySnapshot, []int64, error) {
	var result model.QuerySnapshot
	var expires, maxUntil time.Time
	err := tx.QueryRow(ctx, `SELECT snapshot_id::text,tenant_id,user_id,principal_ref,auth_revision,tenant_auth_revision,
		storage_generation,dataset_hash,dataset_bytes,retention_floor_us,expires_at,max_until
		FROM query_snapshots WHERE tenant_id=$1 AND snapshot_id=$2 AND principal_kind='user' AND state='active' FOR UPDATE`, tenantID, snapshotID).Scan(
		&result.SnapshotID, &result.TenantID, &result.UserID, &result.PrincipalHash, &result.AuthRevision, &result.TenantAuthRevision,
		&result.StorageGeneration, &result.DatasetSHA256, &result.DatasetBytes, &result.RetentionFloorUS, &expires, &maxUntil)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, nil, ErrSnapshotExpired
	}
	if err != nil {
		return result, nil, err
	}
	result.ExpiresAtUS, result.MaxUntilUS = expires.UnixMicro(), maxUntil.UnixMicro()
	rows, err := tx.Query(ctx, `SELECT project_id,project_auth_revision FROM snapshot_projects WHERE tenant_id=$1 AND snapshot_id=$2 ORDER BY project_id`, tenantID, snapshotID)
	if err != nil {
		return result, nil, err
	}
	var revisions []int64
	for rows.Next() {
		var projectID, revision int64
		if err := rows.Scan(&projectID, &revision); err != nil {
			rows.Close()
			return result, nil, err
		}
		result.ProjectIDs = append(result.ProjectIDs, projectID)
		revisions = append(revisions, revision)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return result, nil, err
	}
	rows.Close()
	result.ProjectRevisions = append([]int64(nil), revisions...)
	laneRows, err := tx.Query(ctx, `SELECT lane_id,cut_seq,catalog_generation FROM snapshot_lanes WHERE tenant_id=$1 AND snapshot_id=$2 ORDER BY lane_id`, tenantID, snapshotID)
	if err != nil {
		return result, nil, err
	}
	index := 0
	for laneRows.Next() {
		if index >= model.LaneCount {
			laneRows.Close()
			return result, nil, errors.New("snapshot lane vector is invalid")
		}
		if err := laneRows.Scan(&result.Lanes[index].LaneID, &result.Lanes[index].CutSeq, &result.Lanes[index].CatalogGeneration); err != nil {
			laneRows.Close()
			return result, nil, err
		}
		if result.Lanes[index].LaneID != index {
			laneRows.Close()
			return result, nil, errors.New("snapshot lane vector is invalid")
		}
		index++
	}
	if err := laneRows.Err(); err != nil {
		laneRows.Close()
		return result, nil, err
	}
	laneRows.Close()
	if index != model.LaneCount {
		return result, nil, errors.New("snapshot lane vector is incomplete")
	}
	return result, revisions, nil
}

func snapshotResult(command CreateSnapshotCommand, authority queryAuthority, revisions []int64, lanes [model.LaneCount]model.SnapshotLane, expiresAt, maxUntil time.Time) model.QuerySnapshot {
	return model.QuerySnapshot{
		SnapshotID: command.SnapshotID, TenantID: command.TenantID, UserID: authority.userID, PrincipalHash: authority.principalHash,
		AuthRevision: authority.authRevision, TenantAuthRevision: authority.tenantAuthRevision, StorageGeneration: authority.generation,
		DatasetSHA256: command.DatasetSHA256, DatasetBytes: append([]byte(nil), command.DatasetBytes...), RetentionFloorUS: authority.retentionFloorUS,
		ProjectIDs: append([]int64(nil), command.ProjectIDs...), ProjectRevisions: append([]int64(nil), revisions...), Lanes: lanes,
		ExpiresAtUS: expiresAt.UnixMicro(), MaxUntilUS: maxUntil.UnixMicro(),
	}
}
