package control

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

const migrationLockID int64 = 0x4556474c415353 // "EVGLASS"

type Migration struct {
	Version int    `json:"version"`
	Name    string `json:"name"`
	SHA256  string `json:"sha256"`
	UpSQL   string `json:"-"`
	DownSQL string `json:"-"`
}

func MigrationManifest() ([]Migration, error) {
	entries, err := fs.Glob(migrationFiles, "migrations/*.sql")
	if err != nil {
		return nil, err
	}
	manifest := make([]Migration, 0, len(entries))
	for _, name := range entries {
		data, err := migrationFiles.ReadFile(name)
		if err != nil {
			return nil, err
		}
		base := strings.TrimSuffix(strings.TrimPrefix(name, "migrations/"), ".sql")
		parts := strings.SplitN(base, "_", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("migration %q must be named NNNN_name.sql", name)
		}
		version, err := strconv.Atoi(parts[0])
		if err != nil || version <= 0 {
			return nil, fmt.Errorf("migration %q has invalid version", name)
		}
		up, down, err := splitMigration(string(data))
		if err != nil {
			return nil, fmt.Errorf("migration %q: %w", name, err)
		}
		digest := sha256.Sum256(data)
		manifest = append(manifest, Migration{Version: version, Name: parts[1], SHA256: hex.EncodeToString(digest[:]), UpSQL: up, DownSQL: down})
	}
	sort.Slice(manifest, func(i, j int) bool { return manifest[i].Version < manifest[j].Version })
	if err := validateMigrationManifest(manifest); err != nil {
		return nil, err
	}
	return manifest, nil
}

func validateMigrationManifest(manifest []Migration) error {
	for index, migration := range manifest {
		expected := index + 1
		if migration.Version != expected {
			return fmt.Errorf("migration manifest must be contiguous: position %d has version %d", expected, migration.Version)
		}
	}
	return nil
}

func splitMigration(contents string) (string, string, error) {
	const upMarker = "-- +eventglass Up"
	const downMarker = "-- +eventglass Down"
	upIndex := strings.Index(contents, upMarker)
	downIndex := strings.Index(contents, downMarker)
	if upIndex < 0 || downIndex < 0 || downIndex <= upIndex {
		return "", "", errors.New("requires ordered Up and Down markers")
	}
	up := strings.TrimSpace(contents[upIndex+len(upMarker) : downIndex])
	down := strings.TrimSpace(contents[downIndex+len(downMarker):])
	if up == "" || down == "" {
		return "", "", errors.New("Up and Down sections must be non-empty")
	}
	return up, down, nil
}

func ApplyMigrations(ctx context.Context, databaseURL string) error {
	manifest, err := MigrationManifest()
	if err != nil {
		return err
	}
	connection, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("connect PostgreSQL: %w", err)
	}
	defer connection.Close(ctx)
	if _, err := connection.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockID); err != nil {
		return fmt.Errorf("lock migrations: %w", err)
	}
	defer connection.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", migrationLockID)

	if _, err := connection.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		sha256 TEXT NOT NULL CHECK (length(sha256) = 64),
		applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
	)`); err != nil {
		return fmt.Errorf("create migration ledger: %w", err)
	}
	rows, err := connection.Query(ctx, "SELECT version, sha256 FROM schema_migrations ORDER BY version")
	if err != nil {
		return fmt.Errorf("read migration ledger: %w", err)
	}
	applied := make(map[int]string)
	for rows.Next() {
		var version int
		var checksum string
		if err := rows.Scan(&version, &checksum); err != nil {
			rows.Close()
			return err
		}
		applied[version] = checksum
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	if err := validateMigrationLedger(manifest, applied); err != nil {
		return err
	}

	for _, migration := range manifest {
		if checksum, exists := applied[migration.Version]; exists {
			if checksum != migration.SHA256 {
				return fmt.Errorf("migration %d checksum mismatch: database=%s binary=%s", migration.Version, checksum, migration.SHA256)
			}
			continue
		}
		tx, err := connection.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, migration.UpSQL); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("apply migration %d: %w", migration.Version, err)
		}
		if _, err := tx.Exec(ctx, "INSERT INTO schema_migrations(version, name, sha256) VALUES($1,$2,$3)", migration.Version, migration.Name, migration.SHA256); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("record migration %d: %w", migration.Version, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %d: %w", migration.Version, err)
		}
	}
	return nil
}

// Only an exact prefix of this binary's manifest is a migratable database.
func validateMigrationLedger(manifest []Migration, applied map[int]string) error {
	known := make(map[int]string, len(manifest))
	missing := false
	for _, migration := range manifest {
		known[migration.Version] = migration.SHA256
		checksum, exists := applied[migration.Version]
		if !exists {
			missing = true
			continue
		}
		if missing {
			return fmt.Errorf("migration ledger has a gap before %d", migration.Version)
		}
		if checksum != migration.SHA256 {
			return fmt.Errorf("migration %d checksum mismatch", migration.Version)
		}
	}
	for version := range applied {
		if _, exists := known[version]; !exists {
			return fmt.Errorf("unsupported migration version %d", version)
		}
	}
	return nil
}
