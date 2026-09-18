package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	_ "github.com/duckdb/duckdb-go/v2"
)

// ChildRequest is deliberately narrow. Public query input is never accepted by
// engine-child; a supervisor supplies already validated internal operations.
type ChildRequest struct {
	Operation string `json:"operation"`
}

type ChildResponse struct {
	DuckDBVersion string `json:"duckdb_version"`
}

func RunChild(ctx context.Context, input io.Reader, output io.Writer) error {
	decoder := json.NewDecoder(io.LimitReader(input, 64<<10))
	decoder.DisallowUnknownFields()
	var request ChildRequest
	if err := decoder.Decode(&request); err != nil {
		return fmt.Errorf("decode child request: %w", err)
	}
	if request.Operation != "probe" {
		return fmt.Errorf("unsupported internal operation %q", request.Operation)
	}

	db, err := Open(ctx, "")
	if err != nil {
		return err
	}
	defer db.Close()
	var response ChildResponse
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&response.DuckDBVersion); err != nil {
		return fmt.Errorf("query DuckDB version: %w", err)
	}
	return json.NewEncoder(output).Encode(response)
}

func Open(ctx context.Context, path string) (*sql.DB, error) {
	if !customDuckDB2Linked {
		return nil, errors.New("DuckDB must be linked from the pinned 2.0 static bundle; the duckdb-go bundled engine is forbidden")
	}
	dsn := path
	if dsn == "" {
		dsn = ":memory:"
	}
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, fmt.Errorf("open DuckDB: %w", err)
	}
	db.SetMaxOpenConns(1)
	for _, statement := range []string{
		"SET threads = 1",
		"SET autoinstall_known_extensions = false",
		"SET autoload_known_extensions = false",
		"LOAD parquet",
		"LOAD json",
		"LOAD httpfs",
		"SET auto_fallback_to_full_download = false",
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			db.Close()
			return nil, fmt.Errorf("configure DuckDB with %q: %w", statement, err)
		}
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping DuckDB: %w", err)
	}
	return db, nil
}

func IsCanceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
