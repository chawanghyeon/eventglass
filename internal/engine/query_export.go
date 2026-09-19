package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/chawanghyeon/eventglass/internal/storage"
)

const MaxPublicQueryResultBytes = int64(8 << 20)

type QueryExportRequest struct {
	Version    int    `json:"version"`
	InputPath  string `json:"input_path"`
	OutputPath string `json:"output_path"`
}

type QueryExportSummary struct {
	DuckDBVersion string               `json:"duckdb_version"`
	Rows          int64                `json:"rows"`
	Evidence      storage.FileEvidence `json:"evidence"`
}

type QueryExportMessage struct {
	Version int                `json:"version"`
	Type    string             `json:"type"`
	Summary QueryExportSummary `json:"summary"`
}

func ExportQueryResult(ctx context.Context, request QueryExportRequest) (summary QueryExportSummary, retErr error) {
	if request.Version != QueryExecutionProtocolVersion || !filepath.IsAbs(request.InputPath) || !filepath.IsAbs(request.OutputPath) || request.InputPath == request.OutputPath {
		return summary, ErrQueryExecutionInvalid
	}
	input, err := os.Stat(request.InputPath)
	if err != nil || !input.Mode().IsRegular() || input.Size() <= 0 || input.Size() > MaxQueryOutputBytes {
		return summary, ErrQueryExecutionInvalid
	}
	if _, err := os.Stat(request.OutputPath); !errors.Is(err, os.ErrNotExist) {
		return summary, ErrQueryExecutionInvalid
	}
	defer func() {
		if retErr != nil {
			_ = os.Remove(request.OutputPath)
		}
	}()
	db, err := Open(ctx, "")
	if err != nil {
		return summary, err
	}
	defer db.Close()
	statement := `COPY (SELECT * FROM read_parquet('` + quoteSQLString(request.InputPath) + `')) TO '` + quoteSQLString(request.OutputPath) + `' (FORMAT JSON,ARRAY false)`
	if _, err := db.ExecContext(ctx, statement); err != nil {
		return summary, fmt.Errorf("export query result: %w", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM read_parquet(?)`, request.InputPath).Scan(&summary.Rows); err != nil {
		return summary, err
	}
	summary.Evidence, err = storage.InspectFile(request.OutputPath)
	if err != nil {
		return summary, err
	}
	if summary.Evidence.Bytes < 0 || summary.Evidence.Bytes > MaxPublicQueryResultBytes {
		return summary, ErrQueryExecutionLimit
	}
	if err := db.QueryRowContext(ctx, `SELECT version()`).Scan(&summary.DuckDBVersion); err != nil {
		return summary, err
	}
	return summary, nil
}
