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
	Operation   string              `json:"operation"`
	Conversion  *ConversionRequest  `json:"conversion,omitempty"`
	Query       *QueryRequest       `json:"query,omitempty"`
	QueryExport *QueryExportRequest `json:"query_export,omitempty"`
}

type ChildResponse struct {
	DuckDBVersion string `json:"duckdb_version"`
}

func RunChild(ctx context.Context, input io.Reader, output io.Writer) error {
	decoder := json.NewDecoder(io.LimitReader(input, (1<<20)+1))
	decoder.DisallowUnknownFields()
	var request ChildRequest
	if err := decoder.Decode(&request); err != nil {
		return fmt.Errorf("decode child request: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("child request contains trailing JSON")
	}
	switch request.Operation {
	case "probe":
		if request.Conversion != nil || request.Query != nil || request.QueryExport != nil {
			return errors.New("probe request cannot contain conversion input")
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
	case "convert":
		if request.Conversion == nil || request.Query != nil || request.QueryExport != nil {
			return errors.New("convert request requires conversion input")
		}
		encoder := json.NewEncoder(output)
		summary, err := Convert(ctx, *request.Conversion, func(bundle ConvertedBundle) error {
			return encoder.Encode(ConversionMessage{Version: ConversionProtocolVersion, Type: "bundle", Bundle: &bundle})
		})
		if err != nil {
			return err
		}
		return encoder.Encode(ConversionMessage{Version: ConversionProtocolVersion, Type: "summary", Summary: &summary})
	case "query":
		if request.Query == nil || request.Conversion != nil || request.QueryExport != nil {
			return errors.New("query operation requires only query input")
		}
		summary, err := ExecuteQuery(ctx, *request.Query)
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(QueryMessage{Version: QueryExecutionProtocolVersion, Type: "summary", Summary: summary})
	case "query_export":
		if request.QueryExport == nil || request.Query != nil || request.Conversion != nil {
			return errors.New("query export requires only export input")
		}
		summary, err := ExportQueryResult(ctx, *request.QueryExport)
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(QueryExportMessage{Version: QueryExecutionProtocolVersion, Type: "summary", Summary: summary})
	default:
		return fmt.Errorf("unsupported internal operation %q", request.Operation)
	}
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
