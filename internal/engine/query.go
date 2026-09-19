package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

const (
	MaxQueryInputFiles  = 8
	MaxQueryOutputBytes = int64(64 << 20)
	MaxQueryOutputRows  = int64(20000)
)

var (
	ErrQueryExecutionInvalid = errors.New("invalid query execution")
	ErrQueryExecutionLimit   = errors.New("query execution limit exceeded")
)

type QueryArgument struct {
	Type  string `json:"type"`
	Value string `json:"value,omitempty"`
}

type QueryOperation struct {
	Version         int             `json:"version"`
	Kind            string          `json:"kind"`
	ScanSQL         string          `json:"scan_sql"`
	ReduceSQL       string          `json:"reduce_sql"`
	EmptySQL        string          `json:"empty_sql"`
	ScanArguments   []QueryArgument `json:"scan_arguments"`
	ReduceArguments []QueryArgument `json:"reduce_arguments"`
	MaxRows         int64           `json:"max_rows"`
}

type QueryRequest struct {
	Version           int                `json:"version"`
	QueryID           string             `json:"query_id"`
	Task              model.QueryTaskKey `json:"task"`
	Operation         QueryOperation     `json:"operation"`
	InputPaths        []string           `json:"input_paths"`
	OutputPath        string             `json:"output_path"`
	SpillDirectory    string             `json:"spill_directory"`
	NativeMemoryBytes int64              `json:"native_memory_bytes"`
	NativeSpillBytes  int64              `json:"native_spill_bytes"`
}

type QuerySummary struct {
	DuckDBVersion string               `json:"duckdb_version"`
	Rows          int64                `json:"rows"`
	Evidence      storage.FileEvidence `json:"evidence"`
}

type QueryMessage struct {
	Version int          `json:"version"`
	Type    string       `json:"type"`
	Summary QuerySummary `json:"summary"`
}

func ExecuteQuery(ctx context.Context, request QueryRequest) (summary QuerySummary, retErr error) {
	request.applyDefaults()
	statement, arguments, err := request.validate()
	if err != nil {
		return summary, errors.Join(ErrQueryExecutionInvalid, err)
	}
	if err := prepareQueryPaths(request); err != nil {
		return summary, err
	}
	defer func() {
		_ = os.RemoveAll(request.SpillDirectory)
		if retErr != nil {
			_ = os.Remove(request.OutputPath)
		}
	}()
	db, err := Open(ctx, "")
	if err != nil {
		return summary, err
	}
	defer db.Close()
	for _, configuration := range []string{
		fmt.Sprintf("SET memory_limit = '%dB'", request.NativeMemoryBytes),
		fmt.Sprintf("SET max_temp_directory_size = '%dB'", request.NativeSpillBytes),
		"SET temp_directory = '" + quoteSQLString(request.SpillDirectory) + "'",
		"SET threads = 1",
		"SET preserve_insertion_order = false",
	} {
		if _, err := db.ExecContext(ctx, configuration); err != nil {
			return summary, err
		}
	}
	if len(request.InputPaths) > 0 {
		paths := make([]string, len(request.InputPaths))
		for index, path := range request.InputPaths {
			paths[index] = "'" + quoteSQLString(path) + "'"
		}
		if _, err := db.ExecContext(ctx, `CREATE TEMP VIEW input_rows AS SELECT * FROM read_parquet([`+strings.Join(paths, ",")+`],union_by_name=false)`); err != nil {
			return summary, fmt.Errorf("open query inputs: %w", err)
		}
	}
	copySQL := `COPY (` + statement + `) TO '` + quoteSQLString(request.OutputPath) + `' (FORMAT PARQUET,COMPRESSION ZSTD,COMPRESSION_LEVEL 3,ROW_GROUP_SIZE 16384)`
	if _, err := db.ExecContext(ctx, copySQL, arguments...); err != nil {
		return summary, fmt.Errorf("execute query task: %w", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM read_parquet(?)`, request.OutputPath).Scan(&summary.Rows); err != nil {
		return summary, err
	}
	if summary.Rows < 0 || summary.Rows > request.Operation.MaxRows {
		return summary, ErrQueryExecutionLimit
	}
	summary.Evidence, err = storage.InspectFile(request.OutputPath)
	if err != nil {
		return summary, err
	}
	if summary.Evidence.Bytes <= 0 || summary.Evidence.Bytes > MaxQueryOutputBytes {
		return summary, ErrQueryExecutionLimit
	}
	if err := db.QueryRowContext(ctx, `SELECT version()`).Scan(&summary.DuckDBVersion); err != nil {
		return summary, err
	}
	return summary, nil
}

func (request *QueryRequest) applyDefaults() {
	if request.NativeMemoryBytes == 0 {
		request.NativeMemoryBytes = DefaultNativeMemoryBytes
	}
	if request.NativeSpillBytes == 0 {
		request.NativeSpillBytes = DefaultNativeSpillBytes
	}
}

func (request QueryRequest) validate() (string, []any, error) {
	if request.Version != QueryExecutionProtocolVersion || request.QueryID == "" || request.Operation.Version != QueryExecutionProtocolVersion || request.Operation.MaxRows < 1 || request.Operation.MaxRows > MaxQueryOutputRows || len(request.InputPaths) > MaxQueryInputFiles || !filepath.IsAbs(request.OutputPath) || !filepath.IsAbs(request.SpillDirectory) || request.OutputPath == request.SpillDirectory {
		return "", nil, errors.New("invalid query request")
	}
	if request.Operation.Kind != "rows" && request.Operation.Kind != "aggregate" {
		return "", nil, errors.New("invalid query operation kind")
	}
	if request.NativeMemoryBytes < 32<<20 || request.NativeMemoryBytes > DefaultNativeMemoryBytes || request.NativeSpillBytes < 64<<20 || request.NativeSpillBytes > DefaultNativeSpillBytes {
		return "", nil, errors.New("query native limits are outside the supported range")
	}
	var statement string
	switch request.Task.Stage {
	case model.QueryTaskScan:
		if request.Task.Level != 0 || len(request.InputPaths) < 1 {
			return "", nil, errors.New("scan task requires inputs at level zero")
		}
		statement = request.Operation.ScanSQL
	case model.QueryTaskReduce:
		if request.Task.Level < 1 {
			return "", nil, errors.New("reducer level is invalid")
		}
		if len(request.InputPaths) == 0 {
			statement = request.Operation.EmptySQL
		} else {
			statement = request.Operation.ReduceSQL
		}
	default:
		return "", nil, errors.New("query task stage is invalid")
	}
	if !safeQueryStatement(statement, len(request.InputPaths) > 0) {
		return "", nil, errors.New("unsafe internal query statement")
	}
	encodedArguments := request.Operation.ReduceArguments
	if request.Task.Stage == model.QueryTaskScan {
		encodedArguments = request.Operation.ScanArguments
	}
	arguments := make([]any, len(encodedArguments))
	for index, argument := range encodedArguments {
		value, err := queryArgumentValue(argument)
		if err != nil {
			return "", nil, err
		}
		arguments[index] = value
	}
	return statement, arguments, nil
}

func prepareQueryPaths(request QueryRequest) error {
	if err := os.MkdirAll(request.SpillDirectory, 0o700); err != nil {
		return err
	}
	info, err := os.Stat(request.SpillDirectory)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return errors.New("query spill directory must be private")
	}
	entries, err := os.ReadDir(request.SpillDirectory)
	if err != nil || len(entries) != 0 {
		return errors.New("query spill directory must start empty")
	}
	if _, err := os.Stat(request.OutputPath); !errors.Is(err, os.ErrNotExist) {
		return errors.New("query output path must not exist")
	}
	seen := map[string]bool{}
	for _, path := range request.InputPaths {
		if !filepath.IsAbs(path) || path == request.OutputPath || seen[path] {
			return errors.New("invalid query input path")
		}
		seen[path] = true
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > MaxBundleFileBytes {
			return errors.New("query input file is invalid")
		}
	}
	return nil
}

func safeQueryStatement(statement string, hasInput bool) bool {
	trimmed := strings.TrimSpace(statement)
	lower := strings.ToLower(trimmed)
	if trimmed == "" || len(trimmed) > 65536 || (!strings.HasPrefix(lower, "select ") && !strings.HasPrefix(lower, "with ")) || strings.ContainsAny(trimmed, ";\x00") {
		return false
	}
	for _, forbidden := range []string{"--", "/*", "*/", "read_parquet", "read_csv", "attach ", "install ", "load ", "pragma ", "copy ", "http://", "https://", "s3://"} {
		if strings.Contains(lower, forbidden) {
			return false
		}
	}
	return !hasInput || strings.Contains(lower, "input_rows")
}

func queryArgumentValue(argument QueryArgument) (any, error) {
	switch argument.Type {
	case "null":
		if argument.Value != "" {
			return nil, errors.New("null query argument has a value")
		}
		return nil, nil
	case "string", "decimal":
		return argument.Value, nil
	case "int64":
		return strconv.ParseInt(argument.Value, 10, 64)
	case "double":
		value, err := strconv.ParseFloat(argument.Value, 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, errors.New("invalid finite double query argument")
		}
		return value, nil
	case "bool":
		return strconv.ParseBool(argument.Value)
	default:
		return nil, errors.New("unknown query argument type")
	}
}
