package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/chawanghyeon/eventglass/internal/model"
)

// Compact rewrites verified paired Parquet inputs without changing logical rows.
// Selection and catalog authority stay in control; this native boundary only
// proves pair identity and emits one physical replacement.
func Compact(ctx context.Context, request CompactionRequest) (CompactionResult, error) {
	if err := validateCompactionRequest(request); err != nil {
		return CompactionResult{}, err
	}
	if err := os.Mkdir(request.OutputDirectory, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return CompactionResult{}, err
	}
	entries, err := os.ReadDir(request.OutputDirectory)
	if err != nil || len(entries) != 0 {
		return CompactionResult{}, errors.Join(errors.New("compaction output directory must be empty"), err)
	}
	if err := os.MkdirAll(request.SpillDirectory, 0o700); err != nil {
		return CompactionResult{}, err
	}
	defer os.RemoveAll(request.SpillDirectory)
	db, err := Open(ctx, "")
	if err != nil {
		return CompactionResult{}, err
	}
	defer db.Close()
	for _, statement := range []string{
		"SET memory_limit = '" + strconv.FormatInt(request.NativeMemoryBytes, 10) + "B'",
		"SET max_temp_directory_size = '" + strconv.FormatInt(request.NativeSpillBytes, 10) + "B'",
		"SET temp_directory = '" + quoteSQLString(request.SpillDirectory) + "'",
		"SET preserve_insertion_order = false",
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return CompactionResult{}, err
		}
	}
	day, _ := time.Parse("2006-01-02", request.EventDay)
	startUS, endUS := day.UnixMicro(), day.Add(24*time.Hour).UnixMicro()
	analyticsPaths, payloadPaths := make([]string, len(request.Inputs)), make([]string, len(request.Inputs))
	for index, input := range request.Inputs {
		analyticsPaths[index], payloadPaths[index] = input.AnalyticsPath, input.PayloadPath
		inspected, err := inspectPartition(ctx, db, index, partition{day: request.EventDay, kind: request.Kind}, input.AnalyticsPath, input.PayloadPath)
		if err != nil || inspected.IdentitySHA256 != input.IdentitySHA256 {
			return CompactionResult{}, errors.Join(errors.New("compaction input pair identity mismatch"), err)
		}
		var rows, scoped int64
		err = db.QueryRowContext(ctx, `SELECT count(*),count(*) FILTER (WHERE tenant_id=? AND lane_id=? AND kind=? AND schema_version=? AND grouping_version=? AND event_time_us>=? AND event_time_us<?) FROM read_parquet(?)`, request.TenantID, request.LaneID, request.Kind, request.SchemaVersion, request.GroupingVersion, startUS, endUS, input.AnalyticsPath).Scan(&rows, &scoped)
		if err != nil || rows == 0 || rows != scoped {
			return CompactionResult{}, errors.Join(errors.New("compaction input scope mismatch"), err)
		}
	}
	analyticsPath := filepath.Join(request.OutputDirectory, "compact.analytics.parquet")
	payloadPath := filepath.Join(request.OutputDirectory, "compact.payload.parquet")
	analyticsSQL := `COPY (SELECT * FROM read_parquet(` + parquetPathList(analyticsPaths) + `) ORDER BY received_time_us,lane_id,batch_seq,record_ordinal,record_id) TO '` + quoteSQLString(analyticsPath) + `' (FORMAT PARQUET,COMPRESSION ZSTD,COMPRESSION_LEVEL 3,ROW_GROUP_SIZE 16384)`
	payloadSQL := `COPY (SELECT * FROM read_parquet(` + parquetPathList(payloadPaths) + `) ORDER BY record_id) TO '` + quoteSQLString(payloadPath) + `' (FORMAT PARQUET,COMPRESSION ZSTD,COMPRESSION_LEVEL 3,ROW_GROUP_SIZE 16384)`
	for _, statement := range []string{analyticsSQL, payloadSQL} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return CompactionResult{}, err
		}
	}
	bundle, err := inspectPartition(ctx, db, 0, partition{day: request.EventDay, kind: request.Kind}, analyticsPath, payloadPath)
	if err != nil {
		return CompactionResult{}, err
	}
	var version string
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		return CompactionResult{}, err
	}
	return CompactionResult{DuckDBVersion: version, Bundle: bundle}, nil
}

func validateCompactionRequest(request CompactionRequest) error {
	if request.Version != CompactionProtocolVersion || request.TenantID <= 0 || request.LaneID < 0 || request.LaneID >= model.LaneCount || request.SchemaVersion <= 0 || request.GroupingVersion <= 0 || request.Kind != model.KindError && request.Kind != model.KindLog && request.Kind != model.KindTransaction || len(request.Inputs) < 2 || len(request.Inputs) > 128 {
		return errors.New("invalid compaction request")
	}
	if _, err := time.Parse("2006-01-02", request.EventDay); err != nil {
		return errors.New("invalid compaction event day")
	}
	if !filepath.IsAbs(request.OutputDirectory) || !filepath.IsAbs(request.SpillDirectory) || request.OutputDirectory == request.SpillDirectory || request.NativeMemoryBytes < 32<<20 || request.NativeMemoryBytes > DefaultNativeMemoryBytes || request.NativeSpillBytes < 64<<20 || request.NativeSpillBytes > DefaultNativeSpillBytes {
		return errors.New("invalid compaction paths or native limits")
	}
	seen := make(map[string]bool, len(request.Inputs))
	var total int64
	for _, input := range request.Inputs {
		if input.BundleID == "" || seen[input.BundleID] || !filepath.IsAbs(input.AnalyticsPath) || !filepath.IsAbs(input.PayloadPath) || input.AnalyticsPath == input.PayloadPath || !validEngineSHA(input.IdentitySHA256) {
			return errors.New("invalid compaction input")
		}
		seen[input.BundleID] = true
		for _, path := range []string{input.AnalyticsPath, input.PayloadPath} {
			info, err := os.Stat(path)
			if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || total > (256<<20)-info.Size() {
				return errors.Join(errors.New("compaction input budget exceeded"), err)
			}
			total += info.Size()
		}
	}
	return nil
}

func parquetPathList(paths []string) string {
	values := append([]string(nil), paths...)
	sort.Strings(values)
	for index, value := range values {
		values[index] = "'" + quoteSQLString(value) + "'"
	}
	return "[" + strings.Join(values, ",") + "]"
}

func validEngineSHA(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return true
}
