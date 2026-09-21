package engine

import (
	"context"
	"errors"
	"fmt"
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
	analyticsPaths, payloadPaths := make([]string, len(request.Inputs)), make([]string, len(request.Inputs))
	for index, input := range request.Inputs {
		analyticsPaths[index], payloadPaths[index] = input.AnalyticsPath, input.PayloadPath
	}
	if err := verifyCompactionInputs(ctx, db, request, analyticsPaths, payloadPaths); err != nil {
		return CompactionResult{}, fmt.Errorf("verify compaction inputs: %w", err)
	}
	analyticsPath := filepath.Join(request.OutputDirectory, "compact.analytics.parquet")
	payloadPath := filepath.Join(request.OutputDirectory, "compact.payload.parquet")
	retained := `SELECT * FROM read_parquet(` + parquetPathList(analyticsPaths) + `)`
	if request.MinReceivedTimeUS > 0 {
		retained += ` WHERE received_time_us>=` + strconv.FormatInt(request.MinReceivedTimeUS, 10)
	}
	if err := copyCompactionRole(ctx, db, request, retained, analyticsPath, analyticsPaths, false); err != nil {
		return CompactionResult{}, fmt.Errorf("rewrite analytics parquet: %w", err)
	}
	// Payload's physical schema omits layout keys. Read only the retained
	// analytics keys once, then reuse them across bounded payload COPY batches.
	if _, err := db.ExecContext(ctx, `CREATE TEMP TABLE compact_layout AS SELECT `+bundleSortColumns+` FROM (`+retained+`)`); err != nil {
		return CompactionResult{}, fmt.Errorf("read retained layout keys: %w", err)
	}
	payload := `SELECT p.record_id,p.raw_json,p.envelope_sdk_json,p.normalization_warnings_json,p.canonical_metadata_json,a.project_id,a.service,a.event_time_us FROM read_parquet(` + parquetPathList(payloadPaths) + `) p JOIN compact_layout a USING(record_id)`
	if err := copyCompactionRole(ctx, db, request, payload, payloadPath, payloadPaths, true); err != nil {
		return CompactionResult{}, fmt.Errorf("rewrite payload parquet: %w", err)
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
	if request.Version != CompactionProtocolVersion || request.TenantID <= 0 || request.LaneID < 0 || request.LaneID >= model.LaneCount || request.SchemaVersion <= 0 || request.GroupingVersion <= 0 || request.MinReceivedTimeUS < 0 || request.Kind != model.KindError && request.Kind != model.KindLog && request.Kind != model.KindTransaction || len(request.Inputs) < 1 || len(request.Inputs) > 128 {
		return errors.New("invalid compaction request")
	}
	if request.MinReceivedTimeUS == 0 && len(request.Inputs) < 2 {
		return errors.New("compaction requires at least two inputs")
	}
	if _, err := time.Parse("2006-01-02", request.EventDay); err != nil {
		return errors.New("invalid compaction event day")
	}
	if !filepath.IsAbs(request.OutputDirectory) || !filepath.IsAbs(request.SpillDirectory) || request.OutputDirectory == request.SpillDirectory || request.NativeMemoryBytes < 32<<20 || request.NativeMemoryBytes > DefaultNativeMemoryBytes || request.NativeSpillBytes < 64<<20 || request.NativeSpillBytes > DefaultNativeSpillBytes {
		return errors.New("invalid compaction paths or native limits")
	}
	seen := make(map[string]bool, len(request.Inputs))
	paths := make(map[string]bool, 2*len(request.Inputs))
	var total int64
	for _, input := range request.Inputs {
		if input.BundleID == "" || seen[input.BundleID] || !filepath.IsAbs(input.AnalyticsPath) || !filepath.IsAbs(input.PayloadPath) || input.AnalyticsPath == input.PayloadPath || !validEngineSHA(input.IdentitySHA256) {
			return errors.New("invalid compaction input")
		}
		seen[input.BundleID] = true
		for _, path := range []string{input.AnalyticsPath, input.PayloadPath} {
			if paths[filepath.Clean(path)] {
				return errors.New("compaction input file is repeated")
			}
			paths[filepath.Clean(path)] = true
			info, err := os.Stat(path)
			if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > MaxBundleFileBytes || total > (256<<20)-info.Size() {
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
