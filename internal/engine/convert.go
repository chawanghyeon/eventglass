package engine

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/storage"
	duckdb "github.com/duckdb/duckdb-go/v2"
)

const (
	appendRowsPerFlush  = 2048
	appendBytesPerFlush = 4 << 20
	maxStageLineBytes   = (2 << 20) + (64 << 10)
	bundleSortColumns   = "project_id,service,event_time_us,record_id"
	bundlePhysicalSort  = "project_id,service NULLS FIRST,event_time_us,record_id"
)

type nativeStageLine struct {
	BatchID         string  `json:"batch_id"`
	LaneID          int     `json:"lane_id"`
	BatchSeq        int64   `json:"batch_seq"`
	GlobalOrdinal   int     `json:"global_ordinal"`
	ReceivedTimeUS  int64   `json:"received_time_us"`
	GroupingVersion int     `json:"grouping_version"`
	IssueID         string  `json:"issue_id,omitempty"`
	EventDay        string  `json:"event_day"`
	ExceptionType   *string `json:"exception_type"`
	ExceptionValue  *string `json:"exception_value"`
	Handled         *bool   `json:"handled"`
}

type partition struct {
	day  string
	kind model.Kind
}

// Convert consumes a fully verified private stage and emits one message per
// completed pair. A nil return is the only state in which output files remain.
func Convert(ctx context.Context, request ConversionRequest, emit func(ConvertedBundle) error) (summary ConversionSummary, retErr error) {
	request.applyDefaults()
	if err := request.validate(); err != nil {
		return summary, err
	}
	if emit == nil {
		return summary, errors.New("conversion emitter is required")
	}
	if err := prepareConversionDirectories(request); err != nil {
		return summary, err
	}
	created := make([]string, 0)
	defer func() {
		if retErr != nil {
			for _, path := range created {
				_ = os.Remove(path)
			}
		}
		_ = os.RemoveAll(request.SpillDirectory)
	}()

	db, err := Open(ctx, "")
	if err != nil {
		return summary, err
	}
	defer db.Close()
	if err := configureConversionDB(ctx, db, request); err != nil {
		return summary, err
	}
	actualRecords, actualErrors, err := appendStage(ctx, db, request)
	if err != nil {
		return summary, err
	}
	if actualRecords != request.SelectedRecords || actualErrors != request.SelectedErrors {
		return summary, errors.New("staged selection counts do not match request")
	}
	if err := materializeStage(ctx, db); err != nil {
		return summary, fmt.Errorf("materialize conversion stage: %w", err)
	}
	version := ""
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		return summary, err
	}
	summary = ConversionSummary{DuckDBVersion: version, SelectedRecordCount: actualRecords, SelectedErrorCount: actualErrors}
	identity, err := identityForQuery(ctx, db, "SELECT record_id FROM staged ORDER BY record_id", nil)
	if err != nil {
		return summary, err
	}
	summary.SelectedIdentitySHA256 = identity
	if actualRecords == 0 {
		return summary, nil
	}

	var after *partition
	for bundleIndex := 0; ; bundleIndex++ {
		current, found, err := nextPartition(ctx, db, after)
		if err != nil {
			return summary, fmt.Errorf("find conversion partition: %w", err)
		}
		if !found {
			break
		}
		bundle, paths, err := writePartition(ctx, db, request.OutputDirectory, bundleIndex, current)
		created = append(created, paths...)
		if err != nil {
			return summary, fmt.Errorf("write conversion partition: %w", err)
		}
		if err := emit(bundle); err != nil {
			return summary, err
		}
		summary.BundleCount++
		after = &current
	}
	return summary, nil
}

func prepareConversionDirectories(request ConversionRequest) error {
	for _, path := range []string{request.OutputDirectory, request.SpillDirectory} {
		clean := filepath.Clean(path)
		if clean == string(filepath.Separator) || clean == filepath.Dir(clean) {
			return errors.New("unsafe conversion directory")
		}
		if err := os.MkdirAll(clean, 0o700); err != nil {
			return err
		}
		info, err := os.Stat(clean)
		if err != nil || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
			return errors.New("conversion directories must be private")
		}
		entries, err := os.ReadDir(clean)
		if err != nil || len(entries) != 0 {
			return errors.New("conversion directories must start empty")
		}
	}
	return nil
}

func configureConversionDB(ctx context.Context, db *sql.DB, request ConversionRequest) error {
	quotedSpill := quoteSQLString(request.SpillDirectory)
	statements := []string{
		fmt.Sprintf("SET memory_limit = '%dB'", request.NativeMemoryBytes),
		fmt.Sprintf("SET max_temp_directory_size = '%dB'", request.NativeSpillBytes),
		"SET temp_directory = '" + quotedSpill + "'",
		"SET threads = 1",
		"SET preserve_insertion_order = false",
		// Resolve the fixed attribute structure once per conversion connection.
		// from_json retains its missing-field/NULL semantics without rebinding
		// all nine type strings for each analytics COPY.
		"CREATE TYPE eventglass_attribute AS STRUCT(namespace VARCHAR,path VARCHAR,value_type VARCHAR,string_value VARCHAR,integer_value DECIMAL(38,0),double_value DOUBLE,boolean_value BOOLEAN,json_value VARCHAR,unit VARCHAR)",
		// Appender strings inserted directly into DuckDB's JSON logical type are
		// represented as JSON string scalars. Keep the verified JSONL bytes as
		// VARCHAR and let json_extract parse them when materializing the stage.
		// Payload JSON has already been validated by the supervisor and Go
		// stage reader. Keep it in separate VARCHAR columns, not duplicated
		// inside the analytics JSON that DuckDB needs to parse.
		"CREATE TABLE stage_lines(stage_json VARCHAR NOT NULL,raw_json VARCHAR NOT NULL,envelope_sdk_json VARCHAR,normalization_warnings_json VARCHAR NOT NULL,canonical_metadata_json VARCHAR NOT NULL,payload_chunk BIGINT NOT NULL)",
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("configure conversion: %w", err)
		}
	}
	return nil
}

func appendStage(ctx context.Context, db *sql.DB, request ConversionRequest) (int, int, error) {
	stage, err := os.Open(request.StagePath)
	if err != nil {
		return 0, 0, err
	}
	defer stage.Close()
	connection, err := db.Conn(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer connection.Close()
	records, errorRecords := 0, 0
	err = connection.Raw(func(raw any) error {
		driverConn, ok := raw.(driver.Conn)
		if !ok {
			return errors.New("DuckDB connection does not expose driver connection")
		}
		appender, err := duckdb.NewAppenderFromConn(driverConn, "", "stage_lines")
		if err != nil {
			return err
		}
		closed := false
		defer func() {
			if !closed {
				_ = appender.Clear()
				_ = appender.Close()
			}
		}()
		scanner := bufio.NewScanner(stage)
		scanner.Buffer(make([]byte, 64<<10), maxStageLineBytes)
		chunkRows, chunkBytes := 0, 0
		var payloadChunk int64
		seenOrdinal := -1
		for scanner.Scan() {
			if err := ctx.Err(); err != nil {
				return err
			}
			var staged StageRecord
			if err := strictJSON(scanner.Bytes(), &staged); err != nil {
				return fmt.Errorf("decode conversion stage: %w", err)
			}
			if err := validateStageRecord(staged, request, seenOrdinal); err != nil {
				return err
			}
			seenOrdinal = staged.GlobalOrdinal
			metadata, err := canonicalMetadata(staged.Record)
			if err != nil {
				return err
			}
			exceptionType, exceptionValue, handled := exceptionProjection(staged.Record.Raw)
			// Canonical metadata is shared by analytics and payload output;
			// do not store its wide attributes again inside the scope JSON.
			line := nativeStageLine{BatchID: staged.BatchID, LaneID: staged.LaneID, BatchSeq: staged.BatchSeq,
				GlobalOrdinal: staged.GlobalOrdinal, ReceivedTimeUS: staged.ReceivedTimeUS, GroupingVersion: staged.GroupingVersion, IssueID: staged.IssueID,
				EventDay: time.UnixMicro(staged.Record.EventTimeUS).UTC().Format(time.DateOnly), ExceptionType: exceptionType, ExceptionValue: exceptionValue, Handled: handled}
			encoded, err := json.Marshal(line)
			if err != nil {
				return err
			}
			// Marshal RawMessage to retain the former stage JSON's compact
			// escaping, including null versus absent SDK metadata semantics.
			raw, err := json.Marshal(staged.Record.Raw)
			if err != nil {
				return err
			}
			warnings := []byte("[]")
			if len(staged.Record.Warnings) != 0 {
				warnings, err = json.Marshal(staged.Record.Warnings)
				if err != nil {
					return err
				}
			}
			var envelopeSDK any
			var sdkBytes int
			if len(staged.Record.EnvelopeSDKJSON) != 0 {
				encodedSDK, err := json.Marshal(staged.Record.EnvelopeSDKJSON)
				if err != nil {
					return err
				}
				envelopeSDK, sdkBytes = string(encodedSDK), len(encodedSDK)
			}
			rowBytes := len(encoded) + len(raw) + sdkBytes + len(warnings) + len(metadata)
			if chunkRows > 0 && (chunkRows >= appendRowsPerFlush || chunkBytes > appendBytesPerFlush-rowBytes) {
				if err := appender.FlushWithCancel(ctx); err != nil {
					return err
				}
				chunkRows, chunkBytes = 0, 0
				payloadChunk++
			}
			if err := appender.AppendRow(string(encoded), string(raw), envelopeSDK, string(warnings), string(metadata), payloadChunk); err != nil {
				return err
			}
			records++
			if staged.Record.Kind == model.KindError {
				errorRecords++
			}
			chunkRows++
			chunkBytes += rowBytes
		}
		if err := scanner.Err(); err != nil {
			return err
		}
		if err := appender.CloseWithCancel(ctx); err != nil {
			return err
		}
		closed = true
		return nil
	})
	return records, errorRecords, err
}

func validateStageRecord(staged StageRecord, request ConversionRequest, previousOrdinal int) error {
	record := staged.Record
	if staged.Version != ConversionProtocolVersion || staged.GlobalOrdinal <= previousOrdinal || staged.BatchID != request.BatchID || staged.LaneID != request.LaneID || staged.BatchSeq != request.BatchSeq || record.TenantID != request.TenantID || record.ProjectID <= 0 || record.RecordID == "" || record.Kind == "" {
		return errors.New("conversion stage scope or order mismatch")
	}
	if !json.Valid(record.Raw) || (len(record.EnvelopeSDKJSON) != 0 && !json.Valid(record.EnvelopeSDKJSON)) {
		return errors.New("conversion stage contains invalid canonical JSON")
	}
	if record.Kind == model.KindError {
		if staged.GroupingVersion <= 0 || staged.IssueID == "" || staged.IssueID != staged.FingerprintSHA256 {
			return errors.New("error stage record lacks Issue identity")
		}
	} else if staged.IssueID != "" || staged.FingerprintSHA256 != "" || staged.IssueTitle != "" {
		return errors.New("non-error stage record contains Issue identity")
	}
	return nil
}

func canonicalMetadata(record model.Record) (json.RawMessage, error) {
	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		return nil, err
	}
	delete(fields, "raw")
	delete(fields, "envelope_sdk_json")
	return json.Marshal(fields)
}

func exceptionProjection(raw json.RawMessage) (*string, *string, *bool) {
	var payload map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&payload) != nil {
		return nil, nil, nil
	}
	var values []any
	switch exception := payload["exception"].(type) {
	case map[string]any:
		values, _ = exception["values"].([]any)
	case []any:
		values = exception
	}
	if len(values) == 0 {
		return nil, nil, nil
	}
	representative, ok := values[len(values)-1].(map[string]any)
	if !ok {
		return nil, nil, nil
	}
	optionalString := func(value any) *string {
		text, ok := value.(string)
		if !ok {
			return nil
		}
		return &text
	}
	var handled *bool
	if mechanism, ok := representative["mechanism"].(map[string]any); ok {
		if value, ok := mechanism["handled"].(bool); ok {
			handled = &value
		}
	}
	return optionalString(representative["type"]), optionalString(representative["value"]), handled
}

func materializeStage(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `CREATE TABLE staged AS WITH extracted AS MATERIALIZED (
		SELECT stage_json,raw_json,envelope_sdk_json,normalization_warnings_json,canonical_metadata_json,payload_chunk,
		json_extract(canonical_metadata_json,['$.tenant_id','$.project_id','$.record_id','$.service','$.kind','$.event_time_us']) AS fields,
		json_extract(stage_json,['$.received_time_us','$.batch_seq','$.event_day']) AS scope
		FROM stage_lines)
	SELECT
		stage_json,raw_json,envelope_sdk_json,normalization_warnings_json,canonical_metadata_json,payload_chunk,
		CAST(fields[1] AS BIGINT) AS tenant_id,
		CAST(fields[2] AS BIGINT) AS project_id,
		json_extract_string(fields[3],'$') AS record_id,
		json_extract_string(fields[4],'$') AS service,
		json_extract_string(fields[5],'$') AS kind,
		CAST(fields[6] AS BIGINT) AS event_time_us,
		CAST(scope[1] AS BIGINT) AS received_time_us,
		CAST(scope[2] AS BIGINT) AS batch_seq,
		CAST(json_extract_string(scope[3],'$') AS DATE) AS event_day
	FROM extracted`)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `DROP TABLE stage_lines`)
	return err
}

func nextPartition(ctx context.Context, db *sql.DB, after *partition) (partition, bool, error) {
	query := `SELECT event_day::VARCHAR,kind FROM staged GROUP BY event_day,kind ORDER BY event_day,kind LIMIT 1`
	arguments := []any{}
	if after != nil {
		query = `SELECT event_day::VARCHAR,kind FROM staged WHERE event_day>CAST(? AS DATE) OR (event_day=CAST(? AS DATE) AND kind>?) GROUP BY event_day,kind ORDER BY event_day,kind LIMIT 1`
		arguments = []any{after.day, after.day, string(after.kind)}
	}
	var result partition
	err := db.QueryRowContext(ctx, query, arguments...).Scan(&result.day, &result.kind)
	if errors.Is(err, sql.ErrNoRows) {
		return partition{}, false, nil
	}
	return result, err == nil, err
}

func writePartition(ctx context.Context, db *sql.DB, outputDirectory string, index int, current partition) (ConvertedBundle, []string, error) {
	prefix := fmt.Sprintf("bundle-%06d", index)
	analyticsPath := filepath.Join(outputDirectory, prefix+"-analytics.parquet")
	payloadPath := filepath.Join(outputDirectory, prefix+"-payload.parquet")
	paths := []string{analyticsPath, payloadPath}
	where := ` WHERE event_day=CAST(? AS DATE) AND kind=?`
	order := ` ORDER BY ` + bundlePhysicalSort
	// These typed stage columns were already decoded with the same types by
	// materializeStage. Reuse them instead of parsing the full JSON again.
	// Parse the large canonical stage JSON once per row. Independent extracts
	// retain a separate parsed document per expression/vector: a legal 10k-row
	// batch can exhaust both the native limit and the worker cgroup. Materialize
	// only the requested JSON values plus the already typed sort/scope columns.
	analyticsSQL := `COPY (WITH projected AS MATERIALIZED (
		SELECT tenant_id,project_id,record_id,batch_seq,kind,event_time_us,received_time_us,service,
		json_extract(canonical_metadata_json,[
			'$.acceptance_id','$.arrival_time_us','$.event_time_ns_remainder','$.source_event_id',
			'$.trace_id','$.span_id','$.level','$.original_level','$.severity_number','$.message',
			'$.message_template','$.environment','$.release','$.logger','$.sdk_name','$.sdk_version',
			'$.platform','$.server_name','$.attrs','$.search_values','$.schema_version','$.normalizer_version','$.warnings']) AS fields,
		json_extract(stage_json,['$.batch_id','$.lane_id','$.global_ordinal','$.issue_id',
			'$.exception_type','$.exception_value','$.handled','$.grouping_version']) AS scope
		FROM staged` + where + `)
	SELECT
		tenant_id,
		project_id,
		record_id,
		json_extract_string(fields[1],'$') AS acceptance_id,
		json_extract_string(scope[1],'$') AS batch_id,
		CAST(scope[2] AS INTEGER) AS lane_id,
		batch_seq,
		CAST(scope[3] AS INTEGER) AS record_ordinal,
		kind,
		event_time_us,
		CAST(fields[2] AS BIGINT) AS arrival_time_us,
		received_time_us,
		CAST(fields[3] AS USMALLINT) AS event_time_ns_remainder,
		json_extract_string(fields[4],'$') AS source_event_id,
		json_extract_string(fields[5],'$') AS trace_id,
		json_extract_string(fields[6],'$') AS span_id,
		json_extract_string(fields[7],'$') AS level,
		json_extract_string(fields[8],'$') AS original_level,
		CAST(fields[9] AS SMALLINT) AS severity_number,
		json_extract_string(fields[10],'$') AS message,
		json_extract_string(fields[11],'$') AS message_template,
		service,
		json_extract_string(fields[12],'$') AS environment,
		json_extract_string(fields[13],'$') AS release,
		json_extract_string(fields[14],'$') AS logger,
		json_extract_string(fields[15],'$') AS sdk_name,
		json_extract_string(fields[16],'$') AS sdk_version,
		json_extract_string(fields[17],'$') AS platform,
		json_extract_string(fields[18],'$') AS server_name,
		json_extract_string(scope[4],'$') AS issue_id,
		json_extract_string(scope[5],'$') AS exception_type,
		json_extract_string(scope[6],'$') AS exception_value,
		CAST(scope[7] AS BOOLEAN) AS handled,
		COALESCE(from_json(fields[19], '["eventglass_attribute"]'), []) AS attrs,
		COALESCE(from_json(fields[20], '["VARCHAR"]'), []) AS search_values,
		CAST(fields[21] AS INTEGER) AS schema_version,
		CAST(fields[22] AS INTEGER) AS normalizer_version,
		CAST(scope[8] AS INTEGER) AS grouping_version,
		COALESCE(from_json(fields[23], '["VARCHAR"]'), []) AS warnings
	FROM projected` + order + `) TO '` + quoteSQLString(analyticsPath) + `' (FORMAT PARQUET,COMPRESSION ZSTD,COMPRESSION_LEVEL 3,ROW_GROUP_SIZE 16384)`
	// Canonicalize each payload fragment with the same engine JSON semantics
	// as before (including numeric formatting and escapes). Do not reparse
	// the entire analytics stage document for each of these four fields.
	// A separate statement releases the JSON expression allocators before
	// Parquet COPY needs its writer buffers, including the192MiB role profile.
	payloadStageSQL := `SELECT record_id,project_id,service,event_time_us,
		json_extract(raw_json,'$')::VARCHAR AS raw_json,
		json_extract(envelope_sdk_json,'$')::VARCHAR AS envelope_sdk_json,
		json_extract(normalization_warnings_json,'$')::VARCHAR AS normalization_warnings_json,
		json_extract(canonical_metadata_json,'$')::VARCHAR AS canonical_metadata_json
		FROM staged` + where + ` AND payload_chunk=?`
	payloadSQL := `COPY (SELECT record_id,raw_json,envelope_sdk_json,normalization_warnings_json,canonical_metadata_json
		FROM payload_partition` + order + `) TO '` + quoteSQLString(payloadPath) + `' (FORMAT PARQUET,COMPRESSION ZSTD,COMPRESSION_LEVEL 3,ROW_GROUP_SIZE 16384)`
	if _, err := db.ExecContext(ctx, analyticsSQL, current.day, string(current.kind)); err != nil {
		return ConvertedBundle{}, paths, fmt.Errorf("write analytics parquet: %w", err)
	}
	// DuckDB vectors can otherwise contain an entire legal wide request.
	// Normalize only one appender byte-bounded chunk at a time; retain O(1)
	// chunk cursor state and release each statement's parser before continuing.
	for after := int64(-1); ; {
		var next sql.NullInt64
		if err := db.QueryRowContext(ctx, `SELECT min(payload_chunk) FROM staged`+where+` AND payload_chunk>?`, current.day, string(current.kind), after).Scan(&next); err != nil {
			return ConvertedBundle{}, paths, err
		}
		if !next.Valid {
			break
		}
		prefix := "INSERT INTO payload_partition "
		if after < 0 {
			prefix = "CREATE TEMP TABLE payload_partition AS "
		}
		if _, err := db.ExecContext(ctx, prefix+payloadStageSQL, current.day, string(current.kind), next.Int64); err != nil {
			return ConvertedBundle{}, paths, fmt.Errorf("materialize payload chunk: %w", err)
		}
		after = next.Int64
	}
	if _, err := db.ExecContext(ctx, payloadSQL); err != nil {
		return ConvertedBundle{}, paths, fmt.Errorf("write payload parquet: %w", err)
	}
	if _, err := db.ExecContext(ctx, "DROP TABLE payload_partition"); err != nil {
		return ConvertedBundle{}, paths, err
	}
	bundle, err := inspectPartition(ctx, db, index, current, analyticsPath, payloadPath)
	return bundle, paths, err
}

func inspectPartition(ctx context.Context, db *sql.DB, index int, current partition, analyticsPath, payloadPath string) (ConvertedBundle, error) {
	var result ConvertedBundle
	result.Index, result.EventDay, result.Kind = index, current.day, current.kind
	stats := &result.Analytics
	identity, projects, err := inspectAnalyticsRows(ctx, db, analyticsPath, stats)
	if err != nil {
		return result, err
	}
	stats.Path = analyticsPath
	stats.Evidence, err = storage.InspectFile(analyticsPath)
	if err != nil {
		return result, err
	}
	result.Payload = *stats
	result.Payload.Path = payloadPath
	result.Payload.Evidence, err = storage.InspectFile(payloadPath)
	if err != nil {
		return result, err
	}
	if stats.Evidence.Bytes > MaxBundleFileBytes || result.Payload.Evidence.Bytes > MaxBundleFileBytes {
		return result, errors.New("conversion output exceeds hard file target")
	}
	payloadIdentity, payloadRows, err := identityAndCountForQuery(ctx, db, `SELECT record_id FROM read_parquet(?) ORDER BY record_id`, []any{payloadPath})
	if err != nil || payloadRows != stats.RowCount || payloadIdentity != identity {
		return result, errors.Join(errors.New("analytics and payload record sets differ"), err)
	}
	result.RowCount, result.IdentitySHA256, result.ProjectIDs = stats.RowCount, identity, projects
	return result, nil
}

func inspectAnalyticsRows(ctx context.Context, db *sql.DB, path string, stats *ConvertedFile) (string, []int64, error) {
	rows, err := db.QueryContext(ctx, `SELECT record_id,project_id,event_time_us,received_time_us,batch_seq FROM read_parquet(?) ORDER BY record_id`, path)
	if err != nil {
		return "", nil, err
	}
	defer rows.Close()
	hash := sha256.New()
	previous := ""
	projects := make(map[int64]struct{})
	for rows.Next() {
		var recordID string
		var projectID, eventTime, receivedTime, batchSeq int64
		if err := rows.Scan(&recordID, &projectID, &eventTime, &receivedTime, &batchSeq); err != nil {
			return "", nil, err
		}
		if err := appendRecordIdentity(hash, recordID, previous); err != nil || projectID <= 0 || batchSeq <= 0 {
			return "", nil, errors.Join(errors.New("invalid analytics output row"), err)
		}
		previous = recordID
		if stats.RowCount == 0 {
			stats.MinEventTimeUS, stats.MaxEventTimeUS = eventTime, eventTime
			stats.MinReceivedTimeUS, stats.MaxReceivedTimeUS = receivedTime, receivedTime
			stats.MinBatchSeq, stats.MaxBatchSeq = batchSeq, batchSeq
		} else {
			stats.MinEventTimeUS, stats.MaxEventTimeUS = min(stats.MinEventTimeUS, eventTime), max(stats.MaxEventTimeUS, eventTime)
			stats.MinReceivedTimeUS, stats.MaxReceivedTimeUS = min(stats.MinReceivedTimeUS, receivedTime), max(stats.MaxReceivedTimeUS, receivedTime)
			stats.MinBatchSeq, stats.MaxBatchSeq = min(stats.MinBatchSeq, batchSeq), max(stats.MaxBatchSeq, batchSeq)
		}
		stats.RowCount++
		projects[projectID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return "", nil, err
	}
	if stats.RowCount <= 0 {
		return "", nil, errors.New("invalid analytics output stats")
	}
	projectIDs := make([]int64, 0, len(projects))
	for projectID := range projects {
		projectIDs = append(projectIDs, projectID)
	}
	sort.Slice(projectIDs, func(i, j int) bool { return projectIDs[i] < projectIDs[j] })
	return hex.EncodeToString(hash.Sum(nil)), projectIDs, nil
}

func identityForQuery(ctx context.Context, db *sql.DB, query string, arguments []any) (string, error) {
	identity, _, err := identityAndCountForQuery(ctx, db, query, arguments)
	return identity, err
}

func identityAndCountForQuery(ctx context.Context, db *sql.DB, query string, arguments []any) (string, int64, error) {
	rows, err := db.QueryContext(ctx, query, arguments...)
	if err != nil {
		return "", 0, err
	}
	defer rows.Close()
	hash := sha256.New()
	previous := ""
	var count int64
	for rows.Next() {
		var recordID string
		if err := rows.Scan(&recordID); err != nil {
			return "", 0, err
		}
		if err := appendRecordIdentity(hash, recordID, previous); err != nil {
			return "", 0, err
		}
		previous = recordID
		count++
	}
	if err := rows.Err(); err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(hash.Sum(nil)), count, nil
}

func appendRecordIdentity(hash io.Writer, recordID, previous string) error {
	if recordID <= previous || len(recordID) != 64 || recordID != strings.ToLower(recordID) {
		return errors.New("output record identities are invalid or duplicated")
	}
	if _, err := hex.DecodeString(recordID); err != nil {
		return err
	}
	_, _ = io.WriteString(hash, recordID)
	_, _ = io.WriteString(hash, "\n")
	return nil
}

func strictJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}

func quoteSQLString(value string) string { return strings.ReplaceAll(value, "'", "''") }
