package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const compactCopyOptions = "FORMAT PARQUET,COMPRESSION ZSTD,COMPRESSION_LEVEL 3,ROW_GROUP_SIZE 16384"

// Keep the ordinary path small. Wide sorted vectors otherwise overlap the
// Parquet writer's buffers and exhaust the native limit even when sorting alone
// succeeds. The wide path sorts only keys, writes bounded ordered partitions,
// then concatenates them in that exact order with the same native writer.
func copyCompactionRole(ctx context.Context, db *sql.DB, request CompactionRequest, source, output string, paths []string, payload bool) error {
	order := bundlePhysicalSort
	projection := "*"
	if payload {
		// Sort keys may occur in private intermediate files, never in the stored
		// payload schema. They remain native values, not re-decoded JSON fields.
		projection = "record_id,raw_json,envelope_sdk_json,normalization_warnings_json,canonical_metadata_json"
	}
	var uncompressed int64
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(sum(total_uncompressed_size),0) FROM parquet_metadata(`+parquetPathList(paths)+`)`).Scan(&uncompressed); err != nil {
		return err
	}
	if uncompressed <= request.NativeMemoryBytes/8 {
		_, err := db.ExecContext(ctx, `COPY (SELECT `+projection+` FROM (`+source+`) ORDER BY `+order+`) TO '`+quoteSQLString(output)+`' (`+compactCopyOptions+`)`)
		return err
	}
	// Materialize only scalar keys and sizes BEFORE the window. Serializing a
	// wide analytic row to JSON just to count bytes duplicates its large strings
	// in expression allocators. Measure the typed values without encoding them.
	rowBytes := compactionAnalyticsRowBytesSQL()
	if payload {
		rowBytes = "64+COALESCE(bit_length(raw_json)//8,0)+COALESCE(bit_length(envelope_sdk_json)//8,0)+COALESCE(bit_length(normalization_warnings_json)//8,0)+COALESCE(bit_length(canonical_metadata_json)//8,0)"
	}
	keys := `CREATE TEMP TABLE compact_keys AS WITH sizes AS MATERIALIZED (
		SELECT ` + bundleSortColumns + `,greatest(` + rowBytes + `,4096)::BIGINT AS row_bytes FROM (` + source + `) a)
		SELECT record_id,row_bytes,((sum(row_bytes) OVER(ORDER BY ` + order + ` ROWS UNBOUNDED PRECEDING)-1)//4194304)::BIGINT AS chunk FROM sizes`
	if _, err := db.ExecContext(ctx, keys); err != nil {
		return fmt.Errorf("size ordered partitions: %w", err)
	}
	var bytes, chunks int64
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(sum(row_bytes),0),count(DISTINCT chunk) FROM compact_keys`).Scan(&bytes, &chunks); err != nil {
		return err
	}
	// Intermediate files share, rather than add to, the requested spill budget.
	// Leave at least16MiB to the native sorter. Size accounting includes a2x
	// encoding allowance and64KiB/footer per bounded partition. Check actual
	// files as well before the final COPY. No unbounded Go manifest or fan-out.
	if chunks > 1024 || bytes > request.NativeSpillBytes/2 {
		return errors.New("compaction intermediate budget exceeded")
	}
	reserved := 2*bytes + chunks*(64<<10)
	if reserved > request.NativeSpillBytes-(16<<20) {
		return errors.New("compaction intermediate budget exceeded")
	}
	if _, err := db.ExecContext(ctx, "SET max_temp_directory_size='"+strconv.FormatInt(request.NativeSpillBytes-reserved, 10)+"B'"); err != nil {
		return err
	}
	directory, err := os.MkdirTemp(request.SpillDirectory, "compact-parts-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(directory) // ExecContext has joined its native work first.
	options := compactCopyOptions + ",ROW_GROUP_SIZE_BYTES '4MiB',DATA_PAGE_SIZE_LIMIT 1048576,STRING_DICTIONARY_PAGE_SIZE_LIMIT 1048576"
	var statement string
	// The pinned partitioned writer also retains its input batch. Each4MiB key
	// range needs room for decoded values, sorting and writer state. Scale its
	// fan-in with the managed limit, never admitting more than eight ranges.
	// This is sequential local I/O over already verified files, not more S3 GETs.
	rangesPerWrite := max(int64(1), min(int64(8), request.NativeMemoryBytes/(24<<20)))
	for after := int64(-1); ; {
		var next sql.NullInt64
		if err := db.QueryRowContext(ctx, `SELECT min(chunk) FROM compact_keys WHERE chunk>?`, after).Scan(&next); err != nil {
			return err
		}
		if !next.Valid {
			break
		}
		end := next.Int64 + rangesPerWrite
		statement = `COPY (SELECT a.*,k.chunk FROM (` + source + `) a JOIN (SELECT * FROM compact_keys WHERE chunk>=` + strconv.FormatInt(next.Int64, 10) + ` AND chunk<` + strconv.FormatInt(end, 10) + `) k USING(record_id)) TO '` + quoteSQLString(directory) + `' (` + options + `,PARTITION_BY (chunk),ORDER_BY (` + order + `),OVERWRITE_OR_IGNORE true)`
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("write ordered partitions: %w", err)
		}
		after = end - 1
	}
	parts, err := compactionPartPaths(directory, chunks, reserved)
	if err != nil {
		return err
	}
	if len(parts) == 0 {
		// Retention can remove every row. Preserve the exact source schema.
		statement = `COPY (SELECT ` + projection + ` FROM (` + source + `) WHERE false) TO '` + quoteSQLString(output) + `' (` + compactCopyOptions + `)`
	} else {
		// Do not use parquetPathList here: lexicographic sorting would put chunk10
		// before chunk2. No wildcard/hive column or physical final re-sort.
		for index, path := range parts {
			parts[index] = "'" + quoteSQLString(path) + "'"
		}
		statement = `COPY (SELECT ` + projection + ` FROM read_parquet([` + strings.Join(parts, ",") + `],hive_partitioning=false)) TO '` + quoteSQLString(output) + `' (` + options + `,PRESERVE_ORDER true)`
	}
	if _, err := db.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("concatenate ordered partitions: %w", err)
	}
	if _, err := db.ExecContext(ctx, "DROP TABLE compact_keys"); err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, "SET max_temp_directory_size='"+strconv.FormatInt(request.NativeSpillBytes, 10)+"B'")
	return err
}

func compactionAnalyticsRowBytesSQL() string {
	// Charge fixed scalar/vector/null overhead once per row, then every variable
	// string byte. Native Parquet writes these values, not their JSON escapes.
	// Nested attributes include256B each for their struct/decimal/null overhead;
	// string lists include32B per element. The existing2x intermediate allowance
	// and checked actual file manifest are still required, not replaced by this
	// per-row estimate. bit_length counts UTF-8 bytes rather than characters.
	terms := []string{"4096"}
	for _, column := range []string{"record_id", "acceptance_id", "batch_id", "kind", "source_event_id", "trace_id", "span_id", "level", "original_level", "message", "message_template", "service", "environment", "release", "logger", "sdk_name", "sdk_version", "platform", "server_name", "issue_id", "exception_type", "exception_value"} {
		terms = append(terms, "COALESCE(bit_length("+column+")//8,0)")
	}
	attribute := []string{"256"}
	for _, field := range []string{"namespace", "path", "value_type", "string_value", "json_value", "unit"} {
		attribute = append(attribute, "COALESCE(bit_length(attribute."+field+")//8,0)")
	}
	terms = append(terms, "COALESCE(list_sum(list_transform(attrs,lambda attribute: "+strings.Join(attribute, "+")+")),0)")
	for _, column := range []string{"search_values", "warnings"} {
		terms = append(terms, "COALESCE(list_sum(list_transform("+column+",lambda value: 32+COALESCE(bit_length(value)//8,0))),0)")
	}
	return strings.Join(terms, "+")
}

func compactionPartPaths(directory string, count, maxBytes int64) ([]string, error) {
	if count < 0 || count > 1024 || maxBytes < 0 {
		return nil, errors.New("invalid compaction partition budget")
	}
	root, err := os.Open(directory)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	dirs, err := root.ReadDir(int(count) + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if int64(len(dirs)) != count {
		return nil, errors.New("compaction partition count mismatch")
	}
	type part struct {
		chunk int64
		path  string
	}
	parts := make([]part, 0, len(dirs))
	var bytes int64
	for _, dir := range dirs {
		chunk, parseErr := strconv.ParseInt(strings.TrimPrefix(dir.Name(), "chunk="), 10, 64)
		if parseErr != nil || chunk < 0 || !strings.HasPrefix(dir.Name(), "chunk=") || !dir.IsDir() {
			return nil, errors.New("invalid compaction partition")
		}
		path := filepath.Join(directory, dir.Name(), "data_0.parquet")
		folder, err := os.Open(filepath.Dir(path))
		if err != nil {
			return nil, err
		}
		entries, readErr := folder.ReadDir(2)
		closeErr := folder.Close()
		if readErr != nil && !errors.Is(readErr, io.EOF) || closeErr != nil || len(entries) != 1 || entries[0].Name() != "data_0.parquet" {
			return nil, errors.Join(errors.New("unexpected compaction partition files"), readErr, closeErr)
		}
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maxBytes-bytes {
			return nil, fmt.Errorf("compaction partition byte budget: %w", errors.Join(errors.New("invalid partition size"), err))
		}
		bytes += info.Size()
		parts = append(parts, part{chunk: chunk, path: path})
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].chunk < parts[j].chunk })
	paths := make([]string, len(parts))
	for index, part := range parts {
		if index > 0 && parts[index-1].chunk == part.chunk {
			return nil, errors.New("duplicate compaction partition")
		}
		paths[index] = part.path
	}
	return paths, nil
}
