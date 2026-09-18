package engine

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestParquetNestedDecimalZstdContract(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	statements := []string{
		`CREATE TABLE analytics (
			record_id VARCHAR NOT NULL,
			attrs STRUCT(namespace VARCHAR, path VARCHAR, value_type VARCHAR, integer_value DECIMAL(38,0))[] NOT NULL
		)`,
		`INSERT INTO analytics VALUES (
			'r1',
			[{'namespace': 'attributes', 'path': '/large', 'value_type': 'integer', 'integer_value': 12345678901234567890123456789012345678::DECIMAL(38,0)}]
		)`,
		`INSERT INTO analytics
		 SELECT 'generated-' || i::VARCHAR, []
		 FROM range(40000) AS generated(i)`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}

	parquetPath := filepath.Join(t.TempDir(), "analytics.parquet")
	quotedPath := strings.ReplaceAll(parquetPath, "'", "''")
	if _, err := db.ExecContext(ctx, `COPY analytics TO '`+quotedPath+`' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 16384)`); err != nil {
		t.Fatal(err)
	}

	var decimal string
	if err := db.QueryRowContext(ctx, `SELECT attrs[1].integer_value::VARCHAR FROM read_parquet([?]) WHERE record_id = 'r1'`, parquetPath).Scan(&decimal); err != nil {
		t.Fatalf("bound read_parquet list: %v", err)
	}
	if decimal != "12345678901234567890123456789012345678" {
		t.Fatalf("decimal round trip = %q", decimal)
	}

	var compression string
	var rowGroups int
	var largestRowGroup int
	if err := db.QueryRowContext(ctx, `
		SELECT min(compression), count(DISTINCT row_group_id), max(row_group_num_rows)
		FROM parquet_metadata(?)`, parquetPath).Scan(&compression, &rowGroups, &largestRowGroup); err != nil {
		t.Fatal(err)
	}
	if compression != "ZSTD" || rowGroups != 3 || largestRowGroup > 16384 {
		t.Fatalf("compression/row groups/largest = %q/%d/%d, want ZSTD/3/<=16384", compression, rowGroups, largestRowGroup)
	}
}
