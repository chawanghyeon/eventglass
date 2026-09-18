package engine

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDuckDBSpillsUnderNativeMemoryLimit(t *testing.T) {
	db, err := Open(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tempDirectory := filepath.Join(t.TempDir(), "spill")
	if err := os.Mkdir(tempDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	quoted := strings.ReplaceAll(tempDirectory, "'", "''")
	profilePath := filepath.Join(t.TempDir(), "profile.json")
	quotedProfile := strings.ReplaceAll(profilePath, "'", "''")
	for _, statement := range []string{
		"SET memory_limit = '64MB'",
		"SET max_temp_directory_size = '512MB'",
		"SET temp_directory = '" + quoted + "'",
		"SET preserve_insertion_order = false",
		"PRAGMA enable_profiling = 'json'",
		"PRAGMA profiling_output = '" + quotedProfile + "'",
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, `SELECT i, lpad(i::VARCHAR, 100, 'x') FROM range(2000000) t(i) ORDER BY hash(i)`)
	if err != nil {
		t.Fatal(err)
	}
	var count int
	for rows.Next() {
		var number int64
		var payload string
		if err := rows.Scan(&number, &payload); err != nil {
			t.Fatal(err)
		}
		count++
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 2000000 {
		t.Fatalf("row count = %d", count)
	}
	profileBytes, err := os.ReadFile(profilePath)
	if err != nil {
		t.Fatal(err)
	}
	var profile struct {
		IO struct {
			TotalBytesWritten int64 `json:"total_bytes_written"`
		} `json:"io"`
		System struct {
			PeakTempDirectorySize int64 `json:"peak_temp_dir_size"`
			PeakBufferMemory      int64 `json:"peak_buffer_memory"`
		} `json:"system"`
	}
	if err := json.Unmarshal(profileBytes, &profile); err != nil {
		t.Fatal(err)
	}
	if profile.System.PeakTempDirectorySize == 0 || profile.IO.TotalBytesWritten == 0 {
		t.Fatalf("DuckDB profile reports no spill: %+v", profile)
	}
	if profile.System.PeakBufferMemory > 64<<20 {
		t.Fatalf("DuckDB exceeded its native memory limit: %+v", profile.System)
	}
}
