package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestChildProbe(t *testing.T) {
	var output bytes.Buffer
	if err := RunChild(context.Background(), strings.NewReader(`{"operation":"probe"}`), &output); err != nil {
		t.Fatal(err)
	}
	var response ChildResponse
	if err := json.Unmarshal(output.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(response.DuckDBVersion, "v2.0.0-dev84020") {
		t.Fatalf("unexpected DuckDB version %q", response.DuckDBVersion)
	}
}

func TestDuckDBCancellation(t *testing.T) {
	db, err := Open(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SELECT sum(i) FROM range(1000000000000) t(i)")
	if err == nil {
		defer rows.Close()
		for rows.Next() {
		}
		err = rows.Err()
	}
	if err == nil || !IsCanceled(err) {
		t.Fatalf("expected a context cancellation, got %v", err)
	}
}
