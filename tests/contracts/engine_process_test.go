package contracts

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

type childResponse struct {
	DuckDBVersion string `json:"duckdb_version"`
}

func eventglassBinary(t *testing.T) string {
	t.Helper()
	binary := os.Getenv("EVENTGLASS_TEST_BINARY")
	if binary == "" {
		t.Skip("EVENTGLASS_TEST_BINARY is set by ./scripts/check contracts")
	}
	return binary
}

func TestEngineChildProbeAndForcedKill(t *testing.T) {
	binary := eventglassBinary(t)
	probe := exec.Command(binary, "engine-child")
	probe.Stdin = strings.NewReader(`{"operation":"probe"}`)
	var output bytes.Buffer
	probe.Stdout = &output
	if err := probe.Run(); err != nil {
		t.Fatal(err)
	}
	var response childResponse
	if err := json.Unmarshal(output.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(response.DuckDBVersion, "v2.0.0-dev84020") {
		t.Fatalf("DuckDB version = %q", response.DuckDBVersion)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	blocked := exec.CommandContext(ctx, binary, "engine-child")
	stdin, err := blocked.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	if err := blocked.Start(); err != nil {
		t.Fatal(err)
	}
	if err := blocked.Wait(); err == nil || ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("forced child kill = %v, context = %v", err, ctx.Err())
	}

	probe = exec.Command(binary, "engine-child")
	probe.Stdin = strings.NewReader(`{"operation":"probe"}`)
	if err := probe.Run(); err != nil {
		t.Fatalf("a forced child kill damaged the supervisor process: %v", err)
	}
}
