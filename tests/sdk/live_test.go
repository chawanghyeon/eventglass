package sdk_test

import (
	"context"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/api"
	"github.com/chawanghyeon/eventglass/internal/testkit"
)

const liveSentinel = "eventglass-live-scrub-sentinel"

func TestPinnedSDKsSendToLiveGoAPI(t *testing.T) {
	if os.Getenv("EVENTGLASS_SDK_LIVE") != "1" {
		t.Skip("set EVENTGLASS_SDK_LIVE=1 through ./scripts/check sdk")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	tool := filepath.Join(root, "tools", "sdk-fixtures")
	dependencies := []string{
		filepath.Join(tool, ".venv", "bin", "python"),
		filepath.Join(tool, "go-app", "eventglass-go-fixture"),
		filepath.Join(tool, "browser-app", "dist", "app.js"),
	}
	for _, path := range dependencies {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("missing SDK dependency %s: %v", path, err)
		}
	}
	browserPort := reservePort(t)
	sink := &testkit.MemorySink{}
	handler, err := api.NewIngestHandler(api.Config{
		TenantID: 1, ProjectID: 1, PublicKey: "fixturePublicKey", Sink: sink,
		AllowedOrigins: []string{fmt.Sprintf("http://127.0.0.1:%d", browserPort)},
		DefaultService: "fixture", ForbiddenFixtureValue: liveSentinel,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	dsn := strings.Replace(server.URL, "http://", "http://fixturePublicKey@", 1) + "/1"

	type liveCase struct {
		name    string
		command []string
	}
	cases := []liveCase{
		{"browser-events-and-console", []string{"node", filepath.Join(tool, "browser-app", "run.mjs")}},
		{"go-events", []string{filepath.Join(tool, "go-app", "eventglass-go-fixture")}},
		{"node-client-report", []string{"node", filepath.Join(tool, "node-app", "app.mjs"), "client-report"}},
		{"node-events-and-logs", []string{"node", filepath.Join(tool, "node-app", "app.mjs")}},
		{"python-celery-fork", []string{filepath.Join(tool, ".venv", "bin", "python"), filepath.Join(tool, "python-app", "app.py"), "celery-fork"}},
		{"python-events", []string{filepath.Join(tool, ".venv", "bin", "python"), filepath.Join(tool, "python-app", "app.py"), "events"}},
		{"python-fastapi", []string{filepath.Join(tool, ".venv", "bin", "python"), filepath.Join(tool, "python-app", "app.py"), "fastapi"}},
		{"python-logging-debug", []string{filepath.Join(tool, ".venv", "bin", "python"), filepath.Join(tool, "python-app", "app.py"), "logging-debug"}},
		{"python-logging-default", []string{filepath.Join(tool, ".venv", "bin", "python"), filepath.Join(tool, "python-app", "app.py"), "logging-default"}},
	}
	expectedTotal := 0
	for _, live := range cases {
		var expected expectedFile
		readJSON(t, filepath.Join(root, "tests", "fixtures", "sentry", live.name, "expected.normalized.json"), &expected)
		environment := append(os.Environ(),
			"SENTRY_FIXTURE_DSN="+dsn,
			"SENTRY_FIXTURE_SECRET="+liveSentinel,
			"SENTRY_FIXTURE_MODE=live-sequential", // pragma: allowlist secret
			"SENTRY_FIXTURE_BROWSER_PORT="+strconv.Itoa(browserPort),
			"PLAYWRIGHT_BROWSERS_PATH="+filepath.Join(tool, ".playwright-browsers"),
			"NO_PROXY=127.0.0.1,localhost", "no_proxy=127.0.0.1,localhost",
		)
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		command := exec.CommandContext(ctx, live.command[0], live.command[1:]...)
		command.Dir, command.Env = root, withoutProxy(environment)
		output, runErr := command.CombinedOutput()
		cancel()
		if runErr != nil {
			t.Fatalf("%s failed: %v\n%s", live.name, runErr, output)
		}
		expectedTotal += len(expected.Records)
		actual := flattenRecords(sink.Batches())
		if len(actual) != expectedTotal {
			t.Fatalf("%s cumulative records=%d, want %d", live.name, len(actual), expectedTotal)
		}
	}

	records := flattenRecords(sink.Batches())
	errorLevelLogs, issues := 0, 0
	for _, record := range records {
		if strings.Contains(string(record.Raw), liveSentinel) {
			t.Fatalf("scrub sentinel remained in %s", record.RecordID)
		}
		if record.Kind == "log" && (record.Level == "error" || record.Level == "fatal") {
			errorLevelLogs++
		}
		if record.Kind == "error" {
			issues++
		}
	}
	if errorLevelLogs == 0 || issues == 0 {
		t.Fatalf("fixture did not preserve log/error distinction: logs=%d issues=%d", errorLevelLogs, issues)
	}
	outcomes := 0
	for _, batch := range sink.Batches() {
		outcomes += len(batch.Outcomes)
	}
	if outcomes == 0 {
		t.Fatal("live Node SDK did not deliver a client report")
	}
}

func TestPinnedNodeSDKHonorsServerRateLimit(t *testing.T) {
	if os.Getenv("EVENTGLASS_SDK_LIVE") != "1" {
		t.Skip("set EVENTGLASS_SDK_LIVE=1 through ./scripts/check sdk")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	tool := filepath.Join(root, "tools", "sdk-fixtures")
	var attempts atomic.Int32
	sink := &testkit.MemorySink{}
	handler, err := api.NewIngestHandler(api.Config{
		TenantID: 1, ProjectID: 1, PublicKey: "fixturePublicKey", Sink: sink,
		RateLimited: func() bool { return attempts.Add(1) == 1 },
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	dsn := strings.Replace(server.URL, "http://", "http://fixturePublicKey@", 1) + "/1"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "node", filepath.Join(tool, "node-app", "app.mjs"))
	command.Dir = root
	command.Env = withoutProxy(append(os.Environ(),
		"SENTRY_FIXTURE_DSN="+dsn,
		"SENTRY_FIXTURE_MODE=live-sequential", // pragma: allowlist secret
		"NO_PROXY=127.0.0.1,localhost", "no_proxy=127.0.0.1,localhost",
	))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("Node rate-limit fixture failed: %v\n%s", err, output)
	}
	if attempts.Load() != 1 {
		t.Fatalf("Node sent %d HTTP requests after an all-category rate limit; want 1", attempts.Load())
	}
	if len(sink.Batches()) != 0 {
		t.Fatal("rate-limited SDK data reached the sink")
	}
}

func reservePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func withoutProxy(environment []string) []string {
	result := make([]string, 0, len(environment))
	for _, value := range environment {
		key, _, _ := strings.Cut(value, "=")
		switch strings.ToUpper(key) {
		case "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY":
			continue
		}
		result = append(result, value)
	}
	return result
}
