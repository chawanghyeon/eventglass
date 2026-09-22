//go:build duckdb_use_static_lib

package integration

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/app"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

// This harness launches the actual product, not a substituted dispatcher or
// runner. Its native descendants share the resource runner's cgroup.
type maintenanceRuntime struct {
	t       *testing.T
	command *exec.Cmd
	done    chan struct{}
	err     error
	once    sync.Once
	log     maintenanceRuntimeLog
	metrics string
}

type maintenanceRuntimeLog struct {
	sync.Mutex
	bytes []byte
}

func (log *maintenanceRuntimeLog) Write(value []byte) (int, error) {
	log.Lock()
	defer log.Unlock()
	if remaining := (64 << 10) - len(log.bytes); remaining > 0 {
		log.bytes = append(log.bytes, value[:min(len(value), remaining)]...)
	}
	return len(value), nil
}

func startMaintenanceRuntime(t *testing.T, ctx context.Context, fixture *acceptFixture, prefix string) *maintenanceRuntime {
	t.Helper()
	env := requiredEnvironment(t, "EVENTGLASS_TEST_BINARY", "EVENTGLASS_S3_ENDPOINT", "EVENTGLASS_S3_BUCKET", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")
	config := storage.S3Config{Endpoint: env["EVENTGLASS_S3_ENDPOINT"], Region: "us-east-1", Bucket: env["EVENTGLASS_S3_BUCKET"], Prefix: prefix, PathStyle: true}
	identity, err := app.StorageIdentity(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE installations SET storage_identity=$1,alerts_paused=true WHERE singleton`, identity); err != nil {
		t.Fatal(err)
	}
	marker, key, _, err := app.InstallationMarker(acceptInstallationID, identity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := integrationStore(t, prefix).Put(ctx, key, marker); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	worker := &maintenanceRuntime{t: t, done: make(chan struct{}), metrics: "http://" + address + "/metrics"}
	worker.command = exec.Command(env["EVENTGLASS_TEST_BINARY"], "run")
	worker.command.Env = []string{
		"GOMEMLIMIT=96MiB", "GOGC=100", "GOMAXPROCS=1",
		"EVENTGLASS_DATABASE_URL=" + fixture.pool.Config().ConnString(),
		"EVENTGLASS_ROLES=worker", "EVENTGLASS_HTTP_ADDR=127.0.0.1:0",
		"EVENTGLASS_PUBLIC_URL=http://127.0.0.1", "EVENTGLASS_METRICS_ADDR=" + address,
		"EVENTGLASS_SCRATCH_DIR=" + filepath.Join(t.TempDir(), "runtime"),
		"EVENTGLASS_ALERT_ENCRYPTION_KEY_FILE=" + authHashKeyFile(t),
		"EVENTGLASS_S3_ENDPOINT=" + config.Endpoint, "EVENTGLASS_S3_REGION=" + config.Region,
		"EVENTGLASS_S3_BUCKET=" + config.Bucket, "EVENTGLASS_S3_PREFIX=" + prefix,
		"AWS_ACCESS_KEY_ID=" + env["AWS_ACCESS_KEY_ID"], "AWS_SECRET_ACCESS_KEY=" + env["AWS_SECRET_ACCESS_KEY"],
	}
	worker.command.Stdout, worker.command.Stderr = &worker.log, &worker.log
	worker.command.WaitDelay = time.Second
	if err := worker.command.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { worker.err = worker.command.Wait(); close(worker.done) }()
	// Register after the scratch/key cleanup so the runtime always joins first.
	t.Cleanup(worker.stop)
	return worker
}

func (worker *maintenanceRuntime) stop() {
	worker.once.Do(func() {
		select {
		case <-worker.done:
		default:
			if err := worker.command.Process.Signal(syscall.SIGTERM); err != nil && err != os.ErrProcessDone {
				worker.t.Errorf("stop maintenance runtime: %v", err)
			}
			// Never remove scratch or release the cgroup while descendants run.
			// The outer bounded test timeout/container teardown contains a hung
			// shutdown; it stops the container before deleting its owned volume.
			<-worker.done
		}
		if worker.err != nil {
			worker.t.Errorf("maintenance runtime exit: %v", worker.err)
		}
		worker.log.Lock()
		defer worker.log.Unlock()
		worker.t.Logf("joined maintenance runtime log (at most64KiB):\n%s", worker.log.bytes)
	})
}

func (worker *maintenanceRuntime) awaitTask(t *testing.T, ctx context.Context, fixture *acceptFixture, taskID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	started := time.Now()
	for {
		var state string
		var attempt int
		if err := fixture.pool.QueryRow(ctx, `SELECT state,attempt FROM maintenance_tasks WHERE task_id=$1`, taskID).Scan(&state, &attempt); err != nil {
			t.Fatal(err)
		}
		if state == "completed" {
			t.Logf("real dispatcher completed task=%s attempts=%d wall=%s", taskID, attempt, time.Since(started))
			return
		}
		if state == "failed" {
			t.Fatalf("real dispatcher failed task=%s attempts=%d", taskID, attempt)
		}
		select {
		case <-worker.done:
			t.Fatalf("worker stopped before completion: %v", worker.err)
		case <-ctx.Done():
			t.Fatalf("dispatcher did not finish: state=%s attempts=%d elapsed=%s: %v", state, attempt, time.Since(started), ctx.Err())
		case <-ticker.C:
		}
	}
}

func (worker *maintenanceRuntime) verifyBudget(t *testing.T) {
	t.Helper()
	response, err := (&http.Client{Timeout: 3 * time.Second}).Get(worker.metrics)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 64<<10+1))
	response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK || len(body) > 64<<10 {
		t.Fatalf("bounded runtime metrics: status=%d bytes=%d err=%v", response.StatusCode, len(body), err)
	}
	values := map[string]int64{}
	for _, line := range strings.Split(string(body), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || !strings.HasPrefix(fields[0], "eventglass_operation_") {
			continue
		}
		value, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil || value < 0 {
			t.Fatalf("invalid counter: %s", line)
		}
		values[fields[0]] = value
	}
	metric := func(name, operation string) int64 {
		return values[fmt.Sprintf("eventglass_operation_%s{operation=%q}", name, operation)]
	}
	idle := metric("work_ms_total", "maintenance_spare")
	var spent int64
	for _, operation := range []string{"retention", "compaction", "gc"} {
		if metric("calls_total", operation) == 0 {
			t.Fatalf("missing real dispatch observations: %s", operation)
		}
		spent += metric("elapsed_ms_total", operation)
	}
	if idle == 0 || spent > idle/4+5 || metric("calls_total", "maintenance_budget_overrun") != 0 {
		t.Fatalf("maintenance budget: idle=%dms all_attempts=%dms overrun=%d", idle, spent, metric("calls_total", "maintenance_budget_overrun"))
	}
	if metric("failures_total", "compaction") == 0 {
		t.Fatal("maximum compaction did not exercise budget-canceled retry from a fresh worker")
	}
	t.Logf("real dispatcher budget idle=%dms all_attempts=%dms overrun=0; metrics include worker S3 separately from fixture/oracle I/O:\n%s", idle, spent, body)
}
