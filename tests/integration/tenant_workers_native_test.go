//go:build duckdb_use_static_lib

package integration

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// Start real worker processes without pending work, then pause each only after
// its native scheduling loop has made an idle query sweep. This excludes image
// startup skew from the finite queued-work fixture, not from service SLOs.
// No test dispatcher, product failpoint or altered admission policy is used.
func startPausedTenantWorkers(t *testing.T, ctx context.Context, fixture *acceptFixture, prefix string, count int) ([]*maintenanceRuntime, func()) {
	t.Helper()
	if count != 2 && count != 4 {
		t.Fatal("tenant worker fixture requires two or four actual processes")
	}
	workers := make([]*maintenanceRuntime, 0, count)
	for range count {
		workers = append(workers, startMaintenanceRuntime(t, ctx, fixture, prefix))
	}
	var once sync.Once
	resume := func() {
		once.Do(func() {
			for _, worker := range workers {
				if err := worker.command.Process.Signal(syscall.SIGCONT); err != nil {
					t.Errorf("resume actual tenant worker: %v", err)
				}
			}
		})
	}
	// Registered after every process's stop cleanup: a fixture failure always
	// resumes before TERM/join, otherwise a paused process cannot handle TERM.
	t.Cleanup(resume)
	transport := &http.Transport{MaxIdleConns: count, MaxIdleConnsPerHost: 1, MaxConnsPerHost: 1, IdleConnTimeout: time.Second}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: time.Second}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for _, worker := range workers {
		for {
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, worker.metrics, nil)
			if err != nil {
				t.Fatal(err)
			}
			response, err := client.Do(request)
			ready := false
			if err == nil {
				body, readErr := io.ReadAll(io.LimitReader(response.Body, (64<<10)+1))
				response.Body.Close()
				if readErr != nil || response.StatusCode != http.StatusOK || len(body) > 64<<10 {
					t.Fatalf("invalid bounded worker readiness response: status=%d bytes=%d error=%v", response.StatusCode, len(body), readErr)
				}
				for _, line := range strings.Split(string(body), "\n") {
					fields := strings.Fields(line)
					if len(fields) == 2 && fields[0] == `eventglass_operation_calls_total{operation="query"}` {
						calls, parseErr := strconv.ParseUint(fields[1], 10, 64)
						if parseErr != nil {
							t.Fatal(parseErr)
						}
						ready = calls > 0
					}
				}
			}
			if ready {
				if err := worker.command.Process.Signal(syscall.SIGSTOP); err != nil {
					t.Fatal(err)
				}
				break
			}
			select {
			case <-ctx.Done():
				t.Fatalf("worker did not reach idle scheduling: %v", ctx.Err())
			case <-worker.done:
				t.Fatalf("worker exited before queued-work fixture: %v", worker.err)
			case <-ticker.C:
			}
		}
	}
	return workers, resume
}

// Both tenants have pending work at resume. Each initially unprimed worker may
// pick A once; a second A from that worker must not hide untouched B. We do not
// assert strict global alternation: live occupancy and completion times differ.
func checkMultipleWorkerTenantTurns(t *testing.T, workers int, turns []int64, first, second int64, firstCount, secondCount int, owners int) {
	t.Helper()
	counts := map[int64]int{first: 0, second: 0}
	firstSecond, maxSkew := 0, 0
	for index, tenant := range turns {
		if tenant != first && tenant != second {
			t.Fatalf("unexpected tenant in bounded fixture: %d", tenant)
		}
		counts[tenant]++
		if tenant == second && firstSecond == 0 {
			firstSecond = index + 1
		}
		if counts[first] < firstCount && counts[second] < secondCount {
			difference := counts[first] - counts[second]
			if difference < 0 {
				difference = -difference
			}
			maxSkew = max(maxSkew, difference)
		}
	}
	t.Logf("actual processes=%d claiming_owners=%d claims_A=%d claims_B=%d B_first_turn=%d maximum_prefix_count_skew_while_both_pending=%d order=%v", workers, owners, counts[first], counts[second], firstSecond, maxSkew, turns)
	if len(turns) != firstCount+secondCount || counts[first] != firstCount || counts[second] != secondCount {
		t.Fatal("unexpected retries or missing actual tenant work")
	}
	if owners != workers {
		t.Fatalf("requested %d replicas but only %d claimed work", workers, owners)
	}
	if firstSecond == 0 || firstSecond > workers+1 {
		t.Fatal("untouched ready tenant was hidden beyond one initial turn per worker")
	}
}
