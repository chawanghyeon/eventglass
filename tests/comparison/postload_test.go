//go:build comparison

package comparison

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type phaseIO struct {
	PUT, HEAD, LIST, GET, RangeGET uint64
	PUTBytes, GETBytes, RangeBytes uint64
}

type postLoadEvidence struct {
	Complete                                bool
	CacheScope                              string
	ReplacedWorkers                         int
	StartUS, EndUS                          int64
	ColdMS, WarmMS                          int64
	ColdIO, WarmIO, IdleIO                  phaseIO
	Rows, Objects                           int64
	RowsSHA256                              string
	ColdCacheBytes, WarmCacheBytes          int64
	SameSnapshotAndRows                     bool
	IdleSeconds                             int64
	IdleQueryJobsBefore, IdleQueryJobsAfter int64
	IdleQueryWork                           uint64
	IdleBacklog                             int64
	PGWALBytes                              int64
	Operations                              map[string]comparisonOperation
}

type regexResult struct {
	SnapshotID string            `json:"snapshot_id"`
	ReadToken  string            `json:"read_token"`
	Complete   bool              `json:"complete"`
	Rows       []json.RawMessage `json:"rows"`
	Stats      struct {
		Objects    string `json:"objects"`
		CacheBytes string `json:"cache_bytes"`
	} `json:"stats"`
}

// This phase follows a runner-verified replacement of every query worker and
// its tmpfs, not a product flag that purports to clear cache. PG/S3 are retained.
func TestPostLoadComparison(t *testing.T) {
	env := requiredComparisonEnv(t, "EVENTGLASS_COMPARISON_BASE_URL", "EVENTGLASS_COMPARISON_STATE", "EVENTGLASS_COMPARISON_REPORT", "EVENTGLASS_DATABASE_URL", "EVENTGLASS_COMPARISON_COLD_WORKERS")
	var report comparisonReport
	data, err := os.ReadFile(env[2])
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	phase := &postLoadEvidence{CacheScope: "empty per-worker Eventglass block caches; provider/OS cache not cleared"}
	report.PostLoad = phase
	for _, target := range []string{"cold_all_history_regex", "warm_same_snapshot_rows", "idle_no_query_work", "idle_backlog_drained", "postload_maintenance_time_accounted", "postload_complete"} {
		report.Targets[target] = false
	}
	defer func() { writeReport(t, env[2], report) }()
	workers, err := os.ReadFile(env[4])
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyWorkerReplacements(string(workers), report.Workers); err != nil {
		t.Fatal(err)
	}
	phase.ReplacedWorkers = report.Workers
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, env[3])
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	state := readComparisonState(t, env[1])
	client := newComparisonClient(t)
	client.Timeout = 35 * time.Second // public sync deadline remains30s.
	var session sessionWire
	doJSON(t, client, http.MethodPost, env[0]+"/v1/sessions", map[string]any{"email": state.Email, "password": state.Password}, "", http.StatusOK, &session)
	// The retained installation contains only this run's workload. Include its
	// entire received-time range, including records older than the last15min.
	databaseNow, _, err := comparisonDatabaseClock(ctx, pool)
	if err != nil {
		t.Fatalf("sample comparison post-load database time: %v", err)
	}
	phase.StartUS, phase.EndUS = report.StartedAt.Add(-time.Minute).UnixMicro(), databaseNow.Add(time.Minute).UnixMicro()
	walStart := pgWAL(t, pool)
	var before, coldAfter, warmAfter, idleAfter comparisonReport
	for _, endpoint := range strings.Split(os.Getenv("EVENTGLASS_COMPARISON_METRICS"), ",") {
		waitHTTP(t, client, strings.TrimSpace(endpoint)+"/metrics", 30*time.Second)
	}
	collectS3Metrics(t, client, &before)
	if before.Operations["query"].Work != 0 || before.S3RangeRequests != 0 {
		t.Fatal("new workers performed query/Range work before cold measurement")
	}
	started := time.Now()
	cold, err := allHistoryRegex(ctx, client, env[0], state, session.CSRFToken, phase.StartUS, phase.EndUS, "")
	phase.ColdMS = time.Since(started).Milliseconds()
	if err != nil {
		t.Fatal(err)
	}
	snapshotReleased := false
	defer func() {
		if !snapshotReleased {
			if err := releaseComparisonSnapshot(ctx, client, env[0], state.TenantID, cold.SnapshotID, session.CSRFToken); err != nil {
				t.Error(err)
			}
		}
	}()
	collectS3Metrics(t, client, &coldAfter)
	phase.ColdIO, err = ioDelta(before, coldAfter)
	if err != nil {
		t.Fatal(err)
	}
	phase.Rows, phase.Objects = int64(len(cold.Rows)), parseNonnegative(t, cold.Stats.Objects)
	phase.ColdCacheBytes = parseNonnegative(t, cold.Stats.CacheBytes)
	phase.RowsSHA256 = regexRowsHash(cold.Rows)
	report.Targets["cold_all_history_regex"] = cold.Complete && len(cold.Rows) > 0 && phase.ColdIO.RangeGET > 0 && phase.Objects > 0
	started = time.Now()
	warm, err := allHistoryRegex(ctx, client, env[0], state, session.CSRFToken, phase.StartUS, phase.EndUS, cold.ReadToken)
	phase.WarmMS = time.Since(started).Milliseconds()
	if err != nil {
		t.Fatal(err)
	}
	collectS3Metrics(t, client, &warmAfter)
	phase.WarmIO, err = ioDelta(coldAfter, warmAfter)
	if err != nil {
		t.Fatal(err)
	}
	phase.WarmCacheBytes = parseNonnegative(t, warm.Stats.CacheBytes)
	phase.SameSnapshotAndRows = warm.Complete && warm.SnapshotID == cold.SnapshotID && regexRowsHash(warm.Rows) == phase.RowsSHA256
	report.Targets["warm_same_snapshot_rows"] = phase.SameSnapshotAndRows
	if err := releaseComparisonSnapshot(ctx, client, env[0], state.TenantID, cold.SnapshotID, session.CSRFToken); err != nil {
		t.Fatal(err)
	}
	snapshotReleased = true
	// Cache placement may differ across multiple workers. Report actual warm
	// Range misses instead of assuming affinity or requiring a fictitious zero.
	phase.IdleSeconds = 60
	if os.Getenv("EVENTGLASS_COMPARISON_QUICK") == "1" {
		phase.IdleSeconds = 10
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM query_jobs`).Scan(&phase.IdleQueryJobsBefore); err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(time.Duration(phase.IdleSeconds) * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-timer.C:
	}
	collectS3Metrics(t, client, &idleAfter)
	// Retain the complete new-worker lifetime, not only deltas after the first
	// scrape: startup maintenance can already have overrun before that scrape.
	phase.Operations = idleAfter.Operations
	report.Targets["postload_maintenance_time_accounted"] = maintenanceTimeAccounted(phase.Operations)
	phase.IdleIO, err = ioDelta(warmAfter, idleAfter)
	if err != nil {
		t.Fatal(err)
	}
	if idleAfter.Operations["query"].Work < warmAfter.Operations["query"].Work {
		t.Fatal("query counters reset during idle")
	}
	phase.IdleQueryWork = idleAfter.Operations["query"].Work - warmAfter.Operations["query"].Work
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM query_jobs`).Scan(&phase.IdleQueryJobsAfter); err != nil {
		t.Fatal(err)
	}
	_, phase.IdleBacklog = drainBacklog(t, pool, time.Nanosecond)
	phase.PGWALBytes = pgWAL(t, pool) - walStart
	report.Targets["idle_no_query_work"] = idleNoQueryWork(phase.IdleQueryJobsBefore, phase.IdleQueryJobsAfter, phase.IdleQueryWork)
	report.Targets["idle_backlog_drained"] = phase.IdleBacklog == 0
	phase.Complete = true
	report.Targets["postload_complete"] = true
	for _, target := range []string{"cold_all_history_regex", "warm_same_snapshot_rows", "idle_no_query_work", "idle_backlog_drained", "postload_maintenance_time_accounted"} {
		if !report.Targets[target] {
			t.Errorf("post-load target failed: %s", target)
		}
	}
	t.Logf("post-load cold=%dms warm=%dms ranges=%d/%d bytes=%d/%d idle=%ds new_queries=%d WAL=%d", phase.ColdMS, phase.WarmMS, phase.ColdIO.RangeGET, phase.WarmIO.RangeGET, phase.ColdIO.RangeBytes, phase.WarmIO.RangeBytes, phase.IdleSeconds, phase.IdleQueryJobsAfter-phase.IdleQueryJobsBefore, phase.PGWALBytes)
}

func idleNoQueryWork(jobsBefore, jobsAfter int64, work uint64) bool {
	return jobsBefore == jobsAfter && work == 0
}

func TestIdleNoQueryWorkIsIndependentOfIngestBacklog(t *testing.T) {
	if !idleNoQueryWork(362, 362, 0) {
		t.Fatal("unchanged query jobs with zero query work did not pass")
	}
	if idleNoQueryWork(362, 363, 0) || idleNoQueryWork(362, 362, 1) {
		t.Fatal("new query work passed the idle query target")
	}
}

func verifyWorkerReplacements(data string, expected int) error {
	seen := make(map[string]bool)
	identities := make(map[string]bool)
	for _, line := range strings.Split(strings.TrimSpace(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 || len(fields[1]) != 64 || len(fields[2]) != 64 || fields[1] == fields[2] || seen[fields[0]] {
			return errors.New("missing/invalid worker replacement evidence")
		}
		for _, identity := range fields[1:] {
			if _, err := hex.DecodeString(identity); err != nil || identities[identity] {
				return errors.New("invalid/duplicate worker identity")
			}
			identities[identity] = true
		}
		seen[fields[0]] = true
	}
	if expected < 1 || len(seen) != expected {
		return errors.New("not every worker cache was replaced")
	}
	return nil
}

func allHistoryRegex(ctx context.Context, client *http.Client, baseURL string, state comparisonState, csrf string, start, end int64, token string) (regexResult, error) {
	body := map[string]any{"tenant_id": strconv.FormatInt(state.TenantID, 10), "project_ids": []string{strconv.FormatInt(state.ProjectID, 10)}, "start_us": strconv.FormatInt(start, 10), "end_us": strconv.FormatInt(end, 10), "time_basis": "received", "kinds": []string{"log", "error"}, "expression": `matches(message,"comparison.*(log|error)")`, "limit": 100, "sort": "received_desc", "mode": "sync", "projection": "list"}
	if token != "" {
		body["read_token"] = token
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return regexResult{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/search", bytes.NewReader(encoded))
	if err != nil {
		return regexResult{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Origin", comparisonOrigin)
	request.Header.Set("X-CSRF-Token", csrf)
	response, err := client.Do(request)
	if err != nil {
		return regexResult{}, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
	if err != nil {
		return regexResult{}, err
	}
	if response.StatusCode != http.StatusOK {
		return regexResult{}, fmt.Errorf("search status=%d body=%s", response.StatusCode, data)
	}
	var result regexResult
	if err := json.Unmarshal(data, &result); err != nil {
		return result, err
	}
	if result.SnapshotID == "" || result.ReadToken == "" || !result.Complete {
		if result.SnapshotID != "" {
			return result, errors.Join(errors.New("incomplete regex snapshot result"), releaseComparisonSnapshot(ctx, client, baseURL, state.TenantID, result.SnapshotID, csrf))
		}
		return result, errors.New("incomplete regex snapshot result")
	}
	return result, nil
}

func regexRowsHash(rows []json.RawMessage) string {
	data, _ := json.Marshal(rows)
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

func parseNonnegative(t *testing.T, value string) int64 {
	t.Helper()
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 {
		t.Fatalf("invalid measurement %q", value)
	}
	return parsed
}

func ioDelta(before, after comparisonReport) (phaseIO, error) {
	if after.S3PutRequests < before.S3PutRequests || after.S3HeadRequests < before.S3HeadRequests || after.S3ListRequests < before.S3ListRequests || after.S3GetRequests < before.S3GetRequests || after.S3RangeRequests < before.S3RangeRequests || after.S3PutBytes < before.S3PutBytes || after.S3GetBytes < before.S3GetBytes || after.S3RangeBytes < before.S3RangeBytes {
		return phaseIO{}, errors.New("S3 counters reset during measured phase")
	}
	return phaseIO{PUT: after.S3PutRequests - before.S3PutRequests, HEAD: after.S3HeadRequests - before.S3HeadRequests, LIST: after.S3ListRequests - before.S3ListRequests, GET: after.S3GetRequests - before.S3GetRequests, RangeGET: after.S3RangeRequests - before.S3RangeRequests, PUTBytes: after.S3PutBytes - before.S3PutBytes, GETBytes: after.S3GetBytes - before.S3GetBytes, RangeBytes: after.S3RangeBytes - before.S3RangeBytes}, nil
}
