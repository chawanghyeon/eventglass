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
	"net/http/cookiejar"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	comparisonOrigin = "http://127.0.0.1:8080"
	logsPerSecond    = 100
	errorsPerSecond  = 5
)

type comparisonState struct {
	Email, Password, PublicKey string
	TenantID, ProjectID        int64
}

type comparisonReport struct {
	QueryMeasurements                               []queryMeasurementEvidence `json:",omitempty"`
	LoadEndPartitions                               []partitionEvidence        `json:",omitempty"`
	LoadEndCompactions                              []compactionEvidence       `json:",omitempty"`
	PostLoad                                        *postLoadEvidence          `json:",omitempty"`
	Operations                                      map[string]comparisonOperation
	Revision, Architecture, FixtureDefinitionSHA256 string
	SubmittedInput                                  submittedInput
	PublishedCounts                                 map[string]int64
	PublishedOracleFailure                          string
	PublishedOracleMS                               int64
	Workers                                         int
	WarmupSeconds, LoadSeconds                      int64
	LogicalLogs, LogicalErrors                      int64
	Accepted, Duplicates, Conflicts                 int64
	ACKSamples, QuerySamples                        int
	VisibilitySamples                               int
	RowsQuerySamples, HistogramQuerySamples         int
	QueryFailures                                   int
	QueryObjectsFirst, QueryObjectsLast             int64
	QueryObjectsMax, QueryScannedBytesMax           int64
	QueryFailureCodes                               map[string]int
	QueryJobStates                                  map[string]int
	ACKP50MS, ACKP95MS, ACKP99MS                    int64
	QueryP50MS, QueryP95MS, QueryP99MS              int64
	RowsQueryP95MS, HistogramQueryP95MS             int64
	RowsServerP95MS, HistogramServerP95MS           int64
	RowsOverheadP95MS, HistogramOverheadP95MS       int64
	VisibilityP95MS                                 int64
	PostDrainLast15MinRegexMS                       int64
	PostDrainLast15MinRegexFailed                   bool
	MaxConversionBacklog, FinalBacklog              int64
	LoadBacklogSamples, LoadBacklogMax              int64
	LoadBacklogSlopePerMinute                       float64
	DrainMS                                         int64
	PGDatabaseStartBytes, PGDatabaseEndBytes        int64
	PGWALBytes                                      int64
	S3Objects, S3StoredBytes                        int64
	S3PutRequests, S3HeadRequests                   uint64
	S3GetRequests, S3RangeRequests                  uint64
	S3PutBytes, S3GetBytes, S3RangeBytes            uint64
	StartedAt, FinishedAt                           time.Time
	Targets                                         map[string]bool
}

type comparisonOperation struct {
	Calls, Work, Failures uint64
	ElapsedMS, WorkMS     uint64
}

type sessionWire struct {
	CSRFToken string `json:"csrf_token"`
	Tenants   []struct {
		TenantID string `json:"tenant_id"`
	} `json:"tenants"`
}

func TestPrepareComparisonInstallation(t *testing.T) {
	environment := requiredComparisonEnv(t, "EVENTGLASS_COMPARISON_BASE_URL", "EVENTGLASS_COMPARISON_STATE", "EVENTGLASS_COMPARISON_BOOTSTRAP_TOKEN")
	baseURL, statePath := environment[0], environment[1]
	client := newComparisonClient(t)
	waitHTTP(t, client, baseURL+"/livez", 60*time.Second)
	var session sessionWire
	doJSON(t, client, http.MethodPost, baseURL+"/v1/setup", map[string]any{
		"bootstrap_token": os.Getenv("EVENTGLASS_COMPARISON_BOOTSTRAP_TOKEN"),
		"email":           "comparison-admin@example.invalid", "password": "comparison-local-password-2026", "tenant_name": "R3 comparison",
	}, "", http.StatusCreated, &session)
	if len(session.Tenants) != 1 {
		t.Fatalf("setup tenants=%d", len(session.Tenants))
	}
	tenantID := parsePositive(t, session.Tenants[0].TenantID)
	var project struct {
		ProjectID string `json:"project_id"`
	}
	doJSON(t, client, http.MethodPost, baseURL+"/v1/projects", map[string]any{
		"tenant_id": strconv.FormatInt(tenantID, 10), "name": "R3 load", "default_service": "comparison", "allowed_origins": []string{comparisonOrigin},
	}, session.CSRFToken, http.StatusCreated, &project)
	projectID := parsePositive(t, project.ProjectID)
	var key struct {
		PublicKey string `json:"public_key"`
	}
	doJSON(t, client, http.MethodPost, fmt.Sprintf("%s/v1/projects/%d/keys", baseURL, projectID), map[string]any{
		"tenant_id": strconv.FormatInt(tenantID, 10), "label": "R3 load",
	}, session.CSRFToken, http.StatusCreated, &key)
	state := comparisonState{Email: "comparison-admin@example.invalid", Password: "comparison-local-password-2026", PublicKey: key.PublicKey, TenantID: tenantID, ProjectID: projectID}
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Clean(statePath), encoded, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestSustainedComparison(t *testing.T) {
	environment := requiredComparisonEnv(t,
		"EVENTGLASS_COMPARISON_BASE_URL", "EVENTGLASS_COMPARISON_STATE", "EVENTGLASS_COMPARISON_REPORT", "EVENTGLASS_DATABASE_URL")
	baseURL, statePath, reportPath, databaseURL := environment[0], environment[1], environment[2], environment[3]
	workers, err := strconv.Atoi(os.Getenv("EVENTGLASS_COMPARISON_WORKERS"))
	if err != nil || (workers != 1 && workers != 2 && workers != 4) {
		t.Fatal("EVENTGLASS_COMPARISON_WORKERS must be 1, 2, or 4")
	}
	warmup := comparisonDuration(t, "EVENTGLASS_COMPARISON_WARMUP", 5*time.Minute)
	load := comparisonDuration(t, "EVENTGLASS_COMPARISON_LOAD", 30*time.Minute)
	drain := comparisonDuration(t, "EVENTGLASS_COMPARISON_DRAIN", 10*time.Minute)
	if os.Getenv("EVENTGLASS_COMPARISON_QUICK") != "1" && (warmup != 5*time.Minute || load != 30*time.Minute || drain != 10*time.Minute) {
		t.Fatal("official comparison requires 5m warmup, 30m load, and 10m drain")
	}
	state := readComparisonState(t, statePath)
	client := newComparisonClient(t)
	var session sessionWire
	doJSON(t, client, http.MethodPost, baseURL+"/v1/sessions", map[string]any{"email": state.Email, "password": state.Password}, "", http.StatusOK, &session)
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	report := comparisonReport{Revision: os.Getenv("EVENTGLASS_COMPARISON_REVISION"), Architecture: "linux/arm64", Workers: workers,
		WarmupSeconds: int64(warmup.Seconds()), LoadSeconds: int64(load.Seconds()), StartedAt: time.Now().UTC(), Targets: make(map[string]bool), QueryFailureCodes: make(map[string]int), QueryJobStates: make(map[string]int)}
	report.FixtureDefinitionSHA256 = fixedFixtureSummaries[10_000_000].SHA256
	input := newInputEvidence()
	report.PGDatabaseStartBytes, _, _ = pgSize(t, pool)
	walStart := pgWAL(t, pool)

	var ackLatencies, queryLatencies, rowsQueryLatencies, histogramQueryLatencies, visibilityLatencies []time.Duration
	var rowsServerLatencies, histogramServerLatencies, rowsOverheadLatencies, histogramOverheadLatencies []time.Duration
	var latencyMu sync.Mutex
	queryContext, stopQueries := context.WithCancel(context.Background())
	queryErrors := make(chan error, 1)
	go runMixedQueries(queryContext, client, baseURL, state, session.CSRFToken, warmup, &latencyMu,
		&queryLatencies, &rowsQueryLatencies, &histogramQueryLatencies,
		&rowsServerLatencies, &histogramServerLatencies, &rowsOverheadLatencies, &histogramOverheadLatencies,
		&visibilityLatencies, &report, queryErrors)
	queriesJoined := false
	defer func() {
		stopQueries()
		if !queriesJoined {
			<-queryErrors
		}
	}()

	sequence := int64(0)
	if err := runIngestPhase(client, baseURL, state, warmup, false, &sequence, &ackLatencies, &report, input); err != nil {
		t.Fatal(err)
	}
	backlogContext, stopBacklog := context.WithCancel(context.Background())
	backlogResult := make(chan backlogMonitorResult, 1)
	go monitorBacklog(backlogContext, pool, backlogResult)
	loadStarted := time.Now()
	if err := runIngestPhase(client, baseURL, state, load, true, &sequence, &ackLatencies, &report, input); err != nil {
		stopBacklog()
		<-backlogResult
		t.Fatal(err)
	}
	stopBacklog()
	backlog := <-backlogResult
	if backlog.Err != nil {
		t.Fatal(backlog.Err)
	}
	report.LoadBacklogSamples = int64(len(backlog.Samples))
	for _, value := range backlog.Samples {
		report.LoadBacklogMax = max(report.LoadBacklogMax, value)
	}
	report.LoadBacklogSlopePerMinute = backlogSlopePerMinute(backlog.Samples, 5*time.Second)
	if elapsed := time.Since(loadStarted); elapsed > load+2*time.Second {
		t.Fatalf("load schedule fell behind: elapsed=%s target=%s", elapsed, load)
	}
	stopQueries()
	select {
	case err := <-queryErrors:
		queriesJoined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("mixed query loop did not join")
	}
	if err := collectQueryDiagnostics(context.Background(), pool, state.TenantID, &report); err != nil {
		t.Fatal(err)
	}

	drainStarted := time.Now()
	report.MaxConversionBacklog, report.FinalBacklog = drainBacklog(t, pool, drain)
	report.DrainMS = time.Since(drainStarted).Milliseconds()
	if err := pool.QueryRow(context.Background(), `SELECT COALESCE(sum(accepted_count),0),COALESCE(sum(duplicate_count),0),COALESCE(sum(conflict_count),0) FROM receipts WHERE tenant_id=$1`, state.TenantID).Scan(&report.Accepted, &report.Duplicates, &report.Conflicts); err != nil {
		t.Fatal(err)
	}
	jobStates, err := pool.Query(context.Background(), `SELECT state,COALESCE(error_code,''),count(*) FROM query_jobs WHERE tenant_id=$1 GROUP BY state,error_code ORDER BY state,error_code`, state.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	for jobStates.Next() {
		var state, code string
		var count int
		if err := jobStates.Scan(&state, &code, &count); err != nil {
			jobStates.Close()
			t.Fatal(err)
		}
		if code != "" {
			state += ":" + code
		}
		report.QueryJobStates[state] = count
	}
	if err := jobStates.Err(); err != nil {
		jobStates.Close()
		t.Fatal(err)
	}
	jobStates.Close()

	report.ACKSamples = len(ackLatencies)
	report.QuerySamples = len(queryLatencies)
	report.RowsQuerySamples, report.HistogramQuerySamples = len(rowsQueryLatencies), len(histogramQueryLatencies)
	report.ACKP50MS, report.ACKP95MS, report.ACKP99MS = percentileMS(ackLatencies, .50), percentileMS(ackLatencies, .95), percentileMS(ackLatencies, .99)
	report.QueryP50MS, report.QueryP95MS, report.QueryP99MS = percentileMS(queryLatencies, .50), percentileMS(queryLatencies, .95), percentileMS(queryLatencies, .99)
	report.RowsQueryP95MS, report.HistogramQueryP95MS = percentileMS(rowsQueryLatencies, .95), percentileMS(histogramQueryLatencies, .95)
	report.RowsServerP95MS, report.HistogramServerP95MS = percentileMS(rowsServerLatencies, .95), percentileMS(histogramServerLatencies, .95)
	report.RowsOverheadP95MS, report.HistogramOverheadP95MS = percentileMS(rowsOverheadLatencies, .95), percentileMS(histogramOverheadLatencies, .95)
	report.VisibilitySamples = len(visibilityLatencies)
	report.VisibilityP95MS = percentileMS(visibilityLatencies, .95)
	if measurement, searchErr := search(context.Background(), client, baseURL, state, session.CSRFToken, false, true); searchErr != nil {
		report.PostDrainLast15MinRegexFailed = true
	} else {
		report.PostDrainLast15MinRegexMS = measurement.Total.Milliseconds()
	}
	checkStarted := time.Now()
	report.PublishedCounts, err = publishedCounts(context.Background(), client, baseURL, state, session.CSRFToken, report.StartedAt, checkStarted)
	if err != nil {
		report.PublishedOracleFailure = searchFailureCode(err)
	}
	report.PublishedOracleMS = time.Since(checkStarted).Milliseconds()
	report.SubmittedInput = input.snapshot()
	collectS3Metrics(t, client, &report)
	// Include the post-drain regex and publication oracle in both storage
	// measurements, rather than charging S3 but omitting their PG/WAL work.
	report.PGDatabaseEndBytes, report.S3Objects, report.S3StoredBytes = pgSize(t, pool)
	report.PGWALBytes = pgWAL(t, pool) - walStart
	report.FinishedAt = time.Now().UTC()
	expectedAccepted := int64((warmup+load)/time.Second) * (logsPerSecond + errorsPerSecond)
	setWorkloadTargets(&report, expectedAccepted)
	writeReport(t, reportPath, report)
	for name, passed := range report.Targets {
		if !passed {
			t.Errorf("R3 target failed: %s (full evidence: %s)", name, reportPath)
		}
	}
	t.Logf("R3 workers=%d accepted=%d ack_p95_ms=%d query_p95_ms=%d visible_p95_ms=%d wal_bytes=%d s3_objects=%d s3_bytes=%d", workers, report.Accepted, report.ACKP95MS, report.QueryP95MS, report.VisibilityP95MS, report.PGWALBytes, report.S3Objects, report.S3StoredBytes)
}

func setWorkloadTargets(report *comparisonReport, expectedAccepted int64) {
	report.Targets["measured_ack_samples"] = report.LoadSeconds > 0 && int64(report.ACKSamples) == report.LoadSeconds*(1+errorsPerSecond)
	report.Targets["maintenance_time_accounted"] = maintenanceTimeAccounted(report.Operations)
	compaction := report.Operations["compaction"]
	report.Targets["maintenance_progress"] = compaction.Work > compaction.Failures
	report.Targets["ack_p95_le_500ms"] = report.ACKSamples > 0 && report.ACKP95MS <= 500
	report.Targets["visibility_p95_le_5s"] = report.VisibilitySamples > 0 && report.VisibilityP95MS <= 5_000
	report.Targets["warm_rows_p95_le_500ms"] = report.RowsQuerySamples > 0 && report.RowsQueryP95MS <= 500
	report.Targets["warm_histogram_p95_le_500ms"] = report.HistogramQuerySamples > 0 && report.HistogramQueryP95MS <= 500
	report.Targets["queries_complete"] = report.QueryFailures == 0
	report.Targets["load_backlog_not_growing"] = report.LoadBacklogSamples >= 2 && report.LoadBacklogSlopePerMinute <= .1
	report.Targets["no_growing_backlog"] = report.FinalBacklog == 0
	report.Targets["no_conflicts"] = report.Conflicts == 0
	report.Targets["complete_logical_count"] = report.Accepted == expectedAccepted
	cycles := expectedAccepted / (logsPerSecond + errorsPerSecond)
	report.Targets["published_query_count"] = report.PublishedOracleFailure == "" &&
		report.PublishedCounts["log"] == cycles*logsPerSecond && report.PublishedCounts["error"] == cycles*errorsPerSecond && cycles > 0
	report.Targets["submitted_input_provenance"] = report.SubmittedInput.Envelopes >= cycles*(1+errorsPerSecond)+cycles/60 &&
		report.SubmittedInput.Bytes > 0 && len(report.SubmittedInput.SHA256) == 64
}

func maintenanceTimeAccounted(operations map[string]comparisonOperation) bool {
	spare := operations["maintenance_spare"]
	if spare.Calls == 0 || spare.WorkMS == 0 || operations["maintenance_budget_overrun"].Calls != 0 {
		return false
	}
	var spent uint64
	for _, name := range []string{"retention", "compaction", "gc"} {
		elapsed := operations[name].ElapsedMS
		if elapsed > ^uint64(0)-spent {
			return false
		}
		spent += elapsed
	}
	// Include failed/no-work claims and cancellation joins, not only completed
	// native tasks. This run-wide check supplements the rolling admission tests;
	// it is not a retrospective claim about every sliding-window CPU sample.
	return spent <= spare.WorkMS/4
}

func runIngestPhase(client *http.Client, baseURL string, state comparisonState, duration time.Duration, measured bool, sequence *int64, latencies *[]time.Duration, report *comparisonReport, input *inputEvidence) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	cycles := int(duration / time.Second)
	for range cycles {
		<-ticker.C
		(*sequence)++
		bodies, duplicateBody := comparisonEnvelopes(state, *sequence, time.Now().UTC())
		type answer struct {
			status   int
			response string
			latency  time.Duration
			err      error
		}
		answers := make(chan answer, len(bodies))
		for _, body := range bodies {
			go func(payload []byte) {
				started := time.Now()
				status, response, err := postEnvelopeWithAdmissionRetry(client, baseURL, state, payload, input)
				answers <- answer{status: status, response: response, latency: time.Since(started), err: err}
			}(body)
		}
		var firstErr error
		for range bodies {
			answer := <-answers
			if answer.err != nil || answer.status != http.StatusOK {
				if firstErr == nil {
					firstErr = fmt.Errorf("ingest sequence %d status=%d body=%s err=%v", *sequence, answer.status, answer.response, answer.err)
				}
			}
			if measured {
				*latencies = append(*latencies, answer.latency)
			}
		}
		if firstErr != nil {
			return firstErr
		}
		if measured {
			report.LogicalLogs += logsPerSecond
			report.LogicalErrors += errorsPerSecond
		}
		if *sequence%60 == 0 {
			status, response, err := postEnvelope(client, baseURL, state, duplicateBody, input)
			if err != nil || status != http.StatusOK {
				return fmt.Errorf("duplicate sequence %d status=%d body=%s err=%v", *sequence, status, response, err)
			}
		}
	}
	return nil
}

func comparisonEnvelopes(state comparisonState, sequence int64, now time.Time) ([][]byte, []byte) {
	dsn := fmt.Sprintf("http://%s@127.0.0.1/%d", state.PublicKey, state.ProjectID)
	logs := make([]map[string]any, logsPerSecond)
	for index := range logs {
		timestamp := now
		if (sequence*logsPerSecond+int64(index))%97 == 0 {
			timestamp = timestamp.Add(-time.Hour)
		}
		attributes := map[string]any{"service.name": map[string]any{"type": "string", "value": "comparison"}, "fixture.numeric": map[string]any{"type": "integer", "value": index - 50}}
		if index%23 == 0 {
			attributes["literal.dotted.path"] = map[string]any{"type": "string", "value": "kept"}
		}
		if index%31 == 0 {
			attributes["fixture.null"] = map[string]any{"type": "null", "value": nil}
		}
		logs[index] = map[string]any{"timestamp": float64(timestamp.UnixMicro()) / 1e6, "level": "info", "severity_number": 9, "body": fmt.Sprintf("comparison log %d %d", sequence, index), "attributes": attributes}
	}
	header, _ := json.Marshal(map[string]any{"dsn": dsn, "sdk": map[string]string{"name": "eventglass.r3", "version": "1"}})
	logPayload, _ := json.Marshal(map[string]any{"version": 2, "items": logs})
	logHeader, _ := json.Marshal(map[string]any{"type": "log", "length": len(logPayload), "item_count": len(logs)})
	bodies := [][]byte{bytes.Join([][]byte{header, logHeader, logPayload, []byte{}}, []byte("\n"))}
	var duplicate []byte
	for index := 0; index < errorsPerSecond; index++ {
		digest := sha256.Sum256([]byte(fmt.Sprintf("r3:%d:%d", sequence, index)))
		eventID := hex.EncodeToString(digest[:16])
		event := map[string]any{"event_id": eventID, "timestamp": now.Format(time.RFC3339Nano), "level": "error", "message": fmt.Sprintf("comparison error %d %d", sequence, index), "tags": map[string]any{"equal.timestamp": "true"}}
		payload, _ := json.Marshal(event)
		itemHeader, _ := json.Marshal(map[string]any{"type": "event", "length": len(payload)})
		eventEnvelope := bytes.Join([][]byte{header, itemHeader, payload, []byte{}}, []byte("\n"))
		bodies = append(bodies, eventEnvelope)
		if index == 0 {
			duplicate = eventEnvelope
		}
	}
	return bodies, duplicate
}

func postEnvelope(client *http.Client, baseURL string, state comparisonState, body []byte, input *inputEvidence) (int, string, error) {
	request, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/%d/envelope/", baseURL, state.ProjectID), bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/x-sentry-envelope")
	request.Header.Set("X-Sentry-Auth", "Sentry sentry_version=7, sentry_key="+state.PublicKey)
	input.record(body)
	response, err := client.Do(request)
	if err != nil {
		return 0, "", err
	}
	defer response.Body.Close()
	data, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
	return response.StatusCode, string(data), readErr
}

func postEnvelopeWithAdmissionRetry(client *http.Client, baseURL string, state comparisonState, body []byte, input *inputEvidence) (int, string, error) {
	deadline := time.Now().Add(2 * time.Second)
	for {
		status, response, err := postEnvelope(client, baseURL, state, body, input)
		if err != nil || status != http.StatusTooManyRequests || time.Now().After(deadline) {
			return status, response, err
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func runMixedQueries(ctx context.Context, client *http.Client, baseURL string, state comparisonState, csrf string, delay time.Duration, mu *sync.Mutex,
	latencies, rows, histograms, rowsServer, histogramsServer, rowsOverhead, histogramsOverhead, visibility *[]time.Duration,
	report *comparisonReport, result chan<- error,
) {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		result <- nil
		return
	case <-timer.C:
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	queryIndex := 0
	for {
		select {
		case <-ctx.Done():
			result <- nil
			return
		case <-ticker.C:
			histogram := queryIndex%2 == 1
			queryIndex++
			measurement, err := search(ctx, client, baseURL, state, csrf, histogram, false)
			if err != nil {
				if ctx.Err() != nil {
					result <- nil
					return
				}
				mu.Lock()
				report.QueryFailures++
				report.QueryFailureCodes[searchFailureCode(err)]++
				mu.Unlock()
				continue
			}
			mu.Lock()
			if len(report.QueryMeasurements) >= maxQueryMeasurements {
				mu.Unlock()
				result <- errors.New("query diagnostic sample limit exceeded")
				return
			}
			kind := "rows"
			if histogram {
				kind = "histogram"
			}
			report.QueryMeasurements = append(report.QueryMeasurements, queryMeasurementEvidence{
				Kind: kind, StartedAt: measurement.StartedAt, TotalMS: measurement.Total.Milliseconds(),
				Objects: measurement.Objects, ScannedBytes: measurement.ScannedBytes,
				snapshotID: measurement.SnapshotID, total: measurement.Total, server: measurement.Server,
			})
			if len(*latencies) == 0 {
				report.QueryObjectsFirst = measurement.Objects
			}
			report.QueryObjectsLast = measurement.Objects
			report.QueryObjectsMax = max(report.QueryObjectsMax, measurement.Objects)
			report.QueryScannedBytesMax = max(report.QueryScannedBytesMax, measurement.ScannedBytes)
			*latencies = append(*latencies, measurement.Total)
			if histogram {
				*histograms = append(*histograms, measurement.Total)
				*histogramsServer = append(*histogramsServer, measurement.Server)
				*histogramsOverhead = append(*histogramsOverhead, measurement.Overhead())
			} else {
				*rows = append(*rows, measurement.Total)
				*rowsServer = append(*rowsServer, measurement.Server)
				*rowsOverhead = append(*rowsOverhead, measurement.Overhead())
			}
			if measurement.Visibility >= 0 {
				*visibility = append(*visibility, measurement.Visibility)
			}
			mu.Unlock()
		}
	}
}

func searchFailureCode(err error) string {
	if err == nil {
		return "ok"
	}
	message := err.Error()
	if status, body, ok := strings.Cut(message, " body="); ok && strings.HasPrefix(status, "search status=") {
		// The server returns only a fixed public error code. Do not store
		// raw response bodies, query data or driver messages in reports.
		var response struct {
			Code string `json:"code"`
		}
		if json.Unmarshal([]byte(body), &response) == nil && response.Code != "" {
			return strings.TrimPrefix(status, "search status=") + ":" + response.Code
		}
		return strings.TrimPrefix(status, "search status=") + ":unknown"
	}
	if strings.HasPrefix(message, "snapshot release status=") {
		return strings.Replace(message, "snapshot release status=", "snapshot_release_status_", 1)
	}
	if message == "successful query omitted snapshot_id" {
		return "missing_snapshot_id"
	}
	return "client_or_decode_failure"
}

type searchMeasurement struct {
	SnapshotID   string
	StartedAt    time.Time
	Total        time.Duration
	Server       time.Duration
	Visibility   time.Duration
	Objects      int64
	ScannedBytes int64
}

func (measurement searchMeasurement) Overhead() time.Duration {
	overhead := measurement.Total - measurement.Server
	if overhead < 0 {
		return 0
	}
	return overhead
}

func search(ctx context.Context, client *http.Client, baseURL string, state comparisonState, csrf string, histogram, regex bool) (searchMeasurement, error) {
	now := time.Now()
	body := map[string]any{"tenant_id": strconv.FormatInt(state.TenantID, 10), "project_ids": []string{strconv.FormatInt(state.ProjectID, 10)},
		"start_us": strconv.FormatInt(now.Add(-15*time.Minute).UnixMicro(), 10), "end_us": strconv.FormatInt(now.Add(time.Minute).UnixMicro(), 10),
		"time_basis": "received", "kinds": []string{"log", "error"}, "filter": map[string]any{"op": "constant", "value": true}, "limit": 100, "sort": "received_desc", "mode": "sync", "projection": "list"}
	if regex {
		delete(body, "filter")
		body["expression"] = `matches(message,"comparison.*(log|error)")`
	}
	endpoint := "/v1/search"
	if histogram {
		endpoint = "/v1/aggregate"
		delete(body, "limit")
		delete(body, "sort")
		delete(body, "projection")
		body["metrics"] = []map[string]any{{"name": "events", "op": "count"}}
		body["group_by"] = []any{}
		body["histogram"] = map[string]any{"interval": "1m", "empty_buckets": true}
		body["top"] = 1000
		body["order"] = map[string]any{"metric": "events", "direction": "desc"}
	}
	encoded, _ := json.Marshal(body)
	started := time.Now()
	var data []byte
	for {
		request, _ := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+endpoint, bytes.NewReader(encoded))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Origin", comparisonOrigin)
		request.Header.Set("X-CSRF-Token", csrf)
		response, err := client.Do(request)
		if err != nil {
			return searchMeasurement{}, err
		}
		data, err = io.ReadAll(io.LimitReader(response.Body, 8<<20))
		response.Body.Close()
		if err != nil {
			return searchMeasurement{}, err
		}
		if response.StatusCode == http.StatusOK {
			break
		}
		if (response.StatusCode != http.StatusServiceUnavailable && response.StatusCode != http.StatusTooManyRequests) || time.Since(started) >= 15*time.Second {
			return searchMeasurement{}, fmt.Errorf("search status=%d body=%s", response.StatusCode, data)
		}
		time.Sleep(250 * time.Millisecond)
	}
	var wire struct {
		SnapshotID string `json:"snapshot_id"`
		Rows       []struct {
			ReceivedTimeUS string `json:"received_time_us"`
		} `json:"rows"`
		Stats struct {
			VisibilityLagMS *string `json:"visibility_lag_ms"`
			ElapsedMS       string  `json:"elapsed_ms"`
			Objects         string  `json:"objects"`
			ScannedBytes    string  `json:"scanned_bytes"`
		} `json:"stats"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return searchMeasurement{}, err
	}
	if wire.SnapshotID == "" {
		return searchMeasurement{}, errors.New("successful query omitted snapshot_id")
	}
	queryLatency := time.Since(started)
	if err := releaseComparisonSnapshot(ctx, client, baseURL, state.TenantID, wire.SnapshotID, csrf); err != nil {
		return searchMeasurement{}, err
	}
	serverMS, err := strconv.ParseInt(wire.Stats.ElapsedMS, 10, 64)
	if err != nil || serverMS < 0 {
		return searchMeasurement{}, fmt.Errorf("invalid query elapsed_ms %q", wire.Stats.ElapsedMS)
	}
	objects, err := strconv.ParseInt(wire.Stats.Objects, 10, 64)
	if err != nil || objects < 0 {
		return searchMeasurement{}, fmt.Errorf("invalid query objects %q", wire.Stats.Objects)
	}
	scannedBytes, err := strconv.ParseInt(wire.Stats.ScannedBytes, 10, 64)
	if err != nil || scannedBytes < 0 {
		return searchMeasurement{}, fmt.Errorf("invalid query scanned_bytes %q", wire.Stats.ScannedBytes)
	}
	visible := time.Duration(-1)
	if wire.Stats.VisibilityLagMS != nil {
		value, parseErr := strconv.ParseInt(*wire.Stats.VisibilityLagMS, 10, 64)
		if parseErr != nil {
			return searchMeasurement{}, parseErr
		}
		visible = time.Duration(value) * time.Millisecond
	} else if !histogram && len(wire.Rows) > 0 {
		// visibility_lag_ms is nullable by contract. The comparison still needs
		// an observed ACK-to-query sample, so use the newest returned received
		// timestamp rather than treating a missing server estimate as zero.
		var newest int64
		for _, row := range wire.Rows {
			value, parseErr := strconv.ParseInt(row.ReceivedTimeUS, 10, 64)
			if parseErr != nil {
				return searchMeasurement{}, parseErr
			}
			newest = max(newest, value)
		}
		lag := time.Since(time.UnixMicro(newest))
		if lag < 0 {
			lag = 0
		}
		visible = lag
	}
	return searchMeasurement{SnapshotID: wire.SnapshotID, StartedAt: started, Total: queryLatency, Server: time.Duration(serverMS) * time.Millisecond, Visibility: visible, Objects: objects, ScannedBytes: scannedBytes}, nil
}

func releaseComparisonSnapshot(ctx context.Context, client *http.Client, baseURL string, tenantID int64, snapshotID, csrf string) error {
	// The workload issues moving last-15-minute queries, so each response owns
	// its own snapshot. Release after the response has been measured instead of
	// exhausting the real per-user active-snapshot admission limit of four.
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	endpoint := fmt.Sprintf("%s/v1/snapshots/%s?tenant_id=%d", baseURL, snapshotID, tenantID)
	request, err := http.NewRequestWithContext(cleanup, http.MethodDelete, endpoint, nil)
	if err != nil {
		return err
	}
	request.Header.Set("X-CSRF-Token", csrf)
	request.Header.Set("Origin", comparisonOrigin)
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("snapshot release status=%d", response.StatusCode)
	}
	return nil
}

func drainBacklog(t *testing.T, pool *pgxpool.Pool, limit time.Duration) (int64, int64) {
	t.Helper()
	deadline := time.Now().Add(limit)
	var maximum, current int64
	for {
		if err := pool.QueryRow(context.Background(), `SELECT
			(SELECT count(*) FROM jobs WHERE state IN ('queued','running'))+
			(SELECT count(*) FROM ingest_batches WHERE state='accepted')+
			(SELECT count(*) FROM query_jobs WHERE state IN ('planning','queued','running'))+
			(SELECT count(*) FROM query_tasks WHERE state IN ('queued','running'))`).Scan(&current); err != nil {
			t.Fatal(err)
		}
		maximum = max(maximum, current)
		if current == 0 || time.Now().After(deadline) {
			return maximum, current
		}
		time.Sleep(time.Second)
	}
}

type backlogMonitorResult struct {
	Samples []int64
	Err     error
}

func monitorBacklog(ctx context.Context, pool *pgxpool.Pool, result chan<- backlogMonitorResult) {
	measurement := backlogMonitorResult{}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		var current int64
		err := pool.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM jobs WHERE state IN ('queued','running'))+
			(SELECT count(*) FROM ingest_batches WHERE state='accepted')+
			(SELECT count(*) FROM query_jobs WHERE state IN ('planning','queued','running'))+
			(SELECT count(*) FROM query_tasks WHERE state IN ('queued','running'))`).Scan(&current)
		if err != nil {
			if ctx.Err() != nil {
				result <- measurement
			} else {
				measurement.Err = err
				result <- measurement
			}
			return
		}
		measurement.Samples = append(measurement.Samples, current)
		select {
		case <-ctx.Done():
			result <- measurement
			return
		case <-ticker.C:
		}
	}
}

func backlogSlopePerMinute(samples []int64, interval time.Duration) float64 {
	if len(samples) < 2 || interval <= 0 {
		return 0
	}
	var sumX, sumY, sumXX, sumXY float64
	for index, sample := range samples {
		x, y := float64(index), float64(sample)
		sumX, sumY = sumX+x, sumY+y
		sumXX, sumXY = sumXX+x*x, sumXY+x*y
	}
	n := float64(len(samples))
	denominator := n*sumXX - sumX*sumX
	if denominator == 0 {
		return 0
	}
	perSample := (n*sumXY - sumX*sumY) / denominator
	return perSample * float64(time.Minute) / float64(interval)
}

func pgSize(t *testing.T, pool *pgxpool.Pool) (databaseBytes, objects, objectBytes int64) {
	t.Helper()
	if err := pool.QueryRow(context.Background(), `SELECT pg_database_size(current_database()),(SELECT count(*) FROM object_intents WHERE state='referenced'),(SELECT COALESCE(sum(uploaded_bytes),0) FROM object_intents WHERE state='referenced')`).Scan(&databaseBytes, &objects, &objectBytes); err != nil {
		t.Fatal(err)
	}
	return
}

func pgWAL(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	var bytes int64
	if err := pool.QueryRow(context.Background(), `SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0')::bigint`).Scan(&bytes); err != nil {
		t.Fatal(err)
	}
	return bytes
}

func collectS3Metrics(t *testing.T, client *http.Client, report *comparisonReport) {
	t.Helper()
	if path := strings.TrimSpace(os.Getenv("EVENTGLASS_COMPARISON_RESTART_METRICS")); path != "" {
		data, err := os.ReadFile(filepath.Clean(path))
		if err != nil {
			t.Fatal(err)
		}
		addS3Metrics(data, report)
	}
	endpoints := strings.Split(os.Getenv("EVENTGLASS_COMPARISON_METRICS"), ",")
	if len(endpoints) < 3 {
		t.Fatal("EVENTGLASS_COMPARISON_METRICS must include API, scheduler, and worker listeners")
	}
	collectS3MetricsFrom(t, client, report, endpoints)
}

func collectS3MetricsFrom(t *testing.T, client *http.Client, report *comparisonReport, endpoints []string) {
	t.Helper()
	for _, endpoint := range endpoints {
		response, err := client.Get(strings.TrimSpace(endpoint) + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		response.Body.Close()
		if readErr != nil || response.StatusCode != http.StatusOK {
			t.Fatalf("metrics %s status=%d err=%v", endpoint, response.StatusCode, readErr)
		}
		addS3Metrics(data, report)
	}
}

func addS3Metrics(data []byte, report *comparisonReport) {
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		value, parseErr := strconv.ParseUint(fields[1], 10, 64)
		if parseErr != nil {
			continue
		}
		if metric, label, ok := strings.Cut(fields[0], `{operation="`); ok && strings.HasPrefix(metric, "eventglass_operation_") && strings.HasSuffix(label, `"}`) {
			name := strings.TrimSuffix(label, `"}`)
			if report.Operations == nil {
				report.Operations = make(map[string]comparisonOperation)
			}
			entry := report.Operations[name]
			switch metric {
			case "eventglass_operation_calls_total":
				entry.Calls += value
			case "eventglass_operation_work_total":
				entry.Work += value
			case "eventglass_operation_failures_total":
				entry.Failures += value
			case "eventglass_operation_elapsed_ms_total":
				entry.ElapsedMS += value
			case "eventglass_operation_work_ms_total":
				entry.WorkMS += value
			default:
				continue
			}
			report.Operations[name] = entry
			continue
		}
		switch fields[0] {
		case `eventglass_s3_requests_total{operation="put"}`:
			report.S3PutRequests += value
		case `eventglass_s3_requests_total{operation="head"}`:
			report.S3HeadRequests += value
		case `eventglass_s3_requests_total{operation="get"}`:
			report.S3GetRequests += value
		case `eventglass_s3_requests_total{operation="range_get"}`:
			report.S3RangeRequests += value
		case `eventglass_s3_transfer_bytes_total{direction="put"}`:
			report.S3PutBytes += value
		case `eventglass_s3_transfer_bytes_total{direction="get"}`:
			report.S3GetBytes += value
		case `eventglass_s3_transfer_bytes_total{direction="range_get"}`:
			report.S3RangeBytes += value
		}
	}
}

func percentileMS(values []time.Duration, quantile float64) int64 {
	if len(values) == 0 {
		return 0
	}
	copyOf := append([]time.Duration(nil), values...)
	sort.Slice(copyOf, func(i, j int) bool { return copyOf[i] < copyOf[j] })
	index := int(float64(len(copyOf)-1) * quantile)
	return copyOf[index].Milliseconds()
}

func newComparisonClient(t *testing.T) *http.Client {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	return &http.Client{Jar: jar, Timeout: 15 * time.Second}
}

func doJSON(t *testing.T, client *http.Client, method, target string, body any, csrf string, expected int, output any) {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest(method, target, bytes.NewReader(encoded))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Origin", comparisonOrigin)
	if csrf != "" {
		request.Header.Set("X-CSRF-Token", csrf)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != expected {
		t.Fatalf("%s %s status=%d body=%s", method, target, response.StatusCode, data)
	}
	if output != nil && json.Unmarshal(data, output) != nil {
		t.Fatalf("decode %s %s body=%s", method, target, data)
	}
}

func waitHTTP(t *testing.T, client *http.Client, target string, limit time.Duration) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		response, err := client.Get(target)
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("HTTP endpoint unavailable: %s", target)
}

func requiredComparisonEnv(t *testing.T, names ...string) []string {
	t.Helper()
	values := make([]string, len(names))
	for index, name := range names {
		values[index] = strings.TrimSpace(os.Getenv(name))
		if values[index] == "" {
			t.Fatalf("%s is required", name)
		}
	}
	return values
}

func comparisonDuration(t *testing.T, name string, fallback time.Duration) time.Duration {
	t.Helper()
	if raw := strings.TrimSpace(os.Getenv(name)); raw != "" {
		value, err := time.ParseDuration(raw)
		if err != nil || value <= 0 {
			t.Fatalf("invalid %s=%q", name, raw)
		}
		return value
	}
	return fallback
}

func parsePositive(t *testing.T, raw string) int64 {
	t.Helper()
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		t.Fatalf("invalid positive integer %q", raw)
	}
	return value
}

func readComparisonState(t *testing.T, path string) comparisonState {
	t.Helper()
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatal(err)
	}
	var state comparisonState
	if err := json.Unmarshal(data, &state); err != nil || state.TenantID <= 0 || state.ProjectID <= 0 || state.PublicKey == "" {
		t.Fatalf("invalid comparison state: %v", err)
	}
	return state
}

func writeReport(t *testing.T, path string, report comparisonReport) {
	t.Helper()
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(filepath.Clean(path), data, 0o600); err != nil {
		t.Fatal(err)
	}
}
