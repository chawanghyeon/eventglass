//go:build comparison

package comparison

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestWarmupACKsDoNotEnterMeasuredLatencySamples(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &http.Client{Transport: comparisonTransport(func(*http.Request) (*http.Response, error) {
			time.Sleep(time.Millisecond)
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Header: make(http.Header)}, nil
		})}
		state := comparisonState{ProjectID: 1, PublicKey: "local-fixture"}
		var sequence int64
		var latencies []time.Duration
		var report comparisonReport
		input := newInputEvidence()
		if err := runIngestPhase(client, "http://127.0.0.1", state, 2*time.Second, false, &sequence, &latencies, &report, input); err != nil {
			t.Fatal(err)
		}
		if len(latencies) != 0 || report.LogicalLogs != 0 || report.LogicalErrors != 0 || input.snapshot().Envelopes != 12 {
			t.Fatalf("warmup entered measured population or lost provenance: latencies=%d report=%+v input=%+v", len(latencies), report, input.snapshot())
		}
		if err := runIngestPhase(client, "http://127.0.0.1", state, time.Second, true, &sequence, &latencies, &report, input); err != nil {
			t.Fatal(err)
		}
		if len(latencies) != 6 || report.LogicalLogs != 100 || report.LogicalErrors != 5 || sequence != 3 || input.snapshot().Envelopes != 18 {
			t.Fatalf("incorrect measured population: latencies=%d report=%+v sequence=%d input=%+v", len(latencies), report, sequence, input.snapshot())
		}
	})
}

func TestMeasuredACKTargetRejectsWarmupContamination(t *testing.T) {
	report := comparisonReport{LoadSeconds: 1800, ACKSamples: 12600, Targets: make(map[string]bool)}
	setWorkloadTargets(&report, 220500)
	if report.Targets["measured_ack_samples"] {
		t.Fatal("35-minute population satisfied 30-minute measurement")
	}
	report.ACKSamples = 10800
	setWorkloadTargets(&report, 220500)
	if !report.Targets["measured_ack_samples"] {
		t.Fatal("exact measured request population was rejected")
	}
}

func TestMaintenanceEvidenceRequiresMeasuredSpareAndAllAttemptTime(t *testing.T) {
	operations := map[string]comparisonOperation{"compaction": {Calls: 1, Work: 1, ElapsedMS: 100, WorkMS: 100}}
	if maintenanceTimeAccounted(operations) {
		t.Fatal("legacy counters without spare-time evidence passed")
	}
	operations["maintenance_spare"] = comparisonOperation{Calls: 100, WorkMS: 800}
	operations["retention"] = comparisonOperation{Calls: 1, ElapsedMS: 50}
	operations["gc"] = comparisonOperation{Calls: 1, Failures: 1, ElapsedMS: 50}
	if !maintenanceTimeAccounted(operations) {
		t.Fatal("exact 20% spare-time boundary rejected")
	}
	operations["gc"] = comparisonOperation{Calls: 1, Failures: 1, ElapsedMS: 51}
	if maintenanceTimeAccounted(operations) {
		t.Fatal("failed/no-work maintenance time escaped accounting")
	}
	operations["gc"] = comparisonOperation{Calls: 1, ElapsedMS: 50}
	operations["maintenance_budget_overrun"] = comparisonOperation{Calls: 1}
	if maintenanceTimeAccounted(operations) {
		t.Fatal("cancellation overrun was hidden by aggregate spare time")
	}
	delete(operations, "maintenance_budget_overrun")
	operations["gc"] = comparisonOperation{ElapsedMS: ^uint64(0)}
	if maintenanceTimeAccounted(operations) {
		t.Fatal("overflowed counters passed accounting")
	}
}

func TestBacklogSlopePerMinute(t *testing.T) {
	if slope := backlogSlopePerMinute([]int64{0, 2, 4, 6}, 5*time.Second); math.Abs(slope-24) > .0001 {
		t.Fatalf("growing slope=%f", slope)
	}
	if slope := backlogSlopePerMinute([]int64{6, 4, 2, 0}, 5*time.Second); math.Abs(slope+24) > .0001 {
		t.Fatalf("draining slope=%f", slope)
	}
	if slope := backlogSlopePerMinute([]int64{2}, 5*time.Second); slope != 0 {
		t.Fatalf("single slope=%f", slope)
	}
}

func TestAddS3MetricsAccumulatesRestartedWorker(t *testing.T) {
	var report comparisonReport
	first := []byte("eventglass_s3_requests_total{operation=\"put\"} 2\neventglass_s3_transfer_bytes_total{direction=\"put\"} 30\n")
	second := []byte("eventglass_s3_requests_total{operation=\"put\"} 3\neventglass_s3_transfer_bytes_total{direction=\"put\"} 40\n")
	addS3Metrics(first, &report)
	addS3Metrics(second, &report)
	if report.S3PutRequests != 5 || report.S3PutBytes != 70 {
		t.Fatalf("restart counters requests=%d bytes=%d", report.S3PutRequests, report.S3PutBytes)
	}
}

func TestComparisonOperationMetricsAccumulateRestartedProcess(t *testing.T) {
	var report comparisonReport
	metrics := []byte("eventglass_operation_calls_total{operation=\"query\"} 12\neventglass_operation_work_total{operation=\"query\"} 3\neventglass_operation_failures_total{operation=\"query\"} 1\neventglass_operation_elapsed_ms_total{operation=\"query\"} 55\neventglass_operation_work_ms_total{operation=\"query\"} 40\n")
	addS3Metrics(metrics, &report)
	addS3Metrics(metrics, &report)
	if got := report.Operations["query"]; got != (comparisonOperation{Calls: 24, Work: 6, Failures: 2, ElapsedMS: 110, WorkMS: 80}) {
		t.Fatalf("restart operation counters=%+v", got)
	}
}

func TestWorkloadTargetsRequireLatencySamples(t *testing.T) {
	report := comparisonReport{ACKP95MS: 0, VisibilityP95MS: 0, Targets: make(map[string]bool)}
	setWorkloadTargets(&report, 0)
	if report.Targets["ack_p95_le_500ms"] || report.Targets["visibility_p95_le_5s"] {
		t.Fatal("missing latency samples must not satisfy latency targets")
	}
	report.ACKSamples, report.VisibilitySamples = 1, 1
	setWorkloadTargets(&report, 0)
	if !report.Targets["ack_p95_le_500ms"] || !report.Targets["visibility_p95_le_5s"] {
		t.Fatal("measured zero latency satisfies the numeric targets")
	}
}

func TestSearchMeasurementOverheadDoesNotGoNegative(t *testing.T) {
	if got := (searchMeasurement{Total: 120 * time.Millisecond, Server: 80 * time.Millisecond}).Overhead(); got != 40*time.Millisecond {
		t.Fatalf("overhead=%s", got)
	}
	if got := (searchMeasurement{Total: 80 * time.Millisecond, Server: 120 * time.Millisecond}).Overhead(); got != 0 {
		t.Fatalf("negative overhead=%s", got)
	}
}

func TestSearchFailureCodeNeverReportsRawResponse(t *testing.T) {
	if got := searchFailureCode(fmt.Errorf(`search status=422 body={"code":"query_limit_exceeded","message":"private"}`)); got != "422:query_limit_exceeded" {
		t.Fatalf("error code=%q", got)
	}
	if got := searchFailureCode(fmt.Errorf("a private transport error")); got != "client_or_decode_failure" {
		t.Fatalf("transport error code=%q", got)
	}
	if got := searchFailureCode(fmt.Errorf("snapshot release status=403")); got != "snapshot_release_status_403" {
		t.Fatalf("release error code=%q", got)
	}
}

func TestReleaseComparisonSnapshotHonorsScopeAndFailure(t *testing.T) {
	const snapshotID = "00000000-0000-4000-8000-000000000001"
	var status = http.StatusNoContent
	client := &http.Client{Transport: comparisonTransport(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodDelete || request.URL.Path != "/v1/snapshots/"+snapshotID || request.URL.Query().Get("tenant_id") != "7" || request.Header.Get("X-CSRF-Token") != "csrf" || request.Header.Get("Origin") != comparisonOrigin {
			t.Errorf("release request=%s %s csrf=%q", request.Method, request.URL.String(), request.Header.Get("X-CSRF-Token"))
		}
		return &http.Response{StatusCode: status, Body: http.NoBody, Header: make(http.Header)}, nil
	})}
	if err := releaseComparisonSnapshot(context.Background(), client, "http://127.0.0.1:8080", 7, snapshotID, "csrf"); err != nil {
		t.Fatal(err)
	}
	status = http.StatusForbidden
	if err := releaseComparisonSnapshot(context.Background(), client, "http://127.0.0.1:8080", 7, snapshotID, "csrf"); err == nil {
		t.Fatal("failed snapshot release was ignored")
	}
	status = http.StatusNoContent
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := releaseComparisonSnapshot(canceled, client, "http://127.0.0.1:8080", 7, snapshotID, "csrf"); err != nil {
		t.Fatalf("canceled search must still release its snapshot: %v", err)
	}
}

type comparisonTransport func(*http.Request) (*http.Response, error)

func (transport comparisonTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func TestSubmittedInputFramingAndActualRetries(t *testing.T) {
	a, b := newInputEvidence(), newInputEvidence()
	a.record([]byte("a"))
	a.record([]byte("bc"))
	b.record([]byte("ab"))
	b.record([]byte("c"))
	if a.snapshot().SHA256 == b.snapshot().SHA256 {
		t.Fatal("input framing lost envelope boundaries")
	}
	var attempts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	input := newInputEvidence()
	status, _, err := postEnvelopeWithAdmissionRetry(server.Client(), server.URL, comparisonState{ProjectID: 1}, []byte("abc"), input)
	if err != nil || status != http.StatusOK {
		t.Fatalf("status=%d err=%v", status, err)
	}
	framed := "eventglass-submitted-envelope-v1\x00" + "\x00\x00\x00\x00\x00\x00\x00\x03abc" + "\x00\x00\x00\x00\x00\x00\x00\x03abc"
	digest := sha256.Sum256([]byte(framed))
	got := input.snapshot()
	if got.Envelopes != 2 || got.Bytes != 6 || got.SHA256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("actual retry provenance=%+v", got)
	}
}

func TestPublishedTargetRequiresActualQueryCounts(t *testing.T) {
	report := comparisonReport{Accepted: 105, Targets: make(map[string]bool)}
	setWorkloadTargets(&report, 105)
	if !report.Targets["complete_logical_count"] || report.Targets["published_query_count"] || report.Targets["submitted_input_provenance"] {
		t.Fatal("receipt-only evidence satisfied publication/input provenance")
	}
	report.PublishedCounts = map[string]int64{"log": 100, "error": 5}
	setWorkloadTargets(&report, 105)
	if !report.Targets["published_query_count"] {
		t.Fatal("exact result rejected")
	}
	report.PublishedCounts["log"]--
	setWorkloadTargets(&report, 105)
	if report.Targets["published_query_count"] {
		t.Fatal("missing published row accepted")
	}
}

func TestPublishedCountsChecksFullWindowAndReleasesSnapshotOnInvalidResult(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(45 * time.Minute)
	const valid = `{"snapshot_id":"00000000-0000-4000-8000-000000000001","complete":true,"groups":[{"keys":[{"type":"string","value":"log"}],"metrics":{"events":{"type":"integer","value":"100"}}},{"keys":[{"type":"string","value":"error"}],"metrics":{"events":{"type":"integer","value":"5"}}}]}`
	for _, body := range []string{valid, strings.Replace(valid, `"complete":true`, `"complete":false`, 1), strings.Replace(valid, `"value":"error"`, `"value":"log"`, 1), strings.Replace(valid, `"value":"100"`, `"value":"1.5"`, 1)} {
		var releases atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Origin") != comparisonOrigin || r.Header.Get("X-CSRF-Token") != "csrf" {
				t.Error("missing authority headers")
			}
			if r.Method == http.MethodDelete {
				releases.Add(1)
				w.WriteHeader(http.StatusNoContent)
				return
			}
			var request map[string]any
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			if r.URL.Path != "/v1/aggregate" || request["start_us"] != fmt.Sprint(start.Add(-time.Minute).UnixMicro()) || request["end_us"] != fmt.Sprint(end.Add(time.Minute).UnixMicro()) || request["time_basis"] != "received" {
				t.Errorf("wrong full-window request: %v", request)
			}
			_, _ = w.Write([]byte(body))
		}))
		counts, err := publishedCounts(context.Background(), server.Client(), server.URL, comparisonState{TenantID: 7, ProjectID: 8}, "csrf", start, end)
		server.Close()
		if (err == nil) != (body == valid) || releases.Load() != 1 {
			t.Fatalf("counts=%v err=%v releases=%d", counts, err, releases.Load())
		}
	}
}

func TestFailedIngestCycleJoinsEveryRequest(t *testing.T) {
	var arrived atomic.Int64
	allStarted, release := make(chan struct{}), make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	defer finish()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := arrived.Add(1)
		if n == 6 {
			close(allStarted)
		}
		if n == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		<-release
	}))
	defer server.Close()
	result := make(chan error, 1)
	go func() {
		var sequence int64
		var latencies []time.Duration
		result <- runIngestPhase(server.Client(), server.URL, comparisonState{ProjectID: 1}, time.Second, true, &sequence, &latencies, &comparisonReport{}, newInputEvidence())
	}()
	select {
	case <-allStarted:
	case <-time.After(5 * time.Second):
		finish()
		t.Fatal("requests did not start")
	}
	select {
	case <-result:
		finish()
		t.Fatal("failure returned while requests were live")
	case <-time.After(20 * time.Millisecond):
	}
	finish()
	if err := <-result; err == nil {
		t.Fatal("failed request became successful cycle")
	}
}

func TestColdCacheRequiresEveryChangedWorkerIdentity(t *testing.T) {
	a, b, c, d := strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64), strings.Repeat("d", 64)
	valid := "worker-1 " + a + " " + b + "\nworker-2 " + c + " " + d + "\n"
	if err := verifyWorkerReplacements(valid, 2); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{"", "worker-1 " + a + " " + a, valid + valid, "worker-1 " + a + " " + b, strings.Replace(valid, d, b, 1), strings.Replace(valid, d, strings.Repeat("z", 64), 1)} {
		if err := verifyWorkerReplacements(invalid, 2); err == nil {
			t.Fatal("invalid cold-worker evidence accepted")
		}
	}
}

func TestPhaseIODeltaRejectsCounterResets(t *testing.T) {
	before := comparisonReport{S3RangeRequests: 2, S3RangeBytes: 10, S3GetRequests: 1}
	after := comparisonReport{S3RangeRequests: 3, S3RangeBytes: 30, S3GetRequests: 1}
	delta, err := ioDelta(before, after)
	if err != nil || delta.RangeGET != 1 || delta.RangeBytes != 20 || delta.GET != 0 {
		t.Fatalf("delta=%+v err=%v", delta, err)
	}
	after.S3GetRequests = 0
	if _, err := ioDelta(before, after); err == nil {
		t.Fatal("counter reset became valid phase evidence")
	}
}

func TestAllHistoryRegexPinsWindowAndToken(t *testing.T) {
	var requests atomic.Int64
	start, end := int64(1_000_000), int64(3_601_000_000)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body["start_us"] != fmt.Sprint(start) || body["end_us"] != fmt.Sprint(end) || body["time_basis"] != "received" || body["expression"] == nil {
			t.Errorf("not fixed all-history regex: %v", body)
		}
		if requests.Add(1) == 2 && body["read_token"] != "pinned" {
			t.Error("warm request lost snapshot token")
		}
		_, _ = w.Write([]byte(`{"snapshot_id":"snapshot","read_token":"pinned","complete":true,"rows":[{"record_id":"same"}],"stats":{"objects":"2","cache_bytes":"0"}}`))
	}))
	defer server.Close()
	cold, err := allHistoryRegex(context.Background(), server.Client(), server.URL, comparisonState{TenantID: 1, ProjectID: 2}, "csrf", start, end, "")
	if err != nil {
		t.Fatal(err)
	}
	warm, err := allHistoryRegex(context.Background(), server.Client(), server.URL, comparisonState{TenantID: 1, ProjectID: 2}, "csrf", start, end, cold.ReadToken)
	if err != nil || regexRowsHash(cold.Rows) != regexRowsHash(warm.Rows) {
		t.Fatalf("warm mismatch: %v", err)
	}
}
