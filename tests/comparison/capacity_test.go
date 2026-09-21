//go:build comparison

package comparison

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// This is a fixed-work conversion/publication capacity measurement, not the
// sustained mixed-query SLO. Workers are externally paused before real HTTP ACKs
// preload independent lane jobs, then resumed only after the observer is ready.
type capacityReport struct {
	comparisonReport
	Kind                      string
	Cycles                    int
	FixtureSHA256             string
	PreloadedJobs             int64
	PreloadMS, DrainElapsedMS int64
	DrainStartedAt            time.Time
	PublishedRecords          int64
	PublishedByProject        []map[string]int64
	Progress                  []capacityProgress
	RecordsPerSecond          float64
	DrainIO, VerificationIO   phaseIO
	DrainWALBytes             int64
	WALStartBytes             int64
	Complete                  bool
}

type capacityProgress struct {
	ElapsedMS, Published, Backlog int64
}

type capacityState struct{ Projects []comparisonState }

func capacityCycles(t *testing.T) int {
	t.Helper()
	value, err := strconv.Atoi(os.Getenv("EVENTGLASS_CAPACITY_CYCLES"))
	if err != nil || value < 16 || value > 512 || value%4 != 0 {
		t.Fatal("capacity cycles must be a multiple of four in [16,512]")
	}
	return value
}

func capacityDefinition(cycles int) string {
	input := newInputEvidence()
	for sequence := 1; sequence <= cycles; sequence++ {
		project := (sequence - 1) % 4
		state := comparisonState{TenantID: 1, ProjectID: int64(project + 1), PublicKey: "capacity-definition-v1"}
		bodies, _ := comparisonEnvelopes(state, int64(sequence), time.Unix(1_789_977_600, 0).UTC())
		for _, body := range bodies {
			input.record(body)
		}
	}
	return input.snapshot().SHA256
}

func TestPrepareCapacityWorkload(t *testing.T) {
	env := requiredComparisonEnv(t, "EVENTGLASS_COMPARISON_BASE_URL", "EVENTGLASS_COMPARISON_STATE", "EVENTGLASS_COMPARISON_REPORT", "EVENTGLASS_DATABASE_URL")
	cycles := capacityCycles(t)
	base := readComparisonState(t, env[1])
	client := newComparisonClient(t)
	var session sessionWire
	doJSON(t, client, http.MethodPost, env[0]+"/v1/sessions", map[string]any{"email": base.Email, "password": base.Password}, "", http.StatusOK, &session)
	state := capacityState{Projects: []comparisonState{base}}
	for index := 1; index < 4; index++ {
		var project struct {
			ProjectID string `json:"project_id"`
		}
		doJSON(t, client, http.MethodPost, env[0]+"/v1/projects", map[string]any{"tenant_id": strconv.FormatInt(base.TenantID, 10), "name": fmt.Sprintf("capacity-%d", index), "default_service": "comparison", "allowed_origins": []string{comparisonOrigin}}, session.CSRFToken, http.StatusCreated, &project)
		var key struct {
			PublicKey string `json:"public_key"`
		}
		doJSON(t, client, http.MethodPost, env[0]+"/v1/projects/"+project.ProjectID+"/keys", map[string]any{"tenant_id": strconv.FormatInt(base.TenantID, 10), "label": "capacity"}, session.CSRFToken, http.StatusCreated, &key)
		copy := base
		copy.ProjectID, copy.PublicKey = parsePositive(t, project.ProjectID), key.PublicKey
		state.Projects = append(state.Projects, copy)
	}
	writeCapacityJSON(t, env[1]+".capacity", state)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, env[3])
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	report := capacityReport{Kind: "fixed-work-publication-v1", Cycles: cycles, FixtureSHA256: capacityDefinition(cycles),
		comparisonReport: comparisonReport{Revision: os.Getenv("EVENTGLASS_COMPARISON_REVISION"), Architecture: "linux/arm64", StartedAt: time.Now().UTC(), Targets: make(map[string]bool)}}
	report.Workers, err = strconv.Atoi(os.Getenv("EVENTGLASS_COMPARISON_WORKERS"))
	if err != nil || report.Workers != 1 && report.Workers != 2 && report.Workers != 4 {
		t.Fatal("capacity workers must be 1,2,4")
	}
	report.PGDatabaseStartBytes, _, _ = pgSize(t, pool)
	wal := pgWAL(t, pool)
	report.WALStartBytes = wal
	input := newInputEvidence()
	// Await each ACK before the next request so random acceptance lane IDs
	// cannot coalesce requests into different job counts in different profiles.
	// The durable jobs remain independent across four projects and sixteen lanes.
	for sequence := 1; sequence <= cycles; sequence++ {
		if err := ctx.Err(); err != nil {
			t.Fatal(err)
		}
		principal := state.Projects[(sequence-1)%4]
		bodies, _ := comparisonEnvelopes(principal, int64(sequence), time.Unix(1_789_977_600, 0).UTC())
		for _, body := range bodies {
			status, _, err := postEnvelopeWithAdmissionRetry(client, env[0], principal, body, input)
			if err != nil || status != http.StatusOK {
				t.Fatalf("capacity preload sequence=%d status=%d err=%v", sequence, status, err)
			}
		}
	}
	report.PreloadMS = time.Since(report.StartedAt).Milliseconds()
	report.SubmittedInput = input.snapshot()
	var advanced, nonqueued int64
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM jobs WHERE state='queued'),(SELECT count(*) FROM ingest_batches WHERE state<>'accepted'),(SELECT count(*) FROM jobs WHERE state<>'queued')`).Scan(&report.PreloadedJobs, &advanced, &nonqueued); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT COALESCE(sum(accepted_count),0),COALESCE(sum(duplicate_count),0),COALESCE(sum(conflict_count),0) FROM receipts`).Scan(&report.Accepted, &report.Duplicates, &report.Conflicts); err != nil {
		t.Fatal(err)
	}
	report.PGWALBytes = pgWAL(t, pool) - wal
	report.Targets["durable_preload"] = report.Accepted == int64(cycles*105) && report.Duplicates == 0 && report.Conflicts == 0 && report.PreloadedJobs == int64(cycles*6) && advanced == 0 && nonqueued == 0
	writeCapacityJSON(t, env[2], report)
	if !report.Targets["durable_preload"] {
		t.Fatal("workers advanced before measurement, preload incomplete, or too few independent jobs")
	}
	t.Logf("capacity preloaded cycles=%d records=%d jobs=%d elapsed_ms=%d", cycles, report.Accepted, report.PreloadedJobs, report.PreloadMS)
}

func TestMeasureCapacityDrain(t *testing.T) {
	env := requiredComparisonEnv(t, "EVENTGLASS_COMPARISON_BASE_URL", "EVENTGLASS_COMPARISON_STATE", "EVENTGLASS_COMPARISON_REPORT", "EVENTGLASS_DATABASE_URL", "EVENTGLASS_CAPACITY_READY")
	var report capacityReport
	readCapacityJSON(t, env[2], &report)
	if report.Kind != "fixed-work-publication-v1" || !report.Targets["durable_preload"] {
		t.Fatal("verified capacity preload required")
	}
	var state capacityState
	readCapacityJSON(t, env[1]+".capacity", &state)
	if len(state.Projects) != 4 {
		t.Fatal("four independent projects required")
	}
	defer func() { writeCapacityJSON(t, env[2], report) }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, env[3])
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	client := newComparisonClient(t)
	client.Timeout = 35 * time.Second
	var session sessionWire
	doJSON(t, client, http.MethodPost, env[0]+"/v1/sessions", map[string]any{"email": state.Projects[0].Email, "password": state.Projects[0].Password}, "", http.StatusOK, &session)
	var before, drained, verified comparisonReport
	// Paused workers cannot serve HTTP. Their counters were captured before
	// pause on a fresh installation, then combined with live API/scheduler data.
	paused, err := os.ReadFile(env[1] + ".paused-metrics")
	if err != nil {
		t.Fatal(err)
	}
	addS3Metrics(paused, &before)
	endpoints := strings.Split(os.Getenv("EVENTGLASS_COMPARISON_METRICS"), ",")
	if len(endpoints) != report.Workers+2 {
		t.Fatal("capacity metrics must cover every role")
	}
	collectS3MetricsFrom(t, client, &before, endpoints[:2])
	wal := pgWAL(t, pool)
	var initialPublished, initialPending, initialFailed int64
	if err := pool.QueryRow(ctx, `SELECT COALESCE(sum(accepted_count) FILTER(WHERE state='published'),0),count(*) FILTER(WHERE state='accepted'),count(*) FILTER(WHERE state='failed') FROM ingest_batches`).Scan(&initialPublished, &initialPending, &initialFailed); err != nil {
		t.Fatal(err)
	}
	if initialPublished != 0 || initialFailed != 0 || initialPending != report.PreloadedJobs {
		t.Fatal("capacity preload changed before release")
	}
	drainStarted := time.Now()
	report.DrainStartedAt = drainStarted.UTC()
	report.Progress = []capacityProgress{{0, 0, initialPending}}
	// The runner waits for this owned marker before unpausing any worker. The
	// elapsed measurement intentionally includes sequential Docker resume time.
	if err := os.WriteFile(env[4], []byte(report.DrainStartedAt.Format(time.RFC3339Nano)), 0o600); err != nil {
		t.Fatal(err)
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	lastSample := time.Now()
	for {
		var pending, failed int64
		if err := pool.QueryRow(ctx, `SELECT COALESCE(sum(accepted_count) FILTER(WHERE state='published'),0),count(*) FILTER(WHERE state='accepted'),count(*) FILTER(WHERE state='failed') FROM ingest_batches`).Scan(&report.PublishedRecords, &pending, &failed); err != nil {
			t.Fatal(err)
		}
		if failed > 0 {
			t.Fatal("a durable acknowledged batch failed")
		}
		if lastSample.IsZero() || time.Since(lastSample) >= time.Second || pending == 0 {
			if len(report.Progress) >= 902 {
				t.Fatal("capacity progress bound exceeded")
			}
			report.Progress = append(report.Progress, capacityProgress{time.Since(drainStarted).Milliseconds(), report.PublishedRecords, pending})
			lastSample = time.Now()
		}
		if pending == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
	report.DrainElapsedMS = time.Since(drainStarted).Milliseconds()
	report.DrainWALBytes = pgWAL(t, pool) - wal
	report.RecordsPerSecond = float64(report.PublishedRecords) * 1000 / float64(report.DrainElapsedMS)
	report.Targets["complete_publication"] = report.PublishedRecords == report.Accepted && report.DrainElapsedMS > 0
	collectS3Metrics(t, client, &drained)
	report.DrainIO, err = ioDelta(before, drained)
	if err != nil {
		t.Fatal(err)
	}
	report.Targets["published_query_count"] = true
	for _, principal := range state.Projects {
		counts, err := publishedCounts(ctx, client, env[0], principal, session.CSRFToken, report.StartedAt, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		report.PublishedByProject = append(report.PublishedByProject, counts)
		if counts["log"] != int64(report.Cycles/4*100) || counts["error"] != int64(report.Cycles/4*5) {
			report.Targets["published_query_count"] = false
		}
	}
	collectS3Metrics(t, client, &verified)
	report.VerificationIO, err = ioDelta(drained, verified)
	if err != nil {
		t.Fatal(err)
	}
	report.Operations = verified.Operations
	report.S3PutRequests, report.S3HeadRequests, report.S3GetRequests, report.S3RangeRequests = verified.S3PutRequests, verified.S3HeadRequests, verified.S3GetRequests, verified.S3RangeRequests
	report.S3PutBytes, report.S3GetBytes, report.S3RangeBytes = verified.S3PutBytes, verified.S3GetBytes, verified.S3RangeBytes
	report.PGDatabaseEndBytes, report.S3Objects, report.S3StoredBytes = pgSize(t, pool)
	report.PGWALBytes = pgWAL(t, pool) - report.WALStartBytes
	report.FinishedAt = time.Now().UTC()
	report.Complete = report.Targets["complete_publication"] && report.Targets["published_query_count"]
	if !report.Complete {
		t.Fatal("capacity publication/oracle incomplete")
	}
	t.Logf("capacity workers=%d records=%d drain_ms=%d records_per_second=%.3f", report.Workers, report.PublishedRecords, report.DrainElapsedMS, report.RecordsPerSecond)
}

func readCapacityJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, value); err != nil {
		t.Fatal(err)
	}
}

func writeCapacityJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}
