//go:build comparison

package comparison

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const maxQueryMeasurements = 4096

// These are diagnostic boundaries, not exclusive CPU phases. PreJob includes
// HTTP/auth/snapshot/catalog verification and any HTTP retry; Job includes plan
// sealing, queueing, scan/reduce and durable publication; PostJob includes poll
// delay, authorized result export and the response. All processes in this local
// Docker profile share one kernel wall clock. Keep the monotonic HTTP total too.
type queryMeasurementEvidence struct {
	Kind                                string
	StartedAt                           time.Time
	TotalMS, PreJobMS, JobMS, PostJobMS int64
	Objects, ScannedBytes, CacheBytes   int64
	ScanTasks, ReduceTasks, Attempts    int64
	snapshotID                          string
	total, server                       time.Duration
}

type partitionEvidence struct {
	Lane, Schema, Grouping                  int
	Day, Kind                               string
	Bundles, EligibleSmall, Reserved, Bytes int64
}

type compactionEvidence struct {
	Lane                    int
	State                   string
	Tasks, Inputs, Attempts int64
}

func comparisonDatabaseClock(ctx context.Context, pool *pgxpool.Pool) (time.Time, time.Duration, error) {
	started := time.Now()
	var databaseNow time.Time
	if err := pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
		return time.Time{}, 0, err
	}
	return databaseNow, databaseNow.Sub(started.Add(time.Since(started) / 2)), nil
}

func comparisonSearchBounds(databaseNow time.Time) (int64, int64) {
	return databaseNow.Add(-15 * time.Minute).UnixMicro(), databaseNow.Add(time.Minute).UnixMicro()
}

func (sample *queryMeasurementEvidence) setJobInterval(created, finished time.Time) error {
	end := sample.StartedAt.Add(sample.total)
	// A negative interval is evidence of clock skew or a mismatched job, not a
	// performance improvement. Do not silently clamp it into a valid sample.
	if sample.snapshotID == "" || sample.total <= 0 || created.Before(sample.StartedAt) || finished.Before(created) || finished.After(end) ||
		finished.Sub(created).Milliseconds() != sample.server.Milliseconds() {
		return errors.New("query diagnostic job/HTTP interval mismatch")
	}
	sample.PreJobMS = created.Sub(sample.StartedAt).Milliseconds()
	sample.JobMS = finished.Sub(created).Milliseconds()
	sample.PostJobMS = end.Sub(finished).Milliseconds()
	return nil
}

// Run after the timed load and joined query loop, before draining. One bounded
// query fetches all sampled jobs/tasks; no extra SQL or metrics scraping enters
// the measured HTTP request path. The two maintenance snapshots describe this
// observation instant, not an atomic history of the entire load.
func collectQueryDiagnostics(ctx context.Context, pool *pgxpool.Pool, tenant int64, report *comparisonReport) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if len(report.QueryMeasurements) > maxQueryMeasurements {
		return errors.New("query diagnostic sample limit exceeded")
	}
	ids := make([]string, 0, len(report.QueryMeasurements))
	samples := make(map[string]*queryMeasurementEvidence, len(report.QueryMeasurements))
	for index := range report.QueryMeasurements {
		sample := &report.QueryMeasurements[index]
		if sample.snapshotID == "" || samples[sample.snapshotID] != nil {
			return errors.New("query diagnostic snapshot missing or duplicated")
		}
		ids = append(ids, sample.snapshotID)
		samples[sample.snapshotID] = sample
	}
	rows, err := pool.Query(ctx, `SELECT q.snapshot_id::text,q.created_at,q.updated_at,
		count(t.query_id) FILTER(WHERE t.stage='scan'),count(t.query_id) FILTER(WHERE t.stage='reduce'),COALESCE(sum(t.attempt),0)
		FROM query_jobs q LEFT JOIN query_tasks t ON t.tenant_id=q.tenant_id AND t.query_id=q.query_id
		WHERE q.tenant_id=$1 AND q.snapshot_id=ANY($2::uuid[]) AND q.state='succeeded'
		GROUP BY q.query_id`, tenant, ids)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id string
		var created, finished time.Time
		var scans, reduces, attempts int64
		if err := rows.Scan(&id, &created, &finished, &scans, &reduces, &attempts); err != nil {
			rows.Close()
			return err
		}
		sample := samples[id]
		if sample == nil {
			rows.Close()
			return errors.New("query diagnostic duplicate job")
		}
		if err := sample.setJobInterval(created, finished); err != nil {
			rows.Close()
			return err
		}
		sample.ScanTasks, sample.ReduceTasks, sample.Attempts = scans, reduces, attempts
		delete(samples, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if len(samples) != 0 {
		return fmt.Errorf("query diagnostics missing %d completed jobs", len(samples))
	}
	rows, err = pool.Query(ctx, `WITH current_bundles AS (
		SELECT b.bundle_id,b.lane_id,b.schema_version,b.grouping_version,b.event_day,b.kind,b.reserved_by,
		sum(f.bytes) AS bytes,max(f.bytes) AS largest
		FROM bundles b JOIN files f ON f.tenant_id=b.tenant_id AND f.bundle_id=b.bundle_id
		WHERE b.tenant_id=$1 AND b.valid_to_generation IS NULL GROUP BY b.bundle_id)
		SELECT lane_id,schema_version,grouping_version,event_day::text,kind,count(*),
		count(*) FILTER(WHERE largest<8388608 AND reserved_by IS NULL),count(*) FILTER(WHERE reserved_by IS NOT NULL),sum(bytes)
		FROM current_bundles GROUP BY lane_id,schema_version,grouping_version,event_day,kind
		ORDER BY lane_id,schema_version,grouping_version,event_day,kind LIMIT 1025`, tenant)
	if err != nil {
		return err
	}
	for rows.Next() {
		var part partitionEvidence
		if len(report.LoadEndPartitions) >= 1024 {
			rows.Close()
			return errors.New("partition diagnostic limit exceeded")
		}
		if err := rows.Scan(&part.Lane, &part.Schema, &part.Grouping, &part.Day, &part.Kind, &part.Bundles, &part.EligibleSmall, &part.Reserved, &part.Bytes); err != nil {
			rows.Close()
			return err
		}
		report.LoadEndPartitions = append(report.LoadEndPartitions, part)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	rows, err = pool.Query(ctx, `WITH tasks AS (
		SELECT m.task_id,m.lane_id,m.state,m.attempt,count(i.bundle_id) AS inputs FROM maintenance_tasks m
		LEFT JOIN maintenance_inputs i ON i.tenant_id=m.tenant_id AND i.task_id=m.task_id
		WHERE m.tenant_id=$1 AND m.kind='compact' GROUP BY m.task_id)
		SELECT lane_id,state,count(*),sum(inputs),sum(attempt) FROM tasks GROUP BY lane_id,state ORDER BY lane_id,state`, tenant)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var compaction compactionEvidence
		if err := rows.Scan(&compaction.Lane, &compaction.State, &compaction.Tasks, &compaction.Inputs, &compaction.Attempts); err != nil {
			return err
		}
		report.LoadEndCompactions = append(report.LoadEndCompactions, compaction)
	}
	return rows.Err()
}

func TestQueryDiagnosticIntervals(t *testing.T) {
	start := time.Unix(100, 0)
	for _, test := range []struct {
		name              string
		created, finished time.Duration
		server            time.Duration
		clockOffset       time.Duration
		valid             bool
	}{
		{"valid", 20 * time.Millisecond, 70 * time.Millisecond, 50 * time.Millisecond, 0, true},
		{"inclusive_boundaries", 0, 100 * time.Millisecond, 100 * time.Millisecond, 0, true},
		{"host_clock_offset_calibrated", 20 * time.Millisecond, 70 * time.Millisecond, 50 * time.Millisecond, 114 * time.Millisecond, true},
		{"server_clock_early", -time.Millisecond, 70 * time.Millisecond, 71 * time.Millisecond, 0, false},
		{"server_clock_late", 20 * time.Millisecond, 101 * time.Millisecond, 81 * time.Millisecond, 0, false},
		{"inverted_job", 70 * time.Millisecond, 20 * time.Millisecond, 0, 0, false},
		{"wrong_job", 20 * time.Millisecond, 70 * time.Millisecond, 49 * time.Millisecond, 0, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			alignedStart := start.Add(test.clockOffset)
			sample := queryMeasurementEvidence{snapshotID: "sample", StartedAt: alignedStart, total: 100 * time.Millisecond, server: test.server}
			err := sample.setJobInterval(alignedStart.Add(test.created), alignedStart.Add(test.finished))
			if (err == nil) != test.valid {
				t.Fatalf("interval accepted=%v want=%v", err == nil, test.valid)
			}
			if test.valid && sample.PreJobMS+sample.JobMS+sample.PostJobMS != 100 {
				t.Fatalf("lost boundary time: %+v", sample)
			}
		})
	}
}

func TestComparisonSearchBoundsUseDatabaseClock(t *testing.T) {
	databaseNow := time.Date(2026, time.September, 23, 9, 30, 0, 123000, time.UTC)
	start, end := comparisonSearchBounds(databaseNow)
	if got, want := start, databaseNow.Add(-15*time.Minute).UnixMicro(); got != want {
		t.Fatalf("start_us=%d want=%d", got, want)
	}
	if got, want := end, databaseNow.Add(time.Minute).UnixMicro(); got != want {
		t.Fatalf("end_us=%d want=%d", got, want)
	}
}
