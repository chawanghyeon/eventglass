//go:build duckdb_use_static_lib

package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func measureProductPostingSidecar(t *testing.T, ctx context.Context, db *sql.DB, root, single string, rows int, noisy bool) {
	t.Helper()
	postings := filepath.Join(root, "postings.parquet")
	statement := `COPY (WITH scalars AS (
		SELECT record_id,unnest(search_values) AS value FROM read_parquet(` + universalSQLLiteral(single) + `)
	), tokens AS (
		SELECT record_id,unnest(string_split(lower(value),' ')) AS term FROM scalars
	)
	SELECT term,record_id,count(*)::INTEGER AS tf FROM tokens WHERE term<>''
	GROUP BY term,record_id ORDER BY term,record_id) TO ` + universalSQLLiteral(postings) + ` (FORMAT PARQUET,COMPRESSION ZSTD,COMPRESSION_LEVEL 3,ROW_GROUP_SIZE 16384)`
	if _, err := db.ExecContext(ctx, statement); err != nil {
		t.Fatal(err)
	}
	var postingRows, terms int64
	if err := db.QueryRowContext(ctx, "SELECT count(*),count(DISTINCT term) FROM read_parquet(?)", postings).Scan(&postingRows, &terms); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(postings)
	if err != nil {
		t.Fatal(err)
	}
	baseInfo, err := os.Stat(single)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("posting_sidecar noisy=%t base_bytes=%d postings_bytes=%d total_bytes=%d posting_rows=%d unique_terms=%d", noisy, baseInfo.Size(), info.Size(), baseInfo.Size()+info.Size(), postingRows, terms)
	for _, term := range []string{"fatal", "request", "trace-" + fmt.Sprintf("%064x", 237)} {
		if term[0] == 't' && !noisy {
			continue
		}
		var wantCount, wantSum int64
		for id := range rows {
			if id%2 != 0 {
				continue
			}
			match := term == "fatal" && id%97 == 0 || term == "request" && id%97 != 0 || term[0] == 't' && id == 236
			if match {
				wantCount++
				wantSum += int64(id % 10)
			}
		}
		baseline := "SELECT count(*),coalesce(sum(severity_number),0)::BIGINT FROM read_parquet(?) WHERE project_id=10 AND regexp_matches(message,'(^| )" + term + "( |$)')"
		if term[0] == 't' {
			baseline = "SELECT count(*),coalesce(sum(severity_number),0)::BIGINT FROM read_parquet(?) WHERE project_id=10 AND list_contains(search_values,'" + term + "')"
		}
		indexed := "SELECT count(*),coalesce(sum(a.severity_number),0)::BIGINT FROM read_parquet(?) a JOIN read_parquet(?) p USING(record_id) WHERE p.term=? AND a.project_id=10"
		var scanTimes, indexTimes []int64
		for i := range 30 {
			runScan := func() {
				start := time.Now()
				var count, sum int64
				err := db.QueryRowContext(ctx, baseline, single).Scan(&count, &sum)
				if err != nil || count != wantCount || sum != wantSum {
					t.Fatalf("term=%s scan count=%d sum=%d want=%d/%d err=%v", term, count, sum, wantCount, wantSum, err)
				}
				scanTimes = append(scanTimes, time.Since(start).Microseconds())
			}
			runIndex := func() {
				start := time.Now()
				var count, sum int64
				err := db.QueryRowContext(ctx, indexed, single, postings, term).Scan(&count, &sum)
				if err != nil || count != wantCount || sum != wantSum {
					t.Fatalf("term=%s index count=%d sum=%d want=%d/%d err=%v", term, count, sum, wantCount, wantSum, err)
				}
				indexTimes = append(indexTimes, time.Since(start).Microseconds())
			}
			if i%2 == 0 {
				runScan()
				runIndex()
			} else {
				runIndex()
				runScan()
			}
		}
		slices.Sort(scanTimes)
		slices.Sort(indexTimes)
		t.Logf("posting_query noisy=%t term=%s matches=%d reps=30 scan_p50_us=%d scan_p95_us=%d scan_p99_us=%d index_p50_us=%d index_p95_us=%d index_p99_us=%d", noisy, term, wantCount, scanTimes[15], scanTimes[28], scanTimes[29], indexTimes[15], indexTimes[28], indexTimes[29])
	}
}
