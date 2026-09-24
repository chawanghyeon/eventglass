//go:build duckdb_use_static_lib

package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/storage"
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
	type source struct{ name, base, postings string }
	sources := []source{{"local", single, postings}}
	var remote *productFileRangeStore
	if os.Getenv("EVENTGLASS_PRODUCT_POSTINGS_GATEWAY") == "1" {
		remote = &productFileRangeStore{paths: map[string]string{"single": single, "postings": postings}, counts: make(map[string]productRangeCount)}
		var manifests []storage.ObjectManifest
		for key, path := range remote.paths {
			evidence, err := storage.InspectFile(path)
			if err != nil {
				t.Fatal(err)
			}
			manifests = append(manifests, storage.ObjectManifest{Capability: key, ObjectKey: key, Size: evidence.Bytes, SHA256: evidence.SHA256, BlockSize: evidence.BlockSize, BlockSHA256: evidence.BlockSHA256})
		}
		gateway, err := storage.NewGateway(remote, manifests)
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(gateway)
		defer server.Close()
		sources = append(sources, source{"gateway", server.URL + "/objects/single", server.URL + "/objects/postings"})
	}
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
		for _, src := range sources {
			var scanTimes, indexTimes, scanGets, indexGets, scanBytes, indexBytes []int64
			runQuery := func(query string, args ...any) (count, sum, elapsed int64, reads productRangeCount, err error) {
				queryDB := db
				if src.name == "gateway" {
					remote.take("single", "postings")
				}
				start := time.Now()
				if src.name == "gateway" {
					queryDB, err = engine.Open(ctx, "")
					if err != nil {
						return 0, 0, 0, reads, err
					}
				}
				err = queryDB.QueryRowContext(ctx, query, args...).Scan(&count, &sum)
				if src.name == "gateway" {
					if closeErr := queryDB.Close(); err == nil {
						err = closeErr
					}
					reads = remote.take("single", "postings")
					if err == nil && reads.gets == 0 {
						err = fmt.Errorf("gateway query made no source Range GETs")
					}
				}
				return count, sum, time.Since(start).Microseconds(), reads, err
			}
			for i := range 30 {
				runScan := func() {
					count, sum, elapsed, reads, err := runQuery(baseline, src.base)
					if err != nil || count != wantCount || sum != wantSum {
						t.Fatalf("term=%s scan count=%d sum=%d want=%d/%d err=%v", term, count, sum, wantCount, wantSum, err)
					}
					scanTimes = append(scanTimes, elapsed)
					if src.name == "gateway" {
						scanGets = append(scanGets, reads.gets)
						scanBytes = append(scanBytes, reads.bytes)
					}
				}
				runIndex := func() {
					count, sum, elapsed, reads, err := runQuery(indexed, src.base, src.postings, term)
					if err != nil || count != wantCount || sum != wantSum {
						t.Fatalf("term=%s index count=%d sum=%d want=%d/%d err=%v", term, count, sum, wantCount, wantSum, err)
					}
					indexTimes = append(indexTimes, elapsed)
					if src.name == "gateway" {
						indexGets = append(indexGets, reads.gets)
						indexBytes = append(indexBytes, reads.bytes)
					}
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
			t.Logf("posting_query source=%s noisy=%t term=%s matches=%d reps=30 scan_p50_us=%d scan_p95_us=%d scan_p99_us=%d index_p50_us=%d index_p95_us=%d index_p99_us=%d", src.name, noisy, term, wantCount, scanTimes[15], scanTimes[28], scanTimes[29], indexTimes[15], indexTimes[28], indexTimes[29])
			if src.name == "gateway" {
				for _, values := range [][]int64{scanGets, indexGets, scanBytes, indexBytes} {
					slices.Sort(values)
				}
				t.Logf("posting_gateway noisy=%t term=%s scan_get_p50=%d index_get_p50=%d scan_bytes_p50=%d index_bytes_p50=%d scan_get_range=%d..%d index_get_range=%d..%d", noisy, term, scanGets[15], indexGets[15], scanBytes[15], indexBytes[15], scanGets[0], scanGets[29], indexGets[0], indexGets[29])
			}
		}
	}
}
