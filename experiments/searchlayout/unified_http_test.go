//go:build duckdb_use_static_lib

package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/engine"
)

type universalHTTPCounts struct {
	head, get, bytes int64
	ranges           []string
}

type universalHTTPMeter struct {
	mu     sync.Mutex
	counts map[string]universalHTTPCounts
}

func (m *universalHTTPMeter) take(name string) universalHTTPCounts {
	m.mu.Lock()
	defer m.mu.Unlock()
	got := m.counts[name]
	m.counts[name] = universalHTTPCounts{}
	return got
}

func (m *universalHTTPMeter) serve(root string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Path
		if name != "/single.parquet" && name != "/universal.bin" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodHead && (r.Method != http.MethodGet || r.Header.Get("Range") == "") {
			http.Error(w, "Range GET required", http.StatusBadRequest)
			return
		}
		recorder := httptest.NewRecorder()
		http.ServeFile(recorder, r, filepath.Join(root, strings.TrimPrefix(name, "/")))
		response := recorder.Result()
		defer response.Body.Close()
		if r.Method == http.MethodGet && response.StatusCode != http.StatusPartialContent {
			http.Error(w, "partial content required", http.StatusBadGateway)
			return
		}
		for key, values := range response.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, response.Body)
		if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusPartialContent {
			return
		}
		m.mu.Lock()
		counts := m.counts[name]
		if r.Method == http.MethodHead {
			counts.head++
		} else {
			counts.get++
			counts.bytes += int64(recorder.Body.Len())
			counts.ranges = append(counts.ranges, r.Header.Get("Range")+" => "+response.Header.Get("Content-Range"))
		}
		m.counts[name] = counts
		m.mu.Unlock()
	})
}

func writeUniversalSingleParquet(t *testing.T, ctx context.Context, root string, docs []universalDoc) string {
	t.Helper()
	csvPath := filepath.Join(root, "docs.csv")
	parquetPath := filepath.Join(root, "single.parquet")
	if err := writeUniversalCSV(csvPath, docs); err != nil {
		t.Fatal(err)
	}
	db, err := engine.Open(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "CREATE TABLE docs(id BIGINT,tenant BIGINT,when_ts BIGINT,live BOOLEAN,duration BIGINT,search0 VARCHAR,search1 VARCHAR,fields_json VARCHAR,source_json VARCHAR)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "COPY docs FROM "+universalSQLLiteral(csvPath)+" (HEADER FALSE)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "COPY docs TO "+universalSQLLiteral(parquetPath)+" (FORMAT PARQUET,COMPRESSION ZSTD)"); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(csvPath); err != nil {
		t.Fatal(err)
	}
	return parquetPath
}

func TestUniversalHTTPRangeComparison(t *testing.T) {
	if os.Getenv("EVENTGLASS_UNIFIED_HTTP") != "1" {
		t.Skip("opt-in HTTP Range comparison")
	}
	ctx := context.Background()
	root := t.TempDir()
	docs := universalFixture(10000)
	parquetPath := writeUniversalSingleParquet(t, ctx, root, docs)
	_, segmentBytes, err := writeUniversalObject(filepath.Join(root, "universal.bin"), docs)
	if err != nil {
		t.Fatal(err)
	}
	meter := &universalHTTPMeter{counts: make(map[string]universalHTTPCounts)}
	server := httptest.NewServer(meter.serve(root))
	defer server.Close()
	db, err := engine.Open(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	parquetURL := server.URL + "/single.parquet"
	src := httpRangeSource{base: server.URL, client: server.Client()}
	for _, tc := range []struct {
		name string
		p    universalPredicate
	}{{"rare", universalPredicate{op: "term", text: "fatal"}}, {"broad", universalPredicate{op: "term", text: "request"}}, {"regex", universalPredicate{op: "regex", text: "timeout|한글"}}} {
		want, wantDetails := oracleUniversalRange(docs, tc.p, "tags/region")
		var parquetTimes, parquetGets, parquetHeads, parquetBytes []int64
		var segmentTimes, segmentGets, segmentHeads, segmentReadBytes []int64
		runParquet := func() {
			meter.take("/single.parquet")
			start := time.Now()
			answer, details := universalDuckDBAnswer(t, ctx, db, parquetURL, parquetURL, tc.p, "tags/region")
			elapsed := time.Since(start).Microseconds()
			counts := meter.take("/single.parquet")
			if len(parquetTimes) == 0 {
				t.Logf("http query=%s parquet first ranges=%v", tc.name, counts.ranges)
			}
			if !reflect.DeepEqual(answer, want) || !reflect.DeepEqual(details, wantDetails) {
				t.Fatalf("query=%s parquet answer=%+v want=%+v", tc.name, answer, want)
			}
			parquetTimes = append(parquetTimes, elapsed)
			parquetGets = append(parquetGets, counts.get)
			parquetHeads = append(parquetHeads, counts.head)
			parquetBytes = append(parquetBytes, counts.bytes)
		}
		runSegment := func() {
			meter.take("/universal.bin")
			start := time.Now()
			answer, details, err := runUniversalRange(ctx, src, segmentBytes, tc.p, "tags/region")
			elapsed := time.Since(start).Microseconds()
			counts := meter.take("/universal.bin")
			if len(segmentTimes) == 0 {
				t.Logf("http query=%s segment first ranges=%v", tc.name, counts.ranges)
			}
			if err != nil || !reflect.DeepEqual(answer, want) || !reflect.DeepEqual(details, wantDetails) {
				t.Fatalf("query=%s segment answer=%+v want=%+v err=%v", tc.name, answer, want, err)
			}
			segmentTimes = append(segmentTimes, elapsed)
			segmentGets = append(segmentGets, counts.get)
			segmentHeads = append(segmentHeads, counts.head)
			segmentReadBytes = append(segmentReadBytes, counts.bytes)
		}
		for i := range 30 {
			if i%2 == 0 {
				runParquet()
				runSegment()
			} else {
				runSegment()
				runParquet()
			}
		}
		for _, values := range [][]int64{parquetTimes, parquetGets, parquetHeads, parquetBytes, segmentTimes, segmentGets, segmentHeads, segmentReadBytes} {
			slices.Sort(values)
		}
		t.Logf("http query=%s reps=30 parquet_p50_us=%d parquet_p95_us=%d parquet_p99_us=%d parquet_get_p50=%d parquet_head_p50=%d parquet_bytes_p50=%d segment_p50_us=%d segment_p95_us=%d segment_p99_us=%d segment_get_p50=%d segment_head_p50=%d segment_bytes_p50=%d", tc.name, parquetTimes[15], parquetTimes[28], parquetTimes[29], parquetGets[15], parquetHeads[15], parquetBytes[15], segmentTimes[15], segmentTimes[28], segmentTimes[29], segmentGets[15], segmentHeads[15], segmentReadBytes[15])
	}
	info, err := os.Stat(parquetPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("http objects parquet_bytes=%d segment_bytes=%d", info.Size(), segmentBytes)
}
