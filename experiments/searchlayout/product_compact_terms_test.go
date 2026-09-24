//go:build duckdb_use_static_lib

package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/storage"
	"github.com/klauspost/compress/zstd"
)

type compactPosting struct {
	id, tf uint64
}

type compactCoreRow struct {
	project, severity int64
	service           string
}

func compactDecodeCore(body []byte, rows int) ([]compactCoreRow, []byte, error) {
	if len(body) < 8 {
		return nil, nil, io.ErrUnexpectedEOF
	}
	coreSize := binary.LittleEndian.Uint64(body[:8])
	if coreSize > uint64(len(body)-8) {
		return nil, nil, io.ErrUnexpectedEOF
	}
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, nil, err
	}
	defer decoder.Close()
	data, err := decoder.DecodeAll(body[8:8+coreSize], nil)
	if err != nil {
		return nil, nil, err
	}
	core := make([]compactCoreRow, rows)
	for i := range core {
		project, width := binary.Varint(data)
		if width <= 0 {
			return nil, nil, io.ErrUnexpectedEOF
		}
		data = data[width:]
		severity, width := binary.Varint(data)
		if width <= 0 {
			return nil, nil, io.ErrUnexpectedEOF
		}
		data = data[width:]
		length, width := binary.Uvarint(data)
		if width <= 0 || length > uint64(len(data)-width) {
			return nil, nil, io.ErrUnexpectedEOF
		}
		data = data[width:]
		core[i] = compactCoreRow{project, severity, string(data[:length])}
		data = data[length:]
	}
	if len(data) != 0 {
		return nil, nil, errors.New("trailing core bytes")
	}
	return core, body[8+coreSize:], nil
}

func compactLookup(packed []byte, target string) ([]compactPosting, error) {
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	defer decoder.Close()
	data, err := decoder.DecodeAll(packed, nil)
	if err != nil {
		return nil, err
	}
	read := func() (uint64, error) {
		v, n := binary.Uvarint(data)
		if n <= 0 {
			return 0, io.ErrUnexpectedEOF
		}
		data = data[n:]
		return v, nil
	}
	terms, err := read()
	if err != nil {
		return nil, err
	}
	previous := ""
	for range terms {
		prefix, err := read()
		if err != nil {
			return nil, err
		}
		suffix, err := read()
		if err != nil {
			return nil, err
		}
		if prefix > uint64(len(previous)) || suffix > uint64(len(data)) {
			return nil, errors.New("invalid compact term")
		}
		term := previous[:prefix] + string(data[:suffix])
		data = data[suffix:]
		count, err := read()
		if err != nil {
			return nil, err
		}
		var found []compactPosting
		prior := uint64(0)
		for range count {
			delta, err := read()
			if err != nil || delta == 0 {
				return nil, io.ErrUnexpectedEOF
			}
			tf, err := read()
			if err != nil || tf == 0 {
				return nil, io.ErrUnexpectedEOF
			}
			id := prior + delta - 1
			if term == target {
				found = append(found, compactPosting{id, tf})
			}
			prior = id + 1
		}
		if term == target {
			return found, nil
		}
		if term > target {
			return nil, nil
		}
		previous = term
	}
	return nil, nil
}

// One compressed vocabulary favors storage; queries must decode the whole
// block. It is not a random-access production index.
func measureProductCompactTerms(t *testing.T, ctx context.Context, db *sql.DB, stage, single, analytics, payload string, rows int, noisy bool) {
	t.Helper()
	f, err := os.Open(stage)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	lists := make(map[string][]compactPosting)
	var coreRaw []byte
	for id := range rows {
		var staged engine.StageRecord
		if err := dec.Decode(&staged); err != nil {
			t.Fatal(err)
		}
		if staged.Record.SeverityNumber == nil || staged.Record.Service == nil {
			t.Fatal("fixture lacks core fields")
		}
		coreRaw = binary.AppendVarint(coreRaw, staged.Record.ProjectID)
		coreRaw = binary.AppendVarint(coreRaw, int64(*staged.Record.SeverityNumber))
		coreRaw = binary.AppendUvarint(coreRaw, uint64(len(*staged.Record.Service)))
		coreRaw = append(coreRaw, *staged.Record.Service...)
		perDoc := make(map[string]uint64)
		for _, value := range staged.Record.SearchValues {
			for _, term := range strings.Fields(strings.ToLower(value)) {
				perDoc[term]++
			}
		}
		for term, tf := range perDoc {
			lists[term] = append(lists[term], compactPosting{uint64(id), tf})
		}
	}
	var extra engine.StageRecord
	if err := dec.Decode(&extra); err != io.EOF {
		t.Fatalf("unexpected stage tail: %v", err)
	}
	terms := make([]string, 0, len(lists))
	for term := range lists {
		terms = append(terms, term)
	}
	slices.Sort(terms)
	var raw []byte
	raw = binary.AppendUvarint(raw, uint64(len(terms)))
	prevTerm := ""
	postings := 0
	for _, term := range terms {
		prefix := 0
		for prefix < len(term) && prefix < len(prevTerm) && term[prefix] == prevTerm[prefix] {
			prefix++
		}
		raw = binary.AppendUvarint(raw, uint64(prefix))
		raw = binary.AppendUvarint(raw, uint64(len(term)-prefix))
		raw = append(raw, term[prefix:]...)
		list := lists[term]
		raw = binary.AppendUvarint(raw, uint64(len(list)))
		prior := uint64(0)
		for _, p := range list {
			raw = binary.AppendUvarint(raw, p.id-prior+1)
			raw = binary.AppendUvarint(raw, p.tf)
			prior = p.id + 1
		}
		postings += len(list)
		prevTerm = term
	}
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	packed := encoder.EncodeAll(raw, nil)
	corePacked := encoder.EncodeAll(coreRaw, nil)
	encoder.Close()
	combined := binary.LittleEndian.AppendUint64(nil, uint64(len(corePacked)))
	combined = append(combined, corePacked...)
	combined = append(combined, packed...)
	coreRows, checkPacked, err := compactDecodeCore(combined, rows)
	if err != nil || !bytes.Equal(checkPacked, packed) || len(coreRows) != rows {
		t.Fatalf("core roundtrip rows=%d err=%v", len(coreRows), err)
	}
	for id, row := range coreRows {
		if row.project != int64(10+id%2) || row.severity != int64(id%10) || row.service != []string{"api", "worker", "sdk"}[id%3] {
			t.Fatalf("core mismatch at id=%d: %+v", id, row)
		}
	}
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decoder.DecodeAll(packed, nil)
	decoder.Close()
	if err != nil || !bytes.Equal(decoded, raw) {
		t.Fatalf("compressed index roundtrip: %v", err)
	}
	read := func() uint64 {
		v, n := binary.Uvarint(decoded)
		if n <= 0 {
			t.Fatal("truncated varint")
		}
		decoded = decoded[n:]
		return v
	}
	if count := read(); count != uint64(len(terms)) {
		t.Fatalf("term count %d", count)
	}
	prevTerm = ""
	for _, want := range terms {
		prefix, suffix := read(), read()
		if prefix > uint64(len(prevTerm)) || suffix > uint64(len(decoded)) {
			t.Fatal("invalid dictionary entry")
		}
		term := prevTerm[:prefix] + string(decoded[:suffix])
		decoded = decoded[suffix:]
		if term != want || read() != uint64(len(lists[want])) {
			t.Fatalf("term mismatch %q/%q", term, want)
		}
		prior := uint64(0)
		for _, entry := range lists[want] {
			delta, tf := read(), read()
			if delta == 0 || prior+delta-1 != entry.id || tf != entry.tf {
				t.Fatalf("posting mismatch term=%q id=%d", want, entry.id)
			}
			prior = entry.id + 1
		}
		prevTerm = term
	}
	if len(decoded) != 0 {
		t.Fatalf("trailing index bytes: %d", len(decoded))
	}
	for _, term := range []string{"fatal", "request", productParquetTrace(236)} {
		if strings.HasPrefix(term, "trace-") && !noisy {
			continue
		}
		var count, sum int64
		for _, p := range lists[term] {
			if p.id%2 == 0 {
				count++
				sum += int64(p.id % 10)
			}
		}
		var wantCount, wantSum int64
		for id := range rows {
			if id%2 != 0 || !(term == "fatal" && id%97 == 0 || term == "request" && id%97 != 0 || strings.HasPrefix(term, "trace-") && id == 236) {
				continue
			}
			wantCount++
			wantSum += int64(id % 10)
		}
		if count != wantCount || sum != wantSum {
			t.Fatalf("term=%q count/sum=%d/%d want=%d/%d", term, count, sum, wantCount, wantSum)
		}
		if rows == 100000 {
			query := "SELECT count(*) FROM read_parquet(?) WHERE project_id=10 AND regexp_matches(message,'(^| )" + term + "( |$)')"
			if strings.HasPrefix(term, "trace-") {
				query = "SELECT count(*) FROM read_parquet(?) WHERE project_id=10 AND list_contains(search_values,'" + term + "')"
			}
			var scanTimes, indexTimes []int64
			runScan := func() {
				start := time.Now()
				var got int64
				if err := db.QueryRowContext(ctx, query, single).Scan(&got); err != nil || got != wantCount {
					t.Fatalf("term=%q Parquet count=%d want=%d err=%v", term, got, wantCount, err)
				}
				scanTimes = append(scanTimes, time.Since(start).Microseconds())
			}
			runIndex := func() {
				start := time.Now()
				found, err := compactLookup(packed, term)
				if err != nil {
					t.Fatal(err)
				}
				var gotCount, gotSum int64
				for _, p := range found {
					if p.id%2 == 0 {
						gotCount++
						gotSum += int64(p.id % 10)
					}
				}
				if gotCount != wantCount || gotSum != wantSum {
					t.Fatalf("term=%q compact count/sum=%d/%d want=%d/%d", term, gotCount, gotSum, wantCount, wantSum)
				}
				indexTimes = append(indexTimes, time.Since(start).Microseconds())
			}
			for i := range 30 {
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
			t.Logf("compact_lookup term=%q rows=%d reps=30 parquet_p50_us=%d parquet_p95_us=%d parquet_p99_us=%d compact_p50_us=%d compact_p95_us=%d compact_p99_us=%d", term, rows, scanTimes[15], scanTimes[28], scanTimes[29], indexTimes[15], indexTimes[28], indexTimes[29])
		}
	}
	stat := func(path string) int64 {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		return info.Size()
	}
	t.Logf("compact_terms rows=%d noisy=%t random_trace_128=%t random_trace_256=%t terms=%d postings=%d raw_bytes=%d zstd_bytes=%d core_zstd_bytes=%d combined_bytes=%d single_plus_index=%d single_plus_complete=%d pair_bytes=%d", rows, noisy, os.Getenv("EVENTGLASS_PRODUCT_PARQUET_RANDOM_TRACE_128") == "1", os.Getenv("EVENTGLASS_PRODUCT_PARQUET_RANDOM_TRACE") == "1", len(terms), postings, len(raw), len(packed), len(corePacked), len(combined), stat(single)+int64(len(packed)), stat(single)+int64(len(combined)), stat(analytics)+stat(payload))
	if rows == 100000 && os.Getenv("EVENTGLASS_PRODUCT_COMPACT_GATEWAY") == "1" {
		measureCompactGateway(t, ctx, stage, single, combined, rows, noisy)
	}
}

func measureCompactGateway(t *testing.T, ctx context.Context, stage, single string, packed []byte, rows int, noisy bool) {
	t.Helper()
	indexPath := filepath.Join(filepath.Dir(stage), "compact-index.bin")
	if err := os.WriteFile(indexPath, packed, 0o600); err != nil {
		t.Fatal(err)
	}
	store := &productFileRangeStore{paths: map[string]string{"single": single, "index": indexPath}, counts: make(map[string]productRangeCount)}
	var manifests []storage.ObjectManifest
	for _, key := range []string{"single", "index"} {
		evidence, err := storage.InspectFile(store.paths[key])
		if err != nil {
			t.Fatal(err)
		}
		manifests = append(manifests, storage.ObjectManifest{Capability: key, ObjectKey: key, Size: evidence.Bytes, SHA256: evidence.SHA256, BlockSize: evidence.BlockSize, BlockSHA256: evidence.BlockSHA256})
	}
	gateway, err := storage.NewGateway(store, manifests)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(gateway)
	defer server.Close()
	for _, term := range []string{"fatal", "request", productParquetTrace(236)} {
		if strings.HasPrefix(term, "trace-") && !noisy {
			continue
		}
		type aggregate struct{ count, sum int64 }
		want := make(map[string]aggregate)
		for id := range rows {
			if id%2 == 0 && (term == "fatal" && id%97 == 0 || term == "request" && id%97 != 0 || strings.HasPrefix(term, "trace-") && id == 236) {
				service := []string{"api", "worker", "sdk"}[id%3]
				value := want[service]
				value.count++
				value.sum += int64(id % 10)
				want[service] = value
			}
		}
		query := "SELECT service,count(*)::BIGINT,coalesce(sum(severity_number),0)::BIGINT FROM read_parquet(?) WHERE project_id=10 AND regexp_matches(message,'(^| )" + term + "( |$)') GROUP BY service"
		if strings.HasPrefix(term, "trace-") {
			query = "SELECT service,count(*)::BIGINT,coalesce(sum(severity_number),0)::BIGINT FROM read_parquet(?) WHERE project_id=10 AND list_contains(search_values,'" + term + "') GROUP BY service"
		}
		var scanTimes, indexTimes, scanGets, indexGets, scanBytes, indexBytes []int64
		runScan := func() {
			store.take("single")
			start := time.Now()
			queryDB, err := engine.Open(ctx, "")
			if err != nil {
				t.Fatal(err)
			}
			resultRows, err := queryDB.QueryContext(ctx, query, server.URL+"/objects/single")
			got := make(map[string]aggregate)
			if err == nil {
				for resultRows.Next() {
					var service string
					var value aggregate
					if err = resultRows.Scan(&service, &value.count, &value.sum); err != nil {
						break
					}
					got[service] = value
				}
				if err == nil {
					err = resultRows.Err()
				}
				if closeErr := resultRows.Close(); err == nil {
					err = closeErr
				}
			}
			closeErr := queryDB.Close()
			if err != nil || closeErr != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("gateway scan term=%q got=%v want=%v err=%v close=%v", term, got, want, err, closeErr)
			}
			scanTimes = append(scanTimes, time.Since(start).Microseconds())
			reads := store.take("single")
			scanGets = append(scanGets, reads.gets)
			scanBytes = append(scanBytes, reads.bytes)
		}
		runIndex := func() {
			store.take("index")
			start := time.Now()
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/objects/index", nil)
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Range", fmt.Sprintf("bytes=0-%d", len(packed)-1))
			response, err := server.Client().Do(request)
			if err != nil {
				t.Fatal(err)
			}
			body, readErr := io.ReadAll(response.Body)
			closeErr := response.Body.Close()
			if readErr != nil || closeErr != nil || response.StatusCode != http.StatusPartialContent || len(body) != len(packed) {
				t.Fatalf("index Range status=%d size=%d read=%v close=%v", response.StatusCode, len(body), readErr, closeErr)
			}
			core, postingsData, err := compactDecodeCore(body, rows)
			if err != nil {
				t.Fatal(err)
			}
			found, err := compactLookup(postingsData, term)
			if err != nil {
				t.Fatal(err)
			}
			got := make(map[string]aggregate)
			for _, p := range found {
				if p.id >= uint64(len(core)) {
					t.Fatal("posting outside core")
				}
				row := core[p.id]
				if row.project == 10 {
					value := got[row.service]
					value.count++
					value.sum += row.severity
					got[row.service] = value
				}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("gateway compact term=%q got=%v want=%v", term, got, want)
			}
			indexTimes = append(indexTimes, time.Since(start).Microseconds())
			reads := store.take("index")
			indexGets = append(indexGets, reads.gets)
			indexBytes = append(indexBytes, reads.bytes)
		}
		for i := range 30 {
			if i%2 == 0 {
				runScan()
				runIndex()
			} else {
				runIndex()
				runScan()
			}
		}
		for _, values := range [][]int64{scanTimes, indexTimes, scanGets, indexGets, scanBytes, indexBytes} {
			slices.Sort(values)
		}
		t.Logf("compact_gateway term=%q reps=30 parquet_p50_us=%d parquet_p95_us=%d parquet_p99_us=%d compact_p50_us=%d compact_p95_us=%d compact_p99_us=%d parquet_get_p50=%d compact_get_p50=%d parquet_bytes_p50=%d compact_bytes_p50=%d", term, scanTimes[15], scanTimes[28], scanTimes[29], indexTimes[15], indexTimes[28], indexTimes[29], scanGets[15], indexGets[15], scanBytes[15], indexBytes[15])
	}
}
