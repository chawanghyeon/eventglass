//go:build duckdb_use_static_lib

package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

type productRangeCount struct{ gets, bytes int64 }

type productFileRangeStore struct {
	mu     sync.Mutex
	paths  map[string]string
	counts map[string]productRangeCount
}

func (s *productFileRangeStore) ReadRange(_ context.Context, key string, offset, length int64) ([]byte, error) {
	path := s.paths[key]
	if path == "" || offset < 0 || length < 0 {
		return nil, storage.ErrObjectNotFound
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data := make([]byte, length)
	if _, err := f.ReadAt(data, offset); err != nil {
		return nil, err
	}
	s.mu.Lock()
	count := s.counts[key]
	count.gets++
	count.bytes += length
	s.counts[key] = count
	s.mu.Unlock()
	return data, nil
}

func (s *productFileRangeStore) take(keys ...string) productRangeCount {
	s.mu.Lock()
	defer s.mu.Unlock()
	var total productRangeCount
	for _, key := range keys {
		count := s.counts[key]
		total.gets += count.gets
		total.bytes += count.bytes
		s.counts[key] = productRangeCount{}
	}
	return total
}

func measureProductGateway(t *testing.T, ctx context.Context, root string, db *sql.DB, analytics, payload, single string, aggregate, detail engine.QueryOperation, firstRaw []byte, rows int) {
	t.Helper()
	files := map[string]string{"analytics": analytics, "payload": payload, "single": single}
	store := &productFileRangeStore{paths: files, counts: make(map[string]productRangeCount)}
	var manifests []storage.ObjectManifest
	for _, item := range []struct{ capability, key string }{
		{"pair-analytics", "analytics"}, {"pair-payload", "payload"},
		{"single-analytics", "single"}, {"single-payload", "single"},
	} {
		evidence, err := storage.InspectFile(files[item.key])
		if err != nil {
			t.Fatal(err)
		}
		manifests = append(manifests, storage.ObjectManifest{
			Capability: item.capability, ObjectKey: item.key, Size: evidence.Bytes,
			SHA256: evidence.SHA256, BlockSize: evidence.BlockSize, BlockSHA256: evidence.BlockSHA256,
		})
	}
	gateway, err := storage.NewGateway(store, manifests)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(gateway)
	defer server.Close()
	if response, err := server.Client().Get(server.URL + "/objects/forbidden"); err != nil {
		t.Fatal(err)
	} else {
		response.Body.Close()
		if response.StatusCode != http.StatusNotFound {
			t.Fatalf("unlisted capability status=%d", response.StatusCode)
		}
	}
	id := fmt.Sprintf("%064x", 1)
	for _, operation := range []engine.QueryOperation{aggregate, detail} {
		sources := []struct {
			name, analytics, payload string
			keys                     []string
			times, gets, bytes       []int64
		}{
			{name: "pair", analytics: server.URL + "/objects/pair-analytics", payload: server.URL + "/objects/pair-payload", keys: []string{"analytics", "payload"}},
			{name: "single", analytics: server.URL + "/objects/single-analytics", payload: server.URL + "/objects/single-payload", keys: []string{"single"}},
		}
		for i := range 30 {
			for step := range 2 {
				source := &sources[(i+step)%2]
				store.take(source.keys...)
				output := filepath.Join(root, fmt.Sprintf("gateway-%s-%s-%02d.parquet", source.name, operation.Kind, i))
				var payloadPaths []string
				if operation.Kind == "detail" {
					payloadPaths = []string{source.payload}
				}
				start := time.Now()
				result, err := engine.ExecuteQuery(ctx, engine.QueryRequest{
					Version: 1, QueryID: fmt.Sprintf("gateway-%s-%s-%d", source.name, operation.Kind, i),
					Task: model.QueryTaskKey{Stage: model.QueryTaskScan}, Operation: operation,
					InputPaths: []string{source.analytics}, PayloadPaths: payloadPaths,
					OutputPath: output, SpillDirectory: output + ".spill",
					NativeMemoryBytes: 128 << 20, NativeSpillBytes: 128 << 20,
				})
				elapsed := time.Since(start).Microseconds()
				count := store.take(source.keys...)
				if err != nil || result.Rows != 1 || count.gets == 0 {
					t.Fatalf("gateway source=%s kind=%s rows=%d GET=%d err=%v", source.name, operation.Kind, result.Rows, count.gets, err)
				}
				if operation.Kind == "aggregate" {
					var got int64
					if err := db.QueryRowContext(ctx, "SELECT m0_valid FROM read_parquet(?)", output).Scan(&got); err != nil || got != int64(rows/2) {
						t.Fatalf("gateway aggregate source=%s got=%d err=%v", source.name, got, err)
					}
				} else {
					var gotID, raw string
					if err := db.QueryRowContext(ctx, "SELECT record_id,raw_json FROM read_parquet(?)", output).Scan(&gotID, &raw); err != nil || gotID != id || !reflect.DeepEqual([]byte(raw), firstRaw) {
						t.Fatalf("gateway detail source=%s id=%s err=%v", source.name, gotID, err)
					}
				}
				source.times = append(source.times, elapsed)
				source.gets = append(source.gets, count.gets)
				source.bytes = append(source.bytes, count.bytes)
			}
		}
		for _, source := range sources {
			for _, values := range [][]int64{source.times, source.gets, source.bytes} {
				slices.Sort(values)
			}
			t.Logf("product_gateway kind=%s source=%s reps=30 p50_us=%d p95_us=%d p99_us=%d s3_get_p50=%d s3_bytes_p50=%d s3_get_range=%d..%d s3_bytes_range=%d..%d", operation.Kind, source.name, source.times[15], source.times[28], source.times[29], source.gets[15], source.bytes[15], source.gets[0], source.gets[29], source.bytes[0], source.bytes[29])
		}
	}
}
