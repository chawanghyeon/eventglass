//go:build duckdb_use_static_lib

package query

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/resource"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

// Uses real file reads, block verification/cache and HTTP serving. It isolates
// gateway overhead, not provider latency, durable query work or service SLOs.
type benchmarkFileRanges struct {
	paths map[string]string
	reads atomic.Uint64
	bytes atomic.Uint64
}

func (store *benchmarkFileRanges) ReadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file, err := os.Open(store.paths[key])
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data := make([]byte, length)
	_, err = io.ReadFull(io.NewSectionReader(file, offset, length), data)
	if err == nil {
		store.reads.Add(1)
		store.bytes.Add(uint64(length))
	}
	return data, err
}

func (*benchmarkFileRanges) PutStream(context.Context, string, io.ReadSeeker, int64, string) (storage.ObjectInfo, error) {
	return storage.ObjectInfo{}, errors.New("benchmark input store does not publish")
}

func BenchmarkHistogramGateway(b *testing.B) {
	benchmarkHistogramGateway(b, false)
}

// Identical logical rows with32 larger single-block files among224 tiny files.
// The extra column is intentionally not selected: staging must not be credited
// with reducing provider work that Parquet column pruning would already avoid.
func BenchmarkHistogramGatewayMixedSizes(b *testing.B) {
	benchmarkHistogramGateway(b, true)
}

func benchmarkHistogramGateway(b *testing.B, mixed bool) {
	const files = 256
	paths, inputBytes, digest := histogramTestFilesWithSizes(b, files, mixed)
	b.Logf("identical fixture sha256=%s files=%d rows=%d bytes=%d", digest, files, files*100, inputBytes)
	for _, name := range []string{"local", "warm-gateway", "warm-staged"} {
		b.Run(name, func(b *testing.B) {
			root := b.TempDir()
			inputs := paths
			store := &benchmarkFileRanges{paths: map[string]string{}}
			var requests atomic.Uint64
			var gateway *storage.Gateway
			var cache *storage.BlockCache
			var manifests []storage.ObjectManifest
			if name != "local" {
				var err error
				cache, err = storage.NewBlockCache(filepath.Join(root, "cache"), 16<<20, resource.NewBudget(16<<20))
				if err != nil {
					b.Fatal(err)
				}
				defer cache.Close()
				manifests = make([]storage.ObjectManifest, len(paths))
				for index, path := range paths {
					evidence, err := storage.InspectFile(path)
					if err != nil {
						b.Fatal(err)
					}
					key := fmt.Sprintf("fixture-%d", index)
					store.paths[key] = path
					manifests[index] = storage.ObjectManifest{InstallationID: "gateway-benchmark", ObjectID: key, Capability: key,
						ObjectKey: key, Size: evidence.Bytes, SHA256: evidence.SHA256, BlockSize: storage.DefaultBlockSize, BlockSHA256: evidence.BlockSHA256}
				}
				gateway, err = storage.NewGatewayWithCache(store, cache, manifests)
				if err != nil {
					b.Fatal(err)
				}
				defer gateway.ReleasePins()
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requests.Add(1)
					gateway.ServeHTTP(w, r)
				}))
				defer server.Close()
				inputs = make([]string, len(paths))
				for index, manifest := range manifests {
					inputs[index] = server.URL + "/objects/" + manifest.Capability
				}
			}
			request := engine.QueryRequest{Version: 1, QueryID: "gateway-benchmark", Task: model.QueryTaskKey{Stage: model.QueryTaskScan},
				Operation: histogramTestOperation(b), InputPaths: inputs, OutputPath: filepath.Join(root, "result.parquet"), SpillDirectory: filepath.Join(root, "spill"),
				NativeMemoryBytes: 192 << 20, NativeSpillBytes: 256 << 20}
			run := func() {
				if err := os.Remove(request.OutputPath); err != nil && !os.IsNotExist(err) {
					b.Fatal(err)
				}
				if name == "warm-staged" {
					for index := range manifests {
						if err := os.Remove(filepath.Join(root, fmt.Sprintf("input-%d.parquet", index))); err != nil && !os.IsNotExist(err) {
							b.Fatal(err)
						}
					}
					staged, release, err := (Workflow{Store: store, Cache: cache}).stageAggregateInputs(context.Background(), root, manifests)
					if err != nil {
						b.Fatalf("staged=%d error=%v", len(staged), err)
					}
					defer release()
					request.InputPaths = make([]string, len(manifests))
					for index, manifest := range manifests {
						request.InputPaths[index] = inputs[index]
						if path := staged[manifest.Capability]; path != "" {
							request.InputPaths[index] = path
						}
					}
				}
				summary, err := engine.ExecuteQuery(context.Background(), request)
				if err != nil || summary.Rows != 16 || summary.DuckDBVersion != "v2.0.0-dev84020" {
					b.Fatalf("summary=%+v error=%v", summary, err)
				}
				if gateway != nil {
					gateway.ReleasePins() // ExecuteQuery has joined all native reads.
				}
			}
			run() // Populate the actual block cache outside the measured loop.
			requests.Store(0)
			store.reads.Store(0)
			store.bytes.Store(0)
			b.ReportAllocs()
			for b.Loop() {
				run()
			}
			b.ReportMetric(float64(requests.Load())/float64(b.N), "gateway-requests/op")
			b.ReportMetric(float64(store.reads.Load())/float64(b.N), "source-ranges/op")
			b.ReportMetric(float64(store.bytes.Load())/float64(b.N), "source-B/op")
			b.ReportMetric(float64(inputBytes), "input-B")
			status, err := os.ReadFile("/proc/self/status")
			if err != nil {
				b.Fatal(err)
			}
			for _, line := range strings.Split(string(status), "\n") {
				fields := strings.Fields(line)
				if len(fields) == 3 && (fields[0] == "VmRSS:" || fields[0] == "VmHWM:") {
					kib, err := strconv.ParseUint(fields[1], 10, 64)
					if err != nil || fields[2] != "kB" {
						b.Fatalf("invalid RSS field %q", line)
					}
					b.ReportMetric(float64(kib*1024), fields[0][:len(fields[0])-1]+"-B")
				}
			}
			verifyGatewayHistogram(b, request.OutputPath, files)
		})
	}
}

func histogramTestOperation(t testing.TB) engine.QueryOperation {
	t.Helper()
	filter := &Node{Op: "constant", Constant: true}
	canonical, err := CanonicalFilter(filter)
	if err != nil {
		t.Fatal(err)
	}
	scope := model.SnapshotScope{}
	for lane := range scope.LaneCuts {
		scope.LaneCuts[lane] = 100
	}
	plan, err := BuildPlan(model.DatasetSpec{TenantID: 1, ProjectIDs: []int64{2}, Kinds: []model.Kind{model.KindLog}, TimeBasis: model.QueryTimeEvent,
		StartUS: 0, EndUS: 16 * 60_000_000, Filter: canonical}, scope, filter)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := BuildAggregateOperation(AggregateOperationSpec{Plan: plan,
		Metrics: []AggregateMetric{{Name: "events", Op: "count"}}, Histogram: &AggregateHistogram{IntervalUS: 60_000_000, EmptyBuckets: true}})
	if err != nil {
		t.Fatal(err)
	}
	return operation
}

func histogramTestFilesWithSizes(t testing.TB, count int, mixed bool) ([]string, int64, string) {
	t.Helper()
	root := t.TempDir()
	db, err := engine.Open(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var bytes int64
	digest := sha256.New()
	paths := make([]string, count)
	for index := range paths {
		paths[index] = filepath.Join(root, fmt.Sprintf("input-%d.parquet", index))
		extra := ""
		if mixed {
			// All files keep the same schema. Each larger file contains100
			// deterministic, distinct2KiB strings with no random source or time.
			extra = ",''::VARCHAR ignored_text"
			if index%8 == 0 {
				parts := make([]string, 64)
				for part := range parts {
					parts[part] = fmt.Sprintf("md5(cast(i*64+%d AS VARCHAR))", part)
				}
				extra = ",(" + strings.Join(parts, "||") + ")::VARCHAR ignored_text"
			}
		}
		statement := fmt.Sprintf(`COPY (SELECT 1::BIGINT tenant_id,2::BIGINT project_id,'log'::VARCHAR kind,
			%d+i::BIGINT event_time_us,1::BIGINT received_time_us,0::INTEGER lane_id,1::BIGINT batch_seq%s
			FROM range(100) t(i)) TO '%s' (FORMAT PARQUET,COMPRESSION ZSTD)`, index%15*60_000_000, extra, strings.ReplaceAll(paths[index], "'", "''"))
		if _, err := db.ExecContext(context.Background(), statement); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(paths[index])
		if err != nil {
			t.Fatal(err)
		}
		if mixed && (index%8 == 0 && (info.Size() <= 64<<10 || info.Size() > 256<<10) || index%8 != 0 && info.Size() > 64<<10) {
			t.Fatalf("unexpected mixed fixture size: index=%d bytes=%d", index, info.Size())
		}
		bytes += info.Size()
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(info.Size()))
		digest.Write(length[:])
		input, err := os.Open(paths[index])
		if err != nil {
			t.Fatal(err)
		}
		_, copyErr := io.Copy(digest, input)
		closeErr := input.Close()
		if copyErr != nil || closeErr != nil {
			t.Fatalf("hash fixture: %v %v", copyErr, closeErr)
		}
	}
	return paths, bytes, hex.EncodeToString(digest.Sum(nil))
}

func verifyGatewayHistogram(t testing.TB, path string, files int) {
	t.Helper()
	db, err := engine.Open(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT bucket_start_us,m0_valid,m0_excluded FROM read_parquet(?) ORDER BY bucket_start_us`, path)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	index := 0
	for rows.Next() {
		var bucket, count, excluded int64
		if err := rows.Scan(&bucket, &count, &excluded); err != nil {
			t.Fatal(err)
		}
		want := 0
		if index < 15 {
			want = files / 15 * 100
			if index < files%15 {
				want += 100
			}
		}
		if bucket != int64(index)*60_000_000 || count != int64(want) || excluded != 0 {
			t.Fatalf("bucket=%d count=%d want=%d excluded=%d", bucket, count, want, excluded)
		}
		index++
	}
	if err := rows.Err(); err != nil || index != 16 {
		t.Fatalf("buckets=%d err=%v", index, err)
	}
}
