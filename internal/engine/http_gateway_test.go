package engine_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

type byteRangeStore struct {
	key  string
	data []byte
}

func (s byteRangeStore) ReadRange(_ context.Context, key string, offset, length int64) ([]byte, error) {
	if key != s.key || offset < 0 || length < 0 || offset+length > int64(len(s.data)) {
		return nil, storage.ErrObjectNotFound
	}
	return append([]byte(nil), s.data[offset:offset+length]...), nil
}

func gatewayManifest(capability, key string, data []byte, blockSize int64) storage.ObjectManifest {
	whole := sha256.Sum256(data)
	manifest := storage.ObjectManifest{
		Capability: capability,
		ObjectKey:  key,
		Size:       int64(len(data)),
		SHA256:     fmt.Sprintf("%x", whole),
		BlockSize:  blockSize,
	}
	for start := int64(0); start < int64(len(data)); start += blockSize {
		end := start + blockSize
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		digest := sha256.Sum256(data[start:end])
		manifest.BlockSHA256 = append(manifest.BlockSHA256, fmt.Sprintf("%x", digest))
	}
	return manifest
}

func TestReadParquetThroughCapabilityGatewayUsesRanges(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	parquetPath := filepath.Join(t.TempDir(), "gateway.parquet")
	quotedPath := strings.ReplaceAll(parquetPath, "'", "''")
	if _, err := db.ExecContext(ctx, `COPY (
		SELECT i, lpad(i::VARCHAR, 128, 'x') AS payload
		FROM range(200000) t(i)
	) TO '`+quotedPath+`' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 16384)`); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(parquetPath)
	if err != nil {
		t.Fatal(err)
	}
	const key = "private/tenant/gateway.parquet"
	manifest := gatewayManifest("opaque-parquet", key, data, 64<<10)
	gateway, err := storage.NewGateway(byteRangeStore{key: key, data: data}, []storage.ObjectManifest{manifest})
	if err != nil {
		t.Fatal(err)
	}

	var rangedGets atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			if r.Header.Get("Range") == "" {
				http.Error(w, "full GET forbidden", http.StatusBadRequest)
				return
			}
			rangedGets.Add(1)
		}
		gateway.ServeHTTP(w, r)
	}))
	defer server.Close()

	var count int
	url := server.URL + "/objects/opaque-parquet"
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM read_parquet([?]) WHERE i >= ?`, url, 199990).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 10 {
		t.Fatalf("count = %d, want 10", count)
	}
	if rangedGets.Load() == 0 {
		t.Fatal("DuckDB did not issue a ranged GET")
	}
}
