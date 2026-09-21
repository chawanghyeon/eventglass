package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/storage"
	"github.com/google/uuid"
)

// These are real stored byte objects, not Parquet execution evidence. The
// separate native maintenance regression exercises file-pair semantics.
func TestVerifiedDownloadSupportsLargeBundleBytes(t *testing.T) {
	store := integrationStore(t, "download-"+uuid.NewString())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, size := range []int64{storage.MaxJournalBytes + 1, engine.MaxQueryOutputBytes, engine.MaxBundleFileBytes} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			root := t.TempDir()
			file, err := os.Create(filepath.Join(root, "upload"))
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			hash := sha256.New()
			block := make([]byte, 64<<10)
			for index := range block {
				block[index] = byte(index*31 + 7)
			}
			writer := io.MultiWriter(file, hash)
			for remaining := size; remaining > 0; {
				n := min(remaining, int64(len(block)))
				if _, err := writer.Write(block[:n]); err != nil {
					t.Fatal(err)
				}
				remaining -= n
			}
			checksum := hex.EncodeToString(hash.Sum(nil))
			key := fmt.Sprintf("bundle-%d.parquet", size)
			if _, err := store.PutStream(ctx, key, file, size, checksum); err != nil {
				t.Fatal(err)
			}
			before := store.OperationCounts()
			started := time.Now()
			path := filepath.Join(root, "download")
			limit := int64(engine.MaxBundleFileBytes)
			if size <= engine.MaxQueryOutputBytes {
				limit = engine.MaxQueryOutputBytes
			}
			if err := store.DownloadToFile(ctx, key, path, size, checksum, limit); err != nil {
				t.Fatal(err)
			}
			elapsed := time.Since(started)
			evidence, err := storage.InspectFile(path)
			if err != nil || evidence.Bytes != size || evidence.SHA256 != checksum {
				t.Fatalf("actual download: %+v %v", evidence, err)
			}
			after := store.OperationCounts()
			if after.FullGetRequests-before.FullGetRequests != 1 || after.FullGetBytes-before.FullGetBytes != uint64(size) {
				t.Fatalf("download I/O: before=%+v after=%+v", before, after)
			}
			t.Logf("actual S3 download bytes=%d elapsed=%s GET=1 checksum_verified=true", size, elapsed)
			before = store.OperationCounts()
			rejectedLimits := []int64{storage.MaxJournalBytes, engine.MaxBundleFileBytes + 1, 0}
			if size > engine.MaxQueryOutputBytes {
				rejectedLimits = append(rejectedLimits, engine.MaxQueryOutputBytes)
			}
			for _, limit := range rejectedLimits {
				rejected := filepath.Join(root, fmt.Sprintf("rejected-%d", limit))
				if err := store.DownloadToFile(ctx, key, rejected, size, checksum, limit); err == nil {
					t.Fatalf("limit=%d accepted bytes=%d", limit, size)
				}
				if _, err := os.Stat(rejected); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("rejected download created file: %v", err)
				}
			}
			if store.OperationCounts() != before {
				t.Fatal("rejected limit performed S3 I/O")
			}
			if size == engine.MaxBundleFileBytes {
				partial := filepath.Join(root, "canceled")
				child, stop := context.WithCancel(ctx)
				done := make(chan error, 1)
				go func() { done <- store.DownloadToFile(child, key, partial, size, checksum, engine.MaxBundleFileBytes) }()
				observed := false
				deadline := time.NewTimer(5 * time.Second)
				ticker := time.NewTicker(time.Millisecond)
			wait:
				for {
					select {
					case err := <-done:
						stop()
						t.Fatalf("download ended before in-flight cancellation: %v", err)
					case <-deadline.C:
						break wait
					case <-ticker.C:
						if info, err := os.Stat(partial); err == nil && info.Size() > 0 && info.Size() < size {
							observed = true
							break wait
						}
					}
				}
				ticker.Stop()
				deadline.Stop()
				stop()
				if err := <-done; !errors.Is(err, context.Canceled) || !observed {
					t.Fatalf("in-flight cancellation observed=%t err=%v", observed, err)
				}
				if _, err := os.Stat(partial); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("partial download survived cancellation: %v", err)
				}
				if err := store.DownloadToFile(ctx, key, partial, size, checksum, engine.MaxBundleFileBytes); err != nil {
					t.Fatalf("retry after canceled download: %v", err)
				}
			}
		})
	}
}

func TestVerifiedDownloadFailuresRemoveOnlyOwnedPartialFile(t *testing.T) {
	store := integrationStore(t, "download-failure-"+uuid.NewString())
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	data := []byte(strings.Repeat("verified-byte-fixture", 1024))
	info, err := store.Put(ctx, "object", data)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, key, checksum string
		size                int64
	}{
		{"short", "object", info.SHA256, info.Size + 1},
		{"long", "object", info.SHA256, info.Size - 1},
		{"checksum", "object", strings.Repeat("0", 64), info.Size},
		{"missing", "missing", info.SHA256, info.Size},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "partial")
			if err := store.DownloadToFile(ctx, test.key, path, test.size, test.checksum, engine.MaxBundleFileBytes); err == nil {
				t.Fatal("bad object accepted")
			}
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("partial survived error: %v", err)
			}
		})
	}
	path := filepath.Join(t.TempDir(), "existing")
	if err := os.WriteFile(path, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}
	before := store.OperationCounts()
	if err := store.DownloadToFile(ctx, "object", path, info.Size, info.SHA256, engine.MaxBundleFileBytes); err == nil {
		t.Fatal("existing path overwritten")
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "preserve" || before != store.OperationCounts() {
		t.Fatalf("existing data changed: %q %v", data, err)
	}
}
