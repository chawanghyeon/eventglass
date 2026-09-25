package query

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

func TestVerifiedCatalogStopsBeforeReadingUnplannableMetadata(t *testing.T) {
	checksum := strings.Repeat("a", 64)
	blocks := make([]string, 128)
	for index := range blocks {
		blocks[index] = checksum
	}
	pages := 0
	pager := catalogPagerFunc(func(context.Context, control.CatalogCommand) ([]model.CatalogFile, error) {
		if pages == 10 {
			return nil, nil
		}
		page := make([]model.CatalogFile, control.MaxCatalogPageFiles)
		for index := range page {
			id := fmt.Sprintf("%08x-0000-4000-8000-000000000000", pages*len(page)+index)
			page[index] = model.CatalogFile{FileID: id, ObjectKey: "analytics/" + id, Bytes: 128 << 20,
				SHA256: checksum, BlockSHA256: blocks, PayloadFileID: id,
				PayloadObjectKey: "payload/" + id, PayloadBytes: 128 << 20, PayloadSHA256: checksum, PayloadBlockSHA256: blocks}
		}
		pages++
		return page, nil
	})
	var heads atomic.Int64
	reader := catalogReaderFunc(func(_ context.Context, key string) (storage.ObjectInfo, error) {
		heads.Add(1)
		return storage.ObjectInfo{Key: key, Size: 128 << 20, SHA256: checksum}, nil
	})
	files, err := LoadVerifiedCatalog(context.Background(), pager, reader, control.CatalogCommand{})
	if !errors.Is(err, ErrCatalogLimit) || files != nil {
		t.Fatalf("unplannable metadata retained: files=%d pages=%d heads=%d err=%v", len(files), pages, heads.Load(), err)
	}
	if pages > 4 || heads.Load() >= int64(pages*control.MaxCatalogPageFiles*2) {
		t.Fatalf("metadata limit did not stop before verifying the overflowing page: pages=%d heads=%d", pages, heads.Load())
	}
}

// Local catalog bookkeeping only; the reader performs no S3 I/O.
func BenchmarkLoadVerifiedCatalogMetadata(b *testing.B) {
	checksum := strings.Repeat("a", 64)
	page := make([]model.CatalogFile, 192)
	for index := range page {
		id := fmt.Sprintf("%08x-0000-4000-8000-000000000000", index)
		page[index] = model.CatalogFile{FileID: id, ObjectKey: "analytics/" + id, Bytes: 4096, SHA256: checksum,
			BlockSHA256: []string{checksum}, PayloadFileID: id, PayloadObjectKey: "payload/" + id,
			PayloadBytes: 4096, PayloadSHA256: checksum, PayloadBlockSHA256: []string{checksum}}
	}
	pager := catalogPagerFunc(func(context.Context, control.CatalogCommand) ([]model.CatalogFile, error) { return page, nil })
	reader := catalogReaderFunc(func(_ context.Context, key string) (storage.ObjectInfo, error) {
		return storage.ObjectInfo{Key: key, Size: 4096, SHA256: checksum}, nil
	})
	b.ReportAllocs()
	for b.Loop() {
		if _, err := LoadVerifiedCatalog(context.Background(), pager, reader, control.CatalogCommand{}); err != nil {
			b.Fatal(err)
		}
	}
}

type catalogPagerFunc func(context.Context, control.CatalogCommand) ([]model.CatalogFile, error)

func (function catalogPagerFunc) CatalogPage(ctx context.Context, command control.CatalogCommand) ([]model.CatalogFile, error) {
	return function(ctx, command)
}

type catalogReaderFunc func(context.Context, string) (storage.ObjectInfo, error)

func (f catalogReaderFunc) Head(ctx context.Context, key string) (storage.ObjectInfo, error) {
	return f(ctx, key)
}

func TestCatalogVerificationBoundedAndJoined(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan struct{}, catalogVerificationConcurrency)
	var active atomic.Int32
	reader := catalogReaderFunc(func(ctx context.Context, _ string) (storage.ObjectInfo, error) {
		active.Add(1)
		defer active.Add(-1)
		started <- struct{}{}
		<-ctx.Done()
		return storage.ObjectInfo{}, ctx.Err()
	})
	result := make(chan error, 1)
	go func() { result <- verifyCatalogPage(ctx, reader, make([]model.CatalogFile, 100)) }()
	for range catalogVerificationConcurrency {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("bounded readers did not start")
		}
	}
	if active.Load() != catalogVerificationConcurrency {
		t.Fatal("incorrect metadata concurrency")
	}
	cancel()
	if err := <-result; err == nil {
		t.Fatal("canceled metadata verification succeeded")
	}
	if active.Load() != 0 {
		t.Fatal("verification returned with live readers")
	}
}

func TestCatalogVerificationPreservesOrder(t *testing.T) {
	files := make([]model.CatalogFile, 17)
	for i := range files {
		files[i] = model.CatalogFile{FileID: fmt.Sprint(i), ObjectKey: fmt.Sprint(i), Bytes: 1, SHA256: "sha"}
	}
	pager := catalogPagerFunc(func(context.Context, control.CatalogCommand) ([]model.CatalogFile, error) { return files, nil })
	reader := catalogReaderFunc(func(_ context.Context, key string) (storage.ObjectInfo, error) {
		return storage.ObjectInfo{Key: key, Size: 1, SHA256: "sha"}, nil
	})
	got, err := LoadVerifiedCatalog(context.Background(), pager, reader, control.CatalogCommand{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range got {
		if got[i].FileID != files[i].FileID {
			t.Fatal("verification reordered catalog")
		}
	}
}

// Controlled metadata-latency experiment, not an S3 or end-to-end SLO claim.
func BenchmarkCatalogVerification(b *testing.B) {
	page := make([]model.CatalogFile, 32)
	for i := range page {
		page[i] = model.CatalogFile{Bytes: 1, SHA256: "sha"}
	}
	reader := catalogReaderFunc(func(context.Context, string) (storage.ObjectInfo, error) {
		time.Sleep(time.Millisecond)
		return storage.ObjectInfo{Size: 1, SHA256: "sha"}, nil
	})
	for _, parallel := range []bool{false, true} {
		b.Run(fmt.Sprintf("bounded_parallel=%v", parallel), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if parallel {
					if err := verifyCatalogPage(context.Background(), reader, page); err != nil {
						b.Fatal(err)
					}
				} else {
					for _, file := range page {
						if err := verifyCatalogFile(context.Background(), reader, file); err != nil {
							b.Fatal(err)
						}
					}
				}
			}
		})
	}
}

type catalogHead map[string]storage.ObjectInfo

func (items catalogHead) Head(_ context.Context, key string) (storage.ObjectInfo, error) {
	item, ok := items[key]
	if !ok {
		return storage.ObjectInfo{}, errors.New("not found")
	}
	return item, nil
}

func TestVerifiedCatalogDistinguishesEmptyAndMissing(t *testing.T) {
	empty := catalogPagerFunc(func(context.Context, control.CatalogCommand) ([]model.CatalogFile, error) { return nil, nil })
	files, err := LoadVerifiedCatalog(context.Background(), empty, catalogHead{}, control.CatalogCommand{})
	if err != nil || len(files) != 0 {
		t.Fatalf("authorized empty catalog files=%v err=%v", files, err)
	}

	missing := catalogPagerFunc(func(context.Context, control.CatalogCommand) ([]model.CatalogFile, error) {
		return []model.CatalogFile{{FileID: "00000000-0000-4000-8000-000000000001", ObjectKey: "missing", Bytes: 4, SHA256: "abcd"}}, nil
	})
	if _, err := LoadVerifiedCatalog(context.Background(), missing, catalogHead{}, control.CatalogCommand{}); !errors.Is(err, ErrCatalogObjectMissing) {
		t.Fatalf("missing catalog object became empty: %v", err)
	}
}

func TestVerifiedCatalogRejectsChangedMetadata(t *testing.T) {
	file := model.CatalogFile{FileID: "00000000-0000-4000-8000-000000000001", ObjectKey: "analytics", Bytes: 4, SHA256: "expected"}
	pager := catalogPagerFunc(func(context.Context, control.CatalogCommand) ([]model.CatalogFile, error) {
		return []model.CatalogFile{file}, nil
	})
	for name, info := range map[string]storage.ObjectInfo{
		"size":     {Key: file.ObjectKey, Size: 5, SHA256: file.SHA256},
		"checksum": {Key: file.ObjectKey, Size: file.Bytes, SHA256: "changed"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadVerifiedCatalog(context.Background(), pager, catalogHead{file.ObjectKey: info}, control.CatalogCommand{}); !errors.Is(err, ErrCatalogObjectMissing) {
				t.Fatalf("changed object accepted: %v", err)
			}
		})
	}
}

func TestVerifiedCatalogUsesBoundedPagesAndKeepsEveryObjectVerified(t *testing.T) {
	const checksum = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	total := control.MaxCatalogPageFiles*2 + 3
	files := make([]model.CatalogFile, total)
	indices := make(map[string]int, total)
	for index := range files {
		id := fmt.Sprintf("%08x-0000-4000-8000-000000000000", index)
		files[index] = model.CatalogFile{
			FileID: id, ObjectKey: "analytics/" + id, Bytes: 1, SHA256: checksum,
			PayloadFileID: id, PayloadObjectKey: "payload/" + id, PayloadBytes: 1, PayloadSHA256: checksum,
		}
		indices[id] = index
	}
	var pageSizes []int
	var cursors []string
	pager := catalogPagerFunc(func(_ context.Context, command control.CatalogCommand) ([]model.CatalogFile, error) {
		if command.Limit != control.MaxCatalogPageFiles {
			t.Fatalf("catalog limit=%d want=%d", command.Limit, control.MaxCatalogPageFiles)
		}
		start := 0
		if command.AfterFileID != "" {
			index, ok := indices[command.AfterFileID]
			if !ok {
				t.Fatalf("unknown cursor %q", command.AfterFileID)
			}
			start = index + 1
		}
		end := min(start+command.Limit, len(files))
		pageSizes = append(pageSizes, end-start)
		cursors = append(cursors, command.AfterFileID)
		return append([]model.CatalogFile(nil), files[start:end]...), nil
	})
	var heads atomic.Int64
	reader := catalogReaderFunc(func(_ context.Context, key string) (storage.ObjectInfo, error) {
		heads.Add(1)
		return storage.ObjectInfo{Key: key, Size: 1, SHA256: checksum}, nil
	})

	got, err := LoadVerifiedCatalog(context.Background(), pager, reader, control.CatalogCommand{})
	if err != nil {
		t.Fatal(err)
	}
	wantSizes := []int{control.MaxCatalogPageFiles, control.MaxCatalogPageFiles, 3}
	wantCursors := []string{"", files[control.MaxCatalogPageFiles-1].FileID, files[2*control.MaxCatalogPageFiles-1].FileID}
	if len(got) != total || heads.Load() != int64(total*2) || len(pageSizes) != len(wantSizes) || len(cursors) != len(wantCursors) {
		t.Fatalf("verified=%d heads=%d page_sizes=%v cursors=%v", len(got), heads.Load(), pageSizes, cursors)
	}
	for index := range wantSizes {
		if pageSizes[index] != wantSizes[index] || cursors[index] != wantCursors[index] {
			t.Fatalf("page %d size/cursor=%d/%q want=%d/%q", index, pageSizes[index], cursors[index], wantSizes[index], wantCursors[index])
		}
	}
}

func TestVerifiedCatalogChecksAnalyticsAndPayload(t *testing.T) {
	file := model.CatalogFile{
		FileID: "00000000-0000-4000-8000-000000000001", ObjectKey: "analytics", Bytes: 4, SHA256: "expected",
		PayloadFileID: "00000000-0000-4000-8000-000000000002", PayloadObjectKey: "payload", PayloadBytes: 8, PayloadSHA256: "expected-payload",
	}
	pager := catalogPagerFunc(func(context.Context, control.CatalogCommand) ([]model.CatalogFile, error) {
		return []model.CatalogFile{file}, nil
	})
	var heads []string
	reader := catalogReaderFunc(func(_ context.Context, key string) (storage.ObjectInfo, error) {
		heads = append(heads, key)
		if key == file.ObjectKey {
			return storage.ObjectInfo{Key: key, Size: file.Bytes, SHA256: file.SHA256}, nil
		}
		return storage.ObjectInfo{Key: key, Size: file.PayloadBytes, SHA256: "changed"}, nil
	})
	if _, err := LoadVerifiedCatalog(context.Background(), pager, reader, control.CatalogCommand{}); !errors.Is(err, ErrCatalogObjectMissing) {
		t.Fatalf("catalog accepted changed payload metadata: %v", err)
	}
	if len(heads) != 2 || heads[0] != file.ObjectKey || heads[1] != file.PayloadObjectKey {
		t.Fatalf("detail verification HEADs=%v", heads)
	}
}
