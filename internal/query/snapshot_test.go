package query

import (
	"context"
	"errors"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

type catalogPagerFunc func(context.Context, control.CatalogCommand) ([]model.CatalogFile, error)

func (function catalogPagerFunc) CatalogPage(ctx context.Context, command control.CatalogCommand) ([]model.CatalogFile, error) {
	return function(ctx, command)
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
