package query

import (
	"context"
	"errors"
	"fmt"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

const MaxCatalogFiles = 32768

var (
	ErrCatalogObjectMissing = errors.New("catalog object is missing or changed")
	ErrCatalogLimit         = errors.New("catalog file limit exceeded")
)

type CatalogPager interface {
	CatalogPage(context.Context, control.CatalogCommand) ([]model.CatalogFile, error)
}

type CatalogObjectReader interface {
	Head(context.Context, string) (storage.ObjectInfo, error)
}

// LoadVerifiedCatalog distinguishes an authorized empty dataset from catalog
// corruption. Every selected immutable object must still exist with the exact
// size and checksum recorded by PostgreSQL before a plan may be sealed.
func LoadVerifiedCatalog(ctx context.Context, pager CatalogPager, objects CatalogObjectReader, command control.CatalogCommand) ([]model.CatalogFile, error) {
	if pager == nil || objects == nil {
		return nil, errors.New("catalog dependencies are required")
	}
	command.Limit = control.MaxCatalogPageFiles
	command.AfterFileID = ""
	result := make([]model.CatalogFile, 0)
	for {
		page, err := pager.CatalogPage(ctx, command)
		if err != nil {
			return nil, err
		}
		if len(page) > command.Limit {
			return nil, errors.New("catalog pager exceeded page limit")
		}
		for _, file := range page {
			if len(result) >= MaxCatalogFiles {
				return nil, ErrCatalogLimit
			}
			info, err := objects.Head(ctx, file.ObjectKey)
			if err != nil || info.Size != file.Bytes || info.SHA256 != file.SHA256 {
				return nil, fmt.Errorf("%w: file %s", ErrCatalogObjectMissing, file.FileID)
			}
			result = append(result, file)
		}
		if len(page) < command.Limit {
			return result, nil
		}
		if len(page) == 0 {
			return result, nil
		}
		command.AfterFileID = page[len(page)-1].FileID
	}
}
