package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

const MaxCatalogFiles = 32768

// Bound remote metadata fan-out independently of catalog cardinality. A page
// completes before fetching the next, and cancellation joins all readers.
const catalogVerificationConcurrency = 8

var (
	ErrCatalogObjectMissing = errors.New("catalog object is missing or changed")
	ErrCatalogLimit         = errors.New("catalog limit exceeded")
)

// Bound the full catalog before any optional first-page pruning, retaining the
// original metadata admission ceiling even when a later plan scans fewer files.
// The encoder reuses one file buffer, not a second catalog.
type catalogMetadataBudget struct{ remaining int }

func (budget *catalogMetadataBudget) Write(encoded []byte) (int, error) {
	if len(encoded) > budget.remaining {
		return 0, ErrCatalogLimit
	}
	budget.remaining -= len(encoded)
	return len(encoded), nil
}

type CatalogPager interface {
	CatalogPage(context.Context, control.CatalogCommand) ([]model.CatalogFile, error)
}

type CatalogObjectReader interface {
	Head(context.Context, string) (storage.ObjectInfo, error)
}

// LoadVerifiedCatalog distinguishes an authorized empty dataset from catalog
// corruption and verifies every analytics/payload pair before planning.
func LoadVerifiedCatalog(ctx context.Context, pager CatalogPager, objects CatalogObjectReader, command control.CatalogCommand) ([]model.CatalogFile, error) {
	if pager == nil || objects == nil {
		return nil, errors.New("catalog dependencies are required")
	}
	command.Limit = control.MaxCatalogPageFiles
	command.AfterFileID = ""
	result := make([]model.CatalogFile, 0)
	metadata := json.NewEncoder(&catalogMetadataBudget{remaining: MaxPlanBytes})
	for {
		page, err := pager.CatalogPage(ctx, command)
		if err != nil {
			return nil, err
		}
		if len(page) > command.Limit {
			return nil, errors.New("catalog pager exceeded page limit")
		}
		if len(result)+len(page) > MaxCatalogFiles {
			return nil, ErrCatalogLimit
		}
		for index := range page {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if err := metadata.Encode(&page[index]); err != nil {
				return nil, err
			}
		}
		if err := verifyCatalogPage(ctx, objects, page); err != nil {
			return nil, err
		}
		result = append(result, page...)
		if len(page) < command.Limit {
			return result, nil
		}
		if len(page) == 0 {
			return result, nil
		}
		command.AfterFileID = page[len(page)-1].FileID
	}
}

func verifyCatalogPage(ctx context.Context, objects CatalogObjectReader, page []model.CatalogFile) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var workers sync.WaitGroup
	var first sync.Once
	var failure error
	for worker := 0; worker < min(catalogVerificationConcurrency, len(page)); worker++ {
		workers.Add(1)
		go func(start int) {
			defer workers.Done()
			for index := start; index < len(page); index += catalogVerificationConcurrency {
				if ctx.Err() != nil {
					return
				}
				file := page[index]
				err := verifyCatalogFile(ctx, objects, file)
				if err != nil {
					first.Do(func() { failure = err; cancel() })
					return
				}
			}
		}(worker)
	}
	workers.Wait()
	if failure != nil {
		return failure
	}
	return ctx.Err()
}

func verifyCatalogFile(ctx context.Context, objects CatalogObjectReader, file model.CatalogFile) error {
	info, err := objects.Head(ctx, file.ObjectKey)
	if err != nil || info.Size != file.Bytes || info.SHA256 != file.SHA256 {
		return fmt.Errorf("%w: file %s", ErrCatalogObjectMissing, file.FileID)
	}
	if file.PayloadObjectKey != "" {
		payload, err := objects.Head(ctx, file.PayloadObjectKey)
		if err != nil || payload.Size != file.PayloadBytes || payload.SHA256 != file.PayloadSHA256 {
			return fmt.Errorf("%w: payload for file %s", ErrCatalogObjectMissing, file.FileID)
		}
	}
	return nil
}
