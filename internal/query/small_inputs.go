package query

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/chawanghyeon/eventglass/internal/storage"
)

const (
	maxStagedAggregateFileBytes = 64 << 10
	maxStagedAggregateBytes     = 8 << 20
)

// Reuse already cached, verified small single-block inputs without per-file
// loopback HTTP. Never prefetch a cache miss: native predicate pruning may avoid
// some inputs entirely. Rows/detail, cold and large inputs keep lazy gateway
// reads, so this optimization cannot add provider downloads.
// The task's existing disk permit additionally covers at most8MiB staged data.
func (workflow Workflow) stageAggregateInputs(ctx context.Context, directory string, manifests []storage.ObjectManifest) (map[string]string, func() int64, error) {
	paths := make(map[string]string)
	pins := make([]func(), 0, len(manifests))
	var stagedBytes, cacheBytes int64
	var once sync.Once
	release := func() int64 {
		once.Do(func() {
			for _, unpin := range pins {
				unpin()
			}
		})
		return cacheBytes
	}
	fail := func(err error) (map[string]string, func() int64, error) {
		// No child has started. Execute owns the task directory and removes all
		// partial files before releasing its disk permit, including on failure.
		release()
		return nil, nil, err
	}
	for index, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		if manifest.Size > maxStagedAggregateFileBytes || manifest.Size > maxStagedAggregateBytes-stagedBytes {
			continue
		}
		if manifest.Size <= 0 || manifest.BlockSize != storage.DefaultBlockSize || len(manifest.BlockSHA256) != 1 || manifest.BlockSHA256[0] != manifest.SHA256 {
			return fail(errors.New("invalid small aggregate input identity"))
		}
		data, unpin, hit, err := workflow.Cache.ReadCached(ctx, storage.CacheBlockKey{
			InstallationID: manifest.InstallationID, ObjectID: manifest.ObjectID, ContentSHA256: manifest.SHA256,
		}, manifest.Size, manifest.SHA256)
		if err != nil {
			return fail(err)
		}
		if !hit {
			continue
		}
		pins = append(pins, unpin)
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		path := filepath.Join(directory, fmt.Sprintf("input-%d.parquet", index))
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return fail(err)
		}
		_, writeErr := file.Write(data)
		if err := errors.Join(writeErr, file.Close()); err != nil {
			return fail(err)
		}
		paths[manifest.Capability] = path
		stagedBytes += manifest.Size
		if hit {
			cacheBytes += manifest.Size
		}
	}
	return paths, release, nil
}
