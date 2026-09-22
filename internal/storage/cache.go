package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/chawanghyeon/eventglass/internal/resource"
)

const DefaultCacheBytes int64 = 1 << 30

type CacheBlockKey struct {
	InstallationID string
	ObjectID       string
	ContentSHA256  string
	BlockIndex     int64
}

type cacheEntry struct {
	path     string
	size     int64
	pins     int
	lastUsed time.Time
	permit   *resource.Permit
	invalid  bool
}

type cacheFlight struct {
	done chan struct{}
	err  error
}

// BlockCache is a bounded, process-local index over verified immutable blocks.
// Disk files survive restart; identity derives only from server-owned values.
type BlockCache struct {
	directory string
	maxBytes  int64
	disk      *resource.Budget

	mu      sync.Mutex
	entries map[string]*cacheEntry
	flights map[string]*cacheFlight
	used    int64
	hits    uint64
	misses  uint64
	closed  bool
}

func NewBlockCache(directory string, maxBytes int64, disk *resource.Budget) (*BlockCache, error) {
	if directory == "" || maxBytes <= 0 || disk == nil {
		return nil, errors.New("cache directory, limit, and disk budget are required")
	}
	if err := EnsurePrivateDirectory(directory); err != nil {
		return nil, err
	}
	cache := &BlockCache{directory: directory, maxBytes: maxBytes, disk: disk, entries: map[string]*cacheEntry{}, flights: map[string]*cacheFlight{}}
	items, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		if item.IsDir() {
			continue
		}
		path := filepath.Join(directory, item.Name())
		if filepath.Ext(item.Name()) != ".block" {
			if len(item.Name()) >= len(".block-") && item.Name()[:len(".block-")] == ".block-" {
				_ = os.Remove(path)
			}
			continue
		}
		key := item.Name()[:len(item.Name())-len(".block")]
		if decoded, decodeErr := hex.DecodeString(key); decodeErr != nil || len(decoded) != sha256.Size {
			_ = os.Remove(path)
			continue
		}
		info, statErr := item.Info()
		if statErr != nil || info.Size() <= 0 || cache.used+info.Size() > maxBytes {
			_ = os.Remove(path)
			continue
		}
		permit, acquireErr := disk.Acquire(info.Size())
		if acquireErr != nil {
			_ = os.Remove(path)
			continue
		}
		cache.entries[key] = &cacheEntry{path: path, size: info.Size(), lastUsed: info.ModTime(), permit: permit}
		cache.used += info.Size()
	}
	return cache, nil
}

// ReadCached returns a verified pinned block only when already present. A miss
// never starts, joins or fails another caller's provider load. This lets query
// stage warm small inputs without speculative I/O or interfering with flights.
func (cache *BlockCache) ReadCached(ctx context.Context, key CacheBlockKey, expectedSize int64, expectedSHA string) ([]byte, func(), bool, error) {
	encoded, err := encodeCacheKey(key)
	if err != nil || expectedSize <= 0 || expectedSize > DefaultBlockSize || len(expectedSHA) != sha256.Size*2 {
		return nil, nil, false, errors.Join(errors.New("invalid cached block request"), err)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, false, err
	}
	cache.mu.Lock()
	if cache.closed {
		cache.mu.Unlock()
		return nil, nil, false, resource.ErrDraining
	}
	entry := cache.entries[encoded]
	if entry == nil {
		cache.mu.Unlock()
		return nil, nil, false, nil
	}
	entry.pins++
	entry.lastUsed = time.Now()
	cache.mu.Unlock()
	release := cache.release(encoded, entry)
	data, err := os.ReadFile(entry.path)
	if err != nil || int64(len(data)) != expectedSize || digestHex(data) != expectedSHA {
		release()
		cache.invalidate(encoded, entry)
		return nil, nil, false, nil
	}
	if err := ctx.Err(); err != nil {
		release()
		return nil, nil, false, err
	}
	cache.mu.Lock()
	cache.hits++
	cache.mu.Unlock()
	return data, release, true, nil
}

func (cache *BlockCache) Read(ctx context.Context, key CacheBlockKey, expectedSize int64, expectedSHA string, load func(context.Context) ([]byte, error)) ([]byte, func(), bool, error) {
	encoded, err := encodeCacheKey(key)
	if err != nil || expectedSize <= 0 || expectedSize > DefaultBlockSize || len(expectedSHA) != sha256.Size*2 || load == nil {
		return nil, nil, false, errors.Join(errors.New("invalid cache block request"), err)
	}
	missed := false
	for {
		cache.mu.Lock()
		if cache.closed {
			cache.mu.Unlock()
			return nil, nil, false, resource.ErrDraining
		}
		if entry := cache.entries[encoded]; entry != nil {
			entry.pins++
			entry.lastUsed = time.Now()
			cache.mu.Unlock()
			data, readErr := os.ReadFile(entry.path)
			if readErr == nil && int64(len(data)) == expectedSize && digestHex(data) == expectedSHA {
				if !missed {
					cache.mu.Lock()
					cache.hits++
					cache.mu.Unlock()
				}
				return data, cache.release(encoded, entry), !missed, nil
			}
			cache.release(encoded, entry)()
			cache.invalidate(encoded, entry)
			continue
		}
		if flight := cache.flights[encoded]; flight != nil {
			cache.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, nil, false, ctx.Err()
			case <-flight.done:
				if flight.err != nil {
					return nil, nil, false, flight.err
				}
				continue
			}
		}
		flight := &cacheFlight{done: make(chan struct{})}
		cache.flights[encoded] = flight
		cache.misses++
		missed = true
		cache.mu.Unlock()

		data, loadErr := load(ctx)
		if loadErr == nil && (int64(len(data)) != expectedSize || digestHex(data) != expectedSHA) {
			loadErr = errors.New("cache block identity changed or range was short")
		}
		if loadErr == nil {
			loadErr = cache.install(encoded, data)
		}
		cache.mu.Lock()
		flight.err = loadErr
		delete(cache.flights, encoded)
		close(flight.done)
		cache.mu.Unlock()
		if loadErr != nil {
			return nil, nil, false, loadErr
		}
	}
}

func (cache *BlockCache) install(key string, data []byte) error {
	size := int64(len(data))
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.closed {
		return resource.ErrDraining
	}
	if _, exists := cache.entries[key]; exists {
		return nil
	}
	for cache.used+size > cache.maxBytes {
		if !cache.evictOneLocked() {
			return resource.ErrLimited
		}
	}
	permit, err := cache.disk.Acquire(size)
	for err != nil && cache.evictOneLocked() {
		permit, err = cache.disk.Acquire(size)
	}
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(cache.directory, ".block-")
	if err != nil {
		permit.Release()
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err = temporary.Chmod(0o600); err == nil {
		_, err = temporary.Write(data)
	}
	if err == nil {
		err = temporary.Sync()
	}
	closeErr := temporary.Close()
	if err == nil {
		err = closeErr
	}
	path := filepath.Join(cache.directory, key+".block")
	if err == nil {
		err = os.Rename(temporaryPath, path)
	}
	if err != nil {
		permit.Release()
		return err
	}
	cache.entries[key] = &cacheEntry{path: path, size: size, lastUsed: time.Now(), permit: permit}
	cache.used += size
	return nil
}

func (cache *BlockCache) evictOneLocked() bool {
	candidates := make([]string, 0, len(cache.entries))
	for key, entry := range cache.entries {
		if entry.pins == 0 {
			candidates = append(candidates, key)
		}
	}
	if len(candidates) == 0 {
		return false
	}
	sort.Slice(candidates, func(i, j int) bool {
		return cache.entries[candidates[i]].lastUsed.Before(cache.entries[candidates[j]].lastUsed)
	})
	entry := cache.entries[candidates[0]]
	if err := os.Remove(entry.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false
	}
	delete(cache.entries, candidates[0])
	cache.used -= entry.size
	entry.permit.Release()
	return true
}

func (cache *BlockCache) release(key string, entry *cacheEntry) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			cache.mu.Lock()
			if entry.pins > 0 {
				entry.pins--
			}
			if entry.invalid && entry.pins == 0 {
				cache.used -= entry.size
				entry.permit.Release()
			}
			cache.mu.Unlock()
		})
	}
}

func (cache *BlockCache) invalidate(key string, entry *cacheEntry) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.entries[key] != entry {
		return
	}
	delete(cache.entries, key)
	entry.invalid = true
	_ = os.Remove(entry.path)
	if entry.pins == 0 {
		cache.used -= entry.size
		entry.permit.Release()
	}
}

func (cache *BlockCache) Stats() (used int64, hits, misses uint64) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	return cache.used, cache.hits, cache.misses
}

// Close releases shared-budget reservations while preserving verified files for
// the next process. Pinned entries release only when their task calls its pin.
func (cache *BlockCache) Close() {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.closed {
		return
	}
	cache.closed = true
	for key, entry := range cache.entries {
		delete(cache.entries, key)
		entry.invalid = true
		if entry.pins == 0 {
			cache.used -= entry.size
			entry.permit.Release()
		}
	}
}

func encodeCacheKey(key CacheBlockKey) (string, error) {
	if key.InstallationID == "" || key.ObjectID == "" || key.BlockIndex < 0 || len(key.ContentSHA256) != sha256.Size*2 {
		return "", errors.New("invalid cache identity")
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("eventglass-cache-v1\x00%s\x00%s\x00%s\x00%d", key.InstallationID, key.ObjectID, key.ContentSHA256, key.BlockIndex)))
	return hex.EncodeToString(digest[:]), nil
}

func digestHex(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}
