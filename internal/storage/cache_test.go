package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/resource"
)

func TestBlockCacheSingleflightPinsEvictionAndRestart(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "cache")
	disk := resource.NewBudget(64)
	cache, err := NewBlockCache(directory, 8, disk)
	if err != nil {
		t.Fatal(err)
	}
	first := []byte("12345678")
	key := CacheBlockKey{InstallationID: "installation", ObjectID: "object-a", ContentSHA256: digestHex(first), BlockIndex: 0}
	var loads atomic.Int64
	started := make(chan struct{})
	unblock := make(chan struct{})
	load := func(context.Context) ([]byte, error) {
		if loads.Add(1) == 1 {
			close(started)
		}
		<-unblock
		return first, nil
	}
	type result struct {
		data    []byte
		release func()
		hit     bool
		err     error
	}
	results := make(chan result, 2)
	var workers sync.WaitGroup
	for range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			data, release, hit, err := cache.Read(context.Background(), key, 8, digestHex(first), load)
			results <- result{data, release, hit, err}
		}()
	}
	<-started
	close(unblock)
	workers.Wait()
	close(results)
	var releases []func()
	for result := range results {
		if result.err != nil || string(result.data) != string(first) {
			t.Fatalf("read = %q, %v", result.data, result.err)
		}
		releases = append(releases, result.release)
	}
	if loads.Load() != 1 {
		t.Fatalf("loads = %d, want 1", loads.Load())
	}
	second := []byte("abcdefgh")
	secondKey := CacheBlockKey{InstallationID: "installation", ObjectID: "object-b", ContentSHA256: digestHex(second), BlockIndex: 0}
	if _, _, _, err := cache.Read(context.Background(), secondKey, 8, digestHex(second), func(context.Context) ([]byte, error) { return second, nil }); !errors.Is(err, resource.ErrLimited) {
		t.Fatalf("pinned eviction error = %v", err)
	}
	for _, release := range releases {
		release()
	}
	data, release, hit, err := cache.Read(context.Background(), secondKey, 8, digestHex(second), func(context.Context) ([]byte, error) { return second, nil })
	if err != nil || hit || string(data) != string(second) {
		t.Fatalf("replacement read = %q hit=%v err=%v", data, hit, err)
	}
	release()
	restarted, err := NewBlockCache(directory, 8, resource.NewBudget(64))
	if err != nil {
		t.Fatal(err)
	}
	_, release, hit, err = restarted.Read(context.Background(), secondKey, 8, digestHex(second), func(context.Context) ([]byte, error) {
		return nil, errors.New("restart should hit")
	})
	if err != nil || !hit {
		t.Fatalf("restart hit=%v err=%v", hit, err)
	}
	release()
}

func TestBlockCacheRejectsShortChangedAndCorruptCachedBlock(t *testing.T) {
	cache, err := NewBlockCache(filepath.Join(t.TempDir(), "cache"), 32, resource.NewBudget(64))
	if err != nil {
		t.Fatal(err)
	}
	want := []byte("expected")
	key := CacheBlockKey{InstallationID: "installation", ObjectID: "object", ContentSHA256: digestHex(want), BlockIndex: 0}
	for _, data := range [][]byte{[]byte("short"), []byte("changed!")} {
		if _, _, _, err := cache.Read(context.Background(), key, int64(len(want)), digestHex(want), func(context.Context) ([]byte, error) { return data, nil }); err == nil {
			t.Fatal("invalid range was accepted")
		}
	}
	_, release, _, err := cache.Read(context.Background(), key, int64(len(want)), digestHex(want), func(context.Context) ([]byte, error) { return want, nil })
	if err != nil {
		t.Fatal(err)
	}
	release()
	entries, _ := os.ReadDir(cache.directory)
	if len(entries) != 1 {
		t.Fatalf("entries = %d", len(entries))
	}
	if err := os.WriteFile(filepath.Join(cache.directory, entries[0].Name()), []byte("corrupt!"), 0o600); err != nil {
		t.Fatal(err)
	}
	var reloads int
	data, release, hit, err := cache.Read(context.Background(), key, int64(len(want)), digestHex(want), func(context.Context) ([]byte, error) {
		reloads++
		return want, nil
	})
	if err != nil || hit || reloads != 1 || string(data) != string(want) {
		t.Fatalf("corrupt recovery data=%q hit=%v reloads=%d err=%v", data, hit, reloads, err)
	}
	release()
}

func TestBlockCacheWaiterCancellationDoesNotReleaseLeader(t *testing.T) {
	cache, err := NewBlockCache(filepath.Join(t.TempDir(), "cache"), 32, resource.NewBudget(64))
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("expected")
	key := CacheBlockKey{InstallationID: "installation", ObjectID: "object", ContentSHA256: digestHex(data), BlockIndex: 0}
	started := make(chan struct{})
	unblock := make(chan struct{})
	leaderDone := make(chan error, 1)
	go func() {
		_, release, _, err := cache.Read(context.Background(), key, int64(len(data)), digestHex(data), func(context.Context) ([]byte, error) {
			close(started)
			<-unblock
			return data, nil
		})
		if release != nil {
			release()
		}
		leaderDone <- err
	}()
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, _, err := cache.Read(ctx, key, int64(len(data)), digestHex(data), func(context.Context) ([]byte, error) { return nil, nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("waiter error = %v", err)
	}
	close(unblock)
	if err := <-leaderDone; err != nil {
		t.Fatal(err)
	}
}

func TestBlockCacheCloseWaitsForActivePinAndPreservesFile(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "cache")
	disk := resource.NewBudget(64)
	cache, err := NewBlockCache(directory, 32, disk)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("expected")
	key := CacheBlockKey{InstallationID: "installation", ObjectID: "object", ContentSHA256: digestHex(data), BlockIndex: 0}
	_, release, _, err := cache.Read(context.Background(), key, int64(len(data)), digestHex(data), func(context.Context) ([]byte, error) { return data, nil })
	if err != nil {
		t.Fatal(err)
	}
	cache.Close()
	if disk.Used() != int64(len(data)) {
		t.Fatalf("active pin released during close: used=%d", disk.Used())
	}
	if _, _, _, err := cache.Read(context.Background(), key, int64(len(data)), digestHex(data), func(context.Context) ([]byte, error) { return data, nil }); !errors.Is(err, resource.ErrDraining) {
		t.Fatalf("closed read error=%v", err)
	}
	release()
	if disk.Used() != 0 {
		t.Fatalf("pin release used=%d", disk.Used())
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 1 {
		t.Fatalf("preserved entries=%d err=%v", len(entries), err)
	}
}
