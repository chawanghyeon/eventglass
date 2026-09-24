package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// Run this separately under a Linux ARM64 CPU/memory cgroup. It builds only the
// selected packed layout, then checks a broad exact query against the oracle.
func TestPackedResource(t *testing.T) {
	if os.Getenv("EVENTGLASS_LAYOUT_RESOURCE") != "1" {
		t.Skip("set EVENTGLASS_LAYOUT_RESOURCE=1 under a bounded container")
	}
	docs := generated(1_000_000, 4093)
	dir := t.TempDir()
	if _, err := buildPacked(dir, docs, 100_000, 256); err != nil {
		t.Fatal(err)
	}
	c, err := load(dir)
	if err != nil {
		t.Fatal(err)
	}
	raw := filepath.Join(dir, "raw.jsonl.z")
	if _, err := writeRaw(raw, docs); err != nil {
		t.Fatal(err)
	}
	q := query{Terms: []string{"request"}, Tenant: 3, K: 10}
	want, err := scanRaw(raw, c, q)
	if err != nil {
		t.Fatal(err)
	}
	got, err := run(context.Background(), localSource{dir: dir}, c, q, denseColumns)
	if err != nil || !equivalent(got, want) {
		t.Fatalf("packed query differs from oracle: %v equivalent=%v", err, equivalent(got, want))
	}
	finished := make(chan error, 4)
	for range 4 {
		go func() {
			value, err := run(context.Background(), localSource{dir: dir}, c, q, denseColumns)
			if err == nil && !equivalent(value, want) {
				err = fmt.Errorf("concurrent query differs from oracle")
			}
			finished <- err
		}()
	}
	for range 4 {
		if err := <-finished; err != nil {
			t.Fatal(err)
		}
	}
	stored, err := storedSize(dir, c)
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	t.Logf("documents=%d count=%d groups=%d stored_bytes=%d heap_alloc=%d heap_sys=%d", c.Count, got.Count, len(got.Groups), stored, mem.HeapAlloc, mem.HeapSys)
	for _, name := range []string{"memory.peak", "memory.events"} {
		if data, err := os.ReadFile(filepath.Join("/sys/fs/cgroup", name)); err == nil {
			t.Logf("cgroup_%s=%s", name, strings.TrimSpace(string(data)))
		}
	}
}
