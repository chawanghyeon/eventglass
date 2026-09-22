package maintenance

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/resource"
)

type downloadStore struct {
	CompactionStore
	download func(context.Context, string, string, int64, string, int64) error
}

func (s downloadStore) DownloadToFile(ctx context.Context, key, path string, size int64, checksum string, limit int64) error {
	return s.download(ctx, key, path, size, checksum, limit)
}

func downloadWork(count int) control.CompactionWork {
	work := control.CompactionWork{Inputs: make([]control.CompactionWorkInput, count)}
	for index := range work.Inputs {
		work.Inputs[index] = control.CompactionWorkInput{
			BundleID: fmt.Sprintf("bundle-%03d", index), IdentitySHA256: fmt.Sprintf("%064x", index+1),
			Analytics: control.CompactionFile{ObjectKey: fmt.Sprintf("analytics-%03d", index), Bytes: 8, SHA256: strings.Repeat("a", 64)},
			Payload:   control.CompactionFile{ObjectKey: fmt.Sprintf("payload-%03d", index), Bytes: 8, SHA256: strings.Repeat("b", 64)},
		}
	}
	return work
}

func TestDownloadInputsBoundedConcurrentAndOrdered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()
		work := downloadWork(control.MaxCompactionInputs)
		var active, peak, calls atomic.Int32
		var once sync.Once
		released := make(chan struct{})
		release := func() { once.Do(func() { close(released) }) }
		defer release()
		store := downloadStore{download: func(ctx context.Context, key, path string, size int64, checksum string, limit int64) error {
			current := active.Add(1)
			defer active.Add(-1)
			for previous := peak.Load(); current > previous && !peak.CompareAndSwap(previous, current); previous = peak.Load() {
			}
			calls.Add(1)
			if size != 8 || len(checksum) != 64 || limit != engine.MaxBundleFileBytes {
				return errors.New("lost verified-download admission")
			}
			<-released
			if err := ctx.Err(); err != nil {
				return err
			}
			return os.WriteFile(path, []byte(key), 0o600)
		}}
		var inputs []engine.CompactionInput
		var err error
		done := make(chan struct{})
		go func() {
			inputs, err = downloadInputs(context.Background(), store, root, work)
			close(done)
		}()
		synctest.Wait()
		if active.Load() != 4 || calls.Load() != 4 {
			initialActive, initialCalls := active.Load(), calls.Load()
			release()
			<-done
			t.Fatalf("want exactly four bounded initial reads: active=%d calls=%d", initialActive, initialCalls)
		}
		release()
		<-done
		if err != nil || len(inputs) != len(work.Inputs) || peak.Load() != 4 || active.Load() != 0 || calls.Load() != 256 {
			t.Fatalf("inputs=%d error=%v peak=%d active=%d calls=%d", len(inputs), err, peak.Load(), active.Load(), calls.Load())
		}
		for index, input := range inputs {
			if input.BundleID != work.Inputs[index].BundleID || input.IdentitySHA256 != work.Inputs[index].IdentitySHA256 {
				t.Fatalf("reordered identity at %d", index)
			}
			for role, path := range []string{input.AnalyticsPath, input.PayloadPath} {
				expected := []string{work.Inputs[index].Analytics.ObjectKey, work.Inputs[index].Payload.ObjectKey}[role]
				data, err := os.ReadFile(path)
				if err != nil || string(data) != expected {
					t.Fatalf("input=%d role=%d data=%q error=%v", index, role, data, err)
				}
				info, err := os.Stat(filepath.Dir(path))
				if err != nil || info.Mode().Perm() != 0o700 {
					t.Fatalf("non-private input directory: %v", err)
				}
			}
		}
	})
}

type downloadControl struct {
	CompactionControl
	work control.CompactionWork
}

func (c downloadControl) LoadCompaction(context.Context, control.MaintenanceAuthority) (control.CompactionWork, error) {
	return c.work, nil
}

type forbiddenDownloadRunner struct{ calls atomic.Int32 }

func (r *forbiddenDownloadRunner) Run(context.Context, engine.CompactionRequest) (engine.CompactionResult, error) {
	r.calls.Add(1)
	return engine.CompactionResult{}, errors.New("native runner must not start after failed download")
}

func TestDownloadFailureJoinsBeforeWorkflowCleanup(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("provider-failure-%t", fail), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var active, calls atomic.Int32
				var once sync.Once
				joined := make(chan struct{})
				release := func() { once.Do(func() { close(joined) }) }
				defer release()
				failed := make(chan struct{})
				providerErr := errors.New("provider body failed")
				store := downloadStore{download: func(ctx context.Context, key, path string, _ int64, _ string, _ int64) error {
					active.Add(1)
					defer active.Add(-1)
					calls.Add(1)
					if err := os.WriteFile(path, []byte("partial"), 0o600); err != nil {
						return err
					}
					if fail && key == "analytics-000" {
						<-failed
						return providerErr
					}
					<-ctx.Done()
					<-joined // Models a body/close that has not actually returned yet.
					return ctx.Err()
				}}
				runner := &forbiddenDownloadRunner{}
				budget := resource.NewBudget(4 << 30)
				workflow := Workflow{Control: downloadControl{work: downloadWork(16)}, Store: store, Runner: runner,
					InstallationID: "download-test", ScratchDir: filepath.Join(t.TempDir(), "scratch"), Disk: budget}
				done := make(chan error, 1)
				go func() {
					done <- workflow.CompactAndPrepare(ctx, control.CompactionTask{Authority: control.MaintenanceAuthority{InstallationID: "download-test"}})
				}()
				synctest.Wait()
				if active.Load() != 4 || budget.Used() == 0 {
					if fail {
						close(failed)
					}
					cancel()
					release()
					<-done
					t.Fatalf("download admission active=%d disk=%d", active.Load(), budget.Used())
				}
				if fail {
					close(failed)
				} else {
					cancel()
				}
				synctest.Wait()
				select {
				case err := <-done:
					t.Fatalf("returned before provider joined: %v", err)
				default:
				}
				entries, err := os.ReadDir(workflow.ScratchDir)
				if err != nil || len(entries) != 1 || budget.Used() == 0 || runner.calls.Load() != 0 || calls.Load() != 4 {
					t.Fatalf("early cleanup entries=%d disk=%d runner=%d calls=%d err=%v", len(entries), budget.Used(), runner.calls.Load(), calls.Load(), err)
				}
				release()
				err = <-done
				want := context.Canceled
				if fail {
					want = providerErr
				}
				if !errors.Is(err, want) || active.Load() != 0 || budget.Used() != 0 || runner.calls.Load() != 0 {
					t.Fatalf("join/cleanup err=%v active=%d disk=%d runner=%d", err, active.Load(), budget.Used(), runner.calls.Load())
				}
				entries, err = os.ReadDir(workflow.ScratchDir)
				if err != nil || len(entries) != 0 {
					t.Fatalf("scratch residue=%d err=%v", len(entries), err)
				}
			})
		})
	}
}

func TestDownloadInputsAdmissionAndRetry(t *testing.T) {
	root := t.TempDir()
	var calls int
	store := downloadStore{download: func(ctx context.Context, key, path string, _ int64, _ string, _ int64) error {
		calls++ // Single-input fixture: no simultaneous provider calls.
		return os.WriteFile(path, []byte(key), 0o600)
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := downloadInputs(ctx, store, root, downloadWork(1)); !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatalf("canceled admission err=%v calls=%d", err, calls)
	}
	if _, err := downloadInputs(context.Background(), store, root, downloadWork(control.MaxCompactionInputs+1)); err == nil || calls != 0 {
		t.Fatalf("unbounded admission err=%v calls=%d", err, calls)
	}
	if _, err := downloadInputs(context.Background(), nil, root, downloadWork(1)); err == nil {
		t.Fatal("nil provider accepted")
	}
	inputDir := filepath.Join(root, "input-000")
	if err := os.Mkdir(inputDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := downloadInputs(context.Background(), store, root, downloadWork(1)); err == nil || calls != 0 {
		t.Fatalf("existing private input overwritten err=%v calls=%d", err, calls)
	}
	if err := os.Remove(inputDir); err != nil {
		t.Fatal(err)
	}
	inputs, err := downloadInputs(context.Background(), store, root, downloadWork(1))
	if err != nil || len(inputs) != 1 || calls != 2 {
		t.Fatalf("retry inputs=%d calls=%d err=%v", len(inputs), calls, err)
	}
}
