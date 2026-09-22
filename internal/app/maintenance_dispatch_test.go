package app

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestMaintenanceDeadlineWaitsForActualWorkAndChargesOverrun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runtime := &Runtime{nativeTasks: NewNativeTaskGate()}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		deadlineReached, finishWork, joined := make(chan struct{}), make(chan struct{}), make(chan struct{})
		work := []workerOperation{
			{"conversion", func(context.Context) (bool, error) { return false, nil }},
			{"compaction", func(taskContext context.Context) (bool, error) {
				release, err := runtime.nativeTasks.acquire(taskContext)
				if err != nil {
					t.Error(err)
					return false, err
				}
				defer release()
				<-taskContext.Done()
				close(deadlineReached)
				<-finishWork
				return true, taskContext.Err()
			}},
		}
		go func() { defer close(joined); _ = runtime.runWorkerGroup(ctx, work) }()
		<-deadlineReached
		cancel()
		synctest.Wait()
		if runtime.nativeTasks.used() != 1 {
			t.Fatal("deadline released live native ownership")
		}
		select {
		case <-joined:
			t.Fatal("worker returned before actual maintenance stopped")
		default:
		}
		// Deliberately exceed the reserved join allowance. Accounting must not
		// pretend work ended when cancellation was requested.
		time.Sleep(3 * time.Second)
		close(finishWork)
		<-joined
		if runtime.nativeTasks.used() != 0 {
			t.Fatal("joined maintenance retained native ownership")
		}
		if runtime.stats.entries["maintenance_budget_overrun"].calls != 1 || runtime.stats.entries["compaction"].elapsed < 3*time.Second {
			t.Fatalf("missing actual join/overrun accounting: %#v", runtime.stats.entries)
		}
	})
}

func TestMaintenanceAlwaysYieldsToNewForegroundWork(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "completed", true: "failed"}[failed], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				runtime := &Runtime{}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				ready := false
				var finished time.Time
				work := []workerOperation{
					{"conversion", func(context.Context) (bool, error) {
						if ready {
							if !time.Now().Equal(finished) {
								t.Error("maintenance backoff delayed ready foreground work")
							}
							cancel()
							return true, nil
						}
						return false, nil
					}},
					{"compaction", func(context.Context) (bool, error) {
						time.Sleep(50 * time.Millisecond)
						ready, finished = true, time.Now()
						if failed {
							return false, errors.New("maintenance failed")
						}
						return true, nil
					}},
					{"gc", func(context.Context) (bool, error) {
						t.Fatal("maintenance slot stole ready foreground dispatch")
						return false, nil
					}},
				}
				if err := runtime.runWorkerGroup(ctx, work); err != nil {
					t.Fatal(err)
				}
				if !ready {
					t.Fatal("idle time never admitted maintenance")
				}
			})
		})
	}
}

func TestFailedForegroundClaimsNeverEarnMaintenanceCredit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runtime := &Runtime{}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		work := []workerOperation{
			{"conversion", func(context.Context) (bool, error) { return false, errors.New("database unavailable") }},
			{"compaction", func(context.Context) (bool, error) {
				t.Error("dependency backoff was counted as spare time")
				return false, nil
			}},
		}
		if err := runtime.runWorkerGroup(ctx, work); err != nil {
			t.Fatal(err)
		}
		if runtime.stats.entries["maintenance_spare"].calls != 0 {
			t.Fatal("failed foreground claim earned credit")
		}
	})
}

func TestSharedNativeHelperPreventsFalseIdleCredit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runtime := &Runtime{nativeTasks: NewNativeTaskGate()}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		helperJoined := make(chan struct{})
		go func() {
			defer close(helperJoined)
			for ctx.Err() == nil {
				time.Sleep(25 * time.Millisecond)
				release, err := runtime.nativeTasks.acquire(ctx)
				if err != nil {
					return
				}
				time.Sleep(25 * time.Millisecond)
				release()
			}
		}()
		work := []workerOperation{
			{"conversion", func(context.Context) (bool, error) { return false, nil }},
			{"compaction", func(context.Context) (bool, error) {
				t.Error("busy native helper was counted as spare")
				return false, nil
			}},
		}
		if err := runtime.runWorkerGroup(ctx, work); err != nil {
			t.Fatal(err)
		}
		<-helperJoined
		if runtime.stats.entries["maintenance_spare"].calls != 0 {
			t.Fatal("native activity between idle samples was missed")
		}
	})
}

func TestMaintenanceMakesProgressWithShortSpareIntervals(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runtime := &Runtime{}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		nextForeground := time.Now()
		completed := 0
		work := []workerOperation{
			{"conversion", func(context.Context) (bool, error) {
				if time.Now().Before(nextForeground) {
					return false, nil
				}
				nextForeground = nextForeground.Add(time.Second)
				time.Sleep(700 * time.Millisecond)
				return true, nil
			}},
			{"compaction", func(ctx context.Context) (bool, error) {
				time.Sleep(100 * time.Millisecond)
				if err := ctx.Err(); err != nil {
					return true, err
				}
				completed++
				return true, nil
			}},
		}
		if err := runtime.runWorkerGroup(ctx, work); err != nil {
			t.Fatal(err)
		}
		if completed == 0 {
			t.Fatal("short measured spare intervals never admitted bounded maintenance")
		}
		spare, spent := runtime.stats.entries["maintenance_spare"].busy, runtime.stats.entries["compaction"].elapsed
		if spent > spare/4 || runtime.stats.entries["maintenance_budget_overrun"].calls != 0 {
			t.Fatalf("progress escaped spare budget: idle=%s work=%s", spare, spent)
		}
	})
}

func TestBudgetCanceledLargeMaintenanceEventuallyGetsFreshSpareWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runtime := &Runtime{nativeTasks: NewNativeTaskGate()}
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()
		var nextClaim time.Time
		attempts, completed, otherClaims := 0, 0, 0
		work := []workerOperation{
			{"conversion", func(context.Context) (bool, error) { return false, nil }},
			{"retention", func(context.Context) (bool, error) {
				if attempts > 0 {
					otherClaims++
				}
				return false, nil
			}},
			{"compaction", func(taskContext context.Context) (bool, error) {
				if time.Now().Before(nextClaim) {
					return false, nil
				}
				attempts++
				// Match the real canceled claim's lease expiry, not an immediate
				// retry that changes the production failure trajectory.
				nextClaim = time.Now().Add(workerLease)
				select {
				case <-taskContext.Done():
					return true, taskContext.Err()
				case <-time.After(9 * time.Second):
					completed++
					cancel()
					return true, nil
				}
			}},
		}
		if err := runtime.runWorkerGroup(ctx, work); err != nil {
			t.Fatal(err)
		}
		if completed != 1 || attempts < 2 || otherClaims == 0 {
			t.Fatalf("large work starved or froze other maintenance: completed=%d attempts=%d other=%d", completed, attempts, otherClaims)
		}
		idle := runtime.stats.entries["maintenance_spare"].busy
		spent := runtime.stats.entries["compaction"].elapsed + runtime.stats.entries["retention"].elapsed
		if spent > idle/4 || runtime.stats.entries["maintenance_budget_overrun"].calls != 0 {
			t.Fatalf("retry progress bypassed accounting: idle=%s work=%s", idle, spent)
		}
	})
}
