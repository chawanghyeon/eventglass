package resource

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"
)

func TestBudgetSharedOwnershipDrainAndOverflow(t *testing.T) {
	b := NewBudget(10)
	first, err := b.Acquire(6)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Acquire(5); !errors.Is(err, ErrLimited) {
		t.Fatal("overcommit")
	}
	if _, err := b.Acquire(math.MaxInt64); !errors.Is(err, ErrLimited) {
		t.Fatal("overflow")
	}
	second, err := b.Acquire(4)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Drain(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("drain=%v", err)
	}
	if b.Used() != 10 {
		t.Fatal("canceled drain freed live allocations")
	}
	if _, err := b.Acquire(1); !errors.Is(err, ErrDraining) {
		t.Fatal("drain reopened")
	}
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() { first.Release(); second.Release() })
	}
	wg.Wait()
	if b.Used() != 0 {
		t.Fatal("permit leaked or double released")
	}
	if err := b.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestBudgetWaitingAdmissionReleaseAndDrain(t *testing.T) {
	for _, drain := range []bool{false, true} {
		b := NewBudget(1)
		owner, _ := b.Acquire(1)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		result := make(chan error, 1)
		go func() {
			permit, err := b.AcquireContext(ctx, 1)
			if permit != nil {
				permit.Release()
			}
			result <- err
		}()
		if drain {
			canceled, stop := context.WithCancel(ctx)
			stop()
			_ = b.Drain(canceled)
		}
		owner.Release()
		err := <-result
		cancel()
		if drain && !errors.Is(err, ErrDraining) || !drain && err != nil {
			t.Fatalf("drain=%v admission=%v", drain, err)
		}
		if b.Used() != 0 {
			t.Fatal("waiting admission leaked reservation")
		}
	}
}
