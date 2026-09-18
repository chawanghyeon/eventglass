package resource

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
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
