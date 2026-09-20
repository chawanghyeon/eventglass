package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/resource"
)

func TestNativeTaskGateSerializesAndHonorsCancellation(t *testing.T) {
	gate := NewNativeTaskGate()
	release, err := gate.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := gate.acquire(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("second acquire error = %v", err)
	}
	release()
	second, err := gate.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second()
}

func TestNativeGateSharesDecoderBudgetAndReturnsItAfterJoin(t *testing.T) {
	budget := resource.NewBudget(192 << 20)
	decoder, err := budget.Acquire(1)
	if err != nil {
		t.Fatal(err)
	}
	gate := NewNativeTaskGate()
	gate.working, gate.reservation, gate.memory = budget, 192<<20, 192<<20
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := gate.acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("overlapped decoder: %v", err)
	}
	decoder.Release()
	release, err := gate.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := budget.Acquire(1); !errors.Is(err, resource.ErrLimited) {
		t.Fatal("native memory not reserved")
	}
	if gate.memoryLimit(256<<20) != 192<<20 {
		t.Fatal("combined native memory not clamped")
	}
	release()
	release()
	if budget.Used() != 0 {
		t.Fatal("native budget leaked")
	}
}
