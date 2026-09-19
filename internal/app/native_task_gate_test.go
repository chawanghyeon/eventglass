package app

import (
	"context"
	"errors"
	"testing"
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
