package app

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/chawanghyeon/eventglass/internal/resource"
)

// NativeTaskGate limits one runtime process to one isolated native child. The
// same gate is shared by conversion, query execution, and public-result export.
type NativeTaskGate struct {
	slot        chan struct{}
	working     *resource.Budget
	reservation int64
	memory      int64
	started     atomic.Uint64
}

func NewNativeTaskGate() *NativeTaskGate {
	gate := &NativeTaskGate{slot: make(chan struct{}, 1)}
	gate.slot <- struct{}{}
	return gate
}

func (gate *NativeTaskGate) acquire(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if gate == nil {
		return func() {}, nil
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-gate.slot:
		gate.started.Add(1)
		var permit *resource.Permit
		if gate.working != nil {
			var err error
			permit, err = gate.working.AcquireContext(ctx, gate.reservation)
			if err != nil {
				gate.slot <- struct{}{}
				return nil, err
			}
		}
		var once sync.Once
		return func() {
			once.Do(func() {
				if permit != nil {
					permit.Release()
				}
				gate.slot <- struct{}{}
			})
		}, nil
	}
}

// An idle sample is usable only if no shared native task started during it.
// Checking occupancy at its two endpoints alone would miss a short API helper.
func (gate *NativeTaskGate) activity() (uint64, bool) {
	if gate == nil {
		return 0, false
	}
	return gate.started.Load(), gate.used() != 0
}

func (gate *NativeTaskGate) memoryLimit(requested int64) int64 {
	if gate == nil || gate.memory == 0 {
		return requested
	}
	if requested == 0 || requested > gate.memory {
		return gate.memory
	}
	return requested
}

func (gate *NativeTaskGate) used() int64 {
	if gate == nil || len(gate.slot) == 1 {
		return 0
	}
	return 1
}
