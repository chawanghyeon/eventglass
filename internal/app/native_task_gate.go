package app

import "context"

// NativeTaskGate limits one runtime process to one isolated native child. The
// same gate is shared by conversion, query execution, and public-result export.
type NativeTaskGate struct {
	slot chan struct{}
}

func NewNativeTaskGate() *NativeTaskGate {
	gate := &NativeTaskGate{slot: make(chan struct{}, 1)}
	gate.slot <- struct{}{}
	return gate
}

func (gate *NativeTaskGate) acquire(ctx context.Context) (func(), error) {
	if gate == nil {
		return func() {}, nil
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-gate.slot:
		return func() { gate.slot <- struct{}{} }, nil
	}
}
