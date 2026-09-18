// Package resource owns byte reservations and drain admission across roles.
package resource

import (
	"context"
	"errors"
	"sync"
)

var ErrLimited = errors.New("resource budget exhausted")
var ErrDraining = errors.New("resource budget is draining")

type Budget struct {
	mu          sync.Mutex
	limit, used int64
	draining    bool
	drained     chan struct{}
}

func NewBudget(limit int64) *Budget {
	if limit <= 0 {
		panic("resource budget must be positive")
	}
	return &Budget{limit: limit, drained: make(chan struct{})}
}

// Permit must stay with the live allocation through queueing, upload and
// commit. Ownership transfer does not create a second reservation.
type Permit struct {
	budget *Budget
	bytes  int64
	once   sync.Once
}

func (b *Budget) Acquire(bytes int64) (*Permit, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.draining {
		return nil, ErrDraining
	}
	if bytes <= 0 || bytes > b.limit-b.used {
		return nil, ErrLimited
	}
	b.used += bytes
	return &Permit{budget: b, bytes: bytes}, nil
}

func (p *Permit) Release() {
	p.once.Do(func() {
		b := p.budget
		b.mu.Lock()
		defer b.mu.Unlock()
		b.used -= p.bytes
		if b.draining && b.used == 0 {
			close(b.drained)
		}
	})
}

// Drain rejects new reservations immediately. A timed-out drain never reopens
// admission or releases reservations still owned by running work.
func (b *Budget) Drain(ctx context.Context) error {
	b.mu.Lock()
	if !b.draining {
		b.draining = true
		if b.used == 0 {
			close(b.drained)
		}
	}
	b.mu.Unlock()
	select {
	case <-b.drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Budget) Used() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}
