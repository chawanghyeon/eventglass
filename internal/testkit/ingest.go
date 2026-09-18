// Package testkit contains non-durable test receivers. Production packages
// must not depend on this package (enforced by the architecture check).
package testkit

import (
	"context"
	"github.com/chawanghyeon/eventglass/internal/ingest"
	"github.com/chawanghyeon/eventglass/internal/model"
	"sync"
)

type MemorySink struct {
	mu      sync.Mutex
	batches []model.NormalizedRequest
	err     error
}

func (s *MemorySink) Accept(_ context.Context, command ingest.Command) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.batches = append(s.batches, command.Request)
	return nil
}

func (s *MemorySink) Batches() []model.NormalizedRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]model.NormalizedRequest(nil), s.batches...)
}

func (s *MemorySink) SetError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}
