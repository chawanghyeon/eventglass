package app

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
)

func TestValidateLiveRequestBoundsCatchupWindow(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	filter := &query.Node{Op: "constant", Constant: true}
	canonical, _ := query.CanonicalFilter(filter)
	request := query.PublicLiveRequest{TenantID: 1, ProjectIDs: []int64{2}, Kinds: []model.Kind{model.KindLog}, Filter: filter, Canonical: canonical}
	inside := now.Add(-15 * time.Minute)
	request.CatchupStart = &inside
	if err := validateLiveRequest(request, now); err != nil {
		t.Fatalf("boundary catchup rejected: %v", err)
	}
	outside := inside.Add(-time.Microsecond)
	request.CatchupStart = &outside
	if err := validateLiveRequest(request, now); err == nil {
		t.Fatal("stale catchup accepted")
	}
}

func TestEmitBoundedLiveRejectsPendingOverflowBeforeCallback(t *testing.T) {
	called := false
	err := emitBoundedLive(func(query.LiveEvent) error {
		called = true
		return nil
	}, query.LiveEvent{Type: "rows", Data: strings.Repeat("x", query.LiveMaximumPending)})
	if !errors.Is(err, errLivePendingLimit) || called {
		t.Fatalf("err=%v called=%t", err, called)
	}
}
