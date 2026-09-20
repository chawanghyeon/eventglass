package integration

import (
	"context"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
)

func TestInstallationRetentionPreservesDedupeAfterWidenThenShrink(t *testing.T) {
	f := setupAcceptFixture(t, 1810)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	first := f.batch(t, 0, "retention-authority-first", []control.VerifiedRequest{f.request(f.uuidForLane(0), "first", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", fixtureSHA("same"))})
	if _, err := control.Accept(ctx, f.pool, first); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `UPDATE event_dedupe SET created_at=clock_timestamp()-interval '40 days',expires_at=clock_timestamp()-interval '1 day' WHERE tenant_id=$1`, f.tenantID); err != nil {
		t.Fatal(err)
	}
	ops, _ := control.NewMaintenanceOperations(f.pool)
	revision, _, err := ops.ChangeRetentionPolicy(ctx, acceptInstallationID, 1, 1, 90)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := ops.ChangeRetentionPolicy(ctx, acceptInstallationID, 1, revision, 1); err != nil {
		t.Fatal(err)
	}
	second := f.batch(t, 0, "retention-authority-second", []control.VerifiedRequest{f.request(f.uuidForLane(0), "second", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", fixtureSHA("same"))})
	receipts, err := control.Accept(ctx, f.pool, second)
	if err != nil || len(receipts) != 1 || receipts[0].AcceptedCount != 0 || receipts[0].DuplicateCount != 1 {
		t.Fatalf("dedupe promise lost: %#v err=%v", receipts, err)
	}
}
