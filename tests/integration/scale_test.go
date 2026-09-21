package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/model"
)

func TestConversionClaimsPreferTenantWithoutRunningWork(t *testing.T) {
	fixture := setupAcceptFixture(t, 690)
	second := &acceptFixture{pool: fixture.pool, tenantID: 691, projectID: 6911, keyHash: sha256.Sum256([]byte("scale-second-tenant"))}
	second.auth = control.AuthorizationSnapshot{TenantRevision: 1, ProjectRevision: 1, KeyRevision: 1, ScrubRevision: 1, ConfigRevision: 1, KeyHash: second.keyHash}
	ctx := context.Background()
	keyID := second.uuidForLane(0)
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO tenants(tenant_id) VALUES($1)`, second.tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO projects(tenant_id,project_id,scrub_revision) VALUES($1,$2,1)`, second.tenantID, second.projectID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO project_keys(tenant_id,project_id,key_hash,key_id,key_prefix) VALUES($1,$2,$3,$4,$5)`, second.tenantID, second.projectID, second.keyHash[:], keyID, hex.EncodeToString(second.keyHash[:4])); err != nil {
		t.Fatal(err)
	}
	for lane := 0; lane < model.LaneCount; lane++ {
		if _, err := fixture.pool.Exec(ctx, `INSERT INTO lanes(tenant_id,lane_id) VALUES($1,$2)`, second.tenantID, lane); err != nil {
			t.Fatal(err)
		}
	}
	seed := func(target *acceptFixture, lane int, label string) {
		request := target.request(target.uuidForLane(lane), label, "", "")
		batch := target.batch(t, lane, label, []control.VerifiedRequest{request})
		if _, err := control.Accept(ctx, fixture.pool, batch); err != nil {
			t.Fatal(err)
		}
	}
	seed(fixture, 0, "scale-first-a")
	seed(fixture, 1, "scale-first-b")
	seed(second, 0, "scale-second")
	first, err := control.ClaimConversionJob(ctx, fixture.pool, acceptInstallationID, 1, "scale-worker-1", time.Minute)
	if err != nil || first == nil || first.TenantID != fixture.tenantID {
		t.Fatalf("first=%#v err=%v", first, err)
	}
	next, err := control.ClaimConversionJob(ctx, fixture.pool, acceptInstallationID, 1, "scale-worker-2", time.Minute)
	if err != nil || next == nil || next.TenantID != second.tenantID {
		t.Fatalf("fair next=%#v err=%v", next, err)
	}
	if first.Authority.Owner == next.Authority.Owner || first.TenantID == next.TenantID {
		t.Fatal("claims did not spread across workers and tenants")
	}
}
