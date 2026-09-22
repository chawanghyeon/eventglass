package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/model"
)

func TestConversionClaimsPreferTenantWithoutRunningWork(t *testing.T) {
	checkConversionTenantTurn(t, "running")
}

// Actual PostgreSQL claim/Prepare/Publish transitions with catalog fixtures.
// This checks scheduler order, not native/S3 throughput or durable byte recovery.
func TestConversionClaimsRotateTenantAfterPreviousTurn(t *testing.T) {
	for _, state := range []string{"prepared", "completed", "expired"} {
		t.Run(state, func(t *testing.T) { checkConversionTenantTurn(t, state) })
	}
}

func TestConversionClaimsCursorBoundariesAndAuthority(t *testing.T) {
	fixture := setupAcceptFixture(t, 690)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	second := addSchedulingTenant(t, ctx, fixture, 696)
	for _, target := range []*acceptFixture{fixture, second} {
		label := fmt.Sprintf("cursor-%d", target.tenantID)
		batch := target.batch(t, 0, label, []control.VerifiedRequest{target.request(target.uuidForLane(0), label, "", "")})
		if _, err := control.Accept(ctx, fixture.pool, batch); err != nil {
			t.Fatal(err)
		}
	}
	operations, err := control.NewPublicationOperations(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	claim := func(ctx context.Context, generation, after int64) (*control.ConversionJob, error) {
		return operations.ClaimConversionAfterTenant(ctx, acceptInstallationID, generation, "cursor-worker", time.Minute, after)
	}
	if _, err := claim(ctx, 1, -1); err == nil {
		t.Fatal("negative scheduling cursor admitted")
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := claim(canceled, 1, 690); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled claim: %v", err)
	}
	if _, err := claim(ctx, 2, 690); !errors.Is(err, control.ErrJobFenceStale) {
		t.Fatalf("cursor bypassed generation fence: %v", err)
	}
	var attempts int
	if err := fixture.pool.QueryRow(ctx, `SELECT sum(attempt) FROM jobs`).Scan(&attempts); err != nil || attempts != 0 {
		t.Fatalf("rejected claims mutated attempts: %d %v", attempts, err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE jobs SET retry_at=clock_timestamp()+interval '1 hour' WHERE tenant_id=$1`, second.tenantID); err != nil {
		t.Fatal(err)
	}
	job, err := claim(ctx, 1, math.MaxInt64)
	if err != nil || job == nil || job.TenantID != fixture.tenantID {
		t.Fatalf("maximum cursor did not wrap safely: %+v %v", job, err)
	}
	if job, err := claim(ctx, 1, fixture.tenantID); err != nil || job != nil {
		t.Fatalf("cursor bypassed retry time or active lease: %+v %v", job, err)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE jobs SET retry_at=clock_timestamp() WHERE tenant_id=$1`, second.tenantID); err != nil {
		t.Fatal(err)
	}
	job, err = claim(ctx, 1, 693) // A disappeared/never-existing tenant is only a hint.
	if err != nil || job == nil || job.TenantID != second.tenantID {
		t.Fatalf("cursor gap hid ready tenant: %+v %v", job, err)
	}
}

func checkConversionTenantTurn(t *testing.T, state string) {
	t.Helper()
	fixture := setupAcceptFixture(t, 690)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	second := addSchedulingTenant(t, ctx, fixture, 691)
	seed := func(target *acceptFixture, lane int, label string) (control.VerifiedBatch, control.ReceiptResult) {
		request := target.request(target.uuidForLane(lane), label, "", "")
		batch := target.batch(t, lane, label, []control.VerifiedRequest{request})
		receipts, err := control.Accept(ctx, fixture.pool, batch)
		if err != nil {
			t.Fatal(err)
		}
		return batch, receipts[0]
	}
	firstBatch, firstReceipt := seed(fixture, 0, "scale-first-a")
	seed(fixture, 1, "scale-first-b")
	seed(second, 0, "scale-second")
	first, err := control.ClaimConversionJob(ctx, fixture.pool, acceptInstallationID, 1, "scale-worker-1", time.Minute)
	if err != nil || first == nil || first.TenantID != fixture.tenantID {
		t.Fatalf("first=%#v err=%v", first, err)
	}
	if state == "prepared" || state == "completed" {
		output := prepareFixtureOutput(t, fixture, firstBatch, firstReceipt, first, fixtureSHA("fair-turn"))
		if err := control.Prepare(ctx, fixture.pool, output.command); err != nil {
			t.Fatal(err)
		}
		if state == "completed" {
			publication, err := control.ClaimPublicationJob(ctx, fixture.pool, acceptInstallationID, 1, fixture.tenantID, 0, "scale-publisher", time.Minute)
			if err != nil || publication == nil {
				t.Fatalf("publication=%+v error=%v", publication, err)
			}
			if _, err := control.Publish(ctx, fixture.pool, publishCommand(*publication, fixture.tenantID, 0)); err != nil {
				t.Fatal(err)
			}
		}
	} else if state == "expired" {
		if _, err := fixture.pool.Exec(ctx, `UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE job_id=$1`, first.Authority.JobID); err != nil {
			t.Fatal(err)
		}
	}
	operations, err := control.NewPublicationOperations(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	owner := "scale-worker-1"
	if state == "running" {
		owner = "scale-worker-2"
	}
	next, err := operations.ClaimConversionAfterTenant(ctx, acceptInstallationID, 1, owner, time.Minute, first.TenantID)
	if err != nil || next == nil || next.TenantID != second.tenantID {
		t.Fatalf("fair next=%#v err=%v", next, err)
	}
	if (state == "running" && first.Authority.Owner == next.Authority.Owner) || first.TenantID == next.TenantID {
		t.Fatal("claims did not spread across workers and tenants")
	}
}

func addSchedulingTenant(t *testing.T, ctx context.Context, fixture *acceptFixture, tenantID int64) *acceptFixture {
	t.Helper()
	second := &acceptFixture{pool: fixture.pool, tenantID: tenantID, projectID: tenantID*10 + 1, keyHash: sha256.Sum256([]byte(fmt.Sprintf("scheduling-tenant-%d", tenantID)))}
	second.auth = control.AuthorizationSnapshot{TenantRevision: 1, ProjectRevision: 1, KeyRevision: 1, ScrubRevision: 1, ConfigRevision: 1, KeyHash: second.keyHash}
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
	return second
}
