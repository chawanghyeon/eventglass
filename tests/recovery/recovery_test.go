//go:build recovery

package recovery

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/storage"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestActivatedRestoreReadsVerifiedOldGenerationWithoutAdoptingNewerObject(t *testing.T) {
	if os.Getenv("EVENTGLASS_RECOVERY_REQUIRED") != "1" {
		t.Fatal("recovery gate environment is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, os.Getenv("EVENTGLASS_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var generation int64
	var recovery string
	var paused, sessionRevoked bool
	var oldGeneration, activated int64
	if err := pool.QueryRow(ctx, `SELECT i.storage_generation,i.recovery_state,i.alerts_paused,
		EXISTS(SELECT 1 FROM sessions WHERE revoked_at IS NOT NULL),
		(SELECT storage_generation FROM object_intents WHERE object_key='referenced.bin'),
		(SELECT count(*) FROM recovery_verifications WHERE state='activated')
		FROM installations i WHERE i.singleton`).Scan(&generation, &recovery, &paused, &sessionRevoked, &oldGeneration, &activated); err != nil {
		t.Fatal(err)
	}
	if generation != 2 || recovery != "ready" || !paused || !sessionRevoked || oldGeneration != 1 || activated != 1 {
		t.Fatalf("generation=%d recovery=%s paused=%v session_revoked=%v old_generation=%d activated=%d", generation, recovery, paused, sessionRevoked, oldGeneration, activated)
	}
	var adopted int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM object_intents WHERE object_key='newer-unreferenced.bin'`).Scan(&adopted); err != nil {
		t.Fatal(err)
	}
	if adopted != 0 {
		t.Fatalf("newer unreferenced object was adopted: %d", adopted)
	}
	region := os.Getenv("EVENTGLASS_S3_REGION")
	bucket := os.Getenv("EVENTGLASS_S3_BUCKET")
	prefix := os.Getenv("EVENTGLASS_S3_PREFIX")
	if region == "" || bucket == "" || prefix == "" {
		t.Fatal("recovery S3 region, bucket, and prefix are required")
	}
	store, err := storage.NewS3Store(ctx, storage.S3Config{Endpoint: os.Getenv("EVENTGLASS_S3_ENDPOINT"), Region: region, Bucket: bucket, Prefix: prefix, PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	bytes, err := store.ReadRange(ctx, "referenced.bin", 0, int64(len("restored-reference")))
	if err != nil || string(bytes) != "restored-reference" {
		t.Fatalf("old-generation object=%q err=%v", bytes, err)
	}
}
