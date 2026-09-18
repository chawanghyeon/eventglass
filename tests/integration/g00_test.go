package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/storage"
	"github.com/jackc/pgx/v5"
)

func requiredEnvironment(t *testing.T, names ...string) map[string]string {
	t.Helper()
	values := make(map[string]string, len(names))
	for _, name := range names {
		value := os.Getenv(name)
		if value == "" {
			if os.Getenv("EVENTGLASS_INTEGRATION_REQUIRED") == "1" {
				t.Fatalf("required integration environment variable %s is missing", name)
			}
			t.Skipf("set %s to run this integration contract", name)
		}
		values[name] = value
	}
	return values
}

func TestPostgreSQLMigrationChecksumsAndRollback(t *testing.T) {
	environment := requiredEnvironment(t, "EVENTGLASS_DATABASE_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	databaseURL := environment["EVENTGLASS_DATABASE_URL"]
	if err := control.ApplyMigrations(ctx, databaseURL); err != nil {
		t.Fatal(err)
	}
	connection, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(ctx)
	var major int
	if err := connection.QueryRow(ctx, "SELECT current_setting('server_version_num')::int / 10000").Scan(&major); err != nil {
		t.Fatal(err)
	}
	if major != 17 {
		t.Fatalf("PostgreSQL major = %d, want 17", major)
	}

	manifest, err := control.MigrationManifest()
	if err != nil {
		t.Fatal(err)
	}
	for i := len(manifest) - 1; i >= 0; i-- {
		tx, err := connection.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, manifest[i].DownSQL); err != nil {
			tx.Rollback(ctx)
			t.Fatalf("rollback migration %d: %v", manifest[i].Version, err)
		}
		if _, err := tx.Exec(ctx, "DELETE FROM schema_migrations WHERE version=$1", manifest[i].Version); err != nil {
			tx.Rollback(ctx)
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := control.ApplyMigrations(ctx, databaseURL); err != nil {
		t.Fatalf("reapply after rollback: %v", err)
	}

	if _, err := connection.Exec(ctx, "UPDATE schema_migrations SET sha256=repeat('0',64) WHERE version=$1", manifest[0].Version); err != nil {
		t.Fatal(err)
	}
	if err := control.ApplyMigrations(ctx, databaseURL); err == nil {
		t.Fatal("migration checksum drift was accepted")
	}
	if _, err := connection.Exec(ctx, "UPDATE schema_migrations SET sha256=$1 WHERE version=$2", manifest[0].SHA256, manifest[0].Version); err != nil {
		t.Fatal(err)
	}
}

func TestS3PutRangeListMultipartAbortAndPermissions(t *testing.T) {
	environment := requiredEnvironment(t, "EVENTGLASS_S3_ENDPOINT", "EVENTGLASS_S3_BUCKET", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	settings := storage.S3Config{
		Endpoint: environment["EVENTGLASS_S3_ENDPOINT"], Region: "us-east-1", Bucket: environment["EVENTGLASS_S3_BUCKET"], Prefix: "integration-owned",
		AccessKeyID: environment["AWS_ACCESS_KEY_ID"], SecretAccessKey: environment["AWS_SECRET_ACCESS_KEY"], PathStyle: true,
	}
	store, err := storage.NewS3Store(ctx, settings)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("eventglass-range-contract-"), 100000)
	info, err := store.Put(ctx, "contracts/single.bin", data)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	if info.SHA256 != hex.EncodeToString(digest[:]) || info.Size != int64(len(data)) {
		t.Fatalf("unexpected object info: %+v", info)
	}
	rangeBytes, err := store.ReadRange(ctx, "contracts/single.bin", int64(len(data)-37), 37)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rangeBytes, data[len(data)-37:]) {
		t.Fatal("last range bytes do not match")
	}
	objects, err := store.List(ctx, "contracts")
	if err != nil || len(objects) != 1 {
		t.Fatalf("paginated list = %+v, %v", objects, err)
	}

	partOne := bytes.Repeat([]byte("a"), 5<<20)
	partTwo := bytes.Repeat([]byte("b"), 1024)
	multipartData := append(append([]byte(nil), partOne...), partTwo...)
	multipartDigest := sha256.Sum256(multipartData)
	upload, err := store.BeginMultipart(ctx, "contracts/multipart.bin", hex.EncodeToString(multipartDigest[:]))
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.UploadPart(ctx, upload, 1, partOne)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.UploadPart(ctx, upload, 2, partTwo)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := store.CompleteMultipart(ctx, upload, []storage.UploadedPart{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if completed.Size != int64(len(multipartData)) || completed.SHA256 != hex.EncodeToString(multipartDigest[:]) {
		t.Fatalf("multipart metadata mismatch: %+v", completed)
	}

	aborted, err := store.BeginMultipart(ctx, "contracts/aborted.bin", hex.EncodeToString(multipartDigest[:]))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AbortMultipart(ctx, aborted); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, []string{"contracts/single.bin", "contracts/multipart.bin"}); err != nil {
		t.Fatal(err)
	}

	settings.AccessKeyID = "forbidden"
	settings.SecretAccessKey = "forbidden"
	forbidden, err := storage.NewS3Store(ctx, settings)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := forbidden.Head(ctx, "contracts/does-not-matter"); err == nil {
		t.Fatal("invalid S3 credentials unexpectedly read the owned prefix")
	}
}
