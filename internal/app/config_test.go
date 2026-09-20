package app

import (
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/resource"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

func TestLoadConfigAndRejectUnknownRoles(t *testing.T) {
	values := map[string]string{
		"EVENTGLASS_DATABASE_URL": "postgres://eventglass.invalid/db", "EVENTGLASS_PUBLIC_URL": "https://events.invalid",
		"EVENTGLASS_ROLES": "api", "EVENTGLASS_SCRATCH_DIR": t.TempDir(), "EVENTGLASS_S3_REGION": "us-east-1",
		"EVENTGLASS_S3_BUCKET": "eventglass", "EVENTGLASS_S3_ENDPOINT": "http://127.0.0.1:9000/", "EVENTGLASS_AUTH_HASH_KEY_FILE": "/run/secrets/eventglass-auth-hash-key",
		"EVENTGLASS_TOKEN_KEY_FILE":            "/run/secrets/eventglass-token-key",
		"EVENTGLASS_ALERT_ENCRYPTION_KEY_FILE": "/run/secrets/eventglass-alert-encryption-key",
	}
	lookup := func(key string) (string, bool) { value, ok := values[key]; return value, ok }
	config, err := LoadConfigFromEnv(lookup)
	if err != nil {
		t.Fatal(err)
	}
	if config.HTTPAddr != ":8080" || !config.Roles[RoleAPI] || !config.S3.PathStyle || config.S3.Endpoint != "http://127.0.0.1:9000" || config.DrainTimeout != 30*time.Second {
		t.Fatalf("config=%#v", config)
	}
	delete(values, "EVENTGLASS_ALERT_ENCRYPTION_KEY_FILE")
	if _, err := LoadConfigFromEnv(lookup); err == nil {
		t.Fatal("API role accepted a missing alert encryption key")
	}
	values["EVENTGLASS_ROLES"] = "worker"
	if _, err := LoadConfigFromEnv(lookup); err == nil {
		t.Fatal("worker role accepted a missing alert encryption key")
	}
	values["EVENTGLASS_ALERT_ENCRYPTION_KEY_FILE"] = "/run/secrets/eventglass-alert-encryption-key"
	values["EVENTGLASS_ROLES"] = "api,mystery"
	if _, err := LoadConfigFromEnv(lookup); err == nil {
		t.Fatal("unknown role was accepted")
	}
}

func TestRoleBudgetsAreSharedAndBounded(t *testing.T) {
	apiResources, err := ResourcesForRoles(map[Role]bool{RoleAPI: true})
	if err != nil {
		t.Fatal(err)
	}
	permit, err := apiResources.Ingress.Acquire(64 << 20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := apiResources.Ingress.Acquire(1); err != resource.ErrLimited {
		t.Fatalf("API ingress overflow=%v", err)
	}
	permit.Release()

	combined, err := ResourcesForRoles(map[Role]bool{RoleAPI: true, RoleWorker: true, RoleScheduler: true})
	if err != nil {
		t.Fatal(err)
	}
	working, err := combined.Working.Acquire(192 << 20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := combined.Working.Acquire(1); err != resource.ErrLimited || combined.PoolMaxConns != 12 || combined.GoMemoryLimit != 224<<20 {
		t.Fatalf("combined profile overflow=%v resources=%#v", err, combined)
	}
	working.Release()
}

func TestStorageIdentityAndMarkerAreDeterministic(t *testing.T) {
	identity, err := StorageIdentity(storage.S3Config{Endpoint: "HTTP://S3.Example:9000/", Region: "us-east-1", Bucket: "events", Prefix: "/tenant/"})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"version":1,"endpoint":"http://s3.example:9000","region":"us-east-1","bucket":"events","prefix":"tenant"}`
	if identity != want {
		t.Fatalf("identity=%q want=%q", identity, want)
	}
	marker, key, checksum, err := InstallationMarker("00000000-0000-4000-8000-000000000001", identity)
	if err != nil || len(marker) == 0 || key != "v1/00000000-0000-4000-8000-000000000001/installation.json" || len(checksum) != 64 {
		t.Fatalf("marker=%q key=%q checksum=%q err=%v", marker, key, checksum, err)
	}
}
