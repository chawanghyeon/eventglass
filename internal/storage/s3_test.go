package storage

import (
	"context"
	"testing"
)

func TestS3ObjectKeyStaysInsideConfiguredPrefix(t *testing.T) {
	store := &S3Store{prefix: "installation/live"}
	for _, invalid := range []string{"", "/absolute", "../escape", "a/../../escape", `a\\b`} {
		if _, err := store.objectKey(invalid); err == nil {
			t.Fatalf("objectKey(%q) succeeded", invalid)
		}
	}
	key, err := store.objectKey("journals/tenant/lane/object")
	if err != nil {
		t.Fatal(err)
	}
	if key != "installation/live/journals/tenant/lane/object" {
		t.Fatalf("key = %q", key)
	}
}

func TestS3RequiresExplicitRegionAndBucket(t *testing.T) {
	if _, err := NewS3Store(context.Background(), S3Config{}); err == nil {
		t.Fatal("empty S3 configuration succeeded")
	}
}
