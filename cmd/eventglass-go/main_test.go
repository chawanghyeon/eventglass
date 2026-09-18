package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestVersion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{"version"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != version+"\n" || stderr.Len() != 0 {
		t.Fatalf("stdout/stderr = %q/%q", stdout.String(), stderr.String())
	}
}

func TestRunDoesNotAdvertiseAnUnavailableServer(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{"run"}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "G02 durable ACK") {
		t.Fatalf("run error = %v", err)
	}
}

func TestUnknownCommandFails(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{"serve"}, &stdout, &stderr); err == nil {
		t.Fatal("unknown command succeeded")
	}
}
