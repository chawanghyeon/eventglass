package integration

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/alerts"
	"github.com/chawanghyeon/eventglass/internal/control"
)

func TestDeliveryLostReplyFencingRetryLimitAndManualRetry(t *testing.T) {
	f := setupAlertFixture(t, 1303)
	ctx := context.Background()
	rule := testRule(t, alerts.KindIssue, `{"events":["created"]}`)
	alert, err := f.operations.CreateAlert(ctx, control.CreateAlertCommand{TenantID: f.tenant, ProjectID: f.project, ActorUserID: f.user, AlertID: f.uuid(), Name: "delivery", DestinationID: f.destination, Kind: "issue", RuleBytes: rule.Bytes, RuleSHA256: rule.SHA256, RequestID: f.uuid(), AuditID: f.uuid()})
	if err != nil {
		t.Fatal(err)
	}
	key := [32]byte{3} // pragma: allowlist secret
	cipher, err := alerts.NewSecretCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	encrypted, err := cipher.Encrypt("integration-signing-value") // pragma: allowlist secret
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"version":1,"delivery_id":"stable"}`)
	bodyDigest := sha256.Sum256(body)
	var receives atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		received, _ := io.ReadAll(request.Body)
		if string(received) != string(body) {
			t.Errorf("body=%q", received)
		}
		timestamp := request.Header.Get("X-Eventglass-Timestamp")
		mac := hmac.New(sha256.New, []byte("integration-signing-value")) // pragma: allowlist secret
		_, _ = mac.Write([]byte(timestamp + "."))
		_, _ = mac.Write(body)
		if got, want := request.Header.Get("X-Eventglass-Signature"), "v1="+hex.EncodeToString(mac.Sum(nil)); got != want {
			t.Errorf("signature=%q want=%q", got, want)
		}
		receives.Add(1)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	var installationID string
	var storageGeneration int64
	if err := f.pool.QueryRow(ctx, `SELECT installation_id::text,storage_generation FROM installations WHERE singleton`).Scan(&installationID, &storageGeneration); err != nil {
		t.Fatal(err)
	}

	insertDelivery := func(id string, attempt int, suffix string) {
		t.Helper()
		_, err := f.pool.Exec(ctx, `INSERT INTO deliveries(tenant_id,project_id,delivery_id,alert_id,alert_revision,destination_id,destination_revision,destination_url,destination_secret_ciphertext,destination_encryption_key_id,dedupe_key,body_bytes,body_sha256,attempt) VALUES($1,$2,$3,$4,$5,$6,1,$7,$8,$9,$10,$11,$12,$13)`, f.tenant, f.project, id, alert.AlertID, alert.Revision, f.destination, server.URL, encrypted, cipher.KeyID(), "integration:"+suffix, body, hex.EncodeToString(bodyDigest[:]), attempt)
		if err != nil {
			t.Fatal(err)
		}
	}
	firstID := f.uuid()
	insertDelivery(firstID, 0, "lost-reply")
	first, err := f.operations.ClaimDelivery(ctx, installationID, storageGeneration, "worker-one", time.Minute)
	if err != nil || first == nil || first.Authority.DeliveryID != firstID || first.Attempt != 1 {
		t.Fatalf("first claim=%+v err=%v", first, err)
	}
	secret, err := cipher.Decrypt(first.DestinationEncryptionKeyID, first.DestinationSecretCiphertext)
	if err != nil {
		t.Fatal(err)
	}
	sender := alerts.Sender{AllowLoopbackForTests: true}
	outcome := sender.Send(ctx, alerts.SendRequest{DeliveryID: firstID, URL: first.DestinationURL, Body: first.BodyBytes, Secret: secret}) // pragma: allowlist secret
	if outcome.Status != http.StatusNoContent {
		t.Fatalf("first send=%+v", outcome)
	}
	if _, err := f.pool.Exec(ctx, `UPDATE deliveries SET lease_until=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND delivery_id=$2`, f.tenant, firstID); err != nil {
		t.Fatal(err)
	}
	second, err := f.operations.ClaimDelivery(ctx, installationID, storageGeneration, "worker-two", time.Minute)
	if err != nil || second == nil || second.Authority.DeliveryID != firstID || second.Attempt != 2 {
		t.Fatalf("reclaim=%+v err=%v", second, err)
	}
	outcome = sender.Send(ctx, alerts.SendRequest{DeliveryID: firstID, URL: second.DestinationURL, Body: second.BodyBytes, Secret: secret}) // pragma: allowlist secret
	if err := f.operations.CompleteDelivery(ctx, second.Authority, outcome.Status); err != nil {
		t.Fatal(err)
	}
	if receives.Load() != 2 {
		t.Fatalf("receiver count=%d", receives.Load())
	}
	if err := f.operations.CompleteDelivery(ctx, first.Authority, http.StatusNoContent); !errors.Is(err, control.ErrDeliveryLeaseLost) {
		t.Fatalf("stale completion=%v", err)
	}

	exhaustedID := f.uuid()
	insertDelivery(exhaustedID, 11, "exhausted")
	exhausted, err := f.operations.ClaimDelivery(ctx, installationID, storageGeneration, "worker-three", time.Minute)
	if err != nil || exhausted == nil || exhausted.Attempt != 12 {
		t.Fatalf("exhausted claim=%+v err=%v", exhausted, err)
	}
	err = f.operations.FailDelivery(ctx, exhausted.Authority, http.StatusServiceUnavailable, "http_retryable", true, time.Minute)
	if err != nil {
		t.Fatalf("exhausted failure err=%v", err)
	}
	var storedState, code string
	var revision int64
	if err := f.pool.QueryRow(ctx, `SELECT state,error_code,revision FROM deliveries WHERE tenant_id=$1 AND delivery_id=$2`, f.tenant, exhaustedID).Scan(&storedState, &code, &revision); err != nil || storedState != "failed" || code != "attempts_exhausted" {
		t.Fatalf("stored exhausted=%s %s revision=%d err=%v", storedState, code, revision, err)
	}
	retried, err := f.operations.RetryDelivery(ctx, control.RetryDeliveryCommand{TenantID: f.tenant, ProjectID: f.project, ActorUserID: f.user, ExpectedRevision: revision, DeliveryID: exhaustedID})
	if err != nil || retried.State != "queued" || retried.Attempt != 0 || retried.Authority.DeliveryID != exhaustedID {
		t.Fatalf("manual retry=%+v err=%v", retried, err)
	}

	page, err := f.operations.ListDeliveryPage(ctx, control.DeliveryPageCommand{TenantID: f.tenant, ProjectID: f.project, ActorUserID: f.user, AlertID: alert.AlertID, Limit: 1})
	if err != nil || len(page) != 2 {
		t.Fatalf("delivery page=%+v err=%v", page, err)
	}
}
