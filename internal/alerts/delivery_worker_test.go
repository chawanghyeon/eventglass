package alerts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
)

type deliveryControlFixture struct {
	delivery  *control.Delivery
	failed    bool
	code      string
	retry     bool
	completed bool
}

func (fixture *deliveryControlFixture) ClaimDelivery(context.Context, string, int64, string, time.Duration) (*control.Delivery, error) {
	value := fixture.delivery
	fixture.delivery = nil
	return value, nil
}
func (*deliveryControlFixture) HeartbeatDelivery(context.Context, control.DeliveryAuthority, time.Duration) error {
	return nil
}
func (fixture *deliveryControlFixture) CompleteDelivery(context.Context, control.DeliveryAuthority, int) error {
	fixture.completed = true
	return nil
}
func (fixture *deliveryControlFixture) FailDelivery(_ context.Context, _ control.DeliveryAuthority, _ int, code string, retry bool, _ time.Duration) error {
	fixture.failed, fixture.code, fixture.retry = true, code, retry
	return nil
}

func TestDeliveryWorkerFailsClosedWhenCredentialKeyIsUnavailable(t *testing.T) {
	configured, err := NewSecretCipher([32]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	old, err := NewSecretCipher([32]byte{2})
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := old.Encrypt("old-signing-value") // pragma: allowlist secret
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{}`)
	digest := sha256.Sum256(body)
	fixture := &deliveryControlFixture{delivery: &control.Delivery{
		Authority:                  control.DeliveryAuthority{InstallationID: "00000000-0000-4000-8000-000000000001", StorageGeneration: 1, TenantID: 1, DeliveryID: "00000000-0000-4000-8000-000000000002", Owner: "worker", Fence: 1},
		DestinationEncryptionKeyID: old.KeyID(), DestinationSecretCiphertext: ciphertext, DestinationURL: "https://example.invalid", BodyBytes: body, BodySHA256: hex.EncodeToString(digest[:]), Attempt: 1,
	}}
	worker := &DeliveryWorker{Control: fixture, Cipher: configured, InstallationID: "00000000-0000-4000-8000-000000000001", StorageGeneration: 1, Owner: "worker"}
	progressed, err := worker.RunOnce(context.Background())
	if !progressed || err == nil || !fixture.failed || fixture.code != "credential_unavailable" || fixture.retry || fixture.completed {
		t.Fatalf("progressed=%v err=%v fixture=%+v", progressed, err, fixture)
	}
}
