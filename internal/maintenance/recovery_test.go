package maintenance

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
)

type recoveryControlFixture struct {
	inventory control.RecoveryInventory
	recorded  *control.RecoveryVerification
	failed    string
}

func (fixture *recoveryControlFixture) BeginRecoveryVerification(context.Context, string, int64) (control.RecoveryInventory, error) {
	return fixture.inventory, nil
}
func (fixture *recoveryControlFixture) FailRecoveryVerification(_ context.Context, _ string, code string) error {
	fixture.failed = code
	return nil
}
func (fixture *recoveryControlFixture) RecordRecoveryVerification(_ context.Context, value control.RecoveryVerification) error {
	fixture.recorded = &value
	return nil
}

type validRecoveryStoreFixture struct{ failKey string }

func (fixture validRecoveryStoreFixture) VerifyObject(_ context.Context, key string, _ int64, _ string) error {
	if key == fixture.failKey {
		return errors.New("missing")
	}
	return nil
}

func TestRecoveryVerifierHashesOnlyRestoredAuthorityAndWritesPrivateReport(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, 32)
	controlFixture := &recoveryControlFixture{inventory: control.RecoveryInventory{
		BackupID: "00000000-0000-4000-8000-000000000001", ExternalToolID: "pgbackrest-label",
		InstallationID: "00000000-0000-4000-8000-000000000002", StorageGeneration: 7,
		Objects: []control.RecoveryObject{{IntentID: "intent", ObjectKey: "a", Bytes: 3, SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
	}}
	verifier := RecoveryVerifier{Control: controlFixture, Store: validRecoveryStoreFixture{}, Now: func() time.Time { return time.Unix(100, 0) }}
	report, encoded, digest, err := verifier.Verify(context.Background(), controlFixture.inventory.BackupID, 7, "00000000-0000-4000-8000-000000000003", "0/123", key)
	if err != nil {
		t.Fatal(err)
	}
	if report.ReferencedObjects != 1 || report.ReferencedBytes != 3 || digest == "" || controlFixture.recorded == nil || controlFixture.failed != "" {
		t.Fatalf("report=%#v recorded=%v failed=%s", report, controlFixture.recorded != nil, controlFixture.failed)
	}
	path := filepath.Join(t.TempDir(), "report.json")
	if err := WriteRecoveryReport(path, encoded); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("mode=%v err=%v", info.Mode().Perm(), err)
	}
	read, _, readDigest, err := ReadRecoveryReport(path, key)
	if err != nil || read.VerificationID != report.VerificationID || readDigest != digest {
		t.Fatalf("read=%#v digest=%s err=%v", read, readDigest, err)
	}
	if _, _, _, err := ReadRecoveryReport(path, bytes.Repeat([]byte{0x43}, 32)); err == nil {
		t.Fatal("report accepted with the wrong attestation key")
	}
}

func TestRecoveryVerifierLeavesInstallationUnhealthyOnMissingObject(t *testing.T) {
	controlFixture := &recoveryControlFixture{inventory: control.RecoveryInventory{
		BackupID: "00000000-0000-4000-8000-000000000001", InstallationID: "00000000-0000-4000-8000-000000000002", StorageGeneration: 1,
		Objects: []control.RecoveryObject{{ObjectKey: "missing", Bytes: 1, SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
	}}
	_, _, _, err := (RecoveryVerifier{Control: controlFixture, Store: validRecoveryStoreFixture{failKey: "missing"}}).Verify(context.Background(), controlFixture.inventory.BackupID, 1, "00000000-0000-4000-8000-000000000003", "0/123", bytes.Repeat([]byte{0x42}, 32))
	if err == nil || controlFixture.failed != "object_verification_failed" || controlFixture.recorded != nil {
		t.Fatalf("err=%v failed=%s recorded=%v", err, controlFixture.failed, controlFixture.recorded)
	}
}
