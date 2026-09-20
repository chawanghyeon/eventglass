package maintenance

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
)

type RecoveryStore interface {
	VerifyObject(context.Context, string, int64, string) error
}

type RecoveryControl interface {
	BeginRecoveryVerification(context.Context, string, int64) (control.RecoveryInventory, error)
	FailRecoveryVerification(context.Context, string, string) error
	RecordRecoveryVerification(context.Context, control.RecoveryVerification) error
}

type RecoveryReport struct {
	Version           int       `json:"version"`
	VerificationID    string    `json:"verification_id"`
	BackupID          string    `json:"backup_id"`
	ExternalToolID    string    `json:"external_tool_id"`
	InstallationID    string    `json:"installation_id"`
	SourceGeneration  int64     `json:"source_generation"`
	RecoveryLSN       string    `json:"recovery_lsn"`
	InventorySHA256   string    `json:"inventory_sha256"`
	ReferencedObjects int64     `json:"referenced_objects"`
	ReferencedBytes   int64     `json:"referenced_bytes"`
	VerifiedAt        time.Time `json:"verified_at"`
	Signature         string    `json:"signature,omitempty"`
}

type RecoveryVerifier struct {
	Control RecoveryControl
	Store   RecoveryStore
	Now     func() time.Time
}

func (verifier RecoveryVerifier) Verify(ctx context.Context, backupID string, expectedGeneration int64, verificationID, recoveryLSN string, attestationKey []byte) (RecoveryReport, []byte, string, error) {
	if verifier.Control == nil || verifier.Store == nil || verificationID == "" || recoveryLSN == "" || len(attestationKey) < 32 {
		return RecoveryReport{}, nil, "", errors.New("recovery verifier dependencies and identifiers are required")
	}
	inventory, err := verifier.Control.BeginRecoveryVerification(ctx, backupID, expectedGeneration)
	if err != nil {
		return RecoveryReport{}, nil, "", err
	}
	fail := func(code string, cause error) (RecoveryReport, []byte, string, error) {
		return RecoveryReport{}, nil, "", errors.Join(cause, verifier.Control.FailRecoveryVerification(context.Background(), backupID, code))
	}
	hash := sha256.New()
	var totalBytes int64
	for _, object := range inventory.Objects {
		if object.Bytes < 0 || object.ObjectKey == "" || object.SHA256 == "" {
			return fail("invalid_inventory", errors.New("restored inventory is invalid"))
		}
		fmt.Fprintf(hash, "%s\x00%d\x00%s\n", object.ObjectKey, object.Bytes, object.SHA256)
		if totalBytes > int64(^uint64(0)>>1)-object.Bytes {
			return fail("inventory_overflow", errors.New("restored inventory byte count overflow"))
		}
		totalBytes += object.Bytes
		if err := verifier.Store.VerifyObject(ctx, object.ObjectKey, object.Bytes, object.SHA256); err != nil {
			return fail("object_verification_failed", fmt.Errorf("verify referenced object %q: %w", object.ObjectKey, err))
		}
	}
	now := time.Now().UTC()
	if verifier.Now != nil {
		now = verifier.Now().UTC()
	}
	report := RecoveryReport{
		Version: 1, VerificationID: verificationID, BackupID: inventory.BackupID,
		ExternalToolID: inventory.ExternalToolID, InstallationID: inventory.InstallationID,
		SourceGeneration: inventory.StorageGeneration, RecoveryLSN: recoveryLSN,
		InventorySHA256: hex.EncodeToString(hash.Sum(nil)), ReferencedObjects: int64(len(inventory.Objects)),
		ReferencedBytes: totalBytes, VerifiedAt: now,
	}
	unsigned, err := canonicalRecoveryReport(report)
	if err != nil {
		return fail("report_encoding_failed", err)
	}
	mac := hmac.New(sha256.New, attestationKey)
	_, _ = mac.Write(unsigned)
	report.Signature = hex.EncodeToString(mac.Sum(nil))
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fail("report_encoding_failed", err)
	}
	encoded = append(encoded, '\n')
	reportDigest := sha256.Sum256(encoded)
	reportSHA := hex.EncodeToString(reportDigest[:])
	err = verifier.Control.RecordRecoveryVerification(ctx, control.RecoveryVerification{
		VerificationID: report.VerificationID, BackupID: report.BackupID, InstallationID: report.InstallationID,
		SourceGeneration: report.SourceGeneration, ReportSHA256: reportSHA, InventorySHA256: report.InventorySHA256,
		RecoveryLSN: report.RecoveryLSN, ReferencedObjects: report.ReferencedObjects, ReferencedBytes: report.ReferencedBytes,
	})
	if err != nil {
		return fail("verification_commit_failed", err)
	}
	return report, encoded, reportSHA, nil
}

func WriteRecoveryReport(path string, encoded []byte) error {
	if !filepath.IsAbs(path) || len(encoded) < 2 || len(encoded) > 1<<20 || !json.Valid(encoded) {
		return errors.New("recovery report target or contents are invalid")
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		_ = file.Close()
		if !ok {
			_ = os.Remove(path)
		}
	}()
	if _, err := file.Write(encoded); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	ok = true
	return nil
}

func ReadAttestationKey(path string) ([]byte, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("attestation key path must be absolute")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() < 32 || info.Size() > 4096 {
		return nil, errors.Join(errors.New("attestation key is not a private regular file of 32..4096 bytes"), err)
	}
	return os.ReadFile(path)
}

func ReadRecoveryReport(path string, attestationKey []byte) (RecoveryReport, []byte, string, error) {
	if !filepath.IsAbs(path) {
		return RecoveryReport{}, nil, "", errors.New("recovery report path must be absolute")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() < 2 || info.Size() > 1<<20 {
		return RecoveryReport{}, nil, "", errors.Join(errors.New("recovery report file is not a private regular file"), err)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return RecoveryReport{}, nil, "", err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var report RecoveryReport
	if err := decoder.Decode(&report); err != nil {
		return RecoveryReport{}, nil, "", err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return RecoveryReport{}, nil, "", errors.New("recovery report has trailing JSON values")
	}
	if report.Version != 1 || report.VerificationID == "" || report.BackupID == "" || report.InstallationID == "" || report.SourceGeneration <= 0 || report.RecoveryLSN == "" || report.InventorySHA256 == "" || report.ReferencedObjects < 0 || report.ReferencedBytes < 0 || report.VerifiedAt.IsZero() || len(attestationKey) < 32 || len(report.Signature) != 64 {
		return RecoveryReport{}, nil, "", errors.New("recovery report fields are invalid")
	}
	signature, err := hex.DecodeString(report.Signature)
	if err != nil {
		return RecoveryReport{}, nil, "", errors.New("recovery report signature is invalid")
	}
	report.Signature = ""
	unsigned, err := canonicalRecoveryReport(report)
	if err != nil {
		return RecoveryReport{}, nil, "", err
	}
	mac := hmac.New(sha256.New, attestationKey)
	_, _ = mac.Write(unsigned)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return RecoveryReport{}, nil, "", errors.New("recovery report signature does not match")
	}
	report.Signature = hex.EncodeToString(signature)
	digest := sha256.Sum256(encoded)
	return report, encoded, hex.EncodeToString(digest[:]), nil
}

func canonicalRecoveryReport(report RecoveryReport) ([]byte, error) {
	report.Signature = ""
	return json.Marshal(report)
}
