package app

import (
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/maintenance"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

func openMaintenance(ctx context.Context, databaseURL string) (*control.RuntimeDatabase, *control.MaintenanceOperations, error) {
	database, err := control.OpenRuntimeDatabase(ctx, databaseURL, 4)
	if err != nil {
		return nil, nil, err
	}
	if err := database.VerifySchema(ctx); err != nil {
		database.Close()
		return nil, nil, err
	}
	operations, err := database.MaintenanceOperations()
	if err != nil {
		database.Close()
		return nil, nil, err
	}
	return database, operations, nil
}

func Doctor(ctx context.Context, databaseURL string, output io.Writer) error {
	database, _, err := openMaintenance(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer database.Close()
	installation, err := database.LoadInstallation(ctx)
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(map[string]any{
		"status": "ok", "installation_id": installation.InstallationID,
		"storage_generation": installation.StorageGeneration, "schema_version": installation.SchemaVersion,
	})
}

func RegisterBackup(ctx context.Context, databaseURL string, registration control.BackupRegistration) error {
	database, operations, err := openMaintenance(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer database.Close()
	return operations.RegisterBackup(ctx, registration)
}

func VerifyRestore(ctx context.Context, databaseURL string, s3 storage.S3Config, backupID string, expectedGeneration int64, verificationID, recoveryLSN, reportPath, attestationKeyPath string, output io.Writer) error {
	key, err := maintenance.ReadAttestationKey(attestationKeyPath)
	if err != nil {
		return err
	}
	database, operations, err := openMaintenance(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer database.Close()
	store, err := storage.NewS3Store(ctx, s3)
	if err != nil {
		return err
	}
	_, encoded, reportSHA, err := (maintenance.RecoveryVerifier{Control: operations, Store: store}).Verify(ctx, backupID, expectedGeneration, verificationID, recoveryLSN, key)
	if err != nil {
		return err
	}
	if err := maintenance.WriteRecoveryReport(reportPath, encoded); err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(map[string]string{"verification_id": verificationID, "report_sha256": reportSHA})
}

func ActivateRestore(ctx context.Context, databaseURL, reportPath, attestationKeyPath string, expectedGeneration int64, output io.Writer) error {
	key, err := maintenance.ReadAttestationKey(attestationKeyPath)
	if err != nil {
		return err
	}
	report, _, reportSHA, err := maintenance.ReadRecoveryReport(reportPath, key)
	if err != nil {
		return err
	}
	if report.SourceGeneration != expectedGeneration {
		return control.ErrRecoveryGeneration
	}
	database, operations, err := openMaintenance(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer database.Close()
	newGeneration, err := operations.ActivateRecovery(ctx, report.VerificationID, reportSHA, expectedGeneration)
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(map[string]any{"storage_generation": newGeneration, "alerts_paused": true})
}

func VerifyBackup(ctx context.Context, databaseURL, reportPath, attestationKeyPath string, expectedGeneration int64, output io.Writer) error {
	key, err := maintenance.ReadAttestationKey(attestationKeyPath)
	if err != nil {
		return err
	}
	report, _, reportSHA, err := maintenance.ReadRecoveryReport(reportPath, key)
	if err != nil {
		return err
	}
	if report.SourceGeneration != expectedGeneration {
		return control.ErrRecoveryGeneration
	}
	database, operations, err := openMaintenance(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer database.Close()
	err = operations.RecordBackupRehearsal(ctx, control.RecoveryVerification{
		VerificationID: report.VerificationID, BackupID: report.BackupID, InstallationID: report.InstallationID,
		SourceGeneration: report.SourceGeneration, ReportSHA256: reportSHA, InventorySHA256: report.InventorySHA256,
		RecoveryLSN: report.RecoveryLSN, ReferencedObjects: report.ReferencedObjects, ReferencedBytes: report.ReferencedBytes,
	}, report.VerifiedAt)
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(map[string]any{"backup_id": report.BackupID, "gc_verified_until": report.VerifiedAt.Add(control.RecoveryVerificationFreshness)})
}

func InspectRepair(ctx context.Context, databaseURL string, tenantID int64, laneID int, output io.Writer) error {
	database, operations, err := openMaintenance(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer database.Close()
	state, err := operations.InspectRecoveryLane(ctx, tenantID, laneID)
	if err != nil {
		return err
	}
	if output == nil {
		return errors.New("repair output is required")
	}
	return json.NewEncoder(output).Encode(state)
}
