package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

func validateCreateSnapshot(command CreateSnapshotCommand) error {
	if uuid.Validate(command.SnapshotID) != nil || command.SessionTokenHash == ([32]byte{}) || command.TenantID <= 0 || !validSHA(command.DatasetSHA256) || len(command.DatasetBytes) < 2 || len(command.DatasetBytes) > 32768 || len(command.ProjectIDs) < 1 || len(command.ProjectIDs) > 100 {
		return errors.New("invalid snapshot command")
	}
	datasetDigest := sha256.Sum256(command.DatasetBytes)
	if hex.EncodeToString(datasetDigest[:]) != command.DatasetSHA256 {
		return errors.New("snapshot dataset hash does not match bytes")
	}
	if hex.EncodeToString(command.SessionTokenHash[:]) == command.DatasetSHA256 {
		return errors.New("snapshot identities must be domain separated")
	}
	for index, projectID := range command.ProjectIDs {
		if projectID <= 0 || index > 0 && command.ProjectIDs[index-1] >= projectID {
			return errors.New("snapshot projects must be sorted and unique")
		}
	}
	return nil
}

func retryableSnapshotError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "40P01")
}

func waitSnapshotRetry(ctx context.Context, failedAttempt int) error {
	if failedAttempt >= SnapshotRetryLimit {
		return nil
	}
	base := [...]time.Duration{10 * time.Millisecond, 30 * time.Millisecond, 90 * time.Millisecond}[failedAttempt]
	delay := time.Duration(rand.Int64N(int64(base) + 1))
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
