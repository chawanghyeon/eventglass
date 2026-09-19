package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"

	"github.com/chawanghyeon/eventglass/internal/engine"
)

// These are process boundaries, not repository abstractions. app supplies a
// shared-budget child runner; query owns durable submission and waiting.
type QuerySyncExecutor interface {
	Execute(context.Context, [32]byte, int64, string) error
}
type QueryExportRunner interface {
	Export(context.Context, engine.QueryExportRequest) (engine.QueryExportSummary, error)
}

func ensureResultDirectory(path string) error {
	if path == "" {
		return errors.New("scratch directory is required")
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return errors.New("scratch directory must be private")
	}
	return nil
}

func strictResultJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("trailing result JSON")
	}
	return nil
}
