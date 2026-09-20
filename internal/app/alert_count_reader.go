package app

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
)

type AlertCountReader struct {
	Store interface {
		DownloadToFile(context.Context, string, string, int64, string) error
	}
	Exporter   ProcessQueryExportRunner
	ScratchDir string
}

func (reader AlertCountReader) ReadAlertCount(ctx context.Context, status control.QueryStatus) (int64, error) {
	if status.Result == nil || reader.Store == nil {
		return 0, errors.New("alert query result is unavailable")
	}
	if err := os.MkdirAll(reader.ScratchDir, 0o700); err != nil {
		return 0, err
	}
	directory, err := os.MkdirTemp(reader.ScratchDir, "alert-")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(directory)
	parquetPath := filepath.Join(directory, "result.parquet")
	if err := reader.Store.DownloadToFile(ctx, status.Result.ObjectKey, parquetPath, status.Result.Bytes, status.Result.SHA256); err != nil {
		return 0, err
	}
	jsonPath := filepath.Join(directory, "result.jsonl")
	summary, err := reader.Exporter.Export(ctx, engine.QueryExportRequest{Version: engine.QueryExecutionProtocolVersion, InputPath: parquetPath, OutputPath: jsonPath})
	if err != nil {
		return 0, err
	}
	if summary.Rows != 1 {
		return 0, errors.New("alert count result must contain exactly one row")
	}
	data, err := os.ReadFile(jsonPath)
	if err != nil {
		return 0, err
	}
	return decodeAlertCount(data)
}

func decodeAlertCount(data []byte) (int64, error) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 1024), 64<<10)
	if !scanner.Scan() {
		return 0, errors.New("alert count result is empty")
	}
	decoder := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
	decoder.UseNumber()
	var object map[string]any
	if decoder.Decode(&object) != nil {
		return 0, errors.New("alert count result is invalid")
	}
	number, ok := object["m0_valid"].(json.Number)
	if !ok {
		return 0, errors.New("alert count value is missing")
	}
	value, err := strconv.ParseInt(number.String(), 10, 64)
	if err != nil || value < 0 {
		return 0, errors.New("alert count value is invalid")
	}
	if scanner.Scan() || scanner.Err() != nil {
		return 0, errors.New("alert count result has extra rows")
	}
	return value, nil
}
