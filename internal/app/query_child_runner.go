package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/resource"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

type ProcessQueryRunner struct {
	BinaryPath string
	Gate       *NativeTaskGate
}

type ProcessQueryExportRunner struct {
	BinaryPath string
	Gate       *NativeTaskGate
}

func (runner ProcessQueryExportRunner) Export(ctx context.Context, request engine.QueryExportRequest) (engine.QueryExportSummary, error) {
	release, err := runner.Gate.acquire(ctx)
	if err != nil {
		return engine.QueryExportSummary{}, err
	}
	defer release()
	binary := runner.BinaryPath
	if binary == "" {
		var err error
		binary, err = os.Executable()
		if err != nil {
			return engine.QueryExportSummary{}, err
		}
	}
	input, err := json.Marshal(engine.ChildRequest{Operation: "query_export", QueryExport: &request})
	if err != nil || len(input) > 1<<20 {
		return engine.QueryExportSummary{}, errors.Join(errors.New("query export request is too large"), err)
	}
	var stdout, stderr boundedBuffer
	command := exec.Command(binary, "engine-child")
	command.Stdin, command.Stdout, command.Stderr = bytes.NewReader(input), &stdout, &stderr
	command.Env = childEnvironment(os.Environ())
	if err := runNativeChild(ctx, command); err != nil {
		_ = os.Remove(request.OutputPath)
		if ctx.Err() != nil {
			return engine.QueryExportSummary{}, ctx.Err()
		}
		return engine.QueryExportSummary{}, fmt.Errorf("query export child failed: %w", err)
	}
	var message engine.QueryExportMessage
	if err := strictAppJSON(stdout.Bytes(), &message); err != nil {
		return engine.QueryExportSummary{}, err
	}
	if message.Version != engine.QueryExecutionProtocolVersion || message.Type != "summary" || message.Summary.Rows < 0 {
		return engine.QueryExportSummary{}, errors.New("invalid query export summary")
	}
	actual, err := storage.InspectFile(request.OutputPath)
	if err != nil || !reflect.DeepEqual(actual, message.Summary.Evidence) {
		return engine.QueryExportSummary{}, errors.Join(errors.New("query export evidence differs"), err)
	}
	return message.Summary, nil
}

func (runner ProcessQueryRunner) Run(ctx context.Context, request engine.QueryRequest) (engine.QuerySummary, error) {
	release, err := runner.Gate.acquire(ctx)
	if err != nil {
		return engine.QuerySummary{}, err
	}
	defer release()
	request.NativeMemoryBytes = runner.Gate.memoryLimit(request.NativeMemoryBytes)
	binary := runner.BinaryPath
	if binary == "" {
		var err error
		binary, err = os.Executable()
		if err != nil {
			return engine.QuerySummary{}, err
		}
	}
	input, err := json.Marshal(engine.ChildRequest{Operation: "query", Query: &request})
	if err != nil || len(input) > 1<<20 {
		return engine.QuerySummary{}, errors.Join(errors.New("query child request is too large"), err)
	}
	var stdout, stderr boundedBuffer
	command := exec.Command(binary, "engine-child")
	command.Stdin = bytes.NewReader(input)
	command.Stdout, command.Stderr = &stdout, &stderr
	command.Env = childEnvironment(os.Environ())
	if err := runNativeChild(ctx, command); err != nil {
		_ = os.Remove(request.OutputPath)
		_ = os.RemoveAll(request.SpillDirectory)
		if ctx.Err() != nil {
			return engine.QuerySummary{}, ctx.Err()
		}
		if strings.Contains(strings.ToLower(stderr.String()), "out of memory") {
			return engine.QuerySummary{}, errors.Join(resource.ErrLimited, errors.New("query engine exhausted its native memory or spill budget"))
		}
		return engine.QuerySummary{}, fmt.Errorf("query engine child failed: %w", err)
	}
	var message engine.QueryMessage
	if err := strictAppJSON(stdout.Bytes(), &message); err != nil {
		return engine.QuerySummary{}, err
	}
	if message.Version != engine.QueryExecutionProtocolVersion || message.Type != "summary" || message.Summary.Rows < 0 || message.Summary.Rows > request.Operation.MaxRows {
		return engine.QuerySummary{}, errors.New("invalid query child summary")
	}
	actual, err := storage.InspectFile(request.OutputPath)
	if err != nil || !reflect.DeepEqual(actual, message.Summary.Evidence) {
		return engine.QuerySummary{}, errors.Join(errors.New("query child output evidence differs"), err)
	}
	return message.Summary, nil
}
