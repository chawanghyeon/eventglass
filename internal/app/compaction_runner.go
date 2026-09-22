package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/chawanghyeon/eventglass/internal/engine"
)

type ProcessCompactionRunner struct {
	BinaryPath string
	Gate       *NativeTaskGate
}

// DuckDB's managed limit excludes some Parquet vectors/writer allocations.
// Leave room for those and the supervisor inside the worker's512MiB cgroup.
// This is a maintenance-only cap; smaller combined-role requests remain smaller.
const maintenanceNativeMemoryBytes = int64(96 << 20)

func (runner ProcessCompactionRunner) memoryLimit(requested int64) int64 {
	return min(runner.Gate.memoryLimit(requested), maintenanceNativeMemoryBytes)
}

func (runner ProcessCompactionRunner) Run(ctx context.Context, request engine.CompactionRequest) (engine.CompactionResult, error) {
	release, err := runner.Gate.acquire(ctx)
	if err != nil {
		return engine.CompactionResult{}, err
	}
	defer release()
	request.NativeMemoryBytes = runner.memoryLimit(request.NativeMemoryBytes)
	binary := runner.BinaryPath
	if binary == "" {
		binary, err = os.Executable()
		if err != nil {
			return engine.CompactionResult{}, err
		}
	}
	input, err := json.Marshal(engine.ChildRequest{Operation: "compact", Compaction: &request})
	if err != nil {
		return engine.CompactionResult{}, err
	}
	response, err := os.CreateTemp(filepath.Dir(request.OutputDirectory), "compact-response-*.json")
	if err != nil {
		return engine.CompactionResult{}, err
	}
	defer func() { response.Close(); os.Remove(response.Name()) }()
	command := exec.Command(binary, "engine-child")
	command.Stdin, command.Stdout = bytes.NewReader(input), response
	var stderr boundedBuffer
	command.Stderr, command.Env = &stderr, childEnvironment(os.Environ())
	if err := runNativeChild(ctx, command); err != nil {
		cleanupChildOutputs(request.OutputDirectory)
		if ctx.Err() != nil {
			return engine.CompactionResult{}, ctx.Err()
		}
		return engine.CompactionResult{}, fmt.Errorf("compaction child failed: %w", err)
	}
	if info, err := response.Stat(); err != nil || info.Size() <= 0 || info.Size() > maxChildMessageBytes {
		cleanupChildOutputs(request.OutputDirectory)
		return engine.CompactionResult{}, errors.Join(errors.New("invalid compaction child response size"), err)
	}
	if _, err := response.Seek(0, io.SeekStart); err != nil {
		return engine.CompactionResult{}, err
	}
	decoder := json.NewDecoder(io.LimitReader(response, maxChildMessageBytes+1))
	decoder.DisallowUnknownFields()
	var result engine.CompactionResult
	if err := decoder.Decode(&result); err != nil {
		cleanupChildOutputs(request.OutputDirectory)
		return result, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF || result.DuckDBVersion == "" || result.Bundle.Index != 0 {
		cleanupChildOutputs(request.OutputDirectory)
		return result, errors.New("invalid compaction child response")
	}
	if err := verifyChildBundle(request.OutputDirectory, result.Bundle); err != nil {
		cleanupChildOutputs(request.OutputDirectory)
		return result, err
	}
	return result, nil
}
