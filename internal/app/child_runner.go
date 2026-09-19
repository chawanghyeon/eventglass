package app

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

const maxChildMessageBytes = 256 << 10

type ProcessConversionRunner struct {
	BinaryPath string
}

func (runner ProcessConversionRunner) Run(ctx context.Context, request engine.ConversionRequest, emit func(engine.ConvertedBundle) error) (engine.ConversionSummary, error) {
	binary := runner.BinaryPath
	if binary == "" {
		var err error
		binary, err = os.Executable()
		if err != nil {
			return engine.ConversionSummary{}, err
		}
	}
	response, err := os.CreateTemp(filepath.Dir(request.StagePath), "child-response-*.jsonl")
	if err != nil {
		return engine.ConversionSummary{}, err
	}
	responsePath := response.Name()
	defer func() {
		_ = response.Close()
		_ = os.Remove(responsePath)
	}()
	input, err := json.Marshal(engine.ChildRequest{Operation: "convert", Conversion: &request})
	if err != nil {
		return engine.ConversionSummary{}, err
	}
	command := exec.CommandContext(ctx, binary, "engine-child")
	command.Stdin = bytes.NewReader(input)
	command.Stdout = response
	var stderr boundedBuffer
	command.Stderr = &stderr
	command.Env = childEnvironment(os.Environ())
	if err := command.Run(); err != nil {
		cleanupChildOutputs(request.OutputDirectory)
		if ctx.Err() != nil {
			return engine.ConversionSummary{}, ctx.Err()
		}
		return engine.ConversionSummary{}, fmt.Errorf("engine child failed: %w", err)
	}
	if err := response.Sync(); err != nil {
		cleanupChildOutputs(request.OutputDirectory)
		return engine.ConversionSummary{}, err
	}
	if _, err := response.Seek(0, io.SeekStart); err != nil {
		cleanupChildOutputs(request.OutputDirectory)
		return engine.ConversionSummary{}, err
	}
	summary, err := consumeChildMessages(response, request, nil)
	if err != nil {
		cleanupChildOutputs(request.OutputDirectory)
		return engine.ConversionSummary{}, err
	}
	if _, err := response.Seek(0, io.SeekStart); err != nil {
		cleanupChildOutputs(request.OutputDirectory)
		return engine.ConversionSummary{}, err
	}
	emittedSummary, err := consumeChildMessages(response, request, emit)
	if err != nil {
		cleanupChildOutputs(request.OutputDirectory)
		return engine.ConversionSummary{}, err
	}
	if emittedSummary != summary {
		cleanupChildOutputs(request.OutputDirectory)
		return engine.ConversionSummary{}, errors.New("engine child response changed between validation and emission")
	}
	return summary, nil
}

func consumeChildMessages(input io.Reader, request engine.ConversionRequest, emit func(engine.ConvertedBundle) error) (engine.ConversionSummary, error) {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64<<10), maxChildMessageBytes)
	bundles := 0
	var summary *engine.ConversionSummary
	for scanner.Scan() {
		if summary != nil {
			return engine.ConversionSummary{}, errors.New("engine child emitted data after summary")
		}
		var message engine.ConversionMessage
		if err := strictAppJSON(scanner.Bytes(), &message); err != nil {
			return engine.ConversionSummary{}, err
		}
		if message.Version != engine.ConversionProtocolVersion {
			return engine.ConversionSummary{}, errors.New("unsupported engine child protocol")
		}
		switch message.Type {
		case "bundle":
			if message.Bundle == nil || message.Summary != nil || message.Bundle.Index != bundles {
				return engine.ConversionSummary{}, errors.New("invalid engine child bundle sequence")
			}
			if err := verifyChildBundle(request.OutputDirectory, *message.Bundle); err != nil {
				return engine.ConversionSummary{}, err
			}
			if emit != nil {
				if err := emit(*message.Bundle); err != nil {
					return engine.ConversionSummary{}, err
				}
			}
			bundles++
		case "summary":
			if message.Summary == nil || message.Bundle != nil || message.Summary.BundleCount != bundles {
				return engine.ConversionSummary{}, errors.New("invalid engine child summary")
			}
			value := *message.Summary
			summary = &value
		default:
			return engine.ConversionSummary{}, errors.New("unknown engine child message")
		}
	}
	if err := scanner.Err(); err != nil {
		return engine.ConversionSummary{}, err
	}
	if summary == nil {
		return engine.ConversionSummary{}, errors.New("engine child summary missing")
	}
	return *summary, nil
}

func verifyChildBundle(outputDirectory string, bundle engine.ConvertedBundle) error {
	if bundle.RowCount <= 0 || bundle.Analytics.RowCount != bundle.RowCount || bundle.Payload.RowCount != bundle.RowCount || len(bundle.ProjectIDs) == 0 || len(bundle.IdentitySHA256) != 64 {
		return errors.New("invalid child bundle metadata")
	}
	root, err := filepath.Abs(outputDirectory)
	if err != nil {
		return err
	}
	for _, output := range []engine.ConvertedFile{bundle.Analytics, bundle.Payload} {
		path, err := filepath.Abs(output.Path)
		if err != nil || filepath.Dir(path) != root {
			return errors.New("child output escaped its assigned directory")
		}
		if output.Evidence.Bytes <= 0 || output.Evidence.Bytes > engine.MaxBundleFileBytes {
			return errors.New("child output size is invalid")
		}
		if err := storage.VerifyFile(path, output.Evidence); err != nil {
			return err
		}
	}
	return nil
}

func cleanupChildOutputs(directory string) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return
	}
	for _, entry := range entries {
		_ = os.RemoveAll(filepath.Join(directory, entry.Name()))
	}
}

func childEnvironment(environment []string) []string {
	result := make([]string, 0, len(environment))
	for _, entry := range environment {
		key, _, _ := strings.Cut(entry, "=")
		upper := strings.ToUpper(key)
		if strings.HasPrefix(upper, "AWS_") || strings.HasPrefix(upper, "AZURE_") || strings.HasPrefix(upper, "GOOGLE_") || strings.HasPrefix(upper, "DUCKDB_") || strings.Contains(upper, "DATABASE_URL") || strings.Contains(upper, "SECRET") || strings.Contains(upper, "TOKEN") || strings.Contains(upper, "PASSWORD") || strings.Contains(upper, "CREDENTIAL") {
			continue
		}
		result = append(result, entry)
	}
	return result
}

type boundedBuffer struct {
	bytes.Buffer
}

func (buffer *boundedBuffer) Write(data []byte) (int, error) {
	original := len(data)
	remaining := (64 << 10) - buffer.Len()
	if remaining > 0 {
		if len(data) > remaining {
			data = data[:remaining]
		}
		_, _ = buffer.Buffer.Write(data)
	}
	return original, nil
}

func strictAppJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("trailing child JSON")
	}
	return nil
}
