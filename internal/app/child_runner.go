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
	"strings"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

const maxChildMessageBytes = 256 << 10

type ProcessConversionRunner struct {
	BinaryPath string
	Gate       *NativeTaskGate
}

func (runner ProcessConversionRunner) Run(ctx context.Context, request engine.ConversionRequest, emit func(engine.ConvertedBundle) error) (engine.ConversionSummary, error) {
	if emit == nil {
		return engine.ConversionSummary{}, errors.New("conversion consumer is required")
	}
	release, err := runner.Gate.acquire(ctx)
	if err != nil {
		return engine.ConversionSummary{}, err
	}
	defer release()
	request.NativeMemoryBytes = runner.Gate.memoryLimit(request.NativeMemoryBytes)
	binary := runner.BinaryPath
	if binary == "" {
		binary, err = os.Executable()
		if err != nil {
			return engine.ConversionSummary{}, err
		}
	}
	inputReader, inputWriter, err := os.Pipe()
	if err != nil {
		return engine.ConversionSummary{}, err
	}
	defer inputReader.Close()
	defer inputWriter.Close()
	outputReader, outputWriter, err := os.Pipe()
	if err != nil {
		return engine.ConversionSummary{}, err
	}
	defer outputReader.Close()
	defer outputWriter.Close()
	command := exec.Command(binary, "engine-child")
	command.Stdin = inputReader
	command.Stdout = outputWriter
	var stderr boundedBuffer
	command.Stderr = &stderr
	command.Env = childEnvironment(os.Environ())
	childContext, cancel := context.WithCancel(ctx)
	defer cancel()
	joined := make(chan error, 1)
	go func() {
		err := runNativeChild(childContext, command)
		// Unblock protocol I/O after start failure or exit without a terminal
		// frame. These copies are not the child's file descriptors.
		_ = inputReader.Close()
		_ = outputWriter.Close()
		joined <- err
	}()
	err = engine.WriteConversionFrame(inputWriter, engine.ChildRequest{Operation: "convert", Conversion: &request})
	var summary engine.ConversionSummary
	if err == nil {
		summary, err = consumeConversionFrames(ctx, outputReader, inputWriter, request, emit)
	}
	if err != nil {
		cancel()
	}
	_ = inputWriter.Close()
	childErr := <-joined
	if err = errors.Join(err, childErr, ctx.Err()); err != nil {
		// Partial uploads remain unprepared intents. Reap the whole process
		// group before removing an unfinished pair or releasing its permit.
		cleanupChildOutputs(request.OutputDirectory)
		return engine.ConversionSummary{}, fmt.Errorf("engine conversion failed: %w", err)
	}
	return summary, nil
}

func consumeConversionFrames(ctx context.Context, input io.Reader, control io.Writer, request engine.ConversionRequest, emit func(engine.ConvertedBundle) error) (engine.ConversionSummary, error) {
	bundles := 0
	var rows, errorRows int64
	var summary *engine.ConversionSummary
	for {
		var message engine.ConversionMessage
		if err := engine.ReadConversionFrame(input, &message); err != nil {
			if err == io.EOF {
				break
			}
			return engine.ConversionSummary{}, err
		}
		if summary != nil {
			return engine.ConversionSummary{}, errors.New("engine child emitted data after summary")
		}
		if message.Version != engine.ConversionProtocolVersion {
			return engine.ConversionSummary{}, errors.New("unsupported engine child protocol")
		}
		switch message.Type {
		case "bundle":
			if message.Bundle == nil || message.Summary != nil || message.Bundle.Index != bundles || bundles >= request.SelectedRecords {
				return engine.ConversionSummary{}, errors.New("invalid engine child bundle sequence")
			}
			if err := verifyChildBundle(request.OutputDirectory, *message.Bundle); err != nil {
				return engine.ConversionSummary{}, err
			}
			if err := ctx.Err(); err != nil {
				return engine.ConversionSummary{}, err
			}
			if err := emit(*message.Bundle); err != nil {
				return engine.ConversionSummary{}, err
			}
			if err := ctx.Err(); err != nil {
				return engine.ConversionSummary{}, err
			}
			// COPY has finished and the child waits for this exact ACK. A
			// consumer retaining bytes must move them into its own reservation.
			for _, path := range []string{message.Bundle.Analytics.Path, message.Bundle.Payload.Path} {
				if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
					return engine.ConversionSummary{}, err
				}
			}
			if err := engine.WriteConversionFrame(control, engine.ConversionContinue{Version: engine.ConversionProtocolVersion, Index: bundles, Continue: true}); err != nil {
				return engine.ConversionSummary{}, err
			}
			rows += message.Bundle.RowCount
			if message.Bundle.Kind == model.KindError {
				errorRows += message.Bundle.RowCount
			}
			bundles++
		case "summary":
			if message.Summary == nil || message.Bundle != nil || message.Summary.BundleCount != bundles ||
				message.Summary.SelectedRecordCount != request.SelectedRecords || message.Summary.SelectedErrorCount != request.SelectedErrors ||
				rows != int64(request.SelectedRecords) || errorRows != int64(request.SelectedErrors) {
				return engine.ConversionSummary{}, errors.New("invalid engine child summary")
			}
			value := *message.Summary
			summary = &value
		default:
			return engine.ConversionSummary{}, errors.New("unknown engine child message")
		}
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
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Size() != output.Evidence.Bytes {
			return errors.Join(errors.New("child output must be a regular file with the declared size"), err)
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
