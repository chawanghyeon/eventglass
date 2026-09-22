package contracts

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/app"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
)

func TestEngineChildConversionProtocol(t *testing.T) {
	binary := eventglassBinary(t)
	request := conversionProcessRequest(t)
	var bundles []engine.ConvertedBundle
	summary, err := (app.ProcessConversionRunner{BinaryPath: binary}).Run(context.Background(), request, func(bundle engine.ConvertedBundle) error {
		bundles = append(bundles, bundle)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(summary.DuckDBVersion, "v2.0.0-dev84020") || summary.BundleCount != 1 || len(bundles) != 1 || bundles[0].RowCount != 1 {
		t.Fatalf("summary=%#v bundles=%#v", summary, bundles)
	}
}

func conversionProcessRequest(t *testing.T) engine.ConversionRequest {
	t.Helper()
	root := t.TempDir()
	stagePath := filepath.Join(root, "selected.jsonl")
	stage, err := os.OpenFile(stagePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	record := model.Record{
		TenantID: 1, ProjectID: 2, RecordID: strings.Repeat("a", 64), AcceptanceID: "00000000-0000-4000-8000-000000000111",
		Kind: model.KindLog, EventTimeUS: 1_700_000_000_000_000, ArrivalTimeUS: 1_700_000_000_000_001,
		Message: "child", Raw: json.RawMessage(`{"body":"child"}`), Attrs: []model.Attribute{}, SearchValues: []string{"child"}, Warnings: []string{},
		SchemaVersion: model.SchemaVersion, NormalizerVersion: model.NormalizerVersion, ScrubVersion: model.ScrubVersion,
	}
	staged := engine.StageRecord{
		Version: 1, GlobalOrdinal: 0, BatchID: "00000000-0000-4000-8000-000000000222", LaneID: 0, BatchSeq: 1,
		ReceivedTimeUS: 1_700_000_000_000_002, GroupingVersion: 1, Record: record,
	}
	if err := json.NewEncoder(stage).Encode(staged); err != nil {
		t.Fatal(err)
	}
	if err := stage.Close(); err != nil {
		t.Fatal(err)
	}
	return engine.ConversionRequest{
		Version: 1, StagePath: stagePath, OutputDirectory: filepath.Join(root, "output"), SpillDirectory: filepath.Join(root, "spill"),
		TenantID: 1, LaneID: 0, BatchSeq: 1, BatchID: staged.BatchID, SelectedRecords: 1,
		NativeMemoryBytes: 64 << 20, NativeSpillBytes: 128 << 20,
	}
}

func TestEngineChildConversionCancelWaitsForConsumerAndRetries(t *testing.T) {
	runner := app.ProcessConversionRunner{BinaryPath: eventglassBinary(t), Gate: app.NewNativeTaskGate()}
	request := conversionProcessRequest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	entered := make(chan engine.ConvertedBundle, 1)
	release := make(chan struct{})
	releaseConsumer := sync.OnceFunc(func() { close(release) })
	defer releaseConsumer()
	done := make(chan error, 1)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		_, err := runner.Run(ctx, request, func(bundle engine.ConvertedBundle) error {
			entered <- bundle
			<-release
			return ctx.Err()
		})
		done <- err
	}()
	defer func() {
		cancel()
		releaseConsumer()
		<-joined // Fatal assertions must also join before TempDir cleanup.
	}()
	var bundle engine.ConvertedBundle
	select {
	case bundle = <-entered:
	case err := <-done:
		t.Fatalf("exited before consumer: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	cancel()
	// The child may already have exited, but the consumer still owns its files
	// and the shared native permit until it joins the cancellation path.
	blockedCtx, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stop()
	_, err := runner.Run(blockedCtx, conversionProcessRequest(t), func(engine.ConvertedBundle) error {
		t.Error("second native task started before consumer returned")
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shared gate was released: %v", err)
	}
	select {
	case err := <-done:
		t.Fatalf("returned before consumer joined: %v", err)
	default:
	}
	for _, path := range []string{bundle.Analytics.Path, bundle.Payload.Path} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("live consumer lost file: %v", err)
		}
	}
	releaseConsumer()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancel did not join child and consumer")
	}
	entries, err := os.ReadDir(request.OutputDirectory)
	if err != nil || len(entries) != 0 {
		t.Fatalf("canceled output retained: %v %v", entries, err)
	}
	retryCtx, retryCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer retryCancel()
	summary, err := runner.Run(retryCtx, conversionProcessRequest(t), func(engine.ConvertedBundle) error { return nil })
	if err != nil || summary.BundleCount != 1 {
		t.Fatalf("retry: %+v %v", summary, err)
	}
}

func TestEngineChildConversionRejectsInvalidAcknowledgment(t *testing.T) {
	binary := eventglassBinary(t)
	for name, ack := range map[string]engine.ConversionContinue{
		"wrong version": {Version: 2, Index: 0, Continue: true},
		"wrong index":   {Version: 1, Index: 1, Continue: true},
		"abort":         {Version: 1, Index: 0, Continue: false},
	} {
		t.Run(name, func(t *testing.T) {
			request := conversionProcessRequest(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, binary, "engine-child")
			input, err := child.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			defer input.Close()
			output, err := child.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := child.Start(); err != nil {
				t.Fatal(err)
			}
			// Always reap even if a protocol assertion fails.
			defer func() { cancel(); _ = child.Wait() }()
			if err := engine.WriteConversionFrame(input, engine.ChildRequest{Operation: "convert", Conversion: &request}); err != nil {
				t.Fatal(err)
			}
			var message engine.ConversionMessage
			if err := engine.ReadConversionFrame(output, &message); err != nil || message.Bundle == nil {
				t.Fatalf("bundle: %+v %v", message, err)
			}
			if err := engine.WriteConversionFrame(input, ack); err != nil {
				t.Fatal(err)
			}
			input.Close()
			if err := engine.ReadConversionFrame(output, &message); err == nil {
				t.Fatal("invalid ACK yielded a terminal success")
			}
			if err := child.Wait(); err == nil || ctx.Err() != nil {
				t.Fatalf("invalid ACK not rejected promptly: %v ctx=%v", err, ctx.Err())
			}
			entries, err := os.ReadDir(request.OutputDirectory)
			if err != nil || len(entries) != 0 {
				t.Fatalf("failed child retained output: %v %v", entries, err)
			}
		})
	}
}
