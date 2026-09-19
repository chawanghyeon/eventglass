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

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

type QueryRunner interface {
	Run(context.Context, engine.QueryRequest) (engine.QuerySummary, error)
}

type ProcessQueryRunner struct {
	BinaryPath string
}

func (runner ProcessQueryRunner) Run(ctx context.Context, request engine.QueryRequest) (engine.QuerySummary, error) {
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
	command := exec.CommandContext(ctx, binary, "engine-child")
	command.Stdin = bytes.NewReader(input)
	command.Stdout, command.Stderr = &stdout, &stderr
	command.Env = childEnvironment(os.Environ())
	if err := command.Run(); err != nil {
		_ = os.Remove(request.OutputPath)
		_ = os.RemoveAll(request.SpillDirectory)
		if ctx.Err() != nil {
			return engine.QuerySummary{}, ctx.Err()
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
