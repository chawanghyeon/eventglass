package app

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/engine"
)

func TestConversionStreamRequiresCompleteTerminalFraming(t *testing.T) {
	var terminal bytes.Buffer
	if err := engine.WriteConversionFrame(&terminal, engine.ConversionMessage{Version: 1, Type: "summary", Summary: &engine.ConversionSummary{}}); err != nil {
		t.Fatal(err)
	}
	frame := func(body []byte, declared uint32) []byte {
		data := make([]byte, 4, 4+len(body))
		binary.BigEndian.PutUint32(data, declared)
		return append(data, body...)
	}
	for name, data := range map[string][]byte{
		"missing terminal": nil,
		"truncated header": append(bytes.Clone(terminal.Bytes()), 0),
		"truncated body":   append(bytes.Clone(terminal.Bytes()), frame(nil, 1)...),
		"whitespace body":  append(bytes.Clone(terminal.Bytes()), frame([]byte(" "), 1)...),
		"zero frame":       append(bytes.Clone(terminal.Bytes()), frame(nil, 0)...),
		"oversized frame":  frame(nil, engine.MaxConversionFrameBytes+1),
		"second terminal":  append(bytes.Clone(terminal.Bytes()), terminal.Bytes()...),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := consumeConversionFrames(context.Background(), bytes.NewReader(data), &bytes.Buffer{}, engine.ConversionRequest{}, nil); err == nil {
				t.Fatal("incomplete conversion stream succeeded")
			}
		})
	}
	if _, err := consumeConversionFrames(context.Background(), bytes.NewReader(terminal.Bytes()), &bytes.Buffer{}, engine.ConversionRequest{}, nil); err != nil {
		t.Fatal(err)
	}
}

func TestConversionStreamRejectsInconsistentTerminalMetadata(t *testing.T) {
	for name, message := range map[string]engine.ConversionMessage{
		"version":              {Version: 2, Type: "summary", Summary: &engine.ConversionSummary{}},
		"missing summary":      {Version: 1, Type: "summary"},
		"unexpected bundle":    {Version: 1, Type: "summary", Summary: &engine.ConversionSummary{}, Bundle: &engine.ConvertedBundle{}},
		"wrong bundle count":   {Version: 1, Type: "summary", Summary: &engine.ConversionSummary{BundleCount: 1}},
		"wrong selected count": {Version: 1, Type: "summary", Summary: &engine.ConversionSummary{SelectedRecordCount: 1}},
		"wrong error count":    {Version: 1, Type: "summary", Summary: &engine.ConversionSummary{SelectedErrorCount: 1}},
		"unknown type":         {Version: 1, Type: "unexpected", Summary: &engine.ConversionSummary{}},
	} {
		t.Run(name, func(t *testing.T) {
			var input bytes.Buffer
			if err := engine.WriteConversionFrame(&input, message); err != nil {
				t.Fatal(err)
			}
			if _, err := consumeConversionFrames(context.Background(), &input, &bytes.Buffer{}, engine.ConversionRequest{}, nil); err == nil {
				t.Fatal("inconsistent terminal accepted")
			}
		})
	}
}
