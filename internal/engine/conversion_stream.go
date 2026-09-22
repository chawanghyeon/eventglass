package engine

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const MaxConversionFrameBytes = 1 << 20

// ConversionContinue releases exactly one finished file pair. The supervisor
// verifies and consumes that pair before permitting the next native COPY.
type ConversionContinue struct {
	Version  int  `json:"version"`
	Index    int  `json:"index"`
	Continue bool `json:"continue"`
}

func WriteConversionFrame(output io.Writer, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) == 0 || len(data) > MaxConversionFrameBytes {
		return errors.New("conversion frame exceeds protocol limit")
	}
	frame := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(frame, uint32(len(data)))
	copy(frame[4:], data)
	n, err := output.Write(frame)
	if err == nil && n != len(frame) {
		return io.ErrShortWrite
	}
	return err
}

func ReadConversionFrame(input io.Reader, value any) error {
	var header [4]byte
	if _, err := io.ReadFull(input, header[:]); err != nil {
		return err
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 || size > MaxConversionFrameBytes {
		return errors.New("invalid conversion frame length")
	}
	data := make([]byte, int(size))
	if _, err := io.ReadFull(input, data); err != nil {
		return fmt.Errorf("read conversion frame body: %w", err)
	}
	if err := strictJSON(data, value); err != nil {
		return fmt.Errorf("decode conversion frame body: %w", err)
	}
	return nil
}

func runConversionStream(ctx context.Context, input io.Reader, output io.Writer) error {
	var request ChildRequest
	if err := ReadConversionFrame(input, &request); err != nil {
		return err
	}
	if request.Operation != "convert" || request.Conversion == nil || request.Compaction != nil || request.Query != nil || request.QueryExport != nil {
		return errors.New("framed conversion requires only conversion input")
	}
	summary, err := Convert(ctx, *request.Conversion, func(bundle ConvertedBundle) error {
		if err := WriteConversionFrame(output, ConversionMessage{Version: ConversionProtocolVersion, Type: "bundle", Bundle: &bundle}); err != nil {
			return err
		}
		var ack ConversionContinue
		if err := ReadConversionFrame(input, &ack); err != nil {
			return err
		}
		if ack.Version != ConversionProtocolVersion || ack.Index != bundle.Index || !ack.Continue {
			return errors.New("conversion pair was not acknowledged")
		}
		return ctx.Err()
	})
	if err != nil {
		return err
	}
	return WriteConversionFrame(output, ConversionMessage{Version: ConversionProtocolVersion, Type: "summary", Summary: &summary})
}
