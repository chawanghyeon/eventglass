package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestConversionFrameBoundsAndStrictDecode(t *testing.T) {
	var output bytes.Buffer
	want := ConversionContinue{Version: ConversionProtocolVersion, Index: 17, Continue: true}
	if err := WriteConversionFrame(&output, want); err != nil {
		t.Fatal(err)
	}
	var got ConversionContinue
	if err := ReadConversionFrame(&output, &got); err != nil || got != want {
		t.Fatalf("round trip: %+v %v", got, err)
	}
	if err := ReadConversionFrame(&output, &got); err != io.EOF {
		t.Fatalf("terminal EOF: %v", err)
	}
	for _, body := range []string{`{"version":1,"unknown":true}`, `{} {}`, ` `} {
		var frame bytes.Buffer
		var header [4]byte
		binary.BigEndian.PutUint32(header[:], uint32(len(body)))
		frame.Write(header[:])
		frame.WriteString(body)
		if err := ReadConversionFrame(&frame, &got); err == nil || err == io.EOF {
			t.Fatalf("accepted invalid body %q: %v", body, err)
		}
	}
	if err := WriteConversionFrame(&output, strings.Repeat("x", MaxConversionFrameBytes)); err == nil || output.Len() != 0 {
		t.Fatalf("oversized frame wrote bytes: %v size=%d", err, output.Len())
	}
	if err := WriteConversionFrame(shortFrameWriter{}, want); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write: %v", err)
	}
}

type shortFrameWriter struct{}

func (shortFrameWriter) Write(data []byte) (int, error) { return len(data) - 1, nil }
