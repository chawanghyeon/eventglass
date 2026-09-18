package sdk

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestDecodeJSONPreservesNumbersAndRejectsDuplicates(t *testing.T) {
	value, err := DecodeObject([]byte(`{"integer":9223372036854775807,"decimal":1.0,"text":"안녕"}`), DefaultJSONLimits)
	if err != nil {
		t.Fatal(err)
	}
	if value["integer"] != json.Number("9223372036854775807") || value["decimal"] != json.Number("1.0") {
		t.Fatalf("numbers lost their representation: %#v", value)
	}
	for _, input := range [][]byte{
		[]byte(`{"a":1,"a":2}`),
		{0xff},
		[]byte(`{"a":1} trailing`),
	} {
		if _, err := DecodeJSON(input, DefaultJSONLimits); err == nil {
			t.Fatalf("expected input to fail: %q", input)
		}
	}
}

func TestDecodeJSONBoundsDepthAndNodes(t *testing.T) {
	if _, err := DecodeJSON([]byte(`[[[0]]]`), JSONLimits{MaxDepth: 2, MaxNodes: 20}); !errors.Is(err, ErrLimitExceeded) {
		t.Fatal("expected depth rejection")
	}
	if _, err := DecodeJSON([]byte(`[1,2,3]`), JSONLimits{MaxDepth: 4, MaxNodes: 3}); !errors.Is(err, ErrLimitExceeded) {
		t.Fatal("expected node rejection")
	}
	value, err := DecodeObject([]byte(`{"surrogate":"\ud800"}`), DefaultJSONLimits)
	if err != nil {
		t.Fatal(err)
	}
	if value["surrogate"] != "�" {
		t.Fatalf("lone surrogate was not normalized: %#v", value)
	}
	if !HasLoneSurrogateEscape([]byte(`{"surrogate":"\ud800"}`)) || HasLoneSurrogateEscape([]byte(`{"pair":"\ud83d\udc4b","literal":"\\ud800"}`)) {
		t.Fatal("lone surrogate diagnostics are incorrect")
	}
}

func TestParseEnvelopeLengthsUnknownBinaryAndEOF(t *testing.T) {
	binary := []byte{0, '\n', 0xff, 1}
	input := append([]byte("{\"event_id\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}\n{\"type\":\"attachment\",\"length\":4}\n"), binary...)
	input = append(input, []byte("\n{\"type\":\"event\",\"length\":29}\n{\"message\":\"hello\",\"value\":1}")...)
	envelope, err := ParseEnvelope(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(envelope.Items) != 2 || envelope.Items[0].Supported() || !envelope.Items[1].Supported() {
		t.Fatalf("unexpected items: %#v", envelope.Items)
	}
	if string(envelope.Items[0].Payload) != string(binary) {
		t.Fatal("unknown binary item was not preserved by declared length")
	}

	headerOnly, err := ParseEnvelope([]byte(`{"dsn":"http://key@example.invalid/1"}`))
	if err != nil || len(headerOnly.Items) != 0 {
		t.Fatalf("header-only envelope failed: %#v %v", headerOnly, err)
	}
}

func TestParseEnvelopeRejectsFramingAndEventMultiplicity(t *testing.T) {
	cases := []string{
		"{}\n{\"type\":\"event\",\"length\":10}\n{}",
		"{}\n{\"type\":\"event\"}\n{}\n{\"type\":\"transaction\"}\n{}",
		"{}\n{\"type\":\"event\",\"length\":2}\n{}x",
		"{}\n{\"type\":\"event\",\"type\":\"event\"}\n{}",
	}
	for _, input := range cases {
		if _, err := ParseEnvelope([]byte(input)); err == nil {
			t.Fatalf("expected malformed envelope to fail: %q", input)
		}
	}
	oversized := strings.Repeat("a", MaxEnvelopeHeaderBytes+1)
	if _, err := ParseEnvelope([]byte(oversized)); !errors.Is(err, ErrLimitExceeded) {
		t.Fatal("expected oversized envelope header rejection")
	}
}

func TestDecodeLogContainerAppliesLimitsPerRecord(t *testing.T) {
	container, err := DecodeLogContainer([]byte(`{"version":2,"items":[{"body":"one"},{"body":"two"}]}`), JSONLimits{MaxDepth: 3, MaxNodes: 3})
	if err != nil {
		t.Fatal(err)
	}
	if items, _ := container["items"].([]any); len(items) != 2 {
		t.Fatalf("items=%#v", container["items"])
	}
	if _, err := DecodeLogContainer([]byte(`{"items":[{"a":{"b":{"c":1}}}]}`), JSONLimits{MaxDepth: 3, MaxNodes: 20}); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("deep record returned %v", err)
	}
	if _, err := DecodeLogContainer([]byte(`{"items":[],"items":[]}`), DefaultJSONLimits); err == nil {
		t.Fatal("duplicate container key was accepted")
	}
}
