package ingest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"

	"github.com/chawanghyeon/eventglass/internal/model"
)

const DedupeHashVersion = 1

type dedupeHashInput struct {
	Version       int             `json:"version"`
	Kind          model.Kind      `json:"kind"`
	SourceEventID string          `json:"source_event_id"`
	Raw           json.RawMessage `json:"raw"`
}

// DedupePayloadSHA256 returns no key for logs or records without a valid
// source event ID. The hash intentionally excludes occurrence and arrival data.
func DedupePayloadSHA256(record model.Record) (string, bool, error) {
	if record.SourceEventID == nil || (record.Kind != model.KindError && record.Kind != model.KindTransaction) {
		return "", false, nil
	}
	sourceEventID, valid := normalizeEventID(*record.SourceEventID)
	if !valid {
		return "", false, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(record.Raw))
	decoder.UseNumber()
	var raw map[string]any
	if err := decoder.Decode(&raw); err != nil {
		return "", false, errors.New("invalid dedupe raw JSON")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return "", false, errors.New("trailing dedupe raw JSON")
	}
	delete(raw, "event_id")
	canonicalRaw, err := encodeJSONWithoutNewline(raw)
	if err != nil {
		return "", false, err
	}
	encoded, err := encodeJSONWithoutNewline(dedupeHashInput{
		Version: DedupeHashVersion, Kind: record.Kind, SourceEventID: sourceEventID, Raw: canonicalRaw,
	})
	if err != nil {
		return "", false, err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), true, nil
}

func encodeJSONWithoutNewline(value any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	encoded := buffer.Bytes()
	if len(encoded) == 0 || encoded[len(encoded)-1] != '\n' {
		return nil, errors.New("JSON encoder did not terminate value")
	}
	return append([]byte(nil), encoded[:len(encoded)-1]...), nil
}
