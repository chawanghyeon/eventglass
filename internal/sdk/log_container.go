package sdk

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

// DecodeLogContainer applies JSON depth and node limits to each log record,
// rather than incorrectly charging an entire legal batch to one record.
func DecodeLogContainer(data []byte, limits JSONLimits) (map[string]any, error) {
	if !utf8.Valid(data) {
		return nil, errors.New("invalid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return nil, errors.New("log container must be an object")
	}
	container := make(map[string]any)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil, errors.New("log container key is not a string")
		}
		if _, exists := container[key]; exists {
			return nil, fmt.Errorf("duplicate JSON key %q", key)
		}
		if key != "items" {
			var raw json.RawMessage
			if err := decoder.Decode(&raw); err != nil {
				return nil, err
			}
			value, err := DecodeJSON(raw, limits)
			if err != nil {
				return nil, err
			}
			container[key] = value
			continue
		}
		open, err := decoder.Token()
		if err != nil || open != json.Delim('[') {
			return nil, errors.New("log container items must be an array")
		}
		items := make([]any, 0)
		for decoder.More() {
			if len(items) >= 10_000 {
				return nil, fmt.Errorf("%w: canonical record count exceeds 10000", ErrLimitExceeded)
			}
			var raw json.RawMessage
			if err := decoder.Decode(&raw); err != nil {
				return nil, err
			}
			item, err := DecodeObject(raw, limits)
			if err != nil {
				return nil, err
			}
			items = append(items, item)
		}
		closeToken, err := decoder.Token()
		if err != nil || closeToken != json.Delim(']') {
			return nil, errors.New("unterminated log items array")
		}
		container[key] = items
	}
	closeToken, err := decoder.Token()
	if err != nil || closeToken != json.Delim('}') {
		return nil, errors.New("unterminated log container")
	}
	if token, err := decoder.Token(); err != io.EOF {
		if err != nil {
			return nil, fmt.Errorf("trailing JSON: %w", err)
		}
		return nil, fmt.Errorf("trailing JSON token %v", token)
	}
	return container, nil
}
