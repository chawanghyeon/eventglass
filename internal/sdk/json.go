package sdk

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

type JSONLimits struct {
	MaxDepth int
	MaxNodes int
}

var DefaultJSONLimits = JSONLimits{MaxDepth: 64, MaxNodes: 20_000}
var ErrLimitExceeded = errors.New("SDK payload limit exceeded")

func DecodeJSON(data []byte, limits JSONLimits) (any, error) {
	if !utf8.Valid(data) {
		return nil, errors.New("invalid UTF-8")
	}
	if limits.MaxDepth <= 0 || limits.MaxNodes <= 0 {
		return nil, errors.New("invalid JSON limits")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	nodes := 0
	value, err := decodeValue(decoder, limits, 1, &nodes)
	if err != nil {
		return nil, err
	}
	if token, err := decoder.Token(); err != io.EOF {
		if err != nil {
			return nil, fmt.Errorf("trailing JSON: %w", err)
		}
		return nil, fmt.Errorf("trailing JSON token %v", token)
	}
	return value, nil
}

func DecodeObject(data []byte, limits JSONLimits) (map[string]any, error) {
	value, err := DecodeJSON(data, limits)
	if err != nil {
		return nil, err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("expected JSON object")
	}
	return object, nil
}

func decodeValue(decoder *json.Decoder, limits JSONLimits, depth int, nodes *int) (any, error) {
	if depth > limits.MaxDepth {
		return nil, fmt.Errorf("%w: JSON depth exceeds %d", ErrLimitExceeded, limits.MaxDepth)
	}
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	*nodes++
	if *nodes > limits.MaxNodes {
		return nil, fmt.Errorf("%w: JSON nodes exceed %d", ErrLimitExceeded, limits.MaxNodes)
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return token, nil
	}
	switch delimiter {
	case '{':
		object := make(map[string]any)
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, errors.New("JSON object key is not a string")
			}
			if _, exists := object[key]; exists {
				return nil, fmt.Errorf("duplicate JSON key %q", key)
			}
			value, err := decodeValue(decoder, limits, depth+1, nodes)
			if err != nil {
				return nil, err
			}
			object[key] = value
		}
		if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
			return nil, errors.New("unterminated JSON object")
		}
		return object, nil
	case '[':
		array := make([]any, 0)
		for decoder.More() {
			value, err := decodeValue(decoder, limits, depth+1, nodes)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		if token, err := decoder.Token(); err != nil || token != json.Delim(']') {
			return nil, errors.New("unterminated JSON array")
		}
		return array, nil
	default:
		return nil, fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
}
