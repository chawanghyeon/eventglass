package sdk

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

const (
	MaxEnvelopeHeaderBytes = 16 << 10
	MaxItemHeaderBytes     = 16 << 10
	MaxItems               = 1_000
)

type Envelope struct {
	Header map[string]any
	Items  []Item
}

type Item struct {
	Ordinal  int
	Type     string
	Header   map[string]any
	Payload  []byte
	Value    any
	Warnings []string
}

func (item Item) Supported() bool {
	switch item.Type {
	case "event", "transaction", "log", "client_report":
		return true
	default:
		return false
	}
}

func ParseEnvelope(data []byte) (Envelope, error) {
	var envelope Envelope
	headerLine, cursor, hadNewline, err := nextLine(data, 0, MaxEnvelopeHeaderBytes)
	if err != nil {
		return envelope, fmt.Errorf("envelope header: %w", err)
	}
	envelope.Header, err = DecodeObject(headerLine, DefaultJSONLimits)
	if err != nil {
		return envelope, fmt.Errorf("envelope header: %w", err)
	}
	if !hadNewline {
		return envelope, nil
	}

	eventLike := 0
	for cursor < len(data) {
		if len(envelope.Items) >= MaxItems {
			return envelope, fmt.Errorf("%w: item count exceeds %d", ErrLimitExceeded, MaxItems)
		}
		line, next, newline, err := nextLine(data, cursor, MaxItemHeaderBytes)
		if err != nil {
			return envelope, fmt.Errorf("item %d header: %w", len(envelope.Items), err)
		}
		if len(line) == 0 {
			return envelope, fmt.Errorf("item %d has an empty header", len(envelope.Items))
		}
		if !newline {
			return envelope, fmt.Errorf("item %d header has no payload boundary", len(envelope.Items))
		}
		header, err := DecodeObject(line, DefaultJSONLimits)
		if err != nil {
			return envelope, fmt.Errorf("item %d header: %w", len(envelope.Items), err)
		}
		typeName, _ := header["type"].(string)
		if typeName == "" {
			return envelope, fmt.Errorf("item %d has no type", len(envelope.Items))
		}
		cursor = next
		payload, nextCursor, err := itemPayload(data, cursor, header)
		if err != nil {
			return envelope, fmt.Errorf("item %d payload: %w", len(envelope.Items), err)
		}
		item := Item{Ordinal: len(envelope.Items), Type: typeName, Header: header, Payload: payload}
		if item.Supported() {
			if typeName == "log" {
				item.Value, err = DecodeLogContainer(payload, DefaultJSONLimits)
			} else {
				item.Value, err = DecodeJSON(payload, DefaultJSONLimits)
			}
			if err != nil {
				return envelope, fmt.Errorf("item %d %s payload: %w", item.Ordinal, typeName, err)
			}
			if _, ok := item.Value.(map[string]any); !ok {
				return envelope, fmt.Errorf("item %d %s payload must be an object", item.Ordinal, typeName)
			}
			if HasLoneSurrogateEscape(payload) {
				item.Warnings = append(item.Warnings, "lone_surrogate_replaced")
			}
		}
		if typeName == "event" || typeName == "transaction" {
			eventLike++
			if eventLike > 1 {
				return envelope, errors.New("an envelope may contain at most one event or transaction")
			}
		}
		envelope.Items = append(envelope.Items, item)
		cursor = nextCursor
	}
	return envelope, nil
}

func HasLoneSurrogateEscape(data []byte) bool {
	for index := 0; index+5 < len(data); index++ {
		if data[index] != '\\' {
			continue
		}
		if index+1 < len(data) && data[index+1] == '\\' {
			index++
			continue
		}
		if data[index+1] != 'u' {
			continue
		}
		value, ok := decodeHex16(data[index+2 : index+6])
		if !ok || value < 0xd800 || value > 0xdfff {
			continue
		}
		if value >= 0xdc00 {
			return true
		}
		if index+11 >= len(data) || data[index+6] != '\\' || data[index+7] != 'u' {
			return true
		}
		low, ok := decodeHex16(data[index+8 : index+12])
		if !ok || low < 0xdc00 || low > 0xdfff {
			return true
		}
		index += 11
	}
	return false
}

func decodeHex16(data []byte) (uint16, bool) {
	if len(data) != 4 {
		return 0, false
	}
	var value uint16
	for _, character := range data {
		value <<= 4
		switch {
		case character >= '0' && character <= '9':
			value |= uint16(character - '0')
		case character >= 'a' && character <= 'f':
			value |= uint16(character-'a') + 10
		case character >= 'A' && character <= 'F':
			value |= uint16(character-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}

func nextLine(data []byte, cursor, limit int) ([]byte, int, bool, error) {
	if cursor >= len(data) {
		return nil, cursor, false, errors.New("unexpected end of input")
	}
	relative := bytes.IndexByte(data[cursor:], '\n')
	if relative < 0 {
		line := data[cursor:]
		if len(line) > limit {
			return nil, cursor, false, fmt.Errorf("%w: line exceeds %d bytes", ErrLimitExceeded, limit)
		}
		return line, len(data), false, nil
	}
	if relative > limit {
		return nil, cursor, false, fmt.Errorf("%w: line exceeds %d bytes", ErrLimitExceeded, limit)
	}
	return data[cursor : cursor+relative], cursor + relative + 1, true, nil
}

func itemPayload(data []byte, cursor int, header map[string]any) ([]byte, int, error) {
	if lengthValue, exists := header["length"]; exists {
		length, err := exactNonNegativeInt(lengthValue)
		if err != nil {
			return nil, cursor, fmt.Errorf("invalid length: %w", err)
		}
		if length > len(data)-cursor {
			return nil, cursor, errors.New("declared length exceeds remaining bytes")
		}
		end := cursor + length
		payload := data[cursor:end]
		if end < len(data) {
			if data[end] != '\n' {
				return nil, cursor, errors.New("declared payload is not followed by LF or EOF")
			}
			end++
		}
		return payload, end, nil
	}
	relative := bytes.IndexByte(data[cursor:], '\n')
	if relative < 0 {
		return data[cursor:], len(data), nil
	}
	return data[cursor : cursor+relative], cursor + relative + 1, nil
}

func exactNonNegativeInt(value any) (int, error) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, errors.New("length must be a JSON integer")
	}
	if bytes.ContainsAny([]byte(number.String()), ".eE") {
		return 0, errors.New("length must be an integer")
	}
	parsed, err := strconv.ParseInt(number.String(), 10, 32)
	if err != nil || parsed < 0 {
		return 0, errors.New("length is outside the supported range")
	}
	return int(parsed), nil
}
