// Package issues owns pure Issue grouping and lifecycle calculations.
package issues

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/chawanghyeon/eventglass/internal/model"
)

const GroupingVersion = 1

var ErrNotError = errors.New("only error records have Issue groups")

type Group struct {
	Version        int
	CanonicalBytes []byte
	IssueID        string
	FingerprintSHA string
	Title          string
}

type exceptionInfo struct {
	Type   string
	Value  string
	Frames []any
}

func GroupRecord(record model.Record) (Group, error) {
	if record.Kind != model.KindError {
		return Group{}, ErrNotError
	}
	if record.ProjectID <= 0 {
		return Group{}, errors.New("project ID is required")
	}
	var raw map[string]any
	decoder := json.NewDecoder(bytes.NewReader(record.Raw))
	decoder.UseNumber()
	if err := decoder.Decode(&raw); err != nil {
		return Group{}, fmt.Errorf("decode scrubbed error: %w", err)
	}
	defaults, representative, err := defaultComponents(raw, record)
	if err != nil {
		return Group{}, err
	}
	components := defaults
	if fingerprint, exists := raw["fingerprint"]; exists {
		items, ok := fingerprint.([]any)
		if !ok {
			return Group{}, errors.New("explicit fingerprint must be a string array")
		}
		if len(items) != 0 {
			expanded := make([]any, 0, len(items))
			for _, item := range items {
				literal, ok := item.(string)
				if !ok {
					return Group{}, errors.New("explicit fingerprint must be a string array")
				}
				if literal == "{{default}}" || literal == "{{ default }}" {
					expanded = append(expanded, []any{"default", defaults})
				} else {
					expanded = append(expanded, []any{"literal", literal})
				}
			}
			components = []any{"custom", expanded}
		}
	}
	canonical, err := canonicalJSON([]any{"eventglass-grouping-v1", strconv.FormatInt(record.ProjectID, 10), components})
	if err != nil {
		return Group{}, err
	}
	digest := sha256.Sum256(canonical)
	identifier := hex.EncodeToString(digest[:])
	title := record.Message
	if title == "" {
		title = representative.Type
		if representative.Value != "" && title != "" {
			title += ": " + representative.Value
		} else if title == "" {
			title = representative.Value
		}
	}
	title = truncateRunes(title, 512)
	return Group{Version: GroupingVersion, CanonicalBytes: canonical, IssueID: identifier, FingerprintSHA: identifier, Title: title}, nil
}

func defaultComponents(raw map[string]any, record model.Record) (any, exceptionInfo, error) {
	values := exceptionValues(raw["exception"])
	if len(values) == 0 {
		message := record.Message
		if record.MessageTemplate != nil {
			message = *record.MessageTemplate
		}
		return []any{"message", message}, exceptionInfo{}, nil
	}
	chainTypes := make([]any, 0, len(values))
	for _, value := range values {
		chainTypes = append(chainTypes, stringField(value, "type"))
	}
	representative := values[len(values)-1]
	info := exceptionInfo{Type: stringField(representative, "type"), Value: stringField(representative, "value")}
	stacktrace, _ := representative["stacktrace"].(map[string]any)
	frames, _ := stacktrace["frames"].([]any)
	selected := selectFrames(frames)
	if len(selected) == 0 {
		return []any{"exception", info.Type, info.Value}, info, nil
	}
	encodedFrames := make([]any, 0, len(selected))
	for _, frame := range selected {
		object, ok := frame.(map[string]any)
		if !ok {
			return nil, exceptionInfo{}, errors.New("stack frame must be an object")
		}
		encodedFrames = append(encodedFrames, []any{
			stringField(object, "module"), stringField(object, "function"), strings.ReplaceAll(stringField(object, "filename"), `\`, "/"),
		})
	}
	info.Frames = encodedFrames
	return []any{"stack", chainTypes, encodedFrames}, info, nil
}

func exceptionValues(value any) []map[string]any {
	var values []any
	switch typed := value.(type) {
	case map[string]any:
		values, _ = typed["values"].([]any)
	case []any:
		values = typed
	}
	result := make([]map[string]any, 0, len(values))
	for _, value := range values {
		if object, ok := value.(map[string]any); ok {
			result = append(result, object)
		}
	}
	return result
}

func selectFrames(frames []any) []any {
	inApp := make([]any, 0, len(frames))
	for _, frame := range frames {
		if object, ok := frame.(map[string]any); ok {
			if value, ok := object["in_app"].(bool); ok && value {
				inApp = append(inApp, frame)
			}
		}
	}
	selected := frames
	if len(inApp) != 0 {
		selected = inApp
	}
	if len(selected) > 8 {
		selected = selected[len(selected)-8:]
	}
	return selected
}

func stringField(object map[string]any, name string) string {
	value, _ := object[name].(string)
	return value
}

func canonicalJSON(value any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buffer.Bytes(), []byte{'\n'}), nil
}

func truncateRunes(value string, limit int) string {
	if utf8.RuneCountInString(value) <= limit {
		return value
	}
	runes := []rune(value)
	return string(runes[:limit])
}
