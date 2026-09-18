package ingest

import (
	"fmt"
	"net/url"
	"strings"
)

const filteredValue = "[Filtered]"

func Scrub(value any) any {
	return scrubValue(value, "")
}

func scrubValue(value any, key string) any {
	if sensitiveKey(key) {
		return filteredValue
	}
	switch typed := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(typed))
		for childKey, child := range typed {
			if strings.EqualFold(childKey, "headers") {
				result[childKey] = scrubHeaders(child)
				continue
			}
			result[childKey] = scrubValue(child, childKey)
		}
		return result
	case []any:
		result := make([]any, len(typed))
		for index, child := range typed {
			result[index] = scrubValue(child, key)
		}
		return result
	case string:
		if strings.EqualFold(key, "url") || strings.HasSuffix(strings.ToLower(key), "_url") {
			return scrubURL(typed)
		}
		return typed
	default:
		return value
	}
}

func sensitiveKey(key string) bool {
	normalized := strings.ToLower(strings.TrimSpace(key))
	for _, part := range []string{"authorization", "cookie", "password", "passwd", "token", "secret", "api_key", "apikey"} {
		if normalized == part || strings.HasSuffix(normalized, "."+part) || strings.HasSuffix(normalized, "_"+part) {
			return true
		}
	}
	return false
}

func scrubHeaders(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, child := range typed {
			if sensitiveKey(key) || strings.EqualFold(key, "set-cookie") {
				result[key] = filteredValue
			} else {
				result[key] = scrubValue(child, key)
			}
		}
		return result
	case []any:
		result := make([]any, 0, len(typed))
		for _, child := range typed {
			pair, ok := child.([]any)
			if !ok || len(pair) != 2 {
				result = append(result, scrubValue(child, "headers"))
				continue
			}
			name, _ := pair[0].(string)
			if sensitiveKey(name) || strings.EqualFold(name, "set-cookie") {
				result = append(result, []any{pair[0], filteredValue})
			} else {
				result = append(result, []any{pair[0], scrubValue(pair[1], name)})
			}
		}
		return result
	default:
		return scrubValue(value, "headers")
	}
}

func scrubURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.RawQuery == "" {
		return raw
	}
	query := parsed.Query()
	for key, values := range query {
		for index := range values {
			values[index] = filteredValue
		}
		query[key] = values
	}
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func ensureScrubbed(raw []byte, forbidden string) error {
	if forbidden != "" && strings.Contains(string(raw), forbidden) {
		return fmt.Errorf("scrubbed record retained forbidden sentinel")
	}
	return nil
}
