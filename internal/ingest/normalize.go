package ingest

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/sdk"
)

const (
	MaxCanonicalRecords = 10_000
	MaxCanonicalBytes   = 20 << 20
	MaxRecordBytes      = 1 << 20
	MaxTypedAttributes  = 1_000
)

var ErrLimitExceeded = errors.New("canonical payload limit exceeded")

type NormalizeOptions struct {
	TenantID       int64
	ProjectID      int64
	AcceptanceID   string
	ArrivalTime    time.Time
	DefaultService string
	ForbiddenValue string
}

func NormalizeEnvelope(envelope sdk.Envelope, options NormalizeOptions) (model.NormalizedRequest, error) {
	batch := model.NormalizedRequest{TenantID: options.TenantID, ProjectID: options.ProjectID, AcceptanceID: options.AcceptanceID}
	if batch.AcceptanceID == "" {
		return batch, errors.New("acceptance ID is required")
	}
	if options.ArrivalTime.IsZero() {
		return batch, errors.New("arrival time is required")
	}
	envelopeEventID := stringValue(envelope.Header["event_id"])
	sdkName, sdkVersion := envelopeSDK(envelope.Header)
	envelopeSDKJSON := optionalJSON(envelope.Header, "sdk")
	sentAtJSON := optionalJSON(envelope.Header, "sent_at")
	for _, item := range envelope.Items {
		switch item.Type {
		case "event", "transaction":
			payload := item.Value.(map[string]any)
			record, err := normalizeRecord(payload, item.Type, item.Ordinal, 0, envelopeEventID, sdkName, sdkVersion, envelopeSDKJSON, sentAtJSON, options)
			if err != nil {
				return batch, err
			}
			record.Warnings = append(record.Warnings, item.Warnings...)
			batch.Records = append(batch.Records, record)
		case "log":
			logs, err := logItems(item)
			if err != nil {
				return batch, fmt.Errorf("item %d: %w", item.Ordinal, err)
			}
			for index, payload := range logs {
				record, err := normalizeRecord(payload, "log", item.Ordinal, index, "", sdkName, sdkVersion, envelopeSDKJSON, sentAtJSON, options)
				if err != nil {
					return batch, err
				}
				if requestsInference(item.Value.(map[string]any)) {
					record.Warnings = append(record.Warnings, "inference_disabled")
				}
				record.Warnings = append(record.Warnings, item.Warnings...)
				batch.Records = append(batch.Records, record)
			}
		case "client_report":
			outcomes, err := clientReportOutcomes(item.Ordinal, item.Value.(map[string]any))
			if err != nil {
				return batch, fmt.Errorf("item %d: %w", item.Ordinal, err)
			}
			batch.Outcomes = append(batch.Outcomes, outcomes...)
		default:
			batch.Unsupported++
			batch.UnsupportedItems = append(batch.UnsupportedItems, model.UnsupportedItem{
				ItemOrdinal: item.Ordinal, Type: item.Type, Bytes: len(item.Payload),
			})
		}
		if len(batch.Records) > MaxCanonicalRecords {
			return batch, fmt.Errorf("%w: canonical record count exceeds %d", ErrLimitExceeded, MaxCanonicalRecords)
		}
	}
	canonicalBytes := 0
	for index := range batch.Records {
		encoded, err := json.Marshal(batch.Records[index])
		if err != nil {
			return batch, err
		}
		if len(encoded) > MaxRecordBytes {
			return batch, fmt.Errorf("%w: record %d exceeds %d bytes", ErrLimitExceeded, index, MaxRecordBytes)
		}
		canonicalBytes += len(encoded)
	}
	// Size the request without serializing all records into a second full
	// buffer. Replace metadata's "records":null with the measured JSON array.
	metadata := batch
	metadata.Records = nil
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return batch, err
	}
	canonicalBytes += len(encoded)
	if len(batch.Records) > 0 {
		canonicalBytes += len(batch.Records) - 3
	}
	if canonicalBytes > MaxCanonicalBytes {
		return batch, fmt.Errorf("%w: request exceeds %d canonical bytes", ErrLimitExceeded, MaxCanonicalBytes)
	}
	return batch, nil
}

func logItems(item sdk.Item) ([]map[string]any, error) {
	container := item.Value.(map[string]any)
	var (
		items []map[string]any
		err   error
	)
	version, exists := container["version"]
	if !exists {
		items, err = logItemsAbsentVersion(container)
	} else if number, ok := version.(json.Number); !ok {
		return nil, errors.New("log version must be an integer")
	} else {
		switch number.String() {
		case "1":
			items, err = logItemsVersion1(container)
		case "2":
			items, err = logItemsVersion2(container)
		default:
			return nil, fmt.Errorf("unsupported log version %s", number.String())
		}
	}
	if err != nil {
		return nil, err
	}
	if declared, exists := item.Header["item_count"]; exists {
		count, err := exactCount(declared)
		if err != nil || count != len(items) {
			return nil, errors.New("log item_count does not match items")
		}
	}
	return items, nil
}

func logItemsAbsentVersion(container map[string]any) ([]map[string]any, error) {
	return extractLogItems(container)
}

func logItemsVersion1(container map[string]any) ([]map[string]any, error) {
	return extractLogItems(container)
}

func logItemsVersion2(container map[string]any) ([]map[string]any, error) {
	return extractLogItems(container)
}

func extractLogItems(container map[string]any) ([]map[string]any, error) {
	items, ok := container["items"].([]any)
	if !ok {
		return nil, errors.New("log container requires items")
	}
	result := make([]map[string]any, 0, len(items))
	for index, item := range items {
		object, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("log item %d must be an object", index)
		}
		result = append(result, object)
	}
	return result, nil
}

func exactCount(value any) (int, error) {
	number, ok := value.(json.Number)
	if !ok || strings.ContainsAny(number.String(), ".eE") {
		return 0, errors.New("count must be an integer")
	}
	count, err := strconv.ParseInt(number.String(), 10, 32)
	if err != nil || count < 0 {
		return 0, errors.New("count is outside the supported range")
	}
	return int(count), nil
}

func normalizeRecord(payload map[string]any, itemType string, itemOrdinal, recordOrdinal int, envelopeEventID, sdkName, sdkVersion string, envelopeSDKJSON, sentAtJSON json.RawMessage, options NormalizeOptions) (model.Record, error) {
	scrubbed := Scrub(payload).(map[string]any)
	record := model.Record{
		TenantID: options.TenantID, ProjectID: options.ProjectID, AcceptanceID: options.AcceptanceID,
		ItemOrdinal: itemOrdinal, RecordOrdinal: recordOrdinal,
		ArrivalTimeUS: options.ArrivalTime.UTC().UnixMicro(), SchemaVersion: model.SchemaVersion,
		NormalizerVersion: model.NormalizerVersion, ScrubVersion: model.ScrubVersion,
		EnvelopeSDKJSON: envelopeSDKJSON, SentAtJSON: sentAtJSON,
	}
	if itemType == "log" {
		record.Kind = model.KindLog
	} else if itemType == "transaction" {
		record.Kind = model.KindTransaction
	} else {
		record.Kind = model.KindError
	}

	timestamp := scrubbed["timestamp"]
	if timestamp == nil {
		record.EventTimeUS = record.ArrivalTimeUS
		record.TimestampSource = "arrival"
	} else {
		micros, remainder, original, err := parseTimestamp(timestamp)
		if err != nil {
			return record, fmt.Errorf("item %d record %d timestamp: %w", itemOrdinal, recordOrdinal, err)
		}
		record.EventTimeUS, record.EventTimeNSRemainder = micros, remainder
		record.TimestampSource, record.TimestampOriginal = "sdk", original
	}

	if record.Kind != model.KindLog {
		payloadID := stringValue(scrubbed["event_id"])
		selected := payloadID
		if envelopeEventID != "" {
			selected = envelopeEventID
			if payloadID != "" && !eventIDsEquivalent(payloadID, envelopeEventID) {
				record.Warnings = append(record.Warnings, "event_id_conflict")
			}
		}
		if selected != "" {
			if normalized, ok := normalizeEventID(selected); ok {
				record.SourceEventID = &normalized
			} else {
				record.Warnings = append(record.Warnings, "invalid_event_id")
			}
		}
	}

	record.SDKName, record.SDKVersion = optional(sdkName), optional(sdkVersion)
	if record.Kind == model.KindLog {
		if err := normalizeLog(&record, scrubbed, options.DefaultService); err != nil {
			return record, err
		}
	} else {
		normalizeEvent(&record, scrubbed, options.DefaultService)
	}
	record.Raw, _ = json.Marshal(scrubbed)
	if err := ensureScrubbed(record.Raw, options.ForbiddenValue); err != nil {
		return record, err
	}
	// The source ID is a dedupe key, not an occurrence ID. A later acceptance
	// after dedupe expiry must not collide with a retained Issue occurrence.
	record.RecordID = recordID(strconv.FormatInt(options.ProjectID, 10), options.AcceptanceID, strconv.Itoa(itemOrdinal), strconv.Itoa(recordOrdinal))
	return record, nil
}

func eventIDsEquivalent(first, second string) bool {
	firstNormalized, firstOK := normalizeEventID(first)
	secondNormalized, secondOK := normalizeEventID(second)
	if firstOK && secondOK {
		return firstNormalized == secondNormalized
	}
	return first == second
}

func normalizeLog(record *model.Record, payload map[string]any, defaultService string) error {
	record.Message = stringValue(payload["body"])
	record.OriginalLevel = stringValue(payload["level"])
	record.Level = normalizeLevel(record.OriginalLevel, false)
	attrs, _ := payload["attributes"].(map[string]any)
	if len(attrs) > MaxTypedAttributes {
		return fmt.Errorf("%w: typed attributes exceed %d", ErrLimitExceeded, MaxTypedAttributes)
	}
	record.Attrs = attributesFromNamespace("attributes", attrs)
	severity, valid := severityNumber(payload["severity_number"])
	if !valid {
		severity, valid = severityFromAttribute(attrs["sentry.severity_number"])
	}
	if valid {
		record.SeverityNumber = &severity
		if severityLevel(severity) != record.Level && record.Level != "unknown" {
			record.Warnings = append(record.Warnings, "severity_level_conflict")
		}
	} else if payload["severity_number"] != nil || attrs["sentry.severity_number"] != nil {
		record.Warnings = append(record.Warnings, "invalid_severity_number")
	}
	if value := attributeString(attrs, "sentry.severity_text"); value != "" && record.OriginalLevel == "" {
		record.OriginalLevel = value
		record.Level = normalizeLevel(value, false)
	}
	if record.SeverityNumber == nil {
		if value, ok := representativeSeverity(record.Level); ok {
			record.SeverityNumber = &value
		}
	}
	record.MessageTemplate = optional(attributeString(attrs, "sentry.message.template"))
	record.Release = attributeOptional(attrs, "sentry.release")
	record.Environment = attributeOptional(attrs, "sentry.environment")
	record.Service = firstAttributeOptional(attrs, []string{"service.name"}, optional(defaultService))
	record.Logger = firstAttributeOptional(attrs, []string{"logger.name"}, mapOptional(payload, "logger"))
	record.ServerName = firstAttributeOptional(attrs, []string{"server.address", "sentry.server.address", "server.name"}, nil)
	record.SDKName = firstAttributeOptional(attrs, []string{"sentry.sdk.name"}, record.SDKName)
	record.SDKVersion = firstAttributeOptional(attrs, []string{"sentry.sdk.version"}, record.SDKVersion)
	record.TraceID = validID(stringValue(payload["trace_id"]), 32, "invalid_trace_id", &record.Warnings)
	span := stringValue(payload["span_id"])
	if span == "" {
		span = attributeString(attrs, "sentry.trace.parent_span_id")
	}
	record.SpanID = validID(span, 16, "invalid_span_id", &record.Warnings)
	record.SearchValues = appendSearchValues(record.Message, attrs)
	return nil
}

func normalizeEvent(record *model.Record, payload map[string]any, defaultService string) {
	record.OriginalLevel = stringValue(payload["level"])
	record.Level = normalizeLevel(record.OriginalLevel, true)
	record.Message, record.MessageTemplate = eventMessage(payload)
	record.Release = mapOptional(payload, "release")
	record.Environment = mapOptional(payload, "environment")
	record.Logger = mapOptional(payload, "logger")
	record.Platform = mapOptional(payload, "platform")
	record.ServerName = mapOptional(payload, "server_name")
	if sdkObject, ok := payload["sdk"].(map[string]any); ok {
		record.SDKName = preferMapOptional(sdkObject, "name", record.SDKName)
		record.SDKVersion = preferMapOptional(sdkObject, "version", record.SDKVersion)
	}
	tags, _ := payload["tags"].(map[string]any)
	contexts, _ := payload["contexts"].(map[string]any)
	if service, exists := mapString(tags, "service.name"); exists {
		record.Service = &service
	} else if service, exists := nestedStringPresent(contexts, "service", "name"); exists {
		record.Service = &service
	} else {
		record.Service = optional(defaultService)
	}
	if trace, ok := contexts["trace"].(map[string]any); ok {
		record.TraceID = validID(stringValue(trace["trace_id"]), 32, "invalid_trace_id", &record.Warnings)
		record.SpanID = validID(stringValue(trace["span_id"]), 16, "invalid_span_id", &record.Warnings)
	}
	for _, namespace := range []string{"tags", "extra", "contexts", "user", "request", "sdk"} {
		if object, ok := payload[namespace].(map[string]any); ok {
			record.Attrs = append(record.Attrs, eventAttributesFromNamespace(namespace, object)...)
		}
	}
	record.SearchValues = appendSearchValues(record.Message, payload)
}

func eventMessage(payload map[string]any) (string, *string) {
	if logentry, ok := payload["logentry"].(map[string]any); ok {
		if formatted := stringValue(logentry["formatted"]); formatted != "" {
			return formatted, optional(stringValue(logentry["message"]))
		}
	}
	if message := stringValue(payload["message"]); message != "" {
		return message, nil
	}
	if message, ok := payload["message"].(map[string]any); ok {
		if formatted := stringValue(message["formatted"]); formatted != "" {
			return formatted, optional(stringValue(message["message"]))
		}
	}
	if message := exceptionMessage(payload["exception"]); message != "" {
		return message, nil
	}
	if logentry, ok := payload["logentry"].(map[string]any); ok {
		return stringValue(logentry["message"]), optional(stringValue(logentry["message"]))
	}
	return "", nil
}

func exceptionMessage(value any) string {
	var values []any
	switch typed := value.(type) {
	case map[string]any:
		values, _ = typed["values"].([]any)
	case []any:
		values = typed
	}
	if len(values) == 0 {
		return ""
	}
	last, ok := values[len(values)-1].(map[string]any)
	if !ok {
		return ""
	}
	if message := stringValue(last["value"]); message != "" {
		return message
	}
	return stringValue(last["type"])
}

func parseTimestamp(value any) (int64, uint16, string, error) {
	switch typed := value.(type) {
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, typed)
		if err != nil {
			return 0, 0, typed, errors.New("invalid RFC3339 timestamp")
		}
		nanoseconds := new(big.Int).Mul(big.NewInt(parsed.UTC().Unix()), big.NewInt(1_000_000_000))
		nanoseconds.Add(nanoseconds, big.NewInt(int64(parsed.UTC().Nanosecond())))
		micros, remainder, err := splitNanoseconds(nanoseconds)
		return micros, remainder, typed, err
	case json.Number:
		nanoseconds, err := decimalEpochNanoseconds(typed.String())
		if err != nil {
			return 0, 0, typed.String(), errors.New("invalid decimal timestamp")
		}
		micros, remainder, err := splitNanoseconds(nanoseconds)
		return micros, remainder, typed.String(), err
	default:
		return 0, 0, fmt.Sprint(value), errors.New("timestamp must be RFC3339 or decimal epoch seconds")
	}
}

func splitNanoseconds(nanoseconds *big.Int) (int64, uint16, error) {
	micros, remainder := new(big.Int), new(big.Int)
	micros.QuoRem(nanoseconds, big.NewInt(1_000), remainder)
	if nanoseconds.Sign() < 0 && remainder.Sign() != 0 {
		micros.Sub(micros, big.NewInt(1))
		remainder.Add(remainder, big.NewInt(1_000))
	}
	if !micros.IsInt64() {
		return 0, 0, errors.New("timestamp outside int64 microseconds")
	}
	return micros.Int64(), uint16(remainder.Int64()), nil
}

func decimalEpochNanoseconds(raw string) (*big.Int, error) {
	mantissa, exponentText := raw, "0"
	if index := strings.IndexAny(mantissa, "eE"); index >= 0 {
		mantissa, exponentText = mantissa[:index], mantissa[index+1:]
	}
	exponent, err := strconv.Atoi(exponentText)
	if err != nil {
		return nil, err
	}
	negative := strings.HasPrefix(mantissa, "-")
	mantissa = strings.TrimPrefix(strings.TrimPrefix(mantissa, "+"), "-")
	fractionDigits := 0
	if point := strings.IndexByte(mantissa, '.'); point >= 0 {
		fractionDigits = len(mantissa) - point - 1
		mantissa = mantissa[:point] + mantissa[point+1:]
	}
	coefficient, ok := new(big.Int).SetString(mantissa, 10)
	if !ok {
		return nil, errors.New("invalid decimal")
	}
	if negative {
		coefficient.Neg(coefficient)
	}
	power := exponent - fractionDigits + 9
	if power >= 0 {
		if coefficient.Sign() == 0 {
			return coefficient, nil
		}
		if power > 30 {
			return nil, errors.New("decimal is outside timestamp range")
		}
		factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(power)), nil)
		return coefficient.Mul(coefficient, factor), nil
	}
	divisorPower := -power
	if divisorPower > len(mantissa)+1 {
		if coefficient.Sign() < 0 {
			return big.NewInt(-1), nil
		}
		return big.NewInt(0), nil
	}
	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(divisorPower)), nil)
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(coefficient, divisor, remainder)
	if coefficient.Sign() < 0 && remainder.Sign() != 0 {
		quotient.Sub(quotient, big.NewInt(1))
	}
	return quotient, nil
}

func attributesFromNamespace(namespace string, object map[string]any) []model.Attribute {
	result := make([]model.Attribute, 0, len(object))
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := object[key]
		typed, unit := value, ""
		declared := ""
		if wrapper, ok := value.(map[string]any); ok {
			if candidate, exists := wrapper["value"]; exists && wrapper["type"] != nil {
				typed, declared, unit = candidate, stringValue(wrapper["type"]), stringValue(wrapper["unit"])
			}
		}
		attribute := model.Attribute{Namespace: namespace, Path: "/" + escapePointer(key), Unit: optional(unit)}
		fillAttribute(&attribute, typed, declared)
		result = append(result, attribute)
	}
	return result
}

func eventAttributesFromNamespace(namespace string, object map[string]any) []model.Attribute {
	result := make([]model.Attribute, 0, len(object))
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		appendEventAttribute(&result, namespace, "/"+escapePointer(key), object[key])
	}
	return result
}

func appendEventAttribute(result *[]model.Attribute, namespace, path string, value any) {
	switch typed := value.(type) {
	case map[string]any:
		if len(typed) == 0 {
			attribute := model.Attribute{Namespace: namespace, Path: path}
			fillAttribute(&attribute, typed, "")
			*result = append(*result, attribute)
			return
		}
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			appendEventAttribute(result, namespace, path+"/"+escapePointer(key), typed[key])
		}
	case []any:
		if len(typed) == 0 {
			attribute := model.Attribute{Namespace: namespace, Path: path}
			fillAttribute(&attribute, typed, "")
			*result = append(*result, attribute)
			return
		}
		for index, child := range typed {
			appendEventAttribute(result, namespace, path+"/"+strconv.Itoa(index), child)
		}
	default:
		attribute := model.Attribute{Namespace: namespace, Path: path}
		fillAttribute(&attribute, value, "")
		*result = append(*result, attribute)
	}
}

func fillAttribute(attribute *model.Attribute, value any, declared string) {
	if declared != "" && !matchesDeclaredType(value, declared) {
		attribute.ValueType, attribute.JSONValue = "invalid", mustJSON(value)
		return
	}
	switch typed := value.(type) {
	case string:
		attribute.ValueType, attribute.StringValue = "string", &typed
	case bool:
		attribute.ValueType, attribute.BooleanValue = "boolean", &typed
	case json.Number:
		if declared == "double" || strings.ContainsAny(typed.String(), ".eE") {
			parsed, err := strconv.ParseFloat(typed.String(), 64)
			if err == nil && !math.IsInf(parsed, 0) && !math.IsNaN(parsed) {
				attribute.ValueType, attribute.DoubleValue = "double", &parsed
				return
			}
			attribute.ValueType, attribute.JSONValue = "invalid", mustJSON(value)
			return
		}
		digits := strings.TrimLeft(typed.String(), "+-")
		if len(digits) <= 38 {
			text := typed.String()
			attribute.ValueType, attribute.IntegerValue = "integer", &text
		} else {
			attribute.ValueType, attribute.JSONValue = "big_integer", mustJSON(value)
		}
	case nil:
		attribute.ValueType, attribute.JSONValue = "null", json.RawMessage("null")
	case []any:
		attribute.ValueType, attribute.JSONValue = "array", mustJSON(value)
	case map[string]any:
		attribute.ValueType, attribute.JSONValue = "object", mustJSON(value)
	default:
		attribute.ValueType, attribute.JSONValue = "invalid", mustJSON(value)
	}
}

func matchesDeclaredType(value any, declared string) bool {
	switch declared {
	case "string":
		_, ok := value.(string)
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "integer":
		number, ok := value.(json.Number)
		return ok && !strings.ContainsAny(number.String(), ".eE")
	case "double":
		_, ok := value.(json.Number)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	default:
		return false
	}
}

func severityFromAttribute(value any) (int16, bool) {
	if wrapper, ok := value.(map[string]any); ok {
		return severityNumber(wrapper["value"])
	}
	return severityNumber(value)
}

func severityNumber(value any) (int16, bool) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, false
	}
	parsed, err := strconv.ParseInt(number.String(), 10, 16)
	if err != nil || parsed < 1 || parsed > 24 {
		return 0, false
	}
	return int16(parsed), true
}

func normalizeLevel(level string, eventDefault bool) string {
	switch strings.ToLower(level) {
	case "warn":
		return "warning"
	case "critical":
		return "fatal"
	case "trace", "debug", "info", "warning", "error", "fatal":
		return strings.ToLower(level)
	case "":
		if eventDefault {
			return "error"
		}
		return "unknown"
	default:
		return "unknown"
	}
}

func representativeSeverity(level string) (int16, bool) {
	values := map[string]int16{"trace": 1, "debug": 5, "info": 9, "warning": 13, "error": 17, "fatal": 21}
	value, ok := values[level]
	return value, ok
}

func severityLevel(value int16) string {
	switch {
	case value <= 4:
		return "trace"
	case value <= 8:
		return "debug"
	case value <= 12:
		return "info"
	case value <= 16:
		return "warning"
	case value <= 20:
		return "error"
	default:
		return "fatal"
	}
}

func attributeString(attrs map[string]any, key string) string {
	value, exists := attrs[key]
	if !exists {
		return ""
	}
	if wrapper, ok := value.(map[string]any); ok {
		return stringValue(wrapper["value"])
	}
	return stringValue(value)
}

func attributeOptional(attrs map[string]any, key string) *string {
	value, exists := attrs[key]
	if !exists {
		return nil
	}
	if wrapper, ok := value.(map[string]any); ok {
		value = wrapper["value"]
	}
	text, ok := value.(string)
	if !ok {
		return nil
	}
	return &text
}

func firstAttributeOptional(attrs map[string]any, keys []string, fallback *string) *string {
	for _, key := range keys {
		if _, exists := attrs[key]; exists {
			return attributeOptional(attrs, key)
		}
	}
	return fallback
}

func appendSearchValues(message string, value any) []string {
	result := make([]string, 0)
	if message != "" {
		result = append(result, message)
	}
	var walk func(any)
	walk = func(candidate any) {
		switch typed := candidate.(type) {
		case map[string]any:
			keys := make([]string, 0, len(typed))
			for key := range typed {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				walk(typed[key])
			}
		case []any:
			for _, child := range typed {
				walk(child)
			}
		case string:
			if typed != "" && typed != filteredValue && typed != message {
				result = append(result, typed)
			}
		}
	}
	walk(value)
	return result
}

func validID(value string, width int, warning string, warnings *[]string) *string {
	if value == "" {
		return nil
	}
	if normalized, ok := normalizeTraceID(value, width); ok {
		return &normalized
	}
	*warnings = append(*warnings, warning)
	return nil
}

func envelopeSDK(header map[string]any) (string, string) {
	sdkObject, _ := header["sdk"].(map[string]any)
	return stringValue(sdkObject["name"]), stringValue(sdkObject["version"])
}

func optionalJSON(object map[string]any, key string) json.RawMessage {
	value, exists := object[key]
	if !exists {
		return nil
	}
	return mustJSON(Scrub(value))
}

func requestsInference(container map[string]any) bool {
	settings, ok := container["ingest_settings"].(map[string]any)
	if !ok {
		return false
	}
	for _, key := range []string{"infer_ip", "infer_user_agent"} {
		if value, exists := settings[key]; exists && stringValue(value) != "never" {
			return true
		}
	}
	return false
}

func clientReportOutcomes(ordinal int, payload map[string]any) ([]model.Outcome, error) {
	items, ok := payload["discarded_events"].([]any)
	if !ok {
		return nil, errors.New("client report requires discarded_events")
	}
	result := make([]model.Outcome, 0, len(items))
	for index, value := range items {
		item, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("discarded event %d must be an object", index)
		}
		quantityValue, ok := item["quantity"].(json.Number)
		quantity, err := strconv.ParseInt(fmt.Sprint(quantityValue), 10, 64)
		category, categoryOK := item["category"].(string)
		reason, reasonOK := item["reason"].(string)
		if !ok || err != nil || quantity < 0 || !categoryOK || category == "" || !reasonOK || reason == "" {
			return nil, fmt.Errorf("discarded event %d is malformed", index)
		}
		result = append(result, model.Outcome{ItemOrdinal: ordinal, Category: category, Reason: reason, Quantity: quantity, Approximate: true})
	}
	return result, nil
}

func stringValue(value any) string {
	if typed, ok := value.(string); ok {
		return typed
	}
	return ""
}
func optional(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
func mapOptional(object map[string]any, key string) *string {
	value, exists := mapString(object, key)
	if !exists {
		return nil
	}
	return &value
}
func preferMapOptional(object map[string]any, key string, fallback *string) *string {
	if _, exists := object[key]; !exists {
		return fallback
	}
	return mapOptional(object, key)
}
func mapString(object map[string]any, key string) (string, bool) {
	value, exists := object[key]
	if !exists {
		return "", false
	}
	text, ok := value.(string)
	return text, ok
}
func nestedStringPresent(object map[string]any, keys ...string) (string, bool) {
	var current any = object
	for _, key := range keys {
		next, ok := current.(map[string]any)
		if !ok {
			return "", false
		}
		var exists bool
		current, exists = next[key]
		if !exists {
			return "", false
		}
	}
	text, ok := current.(string)
	return text, ok
}
func escapePointer(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "~", "~0"), "/", "~1")
}
func mustJSON(value any) json.RawMessage { encoded, _ := json.Marshal(value); return encoded }
