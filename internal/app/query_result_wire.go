package app

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"

	generated "github.com/chawanghyeon/eventglass/api/generated"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
)

func decodeListRow(object map[string]json.RawMessage, sortName string) (generated.ListRow, query.CursorTuple, error) {
	recordID, err := requiredRawString(object["record_id"])
	if err != nil || !validHexDigest(recordID) {
		return generated.ListRow{}, query.CursorTuple{}, errors.New("invalid result record id")
	}
	projectID, err := rawInt64(object["project_id"])
	if err != nil {
		return generated.ListRow{}, query.CursorTuple{}, err
	}
	eventUS, err := rawInt64(object["event_time_us"])
	if err != nil {
		return generated.ListRow{}, query.CursorTuple{}, err
	}
	eventNS64, err := rawInt64(object["event_time_ns_remainder"])
	if err != nil || eventNS64 < 0 || eventNS64 > 999 {
		return generated.ListRow{}, query.CursorTuple{}, errors.New("invalid event nanoseconds")
	}
	receivedUS, err := rawInt64(object["received_time_us"])
	if err != nil {
		return generated.ListRow{}, query.CursorTuple{}, err
	}
	kind, err := requiredRawString(object["kind"])
	if err != nil || kind != string(model.KindError) && kind != string(model.KindLog) && kind != string(model.KindTransaction) {
		return generated.ListRow{}, query.CursorTuple{}, errors.New("invalid result kind")
	}
	level, err := requiredRawString(object["level"])
	if err != nil {
		return generated.ListRow{}, query.CursorTuple{}, errors.New("invalid result level")
	}
	message, err := requiredRawString(object["message"])
	if err != nil || !utf8.ValidString(message) {
		return generated.ListRow{}, query.CursorTuple{}, errors.New("invalid result message")
	}
	message, truncated := truncateUTF8(message, 2048)
	row := generated.ListRow{RecordId: recordID, ProjectId: strconv.FormatInt(projectID, 10), Kind: generated.Kind(kind),
		EventTimeUs: strconv.FormatInt(eventUS, 10), EventTimeNsRemainder: int(eventNS64), ReceivedTimeUs: strconv.FormatInt(receivedUS, 10),
		Level: level, Message: message, MessageTruncated: truncated}
	row.Service, err = rawString(object["service"])
	if err == nil {
		row.Environment, err = rawString(object["environment"])
	}
	if err == nil {
		row.Release, err = rawString(object["release"])
	}
	if err == nil {
		row.TraceId, err = rawString(object["trace_id"])
	}
	if err != nil {
		return row, query.CursorTuple{}, err
	}
	if value := object["severity_number"]; !rawNull(value) {
		severity, parseErr := rawInt64(value)
		if parseErr != nil || severity < math.MinInt16 || severity > math.MaxInt16 {
			return row, query.CursorTuple{}, errors.New("invalid severity")
		}
		integer := int(severity)
		row.SeverityNumber = &integer
	}
	if value := object["issue_id"]; !rawNull(value) {
		issue, parseErr := requiredRawString(value)
		if parseErr != nil || !validHexDigest(issue) {
			return row, query.CursorTuple{}, errors.New("invalid issue id")
		}
		row.IssueId = &issue
	}
	lane, laneErr := rawInt64(object["lane_id"])
	batch, batchErr := rawInt64(object["batch_seq"])
	ordinal, ordinalErr := rawInt64(object["record_ordinal"])
	if laneErr != nil || batchErr != nil || ordinalErr != nil || lane < 0 || lane >= model.LaneCount || batch < 1 || ordinal < 0 || ordinal > math.MaxInt32 {
		return row, query.CursorTuple{}, errors.New("invalid cursor columns")
	}
	tuple := query.CursorTuple{RecordID: recordID}
	if sortName == "event_desc" {
		ns := int(eventNS64)
		tuple.EventUS, tuple.EventNS = &eventUS, &ns
	} else {
		l, o := int(lane), int(ordinal)
		tuple.ReceivedUS, tuple.LaneID, tuple.BatchSeq, tuple.RecordOrdinal = &receivedUS, &l, &batch, &o
	}
	return row, tuple, nil
}

func queryStats(status control.QueryStatus, snapshot model.QuerySnapshot, operationKind string) generated.QueryStats {
	cuts := make([]generated.Cut, model.LaneCount)
	for index, lane := range snapshot.Lanes {
		cuts[index] = generated.Cut{LaneId: lane.LaneID, Seq: strconv.FormatInt(lane.CutSeq, 10)}
	}
	elapsed := status.UpdatedAt.Sub(status.CreatedAt).Milliseconds()
	if elapsed < 0 {
		elapsed = 0
	}
	objects := status.PlanFiles
	if operationKind == "detail" {
		objects *= 2
	}
	return generated.QueryStats{ScannedBytes: strconv.FormatInt(status.PlanInputBytes, 10), Objects: strconv.Itoa(objects), CacheBytes: "0", ElapsedMs: strconv.FormatInt(elapsed, 10), Cut: cuts, VisibilityLagMs: nil}
}

func openJSONLines(path string) (*bufio.Scanner, func() error, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64<<10), 2<<20)
	return scanner, file.Close, nil
}

func decodeJSONLine(line []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.UseNumber()
	var result map[string]json.RawMessage
	if err := decoder.Decode(&result); err != nil || result == nil {
		return nil, errors.New("invalid query result row")
	}
	var trailing any
	if trailingErr := decoder.Decode(&trailing); !errors.Is(trailingErr, io.EOF) {
		return nil, errors.New("query result row has trailing JSON")
	}
	return result, nil
}

func rawNull(value json.RawMessage) bool {
	return len(value) == 0 || bytes.Equal(bytes.TrimSpace(value), []byte("null"))
}

func rawString(value json.RawMessage) (*string, error) {
	if rawNull(value) {
		return nil, nil
	}
	var result string
	if json.Unmarshal(value, &result) != nil {
		return nil, errors.New("invalid string result")
	}
	return &result, nil
}

func requiredRawString(value json.RawMessage) (string, error) {
	result, err := rawString(value)
	if err != nil || result == nil {
		return "", errors.New("missing string result")
	}
	return *result, nil
}

func rawNumberString(value json.RawMessage) (string, error) {
	if rawNull(value) {
		return "", errors.New("missing numeric result")
	}
	trimmed := strings.Trim(string(bytes.TrimSpace(value)), `"`)
	if trimmed == "" {
		return "", errors.New("invalid numeric result")
	}
	return trimmed, nil
}

func rawInt64(value json.RawMessage) (int64, error) {
	encoded, err := rawNumberString(value)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(encoded, 10, 64)
}

func rawFloat64(value json.RawMessage) (float64, error) {
	encoded, err := rawNumberString(value)
	if err != nil {
		return 0, err
	}
	result, err := strconv.ParseFloat(encoded, 64)
	if err != nil || math.IsNaN(result) || math.IsInf(result, 0) {
		return 0, errors.New("invalid finite double result")
	}
	return result, nil
}

func rawBool(value json.RawMessage) (bool, error) {
	var result bool
	if json.Unmarshal(value, &result) != nil {
		return false, errors.New("invalid boolean result")
	}
	return result, nil
}

func decodeJSONObjectString(value json.RawMessage) (map[string]any, error) {
	encoded, err := requiredRawString(value)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(strings.NewReader(encoded))
	decoder.UseNumber()
	var result map[string]any
	if err := decoder.Decode(&result); err != nil || result == nil {
		return nil, errors.New("invalid JSON object result")
	}
	var trailing any
	if trailingErr := decoder.Decode(&trailing); !errors.Is(trailingErr, io.EOF) {
		return nil, errors.New("JSON object result has trailing data")
	}
	return result, nil
}

func decodeJSONArrayString(value json.RawMessage) ([]any, error) {
	encoded, err := requiredRawString(value)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(strings.NewReader(encoded))
	decoder.UseNumber()
	var result []any
	if err := decoder.Decode(&result); err != nil || result == nil {
		return nil, errors.New("invalid JSON array result")
	}
	var trailing any
	if trailingErr := decoder.Decode(&trailing); !errors.Is(trailingErr, io.EOF) {
		return nil, errors.New("JSON array result has trailing data")
	}
	return result, nil
}

func validHexDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if c < '0' || c > '9' && c < 'a' || c > 'f' {
			return false
		}
	}
	return true
}

func truncateUTF8(value string, maximum int) (string, bool) {
	if len(value) <= maximum {
		return value, false
	}
	value = value[:maximum]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value, true
}
