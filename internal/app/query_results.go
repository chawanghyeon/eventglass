package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"

	generated "github.com/chawanghyeon/eventglass/api/generated"
	"github.com/chawanghyeon/eventglass/internal/api"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
)

type publicSearchResult struct {
	Rows       []generated.ListRow  `json:"rows"`
	ReadToken  string               `json:"read_token"`
	NextCursor *string              `json:"next_cursor"`
	Complete   bool                 `json:"complete"`
	Stats      generated.QueryStats `json:"stats"`
	Warnings   []string             `json:"warnings"`
}

type publicAggregateResult struct {
	Groups    []generated.AggregateGroup `json:"groups"`
	ReadToken string                     `json:"read_token"`
	Complete  bool                       `json:"complete"`
	Stats     generated.QueryStats       `json:"stats"`
	Warnings  []string                   `json:"warnings"`
}

type publicRecordDetail struct {
	Record      map[string]any `json:"record"`
	Raw         map[string]any `json:"raw"`
	EnvelopeSDK any            `json:"envelope_sdk"`
	ReadToken   string         `json:"read_token"`
}

func (service *PublicQueryService) finalizeQueryResult(ctx context.Context, tokenHash [32]byte, status control.QueryStatus) (any, error) {
	if status.Result == nil || service.Exporter == nil {
		return nil, errors.New("query result artifact is unavailable")
	}
	if err := ensurePrivateDirectory(service.ScratchDir); err != nil {
		return nil, err
	}
	directory, err := os.MkdirTemp(service.ScratchDir, "public-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(directory)
	parquetPath := filepath.Join(directory, "result.parquet")
	if err := service.Store.DownloadToFile(ctx, status.Result.ObjectKey, parquetPath, status.Result.Bytes, status.Result.SHA256); err != nil {
		return nil, err
	}
	jsonPath := filepath.Join(directory, "result.jsonl")
	if _, err := service.Exporter.Export(ctx, engine.QueryExportRequest{Version: engine.QueryExecutionProtocolVersion, InputPath: parquetPath, OutputPath: jsonPath}); err != nil {
		return nil, err
	}
	var operation engine.QueryOperation
	if err := strictAppJSON(status.Operation, &operation); err != nil || operation.Version != engine.QueryExecutionProtocolVersion || !operationMatchesPublicKind(status.OperationKind, operation.Kind) || operation.Result.Kind != operation.Kind {
		return nil, errors.Join(engine.ErrQueryExecutionInvalid, err)
	}
	snapshot, err := service.Control.LoadSnapshot(ctx, tokenHash, status.TenantID, status.SnapshotID, status.DatasetHash)
	if err != nil {
		return nil, err
	}
	readToken, err := service.Tokens.SignRead(snapshot)
	if err != nil {
		return nil, err
	}
	stats := queryStats(status, snapshot, operation.Kind)
	var final any
	switch operation.Kind {
	case "rows":
		final, err = service.finalizeRows(jsonPath, operation.Result, snapshot, readToken, stats)
	case "aggregate":
		final, err = service.finalizeAggregate(jsonPath, operation.Result, readToken, stats)
	case "detail":
		final, err = finalizeDetail(jsonPath, operation.Result, readToken)
	default:
		return nil, engine.ErrQueryExecutionInvalid
	}
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(final)
	if err != nil || int64(len(encoded)+1) > engine.MaxPublicQueryResultBytes {
		return nil, errors.Join(engine.ErrQueryExecutionLimit, err)
	}
	return final, nil
}

func operationMatchesPublicKind(publicKind, engineKind string) bool {
	return publicKind == "search" && engineKind == "rows" || publicKind == engineKind && (engineKind == "aggregate" || engineKind == "detail")
}

func finalizeDetail(path string, plan engine.QueryResultPlan, readToken string) (publicRecordDetail, error) {
	lines, closeLines, err := openJSONLines(path)
	if err != nil {
		return publicRecordDetail{}, err
	}
	defer closeLines()
	if !lines.Scan() {
		if err := lines.Err(); err != nil {
			return publicRecordDetail{}, err
		}
		return publicRecordDetail{}, api.ErrPublicQueryNotFound
	}
	object, err := decodeJSONLine(lines.Bytes())
	if err != nil {
		return publicRecordDetail{}, err
	}
	if lines.Scan() {
		return publicRecordDetail{}, errors.New("record identity is not unique")
	}
	if err := lines.Err(); err != nil {
		return publicRecordDetail{}, err
	}
	recordID, err := requiredRawString(object["record_id"])
	if err != nil || recordID != plan.RecordID {
		return publicRecordDetail{}, errors.New("detail result identity mismatch")
	}
	record, err := decodeJSONObjectString(object["canonical_metadata_json"])
	if err != nil || record["record_id"] != recordID {
		return publicRecordDetail{}, errors.New("invalid canonical record metadata")
	}
	raw, err := decodeJSONObjectString(object["raw_json"])
	if err != nil {
		return publicRecordDetail{}, errors.New("invalid scrubbed record payload")
	}
	warnings, err := decodeJSONArrayString(object["normalization_warnings_json"])
	if err != nil {
		return publicRecordDetail{}, errors.New("invalid record warnings")
	}
	record["warnings"] = warnings
	for _, field := range []string{"tenant_id", "project_id", "event_time_us", "arrival_time_us"} {
		if value, ok := record[field].(json.Number); ok {
			if _, parseErr := strconv.ParseInt(value.String(), 10, 64); parseErr != nil {
				return publicRecordDetail{}, errors.New("invalid canonical record integer")
			}
			record[field] = value.String()
		}
	}
	for _, field := range []string{"received_time_us", "batch_seq"} {
		value, parseErr := rawInt64(object[field])
		if parseErr != nil {
			return publicRecordDetail{}, parseErr
		}
		record[field] = strconv.FormatInt(value, 10)
	}
	for _, field := range []string{"lane_id", "record_ordinal"} {
		value, parseErr := rawInt64(object[field])
		if parseErr != nil || value < 0 || value > math.MaxInt32 || field == "lane_id" && value >= model.LaneCount {
			return publicRecordDetail{}, errors.New("invalid detail position")
		}
		record[field] = int(value)
	}
	if rawNull(object["issue_id"]) {
		record["issue_id"] = nil
	} else {
		issueID, issueErr := requiredRawString(object["issue_id"])
		if issueErr != nil || !validHexDigest(issueID) {
			return publicRecordDetail{}, errors.New("invalid detail issue identity")
		}
		record["issue_id"] = issueID
	}
	if !rawNull(object["grouping_version"]) {
		version, versionErr := rawInt64(object["grouping_version"])
		if versionErr != nil || version < 1 || version > math.MaxInt32 {
			return publicRecordDetail{}, errors.New("invalid detail grouping version")
		}
		record["grouping_version"] = int(version)
	}
	var envelope any
	if !rawNull(object["envelope_sdk_json"]) {
		envelope, err = decodeJSONObjectString(object["envelope_sdk_json"])
		if err != nil {
			return publicRecordDetail{}, errors.New("invalid envelope SDK metadata")
		}
	}
	return publicRecordDetail{Record: record, Raw: raw, EnvelopeSDK: envelope, ReadToken: readToken}, nil
}

func (service *PublicQueryService) finalizeRows(path string, plan engine.QueryResultPlan, snapshot model.QuerySnapshot, readToken string, stats generated.QueryStats) (publicSearchResult, error) {
	lines, closeLines, err := openJSONLines(path)
	if err != nil {
		return publicSearchResult{}, err
	}
	defer closeLines()
	rows := make([]generated.ListRow, 0, plan.Limit+1)
	var last query.CursorTuple
	for lines.Scan() {
		object, err := decodeJSONLine(lines.Bytes())
		if err != nil {
			return publicSearchResult{}, err
		}
		row, tuple, err := decodeListRow(object, plan.Sort)
		if err != nil {
			return publicSearchResult{}, err
		}
		rows = append(rows, row)
		if len(rows) == plan.Limit {
			last = tuple
		}
		if len(rows) > plan.Limit+1 {
			return publicSearchResult{}, query.ErrQueryLimit
		}
	}
	if err := lines.Err(); err != nil {
		return publicSearchResult{}, err
	}
	result := publicSearchResult{Rows: rows, ReadToken: readToken, Complete: true, Stats: stats, Warnings: []string{}}
	if len(rows) > plan.Limit {
		result.Rows = rows[:plan.Limit]
		cursor, err := service.Tokens.SignCursor(snapshot, plan.CursorHash, plan.Sort, plan.Limit, last)
		if err != nil {
			return publicSearchResult{}, err
		}
		result.NextCursor = &cursor
	}
	return result, nil
}

func (service *PublicQueryService) finalizeAggregate(path string, plan engine.QueryResultPlan, readToken string, stats generated.QueryStats) (publicAggregateResult, error) {
	lines, closeLines, err := openJSONLines(path)
	if err != nil {
		return publicAggregateResult{}, err
	}
	defer closeLines()
	order, err := aggregateRankOrder(plan)
	if err != nil {
		return publicAggregateResult{}, err
	}
	next := func() (*query.AggregateRankedGroup, error) {
		if !lines.Scan() {
			if err := lines.Err(); err != nil {
				return nil, err
			}
			return nil, nil
		}
		object, err := decodeJSONLine(lines.Bytes())
		if err != nil {
			return nil, err
		}
		group, rank, err := decodeAggregateGroup(object, plan)
		if err != nil {
			return nil, err
		}
		rank.Payload = group
		return &rank, nil
	}
	selected, err := query.SelectAggregateTopK(next, plan.Top, order)
	if err != nil {
		return publicAggregateResult{}, err
	}
	groups := make([]generated.AggregateGroup, len(selected))
	for index, group := range selected {
		groups[index] = group.Payload.(generated.AggregateGroup)
	}
	return publicAggregateResult{Groups: groups, ReadToken: readToken, Complete: true, Stats: stats, Warnings: []string{}}, nil
}

func aggregateRankOrder(plan engine.QueryResultPlan) (query.AggregateRankOrder, error) {
	result := query.AggregateRankOrder{Kind: query.RankByCount, Descending: plan.OrderDirection == "desc"}
	if plan.OrderMetric == "count" {
		return result, nil
	}
	for _, metric := range plan.Metrics {
		if metric.Name != plan.OrderMetric {
			continue
		}
		if metric.Op == "count" {
			return result, nil
		}
		if metric.FieldType == "double" {
			result.Kind = query.RankByDouble
		} else if metric.Op == "avg" {
			result.Kind = query.RankByIntegerAvg
		} else {
			result.Kind = query.RankByIntegerSum
		}
		return result, nil
	}
	return result, errors.New("aggregate order metric is missing")
}

func decodeAggregateGroup(object map[string]json.RawMessage, plan engine.QueryResultPlan) (generated.AggregateGroup, query.AggregateRankedGroup, error) {
	result := generated.AggregateGroup{Keys: make([]generated.TypedValue, len(plan.Groups)), Metrics: map[string]generated.MetricValue{}}
	values := make([]query.AggregateGroupValue, len(plan.Groups))
	var bucket *int64
	if plan.Histogram {
		value, err := rawInt64(object["bucket_start_us"])
		if err != nil {
			return result, query.AggregateRankedGroup{}, err
		}
		bucket = &value
		encoded := strconv.FormatInt(value, 10)
		result.BucketStartUs = &encoded
	}
	for index := range plan.Groups {
		value, public, err := decodeGroupValue(object, index)
		if err != nil {
			return result, query.AggregateRankedGroup{}, err
		}
		values[index], result.Keys[index] = value, public
	}
	dimensions := make([]query.GroupDimension, len(plan.Groups))
	for index, group := range plan.Groups {
		dimensions[index] = query.GroupDimension{Op: group.Op, Name: group.Name, Namespace: group.Namespace, Path: group.Path, Type: query.ScalarType(group.Type)}
	}
	key, err := query.EncodeAggregateGroupKey(dimensions, values, bucket)
	if err != nil {
		return result, query.AggregateRankedGroup{}, err
	}
	rank := query.AggregateRankedGroup{Key: key}
	for index, metric := range plan.Metrics {
		value, metricRank, total, err := decodeMetricValue(object, index, metric)
		if err != nil {
			return result, rank, err
		}
		result.Metrics[metric.Name] = value
		if index == 0 {
			rank.Count = total
		}
		if plan.OrderMetric == metric.Name {
			rank.IntegerState, rank.Double, rank.Count = metricRank.IntegerState, metricRank.Double, metricRank.Count
		}
	}
	return result, rank, nil
}

func decodeGroupValue(object map[string]json.RawMessage, index int) (query.AggregateGroupValue, generated.TypedValue, error) {
	prefix := fmt.Sprintf("g%d_", index)
	typeName, err := rawString(object[prefix+"type"])
	if err != nil || typeName == nil {
		return query.AggregateGroupValue{}, generated.TypedValue{}, errors.New("aggregate group type is missing")
	}
	value := query.AggregateGroupValue{Type: *typeName}
	public := generated.TypedValue{Type: *typeName}
	switch *typeName {
	case query.GroupMissing, query.GroupNull:
	case string(query.StringType):
		value.String, err = rawString(object[prefix+"string"])
		if value.String != nil {
			public.Value = *value.String
		}
	case string(query.IntegerType):
		var encoded string
		encoded, err = rawNumberString(object[prefix+"integer"])
		value.Integer, public.Value = &encoded, encoded
	case string(query.DoubleType):
		var number float64
		number, err = rawFloat64(object[prefix+"double"])
		value.Double, public.Value = &number, number
	case string(query.BooleanType):
		var boolean bool
		boolean, err = rawBool(object[prefix+"boolean"])
		value.Boolean, public.Value = &boolean, boolean
	default:
		return value, public, errors.New("unsupported aggregate group type")
	}
	return value, public, err
}

func decodeMetricValue(object map[string]json.RawMessage, index int, metric engine.QueryResultMetric) (generated.MetricValue, query.AggregateRankedGroup, int64, error) {
	prefix := fmt.Sprintf("m%d_", index)
	valid, err := rawInt64(object[prefix+"valid"])
	if err != nil || valid < 0 {
		return generated.MetricValue{}, query.AggregateRankedGroup{}, 0, errors.New("invalid aggregate valid count")
	}
	excluded, err := rawInt64(object[prefix+"excluded"])
	if err != nil || excluded < 0 || valid > math.MaxInt64-excluded {
		return generated.MetricValue{}, query.AggregateRankedGroup{}, 0, errors.New("invalid aggregate excluded count")
	}
	result := generated.MetricValue{ValidCount: strconv.FormatInt(valid, 10), ExcludedCount: strconv.FormatInt(excluded, 10)}
	rank := query.AggregateRankedGroup{}
	if metric.Op == "count" {
		result.Type, result.Value, rank.Count = "integer", strconv.FormatInt(valid, 10), valid
		return result, rank, valid + excluded, nil
	}
	if metric.FieldType == "integer" {
		state := query.IntegerState{Count: valid, Excluded: excluded}
		for limb := range state.Limbs {
			state.Limbs[limb], err = rawNumberString(object[fmt.Sprintf("%sl%d", prefix, limb)])
			if err != nil {
				return result, rank, 0, err
			}
		}
		result.Type = "integer"
		switch metric.Op {
		case "sum":
			result.Value, err = query.FinalizeIntegerSum(state)
			rank.IntegerState = &state
		case "avg":
			result.Type = "decimal"
			result.Value, err = query.FinalizeIntegerAverage(state)
			rank.IntegerState = &state
		case "min", "max":
			if valid == 0 {
				result.Value = nil
			} else {
				encoded, valueErr := rawNumberString(object[prefix+metric.Op])
				if valueErr != nil {
					return result, rank, 0, valueErr
				}
				limbs, limbErr := query.DecimalLimbs(encoded)
				if limbErr != nil {
					return result, rank, 0, limbErr
				}
				result.Value = encoded
				rank.IntegerState = &query.IntegerState{Limbs: limbs, Count: 1}
			}
		}
		return result, rank, valid + excluded, err
	}
	result.Type = "double"
	var numeric *float64
	if valid > 0 {
		name := metric.Op
		if metric.Op == "avg" || metric.Op == "sum" {
			name = "sum"
		}
		value, valueErr := rawFloat64(object[prefix+name])
		if valueErr != nil {
			return result, rank, 0, valueErr
		}
		if metric.Op == "avg" {
			value /= float64(valid)
		}
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return result, rank, 0, errors.New("nonfinite aggregate result")
		}
		numeric, result.Value = &value, value
	}
	if valid == 0 {
		result.Value = nil
	}
	rank.Double = numeric
	return result, rank, valid + excluded, nil
}
