package api

import (
	"encoding/json"
	"errors"
	"regexp"
	"slices"
	"strconv"

	generated "github.com/chawanghyeon/eventglass/api/generated"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
)

var canonicalInt64Pattern = regexp.MustCompile(`^-?(0|[1-9][0-9]*)$`)

func parseSearchRequest(body generated.SearchRequest) (query.PublicSearchRequest, error) {
	dataset, err := parseDataset(body.TenantId, body.ProjectIds, body.Kinds, string(body.TimeBasis), body.StartUs, body.EndUs, body.Expression, body.Filter)
	if err != nil {
		return query.PublicSearchRequest{}, err
	}
	result := query.PublicSearchRequest{Dataset: dataset, Limit: 100, Sort: "event_desc", Mode: query.ModeAuto}
	if body.Limit != nil {
		result.Limit = *body.Limit
	}
	if result.Limit < 1 || result.Limit > 1000 {
		return result, errors.New("invalid search limit")
	}
	if body.Sort != nil {
		result.Sort = string(*body.Sort)
	}
	if result.Sort != "event_desc" && result.Sort != "received_desc" {
		return result, errors.New("invalid search sort")
	}
	if body.Mode != nil {
		result.Mode = query.RequestMode(*body.Mode)
	}
	if !validRequestMode(result.Mode) {
		return result, errors.New("invalid query mode")
	}
	if body.Projection != nil && string(*body.Projection) != "list" {
		return result, errors.New("unsupported projection")
	}
	if body.ReadToken != nil {
		result.ReadToken = *body.ReadToken
	}
	if body.Cursor != nil {
		result.Cursor = *body.Cursor
	}
	if len(result.ReadToken) > query.MaxTokenPayload*2 || len(result.Cursor) > query.MaxTokenPayload*2 {
		return result, errors.New("query token is too large")
	}
	return result, nil
}

func parseAggregateRequest(body generated.AggregateRequest) (query.PublicAggregateRequest, error) {
	dataset, err := parseDataset(body.TenantId, body.ProjectIds, body.Kinds, string(body.TimeBasis), body.StartUs, body.EndUs, body.Expression, body.Filter)
	if err != nil {
		return query.PublicAggregateRequest{}, err
	}
	result := query.PublicAggregateRequest{Dataset: dataset, Mode: query.ModeAuto, Top: 100, Order: query.AggregateOrder{Metric: "count", Direction: "desc"}}
	if body.Mode != nil {
		result.Mode = query.RequestMode(*body.Mode)
	}
	if !validRequestMode(result.Mode) {
		return result, errors.New("invalid query mode")
	}
	if body.ReadToken != nil {
		result.ReadToken = *body.ReadToken
	}
	if len(result.ReadToken) > query.MaxTokenPayload*2 {
		return result, errors.New("query token is too large")
	}
	if body.Top != nil {
		result.Top = *body.Top
	}
	if result.Top < 1 || result.Top > 1000 || len(body.GroupBy) > 2 || len(body.Metrics) < 1 || len(body.Metrics) > 8 {
		return result, errors.New("invalid aggregate limits")
	}
	for _, raw := range body.GroupBy {
		dimension, err := parseGroupNode(raw, true)
		if err != nil {
			return result, err
		}
		result.GroupBy = append(result.GroupBy, dimension)
	}
	seen := map[string]bool{}
	for _, raw := range body.Metrics {
		metric := query.AggregateMetric{Name: raw.Name, Op: string(raw.Op)}
		if seen[metric.Name] {
			return result, errors.New("duplicate aggregate metric")
		}
		seen[metric.Name] = true
		if raw.Field != nil {
			field, err := parseGroupNode(*raw.Field, false)
			if err != nil {
				return result, err
			}
			metric.Field = &query.NumericField{Op: field.Op, Name: field.Name, Namespace: field.Namespace, Path: field.Path, Type: field.Type}
		}
		result.Metrics = append(result.Metrics, metric)
	}
	if body.Histogram != nil {
		interval := map[string]int64{"1s": 1_000_000, "10s": 10_000_000, "1m": 60_000_000, "5m": 300_000_000, "1h": 3_600_000_000, "1d": 86_400_000_000}[body.Histogram.Interval]
		if interval == 0 {
			return result, errors.New("invalid histogram interval")
		}
		result.Histogram = &query.AggregateHistogram{IntervalUS: interval, EmptyBuckets: body.Histogram.EmptyBuckets}
	}
	if body.Order != nil {
		result.Order = query.AggregateOrder{Metric: body.Order.Metric, Direction: string(body.Order.Direction)}
		if !seen[result.Order.Metric] || result.Order.Direction != "asc" && result.Order.Direction != "desc" {
			return result, errors.New("invalid aggregate order")
		}
	}
	return result, nil
}

func parseDataset(tenantValue generated.Int64, projectValues []generated.Int64, kinds []generated.Kind, timeBasis, startValue, endValue string, expression *string, filter *generated.FilterNode) (query.PublicDataset, error) {
	tenantID, err := parseCanonicalInt64(tenantValue)
	if err != nil || tenantID <= 0 || len(projectValues) < 1 || len(projectValues) > 100 || expression != nil && filter != nil {
		return query.PublicDataset{}, errors.New("invalid dataset scope")
	}
	projects := make([]int64, len(projectValues))
	for index, value := range projectValues {
		projects[index], err = parseCanonicalInt64(value)
		if err != nil || projects[index] <= 0 {
			return query.PublicDataset{}, errors.New("invalid project scope")
		}
	}
	startUS, startErr := parseCanonicalInt64(startValue)
	endUS, endErr := parseCanonicalInt64(endValue)
	if startErr != nil || endErr != nil || startUS >= endUS {
		return query.PublicDataset{}, errors.New("invalid dataset time range")
	}
	root := &query.Node{Op: "constant", Constant: true}
	if expression != nil {
		root, err = query.ParseCEL(*expression)
	} else if filter != nil {
		encoded, marshalErr := json.Marshal(*filter)
		if marshalErr != nil {
			return query.PublicDataset{}, marshalErr
		}
		root, err = query.DecodeJSON(encoded)
	}
	if err != nil {
		return query.PublicDataset{}, err
	}
	canonical, err := query.CanonicalFilter(root)
	if err != nil {
		return query.PublicDataset{}, err
	}
	spec := model.DatasetSpec{TenantID: tenantID, ProjectIDs: projects, TimeBasis: model.QueryTimeBasis(timeBasis), StartUS: startUS, EndUS: endUS, Filter: canonical}
	for _, kind := range kinds {
		spec.Kinds = append(spec.Kinds, model.Kind(kind))
	}
	slices.Sort(spec.ProjectIDs)
	slices.Sort(spec.Kinds)
	digest, encoded, err := query.DatasetHash(spec)
	if err != nil {
		return query.PublicDataset{}, err
	}
	return query.PublicDataset{Spec: spec, Filter: root, SHA256: digest, EncodedBytes: encoded}, nil
}

func parseGroupNode(raw generated.GroupNode, allowGroupAttr bool) (query.GroupDimension, error) {
	op, ok := raw["op"].(string)
	if !ok {
		return query.GroupDimension{}, errors.New("aggregate node requires op")
	}
	allowed := map[string]bool{"op": true}
	read := func(name string, required bool) (string, error) {
		allowed[name] = true
		value, exists := raw[name]
		if !exists && !required {
			return "", nil
		}
		text, ok := value.(string)
		if !ok || text == "" {
			return "", errors.New("invalid aggregate node")
		}
		return text, nil
	}
	result := query.GroupDimension{Op: op}
	var err error
	switch op {
	case "field":
		result.Name, err = read("name", true)
	case "attr":
		result.Namespace, err = read("namespace", true)
		if err == nil {
			result.Path, err = read("path", true)
		}
		if err == nil {
			value, typeErr := read("type", true)
			result.Type, err = query.ScalarType(value), typeErr
		}
	case "group_attr":
		if !allowGroupAttr {
			return result, errors.New("group_attr is not a metric field")
		}
		result.Namespace, err = read("namespace", true)
		if err == nil {
			result.Path, err = read("path", true)
		}
	default:
		return result, errors.New("unsupported aggregate node")
	}
	if err != nil {
		return result, err
	}
	for name := range raw {
		if !allowed[name] {
			return result, errors.New("unknown aggregate node field")
		}
	}
	return result, nil
}

func parseCanonicalInt64(value string) (int64, error) {
	if !canonicalInt64Pattern.MatchString(value) {
		return 0, errors.New("invalid int64 encoding")
	}
	return strconv.ParseInt(value, 10, 64)
}

func validRequestMode(value query.RequestMode) bool {
	return value == query.ModeAuto || value == query.ModeSync || value == query.ModeAsync
}
