package api

import (
	"testing"

	generated "github.com/chawanghyeon/eventglass/api/generated"
	"github.com/chawanghyeon/eventglass/internal/query"
)

func TestParseSearchRequestCanonicalizesDatasetAndDefaults(t *testing.T) {
	request := generated.SearchRequest{
		TenantId: "7", ProjectIds: []string{"12", "3"}, Kinds: []generated.Kind{generated.KindLog},
		TimeBasis: generated.SearchRequestTimeBasis("event"), StartUs: "-1", EndUs: "100",
	}
	parsed, err := parseSearchRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Limit != 100 || parsed.Sort != "event_desc" || parsed.Mode != query.ModeAuto || parsed.Dataset.SHA256 == "" || len(parsed.Dataset.EncodedBytes) == 0 {
		t.Fatalf("parsed=%#v", parsed)
	}
	if parsed.Dataset.Spec.ProjectIDs[0] != 3 || parsed.Dataset.Filter.Op != "constant" || !parsed.Dataset.Filter.Constant {
		t.Fatalf("dataset=%#v filter=%#v", parsed.Dataset.Spec, parsed.Dataset.Filter)
	}
}

func TestParseSearchRequestRejectsAmbiguousAndNoncanonicalScope(t *testing.T) {
	expression := "true"
	filter := generated.FilterNode{"op": "constant", "value": true}
	base := generated.SearchRequest{
		TenantId: "1", ProjectIds: []string{"2"}, Kinds: []generated.Kind{generated.KindLog},
		TimeBasis: generated.SearchRequestTimeBasis("event"), StartUs: "0", EndUs: "1",
	}
	base.Expression, base.Filter = &expression, &filter
	if _, err := parseSearchRequest(base); err == nil {
		t.Fatal("expression and filter were both accepted")
	}
	base.Expression, base.Filter, base.TenantId = nil, nil, "01"
	if _, err := parseSearchRequest(base); err == nil {
		t.Fatal("noncanonical tenant id accepted")
	}
}

func TestParseAggregateRequestBoundsAndTypedNodes(t *testing.T) {
	top := 1000
	request := generated.AggregateRequest{
		TenantId: "1", ProjectIds: []string{"2"}, Kinds: []generated.Kind{generated.KindError},
		TimeBasis: generated.AggregateRequestTimeBasis("received"), StartUs: "0", EndUs: "10000000", Top: &top,
		GroupBy: []generated.GroupNode{{"op": "group_attr", "namespace": "attributes", "path": "/region"}},
		Metrics: []generated.Metric{
			{Name: "events", Op: generated.MetricOpCount},
			{Name: "latency", Op: generated.MetricOpAvg, Field: &generated.GroupNode{"op": "attr", "namespace": "attributes", "path": "/latency", "type": "integer"}},
		},
		Histogram: &generated.Histogram{Interval: "1s", EmptyBuckets: true},
		Order:     &generated.MetricOrder{Metric: "latency", Direction: generated.Desc},
	}
	parsed, err := parseAggregateRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Top != 1000 || parsed.Histogram.IntervalUS != 1_000_000 || parsed.GroupBy[0].Op != "group_attr" || parsed.Metrics[1].Field.Type != query.IntegerType {
		t.Fatalf("parsed=%#v", parsed)
	}
	top = 1001
	if _, err := parseAggregateRequest(request); err == nil {
		t.Fatal("aggregate top above 1,000 accepted")
	}
}
