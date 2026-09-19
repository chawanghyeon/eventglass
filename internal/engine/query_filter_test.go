//go:build duckdb_use_static_lib

package engine_test

import (
	"context"
	"database/sql"
	"math/big"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/model"
	"github.com/chawanghyeon/eventglass/internal/query"
)

type oracleAttribute struct {
	Namespace string
	Path      string
	Type      query.ScalarType
	Value     any
}

type oracleRecord struct {
	ID, Message, Service string
	Attributes           []oracleAttribute
	SearchValues         []string
}

func TestFilterSQLMatchesIndependentOracle(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	createQueryFixture(t, ctx, db)

	records := []oracleRecord{
		{ID: "a", Message: "literal a_%b marker", Service: "api"},
		{ID: "b", Message: "missing differs from null", Service: "api", Attributes: []oracleAttribute{{"attributes", "/v", "null", nil}}},
		{ID: "c", Message: "integer one", Service: "api", Attributes: []oracleAttribute{{"attributes", "/v", query.IntegerType, "1"}}},
		{ID: "d", Message: "string one", Service: "api", Attributes: []oracleAttribute{{"attributes", "/v", query.StringType, "1"}}},
		{ID: "e", Message: "integer two", Service: "api", Attributes: []oracleAttribute{{"attributes", "/v", query.IntegerType, "2"}}},
		{ID: "f", Message: "array", Service: "api", Attributes: []oracleAttribute{{"attributes", "/features/0", query.StringType, "checkout"}, {"attributes", "/features/0/name", query.StringType, "checkout"}}},
		{ID: "g", Message: "dotted", Service: "api", Attributes: []oracleAttribute{{"attributes", "/a.b", query.StringType, "x"}}},
		{ID: "h", Message: "nested", Service: "api", Attributes: []oracleAttribute{{"attributes", "/a/b", query.StringType, "x"}}},
		{ID: "i", Message: "ÄPFEL", Service: "api", SearchValues: []string{"health database"}},
		{ID: "j", Message: "request timed out", Service: "api"},
	}

	cases := []string{
		`exists("attributes", "/v")`,
		`is_null("attributes", "/v")`,
		`iattr("attributes", "/v") == 1`,
		`iattr("attributes", "/v") != 1`,
		`!(iattr("attributes", "/v") == 1)`,
		`sattr("attributes", "/v") == "1"`,
		`contains(message, "a_%b")`,
		`array_contains("attributes", "/features", "checkout")`,
		`exists("attributes", "/a.b")`,
		`exists("attributes", "/a/b")`,
		`icontains(message, "ä")`,
		`matches(message, "timeout|timed out")`,
		`text("database")`,
	}
	for _, expression := range cases {
		node, err := query.ParseCEL(expression)
		if err != nil {
			t.Fatalf("parse %s: %v", expression, err)
		}
		predicate, err := query.CompilePredicate(node)
		if err != nil {
			t.Fatalf("compile %s: %v", expression, err)
		}
		got := queryIDs(t, ctx, db, "SELECT record_id FROM records r WHERE tenant_id=1 AND project_id=10 AND kind='error' AND event_time_us<200 AND batch_seq<=9 AND "+predicate.Text+" ORDER BY record_id", predicate.Args...)
		var want []string
		for _, record := range records {
			if oraclePredicate(t, node, record) {
				want = append(want, record.ID)
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: DuckDB=%v oracle=%v; SQL=%s args=%#v", expression, got, want, predicate.Text, predicate.Args)
		}
	}
}

func TestScopedPlanExecutesMandatoryPredicatesBeforeConstantOR(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	createQueryFixture(t, ctx, db)

	node, err := query.ParseCEL(`service == "api" || true`)
	if err != nil {
		t.Fatal(err)
	}
	canonical, _ := query.CanonicalFilter(node)
	spec := model.DatasetSpec{TenantID: 1, ProjectIDs: []int64{10}, Kinds: []model.Kind{model.KindError}, TimeBasis: model.QueryTimeEvent, StartUS: 100, EndUS: 200, Filter: canonical}
	var cuts [model.LaneCount]int64
	for index := range cuts {
		cuts[index] = 9
	}
	plan, err := query.BuildPlan(spec, model.SnapshotScope{RetentionFloorUS: 100, LaneCuts: cuts}, node)
	if err != nil {
		t.Fatal(err)
	}
	got := queryIDs(t, ctx, db, "SELECT record_id FROM records r WHERE "+plan.Where.Text+" ORDER BY record_id", plan.Where.Args...)
	want := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("scope bypassed: got=%v want=%v SQL=%s", got, want, plan.Where.Text)
	}
}

func TestCursorKeysetKeepsEqualTimeRowsExactlyOnce(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a, b, c, z := strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64), strings.Repeat("f", 64)
	for _, statement := range []string{
		`CREATE TABLE positions(event_time_us BIGINT,event_time_ns_remainder INTEGER,record_id VARCHAR)`,
		`INSERT INTO positions VALUES (10,5,'` + c + `'),(10,5,'` + b + `'),(10,5,'` + a + `'),(9,999,'` + z + `')`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	first := queryIDs(t, ctx, db, `SELECT record_id FROM positions r ORDER BY event_time_us DESC,event_time_ns_remainder DESC,record_id DESC LIMIT 2`)
	eventUS, eventNS := int64(10), 5
	predicate, err := query.CompileCursorPredicate("event_desc", query.CursorTuple{EventUS: &eventUS, EventNS: &eventNS, RecordID: first[len(first)-1]})
	if err != nil {
		t.Fatal(err)
	}
	second := queryIDs(t, ctx, db, `SELECT record_id FROM positions r WHERE `+predicate.Text+` ORDER BY event_time_us DESC,event_time_ns_remainder DESC,record_id DESC LIMIT 2`, predicate.Args...)
	if got, want := append(first, second...), []string{c, b, a, z}; !reflect.DeepEqual(got, want) {
		t.Fatalf("pages=%v want=%v", got, want)
	}
}

func createQueryFixture(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(ctx, `CREATE TABLE records (
		tenant_id BIGINT, project_id BIGINT, record_id VARCHAR, kind VARCHAR,
		event_time_us BIGINT, received_time_us BIGINT, lane_id INTEGER, batch_seq BIGINT,
		message VARCHAR, service VARCHAR,
		attrs STRUCT(namespace VARCHAR,path VARCHAR,value_type VARCHAR,string_value VARCHAR,integer_value DECIMAL(38,0),double_value DOUBLE,boolean_value BOOLEAN,json_value VARCHAR,unit VARCHAR)[],
		search_values VARCHAR[]
	)`)
	if err != nil {
		t.Fatal(err)
	}
	type fixture struct{ id, message, attrs, search string }
	fixtures := []fixture{
		{"a", "literal a_%b marker", `[]`, `[]`},
		{"b", "missing differs from null", `[{"namespace":"attributes","path":"/v","value_type":"null"}]`, `[]`},
		{"c", "integer one", `[{"namespace":"attributes","path":"/v","value_type":"integer","integer_value":"1"}]`, `[]`},
		{"d", "string one", `[{"namespace":"attributes","path":"/v","value_type":"string","string_value":"1"}]`, `[]`},
		{"e", "integer two", `[{"namespace":"attributes","path":"/v","value_type":"integer","integer_value":"2"}]`, `[]`},
		{"f", "array", `[{"namespace":"attributes","path":"/features/0","value_type":"string","string_value":"checkout"},{"namespace":"attributes","path":"/features/0/name","value_type":"string","string_value":"checkout"}]`, `[]`},
		{"g", "dotted", `[{"namespace":"attributes","path":"/a.b","value_type":"string","string_value":"x"}]`, `[]`},
		{"h", "nested", `[{"namespace":"attributes","path":"/a/b","value_type":"string","string_value":"x"}]`, `[]`},
		{"i", "ÄPFEL", `[]`, `["health database"]`},
		{"j", "request timed out", `[]`, `[]`},
	}
	const attrType = `'[{"namespace":"VARCHAR","path":"VARCHAR","value_type":"VARCHAR","string_value":"VARCHAR","integer_value":"DECIMAL(38,0)","double_value":"DOUBLE","boolean_value":"BOOLEAN","json_value":"VARCHAR","unit":"VARCHAR"}]'`
	for _, item := range fixtures {
		_, err = db.ExecContext(ctx, `INSERT INTO records SELECT 1,10,?,'error',110,110,0,1,?,'api',from_json(?,`+attrType+`),from_json(?,'["VARCHAR"]')`, item.id, item.message, item.attrs, item.search)
		if err != nil {
			t.Fatalf("insert %s: %v", item.id, err)
		}
	}
	// These rows satisfy the user predicate but each violates a mandatory scope dimension.
	for _, values := range [][]any{{int64(2), int64(10), "wrong-tenant", "error", int64(110), int64(110), 0, int64(1)}, {int64(1), int64(11), "wrong-project", "error", int64(110), int64(110), 0, int64(1)}, {int64(1), int64(10), "wrong-kind", "log", int64(110), int64(110), 0, int64(1)}, {int64(1), int64(10), "wrong-time", "error", int64(200), int64(200), 0, int64(1)}, {int64(1), int64(10), "wrong-cut", "error", int64(110), int64(110), 0, int64(10)}} {
		_, err = db.ExecContext(ctx, `INSERT INTO records VALUES (?,?,?,?,?,?,?,?, 'x','api',[],[])`, values...)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func queryIDs(t *testing.T, ctx context.Context, db *sql.DB, statement string, args ...any) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx, statement, args...)
	if err != nil {
		t.Fatalf("query: %v\n%s\n%#v", err, statement, args)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		result = append(result, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

type oracleValue struct {
	typeName query.ScalarType
	value    any
	present  bool
}

func oraclePredicate(t *testing.T, node *query.Node, record oracleRecord) bool {
	t.Helper()
	switch node.Op {
	case "constant":
		return node.Constant
	case "and":
		for _, child := range node.Args {
			if !oraclePredicate(t, child, record) {
				return false
			}
		}
		return true
	case "or":
		for _, child := range node.Args {
			if oraclePredicate(t, child, record) {
				return true
			}
		}
		return false
	case "not":
		return !oraclePredicate(t, node.Arg, record)
	case "eq", "ne", "lt", "le", "gt", "ge":
		left, right := oracleValueOf(node.Left, record), oracleValueOf(node.Right, record)
		if !left.present || !right.present || left.typeName != right.typeName {
			return false
		}
		comparison := oracleCompare(left, right)
		return map[string]bool{"eq": comparison == 0, "ne": comparison != 0, "lt": comparison < 0, "le": comparison <= 0, "gt": comparison > 0, "ge": comparison >= 0}[node.Op]
	case "contains", "starts_with", "ends_with", "icontains", "matches":
		value := oracleValueOf(node.Value, record)
		if !value.present {
			return false
		}
		text := value.value.(string)
		switch node.Op {
		case "contains":
			return strings.Contains(text, node.Pattern)
		case "starts_with":
			return strings.HasPrefix(text, node.Pattern)
		case "ends_with":
			return strings.HasSuffix(text, node.Pattern)
		case "icontains":
			return strings.Contains(strings.ToLower(text), strings.ToLower(node.Pattern))
		case "matches":
			return regexp.MustCompile(node.Pattern).MatchString(text)
		}
	case "text":
		for _, value := range record.SearchValues {
			if strings.Contains(value, node.Pattern) {
				return true
			}
		}
	case "exists", "is_null":
		for _, attribute := range record.Attributes {
			if attribute.Namespace == node.Namespace && attribute.Path == node.Path {
				return node.Op == "exists" || attribute.Type == "null"
			}
		}
	case "array_contains":
		prefix := node.Path + "/"
		for _, attribute := range record.Attributes {
			suffix := strings.TrimPrefix(attribute.Path, prefix)
			if attribute.Namespace == node.Namespace && strings.HasPrefix(attribute.Path, prefix) && suffix != "" && !strings.Contains(suffix, "/") && digitsOnly(suffix) {
				literal := oracleValueOf(&query.Node{Op: "literal", Type: node.Literal.Type, Literal: node.Literal}, record)
				value := oracleValue{typeName: attribute.Type, value: attribute.Value, present: true}
				if value.typeName == literal.typeName && oracleCompare(value, literal) == 0 {
					return true
				}
			}
		}
	default:
		t.Fatalf("oracle does not support %s", node.Op)
	}
	return false
}

func oracleValueOf(node *query.Node, record oracleRecord) oracleValue {
	switch node.Op {
	case "field":
		if node.Name == "message" {
			return oracleValue{query.StringType, record.Message, true}
		}
		if node.Name == "service" {
			return oracleValue{query.StringType, record.Service, true}
		}
	case "attr":
		for _, attribute := range record.Attributes {
			if attribute.Namespace == node.Namespace && attribute.Path == node.Path && attribute.Type == node.Type {
				return oracleValue{attribute.Type, attribute.Value, true}
			}
		}
	case "literal":
		return oracleValue{node.Literal.Type, node.Literal.BoundValue(), true}
	}
	return oracleValue{}
}

func oracleCompare(left, right oracleValue) int {
	if left.typeName == query.IntegerType {
		leftInt, _ := new(big.Int).SetString(left.value.(string), 10)
		rightInt, _ := new(big.Int).SetString(right.value.(string), 10)
		return leftInt.Cmp(rightInt)
	}
	switch lhs := left.value.(type) {
	case string:
		return strings.Compare(lhs, right.value.(string))
	case float64:
		rhs := right.value.(float64)
		if lhs < rhs {
			return -1
		}
		if lhs > rhs {
			return 1
		}
	case bool:
		if lhs == right.value.(bool) {
			return 0
		}
		if !lhs {
			return -1
		}
		return 1
	}
	return 0
}

func digitsOnly(value string) bool {
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
	}
	return value != ""
}
