package query

import (
	"bytes"
	"strings"
	"testing"

	"github.com/chawanghyeon/eventglass/internal/model"
)

func TestJSONAndCELLowerToEquivalentFilter(t *testing.T) {
	jsonFilter := []byte(`{"op":"and","args":[{"op":"or","args":[{"op":"eq","left":{"op":"field","name":"service"},"right":{"op":"literal","type":"string","value":"api"}},{"op":"eq","left":{"op":"field","name":"service"},"right":{"op":"literal","type":"string","value":"worker"}}]},{"op":"ge","left":{"op":"field","name":"severity_number"},"right":{"op":"literal","type":"integer","value":"17"}}]}`)
	fromJSON, err := DecodeJSON(jsonFilter)
	if err != nil {
		t.Fatal(err)
	}
	fromCEL, err := ParseCEL(`(service == "api" || service == "worker") && severity_number >= 17`)
	if err != nil {
		t.Fatal(err)
	}
	jsonBytes, _ := CanonicalFilter(fromJSON)
	celBytes, _ := CanonicalFilter(fromCEL)
	if !bytes.Equal(jsonBytes, celBytes) {
		t.Fatalf("JSON/CEL differ\n%x\n%x", jsonBytes, celBytes)
	}
}

func TestBuildPlanKeepsMandatoryScopeOutsideUserOR(t *testing.T) {
	root := &Node{Op: "or", Args: []*Node{
		{Op: "constant", Constant: true},
		{Op: "eq", Left: &Node{Op: "field", Name: "service"}, Right: stringLiteral("api")},
	}}
	canonical, err := CanonicalFilter(root)
	if err != nil {
		t.Fatal(err)
	}
	spec := model.DatasetSpec{
		TenantID: 7, ProjectIDs: []int64{12, 3}, Kinds: []model.Kind{model.KindLog, model.KindError},
		TimeBasis: model.QueryTimeEvent, StartUS: 100, EndUS: 200, Filter: canonical,
	}
	var cuts [model.LaneCount]int64
	for index := range cuts {
		cuts[index] = int64(index + 10)
	}
	plan, err := BuildPlan(spec, model.SnapshotScope{RetentionFloorUS: 50, LaneCuts: cuts}, root)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(plan.Where.Text, "r.tenant_id=? AND r.project_id IN (?,?)") ||
		!strings.Contains(plan.Where.Text, "AND r.received_time_us>=? AND ((r.lane_id=? AND r.batch_seq<=?)") ||
		!strings.HasSuffix(plan.Where.Text, "AND ((TRUE OR COALESCE((r.service = ?),FALSE)))") {
		t.Fatalf("scope is not structurally outside predicate: %s", plan.Where.Text)
	}
	if got, want := len(plan.Where.Args), 1+2+2+3+model.LaneCount*2+1; got != want {
		t.Fatalf("args=%d want=%d: %#v", got, want, plan.Where.Args)
	}
	if plan.Where.Args[0] != int64(7) || plan.Where.Args[1] != int64(3) || plan.Where.Args[2] != int64(12) {
		t.Fatalf("scope args are not deterministic: %#v", plan.Where.Args[:3])
	}

	mismatch := spec
	mismatch.Filter = []byte("different")
	if _, err := BuildPlan(mismatch, model.SnapshotScope{LaneCuts: cuts}, root); err == nil {
		t.Fatal("accepted predicate not bound by dataset hash")
	}
}

func TestDatasetHashNormalizesSetsButPreservesExpressionOrder(t *testing.T) {
	a := &Node{Op: "and", Args: []*Node{
		{Op: "eq", Left: &Node{Op: "field", Name: "service"}, Right: stringLiteral("api")},
		{Op: "eq", Left: &Node{Op: "field", Name: "level"}, Right: stringLiteral("error")},
	}}
	b := &Node{Op: "and", Args: []*Node{a.Args[1], a.Args[0]}}
	aBytes, _ := CanonicalFilter(a)
	bBytes, _ := CanonicalFilter(b)
	base := model.DatasetSpec{TenantID: 1, ProjectIDs: []int64{9, 2}, Kinds: []model.Kind{model.KindLog, model.KindError}, TimeBasis: model.QueryTimeEvent, StartUS: 1, EndUS: 2, Filter: aBytes}
	aHash, _, err := DatasetHash(base)
	if err != nil {
		t.Fatal(err)
	}
	reordered := base
	reordered.ProjectIDs = []int64{2, 9}
	reordered.Kinds = []model.Kind{model.KindError, model.KindLog}
	reorderedHash, _, _ := DatasetHash(reordered)
	if aHash != reorderedHash {
		t.Fatal("set ordering changed dataset identity")
	}
	reordered.Filter = bBytes
	bHash, _, _ := DatasetHash(reordered)
	if aHash == bHash {
		t.Fatal("expression node ordering was erased")
	}
}

func TestDatasetIdentityRoundTripsAndRejectsTrailingBytes(t *testing.T) {
	root := &Node{Op: "constant", Constant: true}
	filter, err := CanonicalFilter(root)
	if err != nil {
		t.Fatal(err)
	}
	spec := model.DatasetSpec{TenantID: 7, ProjectIDs: []int64{9, 2}, Kinds: []model.Kind{model.KindLog, model.KindError}, TimeBasis: model.QueryTimeReceived, StartUS: -1, EndUS: 10, Filter: filter}
	_, encoded, err := DatasetHash(spec)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeDatasetIdentity(encoded)
	if err != nil || decoded.TenantID != 7 || decoded.ProjectIDs[0] != 2 || decoded.Kinds[0] != model.KindError || decoded.StartUS != -1 {
		t.Fatalf("decoded=%#v err=%v", decoded, err)
	}
	if _, err := DecodeDatasetIdentity(append(encoded, 0)); err == nil {
		t.Fatal("dataset identity with trailing bytes accepted")
	}
}

func stringLiteral(value string) *Node {
	literal := Literal{Type: StringType, String: value}
	return &Node{Op: "literal", Type: StringType, Literal: &literal}
}

func TestMissingNegationAndTypedAttributeShape(t *testing.T) {
	node, err := ParseCEL(`!(iattr("attributes", "/v") == 1)`)
	if err != nil {
		t.Fatal(err)
	}
	predicate, err := CompilePredicate(node)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(predicate.Text, "NOT") || !strings.Contains(predicate.Text, "COALESCE") || len(predicate.Args) != 4 {
		t.Fatalf("predicate=%s args=%#v", predicate.Text, predicate.Args)
	}
	if predicate.Args[0] != "attributes" || predicate.Args[1] != "/v" || predicate.Args[2] != "integer" || predicate.Args[3] != "1" {
		t.Fatalf("args=%#v", predicate.Args)
	}
}

func TestLiteralSubstringAndSQLInjectionStayBound(t *testing.T) {
	pattern := `a_%b' OR TRUE --`
	node := &Node{Op: "contains", Value: &Node{Op: "field", Name: "message"}, Pattern: pattern}
	predicate, err := CompilePredicate(node)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(predicate.Text, pattern) || strings.Contains(predicate.Text, "LIKE") || len(predicate.Args) != 1 || predicate.Args[0] != pattern {
		t.Fatalf("predicate=%s args=%#v", predicate.Text, predicate.Args)
	}
}

func TestPointersWideIntegersAndInvalidForms(t *testing.T) {
	valid := []string{
		`{"op":"eq","left":{"op":"attr","type":"integer","namespace":"attributes","path":"/a.b"},"right":{"op":"literal","type":"integer","value":"99999999999999999999999999999999999999"}}`,
		`{"op":"eq","left":{"op":"attr","type":"integer","namespace":"attributes","path":"/leading"},"right":{"op":"literal","type":"integer","value":"-0000000001"}}`,
		`{"op":"eq","left":{"op":"attr","type":"string","namespace":"attributes","path":"/a~1b"},"right":{"op":"literal","type":"string","value":"x"}}`,
	}
	for _, input := range valid {
		if _, err := DecodeJSON([]byte(input)); err != nil {
			t.Fatalf("valid %s: %v", input, err)
		}
	}
	invalid := []string{
		`{"op":"literal","type":"integer","value":"100000000000000000000000000000000000000"}`,
		`{"op":"exists","namespace":"attributes","path":"/bad~x"}`,
		`{"op":"constant","value":true,"value":false}`,
	}
	for _, input := range invalid {
		if _, err := DecodeJSON([]byte(input)); err == nil {
			t.Fatalf("accepted invalid %s", input)
		}
	}
	for _, expression := range []string{`[1,2].all(x,x>0)`, `matches(message,"[")`, `service.foo == "x"`, `iattr(service,"/x") > 1`} {
		if _, err := ParseCEL(expression); err == nil {
			t.Fatalf("accepted invalid CEL %s", expression)
		}
	}
	for _, expression := range []string{`true`, `service in ["api", "worker"]`, `battr("attributes", "/enabled") == true`} {
		if _, err := ParseCEL(expression); err != nil {
			t.Fatalf("rejected approved CEL %s: %v", expression, err)
		}
	}
	if err := Validate(&Node{Op: "literal", Type: BooleanType, Literal: &Literal{Type: BooleanType, Boolean: true}}); err == nil {
		t.Fatal("accepted a value node as the filter root")
	}
}

func TestNodeAndRegexLimits(t *testing.T) {
	args := make([]*Node, 17)
	for index := range args {
		args[index] = &Node{Op: "constant", Constant: true}
	}
	if err := Validate(&Node{Op: "and", Args: args}); err == nil {
		t.Fatal("accepted 17 boolean arguments")
	}
	regexes := make([]*Node, 5)
	for index := range regexes {
		regexes[index] = &Node{Op: "matches", Value: &Node{Op: "field", Name: "message"}, Pattern: "x"}
	}
	if err := Validate(&Node{Op: "and", Args: regexes}); err == nil {
		t.Fatal("accepted five regexes")
	}
}
