package query

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/chawanghyeon/eventglass/internal/model"
)

type SQLPredicate struct {
	Text string
	Args []any
}

type CompiledPlan struct {
	Plan  model.QueryPlan
	Where SQLPredicate
}

type sqlCompiler struct{ args []any }

func CompilePredicate(root *Node) (SQLPredicate, error) {
	if err := Validate(root); err != nil {
		return SQLPredicate{}, err
	}
	compiler := &sqlCompiler{}
	text, _, err := compiler.node(root)
	if err != nil {
		return SQLPredicate{}, err
	}
	return SQLPredicate{Text: text, Args: compiler.args}, nil
}

func CompileCursorPredicate(sortName string, last CursorTuple) (SQLPredicate, error) {
	if !validCursor(sortName, 1, last) {
		return SQLPredicate{}, errors.New("invalid cursor tuple")
	}
	switch sortName {
	case "event_desc":
		return SQLPredicate{
			Text: `(r.event_time_us,r.event_time_ns_remainder,r.record_id) < (?,?,?)`,
			Args: []any{*last.EventUS, *last.EventNS, last.RecordID},
		}, nil
	case "received_desc":
		return SQLPredicate{
			Text: `(r.received_time_us,r.lane_id,r.batch_seq,r.record_ordinal,r.record_id) < (?,?,?,?,?)`,
			Args: []any{*last.ReceivedUS, *last.LaneID, *last.BatchSeq, *last.RecordOrdinal, last.RecordID},
		}, nil
	default:
		return SQLPredicate{}, errors.New("unsupported cursor sort")
	}
}

// BuildPlan is the only public composition point for a row scan. It keeps the
// authenticated dataset and captured snapshot scope outside the user predicate,
// so even a constant-true OR expression cannot weaken mandatory predicates.
func BuildPlan(spec model.DatasetSpec, snapshot model.SnapshotScope, root *Node) (CompiledPlan, error) {
	canonical, err := CanonicalFilter(root)
	if err != nil {
		return CompiledPlan{}, err
	}
	if !bytes.Equal(canonical, spec.Filter) {
		return CompiledPlan{}, errors.New("dataset filter does not match predicate")
	}
	digest, _, err := DatasetHash(spec)
	if err != nil {
		return CompiledPlan{}, err
	}
	for _, cut := range snapshot.LaneCuts {
		if cut < 0 {
			return CompiledPlan{}, errors.New("snapshot lane cut is invalid")
		}
	}
	predicate, err := CompilePredicate(root)
	if err != nil {
		return CompiledPlan{}, err
	}

	projects := append([]int64(nil), spec.ProjectIDs...)
	kinds := append([]model.Kind(nil), spec.Kinds...)
	if len(kinds) == 0 {
		kinds = []model.Kind{model.KindError, model.KindLog, model.KindTransaction}
	}
	// DatasetHash already validates and normalizes set ordering for identity. SQL
	// order is immaterial, but sorting here makes plans deterministic as well.
	slices.Sort(projects)
	slices.Sort(kinds)
	args := make([]any, 0, 8+len(projects)+len(kinds)+model.LaneCount*2+len(predicate.Args))
	args = append(args, spec.TenantID)
	projectSlots := bindList(&args, projects)
	kindSlots := bindList(&args, kinds)
	timeColumn := "r.event_time_us"
	if spec.TimeBasis == model.QueryTimeReceived {
		timeColumn = "r.received_time_us"
	}
	args = append(args, spec.StartUS, spec.EndUS, snapshot.RetentionFloorUS)
	laneParts := make([]string, 0, model.LaneCount)
	for lane, cut := range snapshot.LaneCuts {
		laneParts = append(laneParts, "(r.lane_id=? AND r.batch_seq<=?)")
		args = append(args, lane, cut)
	}
	args = append(args, predicate.Args...)
	where := "r.tenant_id=? AND r.project_id IN (" + projectSlots + ")" +
		" AND r.kind IN (" + kindSlots + ")" +
		" AND " + timeColumn + ">=? AND " + timeColumn + "<?" +
		" AND r.received_time_us>=?" +
		" AND (" + strings.Join(laneParts, " OR ") + ")" +
		" AND (" + predicate.Text + ")"
	planSpec := spec
	planSpec.ProjectIDs = projects
	planSpec.Kinds = kinds
	planSpec.Filter = append([]byte(nil), canonical...)
	return CompiledPlan{
		Plan:  model.QueryPlan{Version: 1, DatasetSHA256: digest, Dataset: planSpec, Snapshot: snapshot},
		Where: SQLPredicate{Text: where, Args: args},
	}, nil
}

func bindList[T ~int64 | ~string](args *[]any, values []T) string {
	slots := make([]string, len(values))
	for index, value := range values {
		slots[index] = "?"
		*args = append(*args, value)
	}
	return strings.Join(slots, ",")
}

func (compiler *sqlCompiler) bind(value any) string {
	compiler.args = append(compiler.args, value)
	return "?"
}

func (compiler *sqlCompiler) node(node *Node) (string, ScalarType, error) {
	switch node.Op {
	case "constant":
		if node.Constant {
			return "TRUE", BooleanType, nil
		}
		return "FALSE", BooleanType, nil
	case "field":
		column, ok := fixedColumns[node.Name]
		if !ok {
			return "", "", errors.New("unsupported fixed field")
		}
		return "r." + column, fieldTypes[node.Name], nil
	case "attr":
		valueColumn := map[ScalarType]string{StringType: "string_value", IntegerType: "integer_value", DoubleType: "double_value", BooleanType: "boolean_value"}[node.Type]
		text := `(SELECT a.` + valueColumn + ` FROM UNNEST(r.attrs) AS u(a) WHERE a.namespace=` + compiler.bind(node.Namespace) + ` AND a.path=` + compiler.bind(node.Path) + ` AND a.value_type=` + compiler.bind(string(node.Type)) + `)`
		return text, node.Type, nil
	case "literal":
		return compiler.literal(*node.Literal), node.Type, nil
	case "eq", "ne", "lt", "le", "gt", "ge":
		left, _, err := compiler.node(node.Left)
		if err != nil {
			return "", "", err
		}
		right, _, err := compiler.node(node.Right)
		if err != nil {
			return "", "", err
		}
		operator := map[string]string{"eq": "=", "ne": "<>", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}[node.Op]
		return "COALESCE((" + left + " " + operator + " " + right + "),FALSE)", BooleanType, nil
	case "and", "or":
		parts := make([]string, 0, len(node.Args))
		for _, child := range node.Args {
			part, _, err := compiler.node(child)
			if err != nil {
				return "", "", err
			}
			parts = append(parts, part)
		}
		join := " AND "
		if node.Op == "or" {
			join = " OR "
		}
		return "(" + strings.Join(parts, join) + ")", BooleanType, nil
	case "not":
		child, _, err := compiler.node(node.Arg)
		if err != nil {
			return "", "", err
		}
		return "(NOT (" + child + "))", BooleanType, nil
	case "in":
		value, _, err := compiler.node(node.Value)
		if err != nil {
			return "", "", err
		}
		items := make([]string, 0, len(node.Items))
		for _, item := range node.Items {
			items = append(items, compiler.literal(item))
		}
		return "COALESCE((" + value + " IN (" + strings.Join(items, ",") + ")),FALSE)", BooleanType, nil
	case "contains", "starts_with", "ends_with", "icontains", "matches":
		value, _, err := compiler.node(node.Value)
		if err != nil {
			return "", "", err
		}
		pattern := compiler.bind(node.Pattern)
		var expression string
		switch node.Op {
		case "contains":
			expression = "contains(" + value + "," + pattern + ")"
		case "starts_with":
			expression = "starts_with(" + value + "," + pattern + ")"
		case "ends_with":
			expression = "ends_with(" + value + "," + pattern + ")"
		case "icontains":
			expression = "contains(lower(" + value + "),lower(" + pattern + "))"
		case "matches":
			expression = "regexp_matches(" + value + "," + pattern + ")"
		}
		return "COALESCE((" + expression + "),FALSE)", BooleanType, nil
	case "text":
		return `EXISTS (SELECT 1 FROM UNNEST(r.search_values) AS s(value) WHERE contains(s.value,` + compiler.bind(node.Pattern) + `))`, BooleanType, nil
	case "exists":
		return compiler.attributeExists(node.Namespace, node.Path, ""), BooleanType, nil
	case "is_null":
		return compiler.attributeExists(node.Namespace, node.Path, "null"), BooleanType, nil
	case "array_contains":
		valueColumn := map[ScalarType]string{StringType: "string_value", IntegerType: "integer_value", DoubleType: "double_value", BooleanType: "boolean_value"}[node.Literal.Type]
		namespace := compiler.bind(node.Namespace)
		prefixMatch := compiler.bind(node.Path + "/")
		prefixLength := compiler.bind(node.Path + "/")
		kind := compiler.bind(string(node.Literal.Type))
		literal := compiler.literal(*node.Literal)
		return `EXISTS (SELECT 1 FROM UNNEST(r.attrs) AS u(a) WHERE a.namespace=` + namespace + ` AND starts_with(a.path,` + prefixMatch + `) AND regexp_matches(substr(a.path,length(` + prefixLength + `)+1),'^[0-9]+$') AND a.value_type=` + kind + ` AND COALESCE(a.` + valueColumn + `=` + literal + `,FALSE))`, BooleanType, nil
	default:
		return "", "", fmt.Errorf("unsupported filter op %q", node.Op)
	}
}

var fixedColumns = map[string]string{
	"kind": "kind", "level": "level", "severity_number": "severity_number", "message": "message",
	"message_template": "message_template", "service": "service", "environment": "environment", "release": "release",
	"logger": "logger", "sdk_name": "sdk_name", "sdk_version": "sdk_version", "platform": "platform",
	"server_name": "server_name", "trace_id": "trace_id", "span_id": "span_id", "source_event_id": "source_event_id", "issue_id": "issue_id",
}

func (compiler *sqlCompiler) literal(literal Literal) string {
	placeholder := compiler.bind(literal.BoundValue())
	if literal.Type == IntegerType {
		return "CAST(" + placeholder + " AS DECIMAL(38,0))"
	}
	return placeholder
}

func (compiler *sqlCompiler) attributeExists(namespace, path, valueType string) string {
	text := `EXISTS (SELECT 1 FROM UNNEST(r.attrs) AS u(a) WHERE a.namespace=` + compiler.bind(namespace) + ` AND a.path=` + compiler.bind(path)
	if valueType != "" {
		text += ` AND a.value_type=` + compiler.bind(valueType)
	}
	return text + `)`
}
