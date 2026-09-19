package query

import (
	"errors"
	"math"
	"regexp"
	"strings"
)

const (
	MaxExpressionBytes = 8 << 10
	MaxNodes           = 128
	MaxDepth           = 16
	MaxListItems       = 100
	MaxRegex           = 4
	MaxPatternBytes    = 1 << 10
	MaxPointerBytes    = 1 << 10
)

type ScalarType string

const (
	StringType  ScalarType = "string"
	IntegerType ScalarType = "integer"
	DoubleType  ScalarType = "double"
	BooleanType ScalarType = "boolean"
)

type Literal struct {
	Type    ScalarType
	String  string
	Integer string
	Double  float64
	Boolean bool
}

type Node struct {
	Op        string
	Name      string
	Type      ScalarType
	Namespace string
	Path      string
	Literal   *Literal
	Left      *Node
	Right     *Node
	Value     *Node
	Arg       *Node
	Args      []*Node
	Items     []Literal
	Pattern   string
	Constant  bool
}

var fieldTypes = map[string]ScalarType{
	"kind": StringType, "level": StringType, "severity_number": IntegerType, "message": StringType,
	"message_template": StringType, "service": StringType, "environment": StringType, "release": StringType,
	"logger": StringType, "sdk_name": StringType, "sdk_version": StringType, "platform": StringType,
	"server_name": StringType, "trace_id": StringType, "span_id": StringType, "source_event_id": StringType,
	"issue_id": StringType,
}

var namespaces = map[string]bool{"attributes": true, "tags": true, "extra": true, "contexts": true, "user": true, "request": true, "sdk": true}

type validationState struct{ nodes, regex int }

func Validate(root *Node) error {
	state := &validationState{}
	kind, err := validateNode(root, 1, state)
	if err != nil {
		return err
	}
	if kind != BooleanType || !isPredicate(root) {
		return errors.New("filter root is not a predicate")
	}
	if state.nodes > MaxNodes {
		return errors.New("filter exceeds node limit")
	}
	if state.regex > MaxRegex {
		return errors.New("filter exceeds regex limit")
	}
	return nil
}

func validateNode(node *Node, depth int, state *validationState) (ScalarType, error) {
	if node == nil || depth > MaxDepth {
		return "", errors.New("filter exceeds depth limit")
	}
	state.nodes++
	switch node.Op {
	case "field":
		kind, ok := fieldTypes[node.Name]
		if !ok {
			return "", errors.New("unsupported fixed field")
		}
		return kind, nil
	case "attr":
		if !namespaces[node.Namespace] || !validPointer(node.Path) || !validScalarType(node.Type) {
			return "", errors.New("invalid typed attribute")
		}
		return node.Type, nil
	case "literal":
		if node.Literal == nil || node.Literal.Type != node.Type || !validLiteral(*node.Literal) {
			return "", errors.New("invalid literal")
		}
		return node.Type, nil
	case "eq", "ne", "lt", "le", "gt", "ge":
		left, err := validateNode(node.Left, depth+1, state)
		if err != nil {
			return "", err
		}
		right, err := validateNode(node.Right, depth+1, state)
		if err != nil {
			return "", err
		}
		if (node.Left.Op != "field" && node.Left.Op != "attr") || node.Right.Op != "literal" || left != right || ((node.Op != "eq" && node.Op != "ne") && left == BooleanType) {
			return "", errors.New("invalid comparison types")
		}
		return BooleanType, nil
	case "and", "or":
		if len(node.Args) < 2 || len(node.Args) > 16 {
			return "", errors.New("boolean argument count is invalid")
		}
		for _, child := range node.Args {
			kind, err := validateNode(child, depth+1, state)
			if err != nil || kind != BooleanType || !isPredicate(child) {
				return "", errors.New("boolean argument is not a predicate")
			}
		}
		return BooleanType, nil
	case "not":
		kind, err := validateNode(node.Arg, depth+1, state)
		if err != nil || kind != BooleanType || !isPredicate(node.Arg) {
			return "", errors.New("not argument is not a predicate")
		}
		return BooleanType, nil
	case "in":
		kind, err := validateNode(node.Value, depth+1, state)
		if err != nil || (node.Value.Op != "field" && node.Value.Op != "attr") || len(node.Items) < 1 || len(node.Items) > MaxListItems {
			return "", errors.New("invalid set predicate")
		}
		for _, item := range node.Items {
			state.nodes++
			if item.Type != kind || !validLiteral(item) {
				return "", errors.New("set literal types differ")
			}
		}
		return BooleanType, nil
	case "contains", "starts_with", "ends_with", "icontains", "matches":
		kind, err := validateNode(node.Value, depth+1, state)
		if err != nil || kind != StringType || (node.Value.Op != "field" && node.Value.Op != "attr") || len(node.Pattern) > MaxPatternBytes {
			return "", errors.New("invalid string predicate")
		}
		if node.Op == "matches" {
			state.regex++
			if _, err := regexp.Compile(node.Pattern); err != nil {
				return "", errors.New("invalid RE2 pattern")
			}
		}
		return BooleanType, nil
	case "text":
		if len(node.Pattern) > MaxPatternBytes {
			return "", errors.New("text pattern is too large")
		}
		return BooleanType, nil
	case "exists", "is_null":
		if !namespaces[node.Namespace] || !validPointer(node.Path) {
			return "", errors.New("invalid presence predicate")
		}
		return BooleanType, nil
	case "array_contains":
		if !namespaces[node.Namespace] || !validPointer(node.Path) || node.Literal == nil || !validScalarType(node.Literal.Type) || !validLiteral(*node.Literal) {
			return "", errors.New("invalid array predicate")
		}
		state.nodes++
		return BooleanType, nil
	case "constant":
		return BooleanType, nil
	default:
		return "", errors.New("unsupported filter node")
	}
}

func isPredicate(node *Node) bool {
	switch node.Op {
	case "eq", "ne", "lt", "le", "gt", "ge", "and", "or", "not", "in", "contains", "starts_with", "ends_with", "icontains", "matches", "text", "exists", "is_null", "array_contains", "constant":
		return true
	default:
		return false
	}
}

func validLiteral(literal Literal) bool {
	switch literal.Type {
	case StringType, BooleanType:
		return true
	case IntegerType:
		return validDecimal38(literal.Integer)
	case DoubleType:
		return !math.IsInf(literal.Double, 0) && !math.IsNaN(literal.Double)
	default:
		return false
	}
}

func validDecimal38(value string) bool {
	digits := value
	if strings.HasPrefix(digits, "-") {
		digits = digits[1:]
	}
	if digits == "" {
		return false
	}
	for _, digit := range digits {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	digits = strings.TrimLeft(digits, "0")
	return len(digits) <= 38
}

func validScalarType(kind ScalarType) bool {
	return kind == StringType || kind == IntegerType || kind == DoubleType || kind == BooleanType
}

func validPointer(path string) bool {
	if len(path) == 0 || len(path) > MaxPointerBytes || path[0] != '/' {
		return false
	}
	for index := 0; index < len(path); index++ {
		if path[index] == '~' && (index+1 >= len(path) || (path[index+1] != '0' && path[index+1] != '1')) {
			return false
		} else if path[index] == '~' {
			index++
		}
	}
	return true
}

func (literal Literal) BoundValue() any {
	switch literal.Type {
	case StringType:
		return literal.String
	case IntegerType:
		return literal.Integer
	case DoubleType:
		return literal.Double
	case BooleanType:
		return literal.Boolean
	}
	return nil
}

func normalizeInteger(value string) string {
	negative := strings.HasPrefix(value, "-")
	digits := strings.TrimLeft(strings.TrimPrefix(value, "-"), "0")
	if digits == "" {
		return "0"
	}
	if negative {
		return "-" + digits
	}
	return digits
}
