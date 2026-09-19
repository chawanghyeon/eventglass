package query

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"

	"cel.dev/cel-go/cel"
	celast "cel.dev/cel-go/common/ast"
	"cel.dev/cel-go/common/types"
)

var (
	filterEnvOnce sync.Once
	filterEnv     *cel.Env
	filterEnvErr  error
)

func ParseCEL(expression string) (*Node, error) {
	if len(expression) > MaxExpressionBytes {
		return nil, errors.New("expression exceeds size limit")
	}
	if expression == "" {
		return &Node{Op: "constant", Constant: true}, nil
	}
	filterEnvOnce.Do(buildFilterEnv)
	if filterEnvErr != nil {
		return nil, filterEnvErr
	}
	checked, issues := filterEnv.Compile(expression)
	if issues.Err() != nil {
		return nil, errors.New("invalid filter expression")
	}
	node, err := lowerCEL(checked.NativeRep().Expr())
	if err != nil {
		return nil, err
	}
	node = promoteBooleanLiteral(node)
	return node, Validate(node)
}

func buildFilterEnv() {
	options := []cel.EnvOption{cel.ClearMacros(), cel.ParserExpressionSizeLimit(MaxExpressionBytes), cel.ParserRecursionLimit(MaxDepth * 4)}
	for name, kind := range fieldTypes {
		options = append(options, cel.Variable(name, celType(kind)))
	}
	functions := []cel.EnvOption{
		cel.Function("sattr", cel.Overload("sattr_string_string", []*cel.Type{cel.StringType, cel.StringType}, cel.StringType)),
		cel.Function("iattr", cel.Overload("iattr_string_string", []*cel.Type{cel.StringType, cel.StringType}, cel.IntType)),
		cel.Function("dattr", cel.Overload("dattr_string_string", []*cel.Type{cel.StringType, cel.StringType}, cel.DoubleType)),
		cel.Function("battr", cel.Overload("battr_string_string", []*cel.Type{cel.StringType, cel.StringType}, cel.BoolType)),
		cel.Function("exists", cel.Overload("exists_string_string", []*cel.Type{cel.StringType, cel.StringType}, cel.BoolType)),
		cel.Function("is_null", cel.Overload("is_null_string_string", []*cel.Type{cel.StringType, cel.StringType}, cel.BoolType)),
		cel.Function("contains", cel.Overload("contains_string_string", []*cel.Type{cel.StringType, cel.StringType}, cel.BoolType)),
		cel.Function("starts_with", cel.Overload("starts_with_string_string", []*cel.Type{cel.StringType, cel.StringType}, cel.BoolType)),
		cel.Function("ends_with", cel.Overload("ends_with_string_string", []*cel.Type{cel.StringType, cel.StringType}, cel.BoolType)),
		cel.Function("icontains", cel.Overload("icontains_string_string", []*cel.Type{cel.StringType, cel.StringType}, cel.BoolType)),
		cel.Function("text", cel.Overload("text_string", []*cel.Type{cel.StringType}, cel.BoolType)),
		cel.Function("array_contains",
			cel.Overload("array_contains_string", []*cel.Type{cel.StringType, cel.StringType, cel.StringType}, cel.BoolType),
			cel.Overload("array_contains_int", []*cel.Type{cel.StringType, cel.StringType, cel.IntType}, cel.BoolType),
			cel.Overload("array_contains_double", []*cel.Type{cel.StringType, cel.StringType, cel.DoubleType}, cel.BoolType),
			cel.Overload("array_contains_bool", []*cel.Type{cel.StringType, cel.StringType, cel.BoolType}, cel.BoolType)),
	}
	options = append(options, functions...)
	filterEnv, filterEnvErr = cel.NewEnv(options...)
}

func celType(kind ScalarType) *cel.Type {
	switch kind {
	case IntegerType:
		return cel.IntType
	case DoubleType:
		return cel.DoubleType
	case BooleanType:
		return cel.BoolType
	default:
		return cel.StringType
	}
}

func lowerCEL(expression celast.Expr) (*Node, error) {
	switch expression.Kind() {
	case celast.IdentKind:
		name := expression.AsIdent()
		if _, ok := fieldTypes[name]; !ok {
			return nil, errors.New("unsupported identifier")
		}
		return &Node{Op: "field", Name: name}, nil
	case celast.LiteralKind:
		literal, err := celLiteral(expression.AsLiteral().Value())
		if err != nil {
			return nil, err
		}
		return &Node{Op: "literal", Type: literal.Type, Literal: &literal}, nil
	case celast.CallKind:
		call := expression.AsCall()
		if call.IsMemberFunction() {
			return nil, errors.New("member calls are not allowed")
		}
		name, arguments := call.FunctionName(), call.Args()
		switch name {
		case "_&&_", "_||_":
			op := "and"
			if name == "_||_" {
				op = "or"
			}
			result := &Node{Op: op}
			for _, argument := range arguments {
				child, err := lowerCEL(argument)
				if err != nil {
					return nil, err
				}
				child = promoteBooleanLiteral(child)
				if child.Op == op {
					result.Args = append(result.Args, child.Args...)
				} else {
					result.Args = append(result.Args, child)
				}
			}
			return result, nil
		case "!_":
			if len(arguments) != 1 {
				return nil, errors.New("invalid negation")
			}
			child, err := lowerCEL(arguments[0])
			child = promoteBooleanLiteral(child)
			return &Node{Op: "not", Arg: child}, err
		case "_==_", "_!=_", "_<_", "_<=_", "_>_", "_>=_":
			if len(arguments) != 2 {
				return nil, errors.New("invalid comparison")
			}
			left, err := lowerCEL(arguments[0])
			if err != nil {
				return nil, err
			}
			right, err := lowerCEL(arguments[1])
			if err != nil {
				return nil, err
			}
			op := map[string]string{"_==_": "eq", "_!=_": "ne", "_<_": "lt", "_<=_": "le", "_>_": "gt", "_>=_": "ge"}[name]
			return &Node{Op: op, Left: left, Right: right}, nil
		case "@in", "_in_":
			if len(arguments) != 2 || arguments[1].Kind() != celast.ListKind {
				return nil, errors.New("invalid in expression")
			}
			value, err := lowerCEL(arguments[0])
			if err != nil {
				return nil, err
			}
			result := &Node{Op: "in", Value: value}
			for _, element := range arguments[1].AsList().Elements() {
				item, err := lowerCEL(element)
				if err != nil || item.Op != "literal" {
					return nil, errors.New("in items must be literals")
				}
				result.Items = append(result.Items, *item.Literal)
			}
			return result, nil
		case "sattr", "iattr", "dattr", "battr":
			if len(arguments) != 2 {
				return nil, errors.New("invalid attribute accessor")
			}
			namespace, err := literalString(arguments[0])
			if err != nil {
				return nil, err
			}
			path, err := literalString(arguments[1])
			if err != nil {
				return nil, err
			}
			kind := map[string]ScalarType{"sattr": StringType, "iattr": IntegerType, "dattr": DoubleType, "battr": BooleanType}[name]
			return &Node{Op: "attr", Type: kind, Namespace: namespace, Path: path}, nil
		case "exists", "is_null":
			if len(arguments) != 2 {
				return nil, errors.New("invalid presence function")
			}
			namespace, err := literalString(arguments[0])
			if err != nil {
				return nil, err
			}
			path, err := literalString(arguments[1])
			if err != nil {
				return nil, err
			}
			return &Node{Op: name, Namespace: namespace, Path: path}, nil
		case "contains", "starts_with", "ends_with", "icontains", "matches":
			if len(arguments) != 2 {
				return nil, errors.New("invalid string function")
			}
			value, err := lowerCEL(arguments[0])
			if err != nil {
				return nil, err
			}
			pattern, err := literalString(arguments[1])
			if err != nil {
				return nil, err
			}
			return &Node{Op: name, Value: value, Pattern: pattern}, nil
		case "text":
			if len(arguments) != 1 {
				return nil, errors.New("invalid text function")
			}
			value, err := literalString(arguments[0])
			if err != nil {
				return nil, err
			}
			return &Node{Op: "text", Pattern: value}, nil
		case "array_contains":
			if len(arguments) != 3 {
				return nil, errors.New("invalid array function")
			}
			namespace, err := literalString(arguments[0])
			if err != nil {
				return nil, err
			}
			path, err := literalString(arguments[1])
			if err != nil {
				return nil, err
			}
			value, err := lowerCEL(arguments[2])
			if err != nil || value.Op != "literal" {
				return nil, errors.New("array value must be literal")
			}
			return &Node{Op: "array_contains", Namespace: namespace, Path: path, Literal: value.Literal}, nil
		default:
			return nil, fmt.Errorf("unsupported CEL call %q", name)
		}
	default:
		return nil, errors.New("unsupported CEL expression form")
	}
}

func promoteBooleanLiteral(node *Node) *Node {
	if node != nil && node.Op == "literal" && node.Literal != nil && node.Type == BooleanType {
		return &Node{Op: "constant", Constant: node.Literal.Boolean}
	}
	return node
}

func celLiteral(value any) (Literal, error) {
	switch typed := value.(type) {
	case string:
		return Literal{Type: StringType, String: typed}, nil
	case int64:
		return Literal{Type: IntegerType, Integer: strconv.FormatInt(typed, 10)}, nil
	case float64:
		if math.IsInf(typed, 0) || math.IsNaN(typed) {
			break
		}
		return Literal{Type: DoubleType, Double: typed}, nil
	case bool:
		return Literal{Type: BooleanType, Boolean: typed}, nil
	case types.String:
		return Literal{Type: StringType, String: string(typed)}, nil
	case types.Int:
		return Literal{Type: IntegerType, Integer: strconv.FormatInt(int64(typed), 10)}, nil
	case types.Double:
		return Literal{Type: DoubleType, Double: float64(typed)}, nil
	case types.Bool:
		return Literal{Type: BooleanType, Boolean: bool(typed)}, nil
	}
	return Literal{}, errors.New("unsupported CEL literal")
}

func literalString(expression celast.Expr) (string, error) {
	if expression.Kind() != celast.LiteralKind {
		return "", errors.New("argument must be a literal string")
	}
	literal, err := celLiteral(expression.AsLiteral().Value())
	if err != nil || literal.Type != StringType {
		return "", errors.New("argument must be a literal string")
	}
	return literal.String, nil
}
