package query

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
)

func DecodeJSON(data []byte) (*Node, error) {
	if len(data) == 0 || len(data) > MaxExpressionBytes {
		return nil, errors.New("filter JSON size is invalid")
	}
	if err := rejectDuplicateKeys(data); err != nil {
		return nil, err
	}
	var raw json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&raw); err != nil {
		return nil, errors.New("invalid filter JSON")
	}
	var trailing any
	if decoder.Decode(&trailing) == nil {
		return nil, errors.New("trailing filter JSON")
	}
	node, err := decodeNode(raw)
	if err != nil {
		return nil, err
	}
	return node, Validate(node)
}

func decodeNode(data json.RawMessage) (*Node, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil || object == nil {
		return nil, errors.New("filter node must be an object")
	}
	var op string
	if err := json.Unmarshal(object["op"], &op); err != nil || op == "" {
		return nil, errors.New("filter node requires op")
	}
	node := &Node{Op: op}
	allowed := map[string]bool{"op": true}
	decodeString := func(name string, target *string) error {
		allowed[name] = true
		if err := json.Unmarshal(object[name], target); err != nil {
			return fmt.Errorf("%s must be a string", name)
		}
		return nil
	}
	decodeType := func() error {
		var value string
		if err := decodeString("type", &value); err != nil {
			return err
		}
		node.Type = ScalarType(value)
		return nil
	}
	switch op {
	case "field":
		if err := decodeString("name", &node.Name); err != nil {
			return nil, err
		}
	case "attr":
		if err := decodeType(); err != nil {
			return nil, err
		}
		if err := decodeString("namespace", &node.Namespace); err != nil {
			return nil, err
		}
		if err := decodeString("path", &node.Path); err != nil {
			return nil, err
		}
	case "literal":
		if err := decodeType(); err != nil {
			return nil, err
		}
		allowed["value"] = true
		literal, err := decodeLiteral(node.Type, object["value"])
		if err != nil {
			return nil, err
		}
		node.Literal = &literal
	case "eq", "ne", "lt", "le", "gt", "ge":
		allowed["left"], allowed["right"] = true, true
		left, err := decodeNode(object["left"])
		if err != nil {
			return nil, err
		}
		right, err := decodeNode(object["right"])
		if err != nil {
			return nil, err
		}
		node.Left, node.Right = left, right
	case "and", "or":
		allowed["args"] = true
		var values []json.RawMessage
		if err := json.Unmarshal(object["args"], &values); err != nil {
			return nil, errors.New("args must be an array")
		}
		for _, value := range values {
			child, err := decodeNode(value)
			if err != nil {
				return nil, err
			}
			node.Args = append(node.Args, child)
		}
	case "not":
		allowed["arg"] = true
		child, err := decodeNode(object["arg"])
		if err != nil {
			return nil, err
		}
		node.Arg = child
	case "in":
		allowed["value"], allowed["items"] = true, true
		value, err := decodeNode(object["value"])
		if err != nil {
			return nil, err
		}
		node.Value = value
		var values []json.RawMessage
		if err := json.Unmarshal(object["items"], &values); err != nil {
			return nil, errors.New("items must be an array")
		}
		for _, value := range values {
			item, err := decodeNode(value)
			if err != nil || item.Op != "literal" {
				return nil, errors.New("items must contain literals")
			}
			node.Items = append(node.Items, *item.Literal)
		}
	case "contains", "starts_with", "ends_with", "icontains", "matches":
		allowed["value"] = true
		value, err := decodeNode(object["value"])
		if err != nil {
			return nil, err
		}
		node.Value = value
		if err := decodeString("pattern", &node.Pattern); err != nil {
			return nil, err
		}
	case "text":
		if err := decodeString("value", &node.Pattern); err != nil {
			return nil, err
		}
	case "exists", "is_null":
		if err := decodeString("namespace", &node.Namespace); err != nil {
			return nil, err
		}
		if err := decodeString("path", &node.Path); err != nil {
			return nil, err
		}
	case "array_contains":
		if err := decodeString("namespace", &node.Namespace); err != nil {
			return nil, err
		}
		if err := decodeString("path", &node.Path); err != nil {
			return nil, err
		}
		allowed["value"] = true
		value, err := decodeNode(object["value"])
		if err != nil || value.Op != "literal" {
			return nil, errors.New("array_contains value must be a literal")
		}
		node.Literal = value.Literal
	case "constant":
		allowed["value"] = true
		if err := json.Unmarshal(object["value"], &node.Constant); err != nil {
			return nil, errors.New("constant value must be boolean")
		}
	default:
		return nil, fmt.Errorf("unsupported filter op %q", op)
	}
	for key := range object {
		if !allowed[key] {
			return nil, fmt.Errorf("unknown field %q for %s", key, op)
		}
	}
	return node, nil
}

func decodeLiteral(kind ScalarType, raw json.RawMessage) (Literal, error) {
	result := Literal{Type: kind}
	switch kind {
	case StringType:
		if err := json.Unmarshal(raw, &result.String); err != nil {
			return result, errors.New("string literal must be string")
		}
	case IntegerType:
		if err := json.Unmarshal(raw, &result.Integer); err != nil || !validDecimal38(result.Integer) {
			return result, errors.New("integer literal exceeds signed decimal(38,0)")
		}
	case DoubleType:
		if err := json.Unmarshal(raw, &result.Double); err != nil || math.IsInf(result.Double, 0) || math.IsNaN(result.Double) {
			return result, errors.New("double literal must be finite")
		}
	case BooleanType:
		if err := json.Unmarshal(raw, &result.Boolean); err != nil {
			return result, errors.New("boolean literal must be boolean")
		}
	default:
		return result, errors.New("unsupported literal type")
	}
	return result, nil
}

func rejectDuplicateKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var parse func() error
	parse = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delimiter {
		case '{':
			seen := map[string]bool{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key := keyToken.(string)
				if seen[key] {
					return errors.New("duplicate JSON field")
				}
				seen[key] = true
				if err := parse(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		case '[':
			for decoder.More() {
				if err := parse(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		default:
			return errors.New("invalid JSON delimiter")
		}
	}
	return parse()
}
