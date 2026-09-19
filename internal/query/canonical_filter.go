package query

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
)

// DecodeCanonicalFilter reconstructs the validated AST retained in a snapshot.
// Re-encoding must reproduce the exact bytes, which rejects aliases and trailing
// data before the tree can be compiled back to SQL.
func DecodeCanonicalFilter(encoded []byte) (*Node, error) {
	if len(encoded) == 0 || len(encoded) > 32768 {
		return nil, errors.New("invalid canonical filter")
	}
	reader := bytes.NewReader(encoded)
	root, err := decodeCanonicalNode(reader, 1)
	if err != nil || reader.Len() != 0 || Validate(root) != nil {
		return nil, errors.New("invalid canonical filter")
	}
	canonical, err := CanonicalFilter(root)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return nil, errors.New("noncanonical filter")
	}
	return root, nil
}

func decodeCanonicalNode(reader *bytes.Reader, depth int) (*Node, error) {
	if depth > MaxDepth {
		return nil, errors.New("filter exceeds depth")
	}
	op, err := readCanonicalText(reader, 32)
	if err != nil {
		return nil, err
	}
	node := &Node{Op: op}
	switch op {
	case "field":
		node.Name, err = readCanonicalText(reader, 64)
	case "attr":
		var scalar string
		scalar, err = readCanonicalText(reader, 16)
		node.Type = ScalarType(scalar)
		if err == nil {
			node.Namespace, err = readCanonicalText(reader, 64)
		}
		if err == nil {
			node.Path, err = readCanonicalText(reader, MaxPointerBytes)
		}
	case "literal":
		var literal Literal
		literal, err = decodeCanonicalLiteral(reader)
		node.Literal, node.Type = &literal, literal.Type
	case "eq", "ne", "lt", "le", "gt", "ge":
		node.Left, err = decodeCanonicalNode(reader, depth+1)
		if err == nil {
			node.Right, err = decodeCanonicalNode(reader, depth+1)
		}
	case "and", "or":
		var count uint64
		count, err = binary.ReadUvarint(reader)
		if err == nil && (count < 2 || count > 16) {
			err = errors.New("invalid Boolean arity")
		}
		if err == nil {
			node.Args = make([]*Node, int(count))
			for index := range node.Args {
				node.Args[index], err = decodeCanonicalNode(reader, depth+1)
				if err != nil {
					break
				}
			}
		}
	case "not":
		node.Arg, err = decodeCanonicalNode(reader, depth+1)
	case "in":
		node.Value, err = decodeCanonicalNode(reader, depth+1)
		var count uint64
		if err == nil {
			count, err = binary.ReadUvarint(reader)
		}
		if err == nil && (count < 1 || count > MaxListItems) {
			err = errors.New("invalid set size")
		}
		if err == nil {
			node.Items = make([]Literal, int(count))
			for index := range node.Items {
				node.Items[index], err = decodeCanonicalLiteral(reader)
				if err != nil {
					break
				}
			}
		}
	case "contains", "starts_with", "ends_with", "icontains", "matches":
		node.Value, err = decodeCanonicalNode(reader, depth+1)
		if err == nil {
			node.Pattern, err = readCanonicalText(reader, MaxPatternBytes)
		}
	case "text":
		node.Pattern, err = readCanonicalText(reader, MaxPatternBytes)
	case "exists", "is_null":
		node.Namespace, err = readCanonicalText(reader, 64)
		if err == nil {
			node.Path, err = readCanonicalText(reader, MaxPointerBytes)
		}
	case "array_contains":
		node.Namespace, err = readCanonicalText(reader, 64)
		if err == nil {
			node.Path, err = readCanonicalText(reader, MaxPointerBytes)
		}
		if err == nil {
			var literal Literal
			literal, err = decodeCanonicalLiteral(reader)
			node.Literal = &literal
		}
	case "constant":
		value, readErr := reader.ReadByte()
		if readErr != nil || value > 1 {
			err = errors.New("invalid constant")
		} else {
			node.Constant = value == 1
		}
	default:
		err = errors.New("unsupported canonical filter node")
	}
	return node, err
}

func decodeCanonicalLiteral(reader *bytes.Reader) (Literal, error) {
	typeName, err := readCanonicalText(reader, 16)
	literal := Literal{Type: ScalarType(typeName)}
	if err != nil {
		return literal, err
	}
	switch literal.Type {
	case StringType:
		literal.String, err = readCanonicalText(reader, MaxExpressionBytes)
	case IntegerType:
		literal.Integer, err = readCanonicalText(reader, 128)
	case DoubleType:
		bits, readErr := binary.ReadUvarint(reader)
		literal.Double, err = math.Float64frombits(bits), readErr
	case BooleanType:
		value, readErr := reader.ReadByte()
		if readErr != nil || value > 1 {
			err = errors.New("invalid Boolean literal")
		} else {
			literal.Boolean = value == 1
		}
	default:
		err = errors.New("invalid literal type")
	}
	return literal, err
}

func readCanonicalText(reader *bytes.Reader, maximum int) (string, error) {
	value, err := readDatasetBytes(reader, uint64(maximum))
	if err != nil {
		return "", err
	}
	return string(value), nil
}
