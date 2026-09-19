package query

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"sort"

	"github.com/chawanghyeon/eventglass/internal/model"
)

const datasetEncodingVersion = 1

func CanonicalFilter(root *Node) ([]byte, error) {
	if err := Validate(root); err != nil {
		return nil, err
	}
	var buffer bytes.Buffer
	encodeNode(&buffer, root)
	return buffer.Bytes(), nil
}

func DatasetHash(spec model.DatasetSpec) (string, []byte, error) {
	if spec.TenantID <= 0 || spec.StartUS >= spec.EndUS || (spec.TimeBasis != model.QueryTimeEvent && spec.TimeBasis != model.QueryTimeReceived) || len(spec.ProjectIDs) < 1 || len(spec.ProjectIDs) > 100 {
		return "", nil, errors.New("invalid dataset")
	}
	projects := append([]int64(nil), spec.ProjectIDs...)
	sort.Slice(projects, func(i, j int) bool { return projects[i] < projects[j] })
	for index, project := range projects {
		if project <= 0 || index > 0 && projects[index-1] == project {
			return "", nil, errors.New("invalid project scope")
		}
	}
	kinds := append([]model.Kind(nil), spec.Kinds...)
	if len(kinds) == 0 {
		kinds = []model.Kind{model.KindError, model.KindLog, model.KindTransaction}
	}
	sort.Slice(kinds, func(i, j int) bool { return kinds[i] < kinds[j] })
	for index, kind := range kinds {
		if kind != model.KindError && kind != model.KindLog && kind != model.KindTransaction || index > 0 && kinds[index-1] == kind {
			return "", nil, errors.New("invalid kind scope")
		}
	}
	if len(spec.Filter) == 0 || len(spec.Filter) > 32768 {
		return "", nil, errors.New("invalid canonical filter")
	}
	var buffer bytes.Buffer
	buffer.WriteString("eventglass-dataset")
	writeUint(&buffer, datasetEncodingVersion)
	writeInt(&buffer, spec.TenantID)
	writeUint(&buffer, uint64(len(projects)))
	for _, project := range projects {
		writeInt(&buffer, project)
	}
	writeUint(&buffer, uint64(len(kinds)))
	for _, kind := range kinds {
		writeBytes(&buffer, []byte(kind))
	}
	writeBytes(&buffer, []byte(spec.TimeBasis))
	writeInt(&buffer, spec.StartUS)
	writeInt(&buffer, spec.EndUS)
	writeBytes(&buffer, spec.Filter)
	encoded := buffer.Bytes()
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), append([]byte(nil), encoded...), nil
}

func encodeNode(buffer *bytes.Buffer, node *Node) {
	writeBytes(buffer, []byte(node.Op))
	switch node.Op {
	case "field":
		writeBytes(buffer, []byte(node.Name))
	case "attr":
		writeBytes(buffer, []byte(node.Type))
		writeBytes(buffer, []byte(node.Namespace))
		writeBytes(buffer, []byte(node.Path))
	case "literal":
		encodeLiteral(buffer, *node.Literal)
	case "eq", "ne", "lt", "le", "gt", "ge":
		encodeNode(buffer, node.Left)
		encodeNode(buffer, node.Right)
	case "and", "or":
		writeUint(buffer, uint64(len(node.Args)))
		for _, child := range node.Args {
			encodeNode(buffer, child)
		}
	case "not":
		encodeNode(buffer, node.Arg)
	case "in":
		encodeNode(buffer, node.Value)
		writeUint(buffer, uint64(len(node.Items)))
		for _, item := range node.Items {
			encodeLiteral(buffer, item)
		}
	case "contains", "starts_with", "ends_with", "icontains", "matches":
		encodeNode(buffer, node.Value)
		writeBytes(buffer, []byte(node.Pattern))
	case "text":
		writeBytes(buffer, []byte(node.Pattern))
	case "exists", "is_null":
		writeBytes(buffer, []byte(node.Namespace))
		writeBytes(buffer, []byte(node.Path))
	case "array_contains":
		writeBytes(buffer, []byte(node.Namespace))
		writeBytes(buffer, []byte(node.Path))
		encodeLiteral(buffer, *node.Literal)
	case "constant":
		if node.Constant {
			buffer.WriteByte(1)
		} else {
			buffer.WriteByte(0)
		}
	}
}

func encodeLiteral(buffer *bytes.Buffer, literal Literal) {
	writeBytes(buffer, []byte(literal.Type))
	switch literal.Type {
	case StringType:
		writeBytes(buffer, []byte(literal.String))
	case IntegerType:
		writeBytes(buffer, []byte(normalizeInteger(literal.Integer)))
	case DoubleType:
		writeUint(buffer, math.Float64bits(literal.Double))
	case BooleanType:
		if literal.Boolean {
			buffer.WriteByte(1)
		} else {
			buffer.WriteByte(0)
		}
	}
}

func writeUint(buffer *bytes.Buffer, value uint64) {
	var data [10]byte
	size := binary.PutUvarint(data[:], value)
	buffer.Write(data[:size])
}
func writeInt(buffer *bytes.Buffer, value int64) {
	var data [10]byte
	size := binary.PutVarint(data[:], value)
	buffer.Write(data[:size])
}
func writeBytes(buffer *bytes.Buffer, value []byte) {
	writeUint(buffer, uint64(len(value)))
	buffer.Write(value)
}
