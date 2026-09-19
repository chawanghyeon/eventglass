package query

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
)

const (
	GroupMissing = "missing"
	GroupNull    = "null"
)

type AggregateGroupValue struct {
	Type    string
	String  *string
	Integer *string
	Double  *float64
	Boolean *bool
}

// EncodeAggregateGroupKey produces the exact byte sequence used for final
// tie-breaking. Dimension identity is included so keys remain unambiguous when
// retained outside the operation that created them.
func EncodeAggregateGroupKey(dimensions []GroupDimension, values []AggregateGroupValue, bucketStartUS *int64) ([]byte, error) {
	if len(dimensions) != len(values) || len(dimensions) > 2 {
		return nil, errors.New("aggregate group shape mismatch")
	}
	var buffer bytes.Buffer
	buffer.WriteByte(1)
	if bucketStartUS != nil {
		buffer.WriteByte('h')
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], uint64(*bucketStartUS)^(uint64(1)<<63))
		buffer.Write(encoded[:])
	}
	for index, dimension := range dimensions {
		buffer.WriteByte('g')
		writeGroupKeyBytes(&buffer, []byte(dimension.Op))
		writeGroupKeyBytes(&buffer, []byte(dimension.Name))
		writeGroupKeyBytes(&buffer, []byte(dimension.Namespace))
		writeGroupKeyBytes(&buffer, []byte(dimension.Path))
		if err := writeAggregateGroupValue(&buffer, values[index]); err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}

func writeAggregateGroupValue(buffer *bytes.Buffer, value AggregateGroupValue) error {
	nonNil := 0
	for _, present := range []bool{value.String != nil, value.Integer != nil, value.Double != nil, value.Boolean != nil} {
		if present {
			nonNil++
		}
	}
	switch value.Type {
	case GroupMissing:
		if nonNil != 0 {
			return errors.New("missing group has a value")
		}
		buffer.WriteByte('m')
	case GroupNull:
		if nonNil != 0 {
			return errors.New("null group has a value")
		}
		buffer.WriteByte('n')
	case string(StringType):
		if nonNil != 1 || value.String == nil {
			return errors.New("invalid string group")
		}
		buffer.WriteByte('s')
		writeGroupKeyBytes(buffer, []byte(*value.String))
	case string(IntegerType):
		if nonNil != 1 || value.Integer == nil || normalizeInteger(*value.Integer) != *value.Integer {
			return errors.New("invalid integer group")
		}
		if _, err := DecimalLimbs(*value.Integer); err != nil {
			return errors.New("invalid integer group")
		}
		buffer.WriteByte('i')
		writeGroupKeyBytes(buffer, []byte(*value.Integer))
	case string(DoubleType):
		if nonNil != 1 || value.Double == nil || math.IsNaN(*value.Double) || math.IsInf(*value.Double, 0) {
			return errors.New("invalid double group")
		}
		buffer.WriteByte('d')
		bits := math.Float64bits(*value.Double)
		if *value.Double == 0 {
			bits = 0
		}
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], bits)
		buffer.Write(encoded[:])
	case string(BooleanType):
		if nonNil != 1 || value.Boolean == nil {
			return errors.New("invalid boolean group")
		}
		buffer.WriteByte('b')
		if *value.Boolean {
			buffer.WriteByte(1)
		} else {
			buffer.WriteByte(0)
		}
	default:
		return errors.New("unsupported aggregate group type")
	}
	return nil
}

func writeGroupKeyBytes(buffer *bytes.Buffer, value []byte) {
	var encoded [binary.MaxVarintLen64]byte
	size := binary.PutUvarint(encoded[:], uint64(len(value)))
	buffer.Write(encoded[:size])
	buffer.Write(value)
}
