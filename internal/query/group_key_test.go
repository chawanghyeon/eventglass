package query

import (
	"bytes"
	"math"
	"testing"
)

func TestAggregateGroupKeySeparatesTypesNullAndMissing(t *testing.T) {
	dimension := []GroupDimension{{Op: "group_attr", Namespace: "attributes", Path: "/value"}}
	stringOne, integerOne, doubleOne, truth := "1", "1", 1.0, true
	values := []AggregateGroupValue{
		{Type: GroupMissing},
		{Type: GroupNull},
		{Type: string(StringType), String: &stringOne},
		{Type: string(IntegerType), Integer: &integerOne},
		{Type: string(DoubleType), Double: &doubleOne},
		{Type: string(BooleanType), Boolean: &truth},
	}
	seen := map[string]bool{}
	for _, value := range values {
		key, err := EncodeAggregateGroupKey(dimension, []AggregateGroupValue{value}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if seen[string(key)] {
			t.Fatalf("group values collided at %#v", value)
		}
		seen[string(key)] = true
	}
}

func TestAggregateGroupKeyNormalizesNegativeZeroAndRejectsUnsupported(t *testing.T) {
	dimension := []GroupDimension{{Op: "attr", Namespace: "attributes", Path: "/value", Type: DoubleType}}
	positive, negative := 0.0, math.Copysign(0, -1)
	positiveKey, err := EncodeAggregateGroupKey(dimension, []AggregateGroupValue{{Type: string(DoubleType), Double: &positive}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	negativeKey, err := EncodeAggregateGroupKey(dimension, []AggregateGroupValue{{Type: string(DoubleType), Double: &negative}}, nil)
	if err != nil || !bytes.Equal(positiveKey, negativeKey) {
		t.Fatalf("negative zero key differs: %x %x err=%v", positiveKey, negativeKey, err)
	}
	if _, err := EncodeAggregateGroupKey(dimension, []AggregateGroupValue{{Type: "json"}}, nil); err == nil {
		t.Fatal("unsupported group type accepted")
	}
	invalidInteger := "01"
	if _, err := EncodeAggregateGroupKey(dimension, []AggregateGroupValue{{Type: string(IntegerType), Integer: &invalidInteger}}, nil); err == nil {
		t.Fatal("noncanonical integer group accepted")
	}
}
