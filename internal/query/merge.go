package query

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"
)

const (
	IntegerLimbCount   = 5
	MaxAggregateGroups = 20000
)

var (
	decimalBase  = big.NewInt(1_000_000_000)
	decimalLimit = new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil), big.NewInt(1))
)

type IntegerState struct {
	Limbs    [IntegerLimbCount]string `json:"limbs"`
	Count    int64                    `json:"count"`
	Excluded int64                    `json:"excluded"`
}

type AggregateGroupState struct {
	Key     string       `json:"key"`
	Integer IntegerState `json:"integer"`
}

func DecimalLimbs(value string) ([IntegerLimbCount]string, error) {
	var result [IntegerLimbCount]string
	parsed, ok := new(big.Int).SetString(value, 10)
	if !ok || value == "" || strings.HasPrefix(value, "+") || parsed.Cmp(decimalLimit) > 0 || parsed.Cmp(new(big.Int).Neg(decimalLimit)) < 0 {
		return result, errors.New("integer is outside DECIMAL(38,0)")
	}
	sign := parsed.Sign()
	abs := new(big.Int).Abs(parsed)
	for index := range result {
		quotient, remainder := new(big.Int), new(big.Int)
		quotient.QuoRem(abs, decimalBase, remainder)
		if sign < 0 {
			remainder.Neg(remainder)
		}
		result[index] = remainder.String()
		abs = quotient
	}
	if abs.Sign() != 0 {
		return result, errors.New("integer exceeds limb representation")
	}
	return result, nil
}

// IntegerLimbSQL emits exact decimal-string chunk extraction for one trusted
// compiler expression. It avoids decimal division and floating-point pow in
// the pinned native engine.
func IntegerLimbSQL(expression string, index int) (string, error) {
	trimmed := strings.TrimSpace(expression)
	if trimmed == "" || len(trimmed) > 4096 || index < 0 || index >= IntegerLimbCount || strings.ContainsAny(trimmed, ";\x00") || strings.Contains(trimmed, "--") || strings.Contains(trimmed, "/*") {
		return "", errors.New("invalid integer limb expression")
	}
	position := 45 - 9*(index+1) + 1
	return fmt.Sprintf(`CASE WHEN %[1]s IS NULL THEN NULL ELSE (CASE WHEN %[1]s<0 THEN -1 ELSE 1 END)*CAST(substr(lpad(abs(%[1]s)::VARCHAR,45,'0'),%[2]d,9) AS DECIMAL(38,0)) END`, trimmed, position), nil
}

func MergeIntegerStates(states []IntegerState) (IntegerState, error) {
	var result IntegerState
	limbs := make([]*big.Int, IntegerLimbCount)
	for index := range limbs {
		limbs[index] = new(big.Int)
	}
	for _, state := range states {
		if state.Count < 0 || state.Excluded < 0 || result.Count > math.MaxInt64-state.Count || result.Excluded > math.MaxInt64-state.Excluded {
			return IntegerState{}, ErrQueryLimit
		}
		result.Count += state.Count
		result.Excluded += state.Excluded
		for index, encoded := range state.Limbs {
			value, ok := new(big.Int).SetString(encoded, 10)
			if !ok {
				return IntegerState{}, errors.New("invalid integer limb")
			}
			limbs[index].Add(limbs[index], value)
		}
	}
	for index, value := range limbs {
		result.Limbs[index] = value.String()
	}
	return result, nil
}

func IntegerStateValue(state IntegerState) (*big.Int, error) {
	result := new(big.Int)
	power := big.NewInt(1)
	for _, encoded := range state.Limbs {
		limb, ok := new(big.Int).SetString(encoded, 10)
		if !ok {
			return nil, errors.New("invalid integer limb")
		}
		result.Add(result, new(big.Int).Mul(limb, power))
		power.Mul(power, decimalBase)
	}
	return result, nil
}

func FinalizeIntegerSum(state IntegerState) (*string, error) {
	if state.Count == 0 {
		return nil, nil
	}
	value, err := IntegerStateValue(state)
	if err != nil {
		return nil, err
	}
	if new(big.Int).Abs(new(big.Int).Set(value)).Cmp(decimalLimit) > 0 {
		return nil, errors.New("numeric overflow")
	}
	encoded := value.String()
	return &encoded, nil
}

func FinalizeIntegerAverage(state IntegerState) (*string, error) {
	if state.Count == 0 {
		return nil, nil
	}
	if state.Count < 0 {
		return nil, errors.New("invalid average count")
	}
	numerator, err := IntegerStateValue(state)
	if err != nil {
		return nil, err
	}
	scaled := new(big.Int).Mul(numerator, decimalBase)
	denominator := big.NewInt(state.Count)
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(scaled, denominator, remainder)
	twiceRemainder := new(big.Int).Lsh(new(big.Int).Abs(remainder), 1)
	comparison := twiceRemainder.Cmp(denominator)
	if comparison > 0 || comparison == 0 && quotient.Bit(0) == 1 {
		if scaled.Sign() < 0 {
			quotient.Sub(quotient, big.NewInt(1))
		} else {
			quotient.Add(quotient, big.NewInt(1))
		}
	}
	encoded := formatScaleNine(quotient)
	return &encoded, nil
}

func MergeAggregateGroups(partitions [][]AggregateGroupState) ([]AggregateGroupState, error) {
	byKey := make(map[string][]IntegerState)
	for _, partition := range partitions {
		for _, group := range partition {
			if group.Key == "" {
				return nil, errors.New("aggregate group key is empty")
			}
			if _, exists := byKey[group.Key]; !exists && len(byKey) >= MaxAggregateGroups {
				return nil, ErrQueryLimit
			}
			byKey[group.Key] = append(byKey[group.Key], group.Integer)
		}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]AggregateGroupState, 0, len(keys))
	for _, key := range keys {
		merged, err := MergeIntegerStates(byKey[key])
		if err != nil {
			return nil, err
		}
		result = append(result, AggregateGroupState{Key: key, Integer: merged})
	}
	return result, nil
}

func HistogramBucketStarts(startUS, endUS, intervalUS int64) ([]int64, error) {
	if startUS >= endUS || !validHistogramInterval(intervalUS) {
		return nil, errors.New("invalid histogram range or interval")
	}
	start := floorMultiple(startUS, intervalUS)
	if start > math.MaxInt64-intervalUS*2000 {
		return nil, ErrQueryLimit
	}
	result := make([]int64, 0)
	for bucket := start; bucket < endUS; bucket += intervalUS {
		if len(result) >= 2000 {
			return nil, ErrQueryLimit
		}
		result = append(result, bucket)
		if bucket > math.MaxInt64-intervalUS {
			break
		}
	}
	return result, nil
}

func floorMultiple(value, interval int64) int64 {
	quotient, remainder := value/interval, value%interval
	if remainder < 0 {
		quotient--
	}
	return quotient * interval
}

func validHistogramInterval(value int64) bool {
	switch value {
	case 1_000_000, 10_000_000, 60_000_000, 300_000_000, 3_600_000_000, 86_400_000_000:
		return true
	default:
		return false
	}
}

func formatScaleNine(value *big.Int) string {
	sign := ""
	abs := new(big.Int).Set(value)
	if abs.Sign() < 0 {
		sign = "-"
		abs.Abs(abs)
	}
	integer, fraction := new(big.Int), new(big.Int)
	integer.QuoRem(abs, decimalBase, fraction)
	return sign + integer.String() + "." + leftPadNine(fraction.String())
}

func leftPadNine(value string) string {
	if len(value) >= 9 {
		return value
	}
	return strings.Repeat("0", 9-len(value)) + value
}
