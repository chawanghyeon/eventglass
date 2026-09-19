package query

import (
	"bytes"
	"container/heap"
	"errors"
	"math"
	"math/big"
	"sort"
)

type AggregateRankKind string

const (
	RankByCount      AggregateRankKind = "count"
	RankByIntegerSum AggregateRankKind = "integer_sum"
	RankByIntegerAvg AggregateRankKind = "integer_avg"
	RankByDouble     AggregateRankKind = "double"
)

type AggregateRankOrder struct {
	Kind       AggregateRankKind
	Descending bool
}

// AggregateRankedGroup is the bounded finalizer input. Key must already be the
// canonical type-tagged group encoding. Payload is returned untouched to let
// the API finalizer retain only the selected public rows.
type AggregateRankedGroup struct {
	Key          []byte
	Count        int64
	IntegerState *IntegerState
	Double       *float64
	Payload      any
}

// SelectAggregateTopK consumes every globally reduced group while retaining at
// most top groups. The callback returns (nil, nil) at EOF. Integer averages are
// ranked as exact rationals; display rounding happens after selection.
func SelectAggregateTopK(next func() (*AggregateRankedGroup, error), top int, order AggregateRankOrder) ([]AggregateRankedGroup, error) {
	if next == nil || top < 1 || top > 1000 || !validAggregateRank(order.Kind) {
		return nil, errors.New("invalid aggregate top-k request")
	}
	selected := &aggregateWorstHeap{order: order}
	heap.Init(selected)
	seen := 0
	for {
		group, err := next()
		if err != nil {
			return nil, err
		}
		if group == nil {
			break
		}
		seen++
		if seen > MaxAggregateGroups || len(group.Key) == 0 {
			return nil, ErrQueryLimit
		}
		candidate, err := prepareAggregateRank(*group, order.Kind)
		if err != nil {
			return nil, err
		}
		if selected.Len() < top {
			heap.Push(selected, candidate)
		} else if compareAggregateRank(candidate, selected.items[0], order) < 0 {
			heap.Pop(selected)
			heap.Push(selected, candidate)
		}
	}
	sort.Slice(selected.items, func(i, j int) bool {
		return compareAggregateRank(selected.items[i], selected.items[j], order) < 0
	})
	result := make([]AggregateRankedGroup, len(selected.items))
	for index := range selected.items {
		result[index] = selected.items[index].group
	}
	return result, nil
}

type preparedAggregateRank struct {
	group       AggregateRankedGroup
	null        bool
	integer     *big.Int
	denominator int64
	double      float64
}

func prepareAggregateRank(group AggregateRankedGroup, kind AggregateRankKind) (preparedAggregateRank, error) {
	group.Key = bytes.Clone(group.Key)
	prepared := preparedAggregateRank{group: group}
	switch kind {
	case RankByCount:
		if group.Count < 0 {
			return prepared, errors.New("invalid aggregate count")
		}
		prepared.integer = big.NewInt(group.Count)
	case RankByIntegerSum:
		if group.IntegerState == nil || group.IntegerState.Count == 0 {
			prepared.null = true
			break
		}
		if _, err := FinalizeIntegerSum(*group.IntegerState); err != nil {
			return prepared, err
		}
		value, err := IntegerStateValue(*group.IntegerState)
		if err != nil {
			return prepared, err
		}
		prepared.integer = value
	case RankByIntegerAvg:
		if group.IntegerState == nil || group.IntegerState.Count == 0 {
			prepared.null = true
			break
		}
		if group.IntegerState.Count < 0 {
			return prepared, errors.New("invalid aggregate average count")
		}
		value, err := IntegerStateValue(*group.IntegerState)
		if err != nil {
			return prepared, err
		}
		prepared.integer, prepared.denominator = value, group.IntegerState.Count
	case RankByDouble:
		if group.Double == nil {
			prepared.null = true
		} else if math.IsNaN(*group.Double) || math.IsInf(*group.Double, 0) {
			return prepared, errors.New("nonfinite aggregate result")
		} else {
			prepared.double = *group.Double
		}
	}
	return prepared, nil
}

// compareAggregateRank returns -1 when left ranks before right.
func compareAggregateRank(left, right preparedAggregateRank, order AggregateRankOrder) int {
	if left.null != right.null {
		if left.null {
			return 1
		}
		return -1
	}
	comparison := 0
	if !left.null {
		switch order.Kind {
		case RankByIntegerAvg:
			leftCross := new(big.Int).Mul(left.integer, big.NewInt(right.denominator))
			rightCross := new(big.Int).Mul(right.integer, big.NewInt(left.denominator))
			comparison = leftCross.Cmp(rightCross)
		case RankByCount, RankByIntegerSum:
			comparison = left.integer.Cmp(right.integer)
		case RankByDouble:
			if left.double < right.double {
				comparison = -1
			} else if left.double > right.double {
				comparison = 1
			}
		}
		if order.Descending {
			comparison = -comparison
		}
	}
	if comparison != 0 {
		return comparison
	}
	return bytes.Compare(left.group.Key, right.group.Key)
}

func validAggregateRank(kind AggregateRankKind) bool {
	return kind == RankByCount || kind == RankByIntegerSum || kind == RankByIntegerAvg || kind == RankByDouble
}

type aggregateWorstHeap struct {
	items []preparedAggregateRank
	order AggregateRankOrder
}

func (values aggregateWorstHeap) Len() int { return len(values.items) }
func (values aggregateWorstHeap) Less(i, j int) bool {
	return compareAggregateRank(values.items[i], values.items[j], values.order) > 0
}
func (values aggregateWorstHeap) Swap(i, j int) {
	values.items[i], values.items[j] = values.items[j], values.items[i]
}
func (values *aggregateWorstHeap) Push(value any) {
	values.items = append(values.items, value.(preparedAggregateRank))
}
func (values *aggregateWorstHeap) Pop() any {
	last := len(values.items) - 1
	value := values.items[last]
	values.items = values.items[:last]
	return value
}
