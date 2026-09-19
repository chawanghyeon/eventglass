package query

import (
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
)

type contractCases struct {
	Integer []struct {
		ID            string     `json:"id"`
		Metric        string     `json:"metric"`
		Partitions    [][]string `json:"partitions"`
		Expected      *string    `json:"expected"`
		ExpectedError string     `json:"expected_error"`
	} `json:"integer_cases"`
	Average []struct {
		ID       string `json:"id"`
		Sum      string `json:"sum"`
		Count    string `json:"count"`
		Expected string `json:"expected"`
	} `json:"average_finalization_cases"`
}

func TestExactIntegerContractCases(t *testing.T) {
	data, err := os.ReadFile("../../docs/implementation/contract-cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases contractCases
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, test := range cases.Integer {
		t.Run(test.ID, func(t *testing.T) {
			var partials []IntegerState
			for _, partition := range test.Partitions {
				states := make([]IntegerState, 0, len(partition))
				for _, value := range partition {
					limbs, err := DecimalLimbs(value)
					if err != nil {
						t.Fatal(err)
					}
					states = append(states, IntegerState{Limbs: limbs, Count: 1})
				}
				partial, err := MergeIntegerStates(states)
				if err != nil {
					t.Fatal(err)
				}
				partials = append(partials, partial)
			}
			merged, err := MergeIntegerStates(partials)
			if err != nil {
				t.Fatal(err)
			}
			var got *string
			if test.Metric == "avg" {
				got, err = FinalizeIntegerAverage(merged)
			} else {
				got, err = FinalizeIntegerSum(merged)
			}
			if test.ExpectedError != "" {
				if err == nil {
					t.Fatalf("expected %s, got=%v", test.ExpectedError, got)
				}
				return
			}
			if err != nil || !equalOptionalString(got, test.Expected) {
				t.Fatalf("got=%v want=%v err=%v", got, test.Expected, err)
			}
		})
	}
	for _, test := range cases.Average {
		t.Run(test.ID, func(t *testing.T) {
			limbs, err := DecimalLimbs(test.Sum)
			if err != nil {
				t.Fatal(err)
			}
			count, err := strconv.ParseInt(test.Count, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			got, err := FinalizeIntegerAverage(IntegerState{Limbs: limbs, Count: count})
			if err != nil || got == nil || *got != test.Expected {
				t.Fatalf("got=%v want=%s err=%v", got, test.Expected, err)
			}
		})
	}
}

func TestMergeAggregateGroupsKeepsGlobalWinnerAndCapsUnion(t *testing.T) {
	state := func(value string) IntegerState {
		limbs, err := DecimalLimbs(value)
		if err != nil {
			t.Fatal(err)
		}
		return IntegerState{Limbs: limbs, Count: 1}
	}
	groups, err := MergeAggregateGroups([][]AggregateGroupState{
		{{Key: "A", Integer: state("11")}, {Key: "X", Integer: state("10")}},
		{{Key: "B", Integer: state("11")}, {Key: "X", Integer: state("10")}},
	})
	if err != nil || len(groups) != 3 {
		t.Fatalf("groups=%#v err=%v", groups, err)
	}
	value, _ := FinalizeIntegerSum(groups[2].Integer)
	if groups[2].Key != "X" || value == nil || *value != "20" {
		t.Fatalf("global X=%#v value=%v", groups[2], value)
	}
	partition := make([]AggregateGroupState, MaxAggregateGroups+1)
	for index := range partition {
		partition[index] = AggregateGroupState{Key: string(rune(index + 1)), Integer: state("1")}
	}
	if _, err := MergeAggregateGroups([][]AggregateGroupState{partition}); !errors.Is(err, ErrQueryLimit) {
		t.Fatalf("group cap=%v", err)
	}
}

func TestHistogramBucketStartsUsesFloorForNegativeEpoch(t *testing.T) {
	got, err := HistogramBucketStarts(-1, 1_000_001, 1_000_000)
	want := []int64{-1_000_000, 0, 1_000_000}
	if err != nil || len(got) != len(want) {
		t.Fatalf("got=%v err=%v", got, err)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("got=%v want=%v", got, want)
		}
	}
	if _, err := HistogramBucketStarts(0, 2_001_000_000, 1_000_000); !errors.Is(err, ErrQueryLimit) {
		t.Fatalf("bucket cap=%v", err)
	}
}

func TestSelectAggregateTopKKeepsGlobalWinnerAndExactAverageRank(t *testing.T) {
	integer := func(sum string, count int64) *IntegerState {
		limbs, err := DecimalLimbs(sum)
		if err != nil {
			t.Fatal(err)
		}
		return &IntegerState{Limbs: limbs, Count: count}
	}
	groups := []AggregateRankedGroup{
		{Key: []byte("A"), IntegerState: integer("11", 1)},
		{Key: []byte("B"), IntegerState: integer("11", 1)},
		// X was only second locally in two partitions, but wins after the
		// native global reduction retained every local group.
		{Key: []byte("X"), IntegerState: integer("20", 2)},
	}
	selected, err := SelectAggregateTopK(groupIterator(groups), 1, AggregateRankOrder{Kind: RankByIntegerSum, Descending: true})
	if err != nil || len(selected) != 1 || string(selected[0].Key) != "X" {
		t.Fatalf("selected=%#v err=%v", selected, err)
	}

	averages := []AggregateRankedGroup{
		{Key: []byte("z"), IntegerState: integer("1", 3)},
		{Key: []byte("a"), IntegerState: integer("333333333", 1_000_000_000)},
	}
	selected, err = SelectAggregateTopK(groupIterator(averages), 1, AggregateRankOrder{Kind: RankByIntegerAvg, Descending: true})
	if err != nil || len(selected) != 1 || string(selected[0].Key) != "z" {
		t.Fatalf("exact average selected=%#v err=%v", selected, err)
	}
}

func TestSelectAggregateTopKValidatesUnselectedOverflow(t *testing.T) {
	maximum, err := DecimalLimbs(strings.Repeat("9", 38))
	if err != nil {
		t.Fatal(err)
	}
	one, err := DecimalLimbs("1")
	if err != nil {
		t.Fatal(err)
	}
	overflow, err := MergeIntegerStates([]IntegerState{{Limbs: maximum, Count: 1}, {Limbs: one, Count: 1}})
	if err != nil {
		t.Fatal(err)
	}
	groups := []AggregateRankedGroup{
		{Key: []byte("winner"), IntegerState: &IntegerState{Limbs: one, Count: 1}},
		{Key: []byte("overflow"), IntegerState: &overflow},
	}
	if _, err := SelectAggregateTopK(groupIterator(groups), 1, AggregateRankOrder{Kind: RankByIntegerSum, Descending: false}); err == nil {
		t.Fatal("unselected overflowing metric did not fail the whole aggregate")
	}
}

func groupIterator(groups []AggregateRankedGroup) func() (*AggregateRankedGroup, error) {
	index := 0
	return func() (*AggregateRankedGroup, error) {
		if index == len(groups) {
			return nil, nil
		}
		group := groups[index]
		index++
		return &group, nil
	}
}

func equalOptionalString(left, right *string) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}
