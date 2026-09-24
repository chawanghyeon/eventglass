package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
)

// This is a semantic probe for one doc-ID space, not a new on-disk format.
type universalValue struct {
	Kind string // absent is represented by no entry; null is explicit.
	S    string
	I    int64
	A    []string
}

type universalDoc struct {
	ID, Tenant int
	Project    int64           `json:"Project,omitempty"`
	Canonical  json.RawMessage `json:"Canonical,omitempty"`
	When       int64
	Live       bool
	Duration   int64
	Search     []string // each searchable scalar stays separate
	Fields     map[string]universalValue
}

type universalEntry struct {
	ID int
	V  universalValue
}

type universalSegment struct {
	Tenant   []int
	Project  []int64
	When     []int64
	Live     []bool
	Duration []int64
	Search   [][]string
	Fields   map[string][]universalEntry
	terms    map[string][]int
	exact    map[string][]int
	scans    map[string][]int
}

func universalKey(path, kind, value string) string {
	key := appendString(nil, path)
	key = appendString(key, kind)
	return string(appendString(key, value))
}

func makeUniversalSegment(docs []universalDoc) universalSegment {
	s := universalSegment{Fields: make(map[string][]universalEntry), terms: make(map[string][]int), exact: make(map[string][]int)}
	for id, d := range docs {
		s.Tenant = append(s.Tenant, d.Tenant)
		s.Project = append(s.Project, d.Project)
		s.When = append(s.When, d.When)
		s.Live = append(s.Live, d.Live)
		s.Duration = append(s.Duration, d.Duration)
		s.Search = append(s.Search, slices.Clone(d.Search))
		seen := make(map[string]bool)
		for _, scalar := range d.Search {
			for _, term := range strings.Fields(scalar) {
				if !seen[term] {
					s.terms[term] = append(s.terms[term], id)
					seen[term] = true
				}
			}
		}
		for path, v := range d.Fields {
			s.Fields[path] = append(s.Fields[path], universalEntry{ID: id, V: v})
			if v.Kind == "string" {
				key := universalKey(path, v.Kind, v.S)
				s.exact[key] = append(s.exact[key], id)
			}
		}
	}
	return s
}

type universalPredicate struct {
	op, path, text string
	n              int64
	children       []universalPredicate
}

func mergeUniversalIDs(a, b []int, union bool) []int {
	var out []int
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			out = append(out, a[i])
			i++
			j++
		case a[i] < b[j]:
			if union {
				out = append(out, a[i])
			}
			i++
		default:
			if union {
				out = append(out, b[j])
			}
			j++
		}
	}
	if union {
		out = append(out, a[i:]...)
		out = append(out, b[j:]...)
	}
	return out
}

func selectUniversal(s universalSegment, p universalPredicate) []int {
	var out []int
	switch p.op {
	case "term":
		return slices.Clone(s.terms[p.text])
	case "eq":
		return slices.Clone(s.exact[universalKey(p.path, "string", p.text)])
	case "gte", "exists", "null", "neq", "array":
		for _, e := range s.Fields[p.path] {
			match := false
			switch p.op {
			case "gte":
				match = e.V.Kind == "int" && e.V.I >= p.n
			case "exists":
				match = true
			case "null":
				match = e.V.Kind == "null"
			case "neq":
				match = e.V.Kind == "string" && e.V.S != p.text
			case "array":
				match = e.V.Kind == "array" && slices.Contains(e.V.A, p.text)
			}
			if match {
				out = append(out, e.ID)
			}
		}
	case "contains", "regex":
		if ids, ok := s.scans[p.op+"\x00"+p.text]; ok {
			return slices.Clone(ids)
		}
		var re *regexp.Regexp
		if p.op == "regex" {
			re = regexp.MustCompile(p.text)
		}
		for id, scalars := range s.Search {
			for _, scalar := range scalars {
				if p.op == "contains" && strings.Contains(scalar, p.text) || p.op == "regex" && re.MatchString(scalar) {
					out = append(out, id)
					break
				}
			}
		}
	case "and", "or":
		if len(p.children) == 0 && p.op == "and" {
			for id := range s.Tenant {
				out = append(out, id)
			}
			break
		}
		if len(p.children) > 0 {
			out = selectUniversal(s, p.children[0])
			for _, child := range p.children[1:] {
				selected := selectUniversal(s, child)
				out = mergeUniversalIDs(out, selected, p.op == "or")
			}
		}
	case "not":
		selected := selectUniversal(s, p.children[0])
		for id, cursor := 0, 0; id < len(s.Tenant); id++ {
			if cursor < len(selected) && selected[cursor] == id {
				cursor++
			} else {
				out = append(out, id)
			}
		}
	default:
		panic("unsupported probe predicate")
	}
	return out
}

func oracleUniversal(d universalDoc, p universalPredicate) bool {
	v, present := d.Fields[p.path]
	switch p.op {
	case "term":
		for _, scalar := range d.Search {
			for _, word := range strings.Fields(scalar) {
				if word == p.text {
					return true
				}
			}
		}
	case "eq":
		return present && v.Kind == "string" && v.S == p.text
	case "neq":
		return present && v.Kind == "string" && v.S != p.text
	case "gte":
		return present && v.Kind == "int" && v.I >= p.n
	case "exists":
		return present
	case "null":
		return present && v.Kind == "null"
	case "array":
		return present && v.Kind == "array" && slices.Contains(v.A, p.text)
	case "contains":
		for _, scalar := range d.Search {
			if strings.Contains(scalar, p.text) {
				return true
			}
		}
	case "regex":
		re := regexp.MustCompile(p.text)
		for _, scalar := range d.Search {
			if re.MatchString(scalar) {
				return true
			}
		}
	case "and":
		for _, child := range p.children {
			if !oracleUniversal(d, child) {
				return false
			}
		}
		return true
	case "or":
		for _, child := range p.children {
			if oracleUniversal(d, child) {
				return true
			}
		}
	case "not":
		return !oracleUniversal(d, p.children[0])
	}
	return false
}

type universalAnswer struct {
	count int
	sum   int64
	group map[string]int64
	top   []int
}

func universalGroup(v universalValue, present bool) string {
	if !present {
		return "missing"
	}
	return fmt.Sprintf("%s:%s:%d", v.Kind, v.S, v.I)
}

func reduceUniversal(s universalSegment, ids []int, groupPath string) universalAnswer {
	a := universalAnswer{count: len(ids), group: make(map[string]int64)}
	entries := s.Fields[groupPath]
	cursor := 0
	for _, id := range ids {
		for cursor < len(entries) && entries[cursor].ID < id {
			cursor++
		}
		key := universalGroup(universalValue{}, false)
		if cursor < len(entries) && entries[cursor].ID == id {
			key = universalGroup(entries[cursor].V, true)
		}
		a.sum += s.Duration[id]
		a.group[key] += s.Duration[id]
	}
	slices.SortFunc(ids, func(a, b int) int {
		if s.Duration[a] != s.Duration[b] {
			if s.Duration[a] > s.Duration[b] {
				return -1
			}
			return 1
		}
		return a - b
	})
	a.top = slices.Clone(ids[:min(5, len(ids))])
	return a
}

func universalCases() []universalPredicate {
	return []universalPredicate{
		{op: "term", text: "alpha"}, {op: "eq", path: "tags/region", text: "east"},
		{op: "gte", path: "attrs/status", n: 500}, {op: "exists", path: "attrs/note"},
		{op: "null", path: "attrs/note"}, {op: "neq", path: "tags/region", text: "east"},
		{op: "array", path: "attrs/features", text: "checkout"},
		{op: "eq", path: "attrs/a.b", text: "flat"},
		{op: "eq", path: "attrs/a.b", text: "nested"},
		{op: "contains", text: "alpha beta"}, {op: "regex", text: "timeout|한글"},
		{op: "and", children: []universalPredicate{{op: "term", text: "alpha"}, {op: "gte", path: "attrs/status", n: 500}}},
		{op: "or", children: []universalPredicate{{op: "eq", path: "tags/region", text: "east"}, {op: "contains", text: "timeout"}}},
		{op: "not", children: []universalPredicate{{op: "eq", path: "tags/region", text: "east"}}},
		{op: "and", children: []universalPredicate{{op: "contains", text: "alpha beta"}, {op: "not", children: []universalPredicate{{op: "null", path: "attrs/note"}}}}},
	}
}

func TestUniversalSelectionAndAggregation(t *testing.T) {
	messages := []string{"alpha beta", "alpha", "beta", "timeout 한글", "steady"}
	for seed := int64(0); seed < 20; seed++ {
		rng := rand.New(rand.NewSource(seed))
		docs := make([]universalDoc, 200)
		for id := range docs {
			d := universalDoc{ID: id, Tenant: rng.Intn(3), When: int64(rng.Intn(100)), Live: rng.Intn(10) != 0,
				Duration: int64(rng.Intn(1000)), Search: []string{messages[rng.Intn(len(messages))], messages[rng.Intn(len(messages))]}, Fields: make(map[string]universalValue)}
			if rng.Intn(4) != 0 {
				d.Fields["tags/region"] = universalValue{Kind: "string", S: []string{"east", "west", "한글"}[rng.Intn(3)]}
			}
			if rng.Intn(5) != 0 {
				if rng.Intn(10) == 0 {
					d.Fields["attrs/status"] = universalValue{Kind: "string", S: "500"}
				} else {
					d.Fields["attrs/status"] = universalValue{Kind: "int", I: int64(rng.Intn(600))}
				}
			}
			if rng.Intn(3) == 0 {
				d.Fields["attrs/note"] = universalValue{Kind: "null"}
			} else if rng.Intn(3) == 0 {
				d.Fields["attrs/note"] = universalValue{Kind: "string", S: "present"}
			}
			if rng.Intn(2) == 0 {
				d.Fields["attrs/features"] = universalValue{Kind: "array", A: []string{"checkout", "billing"}}
			}
			d.Fields["attrs/order_id"] = universalValue{Kind: "string", S: fmt.Sprintf("order-%d-%d", seed, id)}
			d.Fields["attrs/a.b"] = universalValue{Kind: "string", S: "flat"}
			d.Fields["attrs/a/b"] = universalValue{Kind: "string", S: "nested"}
			d.Search = append(d.Search, d.Fields["attrs/order_id"].S)
			docs[id] = d
		}
		s := makeUniversalSegment(docs)
		oracleDuration := make([]int64, len(docs))
		for id := range docs {
			oracleDuration[id] = docs[id].Duration
		}
		cases := append(universalCases(), universalPredicate{op: "eq", path: "attrs/order_id", text: fmt.Sprintf("order-%d-7", seed)})
		for _, p := range cases {
			selected := selectUniversal(s, p)
			var gotIDs, wantIDs []int
			for _, id := range selected {
				d := docs[id]
				if d.Tenant == 1 && d.When >= 20 && d.When < 80 && d.Live {
					gotIDs = append(gotIDs, id)
				}
			}
			for _, d := range docs {
				allowed := d.Tenant == 1 && d.When >= 20 && d.When < 80 && d.Live
				if allowed && oracleUniversal(d, p) {
					wantIDs = append(wantIDs, d.ID)
				}
			}
			if !slices.Equal(gotIDs, wantIDs) {
				t.Fatalf("seed=%d op=%s selected=%v oracle=%v", seed, p.op, gotIDs, wantIDs)
			}
			for _, groupPath := range []string{"tags/region", "attrs/note", "attrs/status", "attrs/order_id"} {
				got := reduceUniversal(s, slices.Clone(gotIDs), groupPath)
				want := universalAnswer{group: make(map[string]int64), top: slices.Clone(wantIDs)}
				for _, id := range wantIDs {
					want.count++
					want.sum += docs[id].Duration
					key := "missing"
					if v, present := docs[id].Fields[groupPath]; present {
						key = fmt.Sprintf("%s:%s:%d", v.Kind, v.S, v.I)
					}
					want.group[key] += docs[id].Duration
				}
				sort.Slice(want.top, func(i, j int) bool {
					a, b := want.top[i], want.top[j]
					if oracleDuration[a] != oracleDuration[b] {
						return oracleDuration[a] > oracleDuration[b]
					}
					return a < b
				})
				want.top = want.top[:min(5, len(want.top))]
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("seed=%d op=%s group=%s got=%+v want=%+v", seed, p.op, groupPath, got, want)
				}
			}
		}
	}
}

var universalSink int64

func BenchmarkUniversalSelection(b *testing.B) {
	docs := make([]universalDoc, 100000)
	for id := range docs {
		word := "request"
		if id%1000 == 0 {
			word = "fatal"
		}
		docs[id] = universalDoc{ID: id, Tenant: id % 4, Live: true, Duration: int64(id % 1000), Search: []string{word}}
	}
	s := makeUniversalSegment(docs)
	for _, term := range []string{"fatal", "request"} {
		b.Run(term+"/index", func(b *testing.B) {
			p := universalPredicate{op: "term", text: term}
			for range b.N {
				var sum int64
				for _, id := range selectUniversal(s, p) {
					sum += s.Duration[id]
				}
				universalSink = sum
			}
		})
		b.Run(term+"/scan", func(b *testing.B) {
			for range b.N {
				var sum int64
				for id, d := range docs {
					if d.Search[0] == term {
						sum += docs[id].Duration
					}
				}
				universalSink = sum
			}
		})
	}
	b.Run("contains/index_fallback", func(b *testing.B) {
		p := universalPredicate{op: "contains", text: "fatal"}
		for range b.N {
			var sum int64
			for _, id := range selectUniversal(s, p) {
				sum += s.Duration[id]
			}
			universalSink = sum
		}
	})
	b.Run("contains/scan", func(b *testing.B) {
		for range b.N {
			var sum int64
			for _, d := range docs {
				if strings.Contains(d.Search[0], "fatal") {
					sum += d.Duration
				}
			}
			universalSink = sum
		}
	})
}
