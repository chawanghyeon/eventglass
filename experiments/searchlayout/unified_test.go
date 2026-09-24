package main

import (
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
	kind string // absent is represented by no entry; null is explicit.
	s    string
	i    int64
	a    []string
}

type universalDoc struct {
	id, tenant int
	when       int64
	live       bool
	duration   int64
	search     []string // each searchable scalar stays separate
	fields     map[string]universalValue
}

type universalEntry struct {
	id int
	v  universalValue
}

type universalSegment struct {
	tenant   []int
	when     []int64
	live     []bool
	duration []int64
	search   [][]string
	fields   map[string][]universalEntry
	terms    map[string][]int
	exact    map[string][]int
}

func universalKey(path, kind, value string) string {
	key := appendString(nil, path)
	key = appendString(key, kind)
	return string(appendString(key, value))
}

func makeUniversalSegment(docs []universalDoc) universalSegment {
	s := universalSegment{fields: make(map[string][]universalEntry), terms: make(map[string][]int), exact: make(map[string][]int)}
	for id, d := range docs {
		s.tenant = append(s.tenant, d.tenant)
		s.when = append(s.when, d.when)
		s.live = append(s.live, d.live)
		s.duration = append(s.duration, d.duration)
		s.search = append(s.search, slices.Clone(d.search))
		seen := make(map[string]bool)
		for _, scalar := range d.search {
			for _, term := range strings.Fields(scalar) {
				if !seen[term] {
					s.terms[term] = append(s.terms[term], id)
					seen[term] = true
				}
			}
		}
		for path, v := range d.fields {
			s.fields[path] = append(s.fields[path], universalEntry{id: id, v: v})
			if v.kind == "string" {
				key := universalKey(path, v.kind, v.s)
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
		for _, e := range s.fields[p.path] {
			match := false
			switch p.op {
			case "gte":
				match = e.v.kind == "int" && e.v.i >= p.n
			case "exists":
				match = true
			case "null":
				match = e.v.kind == "null"
			case "neq":
				match = e.v.kind == "string" && e.v.s != p.text
			case "array":
				match = e.v.kind == "array" && slices.Contains(e.v.a, p.text)
			}
			if match {
				out = append(out, e.id)
			}
		}
	case "contains", "regex":
		var re *regexp.Regexp
		if p.op == "regex" {
			re = regexp.MustCompile(p.text)
		}
		for id, scalars := range s.search {
			for _, scalar := range scalars {
				if p.op == "contains" && strings.Contains(scalar, p.text) || p.op == "regex" && re.MatchString(scalar) {
					out = append(out, id)
					break
				}
			}
		}
	case "and", "or":
		if len(p.children) == 0 && p.op == "and" {
			for id := range s.tenant {
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
		for id, cursor := 0, 0; id < len(s.tenant); id++ {
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
	v, present := d.fields[p.path]
	switch p.op {
	case "term":
		for _, scalar := range d.search {
			for _, word := range strings.Fields(scalar) {
				if word == p.text {
					return true
				}
			}
		}
	case "eq":
		return present && v.kind == "string" && v.s == p.text
	case "neq":
		return present && v.kind == "string" && v.s != p.text
	case "gte":
		return present && v.kind == "int" && v.i >= p.n
	case "exists":
		return present
	case "null":
		return present && v.kind == "null"
	case "array":
		return present && v.kind == "array" && slices.Contains(v.a, p.text)
	case "contains":
		for _, scalar := range d.search {
			if strings.Contains(scalar, p.text) {
				return true
			}
		}
	case "regex":
		re := regexp.MustCompile(p.text)
		for _, scalar := range d.search {
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
	return fmt.Sprintf("%s:%s:%d", v.kind, v.s, v.i)
}

func reduceUniversal(s universalSegment, ids []int, groupPath string) universalAnswer {
	a := universalAnswer{count: len(ids), group: make(map[string]int64)}
	entries := s.fields[groupPath]
	cursor := 0
	for _, id := range ids {
		for cursor < len(entries) && entries[cursor].id < id {
			cursor++
		}
		key := universalGroup(universalValue{}, false)
		if cursor < len(entries) && entries[cursor].id == id {
			key = universalGroup(entries[cursor].v, true)
		}
		a.sum += s.duration[id]
		a.group[key] += s.duration[id]
	}
	slices.SortFunc(ids, func(a, b int) int {
		if s.duration[a] != s.duration[b] {
			if s.duration[a] > s.duration[b] {
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
			d := universalDoc{id: id, tenant: rng.Intn(3), when: int64(rng.Intn(100)), live: rng.Intn(10) != 0,
				duration: int64(rng.Intn(1000)), search: []string{messages[rng.Intn(len(messages))], messages[rng.Intn(len(messages))]}, fields: make(map[string]universalValue)}
			if rng.Intn(4) != 0 {
				d.fields["tags/region"] = universalValue{kind: "string", s: []string{"east", "west", "한글"}[rng.Intn(3)]}
			}
			if rng.Intn(5) != 0 {
				if rng.Intn(10) == 0 {
					d.fields["attrs/status"] = universalValue{kind: "string", s: "500"}
				} else {
					d.fields["attrs/status"] = universalValue{kind: "int", i: int64(rng.Intn(600))}
				}
			}
			if rng.Intn(3) == 0 {
				d.fields["attrs/note"] = universalValue{kind: "null"}
			} else if rng.Intn(3) == 0 {
				d.fields["attrs/note"] = universalValue{kind: "string", s: "present"}
			}
			if rng.Intn(2) == 0 {
				d.fields["attrs/features"] = universalValue{kind: "array", a: []string{"checkout", "billing"}}
			}
			d.fields["attrs/order_id"] = universalValue{kind: "string", s: fmt.Sprintf("order-%d-%d", seed, id)}
			d.fields["attrs/a.b"] = universalValue{kind: "string", s: "flat"}
			d.fields["attrs/a/b"] = universalValue{kind: "string", s: "nested"}
			d.search = append(d.search, d.fields["attrs/order_id"].s)
			docs[id] = d
		}
		s := makeUniversalSegment(docs)
		oracleDuration := make([]int64, len(docs))
		for id := range docs {
			oracleDuration[id] = docs[id].duration
		}
		cases := append(universalCases(), universalPredicate{op: "eq", path: "attrs/order_id", text: fmt.Sprintf("order-%d-7", seed)})
		for _, p := range cases {
			selected := selectUniversal(s, p)
			var gotIDs, wantIDs []int
			for _, id := range selected {
				d := docs[id]
				if d.tenant == 1 && d.when >= 20 && d.when < 80 && d.live {
					gotIDs = append(gotIDs, id)
				}
			}
			for _, d := range docs {
				allowed := d.tenant == 1 && d.when >= 20 && d.when < 80 && d.live
				if allowed && oracleUniversal(d, p) {
					wantIDs = append(wantIDs, d.id)
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
					want.sum += docs[id].duration
					key := "missing"
					if v, present := docs[id].fields[groupPath]; present {
						key = fmt.Sprintf("%s:%s:%d", v.kind, v.s, v.i)
					}
					want.group[key] += docs[id].duration
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
		docs[id] = universalDoc{id: id, tenant: id % 4, live: true, duration: int64(id % 1000), search: []string{word}}
	}
	s := makeUniversalSegment(docs)
	for _, term := range []string{"fatal", "request"} {
		b.Run(term+"/index", func(b *testing.B) {
			p := universalPredicate{op: "term", text: term}
			for range b.N {
				var sum int64
				for _, id := range selectUniversal(s, p) {
					sum += s.duration[id]
				}
				universalSink = sum
			}
		})
		b.Run(term+"/scan", func(b *testing.B) {
			for range b.N {
				var sum int64
				for id, d := range docs {
					if d.search[0] == term {
						sum += docs[id].duration
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
				sum += s.duration[id]
			}
			universalSink = sum
		}
	})
	b.Run("contains/scan", func(b *testing.B) {
		for range b.N {
			var sum int64
			for _, d := range docs {
				if strings.Contains(d.search[0], "fatal") {
					sum += d.duration
				}
			}
			universalSink = sum
		}
	})
}
