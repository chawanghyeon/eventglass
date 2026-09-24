package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// One immutable object with independently addressable index, column and source
// ranges. JSON is used for the small probe's column/source codec; its byte size
// is not a proposed production format.
type universalDirectory struct {
	Count   int             `json:"count"`
	Core    part            `json:"core"`
	Fields  map[string]part `json:"fields"`
	Terms   map[string]part `json:"terms"`
	Sources []part          `json:"sources"`
}

type universalCore struct {
	Tenant   []int   `json:"tenant"`
	When     []int64 `json:"when"`
	Live     []bool  `json:"live"`
	Duration []int64 `json:"duration"`
}

const universalPageDocs = 128
const universalFooterSize = 25

func writeUniversalObject(path string, docs []universalDoc) (universalDirectory, int64, error) {
	f, err := os.Create(path)
	if err != nil {
		return universalDirectory{}, 0, err
	}
	defer f.Close()
	s := makeUniversalSegment(docs)
	dir := universalDirectory{Count: len(docs), Fields: make(map[string]part), Terms: make(map[string]part)}
	writeJSON := func(value any) (part, error) {
		data, err := json.Marshal(value)
		if err != nil {
			return part{}, err
		}
		return writePart(f, data, true)
	}
	dir.Core, err = writeJSON(universalCore{Tenant: s.Tenant, When: s.When, Live: s.Live, Duration: s.Duration})
	if err != nil {
		return dir, 0, err
	}
	paths := make([]string, 0, len(s.Fields))
	for path := range s.Fields {
		paths = append(paths, path)
	}
	slices.Sort(paths)
	for _, path := range paths {
		dir.Fields[path], err = writeJSON(s.Fields[path])
		if err != nil {
			return dir, 0, err
		}
	}
	terms := make([]string, 0, len(s.terms))
	for term := range s.terms {
		terms = append(terms, term)
	}
	slices.Sort(terms)
	for _, term := range terms {
		var encoded []byte
		prev := -1
		for _, id := range s.terms[term] {
			encoded = binary.AppendUvarint(encoded, uint64(id-prev))
			prev = id
		}
		dir.Terms[term], err = writePart(f, encoded, len(encoded) >= 64)
		if err != nil {
			return dir, 0, err
		}
	}
	for start := 0; start < len(docs); start += universalPageDocs {
		p, err := writeJSON(docs[start:min(start+universalPageDocs, len(docs))])
		if err != nil {
			return dir, 0, err
		}
		dir.Sources = append(dir.Sources, p)
	}
	directoryPart, err := writeJSON(dir)
	if err != nil {
		return dir, 0, err
	}
	var footer []byte
	footer = append(footer, "UGS1"...)
	footer = binary.LittleEndian.AppendUint64(footer, uint64(directoryPart.Offset))
	footer = binary.LittleEndian.AppendUint64(footer, uint64(directoryPart.Size))
	footer = binary.LittleEndian.AppendUint32(footer, directoryPart.CRC32C)
	footer = append(footer, 1) // directory is zlib-compressed
	if len(footer) != universalFooterSize {
		return dir, 0, errors.New("invalid footer size")
	}
	if _, err := f.Write(footer); err != nil {
		return dir, 0, err
	}
	info, err := f.Stat()
	if err != nil {
		return dir, 0, err
	}
	return dir, info.Size(), f.Sync()
}

func readUniversalDirectory(ctx context.Context, src rangeSource, size int64) (universalDirectory, error) {
	if size < universalFooterSize {
		return universalDirectory{}, io.ErrUnexpectedEOF
	}
	footer, err := src.Get(ctx, "universal.bin", size-universalFooterSize, universalFooterSize)
	if err != nil {
		return universalDirectory{}, err
	}
	if len(footer) != universalFooterSize || string(footer[:4]) != "UGS1" || footer[24] != 1 {
		return universalDirectory{}, errors.New("invalid universal footer")
	}
	p := part{Offset: int64(binary.LittleEndian.Uint64(footer[4:12])), Size: int64(binary.LittleEndian.Uint64(footer[12:20])), CRC32C: binary.LittleEndian.Uint32(footer[20:24]), Zlib: true}
	if p.Offset < 0 || p.Size <= 0 || p.Offset > size-universalFooterSize-p.Size {
		return universalDirectory{}, errors.New("invalid universal directory range")
	}
	raw, err := readPart(ctx, src, segment{Name: "universal.bin"}, p)
	if err != nil {
		return universalDirectory{}, err
	}
	var dir universalDirectory
	if err := json.Unmarshal(raw, &dir); err != nil {
		return dir, err
	}
	if dir.Count <= 0 || len(dir.Sources) != (dir.Count+universalPageDocs-1)/universalPageDocs {
		return dir, errors.New("invalid universal directory")
	}
	check := func(p part) error {
		if p.Offset < 0 || p.Size <= 0 || p.Offset > size-universalFooterSize-p.Size {
			return errors.New("part outside immutable object")
		}
		return nil
	}
	if err := check(dir.Core); err != nil {
		return dir, err
	}
	for _, p := range dir.Fields {
		if err := check(p); err != nil {
			return dir, err
		}
	}
	for _, p := range dir.Terms {
		if err := check(p); err != nil {
			return dir, err
		}
	}
	for _, p := range dir.Sources {
		if err := check(p); err != nil {
			return dir, err
		}
	}
	return dir, nil
}

func universalFixture(n int) []universalDoc {
	docs := make([]universalDoc, n)
	for id := range docs {
		message := []string{"alpha beta", "alpha", "beta", "request completed", "request completed", "request completed", "request completed", "request completed", "request completed", "request completed"}[id%10]
		if id%101 == 0 {
			message = "fatal timeout 한글"
		}
		d := universalDoc{ID: id, Tenant: id % 4, When: int64(id % 100), Live: id%17 != 0,
			Duration: int64(id % 1000), Search: []string{message, fmt.Sprintf("trace%d", id)}, Fields: map[string]universalValue{
				"tags/region":    {Kind: "string", S: []string{"east", "west"}[id%2]},
				"attrs/order_id": {Kind: "string", S: fmt.Sprintf("order%d", id)},
			}}
		if id%3 == 0 {
			d.Fields["attrs/status"] = universalValue{Kind: "int", I: int64(id % 600)}
		}
		if id%5 == 0 {
			d.Fields["attrs/note"] = universalValue{Kind: "null"}
		}
		if id%7 == 0 {
			d.Fields["attrs/features"] = universalValue{Kind: "array", A: []string{"checkout", "billing"}}
		}
		d.Fields["attrs/a.b"] = universalValue{Kind: "string", S: "flat"}
		d.Fields["attrs/a/b"] = universalValue{Kind: "string", S: "nested"}
		d.Fields[fmt.Sprintf("attrs/custom/%d", id%1000)] = universalValue{Kind: "string", S: fmt.Sprintf("v%d", id)}
		docs[id] = d
	}
	return docs
}

func runUniversalRange(ctx context.Context, src rangeSource, size int64, p universalPredicate, groupPath string) (universalAnswer, []universalDoc, error) {
	dir, err := readUniversalDirectory(ctx, src, size)
	if err != nil {
		return universalAnswer{}, nil, err
	}
	read := func(p part, target any) error {
		raw, err := readPart(ctx, src, segment{Name: "universal.bin"}, p)
		if err != nil {
			return err
		}
		return json.Unmarshal(raw, target)
	}
	var core universalCore
	if err := read(dir.Core, &core); err != nil {
		return universalAnswer{}, nil, err
	}
	if len(core.Tenant) != dir.Count || len(core.When) != dir.Count || len(core.Live) != dir.Count || len(core.Duration) != dir.Count {
		return universalAnswer{}, nil, errors.New("invalid universal core")
	}
	s := universalSegment{Tenant: core.Tenant, When: core.When, Live: core.Live, Duration: core.Duration,
		Search: make([][]string, dir.Count), Fields: make(map[string][]universalEntry), terms: make(map[string][]int), exact: make(map[string][]int)}
	neededFields := map[string]bool{groupPath: true}
	neededTerms := make(map[string]bool)
	needsSource := false
	var walk func(universalPredicate)
	walk = func(p universalPredicate) {
		switch p.op {
		case "term":
			neededTerms[p.text] = true
		case "eq", "neq", "gte", "exists", "null", "array":
			neededFields[p.path] = true
		case "contains", "regex":
			needsSource = true
		}
		for _, child := range p.children {
			walk(child)
		}
	}
	walk(p)
	for path := range neededFields {
		part, ok := dir.Fields[path]
		if !ok {
			continue
		}
		var entries []universalEntry
		if err := read(part, &entries); err != nil {
			return universalAnswer{}, nil, err
		}
		s.Fields[path] = entries
		prev := -1
		for _, entry := range s.Fields[path] {
			if entry.ID <= prev || entry.ID >= dir.Count {
				return universalAnswer{}, nil, errors.New("invalid sparse column ID")
			}
			prev = entry.ID
			if entry.V.Kind == "string" {
				key := universalKey(path, "string", entry.V.S)
				s.exact[key] = append(s.exact[key], entry.ID)
			}
		}
	}
	for term := range neededTerms {
		part, ok := dir.Terms[term]
		if !ok {
			continue
		}
		raw, err := readPart(ctx, src, segment{Name: "universal.bin"}, part)
		if err != nil {
			return universalAnswer{}, nil, err
		}
		prev := -1
		for len(raw) > 0 {
			delta, width := binary.Uvarint(raw)
			if width <= 0 || delta == 0 || delta > uint64(dir.Count) {
				return universalAnswer{}, nil, errors.New("invalid posting delta")
			}
			prev += int(delta)
			if prev >= dir.Count {
				return universalAnswer{}, nil, errors.New("posting outside segment")
			}
			s.terms[term] = append(s.terms[term], prev)
			raw = raw[width:]
		}
	}
	pageCache := make(map[int][]universalDoc)
	readPage := func(page int) ([]universalDoc, error) {
		if docs, ok := pageCache[page]; ok {
			return docs, nil
		}
		var docs []universalDoc
		if err := read(dir.Sources[page], &docs); err != nil {
			return nil, err
		}
		if len(docs) != min(universalPageDocs, dir.Count-page*universalPageDocs) {
			return nil, errors.New("invalid source page count")
		}
		for i, d := range docs {
			if d.ID != page*universalPageDocs+i {
				return nil, errors.New("invalid source document ID")
			}
		}
		pageCache[page] = docs
		return docs, nil
	}
	if needsSource {
		for page := range dir.Sources {
			docs, err := readPage(page)
			if err != nil {
				return universalAnswer{}, nil, err
			}
			for _, d := range docs {
				s.Search[d.ID] = d.Search
			}
		}
	}
	var ids []int
	for _, id := range selectUniversal(s, p) {
		if s.Tenant[id] == 1 && s.When[id] >= 20 && s.When[id] < 80 && s.Live[id] {
			ids = append(ids, id)
		}
	}
	answer := reduceUniversal(s, ids, groupPath)
	var details []universalDoc
	for _, id := range answer.top {
		page, err := readPage(id / universalPageDocs)
		if err != nil {
			return universalAnswer{}, nil, err
		}
		details = append(details, page[id%universalPageDocs])
	}
	return answer, details, nil
}

func oracleUniversalRange(docs []universalDoc, p universalPredicate, groupPath string) (universalAnswer, []universalDoc) {
	a := universalAnswer{group: make(map[string]int64)}
	for _, d := range docs {
		if d.Tenant != 1 || d.When < 20 || d.When >= 80 || !d.Live || !oracleUniversal(d, p) {
			continue
		}
		a.count++
		a.sum += d.Duration
		key := "missing"
		if v, ok := d.Fields[groupPath]; ok {
			key = fmt.Sprintf("%s:%s:%d", v.Kind, v.S, v.I)
		}
		a.group[key] += d.Duration
		a.top = append(a.top, d.ID)
	}
	sort.Slice(a.top, func(i, j int) bool {
		x, y := docs[a.top[i]], docs[a.top[j]]
		if x.Duration != y.Duration {
			return x.Duration > y.Duration
		}
		return x.ID < y.ID
	})
	a.top = a.top[:min(5, len(a.top))]
	var details []universalDoc
	for _, id := range a.top {
		details = append(details, docs[id])
	}
	return a, details
}

func TestUniversalRangeRoundTrip(t *testing.T) {
	docs := universalFixture(10000)
	path := filepath.Join(t.TempDir(), "universal.bin")
	dir, size, err := writeUniversalObject(path, docs)
	if err != nil {
		t.Fatal(err)
	}
	local := localSource{dir: filepath.Dir(path)}
	cases := append(universalCases(), universalPredicate{op: "eq", path: "attrs/order_id", text: "order1237"}, universalPredicate{op: "term", text: "trace1237"}, universalPredicate{op: "eq", path: "attrs/custom/237", text: "v1237"})
	check := func(label string, src rangeSource) {
		t.Helper()
		for _, groupPath := range []string{"tags/region", "attrs/note", "attrs/order_id", "attrs/custom/237"} {
			for _, p := range cases {
				got, details, err := runUniversalRange(context.Background(), src, size, p, groupPath)
				want, wantDetails := oracleUniversalRange(docs, p, groupPath)
				if err != nil || !reflect.DeepEqual(got, want) || !reflect.DeepEqual(details, wantDetails) {
					t.Fatalf("%s %s/%s err=%v got=%+v want=%+v", label, p.op, groupPath, err, got, want)
				}
			}
		}
	}
	check("local", local)
	for _, tc := range []struct {
		p   universalPredicate
		min int
	}{{universalPredicate{op: "term", text: "alpha"}, 1}, {universalPredicate{op: "regex", text: "timeout|한글"}, 1}, {universalPredicate{op: "eq", path: "attrs/order_id", text: "order1237"}, 1}, {universalPredicate{op: "term", text: "trace1237"}, 1}, {universalPredicate{op: "eq", path: "attrs/custom/237", text: "v1237"}, 1}} {
		want, _ := oracleUniversalRange(docs, tc.p, "tags/region")
		if want.count < tc.min {
			t.Fatalf("fixture did not exercise %s %s", tc.p.op, tc.p.text)
		}
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() != size {
		t.Fatal("persisted object size mismatch", err)
	}
	if len(dir.Terms) < len(docs) || len(dir.Fields) < 1000 {
		t.Fatal("fixture did not exercise large vocabulary and dynamic fields")
	}
	// A damaged selected posting must fail the query, not yield a partial count.
	bad := corruptUniversalSource{src: local, offset: dir.Terms["fatal"].Offset}
	if _, _, err := runUniversalRange(context.Background(), bad, size, universalPredicate{op: "term", text: "fatal"}, "tags/region"); err == nil {
		t.Fatal("accepted corrupted posting")
	}
	short := shortUniversalSource{src: local, offset: dir.Core.Offset}
	if _, _, err := runUniversalRange(context.Background(), short, size, universalPredicate{op: "term", text: "fatal"}, "tags/region"); err == nil {
		t.Fatal("accepted short column read")
	}
	footer, err := local.Get(context.Background(), "universal.bin", size-universalFooterSize, universalFooterSize)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("local docs=%d objectBytes=%d directoryBytes=%d coreBytes=%d terms=%d fields=%d verified=%d", len(docs), size, binary.LittleEndian.Uint64(footer[12:20]), dir.Core.Size, len(dir.Terms), len(dir.Fields), len(cases)*4)
	endpoint := os.Getenv("EVENTGLASS_UNIFIED_MINIO")
	if endpoint == "" {
		return
	}
	bucket := fmt.Sprintf("eventglass-unified-%d", os.Getpid())
	client, err := newMinIOClient(context.Background(), endpoint, bucket)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("universal.bin"), Body: f})
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String("universal.bin")})
		_, _ = client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})
	remote := s3RangeSource{client: client, bucket: bucket}
	check("minio", remote)
	for _, tc := range []struct {
		name string
		p    universalPredicate
	}{{"rare", universalPredicate{op: "term", text: "fatal"}}, {"broad", universalPredicate{op: "term", text: "request"}}, {"regex", universalPredicate{op: "regex", text: "timeout|한글"}}} {
		meter := &measuredSource{src: remote}
		if _, _, err := runUniversalRange(context.Background(), meter, size, tc.p, "tags/region"); err != nil {
			t.Fatal(err)
		}
		t.Logf("minio query=%s GET=%d bytes=%d", tc.name, meter.calls, meter.bytes)
	}
}

type corruptUniversalSource struct {
	src    rangeSource
	offset int64
}

func (s corruptUniversalSource) Get(ctx context.Context, name string, offset, size int64) ([]byte, error) {
	data, err := s.src.Get(ctx, name, offset, size)
	if err == nil && offset == s.offset && len(data) > 0 {
		data[0] ^= 1
	}
	return data, err
}

type shortUniversalSource struct {
	src    rangeSource
	offset int64
}

func (s shortUniversalSource) Get(ctx context.Context, name string, offset, size int64) ([]byte, error) {
	data, err := s.src.Get(ctx, name, offset, size)
	if err == nil && offset == s.offset && len(data) > 0 {
		return data[:len(data)-1], nil
	}
	return data, err
}
