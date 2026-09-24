// Package main is an isolated layout experiment, not the Eventglass query engine.
package main

import (
	"bytes"
	"compress/zlib"
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type document struct {
	Tenant   uint8  `json:"tenant"`
	Group    uint16 `json:"group"`
	Duration uint16 `json:"duration"`
	Text     string `json:"text"`
}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type part struct {
	Offset int64  `json:"offset"`
	Size   int64  `json:"size"`
	CRC32C uint32 `json:"crc32c"`
	Zlib   bool   `json:"zlib,omitempty"`
	Packed bool   `json:"packed,omitempty"`
}

type segment struct {
	Name     string          `json:"name"`
	Base     int             `json:"base"`
	Count    int             `json:"count"`
	PageDocs int             `json:"page_docs"`
	SHA256   string          `json:"sha256"`
	Postings map[string]part `json:"postings"`
	Columns  []part          `json:"columns"`
	Sources  []part          `json:"sources"`
}

type corpus struct {
	Count  int            `json:"count"`
	AvgLen float64        `json:"avg_len"`
	DF     map[string]int `json:"df"`
	Segs   []segment      `json:"segments"`
}

type posting struct{ id, tf int }

// build writes immutable segment objects first and the catalog last.
func build(dir string, docs []document, segmentDocs, pageDocs int) (corpus, error) {
	return buildWithColumns(dir, docs, segmentDocs, pageDocs, false)
}

func buildPacked(dir string, docs []document, segmentDocs, pageDocs int) (corpus, error) {
	return buildWithColumns(dir, docs, segmentDocs, pageDocs, true)
}

func buildWithColumns(dir string, docs []document, segmentDocs, pageDocs int, packed bool) (corpus, error) {
	if segmentDocs <= 0 || pageDocs <= 0 || len(docs) == 0 {
		return corpus{}, errors.New("invalid build dimensions")
	}
	c := corpus{Count: len(docs), DF: make(map[string]int)}
	totalTerms := 0
	for _, d := range docs {
		terms := tokenize(d.Text)
		if len(terms) > 255 {
			return corpus{}, errors.New("document has more than 255 tokens")
		}
		totalTerms += len(terms)
		for term := range frequencies(terms) {
			c.DF[term]++
		}
	}
	c.AvgLen = float64(totalTerms) / float64(len(docs))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return corpus{}, err
	}
	for base := 0; base < len(docs); base += segmentDocs {
		end := min(base+segmentDocs, len(docs))
		s := segment{
			Name: fmt.Sprintf("segment-%06d.bin", base), Base: base, Count: end - base,
			PageDocs: pageDocs, Postings: make(map[string]part),
		}
		f, err := os.OpenFile(filepath.Join(dir, s.Name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return corpus{}, err
		}
		lists := make(map[string][]posting)
		for i, d := range docs[base:end] {
			for term, tf := range frequencies(tokenize(d.Text)) {
				lists[term] = append(lists[term], posting{id: i, tf: tf})
			}
		}
		terms := make([]string, 0, len(lists))
		for term := range lists {
			terms = append(terms, term)
		}
		sort.Strings(terms)
		for _, term := range terms {
			var encoded []byte
			prev := -1
			for _, p := range lists[term] {
				encoded = binary.AppendUvarint(encoded, uint64(p.id-prev))
				encoded = binary.AppendUvarint(encoded, uint64(p.tf))
				prev = p.id
			}
			// Varints are smaller for short lists; long lists benefit from zlib.
			s.Postings[term], err = writePart(f, encoded, len(encoded) >= 64)
			if err != nil {
				f.Close()
				return corpus{}, err
			}
		}
		for start := base; start < end; start += pageDocs {
			stop := min(start+pageDocs, end)
			var buf []byte
			if packed {
				buf = encodePackedColumns(docs[start:stop])
			} else {
				buf = make([]byte, 0, (stop-start)*6)
				for _, d := range docs[start:stop] {
					buf = append(buf, d.Tenant)
					buf = binary.LittleEndian.AppendUint16(buf, d.Group)
					buf = binary.LittleEndian.AppendUint16(buf, d.Duration)
					buf = append(buf, byte(len(tokenize(d.Text))))
				}
			}
			p, err := writePart(f, buf, !packed)
			if err != nil {
				f.Close()
				return corpus{}, err
			}
			p.Packed = packed
			s.Columns = append(s.Columns, p)
		}
		s.Sources, err = writeSourcePages(f, docs[base:end], pageDocs)
		if err != nil {
			f.Close()
			return corpus{}, err
		}
		if err := f.Close(); err != nil {
			return corpus{}, err
		}
		s.SHA256, err = fileSHA256(filepath.Join(dir, s.Name))
		if err != nil {
			return corpus{}, err
		}
		c.Segs = append(c.Segs, s)
	}
	if err := writeCatalog(dir, c); err != nil {
		return corpus{}, err
	}
	return c, nil
}

func writeSourcePages(f *os.File, docs []document, pageDocs int) ([]part, error) {
	var pages []part
	for start := 0; start < len(docs); start += pageDocs {
		var buf []byte
		for _, d := range docs[start:min(start+pageDocs, len(docs))] {
			buf = binary.LittleEndian.AppendUint32(buf, uint32(len(d.Text)))
			buf = append(buf, d.Text...)
		}
		p, err := writePart(f, buf, true)
		if err != nil {
			return nil, err
		}
		pages = append(pages, p)
	}
	return pages, nil
}

func writeCatalog(dir string, c corpus) error {
	manifest, err := marshalCorpus(c)
	if err != nil {
		return err
	}
	var packed bytes.Buffer
	zw := zlib.NewWriter(&packed)
	if _, err := zw.Write(manifest); err != nil {
		return err
	}
	if err := zw.Close(); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.bin.z"), packed.Bytes(), 0o600); err != nil {
		return err
	}
	return nil
}

func load(dir string) (corpus, error) {
	data, err := os.ReadFile(filepath.Join(dir, "manifest.bin.z"))
	if err != nil {
		return corpus{}, err
	}
	zr, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return corpus{}, err
	}
	defer zr.Close()
	data, err = io.ReadAll(zr)
	if err != nil {
		return corpus{}, err
	}
	c, err := unmarshalCorpus(data)
	if err != nil {
		return corpus{}, err
	}
	if c.Count <= 0 || len(c.Segs) == 0 || c.AvgLen <= 0 {
		return corpus{}, errors.New("invalid catalog")
	}
	return c, nil
}

func writePart(w *os.File, raw []byte, compress bool) (part, error) {
	offset, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		return part{}, err
	}
	payload := raw
	if compress {
		var compressed bytes.Buffer
		zw := zlib.NewWriter(&compressed)
		if _, err := zw.Write(raw); err != nil {
			return part{}, err
		}
		if err := zw.Close(); err != nil {
			return part{}, err
		}
		payload = compressed.Bytes()
	}
	if _, err := w.Write(payload); err != nil {
		return part{}, err
	}
	return part{Offset: offset, Size: int64(len(payload)), CRC32C: crc32.Checksum(payload, crcTable), Zlib: compress}, nil
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func tokenize(text string) []string { return strings.Fields(strings.ToLower(text)) }

func frequencies(words []string) map[string]int {
	out := make(map[string]int, len(words))
	for _, word := range words {
		out[word]++
	}
	return out
}

type rangeSource interface {
	Get(context.Context, string, int64, int64) ([]byte, error)
}

type localSource struct{ dir string }

func (s localSource) Get(_ context.Context, name string, offset, size int64) ([]byte, error) {
	f, err := os.Open(filepath.Join(s.dir, name))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, size)
	_, err = f.ReadAt(buf, offset)
	return buf, err
}

type measuredSource struct {
	src   rangeSource
	calls int
	bytes int64
}

func (s *measuredSource) Get(ctx context.Context, name string, offset, size int64) ([]byte, error) {
	data, err := s.src.Get(ctx, name, offset, size)
	s.calls++
	s.bytes += int64(len(data))
	return data, err
}

func decodePart(payload []byte, p part) ([]byte, error) {
	if int64(len(payload)) != p.Size {
		return nil, io.ErrUnexpectedEOF
	}
	if crc32.Checksum(payload, crcTable) != p.CRC32C {
		return nil, errors.New("range checksum mismatch")
	}
	value := payload
	if p.Zlib {
		zr, err := zlib.NewReader(bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		value, err = io.ReadAll(zr)
		if err != nil {
			return nil, err
		}
	}
	if p.Packed {
		return decodePackedColumns(value)
	}
	return value, nil
}

func readPart(ctx context.Context, src rangeSource, s segment, p part) ([]byte, error) {
	payload, err := src.Get(ctx, s.Name, p.Offset, p.Size)
	if err != nil {
		return nil, err
	}
	return decodePart(payload, p)
}

type mode int

const (
	sparse       mode = iota // fetch only column pages containing matches
	denseColumns             // one column span, then source pages for top K
	densePayload             // one column+source span; fixed path for every query
	coalesced2               // at most two column ranges per segment
	coalesced4               // at most four column ranges per segment
)

type cachedSpan struct {
	start int64
	data  []byte
}

type accessor struct {
	ctx     context.Context
	src     rangeSource
	seg     segment
	mode    mode
	span    []byte
	start   int64
	spans   []cachedSpan
	columns map[int][]byte
	sources map[int][]byte
}

func newAccessor(ctx context.Context, src rangeSource, seg segment, m mode) *accessor {
	return &accessor{ctx: ctx, src: src, seg: seg, mode: m,
		columns: make(map[int][]byte), sources: make(map[int][]byte)}
}

func (a *accessor) ensureSpan() error {
	if a.mode == sparse || a.mode == coalesced2 || a.mode == coalesced4 || a.span != nil {
		return nil
	}
	if len(a.seg.Columns) == 0 {
		return errors.New("segment has no column pages")
	}
	a.start = a.seg.Columns[0].Offset
	last := a.seg.Columns[len(a.seg.Columns)-1]
	if a.mode == densePayload {
		last = a.seg.Sources[len(a.seg.Sources)-1]
	}
	var err error
	a.span, err = a.src.Get(a.ctx, a.seg.Name, a.start, last.Offset+last.Size-a.start)
	return err
}

func (a *accessor) prefetchColumns(needed []bool) error {
	budget := 2
	if a.mode == coalesced4 {
		budget = 4
	}
	type interval struct{ first, last int }
	var intervals []interval
	for page, yes := range needed {
		if !yes {
			continue
		}
		if len(intervals) > 0 && intervals[len(intervals)-1].last+1 == page {
			intervals[len(intervals)-1].last = page
		} else {
			intervals = append(intervals, interval{page, page})
		}
	}
	for len(intervals) > budget {
		joinAt := 0
		smallestGap := int64(math.MaxInt64)
		for i := 0; i+1 < len(intervals); i++ {
			left := a.seg.Columns[intervals[i].last]
			right := a.seg.Columns[intervals[i+1].first]
			gap := right.Offset - left.Offset - left.Size
			if gap < smallestGap {
				smallestGap, joinAt = gap, i
			}
		}
		intervals[joinAt].last = intervals[joinAt+1].last
		intervals = append(intervals[:joinAt+1], intervals[joinAt+2:]...)
	}
	for _, interval := range intervals {
		start := a.seg.Columns[interval.first].Offset
		last := a.seg.Columns[interval.last]
		data, err := a.src.Get(a.ctx, a.seg.Name, start, last.Offset+last.Size-start)
		if err != nil {
			return err
		}
		a.spans = append(a.spans, cachedSpan{start: start, data: data})
	}
	return nil
}

func (a *accessor) page(parts []part, cache map[int][]byte, n int) ([]byte, error) {
	if value, ok := cache[n]; ok {
		return value, nil
	}
	if n < 0 || n >= len(parts) {
		return nil, errors.New("page out of range")
	}
	p := parts[n]
	var value []byte
	var err error
	if a.mode == coalesced2 || a.mode == coalesced4 {
		for _, span := range a.spans {
			if p.Offset >= span.start && p.Offset+p.Size <= span.start+int64(len(span.data)) {
				begin := p.Offset - span.start
				value, err = decodePart(span.data[begin:begin+p.Size], p)
				break
			}
		}
		if value == nil && err == nil {
			value, err = readPart(a.ctx, a.src, a.seg, p)
		}
	} else if a.mode != sparse {
		// The source pages are outside a denseColumns span and fall back to a Range GET.
		if err := a.ensureSpan(); err != nil {
			return nil, err
		}
		if p.Offset >= a.start && p.Offset+p.Size <= a.start+int64(len(a.span)) {
			begin := p.Offset - a.start
			value, err = decodePart(a.span[begin:begin+p.Size], p)
		} else {
			value, err = readPart(a.ctx, a.src, a.seg, p)
		}
	} else {
		value, err = readPart(a.ctx, a.src, a.seg, p)
	}
	if err != nil {
		return nil, err
	}
	cache[n] = value
	return value, nil
}

func (a *accessor) column(local int) (tenant uint8, group, duration uint16, length uint8, err error) {
	page := local / a.seg.PageDocs
	data, err := a.page(a.seg.Columns, a.columns, page)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	offset := (local % a.seg.PageDocs) * 6
	if offset+6 > len(data) {
		return 0, 0, 0, 0, io.ErrUnexpectedEOF
	}
	return data[offset], binary.LittleEndian.Uint16(data[offset+1:]), binary.LittleEndian.Uint16(data[offset+3:]), data[offset+5], nil
}

func (a *accessor) source(local int) (string, error) {
	page := local / a.seg.PageDocs
	data, err := a.page(a.seg.Sources, a.sources, page)
	if err != nil {
		return "", err
	}
	slot := local % a.seg.PageDocs
	for i := 0; i <= slot; i++ {
		if len(data) < 4 {
			return "", io.ErrUnexpectedEOF
		}
		size := int(binary.LittleEndian.Uint32(data))
		data = data[4:]
		if size > len(data) {
			return "", io.ErrUnexpectedEOF
		}
		if i == slot {
			return string(data[:size]), nil
		}
		data = data[size:]
	}
	return "", io.ErrUnexpectedEOF
}

type iterator struct {
	data     []byte
	pos      int
	id       int
	tf       int
	ok       bool
	covering bool
	tenant   uint8
	group    uint16
	duration uint16
	length   uint8
}

func (it *iterator) next() error {
	if it.pos == len(it.data) {
		it.ok = false
		return nil
	}
	delta, n := binary.Uvarint(it.data[it.pos:])
	if n <= 0 || delta == 0 {
		return errors.New("invalid posting delta")
	}
	it.pos += n
	tf, n := binary.Uvarint(it.data[it.pos:])
	if n <= 0 || tf == 0 {
		return errors.New("invalid term frequency")
	}
	it.pos += n
	it.id += int(delta)
	it.tf = int(tf)
	if it.covering {
		if len(it.data)-it.pos < 6 {
			return io.ErrUnexpectedEOF
		}
		it.tenant = it.data[it.pos]
		it.group = binary.LittleEndian.Uint16(it.data[it.pos+1:])
		it.duration = binary.LittleEndian.Uint16(it.data[it.pos+3:])
		it.length = it.data[it.pos+5]
		it.pos += 6
	}
	it.ok = true
	return nil
}

func neededColumnPages(seg segment, its []iterator, all bool) ([]bool, error) {
	probe := make([]iterator, len(its))
	for i := range its {
		if its[i].data == nil {
			continue
		}
		probe[i] = iterator{data: its[i].data, id: -1}
		if err := probe[i].next(); err != nil {
			return nil, err
		}
	}
	needed := make([]bool, len(seg.Columns))
	for {
		minID := math.MaxInt
		for i := range probe {
			if probe[i].ok && probe[i].id < minID {
				minID = probe[i].id
			}
		}
		if minID == math.MaxInt {
			return needed, nil
		}
		matched := 0
		for i := range probe {
			if probe[i].ok && probe[i].id == minID {
				matched++
				if err := probe[i].next(); err != nil {
					return nil, err
				}
			}
		}
		if !all || matched == len(probe) {
			if minID < 0 || minID >= seg.Count {
				return nil, errors.New("posting document ID out of bounds")
			}
			needed[minID/seg.PageDocs] = true
		}
	}
}

type query struct {
	Terms  []string
	All    bool
	Tenant int // -1 means every tenant
	K      int
}

func normalizeQueryTerms(raw []string) ([]string, error) {
	if len(raw) == 0 {
		return nil, errors.New("query has no terms")
	}
	seen := make(map[string]bool, len(raw))
	terms := make([]string, 0, len(raw))
	for _, value := range raw {
		term := strings.ToLower(value)
		if term == "" {
			return nil, errors.New("empty query term")
		}
		if !seen[term] {
			seen[term] = true
			terms = append(terms, term)
		}
	}
	return terms, nil
}

type aggregate struct {
	Count int    `json:"count"`
	Sum   uint64 `json:"sum"`
}

type hit struct {
	ID    int     `json:"id"`
	Score float64 `json:"score"`
	Text  string  `json:"text"`
}

type result struct {
	Count  int                  `json:"count"`
	Sum    uint64               `json:"sum"`
	Groups map[uint16]aggregate `json:"groups"`
	Hits   []hit                `json:"hits"`
}

type topHeap []hit

func (h topHeap) Len() int { return len(h) }
func (h topHeap) Less(i, j int) bool {
	if h[i].Score != h[j].Score {
		return h[i].Score < h[j].Score
	}
	return h[i].ID > h[j].ID
}
func (h topHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *topHeap) Push(x any)   { *h = append(*h, x.(hit)) }
func (h *topHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

func better(a, b hit) bool {
	return a.Score > b.Score || (a.Score == b.Score && a.ID < b.ID)
}

func addTop(h *topHeap, k int, candidate hit) {
	if k == 0 {
		return
	}
	if h.Len() < k {
		heap.Push(h, candidate)
		return
	}
	if better(candidate, (*h)[0]) {
		(*h)[0] = candidate
		heap.Fix(h, 0)
	}
}

func run(ctx context.Context, src rangeSource, c corpus, q query, m mode) (result, error) {
	if q.K < 0 || q.K > 1000 || len(q.Terms) == 0 || q.Tenant < -1 || q.Tenant > 255 {
		return result{}, errors.New("invalid query")
	}
	terms, err := normalizeQueryTerms(q.Terms)
	if err != nil {
		return result{}, err
	}
	idf := make([]float64, len(terms))
	for i, term := range terms {
		df := c.DF[term]
		if df == 0 && q.All {
			return result{Groups: map[uint16]aggregate{}}, nil
		}
		idf[i] = math.Log1p((float64(c.Count-df) + 0.5) / (float64(df) + 0.5))
	}
	out := result{Groups: make(map[uint16]aggregate)}
	var best topHeap
	accessors := make([]*accessor, len(c.Segs))
	for segmentIndex, seg := range c.Segs {
		covering := len(seg.Columns) == 0
		its := make([]iterator, len(terms))
		if q.All {
			missing := false
			for _, term := range terms {
				if _, ok := seg.Postings[term]; !ok {
					missing = true
					break
				}
			}
			if missing {
				continue
			}
		}
		any := false
		for i, term := range terms {
			p, ok := seg.Postings[term]
			if !ok {
				continue
			}
			raw, err := readPart(ctx, src, seg, p)
			if err != nil {
				return result{}, err
			}
			its[i] = iterator{data: raw, id: -1, covering: covering}
			if err := its[i].next(); err != nil {
				return result{}, err
			}
			any = any || its[i].ok
		}
		if !any {
			continue
		}
		accessMode := m
		if covering {
			accessMode = sparse
		}
		a := newAccessor(ctx, src, seg, accessMode)
		accessors[segmentIndex] = a
		if !covering && (m == coalesced2 || m == coalesced4) {
			needed, err := neededColumnPages(seg, its, q.All)
			if err != nil {
				return result{}, err
			}
			if err := a.prefetchColumns(needed); err != nil {
				return result{}, err
			}
		}
		contributions := make([]int, len(its))
		for {
			if err := ctx.Err(); err != nil {
				return result{}, err
			}
			minID := math.MaxInt
			for i := range its {
				if its[i].ok && its[i].id < minID {
					minID = its[i].id
				}
			}
			if minID == math.MaxInt {
				break
			}
			matched := 0
			var tenant uint8
			var group, duration uint16
			var length uint8
			haveFields := false
			clear(contributions)
			for i := range its {
				if its[i].ok && its[i].id == minID {
					if covering {
						if haveFields && (tenant != its[i].tenant || group != its[i].group || duration != its[i].duration || length != its[i].length) {
							return result{}, errors.New("inconsistent covering fields")
						}
						tenant, group, duration, length = its[i].tenant, its[i].group, its[i].duration, its[i].length
						haveFields = true
					}
					contributions[i] = its[i].tf
					matched++
					if err := its[i].next(); err != nil {
						return result{}, err
					}
				}
			}
			if q.All && matched != len(terms) {
				continue
			}
			if minID < 0 || minID >= seg.Count {
				return result{}, errors.New("posting document ID out of bounds")
			}
			if !covering {
				var err error
				tenant, group, duration, length, err = a.column(minID)
				if err != nil {
					return result{}, err
				}
			}
			if q.Tenant >= 0 && int(tenant) != q.Tenant {
				continue
			}
			out.Count++
			out.Sum += uint64(duration)
			agg := out.Groups[group]
			agg.Count++
			agg.Sum += uint64(duration)
			out.Groups[group] = agg
			if q.K > 0 {
				score := 0.0
				for i, tf := range contributions {
					if tf > 0 {
						t := float64(tf)
						score += idf[i] * t * 2.2 / (t + 1.2*(0.25+0.75*float64(length)/c.AvgLen))
					}
				}
				addTop(&best, q.K, hit{ID: seg.Base + minID, Score: score})
			}
		}
	}
	out.Hits = make([]hit, len(best))
	copy(out.Hits, best)
	sort.Slice(out.Hits, func(i, j int) bool { return better(out.Hits[i], out.Hits[j]) })
	for i := range out.Hits {
		id := out.Hits[i].ID
		segmentIndex := sort.Search(len(c.Segs), func(j int) bool {
			return c.Segs[j].Base+c.Segs[j].Count > id
		})
		if segmentIndex == len(c.Segs) || accessors[segmentIndex] == nil {
			return result{}, errors.New("missing result segment")
		}
		text, err := accessors[segmentIndex].source(id - c.Segs[segmentIndex].Base)
		if err != nil {
			return result{}, err
		}
		out.Hits[i].Text = text
	}
	return out, nil
}
