package main

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
)

// The catalog stores a vocabulary once. Segment directories refer to term IDs,
// avoiding repeated strings and per-term JSON keys as the vocabulary grows.
var manifestMagic = []byte("EGSL1")

func appendUint(dst []byte, n uint64) []byte { return binary.AppendUvarint(dst, n) }

func appendString(dst []byte, value string) []byte {
	dst = appendUint(dst, uint64(len(value)))
	return append(dst, value...)
}

func appendPart(dst []byte, p part) []byte {
	dst = appendUint(dst, uint64(p.Offset))
	dst = appendUint(dst, uint64(p.Size))
	dst = binary.LittleEndian.AppendUint32(dst, p.CRC32C)
	flag := byte(0)
	if p.Zlib {
		flag |= 1
	}
	if p.Packed {
		flag |= 2
	}
	if p.Bitmap {
		flag |= 4
	}
	return append(dst, flag)
}

func marshalCorpus(c corpus) ([]byte, error) {
	data := append([]byte(nil), manifestMagic...)
	data = appendUint(data, uint64(c.Count))
	data = binary.LittleEndian.AppendUint64(data, math.Float64bits(c.AvgLen))
	terms := make([]string, 0, len(c.DF))
	for term := range c.DF {
		terms = append(terms, term)
	}
	slices.Sort(terms)
	data = appendUint(data, uint64(len(terms)))
	termID := make(map[string]int, len(terms))
	for i, term := range terms {
		termID[term] = i
		data = appendString(data, term)
		data = appendUint(data, uint64(c.DF[term]))
	}
	data = appendUint(data, uint64(len(c.Segs)))
	for _, seg := range c.Segs {
		data = appendString(data, seg.Name)
		data = appendUint(data, uint64(seg.Base))
		data = appendUint(data, uint64(seg.Count))
		data = appendUint(data, uint64(seg.PageDocs))
		hash, err := hex.DecodeString(seg.SHA256)
		if err != nil || len(hash) != 32 {
			return nil, errors.New("invalid segment SHA-256")
		}
		data = append(data, hash...)
		ids := make([]int, 0, len(seg.Postings))
		for term := range seg.Postings {
			id, ok := termID[term]
			if !ok {
				return nil, fmt.Errorf("term %q absent from global vocabulary", term)
			}
			ids = append(ids, id)
		}
		slices.Sort(ids)
		data = appendUint(data, uint64(len(ids)))
		prev := -1
		for _, id := range ids {
			data = appendUint(data, uint64(id-prev))
			data = appendPart(data, seg.Postings[terms[id]])
			prev = id
		}
		data = appendUint(data, uint64(len(seg.Columns)))
		for _, p := range seg.Columns {
			data = appendPart(data, p)
		}
		data = appendUint(data, uint64(len(seg.Sources)))
		for _, p := range seg.Sources {
			data = appendPart(data, p)
		}
	}
	return data, nil
}

type manifestReader struct {
	data []byte
	pos  int
}

func (r *manifestReader) uint() (uint64, error) {
	if r.pos >= len(r.data) {
		return 0, io.ErrUnexpectedEOF
	}
	n, width := binary.Uvarint(r.data[r.pos:])
	if width <= 0 {
		return 0, errors.New("invalid catalog integer")
	}
	r.pos += width
	return n, nil
}

func (r *manifestReader) bytes(n uint64) ([]byte, error) {
	if n > uint64(len(r.data)-r.pos) {
		return nil, io.ErrUnexpectedEOF
	}
	value := r.data[r.pos : r.pos+int(n)]
	r.pos += int(n)
	return value, nil
}

func (r *manifestReader) string() (string, error) {
	size, err := r.uint()
	if err != nil {
		return "", err
	}
	data, err := r.bytes(size)
	return string(data), err
}

func (r *manifestReader) part() (part, error) {
	offset, err := r.uint()
	if err != nil {
		return part{}, err
	}
	size, err := r.uint()
	if err != nil {
		return part{}, err
	}
	if offset > math.MaxInt64 || size > math.MaxInt64 {
		return part{}, errors.New("range overflow")
	}
	raw, err := r.bytes(5)
	if err != nil {
		return part{}, err
	}
	if raw[4] > 7 || raw[4]&6 == 6 {
		return part{}, errors.New("invalid codec flag")
	}
	return part{Offset: int64(offset), Size: int64(size), CRC32C: binary.LittleEndian.Uint32(raw), Zlib: raw[4]&1 != 0, Packed: raw[4]&2 != 0, Bitmap: raw[4]&4 != 0}, nil
}

func unmarshalCorpus(data []byte) (corpus, error) {
	if len(data) < len(manifestMagic) || string(data[:len(manifestMagic)]) != string(manifestMagic) {
		return corpus{}, errors.New("invalid catalog magic")
	}
	r := manifestReader{data: data, pos: len(manifestMagic)}
	count, err := r.uint()
	if err != nil || count == 0 || count > math.MaxInt {
		return corpus{}, errors.New("invalid document count")
	}
	rawAvg, err := r.bytes(8)
	if err != nil {
		return corpus{}, err
	}
	c := corpus{Count: int(count), AvgLen: math.Float64frombits(binary.LittleEndian.Uint64(rawAvg)), DF: make(map[string]int)}
	if math.IsNaN(c.AvgLen) || math.IsInf(c.AvgLen, 0) || c.AvgLen <= 0 {
		return corpus{}, errors.New("invalid average document length")
	}
	vocabularySize, err := r.uint()
	if err != nil || vocabularySize > 1_000_000 {
		return corpus{}, errors.New("invalid vocabulary size")
	}
	terms := make([]string, int(vocabularySize))
	for i := range terms {
		terms[i], err = r.string()
		if err != nil || terms[i] == "" {
			return corpus{}, errors.New("invalid catalog term")
		}
		df, err := r.uint()
		if err != nil || df > count {
			return corpus{}, errors.New("invalid term frequency")
		}
		c.DF[terms[i]] = int(df)
	}
	segmentCount, err := r.uint()
	if err != nil || segmentCount == 0 || segmentCount > 100_000 {
		return corpus{}, errors.New("invalid segment count")
	}
	c.Segs = make([]segment, 0, int(segmentCount))
	for range segmentCount {
		var seg segment
		seg.Name, err = r.string()
		if err != nil || seg.Name == "" || strings.ContainsAny(seg.Name, "/\\") {
			return corpus{}, errors.New("invalid segment name")
		}
		base, err := r.uint()
		if err != nil || base > count {
			return corpus{}, errors.New("invalid segment base")
		}
		n, err := r.uint()
		if err != nil || n == 0 || n > count {
			return corpus{}, errors.New("invalid segment length")
		}
		pageDocs, err := r.uint()
		if err != nil || pageDocs == 0 || pageDocs > 1_000_000 {
			return corpus{}, errors.New("invalid page size")
		}
		seg.Base, seg.Count, seg.PageDocs = int(base), int(n), int(pageDocs)
		hash, err := r.bytes(32)
		if err != nil {
			return corpus{}, err
		}
		seg.SHA256 = hex.EncodeToString(hash)
		numPostings, err := r.uint()
		if err != nil || numPostings > vocabularySize {
			return corpus{}, errors.New("invalid postings count")
		}
		seg.Postings = make(map[string]part, int(numPostings))
		prev := -1
		for range numPostings {
			delta, err := r.uint()
			if err != nil || delta == 0 || delta > vocabularySize {
				return corpus{}, errors.New("invalid term ID")
			}
			id := prev + int(delta)
			if id >= len(terms) {
				return corpus{}, errors.New("term ID out of bounds")
			}
			p, err := r.part()
			if err != nil {
				return corpus{}, err
			}
			seg.Postings[terms[id]] = p
			prev = id
		}
		numColumns, err := r.uint()
		if err != nil || numColumns > 1_000_000 {
			return corpus{}, errors.New("invalid column page count")
		}
		for range numColumns {
			p, err := r.part()
			if err != nil {
				return corpus{}, err
			}
			seg.Columns = append(seg.Columns, p)
		}
		numSources, err := r.uint()
		expectedPages := (int(n) + int(pageDocs) - 1) / int(pageDocs)
		if err != nil || numSources > 1_000_000 || int(numSources) != expectedPages || (numColumns != 0 && numSources != numColumns) || (numColumns == 0 && numPostings == 0) {
			return corpus{}, errors.New("invalid source page count")
		}
		for range numSources {
			p, err := r.part()
			if err != nil {
				return corpus{}, err
			}
			seg.Sources = append(seg.Sources, p)
		}
		c.Segs = append(c.Segs, seg)
	}
	if r.pos != len(r.data) {
		return corpus{}, errors.New("trailing catalog bytes")
	}
	expectedBase := 0
	for _, seg := range c.Segs {
		if seg.Base != expectedBase || (len(seg.Columns) != 0 && len(seg.Columns) != (seg.Count+seg.PageDocs-1)/seg.PageDocs) {
			return corpus{}, errors.New("invalid segment coverage")
		}
		expectedBase += seg.Count
	}
	if expectedBase != c.Count {
		return corpus{}, errors.New("catalog does not cover document count")
	}
	return c, nil
}
