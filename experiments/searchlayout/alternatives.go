package main

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
)

// A covering posting repeats the four small aggregate/scoring fields for each
// distinct term in a document, eliminating column reads at query time.
func buildCovering(dir string, docs []document, base corpus) (corpus, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return corpus{}, err
	}
	c := corpus{Count: base.Count, AvgLen: base.AvgLen, DF: base.DF}
	for _, original := range base.Segs {
		s := segment{Name: original.Name, Base: original.Base, Count: original.Count,
			PageDocs: original.PageDocs, Postings: make(map[string]part)}
		f, err := os.OpenFile(filepath.Join(dir, s.Name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return corpus{}, err
		}
		lists := make(map[string][]byte)
		previous := make(map[string]int)
		for local, d := range docs[s.Base : s.Base+s.Count] {
			words := tokenize(d.Text)
			for term, tf := range frequencies(words) {
				prev, ok := previous[term]
				if !ok {
					prev = -1
				}
				data := binary.AppendUvarint(lists[term], uint64(local-prev))
				data = binary.AppendUvarint(data, uint64(tf))
				data = append(data, d.Tenant)
				data = binary.LittleEndian.AppendUint16(data, d.Group)
				data = binary.LittleEndian.AppendUint16(data, d.Duration)
				lists[term] = append(data, byte(len(words)))
				previous[term] = local
			}
		}
		terms := make([]string, 0, len(lists))
		for term := range lists {
			terms = append(terms, term)
		}
		slices.Sort(terms)
		for _, term := range terms {
			data := lists[term]
			s.Postings[term], err = writePart(f, data, len(data) >= 64)
			if err != nil {
				f.Close()
				return corpus{}, err
			}
		}
		s.Sources, err = writeSourcePages(f, docs[s.Base:s.Base+s.Count], s.PageDocs)
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
	return load(dir)
}

// The row-only control removes the posting area from identical baseline pages.
// It therefore has a real persisted format and identical source/column codecs.
func buildRowOnly(dir, baselineDir string, base corpus) (corpus, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return corpus{}, err
	}
	c := corpus{Count: base.Count, AvgLen: base.AvgLen, DF: base.DF}
	for _, original := range base.Segs {
		data, err := os.ReadFile(filepath.Join(baselineDir, original.Name))
		if err != nil {
			return corpus{}, err
		}
		start := original.Columns[0].Offset
		if start > int64(len(data)) {
			return corpus{}, io.ErrUnexpectedEOF
		}
		s := segment{Name: original.Name, Base: original.Base, Count: original.Count,
			PageDocs: original.PageDocs, Postings: make(map[string]part)}
		for _, p := range original.Columns {
			p.Offset -= start
			s.Columns = append(s.Columns, p)
		}
		for _, p := range original.Sources {
			p.Offset -= start
			s.Sources = append(s.Sources, p)
		}
		path := filepath.Join(dir, s.Name)
		if err := os.WriteFile(path, data[start:], 0o600); err != nil {
			return corpus{}, err
		}
		s.SHA256, err = fileSHA256(path)
		if err != nil {
			return corpus{}, err
		}
		c.Segs = append(c.Segs, s)
	}
	if err := writeCatalog(dir, c); err != nil {
		return corpus{}, err
	}
	return load(dir)
}

func storedSize(dir string, c corpus) (int64, error) {
	var size int64
	for _, s := range c.Segs {
		info, err := os.Stat(filepath.Join(dir, s.Name))
		if err != nil {
			return 0, err
		}
		size += info.Size()
	}
	info, err := os.Stat(filepath.Join(dir, "manifest.bin.z"))
	if err != nil {
		return 0, err
	}
	return size + info.Size(), nil
}

func runRowScan(ctx context.Context, src rangeSource, c corpus, q query) (result, error) {
	if len(q.Terms) == 0 || q.K < 0 || q.K > 1000 || q.Tenant < -1 || q.Tenant > 255 {
		return result{}, errors.New("invalid row-scan query")
	}
	terms, err := normalizeQueryTerms(q.Terms)
	if err != nil {
		return result{}, err
	}
	idf := make([]float64, len(terms))
	for i, term := range terms {
		df := c.DF[term]
		idf[i] = math.Log1p((float64(c.Count-df) + 0.5) / (float64(df) + 0.5))
	}
	out := result{Groups: make(map[uint16]aggregate)}
	var best topHeap
	for _, s := range c.Segs {
		if len(s.Columns) == 0 {
			return result{}, errors.New("row scan needs columns")
		}
		a := newAccessor(ctx, src, s, densePayload)
		if err := a.ensureSpan(); err != nil {
			return result{}, err
		}
		for page := range s.Columns {
			if err := ctx.Err(); err != nil {
				return result{}, err
			}
			cols, err := a.page(s.Columns, a.columns, page)
			if err != nil {
				return result{}, err
			}
			rows, err := a.page(s.Sources, a.sources, page)
			if err != nil {
				return result{}, err
			}
			pos := 0
			for slot := 0; slot < min(s.PageDocs, s.Count-page*s.PageDocs); slot++ {
				if pos+4 > len(rows) || (slot+1)*6 > len(cols) {
					return result{}, io.ErrUnexpectedEOF
				}
				length := int(binary.LittleEndian.Uint32(rows[pos:]))
				pos += 4
				if length > len(rows)-pos {
					return result{}, io.ErrUnexpectedEOF
				}
				message := string(rows[pos : pos+length])
				pos += length
				col := cols[slot*6:]
				if q.Tenant >= 0 && int(col[0]) != q.Tenant {
					continue
				}
				words := tokenize(message)
				freq := frequencies(words)
				matched := 0
				for _, term := range terms {
					if freq[term] != 0 {
						matched++
					}
				}
				if matched == 0 || q.All && matched != len(terms) {
					continue
				}
				group := binary.LittleEndian.Uint16(col[1:])
				duration := binary.LittleEndian.Uint16(col[3:])
				out.Count++
				out.Sum += uint64(duration)
				agg := out.Groups[group]
				agg.Count++
				agg.Sum += uint64(duration)
				out.Groups[group] = agg
				if q.K > 0 {
					score := 0.0
					for i, term := range terms {
						if freq[term] > 0 {
							t := float64(freq[term])
							score += idf[i] * t * 2.2 / (t + 1.2*(0.25+0.75*float64(len(words))/c.AvgLen))
						}
					}
					addTop(&best, q.K, hit{ID: s.Base + page*s.PageDocs + slot, Score: score, Text: message})
				}
			}
		}
	}
	out.Hits = append(out.Hits, best...)
	slices.SortFunc(out.Hits, func(a, b hit) int {
		if better(a, b) {
			return -1
		}
		if better(b, a) {
			return 1
		}
		return 0
	})
	return out, nil
}
