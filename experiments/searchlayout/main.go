package main

import (
	"compress/zlib"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"time"
)

func generated(n, vocabulary int) []document {
	docs := make([]document, n)
	for i := range docs {
		term := "request"
		if i%10007 == 0 {
			term = "fatal"
		} else if i%101 == 0 {
			term = "timeout"
		}
		words := []string{"service", "event", "completed", "item", fmt.Sprintf("tag%d", i%vocabulary)}
		for range 1 + i%3 {
			words = append(words, term)
		}
		docs[i] = document{
			Tenant: uint8(i % 4), Group: uint16(i % 2048),
			Duration: uint16((i * 17) % 1000), Text: strings.Join(words, " "),
		}
	}
	return docs
}

func writeRaw(path string, docs []document) (int64, error) {
	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	zw := zlib.NewWriter(f)
	enc := json.NewEncoder(zw)
	for _, d := range docs {
		if err := enc.Encode(d); err != nil {
			zw.Close()
			f.Close()
			return 0, err
		}
	}
	if err := zw.Close(); err != nil {
		f.Close()
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// scanRaw is the no-index control: one compressed object, full exact scan.
func scanRaw(path string, c corpus, q query) (result, error) {
	f, err := os.Open(path)
	if err != nil {
		return result{}, err
	}
	defer f.Close()
	zr, err := zlib.NewReader(f)
	if err != nil {
		return result{}, err
	}
	defer zr.Close()
	decoder := json.NewDecoder(zr)
	out := result{Groups: make(map[uint16]aggregate)}
	var best topHeap
	idf := make([]float64, len(q.Terms))
	for i, term := range q.Terms {
		df := c.DF[term]
		idf[i] = math.Log1p((float64(c.Count-df) + 0.5) / (float64(df) + 0.5))
	}
	for id := 0; ; id++ {
		var d document
		err := decoder.Decode(&d)
		if err == io.EOF {
			break
		}
		if err != nil {
			return result{}, err
		}
		if q.Tenant >= 0 && int(d.Tenant) != q.Tenant {
			continue
		}
		words := tokenize(d.Text)
		freq := frequencies(words)
		matched := 0
		for _, term := range q.Terms {
			if freq[term] > 0 {
				matched++
			}
		}
		if matched == 0 || (q.All && matched != len(q.Terms)) {
			continue
		}
		out.Count++
		out.Sum += uint64(d.Duration)
		agg := out.Groups[d.Group]
		agg.Count++
		agg.Sum += uint64(d.Duration)
		out.Groups[d.Group] = agg
		if q.K > 0 {
			score := 0.0
			for i, term := range q.Terms {
				if freq[term] > 0 {
					t := float64(freq[term])
					score += idf[i] * t * 2.2 / (t + 1.2*(0.25+0.75*float64(len(words))/c.AvgLen))
				}
			}
			addTop(&best, q.K, hit{ID: id, Score: score, Text: d.Text})
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

type measurement struct {
	Milliseconds float64 `json:"median_ms"`
	Allocated    uint64  `json:"median_alloc_bytes"`
	GETs         int     `json:"range_gets"`
	Bytes        int64   `json:"range_bytes"`
}

func measure(repetitions int, fn func() (result, int, int64, error)) (result, measurement, error) {
	var times []float64
	var allocations []uint64
	var answer result
	var calls int
	var bytes int64
	for range repetitions {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		start := time.Now()
		value, n, size, err := fn()
		elapsed := time.Since(start)
		runtime.ReadMemStats(&after)
		if err != nil {
			return result{}, measurement{}, err
		}
		answer, calls, bytes = value, n, size
		times = append(times, float64(elapsed.Microseconds())/1000)
		allocations = append(allocations, after.TotalAlloc-before.TotalAlloc)
	}
	slices.Sort(times)
	slices.Sort(allocations)
	return answer, measurement{
		Milliseconds: times[len(times)/2], Allocated: allocations[len(allocations)/2],
		GETs: calls, Bytes: bytes,
	}, nil
}

func equivalent(a, b result) bool {
	if a.Count != b.Count || a.Sum != b.Sum || !reflect.DeepEqual(a.Groups, b.Groups) || len(a.Hits) != len(b.Hits) {
		return false
	}
	for i := range a.Hits {
		if a.Hits[i].ID != b.Hits[i].ID || a.Hits[i].Text != b.Hits[i].Text || math.Abs(a.Hits[i].Score-b.Hits[i].Score) > 1e-12 {
			return false
		}
	}
	return true
}

func execute() error {
	n := flag.Int("n", 100000, "number of synthetic documents")
	vocabulary := flag.Int("vocabulary", 257, "number of distinct tag terms")
	segmentDocs := flag.Int("segment", 10000, "documents per immutable segment")
	pageDocs := flag.Int("page", 1024, "documents per compressed column/source page")
	repetitions := flag.Int("reps", 5, "timed repetitions per query")
	minioEndpoint := flag.String("minio-endpoint", "", "optional temporary local MinIO endpoint")
	minioBucket := flag.String("minio-bucket", "eventglass-search-probe", "temporary local MinIO bucket")
	flag.Parse()
	if *n < 1 || *vocabulary < 1 || *repetitions < 1 {
		return fmt.Errorf("invalid flags")
	}
	dir, err := os.MkdirTemp("", "eventglass-go-layout-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	docs := generated(*n, *vocabulary)
	buildStart := time.Now()
	_, err = build(dir, docs, *segmentDocs, *pageDocs)
	if err != nil {
		return err
	}
	buildSeconds := time.Since(buildStart).Seconds()
	c, err := load(dir)
	if err != nil {
		return err
	}
	rawPath := filepath.Join(dir, "raw.jsonl.z")
	rawBytes, err := writeRaw(rawPath, docs)
	if err != nil {
		return err
	}
	rawSHA, err := fileSHA256(rawPath)
	if err != nil {
		return err
	}
	var segmentBytes int64
	var postingBytes, columnBytes, sourceBytes int64
	for _, seg := range c.Segs {
		info, err := os.Stat(filepath.Join(dir, seg.Name))
		if err != nil {
			return err
		}
		segmentBytes += info.Size()
		for _, p := range seg.Postings {
			postingBytes += p.Size
		}
		for _, p := range seg.Columns {
			columnBytes += p.Size
		}
		for _, p := range seg.Sources {
			sourceBytes += p.Size
		}
	}
	info, err := os.Stat(filepath.Join(dir, "manifest.bin.z"))
	if err != nil {
		return err
	}
	cases := map[string]query{
		"fatal":                       {Terms: []string{"fatal"}, Tenant: -1, K: 10},
		"timeout":                     {Terms: []string{"timeout"}, Tenant: -1, K: 10},
		"request":                     {Terms: []string{"request"}, Tenant: -1, K: 10},
		"fatal_or_timeout":            {Terms: []string{"fatal", "timeout"}, Tenant: -1, K: 10},
		"service_and_request_tenant3": {Terms: []string{"service", "request"}, All: true, Tenant: 3, K: 10},
	}
	report := map[string]any{
		"platform":  runtime.GOOS + "/" + runtime.GOARCH,
		"go":        runtime.Version(),
		"documents": *n, "vocabulary": *vocabulary, "segments": len(c.Segs),
		"page_documents": *pageDocs, "build_seconds": buildSeconds,
		"raw_zlib_bytes": rawBytes, "raw_sha256": rawSHA, "segment_bytes": segmentBytes,
		"manifest_bytes": info.Size(),
		"posting_bytes":  postingBytes, "column_bytes": columnBytes, "source_bytes": sourceBytes,
		"queries": map[string]any{},
	}
	for _, name := range []string{"fatal", "timeout", "request", "fatal_or_timeout", "service_and_request_tenant3"} {
		q := cases[name]
		oracle, scanMeasurement, err := measure(*repetitions, func() (result, int, int64, error) {
			value, err := scanRaw(rawPath, c, q)
			return value, 1, rawBytes, err
		})
		if err != nil {
			return err
		}
		rows := map[string]any{"count": oracle.Count, "groups": len(oracle.Groups), "sum": oracle.Sum, "scan": scanMeasurement}
		for _, variant := range []struct {
			name string
			mode mode
		}{{"sparse", sparse}, {"dense_columns", denseColumns}, {"dense_payload", densePayload},
			{"coalesced_2", coalesced2}, {"coalesced_4", coalesced4}} {
			value, m, err := measure(*repetitions, func() (result, int, int64, error) {
				src := &measuredSource{src: localSource{dir: dir}}
				value, err := run(context.Background(), src, c, q, variant.mode)
				return value, src.calls, src.bytes, err
			})
			if err != nil {
				return fmt.Errorf("%s/%s: %w", name, variant.name, err)
			}
			if !equivalent(value, oracle) {
				return fmt.Errorf("%s/%s differed from full-scan oracle", name, variant.name)
			}
			rows[variant.name] = m
		}
		report["queries"].(map[string]any)[name] = rows
	}
	if *minioEndpoint != "" {
		m, err := runMinIO(context.Background(), *minioEndpoint, *minioBucket, dir, c, cases["timeout"], *repetitions)
		if err != nil {
			return err
		}
		report["minio_timeout_dense_columns"] = m
	}
	return json.NewEncoder(os.Stdout).Encode(report)
}

func main() {
	if err := execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
