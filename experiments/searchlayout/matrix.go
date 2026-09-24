package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
)

func matrixDocuments(n, vocabulary int, profile string) ([]document, error) {
	docs := generated(n, vocabulary)
	mix := func(i int) uint64 {
		x := uint64(i+1) * 0x9e3779b97f4a7c15
		x ^= x >> 30
		x *= 0xbf58476d1ce4e5b9
		x ^= x >> 27
		x *= 0x94d049bb133111eb
		return x ^ (x >> 31)
	}
	switch profile {
	case "baseline":
	case "noisy":
		for i := range docs {
			x := mix(i)
			docs[i].Text += fmt.Sprintf(" trace%016x span%016x", x, x^0xa5a5a5a5a5a5a5a5)
			docs[i].Group = uint16(x % 2048)
			docs[i].Duration = uint16((x >> 16) % 1000)
		}
	case "wide_fields":
		for i := range docs {
			x := mix(i)
			docs[i].Tenant = uint8(x >> 40)
			docs[i].Group = uint16(x)
			docs[i].Duration = uint16(x >> 16)
		}
	case "clustered":
		rank := func(s string) int {
			if strings.Contains(s, " fatal") {
				return 0
			}
			if strings.Contains(s, " timeout") {
				return 1
			}
			return 2
		}
		slices.SortStableFunc(docs, func(a, b document) int { return rank(a.Text) - rank(b.Text) })
	default:
		return nil, fmt.Errorf("unknown profile %q", profile)
	}
	return docs, nil
}

func executeMatrix(n, vocabulary, segmentDocs, pageDocs, repetitions int, profile, minioEndpoint, minioBucket string) error {
	docs, err := matrixDocuments(n, vocabulary, profile)
	if err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "eventglass-layout-matrix-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	baseDir := filepath.Join(dir, "shared")
	if _, err := build(baseDir, docs, segmentDocs, pageDocs); err != nil {
		return err
	}
	base, err := load(baseDir)
	if err != nil {
		return err
	}
	packedDir := filepath.Join(dir, "packed_shared")
	if _, err := buildPacked(packedDir, docs, segmentDocs, pageDocs); err != nil {
		return err
	}
	packed, err := load(packedDir)
	if err != nil {
		return err
	}
	hybridDir := filepath.Join(dir, "hybrid")
	if _, err := buildHybrid(hybridDir, docs, segmentDocs, pageDocs); err != nil {
		return err
	}
	hybrid, err := load(hybridDir)
	if err != nil {
		return err
	}
	coverDir := filepath.Join(dir, "covering")
	cover, err := buildCovering(coverDir, docs, base)
	if err != nil {
		return err
	}
	rowDir := filepath.Join(dir, "row")
	row, err := buildRowOnly(rowDir, baseDir, base)
	if err != nil {
		return err
	}
	rawPath := filepath.Join(dir, "raw.jsonl.z")
	rawSize, err := writeRaw(rawPath, docs)
	if err != nil {
		return err
	}
	type layout struct {
		name string
		dir  string
		c    corpus
	}
	layouts := []layout{{"shared", baseDir, base}, {"packed_shared", packedDir, packed}, {"hybrid", hybridDir, hybrid}, {"covering", coverDir, cover}, {"row_only", rowDir, row}}
	storage := make(map[string]any)
	catalogSizes := make(map[string]int64)
	for _, layout := range layouts {
		size, err := storedSize(layout.dir, layout.c)
		if err != nil {
			return err
		}
		storage[layout.name] = size
		catalog, err := os.Stat(filepath.Join(layout.dir, "manifest.bin.z"))
		if err != nil {
			return err
		}
		catalogSizes[layout.name] = catalog.Size()
	}
	minioSources := make(map[string]s3RangeSource)
	if minioEndpoint != "" {
		client, err := newMinIOClient(context.Background(), minioEndpoint, minioBucket)
		if err != nil {
			return err
		}
		for _, layout := range layouts {
			prefix := layout.name + "/"
			if err := uploadSegments(context.Background(), client, minioBucket, prefix, layout.dir, layout.c); err != nil {
				return err
			}
			minioSources[layout.name] = s3RangeSource{client: client, bucket: minioBucket, prefix: prefix}
		}
	}
	cases := map[string]query{
		"fatal":                       {Terms: []string{"fatal"}, Tenant: -1, K: 10},
		"timeout":                     {Terms: []string{"timeout"}, Tenant: -1, K: 10},
		"request":                     {Terms: []string{"request"}, Tenant: -1, K: 10},
		"tag7":                        {Terms: []string{"tag7"}, Tenant: -1, K: 10},
		"timeout_and_tag7":            {Terms: []string{"timeout", "tag7"}, All: true, Tenant: -1, K: 10},
		"fatal_or_timeout":            {Terms: []string{"fatal", "timeout"}, Tenant: -1, K: 10},
		"service_and_request_tenant3": {Terms: []string{"service", "request"}, All: true, Tenant: 3, K: 10},
	}
	order := []string{"fatal", "timeout", "request", "tag7", "timeout_and_tag7", "fatal_or_timeout", "service_and_request_tenant3"}
	if profile == "noisy" {
		for _, term := range tokenize(docs[0].Text) {
			if strings.HasPrefix(term, "trace") {
				cases["unique_trace"] = query{Terms: []string{term}, Tenant: -1, K: 10}
				order = append(order, "unique_trace")
				break
			}
		}
	}
	queryResults := make(map[string]any)
	for _, name := range order {
		q := cases[name]
		oracle, control, err := measure(repetitions, func() (result, int, int64, error) {
			value, err := scanRaw(rawPath, base, q)
			return value, 1, rawSize, err
		})
		if err != nil {
			return err
		}
		rowResults := map[string]any{"count": oracle.Count, "groups": len(oracle.Groups), "sum": oracle.Sum, "json_scan": control}
		for _, layout := range layouts {
			queryLayout := func(src rangeSource) (result, error) {
				if layout.name == "row_only" {
					return runRowScan(context.Background(), src, layout.c, q)
				}
				return run(context.Background(), src, layout.c, q, denseColumns)
			}
			value, m, err := measure(repetitions, func() (result, int, int64, error) {
				src := &measuredSource{src: localSource{dir: layout.dir}}
				answer, err := queryLayout(src)
				return answer, src.calls, src.bytes, err
			})
			if err != nil {
				return fmt.Errorf("%s/%s: %w", profile, layout.name, err)
			}
			if !equivalent(value, oracle) {
				return fmt.Errorf("%s/%s differs from full-scan oracle", profile, layout.name)
			}
			rowResults[layout.name] = m
			if remote, ok := minioSources[layout.name]; ok {
				value, m, err := measure(repetitions, func() (result, int, int64, error) {
					src := &measuredSource{src: remote}
					answer, err := queryLayout(src)
					return answer, src.calls, src.bytes, err
				})
				if err != nil || !equivalent(value, oracle) {
					return fmt.Errorf("%s/%s MinIO: %v equivalent=%v", profile, layout.name, err, equivalent(value, oracle))
				}
				rowResults[layout.name+"_minio"] = m
			}
		}
		queryResults[name] = rowResults
	}
	report := map[string]any{
		"platform": runtime.GOOS + "/" + runtime.GOARCH, "go": runtime.Version(),
		"profile": profile, "documents": n, "vocabulary_argument": vocabulary,
		"indexed_terms": len(base.DF),
		"segments":      len(base.Segs), "page_documents": pageDocs, "repetitions": repetitions,
		"raw_zlib_bytes": rawSize, "layouts": storage, "catalog_bytes": catalogSizes, "queries": queryResults,
	}
	return json.NewEncoder(os.Stdout).Encode(report)
}
