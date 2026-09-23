package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// httpRangeSource exercises the same byte-range contract used by S3 GET.
// Authentication is intentionally outside this local-only experiment.
type httpRangeSource struct {
	base   string
	client *http.Client
}

func (s httpRangeSource) Get(ctx context.Context, name string, offset, size int64) ([]byte, error) {
	if offset < 0 || size <= 0 {
		return nil, fmt.Errorf("invalid range")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(s.base, "/")+"/"+url.PathEscape(name), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+size-1))
	client := s.client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		return nil, fmt.Errorf("range response status %d", resp.StatusCode)
	}
	prefix := fmt.Sprintf("bytes %d-%d/", offset, offset+size-1)
	if !strings.HasPrefix(resp.Header.Get("Content-Range"), prefix) {
		return nil, fmt.Errorf("invalid Content-Range %q", resp.Header.Get("Content-Range"))
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, size+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) != size {
		return nil, io.ErrUnexpectedEOF
	}
	return body, nil
}
