package storage

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

const DefaultBlockSize int64 = 1 << 20

var ErrObjectNotFound = errors.New("object not found")

type RangeStore interface {
	ReadRange(ctx context.Context, objectKey string, offset, length int64) ([]byte, error)
}

type ObjectManifest struct {
	Capability  string
	ObjectKey   string
	Size        int64
	SHA256      string
	BlockSize   int64
	BlockSHA256 []string
}

func (m ObjectManifest) validate() error {
	if m.Capability == "" || strings.Contains(m.Capability, "/") {
		return errors.New("capability must be one opaque path segment")
	}
	if m.ObjectKey == "" || m.Size < 0 || m.BlockSize <= 0 {
		return errors.New("invalid object identity, size, or block size")
	}
	expectedBlocks := int((m.Size + m.BlockSize - 1) / m.BlockSize)
	if len(m.BlockSHA256) != expectedBlocks {
		return fmt.Errorf("block hash count %d does not match size %d", len(m.BlockSHA256), m.Size)
	}
	if _, err := decodeHash(m.SHA256); err != nil {
		return fmt.Errorf("object checksum: %w", err)
	}
	for i, hash := range m.BlockSHA256 {
		if _, err := decodeHash(hash); err != nil {
			return fmt.Errorf("block %d checksum: %w", i, err)
		}
	}
	return nil
}

type Gateway struct {
	store        RangeStore
	byCapability map[string]ObjectManifest
}

func NewGateway(store RangeStore, manifests []ObjectManifest) (*Gateway, error) {
	gateway := &Gateway{store: store, byCapability: make(map[string]ObjectManifest, len(manifests))}
	for _, manifest := range manifests {
		if err := manifest.validate(); err != nil {
			return nil, err
		}
		if _, exists := gateway.byCapability[manifest.Capability]; exists {
			return nil, fmt.Errorf("duplicate capability %q", manifest.Capability)
		}
		gateway.byCapability[manifest.Capability] = manifest
	}
	return gateway, nil
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	capability := strings.TrimPrefix(r.URL.EscapedPath(), "/objects/")
	if capability == r.URL.EscapedPath() || capability == "" || strings.Contains(capability, "/") {
		http.NotFound(w, r)
		return
	}
	manifest, ok := g.byCapability[capability]
	if !ok {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", `"sha256:`+manifest.SHA256+`"`)
	start, end, partial, err := parseRange(r.Header.Get("Range"), manifest.Size)
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", manifest.Size))
		http.Error(w, "range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	length := end - start
	if partial {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end-1, manifest.Size))
	}
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
		if partial {
			w.WriteHeader(http.StatusPartialContent)
		}
		return
	}
	data, err := g.readVerified(r.Context(), manifest, start, end)
	if err != nil {
		http.Error(w, "verified object read failed", http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	if partial {
		w.WriteHeader(http.StatusPartialContent)
	}
	_, _ = w.Write(data)
}

func (g *Gateway) readVerified(ctx context.Context, manifest ObjectManifest, start, end int64) ([]byte, error) {
	if start == end {
		return []byte{}, nil
	}
	firstBlock := start / manifest.BlockSize
	lastBlock := (end - 1) / manifest.BlockSize
	expandedStart := firstBlock * manifest.BlockSize
	expandedEnd := (lastBlock + 1) * manifest.BlockSize
	if expandedEnd > manifest.Size {
		expandedEnd = manifest.Size
	}
	data, err := g.store.ReadRange(ctx, manifest.ObjectKey, expandedStart, expandedEnd-expandedStart)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != expandedEnd-expandedStart {
		return nil, io.ErrUnexpectedEOF
	}
	for block := firstBlock; block <= lastBlock; block++ {
		blockStart := block * manifest.BlockSize
		blockEnd := blockStart + manifest.BlockSize
		if blockEnd > manifest.Size {
			blockEnd = manifest.Size
		}
		localStart := blockStart - expandedStart
		localEnd := blockEnd - expandedStart
		actual := sha256.Sum256(data[localStart:localEnd])
		expected, _ := decodeHash(manifest.BlockSHA256[block])
		if subtle.ConstantTimeCompare(actual[:], expected) != 1 {
			return nil, fmt.Errorf("block %d checksum mismatch", block)
		}
	}
	if expandedStart == 0 && expandedEnd == manifest.Size {
		actual := sha256.Sum256(data)
		expected, _ := decodeHash(manifest.SHA256)
		if subtle.ConstantTimeCompare(actual[:], expected) != 1 {
			return nil, errors.New("object checksum mismatch")
		}
	}
	return append([]byte(nil), data[start-expandedStart:end-expandedStart]...), nil
}

func parseRange(value string, size int64) (start, end int64, partial bool, err error) {
	if value == "" {
		return 0, size, false, nil
	}
	if !strings.HasPrefix(value, "bytes=") || strings.Contains(value, ",") || size == 0 {
		return 0, 0, false, errors.New("unsupported range")
	}
	parts := strings.Split(strings.TrimPrefix(value, "bytes="), "-")
	if len(parts) != 2 {
		return 0, 0, false, errors.New("malformed range")
	}
	if parts[0] == "" {
		suffix, parseErr := strconv.ParseInt(parts[1], 10, 64)
		if parseErr != nil || suffix <= 0 {
			return 0, 0, false, errors.New("invalid suffix range")
		}
		if suffix > size {
			suffix = size
		}
		return size - suffix, size, true, nil
	}
	start, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, false, errors.New("invalid range start")
	}
	end = size
	if parts[1] != "" {
		inclusiveEnd, parseErr := strconv.ParseInt(parts[1], 10, 64)
		if parseErr != nil || inclusiveEnd < start {
			return 0, 0, false, errors.New("invalid range end")
		}
		if inclusiveEnd < size-1 {
			end = inclusiveEnd + 1
		}
	}
	return start, end, true, nil
}

func decodeHash(value string) ([]byte, error) {
	if len(value) != sha256.Size*2 {
		return nil, errors.New("SHA-256 must contain 64 hexadecimal characters")
	}
	return hex.DecodeString(value)
}
