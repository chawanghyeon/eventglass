package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

type memoryRangeStore map[string][]byte

func (m memoryRangeStore) ReadRange(_ context.Context, key string, offset, length int64) ([]byte, error) {
	data, ok := m[key]
	if !ok {
		return nil, ErrObjectNotFound
	}
	if offset < 0 || length < 0 || offset+length > int64(len(data)) {
		return nil, fmt.Errorf("invalid read")
	}
	return append([]byte(nil), data[offset:offset+length]...), nil
}

func manifestFor(capability, key string, data []byte, blockSize int64) ObjectManifest {
	whole := sha256.Sum256(data)
	manifest := ObjectManifest{Capability: capability, ObjectKey: key, Size: int64(len(data)), SHA256: fmt.Sprintf("%x", whole), BlockSize: blockSize}
	for start := int64(0); start < int64(len(data)); start += blockSize {
		end := start + blockSize
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		digest := sha256.Sum256(data[start:end])
		manifest.BlockSHA256 = append(manifest.BlockSHA256, fmt.Sprintf("%x", digest))
	}
	return manifest
}

func TestGatewayRangeAndHead(t *testing.T) {
	data := []byte("0123456789abcdefghij")
	gateway, err := NewGateway(memoryRangeStore{"tenant/object": data}, []ObjectManifest{manifestFor("opaque-token", "tenant/object", data, 8)})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name, method, byteRange string
		status                  int
		body                    string
		contentRange            string
	}{
		{"whole", http.MethodGet, "", http.StatusOK, string(data), ""},
		{"cross-block", http.MethodGet, "bytes=6-10", http.StatusPartialContent, "6789a", "bytes 6-10/20"},
		{"suffix", http.MethodGet, "bytes=-4", http.StatusPartialContent, "ghij", "bytes 16-19/20"},
		{"head", http.MethodHead, "bytes=0-7", http.StatusPartialContent, "", "bytes 0-7/20"},
		{"unsatisfiable", http.MethodGet, "bytes=20-30", http.StatusRequestedRangeNotSatisfiable, "range not satisfiable\n", "bytes */20"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, "/objects/opaque-token", nil)
			request.Header.Set("Range", test.byteRange)
			response := httptest.NewRecorder()
			gateway.ServeHTTP(response, request)
			if response.Code != test.status || response.Body.String() != test.body {
				t.Fatalf("status/body = %d/%q, want %d/%q", response.Code, response.Body.String(), test.status, test.body)
			}
			if got := response.Header().Get("Content-Range"); got != test.contentRange {
				t.Fatalf("Content-Range = %q, want %q", got, test.contentRange)
			}
		})
	}
}

func TestGatewayRejectsCorruptLastBlock(t *testing.T) {
	original := []byte("0123456789abcdefghij")
	corrupt := bytes.Clone(original)
	corrupt[len(corrupt)-1] ^= 0xff
	gateway, err := NewGateway(memoryRangeStore{"tenant/object": corrupt}, []ObjectManifest{manifestFor("opaque-token", "tenant/object", original, 8)})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/objects/opaque-token", nil)
	request.Header.Set("Range", "bytes=18-19")
	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, request)
	if response.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadGateway)
	}
}

func TestGatewayDoesNotExposeObjectKeys(t *testing.T) {
	data := []byte("data")
	gateway, err := NewGateway(memoryRangeStore{"private/tenant/object": data}, []ObjectManifest{manifestFor("cap", "private/tenant/object", data, 4)})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/objects/private/tenant/object", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", response.Code)
	}
}
