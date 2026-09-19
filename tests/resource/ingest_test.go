package resource_test

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/api"
	"github.com/chawanghyeon/eventglass/internal/ingest"
	"github.com/chawanghyeon/eventglass/internal/resource"
	"github.com/chawanghyeon/eventglass/internal/sdk"
	"github.com/chawanghyeon/eventglass/internal/testkit"
)

type zeroReader struct{}

func (zeroReader) Read(buffer []byte) (int, error) {
	for index := range buffer {
		buffer[index] = 0
	}
	return len(buffer), nil
}

func TestWireLimitAndAdmissionRejectBeforeAllocationTransfer(t *testing.T) {
	sink := &testkit.MemorySink{}
	admission := resource.NewBudget(api.MaxWireBytes - 1)
	handler, err := api.NewIngestHandler(api.Config{
		TenantID: 1, ProjectID: 1, PublicKey: "key", Sink: sink, IngressBudget: admission,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/1/envelope/?sentry_key=key", strings.NewReader("{}"))
	request.ContentLength = api.MaxWireBytes
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusTooManyRequests || admission.Used() != 0 || len(sink.Batches()) != 0 {
		t.Fatalf("admission status=%d used=%d batches=%d", response.Code, admission.Used(), len(sink.Batches()))
	}

	handler, err = api.NewIngestHandler(api.Config{TenantID: 1, ProjectID: 1, PublicKey: "key", Sink: sink})
	if err != nil {
		t.Fatal(err)
	}
	request = httptest.NewRequest(http.MethodPost, "/api/1/envelope/?sentry_key=key", io.LimitReader(zeroReader{}, api.MaxWireBytes+1))
	request.ContentLength = api.MaxWireBytes + 1
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge || len(sink.Batches()) != 0 {
		t.Fatalf("wire limit status=%d batches=%d", response.Code, len(sink.Batches()))
	}
	t.Logf("I5 admission wire_limit_bytes=%d rejected_before_transfer=true reservation_after=%d", api.MaxWireBytes, admission.Used())
}

func TestMaximumLegalCanonicalEventIsMeasured(t *testing.T) {
	acceptanceID := "00000000-0000-4000-8000-000000000001"
	normalizes := func(messageBytes int) error {
		_, err := ingest.NormalizeEnvelope(sdk.Envelope{Items: []sdk.Item{{Ordinal: 0, Type: "event", Value: map[string]any{
			"message": strings.Repeat("x", messageBytes),
		}}}}, ingest.NormalizeOptions{TenantID: 1, ProjectID: 1, AcceptanceID: acceptanceID, ArrivalTime: time.Unix(1, 0)})
		return err
	}
	low, high := 0, ingest.MaxCanonicalBytes
	for low < high {
		middle := low + (high-low+1)/2
		if err := normalizes(middle); err == nil {
			low = middle
		} else if errors.Is(err, ingest.ErrLimitExceeded) {
			high = middle - 1
		} else {
			t.Fatal(err)
		}
		runtime.GC()
	}
	if err := normalizes(low); err != nil {
		t.Fatalf("largest legal message=%d: %v", low, err)
	}
	if err := normalizes(low + 1); !errors.Is(err, ingest.ErrLimitExceeded) {
		t.Fatalf("message above legal maximum=%d err=%v", low+1, err)
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	t.Logf("I5 canonical_limit_bytes=%d maximum_message_bytes=%d heap_sys_bytes=%d total_alloc_bytes=%d", ingest.MaxCanonicalBytes, low, memory.HeapSys, memory.TotalAlloc)
}
