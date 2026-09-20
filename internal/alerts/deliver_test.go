package alerts

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type staticResolver struct{ addresses []netip.Addr }

func (resolver staticResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	return append([]netip.Addr(nil), resolver.addresses...), nil
}

func TestSenderPreservesBodyAndSignsExactBytes(t *testing.T) {
	body := []byte(`{"version":1,"message":"exact"}`)
	key := []byte("test-signing-value") // pragma: allowlist secret
	now := time.Unix(1_700_000_000, 0).UTC()
	var received []byte
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		received, _ = io.ReadAll(request.Body)
		if request.Header.Get("X-Eventglass-Delivery") != "00000000-0000-4000-8000-000000000001" || request.Header.Get("X-Eventglass-Timestamp") != "1700000000" {
			t.Errorf("delivery headers=%v", request.Header)
		}
		mac := hmac.New(sha256.New, key)
		_, _ = mac.Write([]byte("1700000000."))
		_, _ = mac.Write(body)
		if got, want := request.Header.Get("X-Eventglass-Signature"), "v1="+hex.EncodeToString(mac.Sum(nil)); got != want {
			t.Errorf("signature=%q want=%q", got, want)
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	outcome := (Sender{AllowLoopbackForTests: true, Now: func() time.Time { return now }}).Send(context.Background(), SendRequest{DeliveryID: "00000000-0000-4000-8000-000000000001", URL: server.URL, Body: body, Secret: key}) // pragma: allowlist secret
	if outcome.ErrorCode != "" || outcome.Status != http.StatusNoContent || string(received) != string(body) {
		t.Fatalf("outcome=%+v body=%q", outcome, received)
	}
}

func TestSenderDoesNotFollowRedirectAndClassifiesResponses(t *testing.T) {
	var redirected atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { redirected.Add(1) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", target.URL)
		writer.WriteHeader(http.StatusFound)
	}))
	defer redirect.Close()
	sender := Sender{AllowLoopbackForTests: true}
	outcome := sender.Send(context.Background(), SendRequest{DeliveryID: "d", URL: redirect.URL, Body: []byte(`{}`)})
	if outcome.Retry || outcome.Status != http.StatusFound || redirected.Load() != 0 {
		t.Fatalf("redirect outcome=%+v followed=%d", outcome, redirected.Load())
	}

	retryable := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Retry-After", "7200")
		writer.WriteHeader(http.StatusTooManyRequests)
	}))
	defer retryable.Close()
	outcome = sender.Send(context.Background(), SendRequest{DeliveryID: "d", URL: retryable.URL, Body: []byte(`{}`)})
	if !outcome.Retry || outcome.RetryAfter != time.Hour {
		t.Fatalf("retry outcome=%+v", outcome)
	}

	permanent := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusBadRequest) }))
	defer permanent.Close()
	outcome = sender.Send(context.Background(), SendRequest{DeliveryID: "d", URL: permanent.URL, Body: []byte(`{}`)})
	if outcome.Retry || outcome.ErrorCode != "http_permanent" {
		t.Fatalf("permanent outcome=%+v", outcome)
	}
}

func TestSenderRejectsMixedAndReservedDNSAnswers(t *testing.T) {
	for _, addresses := range [][]netip.Addr{
		{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("127.0.0.1")},
		{netip.MustParseAddr("2606:4700:4700::1111"), netip.MustParseAddr("fd00::1")},
		{netip.MustParseAddr("192.0.2.1")},
		{netip.MustParseAddr("2001:db8::1")},
	} {
		sender := Sender{Resolver: staticResolver{addresses: addresses}}
		if _, err := sender.resolve(context.Background(), "https://hooks.example.invalid/path"); err == nil {
			t.Fatalf("accepted DNS answers %v", addresses)
		}
	}
}

func TestSenderBoundsResponseAndRetryDelay(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = io.CopyN(writer, strings.NewReader(strings.Repeat("x", DeliveryResponseLimit+1)), DeliveryResponseLimit+1)
	}))
	defer server.Close()
	outcome := (Sender{AllowLoopbackForTests: true}).Send(context.Background(), SendRequest{DeliveryID: "d", URL: server.URL, Body: []byte(`{}`)})
	if outcome.ErrorCode != "response_too_large" || outcome.Retry {
		t.Fatalf("large response outcome=%+v", outcome)
	}
	for range 20 {
		delay, err := RetryDelay(1, 3*time.Second)
		if err != nil || delay < 3*time.Second || delay > 5*time.Second {
			t.Fatalf("delay=%s err=%v", delay, err)
		}
	}
}
