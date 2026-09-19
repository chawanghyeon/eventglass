package api

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/query"
)

type liveTransportFixture struct {
	PublicQueryService
	run func(context.Context, func(query.LiveEvent) error) error
}

func (f liveTransportFixture) Live(ctx context.Context, _ control.SessionPrincipal, _ [32]byte, _ query.PublicLiveRequest, _ string, emit func(query.LiveEvent) error) error {
	return f.run(ctx, emit)
}

func liveTransportServer(t *testing.T, run func(context.Context, func(query.LiveEvent) error) error, slots int) *httptest.Server {
	t.Helper()
	h := &ManagementHandler{config: ManagementConfig{Queries: liveTransportFixture{run: run}}, liveSlots: make(chan struct{}, slots)}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.streamLive(w, r, control.SessionPrincipal{}, [32]byte{}, query.PublicLiveRequest{}, "")
	}))
	t.Cleanup(s.Close)
	return s
}

func TestLiveHTTPRevokeBeforeAndAfterHeaders(t *testing.T) {
	for _, started := range []bool{false, true} {
		t.Run(map[bool]string{false: "before", true: "after"}[started], func(t *testing.T) {
			s := liveTransportServer(t, func(ctx context.Context, emit func(query.LiveEvent) error) error {
				if !started {
					return control.ErrForbidden
				}
				if err := emit(query.LiveEvent{Type: "checkpoint", ID: "resume", Data: struct{}{}}); err != nil {
					return err
				}
				return emit(query.LiveEvent{Type: "error", Data: liveControlData{Code: "forbidden"}})
			}, 32)
			r, err := s.Client().Get(s.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Body.Close()
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatal(err)
			}
			if started {
				if r.StatusCode != 200 || !strings.HasPrefix(r.Header.Get("Content-Type"), "text/event-stream") || !strings.Contains(string(body), `"code":"forbidden"`) || strings.Contains(string(body), "event: rows") {
					t.Fatalf("status=%d body=%s", r.StatusCode, body)
				}
			} else if r.StatusCode != 403 {
				t.Fatalf("status=%d body=%s", r.StatusCode, body)
			}
		})
	}
}

func TestLiveHTTPConnectionAdmissionAndDisconnectRelease(t *testing.T) {
	done := make(chan struct{}, 32)
	s := liveTransportServer(t, func(ctx context.Context, emit func(query.LiveEvent) error) error {
		defer func() { done <- struct{}{} }()
		if err := emit(query.LiveEvent{Type: "heartbeat", Data: struct{}{}}); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	}, 32)
	var bodies []io.ReadCloser
	defer func() {
		for _, b := range bodies {
			b.Close()
		}
	}()
	for range 32 {
		r, err := s.Client().Get(s.URL)
		if err != nil {
			t.Fatal(err)
		}
		if r.StatusCode != 200 {
			t.Fatal(r.Status)
		}
		bodies = append(bodies, r.Body)
	}
	r, err := s.Client().Get(s.URL)
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	if r.StatusCode != 429 {
		t.Fatal(r.Status)
	}
	bodies[0].Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("disconnect did not release producer")
	}
	r, err = s.Client().Get(s.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		t.Fatal(r.Status)
	}
}

func TestLiveHTTPIdleLongerThanWriteDeadline(t *testing.T) {
	s := liveTransportServer(t, func(ctx context.Context, emit func(query.LiveEvent) error) error {
		if err := emit(query.LiveEvent{Type: "checkpoint", ID: "one", Data: struct{}{}}); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5100 * time.Millisecond):
		}
		return emit(query.LiveEvent{Type: "heartbeat", Data: struct{}{}})
	}, 32)
	r, err := s.Client().Get(s.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	scan := bufio.NewScanner(r.Body)
	events := 0
	for scan.Scan() {
		if strings.HasPrefix(scan.Text(), "event:") {
			events++
		}
	}
	if scan.Err() != nil || events != 2 {
		t.Fatalf("events=%d err=%v", events, scan.Err())
	}
}

func TestLiveHTTPSlowReaderTerminatesProducer(t *testing.T) {
	done := make(chan error, 1)
	s := liveTransportServer(t, func(ctx context.Context, emit func(query.LiveEvent) error) error {
		data := strings.Repeat("x", 250<<10)
		for {
			err := emit(query.LiveEvent{Type: "rows", Data: data})
			if err != nil {
				done <- err
				return err
			}
			if ctx.Err() != nil {
				done <- ctx.Err()
				return ctx.Err()
			}
		}
	}, 32)
	r, err := s.Client().Get(s.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	// Deliberately do not consume the body; bounded socket writes must time out.
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("missing slow-reader failure")
		}
	case <-time.After(8 * time.Second):
		t.Fatal("producer retained after write deadline")
	}
}
