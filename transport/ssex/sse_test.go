package ssex_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nostosx/servex-contrib/transport/ssex"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/transport/sse"
)

func TestLiveStream(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	finished := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		if r.Header.Get("Last-Event-ID") != "previous" {
			t.Error("missing resume header")
		}
		sender, err := new(ssex.Acceptor).Accept(w, r)
		if err != nil {
			t.Error(err)
			return
		}
		if err := sender.Comment("heartbeat\r\nsecond"); err != nil {
			t.Error(err)
			return
		}
		retry := 2 * time.Second
		if err := sender.Send(sse.Event{ID: "42", Type: "token", Data: "first\r\nsecond\rthird", Retry: &retry}); err != nil {
			t.Error(err)
			return
		}
		<-r.Context().Done() // The client must receive before this handler returns.
	}))
	defer server.Close()
	r, err := new(ssex.Client).Open(ctx, server.URL, http.Header{"Last-Event-Id": {"previous"}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	event, err := r.Receive()
	if err != nil || event.ID != "42" || event.Type != "token" || event.Data != "first\nsecond\nthird" || event.Retry == nil || *event.Retry != 2*time.Second {
		t.Fatalf("event=%+v, error=%v", event, err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestReceiveEventSourceRules(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		wire string
		want []sse.Event
	}{
		{"line endings and BOM", "\ufeffid: a\rdata: one\rdata: two\r\rdata: three\r\n\r\n", []sse.Event{{ID: "a", Type: "message", Data: "one\ntwo"}, {ID: "a", Type: "message", Data: "three"}}},
		{"empty data and id reset", "id: old\n\ndata:\n\nid:\ndata: x\n\n", []sse.Event{{ID: "old", Type: "message"}, {Type: "message", Data: "x"}}},
		{"ignore comments unknown and NUL id", ": comment\nid: good\nid: bad\x00id\nunknown: value\nevent: named\ndata:  leading\n\n", []sse.Event{{ID: "good", Type: "named", Data: " leading"}}},
		{"discard incomplete event", "data: unfinished\n", nil},
		{"discard unterminated data", "data: unfinished", nil},
		{"reset event name without data", "event: discarded\n\ndata: okay\n\n", []sse.Event{{Type: "message", Data: "okay"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, tc.wire)
			}))
			defer server.Close()
			r, err := new(ssex.Client).Open(t.Context(), server.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			for _, want := range tc.want {
				got, err := r.Receive()
				if err != nil || got.ID != want.ID || got.Type != want.Type || got.Data != want.Data {
					t.Fatalf("event=%+v, error=%v; want %+v", got, err, want)
				}
			}
			if _, err := r.Receive(); !errors.Is(err, io.EOF) {
				t.Fatalf("final error=%v", err)
			}
		})
	}
}

func TestRetryAndSizeLimits(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		wire  string
		limit int
		code  errorx.Code
	}{
		{"retry", "retry: 12\nretry: -1\nretry: +2\nretry: 999999999999999999999\ndata: okay\n\n", 200, errorx.OK},
		{"long line", "data: " + strings.Repeat("x", 200) + "\n\n", 32, errorx.ResourceExhausted},
		{"long event", strings.Repeat("data: abc\n", 10) + "\n", 32, errorx.ResourceExhausted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, tc.wire)
			}))
			defer server.Close()
			client := &ssex.Client{MaxEventBytes: tc.limit}
			r, err := client.Open(t.Context(), server.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			event, err := r.Receive()
			if errorx.CodeOf(err) != tc.code {
				t.Fatalf("error=%v", err)
			}
			if tc.code == errorx.OK && (event.Retry == nil || *event.Retry != 12*time.Millisecond) {
				t.Fatalf("retry=%v", event.Retry)
			}
		})
	}
}

func TestResumeCursor(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Last-Event-ID") != "previous" {
			t.Error("resume cursor missing")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: inherited\n\nid:\ndata: reset\n\n")
	}))
	defer server.Close()
	r, err := new(ssex.Client).Open(t.Context(), server.URL, http.Header{"Last-Event-Id": {"previous"}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for _, want := range []sse.Event{{ID: "previous", Data: "inherited"}, {Data: "reset"}} {
		event, err := r.Receive()
		if err != nil || event.ID != want.ID || event.Data != want.Data {
			t.Fatalf("event=%+v error=%v", event, err)
		}
	}
}

func TestCancellationAndClose(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"cancel", "close"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if _, err := new(ssex.Acceptor).Accept(w, r); err != nil {
					t.Error(err)
					return
				}
				<-r.Context().Done()
			}))
			defer server.Close()
			r, err := new(ssex.Client).Open(ctx, server.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			done := make(chan error, 1)
			go func() { _, err := r.Receive(); done <- err }()
			if name == "cancel" {
				cancel()
			} else {
				_ = r.Close()
			}
			select {
			case err := <-done:
				if err == nil || (name == "cancel" && !errors.Is(err, context.Canceled)) {
					t.Fatalf("error=%v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Receive did not unblock")
			}
		})
	}
}

func TestInvalidEventsAndResponses(t *testing.T) {
	t.Parallel()
	for _, event := range []sse.Event{{ID: "bad\nid"}, {ID: "bad\x00id"}, {Type: "bad\revent"}, {Data: string([]byte{0xff})}, {Retry: new(-time.Second)}, {Retry: new(time.Nanosecond)}} {
		w := httptest.NewRecorder()
		sender, err := new(ssex.Acceptor).Accept(w, httptest.NewRequest(http.MethodGet, "/", nil))
		if err != nil {
			t.Fatal(err)
		}
		if err := sender.Send(event); errorx.CodeOf(err) != errorx.InvalidArgument {
			t.Fatalf("event=%+v error=%v", event, err)
		}
		if w.Body.Len() != 0 {
			t.Fatal("invalid event committed data")
		}
	}
	for _, tc := range []struct {
		name, media string
		status      int
	}{{"wrong media", "application/json", 200}, {"unauthorized", "text/event-stream", 401}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", tc.media)
				w.WriteHeader(tc.status)
			}))
			defer server.Close()
			if _, err := new(ssex.Client).Open(t.Context(), server.URL, nil); errorx.CodeOf(err) != errorx.FailedPrecondition {
				t.Fatalf("error=%v", err)
			}
		})
	}
}
