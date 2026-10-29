package httpx

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func nilReplyEndpoint(context.Context, any) (any, error) { return nil, nil }

// Embedding only ResponseWriter deliberately hides recorder.Flush.
type responsePlainWriter struct{ http.ResponseWriter }

type responseUnwrapper struct{ http.ResponseWriter }

func (w *responseUnwrapper) Unwrap() http.ResponseWriter { return w.ResponseWriter }

type responseFlushErrorWriter struct {
	http.ResponseWriter
	err    error
	called bool
}

func (w *responseFlushErrorWriter) FlushError() error {
	w.called = true
	w.WriteHeader(http.StatusOK)
	return w.err
}

type responseHijackWriter struct {
	http.ResponseWriter
	conn   net.Conn
	err    error
	called bool
}

func (w *responseHijackWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.called = true
	return w.conn, nil, w.err
}

type responseControlWriter struct {
	http.ResponseWriter
	readDeadline  time.Time
	writeDeadline time.Time
	fullDuplex    bool
}

func (w *responseControlWriter) SetReadDeadline(deadline time.Time) error {
	w.readDeadline = deadline
	return nil
}

func (w *responseControlWriter) SetWriteDeadline(deadline time.Time) error {
	w.writeDeadline = deadline
	return nil
}

func (w *responseControlWriter) EnableFullDuplex() error {
	w.fullDuplex = true
	return nil
}

type responseFailWriteWriter struct{ http.ResponseWriter }

func (*responseFailWriteWriter) Write([]byte) (int, error) {
	return 0, io.ErrClosedPipe
}

func TestResponseWriterOptionalOperations(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		wrap func(http.ResponseWriter) http.ResponseWriter
	}{
		{"plain", func(w http.ResponseWriter) http.ResponseWriter { return &responsePlainWriter{w} }},
		{"unwrap plain", func(w http.ResponseWriter) http.ResponseWriter {
			return &responseUnwrapper{&responsePlainWriter{w}}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := NewServer(WithLogger(testLogger(slog.New(slog.DiscardHandler))))
			s.Handle("GET /", nilReplyEndpoint, nil, WithResponseEncoder(func(w http.ResponseWriter, _ *http.Request, _ any) error {
				if _, ok := w.(http.Flusher); ok {
					t.Error("plain writer claims http.Flusher")
				}
				if _, ok := w.(interface{ FlushError() error }); ok {
					t.Error("plain writer claims FlushError")
				}
				if _, ok := w.(http.Hijacker); ok {
					t.Error("plain writer claims http.Hijacker")
				}
				if _, ok := w.(http.Pusher); ok {
					t.Error("plain writer claims http.Pusher")
				}
				controller := http.NewResponseController(w)
				if err := controller.Flush(); !errors.Is(err, http.ErrNotSupported) {
					t.Errorf("Flush = %v", err)
				}
				if _, _, err := controller.Hijack(); !errors.Is(err, http.ErrNotSupported) {
					t.Errorf("Hijack = %v", err)
				}
				return errors.New("unsupported operations did not commit")
			}))
			recorder := httptest.NewRecorder()
			s.ServeHTTP(tt.wrap(recorder), httptest.NewRequest(http.MethodGet, "/", nil))
			if recorder.Code != http.StatusInternalServerError || !strings.Contains(recorder.Body.String(), `"reason":"ENCODE_REPLY"`) {
				t.Errorf("response %d %q", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestResponseWriterControllerUnwrap(t *testing.T) {
	t.Parallel()
	recorder := httptest.NewRecorder()
	underlying := &responseControlWriter{ResponseWriter: recorder}
	s := NewServer()
	s.Handle("GET /", nilReplyEndpoint, nil, WithResponseEncoder(func(w http.ResponseWriter, _ *http.Request, _ any) error {
		unwrapper, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok || unwrapper.Unwrap() == nil {
			t.Fatal("encoder writer does not unwrap")
		}
		controller := http.NewResponseController(w)
		deadline := time.Unix(12345, 0)
		if err := controller.SetReadDeadline(deadline); err != nil {
			return err
		}
		if err := controller.SetWriteDeadline(deadline); err != nil {
			return err
		}
		if err := controller.EnableFullDuplex(); err != nil {
			return err
		}
		w.WriteHeader(http.StatusAccepted)
		return nil
	}))
	s.ServeHTTP(&responseUnwrapper{underlying}, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusAccepted || !underlying.readDeadline.Equal(time.Unix(12345, 0)) || !underlying.writeDeadline.Equal(underlying.readDeadline) || !underlying.fullDuplex {
		t.Errorf("response %d, controls %+v", recorder.Code, underlying)
	}
}

func TestResponseWriterFlushErrorCommitment(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		err  error
	}{
		{"success", nil},
		{"transport error", io.ErrClosedPipe},
	} {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			underlying := &responseFlushErrorWriter{ResponseWriter: recorder, err: tt.err}
			s := NewServer(WithLogger(testLogger(slog.New(slog.DiscardHandler))))
			s.Handle("GET /", nilReplyEndpoint, nil, WithResponseEncoder(func(w http.ResponseWriter, _ *http.Request, _ any) error {
				if err := http.NewResponseController(w).Flush(); err != tt.err {
					t.Errorf("Flush = %v, want %v", err, tt.err)
				}
				return errors.New("after flush")
			}))
			s.ServeHTTP(&responseUnwrapper{underlying}, httptest.NewRequest(http.MethodGet, "/", nil))
			if !underlying.called || recorder.Code != http.StatusOK || recorder.Body.Len() != 0 {
				t.Errorf("flush called %v, response %d %q", underlying.called, recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestResponseWriterHijackCommitment(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		flush bool
		err   error
	}{
		{"success", false, nil},
		{"success with flush support", true, nil},
		{"failure", false, io.ErrClosedPipe},
		{"failure with flush support", true, io.ErrClosedPipe},
	} {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			var base http.ResponseWriter = &responsePlainWriter{recorder}
			if tt.flush {
				base = recorder
			}
			underlying := &responseHijackWriter{ResponseWriter: base, err: tt.err}
			// An unwrap chain exposes flush beneath the hijacker as well.
			var writer http.ResponseWriter = underlying
			if tt.flush {
				writer = &responseHijackUnwrapper{underlying}
			}
			s := NewServer(WithLogger(testLogger(slog.New(slog.DiscardHandler))))
			s.Handle("GET /", nilReplyEndpoint, nil, WithResponseEncoder(func(w http.ResponseWriter, _ *http.Request, _ any) error {
				if _, _, err := http.NewResponseController(w).Hijack(); err != tt.err {
					t.Errorf("Hijack = %v, want %v", err, tt.err)
				}
				return errors.New("after hijack")
			}))
			s.ServeHTTP(writer, httptest.NewRequest(http.MethodGet, "/", nil))
			if !underlying.called || recorder.Code != http.StatusOK || recorder.Body.Len() != 0 {
				t.Errorf("hijack called %v, response %d %q", underlying.called, recorder.Code, recorder.Body.String())
			}
		})
	}
}

type responseHijackUnwrapper struct{ *responseHijackWriter }

func (w *responseHijackUnwrapper) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func TestResponseWriterWriteErrorCommitment(t *testing.T) {
	t.Parallel()
	recorder := httptest.NewRecorder()
	s := NewServer(WithLogger(testLogger(slog.New(slog.DiscardHandler))))
	s.Handle("GET /", nilReplyEndpoint, nil, WithResponseEncoder(func(w http.ResponseWriter, _ *http.Request, _ any) error {
		_, err := w.Write([]byte("output"))
		return err
	}))
	s.ServeHTTP(&responseFailWriteWriter{recorder}, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusOK || recorder.Body.Len() != 0 {
		t.Errorf("response %d %q", recorder.Code, recorder.Body.String())
	}
}
