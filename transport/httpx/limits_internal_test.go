package httpx

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nostosx/servex/errorx"
)

func TestServerNativeLimitOptions(t *testing.T) {
	type settings struct {
		ReadTimeout       time.Duration
		ReadHeaderTimeout time.Duration
		WriteTimeout      time.Duration
		IdleTimeout       time.Duration
		MaxHeaderBytes    int
	}
	for _, tc := range []struct {
		name string
		opts []ServerOption
		want settings
	}{
		{name: "native defaults"},
		{
			name: "configured",
			opts: []ServerOption{
				WithReadTimeout(11 * time.Second), WithReadHeaderTimeout(12 * time.Second),
				WithWriteTimeout(13 * time.Second), WithIdleTimeout(14 * time.Second), WithMaxHeaderBytes(12345),
			},
			want: settings{ReadTimeout: 11 * time.Second, ReadHeaderTimeout: 12 * time.Second,
				WriteTimeout: 13 * time.Second, IdleTimeout: 14 * time.Second, MaxHeaderBytes: 12345},
		},
		{
			name: "negative native values",
			opts: []ServerOption{WithReadTimeout(-1), WithReadHeaderTimeout(-2), WithWriteTimeout(-3), WithIdleTimeout(-4), WithMaxHeaderBytes(-5)},
			want: settings{ReadTimeout: -1, ReadHeaderTimeout: -2, WriteTimeout: -3, IdleTimeout: -4, MaxHeaderBytes: -5},
		},
		{
			name: "last option wins",
			opts: []ServerOption{WithReadTimeout(time.Second), WithReadTimeout(0), WithReadHeaderTimeout(time.Second), WithReadHeaderTimeout(0),
				WithWriteTimeout(time.Second), WithWriteTimeout(0), WithIdleTimeout(time.Second), WithIdleTimeout(0), WithMaxHeaderBytes(1024), WithMaxHeaderBytes(0)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := NewServer(tc.opts...)
			acceptErr := errors.New("test listener finished")
			if s.srv != nil {
				t.Fatal("native server created before Start")
			}
			s.listen = func(context.Context, string, string) (net.Listener, error) {
				return failingListener{err: acceptErr}, nil
			}
			if err := s.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			got := s.srv
			if got.ReadTimeout != tc.want.ReadTimeout || got.ReadHeaderTimeout != tc.want.ReadHeaderTimeout ||
				got.WriteTimeout != tc.want.WriteTimeout || got.IdleTimeout != tc.want.IdleTimeout || got.MaxHeaderBytes != tc.want.MaxHeaderBytes {
				t.Errorf("native settings: read=%v header=%v write=%v idle=%v max-header=%d; want read=%v header=%v write=%v idle=%v max-header=%d",
					got.ReadTimeout, got.ReadHeaderTimeout, got.WriteTimeout, got.IdleTimeout, got.MaxHeaderBytes,
					tc.want.ReadTimeout, tc.want.ReadHeaderTimeout, tc.want.WriteTimeout, tc.want.IdleTimeout, tc.want.MaxHeaderBytes)
			}
			if err := <-s.Err(); !errors.Is(err, acceptErr) {
				t.Errorf("serve error = %v, want %v", err, acceptErr)
			}
			if err := s.Stop(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// limitPipeListener lets the native server process connections on net.Pipe,
// whose deadlines run on the synctest clock.
type limitPipeListener struct {
	connections chan net.Conn
	done        chan struct{}
	once        sync.Once
}

func (l *limitPipeListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.connections:
		return conn, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}
func (l *limitPipeListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}
func (*limitPipeListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8000}
}

func startLimitPipeServer(t *testing.T, s *Server) net.Conn {
	t.Helper()
	listener := &limitPipeListener{connections: make(chan net.Conn, 1), done: make(chan struct{})}
	s.listen = func(context.Context, string, string) (net.Listener, error) { return listener, nil }
	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	client, server := net.Pipe()
	listener.connections <- server
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
		if err := s.Stop(context.Background()); err != nil {
			t.Error(err)
		}
		if err := <-s.Err(); err != nil {
			t.Errorf("serve error after shutdown: %v", err)
		}
	})
	return client
}

func TestNativeMaxHeaderBytesRejectsBeforeHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := NewServer(WithMaxHeaderBytes(1024))
		s.HandleHTTP("GET /", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			t.Error("oversized headers reached handler")
		}))
		conn := startLimitPipeServer(t, s)
		// net/http permits a small read-buffer allowance beyond the limit.
		request := "GET / HTTP/1.1\r\nHost: example.test\r\nX-Large: " + strings.Repeat("x", 16<<10) + "\r\n\r\n"
		go func() { _, _ = io.WriteString(conn, request) }()
		response, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusRequestHeaderFieldsTooLarge {
			t.Errorf("status=%d, want 431", response.StatusCode)
		}
	})
}

func TestNativeReadHeaderTimeout(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []ServerOption
	}{
		{name: "explicit", opts: []ServerOption{WithReadHeaderTimeout(time.Second)}},
		{name: "read timeout fallback", opts: []ServerOption{WithReadTimeout(time.Second)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := NewServer(tc.opts...)
				s.HandleHTTP("GET /", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
					t.Error("incomplete headers reached handler")
				}))
				conn := startLimitPipeServer(t, s)
				if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: example.test\r\n"); err != nil {
					t.Fatal(err)
				}
				synctest.Sleep(2 * time.Second)
				if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
					t.Errorf("read after header timeout=%v, want EOF", err)
				}
			})
		})
	}
}

func TestNativeReadTimeoutIncludesBody(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := NewServer(WithReadTimeout(time.Second))
		Route[struct{}](s, "POST /", func(context.Context, any) (any, error) {
			t.Error("incomplete request body reached endpoint")
			return nil, nil
		})
		conn := startLimitPipeServer(t, s)
		if _, err := io.WriteString(conn, "POST / HTTP/1.1\r\nHost: example.test\r\nContent-Length: 3\r\n\r\n{"); err != nil {
			t.Fatal(err)
		}
		synctest.Sleep(2 * time.Second)
		response, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusBadRequest {
			t.Errorf("body read timeout status=%d, want 400", response.StatusCode)
		}
	})
}

func TestNativeWriteTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := NewServer(WithWriteTimeout(time.Second))
		s.Handle("GET /", func(context.Context, any) (any, error) {
			time.Sleep(2 * time.Second)
			return "too late", nil
		}, nil)
		conn := startLimitPipeServer(t, s)
		if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: example.test\r\n\r\n"); err != nil {
			t.Fatal(err)
		}
		synctest.Sleep(3 * time.Second)
		response, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if response != nil {
			if closeErr := response.Body.Close(); closeErr != nil {
				t.Error(closeErr)
			}
		}
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("response after write timeout=%v, want unexpected EOF", err)
		}
	})
}

func TestNativeIdleTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := NewServer(WithIdleTimeout(time.Second))
		s.Handle("GET /", func(context.Context, any) (any, error) { return nil, nil }, nil)
		conn := startLimitPipeServer(t, s)
		if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: example.test\r\n\r\n"); err != nil {
			t.Fatal(err)
		}
		response, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := response.Body.Close(); err != nil {
			t.Fatal(err)
		}
		synctest.Sleep(2 * time.Second)
		if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
			t.Errorf("read after idle timeout=%v, want EOF", err)
		}
	})
}

func TestBodyLimitCauseClassification(t *testing.T) {
	cause := &http.MaxBytesError{Limit: 8}
	err := invalidBody(fmt.Errorf("parser: %w", cause))
	if !errors.Is(err, cause) || errorx.CodeOf(err) != errorx.InvalidArgument || errorx.From(err).Reason != "BODY_TOO_LARGE" || errorx.IsRetryable(err) {
		t.Errorf("body limit classification=%v", err)
	}
	if _, ok := errors.AsType[*bodyTooLargeCause](err); !ok {
		t.Error("body limit marker missing")
	}
}

func TestServerStopWaitsForInFlightRequests(t *testing.T) {
	for _, expired := range []bool{false, true} {
		name := "graceful"
		if expired {
			name = "expired context"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				defer unblock()
				entered := make(chan struct{})
				s := NewServer()
				s.HandleHTTP("GET /slow", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					close(entered)
					<-release
					w.WriteHeader(http.StatusOK)
				}))
				conn := startLimitPipeServer(t, s)
				type result struct {
					status int
					err    error
				}
				response := make(chan result, 1)
				go func() {
					if _, err := io.WriteString(conn, "GET /slow HTTP/1.1\r\nHost: example.test\r\nConnection: close\r\n\r\n"); err != nil {
						response <- result{err: err}
						return
					}
					resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
					if err != nil {
						response <- result{err: err}
						return
					}
					resp.Body.Close()
					response <- result{status: resp.StatusCode}
				}()
				<-entered
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if expired {
					cancel()
					if err := s.Stop(ctx); !errors.Is(err, context.Canceled) {
						t.Errorf("Stop(expired) = %v, want Canceled", err)
					}
					unblock()
					if got := <-response; got.err == nil {
						t.Errorf("request survived forced shutdown: status %d", got.status)
					}
					return
				}
				stopped := make(chan error, 1)
				go func() { stopped <- s.Stop(ctx) }()
				synctest.Wait()
				select {
				case err := <-stopped:
					t.Fatalf("Stop returned while the request was blocked: %v", err)
				default:
				}
				unblock()
				if got := <-response; got.err != nil || got.status != http.StatusOK {
					t.Errorf("in-flight response = %+v, want status 200 and nil error", got)
				}
				if err := <-stopped; err != nil {
					t.Errorf("Stop = %v", err)
				}
			})
		})
	}
}
