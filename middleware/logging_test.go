package middleware_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	applicationmw "github.com/nostosx/servex-contrib/middleware/application"
	clientmw "github.com/nostosx/servex-contrib/middleware/client"
	endpointmw "github.com/nostosx/servex-contrib/middleware/endpoint"
	"github.com/nostosx/servex-contrib/middleware/observe"
	"github.com/nostosx/servex/application"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/observability/logger"
	"github.com/nostosx/servex/observability/tracing"
)

// captureHandler records every record at or above its minimum level.
type captureHandler struct {
	min     slog.Level
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= h.min }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }

func (h *captureHandler) only(t *testing.T) (slog.Record, map[string]slog.Value) {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.records) != 1 {
		t.Fatalf("got %d records, want 1", len(h.records))
	}
	r := h.records[0]
	m := make(map[string]slog.Value)
	r.Attrs(func(a slog.Attr) bool {
		m[a.Key] = a.Value
		return true
	})
	return r, m
}

func serverCtx(ctx context.Context) context.Context {
	return metadata.NewContext(ctx, metadata.Info{
		Kind: metadata.KindServer, Protocol: "grpc", Operation: "/user.v1.Users/Get",
	})
}

func loggingFactories() map[string]func(logger.Logger, ...observe.LoggingOption) endpoint.Middleware {
	return map[string]func(logger.Logger, ...observe.LoggingOption) endpoint.Middleware{
		"endpoint": endpointmw.Logging,
		"client":   clientmw.Logging,
	}
}

func TestSuccess(t *testing.T) {
	for name, factory := range loggingFactories() {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := &captureHandler{min: slog.LevelDebug}
				ep := factory(testLogger(slog.New(h)))(func(context.Context, any) (any, error) {
					time.Sleep(150 * time.Millisecond)
					return "resp", nil
				})
				resp, err := ep(serverCtx(t.Context()), nil)
				if err != nil || resp != "resp" {
					t.Fatalf("got (%v, %v)", resp, err)
				}
				r, a := h.only(t)
				if r.Level != slog.LevelInfo || r.Message != endpointmw.Message {
					t.Fatalf("record = %v %q", r.Level, r.Message)
				}
				want := map[string]string{
					endpointmw.KeyKind:      "server",
					endpointmw.KeyProtocol:  "grpc",
					endpointmw.KeyOperation: "/user.v1.Users/Get",
					endpointmw.KeyCode:      "OK",
				}
				for k, v := range want {
					if got := a[k].String(); got != v {
						t.Errorf("%s = %q, want %q", k, got, v)
					}
				}
				if d := a[endpointmw.KeyDuration]; d.Kind() != slog.KindDuration || d.Duration() != 150*time.Millisecond {
					t.Errorf("duration = %v", d)
				}
				if _, ok := a["error"]; ok {
					t.Error("error attribute on success")
				}
				if _, ok := a[endpointmw.KeyReason]; ok {
					t.Error("reason attribute on success")
				}
			})
		})
	}
}

func TestFailureLevels(t *testing.T) {
	for name, factory := range loggingFactories() {
		t.Run(name, func(t *testing.T) {
			cases := []struct {
				err   error
				level slog.Level
				code  string
			}{
				{errorx.New(errorx.NotFound, "USER_NOT_FOUND", "no user"), slog.LevelWarn, "NotFound"},
				{errorx.New(errorx.InvalidArgument, "", "bad"), slog.LevelWarn, "InvalidArgument"},
				{errorx.New(errorx.Unauthenticated, "", ""), slog.LevelWarn, "Unauthenticated"},
				{context.Canceled, slog.LevelWarn, "Canceled"},
				{errorx.New(errorx.Internal, "PANIC", "x"), slog.LevelError, "Internal"},
				{errorx.New(errorx.Unavailable, "", ""), slog.LevelError, "Unavailable"},
				{context.DeadlineExceeded, slog.LevelError, "DeadlineExceeded"},
				{errors.New("plain"), slog.LevelError, "Unknown"},
			}
			for _, tc := range cases {
				t.Run(tc.code, func(t *testing.T) {
					h := &captureHandler{min: slog.LevelDebug}
					ep := factory(testLogger(slog.New(h)))(func(context.Context, any) (any, error) { return nil, tc.err })
					if _, err := ep(serverCtx(t.Context()), nil); err != tc.err {
						t.Fatalf("err = %v, want unchanged %v", err, tc.err)
					}
					r, a := h.only(t)
					if r.Level != tc.level {
						t.Fatalf("level = %v, want %v", r.Level, tc.level)
					}
					if a[endpointmw.KeyCode].String() != tc.code {
						t.Fatalf("code = %v", a[endpointmw.KeyCode])
					}
					if _, ok := a["error"]; ok {
						t.Fatal("raw error text was logged")
					}
					if reason := errorx.From(tc.err).Reason; reason != "" && a[endpointmw.KeyReason].String() != reason {
						t.Fatalf("reason = %v", a[endpointmw.KeyReason])
					}
				})
			}
		})
	}
}

func TestDefaultLevelUnknownCode(t *testing.T) {
	if l := observe.Level(errorx.Code(99)); l != logger.LevelError {
		t.Fatalf("level = %v", l)
	}
}

func TestCustomLevelAndDisabled(t *testing.T) {
	for name, factory := range loggingFactories() {
		t.Run(name, func(t *testing.T) {
			h := &captureHandler{min: slog.LevelInfo}
			mw := factory(testLogger(slog.New(h)), nil, clientmw.WithLogLevel(nil), clientmw.WithLogLevel(func(code errorx.Code) logger.Level {
				if code == errorx.OK {
					return logger.LevelDebug // below the handler's minimum
				}
				return logger.LevelError
			}))
			if _, err := mw(func(context.Context, any) (any, error) { return nil, nil })(t.Context(), nil); err != nil {
				t.Fatal(err)
			}
			if len(h.records) != 0 {
				t.Fatalf("disabled level logged %d records", len(h.records))
			}
			_, _ = mw(func(context.Context, any) (any, error) { return nil, errorx.New(errorx.NotFound, "", "") })(t.Context(), nil)
			r, a := h.only(t)
			if r.Level != slog.LevelError {
				t.Fatalf("level = %v", r.Level)
			}
			if a[endpointmw.KeyKind].String() != "" || a[endpointmw.KeyOperation].String() != "" {
				t.Fatalf("missing info should log empty values: %v", a)
			}
		})
	}
}

func TestNilLoggerDisablesLogging(t *testing.T) {
	for name, factory := range loggingFactories() {
		t.Run(name, func(t *testing.T) {
			called := false
			_, err := factory(nil)(func(context.Context, any) (any, error) { called = true; return "ok", nil })(t.Context(), nil)
			if !called || err != nil {
				t.Fatalf("nil logger changed execution: %v", err)
			}
		})
	}

}

type call func(context.Context, any) (any, error)

func scopes() map[string]func(logger.Logger, call) call {
	return map[string]func(logger.Logger, call) call{
		"endpoint": func(log logger.Logger, next call) call {
			return call(endpointmw.Logging(log)(func(ctx context.Context, req any) (any, error) {
				return next(ctx, req)
			}))
		},
		"client": func(log logger.Logger, next call) call {
			return call(clientmw.Logging(log)(func(ctx context.Context, req any) (any, error) {
				return next(ctx, req)
			}))
		},
		"application": func(log logger.Logger, next call) call {
			return application.Decorate(application.Metadata{
				Kind: application.KindCall, Operation: "order.inspect",
			}, application.Func[any, any](next), applicationmw.Logging[any, any](log)).Handle
		},
	}
}

type correlationSpan struct{ traceID, spanID string }

func (correlationSpan) End()                     {}
func (correlationSpan) SetAttribute(string, any) {}
func (correlationSpan) RecordError(error)        {}
func (s correlationSpan) TraceID() string        { return s.traceID }
func (s correlationSpan) SpanID() string         { return s.spanID }

const (
	traceID = "0123456789abcdef0123456789abcdef"
	spanID  = "0123456789abcdef"
)

func TestSafeOutcomeAcrossScopes(t *testing.T) {
	cases := []struct {
		name              string
		err               error
		code              string
		reason            string
		retryable, remote bool
		level             slog.Level
	}{
		{name: "success", code: "OK", level: slog.LevelInfo},
		{name: "business", err: errorx.New(errorx.NotFound, "ORDER_MISSING", "message-secret").WithMetadata(map[string]string{"token": "metadata-secret"}).WithCause(errors.New("cause-secret")), code: "NotFound", reason: "ORDER_MISSING", level: slog.LevelWarn},
		{name: "remote retryable", err: errorx.New(errorx.Unavailable, "UPSTREAM_DOWN", "message-secret").WithRetryable(true).WithRemote(), code: "Unavailable", reason: "UPSTREAM_DOWN", retryable: true, remote: true, level: slog.LevelError},
		{name: "unstructured", err: errors.New("message-secret"), code: "Unknown", level: slog.LevelError},
		{name: "non-nil OK", err: errorx.New(errorx.OK, "INVALID_OK", "message-secret"), code: "Unknown", reason: "INVALID_OK", level: slog.LevelError},
		{name: "undefined code", err: errorx.New(errorx.Code(99), "INVALID_CODE", "message-secret"), code: "Unknown", reason: "INVALID_CODE", level: slog.LevelError},
	}
	for scope, wrap := range scopes() {
		for _, tc := range cases {
			t.Run(scope+"/"+tc.name, func(t *testing.T) {
				sink := &captureHandler{min: slog.LevelDebug}
				ctx := tracing.ContextWithSpan(serverCtx(t.Context()), correlationSpan{traceID, spanID})
				next := func(got context.Context, req any) (any, error) {
					if got != ctx || req != "request-secret" {
						t.Fatal("call was changed")
					}
					return "result-secret", tc.err
				}
				result, err := wrap(testLogger(slog.New(sink)), next)(ctx, "request-secret")
				if result != "result-secret" || err != tc.err {
					t.Fatal("result/error identity changed")
				}
				record, attrs := sink.only(t)
				if record.Level != tc.level || attrs[endpointmw.KeyCode].String() != tc.code {
					t.Fatalf("level=%v attrs=%v", record.Level, attrs)
				}
				if attrs[endpointmw.KeyTraceID].String() != traceID || attrs[endpointmw.KeySpanID].String() != spanID {
					t.Fatalf("missing trace correlation: %v", attrs)
				}
				if tc.err != nil {
					if tc.reason != "" {
						if attrs[endpointmw.KeyReason].String() != tc.reason {
							t.Fatalf("reason=%v", attrs[endpointmw.KeyReason])
						}
					} else if _, ok := attrs[endpointmw.KeyReason]; ok {
						t.Fatal("empty reason attribute")
					}
					for key, want := range map[string]bool{endpointmw.KeyRetryable: tc.retryable, endpointmw.KeyRemote: tc.remote} {
						got, ok := attrs[key]
						if !ok || got.Kind() != slog.KindBool || got.Bool() != want {
							t.Fatalf("%s=%v", key, got)
						}
					}
				} else {
					for _, key := range []string{endpointmw.KeyReason, endpointmw.KeyRetryable, endpointmw.KeyRemote} {
						if _, ok := attrs[key]; ok {
							t.Fatalf("%s on successful call", key)
						}
					}
				}
				if _, ok := attrs["error"]; ok {
					t.Fatal("raw error attribute")
				}
				for _, value := range attrs {
					if strings.Contains(value.String(), "secret") {
						t.Fatalf("payload leaked: %v", attrs)
					}
				}
			})
		}
	}
}

func TestInterruptedCallsPropagateAndLog(t *testing.T) {
	for scope, wrap := range scopes() {
		for _, interruption := range []string{"panic", "goexit"} {
			t.Run(scope+"/"+interruption, func(t *testing.T) {
				sink := &captureHandler{min: slog.LevelDebug}
				marker := &struct{ secret string }{"panic-secret"}
				ctx := tracing.ContextWithSpan(t.Context(), correlationSpan{traceID, spanID})
				wrapped := wrap(testLogger(slog.New(sink)), func(context.Context, any) (any, error) {
					if interruption == "goexit" {
						runtime.Goexit()
					}
					panic(marker)
				})
				done := make(chan struct{})
				var recovered any
				returned := false
				go func() {
					defer close(done)
					defer func() { recovered = recover() }()
					_, _ = wrapped(ctx, "request-secret")
					returned = true
				}()
				<-done
				if returned || (interruption == "panic" && recovered != marker) || (interruption == "goexit" && recovered != nil) {
					t.Fatalf("interruption changed: returned=%v recovered=%v", returned, recovered)
				}
				record, attrs := sink.only(t)
				if record.Level != slog.LevelError || attrs[endpointmw.KeyCode].String() != "Internal" || attrs[endpointmw.KeyReason].String() != "INTERRUPTED" {
					t.Fatalf("interrupted outcome: %v", attrs)
				}
				if attrs[endpointmw.KeyRetryable].Bool() || attrs[endpointmw.KeyRemote].Bool() {
					t.Fatalf("interrupted flags: %v", attrs)
				}
				if attrs[endpointmw.KeyTraceID].String() != traceID || attrs[endpointmw.KeySpanID].String() != spanID {
					t.Fatal("interrupted trace correlation lost")
				}
				for _, value := range attrs {
					if strings.Contains(value.String(), "secret") {
						t.Fatal("interruption payload leaked")
					}
				}
			})
		}
	}
}

func TestInvalidTraceContextIsOmitted(t *testing.T) {
	for scope, wrap := range scopes() {
		t.Run(scope, func(t *testing.T) {
			sink := &captureHandler{min: slog.LevelDebug}
			ctx := tracing.ContextWithSpan(t.Context(), correlationSpan{"", ""})
			_, _ = wrap(testLogger(slog.New(sink)), func(context.Context, any) (any, error) { return nil, nil })(ctx, nil)
			_, attrs := sink.only(t)
			if _, ok := attrs[endpointmw.KeyTraceID]; ok {
				t.Fatal("invalid trace ID logged")
			}
			if _, ok := attrs[endpointmw.KeySpanID]; ok {
				t.Fatal("invalid span ID logged")
			}
		})
	}
}

func TestNilLoggerStillChoosesLevelAfterHandler(t *testing.T) {
	for name, factory := range loggingFactories() {
		t.Run(name, func(t *testing.T) {
			calls := 0
			handled := false
			cause := errorx.New(errorx.NotFound, "NOT_FOUND", "missing")
			ep := factory(nil, endpointmw.WithLogLevel(func(code errorx.Code) logger.Level {
				calls++
				if !handled || code != errorx.NotFound {
					t.Fatalf("level callback before handler or wrong code: handled=%v code=%v", handled, code)
				}
				return logger.LevelWarn
			}))(func(ctx context.Context, req any) (any, error) {
				if ctx != t.Context() || req != "request" {
					t.Fatal("request/context changed")
				}
				handled = true
				return "response", cause
			})
			resp, err := ep(t.Context(), "request")
			if resp != "response" || err != cause || calls != 1 {
				t.Fatalf("response=%v error=%v level calls=%d", resp, err, calls)
			}
		})
	}

}

type delayedLogger struct{ duration time.Duration }

func (*delayedLogger) Enabled(context.Context, logger.Level) bool {
	time.Sleep(20 * time.Millisecond)
	return true
}

func (l *delayedLogger) Log(_ context.Context, _ logger.Level, _ string, attrs ...any) {
	for i := 0; i < len(attrs); i += 2 {
		if attrs[i] == endpointmw.KeyDuration {
			l.duration = attrs[i+1].(time.Duration)
		}
	}
}

func TestDurationExcludesLevelAndEnabledWork(t *testing.T) {
	for name, factory := range loggingFactories() {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				l := &delayedLogger{}
				ep := factory(l, endpointmw.WithLogLevel(func(errorx.Code) logger.Level {
					time.Sleep(10 * time.Millisecond)
					return logger.LevelInfo
				}))(func(context.Context, any) (any, error) {
					time.Sleep(150 * time.Millisecond)
					return nil, nil
				})
				if _, err := ep(t.Context(), nil); err != nil {
					t.Fatal(err)
				}
				if l.duration != 150*time.Millisecond {
					t.Fatalf("logged duration=%v, want handler duration only", l.duration)
				}
			})
		})
	}
}

func TestApplicationDurationExcludesCallbacks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		log := &delayedLogger{}
		handler := application.Decorate(application.Metadata{Operation: "order.inspect"},
			application.Func[int, int](func(context.Context, int) (int, error) {
				time.Sleep(150 * time.Millisecond)
				return 1, nil
			}), applicationmw.Logging[int, int](log, endpointmw.WithLogLevel(func(errorx.Code) logger.Level {
				time.Sleep(10 * time.Millisecond)
				return logger.LevelInfo
			})))
		if result, err := handler.Handle(t.Context(), 1); result != 1 || err != nil {
			t.Fatalf("result=%v error=%v", result, err)
		}
		if log.duration != 150*time.Millisecond {
			t.Fatalf("duration includes callback work: %v", log.duration)
		}
	})
}

func TestNilApplicationLoggerStillChoosesLevel(t *testing.T) {
	handled, calls := false, 0
	failure := errorx.New(errorx.NotFound, "ORDER_MISSING", "private")
	handler := application.Decorate(application.Metadata{Operation: "order.inspect"},
		application.Func[int, int](func(context.Context, int) (int, error) { handled = true; return 7, failure }),
		applicationmw.Logging[int, int](nil, endpointmw.WithLogLevel(func(code errorx.Code) logger.Level {
			if !handled || code != errorx.NotFound {
				t.Fatal("level callback ran before handler or wrong code")
			}
			calls++
			return logger.LevelWarn
		})))
	result, err := handler.Handle(t.Context(), 1)
	if result != 7 || err != failure || calls != 1 {
		t.Fatalf("result=%v error=%v calls=%d", result, err, calls)
	}
}

func TestTypedLogging(t *testing.T) {
	t.Parallel()
	for _, kind := range []application.Kind{application.KindCommand, application.KindQuery} {
		t.Run(string(kind), func(t *testing.T) {
			sink := &captureHandler{}
			failure := errors.New("secret-token")
			ctx, cancel := context.WithCancel(serverCtx(t.Context()))
			cancel()
			base := application.Func[string, string](func(got context.Context, req string) (string, error) {
				if got != ctx || got.Err() != context.Canceled || req != "private-payload" {
					t.Error("call was changed")
				}
				return "private-result", failure
			})
			var handler application.Handler[string, string]
			if kind == application.KindCommand {
				handler = application.Decorate(application.Metadata{Kind: application.KindCommand, Operation: "catalog.create"}, base, applicationmw.Logging[string, string](testLogger(slog.New(sink))))
			} else {
				handler = application.Decorate(application.Metadata{Kind: application.KindQuery, Operation: "catalog.get"}, base, applicationmw.Logging[string, string](testLogger(slog.New(sink))))
			}
			got, err := handler.Handle(ctx, "private-payload")
			if got != "private-result" || err != failure {
				t.Fatal("result or error identity changed")
			}
			record, attrs := sink.only(t)
			if attrs[endpointmw.KeyKind].String() != string(kind) || attrs[endpointmw.KeyCode].String() != "Unknown" || !strings.HasPrefix(attrs[endpointmw.KeyOperation].String(), "catalog.") || record.Level != slog.LevelError {
				t.Fatalf("record: %v", attrs)
			}
			if len(attrs) != 6 {
				t.Fatalf("unexpected attributes: %v", attrs)
			}
			for _, value := range attrs {
				if strings.Contains(value.String(), "private") || strings.Contains(value.String(), "secret") {
					t.Fatalf("leaked value: %v", attrs)
				}
			}
		})
	}
}

func TestTypedLoggingPanic(t *testing.T) {
	t.Parallel()
	sink := &captureHandler{}
	marker := &struct{ secret string }{"do-not-log"}
	handler := application.Decorate(application.Metadata{Kind: application.KindCommand, Operation: "catalog.create"}, application.Func[int, int](func(context.Context, int) (int, error) { panic(marker) }), applicationmw.Logging[int, int](testLogger(slog.New(sink))))
	func() {
		defer func() {
			if got := recover(); got != marker {
				t.Errorf("panic changed: %v", got)
			}
		}()
		_, _ = handler.Handle(t.Context(), 1)
		t.Error("panic swallowed")
	}()
	_, attrs := sink.only(t)
	if attrs[endpointmw.KeyCode].String() != "Internal" || attrs[endpointmw.KeyReason].String() != "INTERRUPTED" || len(attrs) != 7 {
		t.Fatalf("panic record: %v", attrs)
	}
}

func TestTypedLoggingDisabledAndConcurrent(t *testing.T) {
	t.Parallel()
	sink := &captureHandler{min: slog.LevelError}
	base := application.Func[int, int](func(_ context.Context, req int) (int, error) { return req, nil })
	handler := application.Decorate(application.Metadata{Kind: application.KindQuery, Operation: "catalog.get"}, base, applicationmw.Logging[int, int](testLogger(slog.New(sink)), nil, endpointmw.WithLogLevel(func(errorx.Code) logger.Level { return logger.LevelDebug })))
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			got, err := handler.Handle(t.Context(), i)
			if got != i || err != nil {
				t.Error("changed call")
			}
		})
	}
	wg.Wait()
	if len(sink.records) != 0 {
		t.Fatal("disabled logger recorded calls")
	}
	if got, err := application.Decorate(application.Metadata{Kind: application.KindQuery, Operation: "catalog.get"}, base, applicationmw.Logging[int, int](nil)).Handle(t.Context(), 2); got != 2 || err != nil {
		t.Fatal("nil logger changed call")
	}
}

func Example_loggingApplication() {
	logger := testLogger(slog.New(slog.NewJSONHandler(io.Discard, nil)))
	create := application.Decorate(application.Metadata{Kind: application.KindCommand, Operation: "catalog.create_product"},
		application.Func[string, string](func(_ context.Context, name string) (string, error) { return name, nil }),
		applicationmw.Tracing[string, string](nil), applicationmw.Metrics[string, string](nil), applicationmw.Logging[string, string](logger),
	)
	get := application.Decorate(application.Metadata{Kind: application.KindQuery, Operation: "catalog.get_product"},
		application.Func[string, string](func(_ context.Context, id string) (string, error) { return id, nil }),
		applicationmw.Tracing[string, string](nil), applicationmw.Metrics[string, string](nil), applicationmw.Logging[string, string](logger),
	)
	created, err := create.Handle(context.Background(), "book")
	fmt.Println(created, err)
	found, err := get.Handle(context.Background(), created)
	fmt.Println(found, err)
	// Output:
	// book <nil>
	// book <nil>
}

func Example_logging() {
	// Drop the time and duration so the output is stable.
	logger := testLogger(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey || a.Key == endpointmw.KeyDuration {
				return slog.Attr{}
			}
			return a
		},
	})))
	ep := endpointmw.Logging(logger)(func(_ context.Context, req any) (any, error) {
		if req == nil {
			return nil, errorx.New(errorx.NotFound, "USER_NOT_FOUND", "user not found")
		}
		return req, nil
	})

	ctx := metadata.NewContext(context.Background(), metadata.Info{
		Kind: metadata.KindServer, Protocol: "http", Operation: "GET /v1/users/{id}",
	})
	_, _ = ep(ctx, "alice")
	_, _ = ep(ctx, nil)
	// Output:
	// level=INFO msg=call kind=server protocol=http operation="GET /v1/users/{id}" code=OK
	// level=WARN msg=call kind=server protocol=http operation="GET /v1/users/{id}" code=NotFound reason=USER_NOT_FOUND retryable=false remote=false
}

// loggerFixture is a test double for the backend-independent contract.
type loggerFixture struct{ *slog.Logger }

func testLogger(l *slog.Logger) *loggerFixture { return &loggerFixture{l} }
func (l *loggerFixture) Enabled(ctx context.Context, level logger.Level) bool {
	return l.Logger.Enabled(ctx, slog.Level(level))
}
func (l *loggerFixture) Log(ctx context.Context, level logger.Level, msg string, fields ...any) {
	l.Logger.Log(ctx, slog.Level(level), msg, fields...)
}
