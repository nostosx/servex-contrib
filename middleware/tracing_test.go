package middleware_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	applicationmw "github.com/nostosx/servex-contrib/middleware/application"
	clientmw "github.com/nostosx/servex-contrib/middleware/client"
	endpointmw "github.com/nostosx/servex-contrib/middleware/endpoint"
	nooptracing "github.com/nostosx/servex-contrib/observability/tracing/noop"
	"github.com/nostosx/servex/application"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/observability/tracing"
)

// recordedSpan is a span recorded by recordingTracer.
type recordedSpan struct {
	name    string
	kind    tracing.SpanKind
	traceID string
	spanID  string
	parent  string // span ID of the parent, local or remote

	mu    sync.Mutex
	attrs map[string]any
	errs  []error
	ended int
}

func (s *recordedSpan) End() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ended++
}

func (s *recordedSpan) SetAttribute(key string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attrs[key] = value
}

func (s *recordedSpan) RecordError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errs = append(s.errs, err)
}

func (s *recordedSpan) TraceID() string { return s.traceID }
func (s *recordedSpan) SpanID() string  { return s.spanID }

// remoteKey stores a remote parent extracted by fakePropagator.
type remoteKey struct{}

type remote struct{ traceID, spanID string }

// recordingTracer records every started span.
type recordingTracer struct {
	mu    sync.Mutex
	spans []*recordedSpan
}

func (tr *recordingTracer) Start(ctx context.Context, name string, kind tracing.SpanKind) (context.Context, tracing.Span) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	s := &recordedSpan{
		name:   name,
		kind:   kind,
		spanID: "span" + strconv.Itoa(len(tr.spans)+1),
		attrs:  make(map[string]any),
	}
	if parent, ok := tracing.SpanFromContext(ctx).(*recordedSpan); ok {
		s.traceID, s.parent = parent.traceID, parent.spanID
	} else if r, ok := ctx.Value(remoteKey{}).(remote); ok {
		s.traceID, s.parent = r.traceID, r.spanID
	} else {
		s.traceID = "trace" + strconv.Itoa(len(tr.spans)+1)
	}
	tr.spans = append(tr.spans, s)
	return tracing.ContextWithSpan(ctx, s), s
}

func (tr *recordingTracer) only(t *testing.T) *recordedSpan {
	t.Helper()
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if len(tr.spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(tr.spans))
	}
	return tr.spans[0]
}

// fakePropagator uses a single "traceparent" header "<trace>-<span>".
type fakePropagator struct {
	mu        sync.Mutex
	injected  int
	extracted int
}

func (p *fakePropagator) Inject(ctx context.Context, c tracing.Carrier) {
	p.mu.Lock()
	p.injected++
	p.mu.Unlock()
	s := tracing.SpanFromContext(ctx)
	if s.TraceID() == "" {
		return
	}
	c.Set("traceparent", s.TraceID()+"-"+s.SpanID())
}

func (p *fakePropagator) Extract(ctx context.Context, c tracing.Carrier) context.Context {
	p.mu.Lock()
	p.extracted++
	p.mu.Unlock()
	traceID, spanID, ok := strings.Cut(c.Get("traceparent"), "-")
	if !ok {
		return ctx
	}
	return context.WithValue(ctx, remoteKey{}, remote{traceID, spanID})
}

func TestServerExtractsAndStartsServerSpan(t *testing.T) {
	tr, p := &recordingTracer{}, &fakePropagator{}
	ctx := metadata.NewContext(t.Context(), metadata.Info{
		Kind: metadata.KindServer, Protocol: "http", Operation: "GET /x",
		Header: metadata.Pairs("traceparent", "remote-trace-remote-span"),
	})
	var inner tracing.Span
	ep := endpointmw.Tracing(tr, p)(func(ctx context.Context, _ any) (any, error) {
		inner = tracing.SpanFromContext(ctx)
		return "ok", nil
	})
	if resp, err := ep(ctx, nil); err != nil || resp != "ok" {
		t.Fatalf("got (%v, %v)", resp, err)
	}
	s := tr.only(t)
	if s.name != "GET /x" || s.kind != tracing.SpanKindServer {
		t.Fatalf("span = %q kind %v", s.name, s.kind)
	}
	if s.traceID != "remote" || s.parent != "trace-remote-span" {
		t.Fatalf("span did not continue the extracted trace: trace %q parent %q", s.traceID, s.parent)
	}
	if inner != s {
		t.Fatal("next endpoint does not see the server span")
	}
	if s.ended != 1 {
		t.Fatalf("ended %d times", s.ended)
	}
	if s.attrs[endpointmw.AttrProtocol] != "http" || s.attrs[endpointmw.AttrOperation] != "GET /x" || s.attrs[endpointmw.AttrCode] != "OK" {
		t.Fatalf("attrs = %v", s.attrs)
	}
	if len(s.errs) != 0 {
		t.Fatalf("errors recorded on success: %v", s.errs)
	}
	if p.injected != 0 || p.extracted != 1 {
		t.Fatalf("injected %d extracted %d", p.injected, p.extracted)
	}
}

func TestServerWithoutHeader(t *testing.T) {
	tr, p := &recordingTracer{}, &fakePropagator{}
	ctx := metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindServer, Protocol: "broker", Operation: "orders"})
	if _, err := endpointmw.Tracing(tr, p)(func(context.Context, any) (any, error) { return nil, nil })(ctx, nil); err != nil {
		t.Fatal(err)
	}
	s := tr.only(t)
	if s.kind != tracing.SpanKindServer || s.parent != "" {
		t.Fatalf("span kind %v parent %q", s.kind, s.parent)
	}
	if p.extracted != 0 {
		t.Fatal("extracted from a nil header")
	}
}

func TestClientInjectsClientSpan(t *testing.T) {
	tr, p := &recordingTracer{}, &fakePropagator{}
	parentCtx, parent := tr.Start(t.Context(), "parent", tracing.SpanKindInternal)
	header := make(metadata.MD)
	ctx := metadata.NewContext(parentCtx, metadata.Info{Kind: metadata.KindClient, Protocol: "grpc", Operation: "/a.B/C", Header: header})

	ep := clientmw.Tracing(tr, p)(func(ctx context.Context, _ any) (any, error) {
		info, _ := metadata.FromContext(ctx)
		return info.Header.Get("traceparent"), nil
	})
	resp, err := ep(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	client := tr.spans[1]
	if client.kind != tracing.SpanKindClient || client.name != "/a.B/C" {
		t.Fatalf("span = %q kind %v", client.name, client.kind)
	}
	if client.parent != parent.SpanID() || client.traceID != parent.TraceID() {
		t.Fatalf("client span is not a child of the caller's span")
	}
	want := client.traceID + "-" + client.spanID
	if header.Get("traceparent") != want || resp != want {
		t.Fatalf("injected %q (next saw %v), want the client span %q", header.Get("traceparent"), resp, want)
	}
	if client.ended != 1 {
		t.Fatalf("ended %d times", client.ended)
	}
	if p.extracted != 0 {
		t.Fatal("client extracted")
	}
}

func TestClientCreatesHeader(t *testing.T) {
	tr, p := &recordingTracer{}, &fakePropagator{}
	ctx := metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindClient, Operation: "op"})
	var got metadata.Info
	_, err := clientmw.Tracing(tr, p)(func(ctx context.Context, _ any) (any, error) {
		got, _ = metadata.FromContext(ctx)
		return nil, nil
	})(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	s := tr.only(t)
	if got.Operation != "op" || got.Header.Get("traceparent") != s.traceID+"-"+s.spanID {
		t.Fatalf("info = %+v", got)
	}
}

func TestNoInfoStartsInternalSpan(t *testing.T) {
	tr, p := &recordingTracer{}, &fakePropagator{}
	if _, err := endpointmw.Tracing(tr, p)(func(context.Context, any) (any, error) { return nil, nil })(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if s := tr.only(t); s.kind != tracing.SpanKindInternal {
		t.Fatalf("kind = %v", s.kind)
	}
	if p.injected+p.extracted != 0 {
		t.Fatal("propagated without Info")
	}
}

// serverFaults is the expected server-span error set of the OpenTelemetry
// gRPC convention.
var serverFaults = map[errorx.Code]bool{
	errorx.Unknown:          true,
	errorx.DeadlineExceeded: true,
	errorx.Unimplemented:    true,
	errorx.Internal:         true,
	errorx.Unavailable:      true,
	errorx.DataLoss:         true,
}

func TestErrorStatusByCodeClass(t *testing.T) {
	kinds := []struct {
		name string
		info *metadata.Info
		all  bool // every failure marks the span as an error
	}{
		{"server", &metadata.Info{Kind: metadata.KindServer, Operation: "op"}, false},
		{"internal", nil, false},
		{"client", &metadata.Info{Kind: metadata.KindClient, Operation: "op", Header: metadata.MD{}}, true},
	}
	for _, k := range kinds {
		for code := errorx.Canceled; code <= errorx.Unauthenticated; code++ {
			t.Run(k.name+"/"+code.String(), func(t *testing.T) {
				tr := &recordingTracer{}
				ctx := t.Context()
				if k.info != nil {
					ctx = metadata.NewContext(ctx, *k.info)
				}
				failure := errorx.New(code, "REASON", "message")
				decorate := endpointmw.Tracing(tr, nil)
				if k.all {
					decorate = clientmw.Tracing(tr, nil)
				}
				_, err := decorate(func(context.Context, any) (any, error) { return nil, failure })(ctx, nil)
				if err != failure {
					t.Fatalf("err = %v, want unchanged", err)
				}
				s := tr.only(t)
				if s.attrs[endpointmw.AttrCode] != code.String() {
					t.Fatalf("code = %v", s.attrs[endpointmw.AttrCode])
				}
				if want := k.all || serverFaults[code]; want != (len(s.errs) == 1) || len(s.errs) > 1 {
					t.Fatalf("errs = %v, want error status %v", s.errs, want)
				}
				if len(s.errs) == 1 && errorx.Classify(s.errs[0]) != errorx.Classify(failure) {
					t.Fatalf("recorded classification %v, want %v", errorx.Classify(s.errs[0]), errorx.Classify(failure))
				}
			})
		}
	}
}

func TestUnclassifiedErrorIsServerFault(t *testing.T) {
	tr := &recordingTracer{}
	failure := errors.New("boom")
	ctx := metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindServer, Operation: "op"})
	_, _ = endpointmw.Tracing(tr, nil)(func(context.Context, any) (any, error) { return nil, failure })(ctx, nil)
	s := tr.only(t)
	if s.attrs[endpointmw.AttrCode] != "Unknown" || len(s.errs) != 1 || errorx.Classify(s.errs[0]).Code != errorx.Unknown {
		t.Fatalf("code %v, errs %v", s.attrs[endpointmw.AttrCode], s.errs)
	}
}

func TestEndsOnPanic(t *testing.T) {
	tr := &recordingTracer{}
	marker := &struct{ private string }{"private-panic"}
	func() {
		defer func() {
			if recover() != marker {
				t.Error("panic identity changed")
			}
		}()
		_, _ = endpointmw.Tracing(tr, nil)(func(context.Context, any) (any, error) { panic(marker) })(t.Context(), nil)
		t.Error("panic swallowed")
	}()
	assertRecordedClassification(t, tr.only(t), errorx.Classification{Code: errorx.Internal, Reason: "INTERRUPTED"}, true)
}

func TestNilTracerAndPropagator(t *testing.T) {
	ctx := metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindClient, Header: metadata.MD{}})
	resp, err := clientmw.Tracing(nil, nil)(func(context.Context, any) (any, error) { return "ok", nil })(ctx, nil)
	if err != nil || resp != "ok" {
		t.Fatalf("got (%v, %v)", resp, err)
	}
}

func TestClientServerPropagation(t *testing.T) {
	// A client span injected into the outbound header becomes the parent of
	// the server span extracted from the same header.
	tr, p := &recordingTracer{}, &fakePropagator{}
	server := endpointmw.Tracing(tr, p)(func(context.Context, any) (any, error) { return nil, nil })
	wire := func(ctx context.Context, req any) (any, error) {
		out, _ := metadata.FromContext(ctx)
		in := metadata.NewContext(context.Background(), metadata.Info{Kind: metadata.KindServer, Operation: "op", Header: out.Header.Clone()})
		return server(in, req)
	}
	client := clientmw.Tracing(tr, p)(wire)
	ctx := metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindClient, Operation: "op", Header: metadata.MD{}})
	if _, err := client(ctx, nil); err != nil {
		t.Fatal(err)
	}
	c, s := tr.spans[0], tr.spans[1]
	if s.traceID != c.traceID || s.parent != c.spanID {
		t.Fatalf("server span trace %q parent %q, want trace %q parent %q", s.traceID, s.parent, c.traceID, c.spanID)
	}
}

func assertRecordedClassification(t *testing.T, span *recordedSpan, want errorx.Classification, failed bool) {
	t.Helper()
	if span.ended != 1 {
		t.Fatalf("span ended %d times, want once", span.ended)
	}
	if span.attrs[endpointmw.AttrCode] != want.Code.String() {
		t.Fatalf("code attribute = %v, want %v", span.attrs[endpointmw.AttrCode], want.Code)
	}
	if want.Code != errorx.OK {
		if want.Reason != "" && span.attrs[endpointmw.AttrReason] != want.Reason ||
			span.attrs[endpointmw.AttrRetryable] != want.Retryable ||
			span.attrs[endpointmw.AttrRemote] != want.Remote {
			t.Fatalf("classification attributes = %v, want %+v", span.attrs, want)
		}
	}
	wantErrors := 0
	if failed && want.Code != errorx.OK {
		wantErrors = 1
	}
	if len(span.errs) != wantErrors {
		t.Fatalf("recorded errors = %v, want %d errors", span.errs, wantErrors)
	}
	if wantErrors == 1 && errorx.Classify(span.errs[0]) != want {
		t.Fatalf("recorded classification = %+v, want %+v", errorx.Classify(span.errs[0]), want)
	}
}

func TestTracingUsesPayloadFreeClassification(t *testing.T) {
	t.Parallel()
	for _, scope := range []string{"server", "client", "application"} {
		for _, tc := range []struct {
			name string
			err  error
		}{
			{"success", nil},
			{"business_english", errorx.New(errorx.NotFound, "ORDER_MISSING", "private English message").WithMetadata(map[string]string{"token": "private-token"})},
			{"business_chinese", errorx.New(errorx.NotFound, "ORDER_MISSING", "私密业务文案").WithCause(errors.New("private database credential"))},
			{"remote_fault", errorx.New(errorx.Unavailable, "DEPENDENCY_DOWN", "private backend address").WithRemote().WithRetryable(false)},
			{"invalid_non_nil_ok", errorx.New(errorx.OK, "INVALID_ERROR", "private invalid value")},
			{"undefined_code", errorx.New(errorx.Code(999), "INVALID_ERROR", "private undefined value")},
		} {
			t.Run(scope+"/"+tc.name, func(t *testing.T) {
				tr := &recordingTracer{}
				parentCtx, parent := tr.Start(t.Context(), "parent", tracing.SpanKindServer)
				defer parent.End()
				var received tracing.Span
				var result any
				var err error
				if scope == "application" {
					handler := application.Decorate(application.Metadata{Operation: "order.read"},
						application.Func[any, any](func(ctx context.Context, _ any) (any, error) {
							received = tracing.SpanFromContext(ctx)
							return "result", tc.err
						}), applicationmw.Tracing[any, any](tr),
					)
					result, err = handler.Handle(parentCtx, nil)
				} else {
					kind := metadata.KindServer
					if scope == "client" {
						kind = metadata.KindClient
					}
					ctx := metadata.NewContext(parentCtx, metadata.Info{Kind: kind, Operation: "order.read"})
					decorate := endpointmw.Tracing(tr, nil)
					if scope == "client" {
						decorate = clientmw.Tracing(tr, nil)
					}
					result, err = decorate(func(ctx context.Context, _ any) (any, error) {
						received = tracing.SpanFromContext(ctx)
						return "result", tc.err
					})(ctx, nil)
				}
				if result != "result" || err != tc.err {
					t.Fatal("business result or error identity changed")
				}
				span := tr.spans[1]
				if received != span || span.traceID != parent.TraceID() || span.parent != parent.SpanID() {
					t.Fatal("child span context is not available for correlated observations")
				}
				class := errorx.Classify(tc.err)
				assertRecordedClassification(t, span, class, scope == "client" || errorx.IsServerFault(class.Code))
				for key, value := range span.attrs {
					text := fmt.Sprint(value)
					if strings.Contains(text, "private") || strings.Contains(text, "私密") {
						t.Fatalf("sensitive attribute %s: %v", key, value)
					}
				}
				for _, recorded := range span.errs {
					if strings.Contains(recorded.Error(), "private") || strings.Contains(recorded.Error(), "私密") {
						t.Fatalf("sensitive text recorded: %v", recorded)
					}
				}
			})
		}
	}
}

func TestTracingGoexitRecordsInterruptedAndEndsOnce(t *testing.T) {
	t.Parallel()
	for _, scope := range []string{"endpoint", "application"} {
		t.Run(scope, func(t *testing.T) {
			tr := &recordingTracer{}
			done := make(chan struct{})
			returned := false
			go func() {
				defer close(done)
				if scope == "endpoint" {
					_, _ = endpointmw.Tracing(tr, nil)(func(context.Context, any) (any, error) {
						runtime.Goexit()
						return nil, nil
					})(t.Context(), nil)
				} else {
					handler := application.Decorate(application.Metadata{Operation: "interrupted"},
						application.Func[int, int](func(context.Context, int) (int, error) {
							runtime.Goexit()
							return 0, nil
						}), applicationmw.Tracing[int, int](tr),
					)
					_, _ = handler.Handle(t.Context(), 0)
				}
				returned = true
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("Goexit invocation did not exit")
			}
			if returned {
				t.Fatal("Goexit was swallowed")
			}
			assertRecordedClassification(t, tr.only(t), errorx.Classification{Code: errorx.Internal, Reason: "INTERRUPTED"}, true)
		})
	}
}

func TestTypedTracing(t *testing.T) {
	t.Parallel()
	for _, kind := range []application.Kind{application.KindCommand, application.KindQuery} {
		t.Run(string(kind), func(t *testing.T) {
			tr := &recordingTracer{}
			parentCtx, parent := tr.Start(t.Context(), "parent", tracing.SpanKindServer)
			defer parent.End()
			header := metadata.Pairs("traceparent", "untrusted-header")
			ctx, cancel := context.WithCancel(metadata.NewContext(parentCtx, metadata.Info{Kind: metadata.KindClient, Protocol: "http", Operation: "transport", Header: header}))
			cancel()
			failure := errorx.New(errorx.Internal, "PRIVATE", "secret-token")
			base := application.Func[int, int](func(got context.Context, req int) (int, error) {
				info, _ := metadata.FromContext(got)
				if got.Err() != context.Canceled || info.Operation != "transport" || tracing.SpanFromContext(got) == parent {
					t.Error("context or transport metadata changed")
				}
				return req, failure
			})
			var handler application.Handler[int, int]
			if kind == application.KindCommand {
				handler = application.Decorate(application.Metadata{Kind: application.KindCommand, Operation: "catalog.create"}, base, applicationmw.Tracing[int, int](tr))
			} else {
				handler = application.Decorate(application.Metadata{Kind: application.KindQuery, Operation: "catalog.get"}, base, applicationmw.Tracing[int, int](tr))
			}
			got, err := handler.Handle(ctx, 7)
			if got != 7 || err != failure {
				t.Fatal("result or error changed")
			}
			span := tr.spans[1]
			if span.kind != tracing.SpanKindInternal || span.parent != parent.SpanID() || span.traceID != parent.TraceID() || span.ended != 1 || span.attrs[applicationmw.AttrKind] != string(kind) || span.attrs[endpointmw.AttrOperation] != span.name || span.attrs[endpointmw.AttrCode] != "Internal" {
				t.Fatalf("span: %+v", span)
			}
			if len(span.errs) != 1 || errorx.Classify(span.errs[0]) != errorx.Classify(failure) || len(span.attrs) != 6 || header.Get("traceparent") != "untrusted-header" {
				t.Fatal("sensitive error or transport header leaked/changed")
			}
		})
	}
}

func TestTypedTracingErrorStatusByCodeClass(t *testing.T) {
	t.Parallel()
	for code := errorx.Canceled; code <= errorx.Unauthenticated; code++ {
		t.Run(code.String(), func(t *testing.T) {
			t.Parallel()
			tr := &recordingTracer{}
			failure := errorx.New(code, "PRIVATE", "secret-token")
			handler := application.Decorate(application.Metadata{Kind: application.KindQuery, Operation: "catalog.get"}, application.Func[int, int](func(context.Context, int) (int, error) {
				return 0, failure
			}), applicationmw.Tracing[int, int](tr))
			if _, err := handler.Handle(t.Context(), 1); err != failure {
				t.Fatalf("err = %v, want unchanged", err)
			}
			span := tr.only(t)
			if span.attrs[endpointmw.AttrCode] != code.String() || span.ended != 1 {
				t.Fatalf("span: %+v", span)
			}
			want := serverFaults[code]
			if want != (len(span.errs) == 1) || len(span.errs) > 1 {
				t.Fatalf("errs = %v, want error status %v", span.errs, want)
			}
			if want && errorx.Classify(span.errs[0]) != errorx.Classify(failure) {
				t.Fatalf("recorded %v, want a payload-free classification", span.errs[0])
			}
		})
	}
}

func TestTypedTracingPanic(t *testing.T) {
	t.Parallel()
	tr := &recordingTracer{}
	marker := &struct{ secret string }{"private-panic"}
	handler := application.Decorate(application.Metadata{Kind: application.KindCommand, Operation: "catalog.create"}, application.Func[int, int](func(context.Context, int) (int, error) { panic(marker) }), applicationmw.Tracing[int, int](tr))
	func() {
		defer func() {
			if recover() != marker {
				t.Error("panic changed")
			}
		}()
		_, _ = handler.Handle(t.Context(), 1)
		t.Error("panic swallowed")
	}()
	span := tr.only(t)
	if span.ended != 1 || span.attrs[endpointmw.AttrCode] != "Internal" || len(span.errs) != 1 || errorx.Classify(span.errs[0]).Reason != "INTERRUPTED" {
		t.Fatalf("panic span: %+v", span)
	}
}

func TestTypedTracingConcurrent(t *testing.T) {
	t.Parallel()
	tr := &recordingTracer{}
	base := application.Func[int, int](func(_ context.Context, req int) (int, error) { return req, nil })
	handler := application.Decorate(application.Metadata{Kind: application.KindQuery, Operation: "catalog.get"}, base, applicationmw.Tracing[int, int](tr))
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
	if len(tr.spans) != 32 {
		t.Fatal("lost concurrent span")
	}
	for _, span := range tr.spans {
		if span.ended != 1 || span.attrs[endpointmw.AttrCode] != "OK" || len(span.errs) != 0 {
			t.Fatal("bad successful span")
		}
	}
	if got, err := application.Decorate(application.Metadata{Kind: application.KindQuery, Operation: "catalog.get"}, base, applicationmw.Tracing[int, int](nil)).Handle(t.Context(), 3); got != 3 || err != nil {
		t.Fatal("nil tracer changed call")
	}
}

func Example_tracing() {
	// Use a real Tracer and Propagator, such as the ones from
	// servex-contrib/tracing/otel, in production.
	tracer, propagator := nooptracing.New(), nooptracing.NewPropagator()
	ep := endpointmw.Tracing(tracer, propagator)(func(ctx context.Context, _ any) (any, error) {
		// The handler sees the server span in its context.
		_ = tracing.SpanFromContext(ctx)
		return "ok", nil
	})

	ctx := metadata.NewContext(context.Background(), metadata.Info{
		Kind: metadata.KindServer, Protocol: "http", Operation: "GET /v1/users/{id}",
		Header: metadata.Pairs("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
	})
	resp, err := ep(ctx, nil)
	fmt.Println(resp, err)
	// Output: ok <nil>
}
