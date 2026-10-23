package tracing_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"

	mwtracing "github.com/nostosx/servex-contrib/middleware/tracing"
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
	ep := mwtracing.New(tr, p)(func(ctx context.Context, _ any) (any, error) {
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
	if s.attrs[mwtracing.AttrProtocol] != "http" || s.attrs[mwtracing.AttrOperation] != "GET /x" || s.attrs[mwtracing.AttrCode] != "OK" {
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
	if _, err := mwtracing.New(tr, p)(func(context.Context, any) (any, error) { return nil, nil })(ctx, nil); err != nil {
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

	ep := mwtracing.New(tr, p)(func(ctx context.Context, _ any) (any, error) {
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
	_, err := mwtracing.New(tr, p)(func(ctx context.Context, _ any) (any, error) {
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
	if _, err := mwtracing.New(tr, p)(func(context.Context, any) (any, error) { return nil, nil })(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if s := tr.only(t); s.kind != tracing.SpanKindInternal {
		t.Fatalf("kind = %v", s.kind)
	}
	if p.injected+p.extracted != 0 {
		t.Fatal("propagated without Info")
	}
}

func TestRecordsError(t *testing.T) {
	tr := &recordingTracer{}
	failure := errorx.New(errorx.NotFound, "MISSING", "missing")
	ctx := metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindServer, Operation: "op"})
	_, err := mwtracing.New(tr, nil)(func(context.Context, any) (any, error) { return nil, failure })(ctx, nil)
	if err != failure {
		t.Fatalf("err = %v, want unchanged", err)
	}
	s := tr.only(t)
	if len(s.errs) != 1 || !errors.Is(s.errs[0], failure) {
		t.Fatalf("errs = %v", s.errs)
	}
	if s.attrs[mwtracing.AttrCode] != "NotFound" {
		t.Fatalf("code = %v", s.attrs[mwtracing.AttrCode])
	}
}

func TestEndsOnPanic(t *testing.T) {
	tr := &recordingTracer{}
	func() {
		defer func() { _ = recover() }()
		_, _ = mwtracing.New(tr, nil)(func(context.Context, any) (any, error) { panic("x") })(t.Context(), nil)
	}()
	if s := tr.only(t); s.ended != 1 {
		t.Fatalf("ended %d times", s.ended)
	}
}

func TestNilTracerAndPropagator(t *testing.T) {
	ctx := metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindClient, Header: metadata.MD{}})
	resp, err := mwtracing.New(nil, nil)(func(context.Context, any) (any, error) { return "ok", nil })(ctx, nil)
	if err != nil || resp != "ok" {
		t.Fatalf("got (%v, %v)", resp, err)
	}
}

func TestClientServerPropagation(t *testing.T) {
	// A client span injected into the outbound header becomes the parent of
	// the server span extracted from the same header.
	tr, p := &recordingTracer{}, &fakePropagator{}
	server := mwtracing.New(tr, p)(func(context.Context, any) (any, error) { return nil, nil })
	wire := func(ctx context.Context, req any) (any, error) {
		out, _ := metadata.FromContext(ctx)
		in := metadata.NewContext(context.Background(), metadata.Info{Kind: metadata.KindServer, Operation: "op", Header: out.Header.Clone()})
		return server(in, req)
	}
	client := mwtracing.New(tr, p)(wire)
	ctx := metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindClient, Operation: "op", Header: metadata.MD{}})
	if _, err := client(ctx, nil); err != nil {
		t.Fatal(err)
	}
	c, s := tr.spans[0], tr.spans[1]
	if s.traceID != c.traceID || s.parent != c.spanID {
		t.Fatalf("server span trace %q parent %q, want trace %q parent %q", s.traceID, s.parent, c.traceID, c.spanID)
	}
}
