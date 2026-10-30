package profile_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/nostosx/servex-contrib/middleware/profile"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/logger"
	"github.com/nostosx/servex/metadata"
	metricapi "github.com/nostosx/servex/observability/metrics"
	traceapi "github.com/nostosx/servex/observability/tracing"
)

type recording struct {
	mu     sync.Mutex
	events []string
	codes  []string
	kinds  []traceapi.SpanKind
	ended  int
}

func (r *recording) event(event string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}
func (r *recording) Start(ctx context.Context, _ string, kind traceapi.SpanKind) (context.Context, traceapi.Span) {
	r.mu.Lock()
	r.events = append(r.events, "span.start")
	r.kinds = append(r.kinds, kind)
	r.mu.Unlock()
	return traceapi.ContextWithSpan(ctx, r), r
}
func (r *recording) End() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, "span.end")
	r.ended++
}
func (*recording) SetAttribute(string, any)                   {}
func (r *recording) RecordError(error)                        { r.event("span.error") }
func (*recording) TraceID() string                            { return "trace" }
func (*recording) SpanID() string                             { return "span" }
func (*recording) Enabled(context.Context, logger.Level) bool { return true }
func (r *recording) Log(ctx context.Context, _ logger.Level, message string, fields ...any) {
	if message != "call" {
		return
	}
	if traceapi.SpanFromContext(ctx) == nil {
		panic("call log lost active span")
	}
	r.event("log")
	for i := 0; i+1 < len(fields); i += 2 {
		if fields[i] == "code" {
			r.mu.Lock()
			r.codes = append(r.codes, fields[i+1].(string))
			r.mu.Unlock()
		}
	}
}

type counter struct{ recording *recording }

func (c counter) With(...string) metricapi.Counter { return c }
func (c counter) Add(ctx context.Context, _ float64) {
	if traceapi.SpanFromContext(ctx) == nil {
		panic("metric lost active span")
	}
	c.recording.event("metric.count")
}

type histogram struct{ recording *recording }

func (h histogram) With(...string) metricapi.Histogram { return h }
func (h histogram) Observe(context.Context, float64)   { h.recording.event("metric.duration") }

type meter struct{ recording *recording }

func (m meter) Counter(string, string, ...string) metricapi.Counter { return counter(m) }
func (m meter) Histogram(string, string, []float64, ...string) metricapi.Histogram {
	return histogram(m)
}
func (meter) Gauge(string, string, ...string) metricapi.Gauge { panic("unexpected gauge") }

type contextKey struct{}
type extractedKey struct{}
type policyKey struct{}
type propagator struct{}

func (propagator) Extract(ctx context.Context, _ traceapi.Carrier) context.Context {
	return context.WithValue(ctx, extractedKey{}, true)
}
func (propagator) Inject(ctx context.Context, carrier traceapi.Carrier) {
	carrier.Set("x-trace", traceapi.SpanFromContext(ctx).TraceID())
}

func configured(r *recording) *profile.Profile {
	return profile.New(profile.Config{Logger: r, Tracer: r, Propagator: propagator{}, Meter: meter{r}})
}

func TestServerObservesPanicAndPreservesContext(t *testing.T) {
	r := &recording{}
	p := configured(r)
	ctx := metadata.NewContext(context.WithValue(t.Context(), contextKey{}, "request"), metadata.Info{
		Kind: metadata.KindServer, Protocol: "http", Operation: "POST /notes", Header: metadata.Pairs("traceparent", "incoming"),
	})
	extra := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, request any) (any, error) {
			if ctx.Value(contextKey{}) != "request" || ctx.Value(extractedKey{}) != true {
				t.Fatal("request/propagator context lost")
			}
			if traceapi.SpanFromContext(ctx) != r {
				t.Fatal("extra runs outside span")
			}
			r.event("extra.before")
			response, err := next(ctx, request)
			r.event("extra.after")
			return response, err
		}
	}
	call := endpoint.Chain(p.Server(extra)...)(func(context.Context, any) (any, error) { r.event("handler"); return "ok", nil })
	if response, err := call(ctx, nil); err != nil || response != "ok" {
		t.Fatal(response, err)
	}
	want := []string{"span.start", "extra.before", "handler", "extra.after", "metric.count", "metric.duration", "log", "span.end"}
	if !reflect.DeepEqual(r.events, want) {
		t.Fatalf("order=%v", r.events)
	}
	panicCall := endpoint.Chain(p.Server()...)(func(context.Context, any) (any, error) { panic("private detail") })
	if _, err := panicCall(ctx, nil); errorx.CodeOf(err) != errorx.Internal {
		t.Fatal(err)
	}
	if r.ended != 2 || !reflect.DeepEqual(r.codes, []string{"OK", "Internal"}) {
		t.Fatalf("ends=%d codes=%v", r.ended, r.codes)
	}
}

func TestStreamLivesUntilOperationReturnsAndSnapshotsAreIndependent(t *testing.T) {
	r := &recording{}
	p := configured(r)
	one := p.Server()
	one[0] = nil
	extra := []endpoint.Middleware{func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, request any) (any, error) {
			return next(context.WithValue(ctx, policyKey{}, "kept"), request)
		}
	}}
	stream := p.Stream(extra...)
	extra[0] = nil
	started, release, completed := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	want := errors.New("stream ended")
	call := endpoint.Chain(stream...)(func(ctx context.Context, _ any) (any, error) {
		if traceapi.SpanFromContext(ctx) != r {
			t.Error("fresh stream snapshot lost tracing")
		}
		if ctx.Value(policyKey{}) != "kept" {
			t.Error("stream snapshot lost its explicit policy")
		}
		close(started)
		<-release
		return nil, want
	})
	ctx := metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindServer, Protocol: "grpc", Operation: "/notes/Watch"})
	go func() { _, err := call(ctx, nil); completed <- err }()
	<-started
	r.mu.Lock()
	ended := r.ended
	r.mu.Unlock()
	if ended != 0 {
		t.Fatal("stream span ended before stream completed")
	}
	close(release)
	if err := <-completed; !errors.Is(err, want) {
		t.Fatal(err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ended != 1 || len(r.codes) != 1 {
		t.Fatalf("ends=%d codes=%v", r.ended, r.codes)
	}
}

func TestClientPropagatesOnceAndDoesNotRetry(t *testing.T) {
	r := &recording{}
	p := configured(r)
	calls := 0
	want := errors.New("upstream failed")
	call := endpoint.Chain(p.Client()...)(func(ctx context.Context, _ any) (any, error) {
		calls++
		info, _ := metadata.FromContext(ctx)
		if info.Header.Get("x-trace") != "trace" {
			t.Fatal("client propagation missing")
		}
		return nil, want
	})
	ctx := metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindClient, Protocol: "http", Operation: "GET /notes"})
	if _, err := call(ctx, nil); !errors.Is(err, want) {
		t.Fatal(err)
	}
	if calls != 1 || r.kinds[0] != traceapi.SpanKindClient || r.ended != 1 {
		t.Fatalf("calls=%d kinds=%v ends=%d", calls, r.kinds, r.ended)
	}
}

func TestNilSignalsAndExplicitPolicies(t *testing.T) {
	p := profile.New(profile.Config{})
	want := errors.New("denied")
	deny := func(endpoint.Endpoint) endpoint.Endpoint {
		return func(context.Context, any) (any, error) { return nil, want }
	}
	call := endpoint.Chain(p.Server(nil, deny)...)(func(context.Context, any) (any, error) { t.Fatal("policy bypassed"); return nil, nil })
	if _, err := call(t.Context(), nil); !errors.Is(err, want) {
		t.Fatal(err)
	}
}
