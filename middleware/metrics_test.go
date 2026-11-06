package middleware_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	applicationmw "github.com/nostosx/servex-contrib/middleware/application"
	clientmw "github.com/nostosx/servex-contrib/middleware/client"
	endpointmw "github.com/nostosx/servex-contrib/middleware/endpoint"
	"github.com/nostosx/servex-contrib/middleware/observe"
	noopmetrics "github.com/nostosx/servex-contrib/observability/metrics/noop"
	"github.com/nostosx/servex/application"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/observability/metrics"
	"github.com/nostosx/servex/transport"
)

// recordingMeter records metric declarations and observations.
type recordingMeter struct {
	mu           sync.Mutex
	labelNames   map[string][]string
	buckets      map[string][]float64
	counters     map[string]float64   // name{values} -> sum
	observations map[string][]float64 // name{values} -> observations
	contexts     []context.Context    // context of every Add and Observe
}

func newRecordingMeter() *recordingMeter {
	return &recordingMeter{
		labelNames:   make(map[string][]string),
		buckets:      make(map[string][]float64),
		counters:     make(map[string]float64),
		observations: make(map[string][]float64),
	}
}

func series(name string, values []string) string {
	return name + "{" + strings.Join(values, ",") + "}"
}

func (m *recordingMeter) Counter(name, _ string, labelNames ...string) metrics.Counter {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.labelNames[name] = labelNames
	return &counter{m: m, name: name}
}

func (m *recordingMeter) Gauge(name, help string, labelNames ...string) metrics.Gauge {
	return noopmetrics.New().Gauge(name, help, labelNames...)
}

func (m *recordingMeter) Histogram(name, _ string, buckets []float64, labelNames ...string) metrics.Histogram {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.labelNames[name] = labelNames
	m.buckets[name] = buckets
	return &histogram{m: m, name: name}
}

type counter struct {
	m      *recordingMeter
	name   string
	values []string
}

func (c *counter) With(values ...string) metrics.Counter {
	return &counter{m: c.m, name: c.name, values: values}
}

func (c *counter) Add(ctx context.Context, delta float64) {
	c.m.mu.Lock()
	defer c.m.mu.Unlock()
	c.m.contexts = append(c.m.contexts, ctx)
	c.m.counters[series(c.name, c.values)] += delta
}

type histogram struct {
	m      *recordingMeter
	name   string
	values []string
}

func (h *histogram) With(values ...string) metrics.Histogram {
	return &histogram{m: h.m, name: h.name, values: values}
}

func (h *histogram) Observe(ctx context.Context, v float64) {
	h.m.mu.Lock()
	defer h.m.mu.Unlock()
	h.m.contexts = append(h.m.contexts, ctx)
	key := series(h.name, h.values)
	h.m.observations[key] = append(h.m.observations[key], v)
}

func metricFactories() map[string]func(metrics.Meter, ...observe.MetricsOption) endpoint.Middleware {
	return map[string]func(metrics.Meter, ...observe.MetricsOption) endpoint.Middleware{
		"endpoint": endpointmw.Metrics,
		"client":   clientmw.Metrics,
	}
}

func TestDeclarations(t *testing.T) {
	for name, factory := range metricFactories() {
		t.Run(name, func(t *testing.T) {
			m := newRecordingMeter()
			factory(m)
			if got := m.labelNames[endpointmw.RequestsTotal]; !slices.Equal(got, []string{"kind", "protocol", "operation", "code"}) {
				t.Fatalf("counter labels = %v", got)
			}
			if got := m.labelNames[endpointmw.RequestDuration]; !slices.Equal(got, []string{"kind", "protocol", "operation"}) {
				t.Fatalf("histogram labels = %v", got)
			}
			if got := m.buckets[endpointmw.RequestDuration]; !slices.Equal(got, metrics.DefaultBuckets) {
				t.Fatalf("buckets = %v", got)
			}
		})
	}
}

func TestCustomBuckets(t *testing.T) {
	for name, factory := range metricFactories() {
		t.Run(name, func(t *testing.T) {
			m := newRecordingMeter()
			b := []float64{0.1, 1}
			factory(m, nil, clientmw.WithMetricBuckets(nil), clientmw.WithMetricBuckets(b))
			b[0] = 42
			if got := m.buckets[endpointmw.RequestDuration]; !slices.Equal(got, []float64{0.1, 1}) {
				t.Fatalf("buckets = %v", got)
			}
		})
	}
}

func TestRecordsCalls(t *testing.T) {
	for name, factory := range metricFactories() {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				m := newRecordingMeter()
				mw := factory(m)
				ctx := metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindServer, Protocol: "http", Operation: "GET /x"})

				ok := mw(func(context.Context, any) (any, error) {
					time.Sleep(200 * time.Millisecond)
					return "ok", nil
				})
				failing := mw(func(context.Context, any) (any, error) {
					return nil, errorx.New(errorx.NotFound, "", "")
				})
				plain := mw(func(context.Context, any) (any, error) { return nil, errors.New("plain") })

				for range 2 {
					if resp, err := ok(ctx, nil); err != nil || resp != "ok" {
						t.Fatalf("got (%v, %v)", resp, err)
					}
				}
				if _, err := failing(ctx, nil); errorx.CodeOf(err) != errorx.NotFound {
					t.Fatalf("err = %v", err)
				}
				_, _ = plain(ctx, nil)

				want := map[string]float64{
					"servex_requests_total{server,http,GET /x,OK}":       2,
					"servex_requests_total{server,http,GET /x,NotFound}": 1,
					"servex_requests_total{server,http,GET /x,Unknown}":  1,
				}
				if len(m.counters) != len(want) {
					t.Fatalf("counters = %v", m.counters)
				}
				for k, v := range want {
					if m.counters[k] != v {
						t.Fatalf("%s = %v, want %v (all: %v)", k, m.counters[k], v, m.counters)
					}
				}
				obs := m.observations["servex_request_duration_seconds{server,http,GET /x}"]
				if !slices.Equal(obs, []float64{0.2, 0.2, 0, 0}) {
					t.Fatalf("observations = %v", obs)
				}
				if len(m.contexts) != 8 {
					t.Fatalf("recorded %d contexts, want 8", len(m.contexts))
				}
				for i, got := range m.contexts {
					if got != ctx {
						t.Fatalf("recording %d did not receive the request context", i)
					}
				}
			})
		})
	}
}

func TestNoInfo(t *testing.T) {
	for name, factory := range metricFactories() {
		t.Run(name, func(t *testing.T) {
			m := newRecordingMeter()
			_, _ = factory(m)(func(context.Context, any) (any, error) { return nil, nil })(t.Context(), nil)
			if m.counters["servex_requests_total{,,,OK}"] != 1 {
				t.Fatalf("counters = %v", m.counters)
			}
			if len(m.observations["servex_request_duration_seconds{,,}"]) != 1 {
				t.Fatalf("observations = %v", m.observations)
			}
		})
	}
}

func TestNilMeter(t *testing.T) {
	for name, factory := range metricFactories() {
		t.Run(name, func(t *testing.T) {
			resp, err := factory(nil)(func(context.Context, any) (any, error) { return "ok", nil })(t.Context(), nil)
			if err != nil || resp != "ok" {
				t.Fatalf("got (%v, %v)", resp, err)
			}
		})
	}
}

func TestOutcomeLabelsStayBounded(t *testing.T) {
	for name, factory := range metricFactories() {
		t.Run(name, func(t *testing.T) {
			meter := newRecordingMeter()
			failures := []error{
				errorx.New(errorx.NotFound, "ORDER_NOT_FOUND", "找不到订单"),
				errorx.New(errorx.NotFound, "ORDER_NOT_FOUND", "Order not found"),
				errorx.New(errorx.Code(99), "CUSTOM", "private"),
				errorx.New(errorx.OK, "INVALID", "private"),
			}
			for _, failure := range failures {
				_, got := factory(meter)(func(context.Context, any) (any, error) { return nil, failure })(t.Context(), nil)
				if got != failure {
					t.Fatal("original error changed")
				}
			}
			if len(meter.counters) != 2 || meter.counters["servex_requests_total{,,,NotFound}"] != 2 || meter.counters["servex_requests_total{,,,Unknown}"] != 2 {
				t.Fatalf("unbounded or localized series: %v", meter.counters)
			}
		})
	}
}

func TestInterruptedEndpointStillRecords(t *testing.T) {
	for name, factory := range metricFactories() {
		t.Run(name, func(t *testing.T) {
			for _, mode := range []string{"panic", "goexit"} {
				t.Run(mode, func(t *testing.T) {
					meter := newRecordingMeter()
					marker := &struct{}{}
					var handler endpoint.Endpoint = func(context.Context, any) (any, error) {
						if mode == "panic" {
							panic(marker)
						}
						runtime.Goexit()
						return nil, nil
					}
					done := make(chan struct{})
					go func() {
						defer close(done)
						defer func() {
							got := recover()
							if mode == "panic" && got != marker {
								t.Error("panic identity changed")
							}
							if mode == "goexit" && got != nil {
								t.Error("Goexit became a panic")
							}
						}()
						_, _ = factory(meter)(handler)(t.Context(), nil)
						t.Error("interrupted handler returned")
					}()
					<-done
					if meter.counters["servex_requests_total{,,,Internal}"] != 1 || len(meter.observations["servex_request_duration_seconds{,,}"]) != 1 {
						t.Fatalf("interruption missing: %v %v", meter.counters, meter.observations)
					}
				})
			}
		})
	}
}

func TestTypedMetrics(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		meter := newRecordingMeter()
		buckets := []float64{.1, 1}
		mw := applicationmw.Metrics[int, int](meter, nil, endpointmw.WithMetricBuckets(buckets))
		buckets[0] = 99
		ctx, cancel := context.WithCancel(metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindClient, Operation: "transport"}))
		cancel()
		failure := errorx.New(errorx.Canceled, "PRIVATE", "secret")
		base := application.Func[int, int](func(got context.Context, req int) (int, error) {
			if got != ctx || got.Err() != context.Canceled {
				t.Error("context changed")
			}
			time.Sleep(200 * time.Millisecond)
			return req, failure
		})
		got, err := application.Decorate(application.Metadata{Kind: application.KindCommand, Operation: "inventory.reserve"}, base, mw).Handle(ctx, 7)
		if got != 7 || err != failure {
			t.Fatal("result or error changed")
		}
		if meter.counters["servex_usecase_calls_total{command,inventory.reserve,Canceled}"] != 1 || !slices.Equal(meter.observations["servex_usecase_duration_seconds{command,inventory.reserve}"], []float64{.2}) {
			t.Fatalf("metrics: %v %v", meter.counters, meter.observations)
		}
		if len(meter.contexts) != 2 || meter.contexts[0] != ctx || meter.contexts[1] != ctx {
			t.Fatal("metrics did not receive the call context")
		}
		if !slices.Equal(meter.labelNames[applicationmw.UsecaseCallsTotal], []string{"kind", "operation", "code"}) || !slices.Equal(meter.labelNames[applicationmw.UsecaseDuration], []string{"kind", "operation"}) || !slices.Equal(meter.buckets[applicationmw.UsecaseDuration], []float64{.1, 1}) {
			t.Fatal("declarations or bucket ownership changed")
		}
	})
}

func TestTypedMetricsPanic(t *testing.T) {
	t.Parallel()
	meter := newRecordingMeter()
	marker := &struct{}{}
	handler := application.Decorate(application.Metadata{Kind: application.KindQuery, Operation: "catalog.get"}, application.Func[int, int](func(context.Context, int) (int, error) { panic(marker) }), applicationmw.Metrics[int, int](meter))
	func() {
		defer func() {
			if recover() != marker {
				t.Error("panic changed")
			}
		}()
		_, _ = handler.Handle(t.Context(), 1)
		t.Error("panic swallowed")
	}()
	if meter.counters["servex_usecase_calls_total{query,catalog.get,Internal}"] != 1 || len(meter.observations["servex_usecase_duration_seconds{query,catalog.get}"]) != 1 {
		t.Fatalf("panic metrics: %v", meter.counters)
	}
}

func TestTypedMetricsConcurrent(t *testing.T) {
	t.Parallel()
	meter := newRecordingMeter()
	base := application.Func[int, int](func(_ context.Context, req int) (int, error) { return req, nil })
	handler := application.Decorate(application.Metadata{Kind: application.KindQuery, Operation: "catalog.get"}, base, applicationmw.Metrics[int, int](meter))
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
	if meter.counters["servex_usecase_calls_total{query,catalog.get,OK}"] != 32 || len(meter.observations["servex_usecase_duration_seconds{query,catalog.get}"]) != 32 {
		t.Fatal("lost concurrent observation")
	}
	if got, err := application.Decorate(application.Metadata{Kind: application.KindQuery, Operation: "catalog.get"}, base, applicationmw.Metrics[int, int](nil)).Handle(t.Context(), 3); got != 3 || err != nil {
		t.Fatal("nil meter changed call")
	}
}

func Example_metrics() {
	// Use a real Meter, such as the one from
	// servex-contrib/metrics/prometheus, in production.
	meter := noopmetrics.New()

	chain := endpoint.Chain(
		endpointmw.Metrics(meter, endpointmw.WithMetricBuckets([]float64{.01, .1, 1})),
	)
	ep := chain(func(context.Context, any) (any, error) { return "ok", nil })

	resp, err := ep(context.Background(), nil)
	fmt.Println(resp, err)
	// Output: ok <nil>
}

func TestClientLogicalObservationWrapsAllAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		log := &captureHandler{min: slog.LevelDebug}
		logical, attempts := newRecordingMeter(), newRecordingMeter()
		calls := 0
		invoker := clientmw.AttemptMetrics(attempts)(transport.InvokerFunc(func(ctx context.Context, address string, request any) (any, error) {
			info, ok := transport.AttemptFromContext(ctx)
			calls++
			if !ok || info.Number != calls || address != "node:80" || request != "private-request" {
				t.Fatal("attempt metadata, address or request changed")
			}
			time.Sleep(50 * time.Millisecond)
			if calls < 3 {
				return nil, errorx.New(errorx.Unavailable, "REMOTE_DOWN", "private-error").WithRetryable(true).WithRemote()
			}
			return "private-response", nil
		}))
		call := endpoint.Chain(clientmw.Logging(testLogger(slog.New(log))), clientmw.Metrics(logical))(func(ctx context.Context, request any) (any, error) {
			for number := 1; number <= 3; number++ {
				result, err := invoker.Invoke(transport.WithAttempt(ctx, transport.Attempt{Number: number, Address: "node:80"}), "node:80", request)
				if err == nil || number == 3 {
					return result, err
				}
				time.Sleep(10 * time.Millisecond)
			}
			panic("unreachable")
		})
		ctx := metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindClient, Protocol: "grpc", Operation: "catalog.get"})
		result, err := call(ctx, "private-request")
		if result != "private-response" || err != nil || calls != 3 {
			t.Fatalf("result = %v, error = %v, attempts = %d", result, err, calls)
		}
		_, attrs := log.only(t)
		if attrs[clientmw.KeyCode].String() != "OK" || attrs[clientmw.KeyDuration].Duration() != 170*time.Millisecond || attrs[clientmw.KeyKind].String() != "client" {
			t.Fatalf("logical log = %v", attrs)
		}
		if logical.counters["servex_requests_total{client,grpc,catalog.get,OK}"] != 1 || !slices.Equal(logical.observations["servex_request_duration_seconds{client,grpc,catalog.get}"], []float64{.17}) {
			t.Fatalf("logical metrics = %v %v", logical.counters, logical.observations)
		}
		if attempts.counters["servex_client_attempt_total{catalog.get,Unavailable}"] != 2 || attempts.counters["servex_client_attempt_total{catalog.get,OK}"] != 1 {
			t.Fatalf("attempt metrics = %v", attempts.counters)
		}
	})
}
