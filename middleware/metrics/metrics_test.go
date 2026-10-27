package metrics_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	mwmetrics "github.com/nostosx/servex-contrib/middleware/metrics"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/observability/metrics"
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
	return metrics.Noop().Gauge(name, help, labelNames...)
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

func TestDeclarations(t *testing.T) {
	m := newRecordingMeter()
	mwmetrics.New(m)
	if got := m.labelNames[mwmetrics.RequestsTotal]; !slices.Equal(got, []string{"kind", "protocol", "operation", "code"}) {
		t.Fatalf("counter labels = %v", got)
	}
	if got := m.labelNames[mwmetrics.RequestDuration]; !slices.Equal(got, []string{"kind", "protocol", "operation"}) {
		t.Fatalf("histogram labels = %v", got)
	}
	if got := m.buckets[mwmetrics.RequestDuration]; !slices.Equal(got, metrics.DefaultBuckets) {
		t.Fatalf("buckets = %v", got)
	}
}

func TestCustomBuckets(t *testing.T) {
	m := newRecordingMeter()
	b := []float64{0.1, 1}
	mwmetrics.New(m, nil, mwmetrics.WithBuckets(nil), mwmetrics.WithBuckets(b))
	b[0] = 42
	if got := m.buckets[mwmetrics.RequestDuration]; !slices.Equal(got, []float64{0.1, 1}) {
		t.Fatalf("buckets = %v", got)
	}
}

func TestRecordsCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := newRecordingMeter()
		mw := mwmetrics.New(m)
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
}

func TestNoInfo(t *testing.T) {
	m := newRecordingMeter()
	_, _ = mwmetrics.New(m)(func(context.Context, any) (any, error) { return nil, nil })(t.Context(), nil)
	if m.counters["servex_requests_total{,,,OK}"] != 1 {
		t.Fatalf("counters = %v", m.counters)
	}
	if len(m.observations["servex_request_duration_seconds{,,}"]) != 1 {
		t.Fatalf("observations = %v", m.observations)
	}
}

func TestNilMeter(t *testing.T) {
	resp, err := mwmetrics.New(nil)(func(context.Context, any) (any, error) { return "ok", nil })(t.Context(), nil)
	if err != nil || resp != "ok" {
		t.Fatalf("got (%v, %v)", resp, err)
	}
}
