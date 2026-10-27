package mongodb

import (
	"context"
	"slices"
	"strings"
	"sync"

	"github.com/nostosx/servex/observability/metrics"
	"github.com/nostosx/servex/observability/tracing"
)

// recSpan records what was done to a span.
type recSpan struct {
	name   string
	kind   tracing.SpanKind
	parent context.Context

	mu    sync.Mutex
	attrs map[string]any
	errs  []error
	ended int
}

func (s *recSpan) End() { s.mu.Lock(); s.ended++; s.mu.Unlock() }

func (s *recSpan) SetAttribute(key string, value any) {
	s.mu.Lock()
	s.attrs[key] = value
	s.mu.Unlock()
}

func (s *recSpan) RecordError(err error) { s.mu.Lock(); s.errs = append(s.errs, err); s.mu.Unlock() }
func (s *recSpan) TraceID() string       { return "" }
func (s *recSpan) SpanID() string        { return "" }

func (s *recSpan) endCount() int { s.mu.Lock(); defer s.mu.Unlock(); return s.ended }

// recTracer records every started span.
type recTracer struct {
	mu    sync.Mutex
	spans []*recSpan
}

func (t *recTracer) Start(ctx context.Context, name string, kind tracing.SpanKind) (context.Context, tracing.Span) {
	s := &recSpan{name: name, kind: kind, parent: ctx, attrs: map[string]any{}}
	t.mu.Lock()
	t.spans = append(t.spans, s)
	t.mu.Unlock()
	return tracing.ContextWithSpan(ctx, s), s
}

func (t *recTracer) all() []*recSpan {
	t.mu.Lock()
	defer t.mu.Unlock()
	return slices.Clone(t.spans)
}

// recMeter records metric definitions and values keyed by
// "name{v1,v2,...}".
type recMeter struct {
	mu      sync.Mutex
	labels  map[string][]string
	buckets map[string][]float64
	counts  map[string]float64
	obs     map[string][]float64
	ctxs    []context.Context // context of every Add and Observe
}

func newRecMeter() *recMeter {
	return &recMeter{
		labels:  map[string][]string{},
		buckets: map[string][]float64{},
		counts:  map[string]float64{},
		obs:     map[string][]float64{},
	}
}

func seriesKey(name string, values []string) string {
	return name + "{" + strings.Join(values, ",") + "}"
}

func (m *recMeter) Counter(name, _ string, labelNames ...string) metrics.Counter {
	m.mu.Lock()
	m.labels[name] = labelNames
	m.mu.Unlock()
	return &recCounter{m: m, name: name}
}

func (m *recMeter) Gauge(name, _ string, labelNames ...string) metrics.Gauge {
	m.mu.Lock()
	m.labels[name] = labelNames
	m.mu.Unlock()
	return metrics.Noop().Gauge(name, "")
}

func (m *recMeter) Histogram(name, _ string, buckets []float64, labelNames ...string) metrics.Histogram {
	m.mu.Lock()
	m.labels[name] = labelNames
	m.buckets[name] = buckets
	m.mu.Unlock()
	return &recHistogram{m: m, name: name}
}

func (m *recMeter) contexts() []context.Context {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.ctxs)
}

func (m *recMeter) count(name string, values ...string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counts[seriesKey(name, values)]
}

func (m *recMeter) observations(name string, values ...string) []float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.obs[seriesKey(name, values)])
}

type recCounter struct {
	m      *recMeter
	name   string
	values []string
}

func (c *recCounter) With(values ...string) metrics.Counter {
	return &recCounter{m: c.m, name: c.name, values: slices.Concat(c.values, values)}
}

func (c *recCounter) Add(ctx context.Context, delta float64) {
	c.m.mu.Lock()
	c.m.ctxs = append(c.m.ctxs, ctx)
	c.m.counts[seriesKey(c.name, c.values)] += delta
	c.m.mu.Unlock()
}

type recHistogram struct {
	m      *recMeter
	name   string
	values []string
}

func (h *recHistogram) With(values ...string) metrics.Histogram {
	return &recHistogram{m: h.m, name: h.name, values: slices.Concat(h.values, values)}
}

func (h *recHistogram) Observe(ctx context.Context, v float64) {
	h.m.mu.Lock()
	h.m.ctxs = append(h.m.ctxs, ctx)
	key := seriesKey(h.name, h.values)
	h.m.obs[key] = append(h.m.obs[key], v)
	h.m.mu.Unlock()
}
