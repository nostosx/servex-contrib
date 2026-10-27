package otel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/nostosx/servex/observability/metrics"
)

// Option configures the Meter returned by New.
type Option func(*options)

type options struct {
	logger *slog.Logger
}

// WithLogger sets the logger used to report misuse such as conflicting
// definitions or a wrong number of label values. The default is
// slog.Default(); a nil logger keeps the default.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.logger = l
		}
	}
}

// kind identifies the type of a metric family.
type kind uint8

const (
	kindCounter kind = iota + 1
	kindGauge
	kindHistogram
)

func (k kind) String() string {
	switch k {
	case kindCounter:
		return "counter"
	case kindGauge:
		return "gauge"
	case kindHistogram:
		return "histogram"
	default:
		return fmt.Sprintf("kind(%d)", uint8(k))
	}
}

// meter is a metrics.Meter backed by an OpenTelemetry metric.Meter.
type meter struct {
	m      otelmetric.Meter
	logger *slog.Logger

	mu       sync.Mutex
	families map[string]*family
}

var _ metrics.Meter = (*meter)(nil)

// New returns a metrics.Meter that creates its instruments on m. A nil m
// yields instruments that record nothing.
//
// Each metric name is created once per Meter: calling Counter, Gauge or
// Histogram again with the same name, kind and label names returns a metric
// backed by the same instrument (and, for gauges, the same tracked values),
// and the help text and buckets of the first call win. A name reused with a
// different kind or different label names, or an instrument the SDK
// rejects, yields a no-op metric and a log entry.
//
// Gauge values are tracked per label set inside the returned Meter, so two
// Meters wrapping the same OpenTelemetry meter do not share gauge state.
//
// With binds label values in the order the label names were declared. It may
// be called repeatedly; values accumulate, so m.Counter("c", "", "a", "b")
// .With("1").With("2") equals .With("1", "2"). Updates on a metric whose
// label values are incomplete, and any With call that supplies more values
// than labels were declared, are dropped without panicking and reported once
// per metric through the logger.
func New(m otelmetric.Meter, opts ...Option) metrics.Meter {
	o := options{logger: slog.Default()}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if m == nil {
		m = noop.Meter{}
	}
	return &meter{m: m, logger: o.logger, families: make(map[string]*family)}
}

// Counter implements metrics.Meter.
func (m *meter) Counter(name, help string, labelNames ...string) metrics.Counter {
	f := m.family(kindCounter, name, help, nil, labelNames)
	if f == nil {
		return metrics.Noop().Counter(name, help)
	}
	return newCounter(f, nil)
}

// Gauge implements metrics.Meter.
func (m *meter) Gauge(name, help string, labelNames ...string) metrics.Gauge {
	f := m.family(kindGauge, name, help, nil, labelNames)
	if f == nil {
		return metrics.Noop().Gauge(name, help)
	}
	return newGauge(f, nil)
}

// Histogram implements metrics.Meter. Nil or empty buckets use
// metrics.DefaultBuckets. Buckets must be strictly increasing; a trailing
// +Inf bound is allowed and dropped, as it is implicit.
func (m *meter) Histogram(name, help string, buckets []float64, labelNames ...string) metrics.Histogram {
	f := m.family(kindHistogram, name, help, buckets, labelNames)
	if f == nil {
		return metrics.Noop().Histogram(name, help, buckets)
	}
	return newHistogram(f, nil)
}

// family returns the family for name, creating its instrument on first use.
// It returns nil, after logging, if the family cannot be used.
func (m *meter) family(k kind, name, help string, buckets []float64, labelNames []string) *family {
	m.mu.Lock()
	defer m.mu.Unlock()

	if f, ok := m.families[name]; ok {
		if f.kind != k || !slices.Equal(f.labels, labelNames) {
			m.logger.Warn("metrics/otel: metric name reused with a different definition; using a no-op metric",
				"name", name,
				"kind", k.String(), "labels", labelNames,
				"registered_kind", f.kind.String(), "registered_labels", f.labels)
			return nil
		}
		return f
	}

	f, err := m.create(k, name, help, buckets, slices.Clone(labelNames))
	if err != nil {
		m.logger.Warn("metrics/otel: cannot create instrument; using a no-op metric",
			"name", name, "kind", k.String(), "error", err)
		return nil
	}
	m.families[name] = f
	return f
}

// create creates the instrument of a new family.
func (m *meter) create(k kind, name, help string, buckets []float64, labels []string) (*family, error) {
	if err := validLabels(labels); err != nil {
		return nil, err
	}
	f := &family{kind: k, name: name, labels: labels, logger: m.logger}
	var err error
	switch k {
	case kindCounter:
		f.counter, err = m.m.Float64Counter(name, otelmetric.WithDescription(help))
	case kindGauge:
		f.gauge, err = m.m.Float64Gauge(name, otelmetric.WithDescription(help))
	case kindHistogram:
		var b []float64
		if b, err = validBuckets(buckets); err != nil {
			return nil, err
		}
		f.histogram, err = m.m.Float64Histogram(name,
			otelmetric.WithDescription(help),
			otelmetric.WithExplicitBucketBoundaries(b...))
	}
	if err != nil {
		return nil, fmt.Errorf("metrics/otel: create %s %q: %w", k, name, err)
	}
	return f, nil
}

// validLabels checks that label names are non-empty and unique, since they
// become attribute keys of a single attribute set.
func validLabels(labels []string) error {
	for i, l := range labels {
		if l == "" {
			return errors.New("metrics/otel: empty label name")
		}
		if slices.Contains(labels[:i], l) {
			return fmt.Errorf("metrics/otel: duplicate label name %q", l)
		}
	}
	return nil
}

// validBuckets returns a copy of buckets, or metrics.DefaultBuckets when
// buckets is empty, after checking that the bounds are strictly increasing
// and finite. A trailing +Inf bound is dropped.
func validBuckets(buckets []float64) ([]float64, error) {
	if len(buckets) == 0 {
		return slices.Clone(metrics.DefaultBuckets), nil
	}
	if last := buckets[len(buckets)-1]; math.IsInf(last, +1) {
		buckets = buckets[:len(buckets)-1]
	}
	for i, b := range buckets {
		if math.IsNaN(b) || math.IsInf(b, 0) {
			return nil, fmt.Errorf("metrics/otel: histogram bucket %v is not finite", b)
		}
		if i > 0 && buckets[i-1] >= b {
			return nil, fmt.Errorf("metrics/otel: histogram buckets must be strictly increasing: %v >= %v", buckets[i-1], b)
		}
	}
	return slices.Clone(buckets), nil
}

// family is an instrument together with its declared labels.
type family struct {
	kind   kind
	name   string
	labels []string
	logger *slog.Logger

	counter   otelmetric.Float64Counter
	gauge     otelmetric.Float64Gauge
	histogram otelmetric.Float64Histogram

	// series maps attribute.Distinct to *series and holds the current value
	// of every gauge label set.
	series sync.Map

	warned atomic.Bool
}

// warn reports a dropped update or binding. Only the first problem of each
// family is logged, so misuse on a hot path cannot flood the log.
func (f *family) warn(msg string, args ...any) {
	if !f.warned.CompareAndSwap(false, true) {
		return
	}
	args = append([]any{"name", f.name, "kind", f.kind.String(), "labels", f.labels}, args...)
	f.logger.Warn("metrics/otel: "+msg+" (further problems with this metric are not logged)", args...)
}

// bind extends the label values prev with values. It reports false, after
// logging, when the result has more values than declared labels.
func (f *family) bind(prev, values []string) ([]string, bool) {
	all := slices.Concat(prev, values)
	if len(all) > len(f.labels) {
		f.warn("too many label values; using a no-op metric", "label_values", all)
		return nil, false
	}
	return all, true
}

// attributes returns the attribute set for a complete list of label values.
func (f *family) attributes(values []string) attribute.Set {
	kvs := make([]attribute.KeyValue, len(values))
	for i, v := range values {
		kvs[i] = attribute.String(f.labels[i], v)
	}
	return attribute.NewSet(kvs...)
}

// binding holds the label values bound so far. opt is non-nil once every
// label value is bound.
type binding struct {
	f      *family
	values []string
	opt    otelmetric.MeasurementOption
}

func newBinding(f *family, values []string) binding {
	b := binding{f: f, values: values}
	if len(values) == len(f.labels) {
		b.opt = otelmetric.WithAttributeSet(f.attributes(values))
	}
	return b
}

// complete reports whether every label value is bound, logging otherwise.
func (b *binding) complete() bool {
	if b.opt == nil {
		b.f.warn("update dropped: label values are incomplete", "label_values", b.values)
		return false
	}
	return true
}

// recordContext returns the context passed to an OpenTelemetry instrument:
// ctx itself, so that the SDK can sample exemplars from the span it carries,
// or context.Background for a nil ctx.
func recordContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// counter is a metrics.Counter.
type counter struct{ binding }

var _ metrics.Counter = (*counter)(nil)

func newCounter(f *family, values []string) *counter {
	return &counter{newBinding(f, values)}
}

// With implements metrics.Counter.
func (c *counter) With(labelValues ...string) metrics.Counter {
	if len(labelValues) == 0 {
		return c
	}
	values, ok := c.f.bind(c.values, labelValues)
	if !ok {
		return metrics.Noop().Counter(c.f.name, "")
	}
	return newCounter(c.f, values)
}

// Add implements metrics.Counter. Negative and NaN deltas are dropped.
func (c *counter) Add(ctx context.Context, delta float64) {
	if !c.complete() {
		return
	}
	if delta < 0 || math.IsNaN(delta) {
		c.f.warn("update dropped: counter delta must not be negative or NaN", "delta", delta)
		return
	}
	c.f.counter.Add(recordContext(ctx), delta, c.opt)
}

// series is the tracked value of one gauge label set. mu also serializes
// recording so the last recorded value always equals v.
type series struct {
	mu sync.Mutex
	v  float64
}

// gauge is a metrics.Gauge. s is non-nil once every label value is bound.
type gauge struct {
	binding
	s *series
}

var _ metrics.Gauge = (*gauge)(nil)

func newGauge(f *family, values []string) *gauge {
	g := &gauge{f: f, values: values}
	if len(values) == len(f.labels) {
		set := f.attributes(values)
		g.opt = otelmetric.WithAttributeSet(set)
		s, _ := f.series.LoadOrStore(set.Equivalent(), &series{})
		g.s = s.(*series)
	}
	return g
}

// With implements metrics.Gauge.
func (g *gauge) With(labelValues ...string) metrics.Gauge {
	if len(labelValues) == 0 {
		return g
	}
	values, ok := g.f.bind(g.values, labelValues)
	if !ok {
		return metrics.Noop().Gauge(g.f.name, "")
	}
	return newGauge(g.f, values)
}

// Set implements metrics.Gauge.
func (g *gauge) Set(ctx context.Context, v float64) {
	if !g.complete() {
		return
	}
	g.s.mu.Lock()
	defer g.s.mu.Unlock()
	g.s.v = v
	g.f.gauge.Record(recordContext(ctx), v, g.opt)
}

// Add implements metrics.Gauge.
func (g *gauge) Add(ctx context.Context, delta float64) {
	if !g.complete() {
		return
	}
	g.s.mu.Lock()
	defer g.s.mu.Unlock()
	g.s.v += delta
	g.f.gauge.Record(recordContext(ctx), g.s.v, g.opt)
}

// histogram is a metrics.Histogram.
type histogram struct{ binding }

var _ metrics.Histogram = (*histogram)(nil)

func newHistogram(f *family, values []string) *histogram {
	return &histogram{newBinding(f, values)}
}

// With implements metrics.Histogram.
func (h *histogram) With(labelValues ...string) metrics.Histogram {
	if len(labelValues) == 0 {
		return h
	}
	values, ok := h.f.bind(h.values, labelValues)
	if !ok {
		return metrics.Noop().Histogram(h.f.name, "", nil)
	}
	return newHistogram(h.f, values)
}

// Observe implements metrics.Histogram.
func (h *histogram) Observe(ctx context.Context, v float64) {
	if !h.complete() {
		return
	}
	h.f.histogram.Record(recordContext(ctx), v, h.opt)
}
