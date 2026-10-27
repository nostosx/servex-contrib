package prometheus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"sync"
	"sync/atomic"

	prom "github.com/prometheus/client_golang/prometheus"

	"github.com/nostosx/servex/observability/metrics"
)

// Option configures the Meter returned by New.
type Option func(*options)

type options struct {
	namespace string
	logger    *slog.Logger
}

// WithNamespace prefixes every metric name with namespace and an underscore,
// as prometheus.BuildFQName does. The default is no namespace.
func WithNamespace(namespace string) Option {
	return func(o *options) { o.namespace = namespace }
}

// WithLogger sets the logger used to report misuse such as registration
// conflicts or a wrong number of label values. The default is slog.Default();
// a nil logger keeps the default.
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

// meter is a metrics.Meter backed by a prometheus.Registerer.
type meter struct {
	reg       prom.Registerer
	namespace string
	logger    *slog.Logger

	mu       sync.Mutex
	families map[string]*family // keyed by the unqualified metric name
}

var _ metrics.Meter = (*meter)(nil)

// New returns a metrics.Meter that registers its collectors on reg. A nil
// reg uses prometheus.DefaultRegisterer.
//
// Each metric name is registered once per Meter: calling Counter, Gauge or
// Histogram again with the same name, kind and label names returns a metric
// backed by the same collector, and the help text and buckets of the first
// call win. If reg already holds an identical collector, registered by
// another Meter or by the caller, that collector is reused. A name reused
// with a different kind or different label names, or a collector the
// registry rejects, yields a no-op metric and a log entry.
//
// With binds label values in the order the label names were declared. It may
// be called repeatedly; values accumulate, so m.Counter("c", "", "a", "b")
// .With("1").With("2") equals .With("1", "2"). Updates on a metric whose
// label values are incomplete, and any With call that supplies more values
// than labels were declared, are dropped without panicking and reported once
// per metric through the logger.
func New(reg prom.Registerer, opts ...Option) metrics.Meter {
	o := options{logger: slog.Default()}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if reg == nil {
		reg = prom.DefaultRegisterer
	}
	return &meter{
		reg:       reg,
		namespace: o.namespace,
		logger:    o.logger,
		families:  make(map[string]*family),
	}
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
// +Inf bound is allowed and implicit.
func (m *meter) Histogram(name, help string, buckets []float64, labelNames ...string) metrics.Histogram {
	f := m.family(kindHistogram, name, help, buckets, labelNames)
	if f == nil {
		return metrics.Noop().Histogram(name, help, buckets)
	}
	return newHistogram(f, nil)
}

// family returns the registered family for name, creating and registering
// it on first use. It returns nil, after logging, if the family cannot be
// used.
func (m *meter) family(k kind, name, help string, buckets []float64, labelNames []string) *family {
	m.mu.Lock()
	defer m.mu.Unlock()

	if f, ok := m.families[name]; ok {
		if f.kind != k || !slices.Equal(f.labels, labelNames) {
			m.logger.Warn("metrics/prometheus: metric name reused with a different definition; using a no-op metric",
				"name", name,
				"kind", k.String(), "labels", labelNames,
				"registered_kind", f.kind.String(), "registered_labels", f.labels)
			return nil
		}
		return f
	}

	f, err := m.register(k, name, help, buckets, slices.Clone(labelNames))
	if err != nil {
		m.logger.Warn("metrics/prometheus: cannot register metric; using a no-op metric",
			"name", name, "kind", k.String(), "error", err)
		return nil
	}
	m.families[name] = f
	return f
}

// register creates the collector for a new family and registers it on the
// registry, reusing an identical collector that is already registered.
func (m *meter) register(k kind, name, help string, buckets []float64, labels []string) (*family, error) {
	f := &family{kind: k, name: name, labels: labels, logger: m.logger}
	var c prom.Collector
	switch k {
	case kindCounter:
		f.counter = prom.NewCounterVec(prom.CounterOpts{Namespace: m.namespace, Name: name, Help: help}, labels)
		c = f.counter
	case kindGauge:
		f.gauge = prom.NewGaugeVec(prom.GaugeOpts{Namespace: m.namespace, Name: name, Help: help}, labels)
		c = f.gauge
	case kindHistogram:
		b, err := validBuckets(buckets)
		if err != nil {
			return nil, err
		}
		if slices.Contains(labels, "le") {
			return nil, errors.New(`metrics/prometheus: label name "le" is reserved for histograms`)
		}
		f.histogram = prom.NewHistogramVec(prom.HistogramOpts{
			Namespace: m.namespace, Name: name, Help: help, Buckets: b,
		}, labels)
		c = f.histogram
	}

	err := m.reg.Register(c)
	if err == nil {
		return f, nil
	}
	are, ok := errors.AsType[prom.AlreadyRegisteredError](err)
	if !ok {
		return nil, fmt.Errorf("metrics/prometheus: register %s %q: %w", k, name, err)
	}
	switch existing := are.ExistingCollector.(type) {
	case *prom.CounterVec:
		if k == kindCounter {
			f.counter = existing
			return f, nil
		}
	case *prom.GaugeVec:
		if k == kindGauge {
			f.gauge = existing
			return f, nil
		}
	case *prom.HistogramVec:
		if k == kindHistogram {
			f.histogram = existing
			return f, nil
		}
	}
	return nil, fmt.Errorf("metrics/prometheus: register %s %q: existing collector has type %T", k, name, are.ExistingCollector)
}

// validBuckets returns a copy of buckets, or metrics.DefaultBuckets when
// buckets is empty, after checking that the bounds are strictly increasing.
func validBuckets(buckets []float64) ([]float64, error) {
	if len(buckets) == 0 {
		return slices.Clone(metrics.DefaultBuckets), nil
	}
	for i, b := range buckets {
		if math.IsNaN(b) {
			return nil, errors.New("metrics/prometheus: histogram bucket is NaN")
		}
		if i > 0 && buckets[i-1] >= b {
			return nil, fmt.Errorf("metrics/prometheus: histogram buckets must be strictly increasing: %v >= %v", buckets[i-1], b)
		}
	}
	return slices.Clone(buckets), nil
}

// family is a registered metric vector together with its declared labels.
type family struct {
	kind   kind
	name   string
	labels []string
	logger *slog.Logger

	counter   *prom.CounterVec
	gauge     *prom.GaugeVec
	histogram *prom.HistogramVec

	warned atomic.Bool
}

// warn reports a dropped update or binding. Only the first problem of each
// family is logged, so misuse on a hot path cannot flood the log.
func (f *family) warn(msg string, args ...any) {
	if !f.warned.CompareAndSwap(false, true) {
		return
	}
	args = append([]any{"name", f.name, "kind", f.kind.String(), "labels", f.labels}, args...)
	f.logger.Warn("metrics/prometheus: "+msg+" (further problems with this metric are not logged)", args...)
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

// resolve resolves the fully bound child metric for values, converting any
// panic raised by the client library into an error.
func resolve[T any](get func(...string) (T, error), values []string) (m T, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("metrics/prometheus: %v", r)
		}
	}()
	return get(values...)
}

// counter is a metrics.Counter. c is non-nil once every label value is bound.
type counter struct {
	f      *family
	values []string
	c      prom.Counter
}

var _ metrics.Counter = (*counter)(nil)

func newCounter(f *family, values []string) metrics.Counter {
	c := &counter{f: f, values: values}
	if len(values) == len(f.labels) {
		child, err := resolve(f.counter.GetMetricWithLabelValues, values)
		if err != nil {
			f.warn("invalid label values; using a no-op metric", "label_values", values, "error", err)
			return metrics.Noop().Counter(f.name, "")
		}
		c.c = child
	}
	return c
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
func (c *counter) Add(_ context.Context, delta float64) {
	switch {
	case c.c == nil:
		c.f.warn("update dropped: label values are incomplete", "label_values", c.values)
	case delta < 0 || math.IsNaN(delta):
		c.f.warn("update dropped: counter delta must not be negative or NaN", "delta", delta)
	default:
		c.c.Add(delta)
	}
}

// gauge is a metrics.Gauge. g is non-nil once every label value is bound.
type gauge struct {
	f      *family
	values []string
	g      prom.Gauge
}

var _ metrics.Gauge = (*gauge)(nil)

func newGauge(f *family, values []string) metrics.Gauge {
	g := &gauge{f: f, values: values}
	if len(values) == len(f.labels) {
		child, err := resolve(f.gauge.GetMetricWithLabelValues, values)
		if err != nil {
			f.warn("invalid label values; using a no-op metric", "label_values", values, "error", err)
			return metrics.Noop().Gauge(f.name, "")
		}
		g.g = child
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
func (g *gauge) Set(_ context.Context, v float64) {
	if g.g == nil {
		g.f.warn("update dropped: label values are incomplete", "label_values", g.values)
		return
	}
	g.g.Set(v)
}

// Add implements metrics.Gauge.
func (g *gauge) Add(_ context.Context, delta float64) {
	if g.g == nil {
		g.f.warn("update dropped: label values are incomplete", "label_values", g.values)
		return
	}
	g.g.Add(delta)
}

// histogram is a metrics.Histogram. o is non-nil once every label value is
// bound.
type histogram struct {
	f      *family
	values []string
	o      prom.Observer
}

var _ metrics.Histogram = (*histogram)(nil)

func newHistogram(f *family, values []string) metrics.Histogram {
	h := &histogram{f: f, values: values}
	if len(values) == len(f.labels) {
		child, err := resolve(f.histogram.GetMetricWithLabelValues, values)
		if err != nil {
			f.warn("invalid label values; using a no-op metric", "label_values", values, "error", err)
			return metrics.Noop().Histogram(f.name, "", nil)
		}
		h.o = child
	}
	return h
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
func (h *histogram) Observe(_ context.Context, v float64) {
	if h.o == nil {
		h.f.warn("update dropped: label values are incomplete", "label_values", h.values)
		return
	}
	h.o.Observe(v)
}
