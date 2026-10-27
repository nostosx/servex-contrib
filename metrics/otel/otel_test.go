package otel_test

import (
	"bytes"
	"context"
	"log/slog"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/nostosx/servex/observability/metrics"

	"github.com/nostosx/servex-contrib/metrics/otel"
)

// harness is a Meter backed by an SDK meter provider with a ManualReader.
type harness struct {
	meter  metrics.Meter
	reader *sdkmetric.ManualReader
	log    *bytes.Buffer
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := provider.Shutdown(ctx); err != nil {
			t.Errorf("shutdown meter provider: %v", err)
		}
	})
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	return &harness{
		meter:  otel.New(provider.Meter("test"), otel.WithLogger(logger)),
		reader: reader,
		log:    &buf,
	}
}

// metric collects and returns the named metric, failing if it is absent.
func (h *harness) metric(t *testing.T, name string) metricdata.Metrics {
	t.Helper()
	m, ok := h.find(t, name)
	if !ok {
		t.Fatalf("metric %q not collected", name)
	}
	return m
}

func (h *harness) find(t *testing.T, name string) (metricdata.Metrics, bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := h.reader.Collect(t.Context(), &rm); err != nil {
		t.Fatal(err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m, true
			}
		}
	}
	return metricdata.Metrics{}, false
}

func TestCounter(t *testing.T) {
	h := newHarness(t)
	c := h.meter.Counter("requests_total", "Total requests.", "method", "code")
	c.With("GET", "200").Add(t.Context(), 1)
	c.With("GET", "200").Add(t.Context(), 2)
	c.With("POST").With("500").Add(t.Context(), 1)

	want := metricdata.Metrics{
		Name:        "requests_total",
		Description: "Total requests.",
		Data: metricdata.Sum[float64]{
			Temporality: metricdata.CumulativeTemporality,
			IsMonotonic: true,
			DataPoints: []metricdata.DataPoint[float64]{
				{Attributes: attribute.NewSet(attribute.String("method", "GET"), attribute.String("code", "200")), Value: 3},
				{Attributes: attribute.NewSet(attribute.String("method", "POST"), attribute.String("code", "500")), Value: 1},
			},
		},
	}
	metricdatatest.AssertEqual(t, want, h.metric(t, "requests_total"),
		metricdatatest.IgnoreTimestamp(), metricdatatest.IgnoreExemplars())
}

func TestGaugeSetAndAdd(t *testing.T) {
	h := newHarness(t)
	g := h.meter.Gauge("in_flight", "In-flight requests.", "kind")
	s := g.With("server")
	s.Add(t.Context(), 2) // starts from zero
	s.Set(t.Context(), 5)
	s.Add(t.Context(), 2)
	s.Add(t.Context(), -3)
	g.With("client").Add(t.Context(), -1)
	h.meter.Gauge("temperature", "Unlabelled.").Set(t.Context(), 21.5)

	want := metricdata.Metrics{
		Name:        "in_flight",
		Description: "In-flight requests.",
		Data: metricdata.Gauge[float64]{
			DataPoints: []metricdata.DataPoint[float64]{
				{Attributes: attribute.NewSet(attribute.String("kind", "server")), Value: 4},
				{Attributes: attribute.NewSet(attribute.String("kind", "client")), Value: -1},
			},
		},
	}
	metricdatatest.AssertEqual(t, want, h.metric(t, "in_flight"),
		metricdatatest.IgnoreTimestamp(), metricdatatest.IgnoreExemplars())

	temp := h.metric(t, "temperature").Data.(metricdata.Gauge[float64])
	if len(temp.DataPoints) != 1 || temp.DataPoints[0].Value != 21.5 {
		t.Fatalf("temperature = %+v", temp.DataPoints)
	}
}

func TestGaugeStateSharedAcrossEquivalentCalls(t *testing.T) {
	h := newHarness(t)
	h.meter.Gauge("shared", "h", "l").With("v").Set(t.Context(), 10)
	h.meter.Gauge("shared", "h", "l").With("v").Add(t.Context(), 1)
	got := h.metric(t, "shared").Data.(metricdata.Gauge[float64])
	if len(got.DataPoints) != 1 || got.DataPoints[0].Value != 11 {
		t.Fatalf("shared gauge = %+v, want 11", got.DataPoints)
	}
}

func TestGaugeConcurrentAdd(t *testing.T) {
	h := newHarness(t)
	g := h.meter.Gauge("conc_gauge", "h", "l")
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Go(func() {
			if i%2 == 0 {
				g.With("v").Add(t.Context(), 2)
			} else {
				g.With("v").Add(t.Context(), -1)
			}
		})
	}
	wg.Wait()
	got := h.metric(t, "conc_gauge").Data.(metricdata.Gauge[float64])
	if len(got.DataPoints) != 1 || got.DataPoints[0].Value != 32 {
		t.Fatalf("gauge = %+v, want 32", got.DataPoints)
	}
}

func TestHistogram(t *testing.T) {
	h := newHarness(t)
	hist := h.meter.Histogram("latency_seconds", "Latency.", []float64{0.1, 1, math.Inf(1)}, "op")
	hist.With("get").Observe(t.Context(), 0.05)
	hist.With("get").Observe(t.Context(), 0.5)
	hist.With("get").Observe(t.Context(), 5)

	want := metricdata.Metrics{
		Name:        "latency_seconds",
		Description: "Latency.",
		Data: metricdata.Histogram[float64]{
			Temporality: metricdata.CumulativeTemporality,
			DataPoints: []metricdata.HistogramDataPoint[float64]{{
				Attributes:   attribute.NewSet(attribute.String("op", "get")),
				Count:        3,
				Bounds:       []float64{0.1, 1},
				BucketCounts: []uint64{1, 1, 1},
				Min:          metricdata.NewExtrema(0.05),
				Max:          metricdata.NewExtrema(5.0),
				Sum:          5.55,
			}},
		},
	}
	metricdatatest.AssertEqual(t, want, h.metric(t, "latency_seconds"),
		metricdatatest.IgnoreTimestamp(), metricdatatest.IgnoreExemplars())
}

func TestHistogramDefaultBuckets(t *testing.T) {
	h := newHarness(t)
	h.meter.Histogram("d_seconds", "D.", nil).Observe(t.Context(), 0.2)
	got := h.metric(t, "d_seconds").Data.(metricdata.Histogram[float64])
	if b := got.DataPoints[0].Bounds; !slices.Equal(b, metrics.DefaultBuckets) {
		t.Fatalf("bounds = %v, want %v", b, metrics.DefaultBuckets)
	}
}

func TestSameNameReturnsEquivalentMetric(t *testing.T) {
	h := newHarness(t)
	h.meter.Counter("c_total", "C.", "l").With("a").Add(t.Context(), 1)
	h.meter.Counter("c_total", "Other help is ignored.", "l").With("a").Add(t.Context(), 1)
	got := h.metric(t, "c_total").Data.(metricdata.Sum[float64])
	if len(got.DataPoints) != 1 || got.DataPoints[0].Value != 2 {
		t.Fatalf("counter = %+v, want 2", got.DataPoints)
	}
	if h.log.Len() != 0 {
		t.Fatalf("unexpected log output: %s", h.log)
	}
}

func TestMisuseDoesNotPanic(t *testing.T) {
	tests := []struct {
		name string
		use  func(m metrics.Meter)
		log  string
	}{
		{
			name: "too few label values",
			use: func(m metrics.Meter) {
				c := m.Counter("few_total", "h", "a", "b")
				c.Add(t.Context(), 1)
				c.With("x").Add(t.Context(), 1)
			},
			log: "label values are incomplete",
		},
		{
			name: "too few gauge label values",
			use: func(m metrics.Meter) {
				g := m.Gauge("few_gauge", "h", "a")
				g.Set(t.Context(), 1)
				g.Add(t.Context(), 1)
			},
			log: "label values are incomplete",
		},
		{
			name: "too few histogram label values",
			use:  func(m metrics.Meter) { m.Histogram("few_hist", "h", nil, "a").Observe(t.Context(), 1) },
			log:  "label values are incomplete",
		},
		{
			name: "too many label values",
			use: func(m metrics.Meter) {
				m.Counter("many_total", "h", "a").With("x", "y").Add(t.Context(), 1)
				m.Gauge("many_gauge", "h").With("x").Set(t.Context(), 1)
				m.Histogram("many_hist", "h", nil, "a").With("x").With("y").Observe(t.Context(), 1)
			},
			log: "too many label values",
		},
		{
			name: "negative counter delta",
			use:  func(m metrics.Meter) { m.Counter("neg_total", "h").Add(t.Context(), -1) },
			log:  "must not be negative",
		},
		{
			name: "kind conflict",
			use: func(m metrics.Meter) {
				m.Counter("conflict", "h").Add(t.Context(), 1)
				m.Gauge("conflict", "h").Set(t.Context(), 1)
			},
			log: "different definition",
		},
		{
			name: "label conflict",
			use: func(m metrics.Meter) {
				m.Counter("labels_total", "h", "a").With("x").Add(t.Context(), 1)
				m.Counter("labels_total", "h", "b").With("x").Add(t.Context(), 1)
			},
			log: "different definition",
		},
		{
			name: "unsorted buckets",
			use:  func(m metrics.Meter) { m.Histogram("bad_buckets", "h", []float64{1, 0.5}).Observe(t.Context(), 1) },
			log:  "strictly increasing",
		},
		{
			name: "NaN bucket",
			use:  func(m metrics.Meter) { m.Histogram("nan_buckets", "h", []float64{math.NaN()}).Observe(t.Context(), 1) },
			log:  "not finite",
		},
		{
			name: "invalid instrument name",
			use:  func(m metrics.Meter) { m.Counter("1bad", "h").Add(t.Context(), 1) },
			log:  "cannot create instrument",
		},
		{
			name: "duplicate label names",
			use:  func(m metrics.Meter) { m.Gauge("dup", "h", "a", "a").With("1", "2").Set(t.Context(), 1) },
			log:  "duplicate label name",
		},
		{
			name: "empty label name",
			use:  func(m metrics.Meter) { m.Gauge("empty_label", "h", "").With("1").Set(t.Context(), 1) },
			log:  "empty label name",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			tt.use(h.meter)
			if !strings.Contains(h.log.String(), tt.log) {
				t.Fatalf("log %q does not contain %q", h.log.String(), tt.log)
			}
		})
	}
}

func TestMisuseRecordsNothing(t *testing.T) {
	h := newHarness(t)
	c := h.meter.Counter("dropped_total", "h", "a")
	c.Add(t.Context(), 1)
	c.With("x", "y").Add(t.Context(), 1)
	c.With("x").Add(t.Context(), -1)
	if m, ok := h.find(t, "dropped_total"); ok && len(m.Data.(metricdata.Sum[float64]).DataPoints) != 0 {
		t.Fatalf("misuse recorded data: %+v", m.Data)
	}
}

func TestMisuseLoggedOncePerMetric(t *testing.T) {
	h := newHarness(t)
	c := h.meter.Counter("once_total", "h", "a")
	for range 10 {
		c.Add(t.Context(), 1)
		c.With("x", "y").Add(t.Context(), 1)
	}
	if n := strings.Count(h.log.String(), "\n"); n != 1 {
		t.Fatalf("logged %d lines, want 1:\n%s", n, h.log)
	}
}

func TestNilMeter(t *testing.T) {
	m := otel.New(nil)
	m.Counter("c", "h", "l").With("v").Add(t.Context(), 1)
	m.Gauge("g", "h").Add(t.Context(), 1)
	m.Histogram("h", "h", nil).Observe(t.Context(), 1)
}

func TestConcurrentUse(t *testing.T) {
	h := newHarness(t)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			h.meter.Counter("conc_total", "h", "l").With("v").Add(t.Context(), 1)
			h.meter.Histogram("conc_hist", "h", metrics.DefaultBuckets, "l").With("v").Observe(t.Context(), 0.1)
		})
	}
	wg.Wait()
	got := h.metric(t, "conc_total").Data.(metricdata.Sum[float64])
	if len(got.DataPoints) != 1 || got.DataPoints[0].Value != 32 {
		t.Fatalf("counter = %+v, want 32", got.DataPoints)
	}
}

// TestExemplarsFromContext checks that recording methods pass their context
// to the SDK, which samples exemplars from the span it carries.
func TestExemplarsFromContext(t *testing.T) {
	h := newHarness(t)
	sc := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID:    oteltrace.TraceID{1, 2, 3},
		SpanID:     oteltrace.SpanID{4, 5, 6},
		TraceFlags: oteltrace.FlagsSampled,
	})
	ctx := oteltrace.ContextWithSpanContext(t.Context(), sc)

	h.meter.Counter("ex_total", "h", "l").With("v").Add(ctx, 2)
	h.meter.Histogram("ex_seconds", "h", nil, "l").With("v").Observe(ctx, 0.3)
	h.meter.Counter("plain_total", "h").Add(t.Context(), 1)

	check := func(name string, exemplars []metricdata.Exemplar[float64], value float64) {
		t.Helper()
		if len(exemplars) != 1 {
			t.Fatalf("%s: %d exemplars, want 1", name, len(exemplars))
		}
		e := exemplars[0]
		traceID, spanID := sc.TraceID(), sc.SpanID()
		if !slices.Equal(e.TraceID, traceID[:]) || !slices.Equal(e.SpanID, spanID[:]) || e.Value != value {
			t.Fatalf("%s: exemplar %+v, want trace %s span %s value %v", name, e, sc.TraceID(), sc.SpanID(), value)
		}
	}
	sum := h.metric(t, "ex_total").Data.(metricdata.Sum[float64])
	check("counter", sum.DataPoints[0].Exemplars, 2)
	hist := h.metric(t, "ex_seconds").Data.(metricdata.Histogram[float64])
	check("histogram", hist.DataPoints[0].Exemplars, 0.3)
	if plain := h.metric(t, "plain_total").Data.(metricdata.Sum[float64]); len(plain.DataPoints[0].Exemplars) != 0 {
		t.Fatalf("exemplar recorded without a span: %+v", plain.DataPoints[0].Exemplars)
	}
}

// TestNilContext checks that a nil context, a misuse, does not panic.
func TestNilContext(t *testing.T) {
	h := newHarness(t)
	//nolint:staticcheck // SA1012: deliberately passing a nil context.
	h.meter.Counter("nil_total", "h").Add(nil, 1)
	if sum := h.metric(t, "nil_total").Data.(metricdata.Sum[float64]); sum.DataPoints[0].Value != 1 {
		t.Fatalf("value = %v, want 1", sum.DataPoints[0].Value)
	}
}
