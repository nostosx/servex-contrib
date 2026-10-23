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
	c.With("GET", "200").Add(1)
	c.With("GET", "200").Add(2)
	c.With("POST").With("500").Add(1)

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
	s.Add(2) // starts from zero
	s.Set(5)
	s.Add(2)
	s.Add(-3)
	g.With("client").Add(-1)
	h.meter.Gauge("temperature", "Unlabelled.").Set(21.5)

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
	h.meter.Gauge("shared", "h", "l").With("v").Set(10)
	h.meter.Gauge("shared", "h", "l").With("v").Add(1)
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
				g.With("v").Add(2)
			} else {
				g.With("v").Add(-1)
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
	hist.With("get").Observe(0.05)
	hist.With("get").Observe(0.5)
	hist.With("get").Observe(5)

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
	h.meter.Histogram("d_seconds", "D.", nil).Observe(0.2)
	got := h.metric(t, "d_seconds").Data.(metricdata.Histogram[float64])
	if b := got.DataPoints[0].Bounds; !slices.Equal(b, metrics.DefaultBuckets) {
		t.Fatalf("bounds = %v, want %v", b, metrics.DefaultBuckets)
	}
}

func TestSameNameReturnsEquivalentMetric(t *testing.T) {
	h := newHarness(t)
	h.meter.Counter("c_total", "C.", "l").With("a").Add(1)
	h.meter.Counter("c_total", "Other help is ignored.", "l").With("a").Add(1)
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
				c.Add(1)
				c.With("x").Add(1)
			},
			log: "label values are incomplete",
		},
		{
			name: "too few gauge label values",
			use: func(m metrics.Meter) {
				g := m.Gauge("few_gauge", "h", "a")
				g.Set(1)
				g.Add(1)
			},
			log: "label values are incomplete",
		},
		{
			name: "too few histogram label values",
			use:  func(m metrics.Meter) { m.Histogram("few_hist", "h", nil, "a").Observe(1) },
			log:  "label values are incomplete",
		},
		{
			name: "too many label values",
			use: func(m metrics.Meter) {
				m.Counter("many_total", "h", "a").With("x", "y").Add(1)
				m.Gauge("many_gauge", "h").With("x").Set(1)
				m.Histogram("many_hist", "h", nil, "a").With("x").With("y").Observe(1)
			},
			log: "too many label values",
		},
		{
			name: "negative counter delta",
			use:  func(m metrics.Meter) { m.Counter("neg_total", "h").Add(-1) },
			log:  "must not be negative",
		},
		{
			name: "kind conflict",
			use: func(m metrics.Meter) {
				m.Counter("conflict", "h").Add(1)
				m.Gauge("conflict", "h").Set(1)
			},
			log: "different definition",
		},
		{
			name: "label conflict",
			use: func(m metrics.Meter) {
				m.Counter("labels_total", "h", "a").With("x").Add(1)
				m.Counter("labels_total", "h", "b").With("x").Add(1)
			},
			log: "different definition",
		},
		{
			name: "unsorted buckets",
			use:  func(m metrics.Meter) { m.Histogram("bad_buckets", "h", []float64{1, 0.5}).Observe(1) },
			log:  "strictly increasing",
		},
		{
			name: "NaN bucket",
			use:  func(m metrics.Meter) { m.Histogram("nan_buckets", "h", []float64{math.NaN()}).Observe(1) },
			log:  "not finite",
		},
		{
			name: "invalid instrument name",
			use:  func(m metrics.Meter) { m.Counter("1bad", "h").Add(1) },
			log:  "cannot create instrument",
		},
		{
			name: "duplicate label names",
			use:  func(m metrics.Meter) { m.Gauge("dup", "h", "a", "a").With("1", "2").Set(1) },
			log:  "duplicate label name",
		},
		{
			name: "empty label name",
			use:  func(m metrics.Meter) { m.Gauge("empty_label", "h", "").With("1").Set(1) },
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
	c.Add(1)
	c.With("x", "y").Add(1)
	c.With("x").Add(-1)
	if m, ok := h.find(t, "dropped_total"); ok && len(m.Data.(metricdata.Sum[float64]).DataPoints) != 0 {
		t.Fatalf("misuse recorded data: %+v", m.Data)
	}
}

func TestMisuseLoggedOncePerMetric(t *testing.T) {
	h := newHarness(t)
	c := h.meter.Counter("once_total", "h", "a")
	for range 10 {
		c.Add(1)
		c.With("x", "y").Add(1)
	}
	if n := strings.Count(h.log.String(), "\n"); n != 1 {
		t.Fatalf("logged %d lines, want 1:\n%s", n, h.log)
	}
}

func TestNilMeter(t *testing.T) {
	m := otel.New(nil)
	m.Counter("c", "h", "l").With("v").Add(1)
	m.Gauge("g", "h").Add(1)
	m.Histogram("h", "h", nil).Observe(1)
}

func TestConcurrentUse(t *testing.T) {
	h := newHarness(t)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			h.meter.Counter("conc_total", "h", "l").With("v").Add(1)
			h.meter.Histogram("conc_hist", "h", metrics.DefaultBuckets, "l").With("v").Observe(0.1)
		})
	}
	wg.Wait()
	got := h.metric(t, "conc_total").Data.(metricdata.Sum[float64])
	if len(got.DataPoints) != 1 || got.DataPoints[0].Value != 32 {
		t.Fatalf("counter = %+v, want 32", got.DataPoints)
	}
}
