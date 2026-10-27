package prometheus_test

import (
	"log/slog"
	"strings"
	"testing"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/nostosx/servex/observability/metrics"

	"github.com/nostosx/servex-contrib/metrics/prometheus"
)

func TestIncompleteLabelsDropUpdates(t *testing.T) {
	tests := []struct {
		name string
		use  func(m metrics.Meter)
	}{
		{"gauge set", func(m metrics.Meter) { m.Gauge("partial", "h", "a", "b").With("x").Set(t.Context(), 1) }},
		{"gauge add", func(m metrics.Meter) { m.Gauge("partial", "h", "a", "b").Add(t.Context(), 1) }},
		{"histogram observe", func(m metrics.Meter) {
			m.Histogram("partial", "h", nil, "a", "b").With("x").Observe(t.Context(), 1)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, reg, buf := newMeter(t)
			tt.use(m)
			if !strings.Contains(buf.String(), "label values are incomplete") {
				t.Fatalf("log %q does not report the incomplete label values", buf.String())
			}
			if n, err := testutil.GatherAndCount(reg, "partial"); err != nil || n != 0 {
				t.Fatalf("GatherAndCount() = %d, %v, want no series", n, err)
			}
		})
	}
}

func TestInvalidLabelValuesUseNoop(t *testing.T) {
	tests := []struct {
		name string
		use  func(m metrics.Meter)
	}{
		{"gauge", func(m metrics.Meter) {
			g := m.Gauge("utf8", "h", "a")
			g.With("\xff").Set(t.Context(), 1)
			// The family stays usable with valid values.
			g.With("ok").Set(t.Context(), 2)
		}},
		{"histogram", func(m metrics.Meter) {
			h := m.Histogram("utf8", "h", []float64{1}, "a")
			h.With("\xff").Observe(t.Context(), 1)
			h.With("ok").Observe(t.Context(), 2)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, reg, buf := newMeter(t)
			tt.use(m)
			if !strings.Contains(buf.String(), "invalid label values") {
				t.Fatalf("log %q does not report the invalid label values", buf.String())
			}
			if n, err := testutil.GatherAndCount(reg, "utf8"); err != nil || n != 1 {
				t.Fatalf("GatherAndCount() = %d, %v, want only the valid series", n, err)
			}
		})
	}
}

func TestWithoutValuesReturnsSameMetric(t *testing.T) {
	m, reg, _ := newMeter(t)
	c := m.Counter("same_total", "h", "a").With("x")
	if got := c.With(); got != c {
		t.Errorf("Counter.With() = %v, want the receiver", got)
	}
	g := m.Gauge("same_gauge", "h", "a").With("x")
	if got := g.With(); got != g {
		t.Errorf("Gauge.With() = %v, want the receiver", got)
	}
	h := m.Histogram("same_hist", "h", []float64{1}, "a").With("x")
	if got := h.With(); got != h {
		t.Errorf("Histogram.With() = %v, want the receiver", got)
	}
	c.With().Add(t.Context(), 1)
	g.With().Set(t.Context(), 3)
	h.With().Observe(t.Context(), 0.5)

	want := `
# HELP same_total h
# TYPE same_total counter
same_total{a="x"} 1
# HELP same_gauge h
# TYPE same_gauge gauge
same_gauge{a="x"} 3
# HELP same_hist h
# TYPE same_hist histogram
same_hist_bucket{a="x",le="1"} 1
same_hist_bucket{a="x",le="+Inf"} 1
same_hist_sum{a="x"} 0.5
same_hist_count{a="x"} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "same_total", "same_gauge", "same_hist"); err != nil {
		t.Fatal(err)
	}
}

func TestExistingCollectorOfAnotherType(t *testing.T) {
	tests := []struct {
		name     string
		existing prom.Collector
		use      func(m metrics.Meter)
		typ      string
		series   int // series of the existing collector
	}{
		{
			// Kinds are not part of a Prometheus descriptor, so a gauge vector
			// registered by another Meter collides with the counter.
			name:     "vector of another kind",
			existing: prom.NewGaugeVec(prom.GaugeOpts{Name: "taken", Help: "h"}, []string{"l"}),
			use:      func(m metrics.Meter) { m.Counter("taken", "h", "l").With("x").Add(t.Context(), 1) },
			typ:      "*prometheus.GaugeVec",
			series:   0,
		},
		{
			name:     "counter vector for a histogram",
			existing: prom.NewCounterVec(prom.CounterOpts{Name: "taken", Help: "h"}, nil),
			use:      func(m metrics.Meter) { m.Histogram("taken", "h", nil).Observe(t.Context(), 1) },
			typ:      "*prometheus.CounterVec",
			series:   0,
		},
		{
			name:     "histogram vector for a gauge",
			existing: prom.NewHistogramVec(prom.HistogramOpts{Name: "taken", Help: "h"}, nil),
			use:      func(m metrics.Meter) { m.Gauge("taken", "h").Set(t.Context(), 1) },
			typ:      "*prometheus.HistogramVec",
			series:   0,
		},
		{
			name:     "plain metric",
			existing: prom.NewGauge(prom.GaugeOpts{Name: "taken", Help: "h"}),
			use:      func(m metrics.Meter) { m.Gauge("taken", "h").Set(t.Context(), 1) },
			typ:      "*prometheus.gauge",
			series:   1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prom.NewRegistry()
			reg.MustRegister(tt.existing)
			var buf strings.Builder
			m := prometheus.New(reg, prometheus.WithLogger(slog.New(slog.NewTextHandler(&buf, nil))))
			tt.use(m)
			if log := buf.String(); !strings.Contains(log, "cannot register") ||
				!strings.Contains(log, "existing collector has type "+tt.typ) {
				t.Fatalf("log %q does not report the existing collector type %s", log, tt.typ)
			}
			// The update went to a no-op metric, not to the existing collector.
			if n, err := testutil.GatherAndCount(reg, "taken"); err != nil || n != tt.series {
				t.Fatalf("GatherAndCount() = %d, %v, want %d", n, err, tt.series)
			}
			if g, ok := tt.existing.(prom.Gauge); ok {
				if got := testutil.ToFloat64(g); got != 0 {
					t.Fatalf("existing gauge = %v, want it untouched", got)
				}
			}
		})
	}
}
