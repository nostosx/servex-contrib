package prometheus_test

import (
	"bytes"
	"log/slog"
	"math"
	"strings"
	"sync"
	"testing"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/nostosx/servex/observability/metrics"

	"github.com/nostosx/servex-contrib/metrics/prometheus"
)

// newMeter returns a Meter on a fresh registry together with a buffer that
// captures its log output.
func newMeter(t *testing.T, opts ...prometheus.Option) (metrics.Meter, *prom.Registry, *bytes.Buffer) {
	t.Helper()
	reg := prom.NewPedanticRegistry()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	return prometheus.New(reg, append([]prometheus.Option{prometheus.WithLogger(logger)}, opts...)...), reg, &buf
}

func TestCounter(t *testing.T) {
	m, reg, _ := newMeter(t, prometheus.WithNamespace("servex"))
	c := m.Counter("requests_total", "Total requests.", "method", "code")
	c.With("GET", "200").Add(1)
	c.With("GET", "200").Add(2)
	c.With("POST").With("500").Add(1)

	want := `
# HELP servex_requests_total Total requests.
# TYPE servex_requests_total counter
servex_requests_total{code="200",method="GET"} 3
servex_requests_total{code="500",method="POST"} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "servex_requests_total"); err != nil {
		t.Fatal(err)
	}
}

func TestGauge(t *testing.T) {
	m, reg, _ := newMeter(t)
	g := m.Gauge("in_flight", "In-flight requests.", "kind")
	s := g.With("server")
	s.Set(5)
	s.Add(2)
	s.Add(-3)
	m.Gauge("temperature", "Unlabelled.").Set(21.5)

	want := `
# HELP in_flight In-flight requests.
# TYPE in_flight gauge
in_flight{kind="server"} 4
# HELP temperature Unlabelled.
# TYPE temperature gauge
temperature 21.5
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "in_flight", "temperature"); err != nil {
		t.Fatal(err)
	}
}

func TestHistogram(t *testing.T) {
	m, reg, _ := newMeter(t)
	h := m.Histogram("latency_seconds", "Latency.", []float64{0.1, 1}, "op")
	h.With("get").Observe(0.05)
	h.With("get").Observe(0.5)
	h.With("get").Observe(5)

	want := `
# HELP latency_seconds Latency.
# TYPE latency_seconds histogram
latency_seconds_bucket{op="get",le="0.1"} 1
latency_seconds_bucket{op="get",le="1"} 2
latency_seconds_bucket{op="get",le="+Inf"} 3
latency_seconds_sum{op="get"} 5.55
latency_seconds_count{op="get"} 3
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "latency_seconds"); err != nil {
		t.Fatal(err)
	}
}

func TestHistogramDefaultBuckets(t *testing.T) {
	m, reg, _ := newMeter(t)
	m.Histogram("d_seconds", "D.", nil).Observe(0.2)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(mfs) != 1 {
		t.Fatalf("gathered %d families, want 1", len(mfs))
	}
	got := mfs[0].GetMetric()[0].GetHistogram().GetBucket()
	if len(got) != len(metrics.DefaultBuckets) {
		t.Fatalf("got %d buckets, want %d", len(got), len(metrics.DefaultBuckets))
	}
	for i, b := range got {
		if b.GetUpperBound() != metrics.DefaultBuckets[i] {
			t.Fatalf("bucket %d = %v, want %v", i, b.GetUpperBound(), metrics.DefaultBuckets[i])
		}
	}
}

func TestSameNameReturnsEquivalentMetric(t *testing.T) {
	m, reg, _ := newMeter(t)
	m.Counter("c_total", "C.", "l").With("a").Add(1)
	m.Counter("c_total", "Other help is ignored.", "l").With("a").Add(1)
	if got := value(t, reg, "c_total"); got != 2 {
		t.Fatalf("counter = %v, want 2", got)
	}
}

func TestReusesCollectorAcrossMeters(t *testing.T) {
	reg := prom.NewRegistry()
	discard := prometheus.WithLogger(slog.New(slog.DiscardHandler))
	a := prometheus.New(reg, discard)
	b := prometheus.New(reg, discard)
	a.Counter("shared_total", "Shared.", "l").With("x").Add(1)
	b.Counter("shared_total", "Shared.", "l").With("x").Add(2)
	a.Gauge("shared_gauge", "Shared.").Set(1)
	b.Gauge("shared_gauge", "Shared.").Add(1)
	a.Histogram("shared_hist", "Shared.", []float64{1}).Observe(0.5)
	b.Histogram("shared_hist", "Shared.", []float64{1}).Observe(0.5)

	want := `
# HELP shared_total Shared.
# TYPE shared_total counter
shared_total{l="x"} 3
# HELP shared_gauge Shared.
# TYPE shared_gauge gauge
shared_gauge 2
# HELP shared_hist Shared.
# TYPE shared_hist histogram
shared_hist_bucket{le="1"} 2
shared_hist_bucket{le="+Inf"} 2
shared_hist_sum 1
shared_hist_count 2
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "shared_total", "shared_gauge", "shared_hist"); err != nil {
		t.Fatal(err)
	}
}

func TestReuseExistingCollectorRegisteredByCaller(t *testing.T) {
	reg := prom.NewRegistry()
	existing := prom.NewCounterVec(prom.CounterOpts{Name: "pre_total", Help: "Pre."}, []string{"l"})
	reg.MustRegister(existing)
	m := prometheus.New(reg, prometheus.WithLogger(slog.New(slog.DiscardHandler)))
	m.Counter("pre_total", "Pre.", "l").With("v").Add(4)
	if got := testutil.ToFloat64(existing.WithLabelValues("v")); got != 4 {
		t.Fatalf("existing collector = %v, want 4", got)
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
			name: "NaN counter delta",
			use:  func(m metrics.Meter) { m.Counter("nan_total", "h").Add(math.NaN()) },
			log:  "must not be negative or NaN",
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
			log:  "NaN",
		},
		{
			name: "reserved le label",
			use:  func(m metrics.Meter) { m.Histogram("le_hist", "h", nil, "le").With("x").Observe(1) },
			log:  "reserved",
		},
		{
			name: "invalid metric name",
			use:  func(m metrics.Meter) { m.Counter("\xff", "h").Add(1) },
			log:  "cannot register",
		},
		{
			name: "duplicate label names",
			use:  func(m metrics.Meter) { m.Gauge("dup", "h", "a", "a").With("1", "2").Set(1) },
			log:  "cannot register",
		},
		{
			name: "invalid UTF-8 label value",
			use:  func(m metrics.Meter) { m.Counter("utf8_total", "h", "a").With("\xff").Add(1) },
			log:  "invalid label values",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, reg, buf := newMeter(t)
			tt.use(m)
			if !strings.Contains(buf.String(), tt.log) {
				t.Fatalf("log %q does not contain %q", buf.String(), tt.log)
			}
			if _, err := reg.Gather(); err != nil {
				t.Fatalf("Gather after misuse: %v", err)
			}
		})
	}
}

func TestMisuseLoggedOncePerMetric(t *testing.T) {
	m, _, buf := newMeter(t)
	c := m.Counter("once_total", "h", "a")
	for range 10 {
		c.Add(1)
		c.With("x", "y").Add(1)
	}
	if n := strings.Count(buf.String(), "\n"); n != 1 {
		t.Fatalf("logged %d lines, want 1:\n%s", n, buf.String())
	}
}

func TestBindingDoesNotAliasValues(t *testing.T) {
	m, reg, _ := newMeter(t)
	base := m.Counter("alias_total", "h", "a", "b").With("x")
	base.With("1").Add(1)
	base.With("2").Add(1)
	want := `
# HELP alias_total h
# TYPE alias_total counter
alias_total{a="x",b="1"} 1
alias_total{a="x",b="2"} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "alias_total"); err != nil {
		t.Fatal(err)
	}
}

func TestNilRegistererUsesDefault(t *testing.T) {
	// Only check that construction succeeds; registering on the global
	// default registry would leak state between tests.
	if prometheus.New(nil) == nil {
		t.Fatal("New(nil) returned nil")
	}
}

func TestConcurrentUse(t *testing.T) {
	m, reg, _ := newMeter(t)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			m.Counter("conc_total", "h", "l").With("v").Add(1)
			m.Gauge("conc_gauge", "h", "l").With("v").Add(1)
			m.Histogram("conc_hist", "h", metrics.DefaultBuckets, "l").With("v").Observe(0.1)
		})
	}
	wg.Wait()
	if got := value(t, reg, "conc_total"); got != 32 {
		t.Fatalf("counter = %v, want 32", got)
	}
	if got := value(t, reg, "conc_gauge"); got != 32 {
		t.Fatalf("gauge = %v, want 32", got)
	}
}

// value gathers reg and returns the value of the single series of the named
// counter or gauge family.
func value(t *testing.T, reg *prom.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		if len(mf.GetMetric()) != 1 {
			t.Fatalf("%s has %d series, want 1", name, len(mf.GetMetric()))
		}
		pm := mf.GetMetric()[0]
		if c := pm.GetCounter(); c != nil {
			return c.GetValue()
		}
		return pm.GetGauge().GetValue()
	}
	t.Fatalf("family %s not found", name)
	return 0
}
