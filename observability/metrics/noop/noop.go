package noop

import (
	"context"

	"github.com/nostosx/servex/observability/metrics"
)

// New returns a metrics.Meter whose metrics discard every observation. It does not
// allocate and is safe for concurrent use.
func New() metrics.Meter { return noopMeter{} }

// noopMeter is a metrics.Meter that creates no-op metrics.
type noopMeter struct{}

var (
	_ metrics.Meter     = noopMeter{}
	_ metrics.Counter   = noopCounter{}
	_ metrics.Gauge     = noopGauge{}
	_ metrics.Histogram = noopHistogram{}
)

func (noopMeter) Counter(string, string, ...string) metrics.Counter { return noopCounter{} }

func (noopMeter) Gauge(string, string, ...string) metrics.Gauge { return noopGauge{} }

func (noopMeter) Histogram(string, string, []float64, ...string) metrics.Histogram {
	return noopHistogram{}
}

// noopCounter is a metrics.Counter that discards every update.
type noopCounter struct{}

func (c noopCounter) With(...string) metrics.Counter { return c }
func (noopCounter) Add(context.Context, float64)     {}

// noopGauge is a metrics.Gauge that discards every update.
type noopGauge struct{}

func (g noopGauge) With(...string) metrics.Gauge { return g }
func (noopGauge) Set(context.Context, float64)   {}
func (noopGauge) Add(context.Context, float64)   {}

// noopHistogram is a metrics.Histogram that discards every observation.
type noopHistogram struct{}

func (h noopHistogram) With(...string) metrics.Histogram { return h }
func (noopHistogram) Observe(context.Context, float64)   {}
