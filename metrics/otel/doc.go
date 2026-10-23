// Package otel implements the servex metrics.Meter on top of an
// OpenTelemetry metric.Meter. Counters map to Float64Counter, histograms to
// Float64Histogram with explicit bucket boundaries, and gauges to
// Float64Gauge with the current value of each label set tracked by the
// adapter so that both Set and Add are supported. Label values become
// string attributes keyed by the declared label names.
//
// Invalid metric definitions and bindings yield no-op metrics. Updates with
// incomplete label values, negative counter deltas or NaN counter deltas are
// dropped without disabling later valid updates. Problems are reported through
// the configured logger; dropped updates and invalid bindings are logged at
// most once per metric family.
package otel
