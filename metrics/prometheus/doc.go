// Package prometheus implements the servex metrics.Meter on top of the
// Prometheus Go client. Counters, gauges and histograms map to CounterVec,
// GaugeVec and HistogramVec collectors registered on a caller-supplied
// prometheus.Registerer.
//
// Invalid metric definitions and bindings yield no-op metrics. Updates with
// incomplete label values, negative counter deltas or NaN counter deltas are
// dropped without disabling later valid updates. Problems are reported through
// the configured logger; dropped updates and invalid bindings are logged at
// most once per metric family.
package prometheus
