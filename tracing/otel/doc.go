// Package otel adapts OpenTelemetry tracing to the servex tracing API.
//
// NewTracer wraps an oteltrace.Tracer as a tracing.Tracer and NewPropagator
// wraps a propagation.TextMapPropagator as a tracing.Propagator operating on
// any tracing.Carrier, such as metadata.MD.
//
// Spans interoperate with OpenTelemetry in both directions: a context
// returned by the Tracer carries the new span under the OpenTelemetry key, so
// oteltrace.SpanFromContext and OpenTelemetry instrumentation see it, and
// under the servex key, so tracing.SpanFromContext returns it. Spans started
// directly through OpenTelemetry become parents of spans started through the
// Tracer.
package otel
