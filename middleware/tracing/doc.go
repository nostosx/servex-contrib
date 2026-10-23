// Package tracing provides middleware that starts a span for every call:
// server calls continue the trace extracted from the inbound header and
// client calls inject their span into the outbound header.
package tracing
