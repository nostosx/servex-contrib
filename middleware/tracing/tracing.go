package tracing

import (
	"context"

	nooptracing "github.com/nostosx/servex-contrib/tracing/noop"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/observability/tracing"
)

// Span attribute keys set by the middleware.
const (
	AttrProtocol  = "servex.protocol"
	AttrOperation = "servex.operation"
	AttrCode      = "servex.code"
)

// New returns middleware that traces every call with t, propagating trace
// context with p. A nil t uses nooptracing.New() and a nil p uses
// nooptracing.NewPropagator().
//
// The span is named after Info.Operation and its kind follows Info.Kind of
// the metadata.Info in the context:
//
//   - server: the trace context is extracted from Info.Header with p, then a
//     tracing.SpanKindServer span is started as its child;
//   - client: a tracing.SpanKindClient span is started and then injected into
//     Info.Header with p, so the outbound request carries the client span. If
//     Info.Header is nil, a fresh header is created and the next endpoint
//     receives a context carrying it;
//   - otherwise, including when the context has no Info, a
//     tracing.SpanKindInternal span is started and nothing is propagated.
//
// The span gets the attributes AttrProtocol, AttrOperation and AttrCode, the
// errorx.Code name of the result. Following the OpenTelemetry RPC and HTTP
// conventions, Span.RecordError, which marks the span status as an error, is
// called for every failed client call, but for server and internal calls only
// when the code is a server fault (see IsServerFault). Client-class failures
// such as NotFound or InvalidArgument leave a server span's status unset; the
// code stays visible in AttrCode. The span ends when the next endpoint returns.
func New(t tracing.Tracer, p tracing.Propagator) endpoint.Middleware {
	if t == nil {
		t = nooptracing.New()
	}
	if p == nil {
		p = nooptracing.NewPropagator()
	}
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			info, _ := metadata.FromContext(ctx)
			kind := tracing.SpanKindInternal
			switch info.Kind {
			case metadata.KindServer:
				kind = tracing.SpanKindServer
				if info.Header != nil {
					ctx = p.Extract(ctx, info.Header)
				}
			case metadata.KindClient:
				kind = tracing.SpanKindClient
			}

			ctx, span := t.Start(ctx, info.Operation, kind)
			defer span.End()
			span.SetAttribute(AttrProtocol, info.Protocol)
			span.SetAttribute(AttrOperation, info.Operation)

			if kind == tracing.SpanKindClient {
				if info.Header == nil {
					info.Header = make(metadata.MD)
					ctx = metadata.NewContext(ctx, info)
				}
				p.Inject(ctx, info.Header)
			}

			resp, err := next(ctx, req)
			span.SetAttribute(AttrCode, errorx.CodeOf(err).String())
			if err != nil && (kind == tracing.SpanKindClient || IsServerFault(errorx.CodeOf(err))) {
				span.RecordError(err)
			}
			return resp, err
		}
	}
}

// IsServerFault reports whether code means the callee failed, which marks a
// server or internal span as an error. The set follows the OpenTelemetry gRPC
// server-span convention: Unknown, DeadlineExceeded, Unimplemented, Internal,
// Unavailable and DataLoss. Every other code, including OK and client-class
// codes such as Canceled, InvalidArgument, NotFound, AlreadyExists,
// PermissionDenied, ResourceExhausted, FailedPrecondition, Aborted, OutOfRange
// and Unauthenticated, is not a server fault.
func IsServerFault(code errorx.Code) bool {
	switch code {
	case errorx.Unknown, errorx.DeadlineExceeded, errorx.Unimplemented,
		errorx.Internal, errorx.Unavailable, errorx.DataLoss:
		return true
	default:
		return false
	}
}
