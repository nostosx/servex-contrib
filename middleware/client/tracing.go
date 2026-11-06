package client

import (
	"context"

	"github.com/nostosx/servex-contrib/middleware/observe"
	nooptracing "github.com/nostosx/servex-contrib/observability/tracing/noop"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/observability/tracing"
	"github.com/nostosx/servex/transport"
)

// Span attribute keys set by the middleware.
const (
	AttrProtocol  = "servex.protocol"
	AttrOperation = "servex.operation"
	AttrCode      = "servex.code"
	AttrReason    = "servex.reason"
	AttrRetryable = "servex.retryable"
	AttrRemote    = "servex.remote"
)

// Tracing returns middleware that traces every call with t, propagating trace
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
// The span records protocol, operation and payload-free error classification
// attributes. Message text, metadata and causes are never recorded. Following the OpenTelemetry RPC and HTTP
// conventions, Span.RecordError, which marks the span status as an error, is
// called for every failed client call, but for server and internal calls only
// when the code is a server fault (see errorx.IsServerFault). Client-class failures
// such as NotFound or InvalidArgument leave a server span's status unset; the
// code stays visible in AttrCode. Panics and Goexit record Internal/INTERRUPTED
// and propagate unchanged. The span ends exactly once on every exit path.
func Tracing(t tracing.Tracer, p tracing.Propagator) endpoint.Middleware {
	if t == nil {
		t = nooptracing.New()
	}
	if p == nil {
		p = nooptracing.NewPropagator()
	}
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (resp any, err error) {
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
			ctx = tracing.ContextWithSpan(ctx, span)
			finished := false
			defer func() {
				defer span.End()
				class := observe.Outcome(err, finished)
				observe.Record(span, class, kind == tracing.SpanKindClient || errorx.IsServerFault(class.Code))
			}()
			span.SetAttribute(AttrProtocol, info.Protocol)
			span.SetAttribute(AttrOperation, info.Operation)

			if kind == tracing.SpanKindClient {
				if info.Header == nil {
					info.Header = make(metadata.MD)
					ctx = metadata.NewContext(ctx, info)
				}
				p.Inject(ctx, info.Header)
			}

			resp, err = next(ctx, req)
			finished = true
			return resp, err
		}
	}
}

// AttemptTracing observes one selected-node invocation without replaying it.
func AttemptTracing(tracer tracing.Tracer, propagation tracing.Propagator) transport.AttemptMiddleware {
	begin := observe.Tracing("client_attempt", tracer, tracing.SpanKindClient)
	return func(next transport.Invoker) transport.Invoker {
		return transport.InvokerFunc(func(ctx context.Context, address string, request any) (response any, err error) {
			info, _ := metadata.FromContext(ctx)
			if tracer != nil {
				info.Header = info.Header.Clone()
				if info.Header == nil {
					info.Header = make(metadata.MD)
				}
				ctx = metadata.NewContext(ctx, info)
			}
			attempt, _ := transport.AttemptFromContext(ctx)
			ctx, finish := begin(ctx, info.Operation, "attempt", attempt.Number, "peer", address)
			finished := false
			defer func() { finish(err, finished) }()
			if tracer != nil && propagation != nil {
				propagation.Inject(ctx, info.Header)
			}
			response, err = next.Invoke(ctx, address, request)
			finished = true
			return response, err
		})
	}
}
