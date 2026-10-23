package recovery

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
)

// ReasonPanic is the errorx reason of errors produced from recovered panics.
const ReasonPanic = "PANIC"

// Option configures the middleware returned by New.
type Option func(*options)

type options struct {
	logger  *slog.Logger
	handler func(ctx context.Context, p any) error
}

// WithLogger sets the logger used to record recovered panics. The default is
// slog.Default(). A nil logger keeps the default.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.logger = l
		}
	}
}

// WithHandler sets a function that converts the recovered value p into the
// error returned to the caller. The panic is logged before fn is called. If
// fn returns nil, the default error is returned, so a panic never looks like
// a successful call.
func WithHandler(fn func(ctx context.Context, p any) error) Option {
	return func(o *options) { o.handler = fn }
}

// New returns middleware that recovers panics raised by the next endpoint.
//
// A recovered panic is logged at Error level with the panic value, the stack
// trace and the call's metadata.Info, and the call returns a nil response
// with an errorx.Internal error whose reason is ReasonPanic. The error is
// explicitly marked as not retryable and keeps the panic value as its cause;
// its Message is generic so that panic details are not exposed to callers.
func New(opts ...Option) endpoint.Middleware {
	o := options{logger: slog.Default()}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (resp any, err error) {
			defer func() {
				p := recover()
				if p == nil {
					return
				}
				resp, err = nil, o.recovered(ctx, p, debug.Stack())
			}()
			return next(ctx, req)
		}
	}
}

// recovered logs p and returns the error reported for it.
func (o *options) recovered(ctx context.Context, p any, stack []byte) error {
	attrs := []slog.Attr{
		slog.Any("panic", p),
		slog.String("stack", string(stack)),
	}
	if info, ok := metadata.FromContext(ctx); ok {
		attrs = append(attrs,
			slog.String("kind", string(info.Kind)),
			slog.String("protocol", info.Protocol),
			slog.String("operation", info.Operation),
		)
	}
	o.logger.LogAttrs(ctx, slog.LevelError, "recovery: panic recovered", attrs...)

	if o.handler != nil {
		if err := o.handler(ctx, p); err != nil {
			return err
		}
	}
	return PanicError(p)
}

// PanicError returns the error New reports for the recovered value p: an
// errorx.Internal error with reason ReasonPanic, explicitly not retryable,
// whose cause describes p. If p is an error it is wrapped, so errors.Is and
// errors.As see it. It is exported for custom handlers that only want to
// decorate the default error.
func PanicError(p any) *errorx.Error {
	var cause error
	if e, ok := p.(error); ok {
		cause = fmt.Errorf("recovery: panic: %w", e)
	} else {
		cause = fmt.Errorf("recovery: panic: %v", p)
	}
	return errorx.New(errorx.Internal, ReasonPanic, "internal error").
		WithCause(cause).
		WithRetryable(false)
}
