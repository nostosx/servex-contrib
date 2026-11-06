package endpoint

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/nostosx/servex-contrib/middleware/observe"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/observability/logger"
)

// ReasonPanic is the errorx reason of errors produced from recovered panics.
const ReasonPanic = "PANIC"

// RecoveryOption configures the middleware returned by Recovery.
type RecoveryOption func(*recoveryOptions)

type recoveryOptions struct {
	logger       logger.Logger
	panicDetails bool
	handler      func(ctx context.Context, p any) error
}

// WithRecoveryLogger sets the logger used to record recovered panics. The default is
// disabled. A nil logger leaves it disabled.
func WithRecoveryLogger(l logger.Logger) RecoveryOption {
	return func(o *recoveryOptions) {
		if l != nil {
			o.logger = l
		}
	}
}

// WithPanicDetails includes the panic value and stack in recovery logs.
// Enable it only when diagnostic output is controlled and appropriate for secrets.
// The default records safe classification and call/trace metadata only.
func WithPanicDetails() RecoveryOption {
	return func(o *recoveryOptions) { o.panicDetails = true }
}

// WithRecoveryHandler sets a function that converts the recovered value p into the
// error returned to the caller. The panic is logged before fn is called. If
// fn returns nil, the default error is returned, so a panic never looks like
// a successful call.
func WithRecoveryHandler(fn func(ctx context.Context, p any) error) RecoveryOption {
	return func(o *recoveryOptions) { o.handler = fn }
}

// Recovery returns middleware that recovers panics raised by the next endpoint.
//
// A recovered panic is logged at Error level with safe classification and call/trace
// metadata. WithPanicDetails explicitly enables panic/stack diagnostics.
// The call returns a nil response
// with an errorx.Internal error whose reason is ReasonPanic. The error is
// explicitly marked as not retryable and keeps the panic value as its cause;
// its Message is generic so that panic details are not exposed to callers.
func Recovery(opts ...RecoveryOption) endpoint.Middleware {
	o := recoveryOptions{logger: nil}
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
				resp, err = nil, o.recovered(ctx, p)
			}()
			return next(ctx, req)
		}
	}
}

// recovered logs p and returns the error reported for it.
func (o *recoveryOptions) recovered(ctx context.Context, p any) error {
	if o.logger != nil && o.logger.Enabled(ctx, logger.LevelError) {
		attrs := observe.Fields(ctx, errorx.Classification{
			Code: errorx.Internal, Reason: ReasonPanic,
		})
		if info, ok := metadata.FromContext(ctx); ok {
			attrs = append(attrs,
				"kind", string(info.Kind), "protocol", info.Protocol,
				"operation", info.Operation,
			)
		}
		if o.panicDetails {
			attrs = append(attrs, "panic", p, "stack", string(debug.Stack()))
		}
		o.logger.Log(ctx, logger.LevelError, "recovery: panic recovered", attrs...)
	}

	if o.handler != nil {
		if err := o.handler(ctx, p); err != nil {
			return err
		}
	}
	return PanicError(p)
}

// PanicError returns the error Recovery reports for the recovered value p: an
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
