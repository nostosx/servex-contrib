package logging

import (
	"context"
	"log/slog"
	"time"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
)

// Message is the message of every record written by the middleware.
const Message = "call"

// Attribute keys of the records written by the middleware.
const (
	KeyKind      = "kind"
	KeyProtocol  = "protocol"
	KeyOperation = "operation"
	KeyDuration  = "duration"
	KeyCode      = "code"
	KeyReason    = "reason"
	KeyError     = "error"
)

// Option configures the middleware returned by New.
type Option func(*options)

type options struct {
	level func(code errorx.Code) slog.Level
}

// WithLevel sets the function choosing the level of a record from the
// errorx code of the call; it is also called with errorx.OK for successful
// calls. The default is DefaultLevel. A nil fn keeps the default.
func WithLevel(fn func(code errorx.Code) slog.Level) Option {
	return func(o *options) {
		if fn != nil {
			o.level = fn
		}
	}
}

// DefaultLevel is the default level function. Successful calls are logged
// at Info. Failures typically caused by the caller or by expected business
// conditions (Canceled, InvalidArgument, NotFound, AlreadyExists,
// PermissionDenied, ResourceExhausted, FailedPrecondition, Aborted,
// OutOfRange, Unauthenticated) are logged at Warn; every other failure
// (Unknown, DeadlineExceeded, Unimplemented, Internal, Unavailable, DataLoss
// and unknown codes) at Error.
func DefaultLevel(code errorx.Code) slog.Level {
	switch code {
	case errorx.OK:
		return slog.LevelInfo
	case errorx.Canceled, errorx.InvalidArgument, errorx.NotFound, errorx.AlreadyExists,
		errorx.PermissionDenied, errorx.ResourceExhausted, errorx.FailedPrecondition,
		errorx.Aborted, errorx.OutOfRange, errorx.Unauthenticated:
		return slog.LevelWarn
	default:
		return slog.LevelError
	}
}

// New returns middleware that logs every call to l once it completes. The
// record has message Message and the attributes kind, protocol and operation
// from the metadata.Info in the context (empty when absent), duration, and
// code, the errorx.Code name of the result ("OK" on success). Failed calls
// also carry the errorx reason, when non-empty, and the error text. A nil l
// uses slog.Default().
func New(l *slog.Logger, opts ...Option) endpoint.Middleware {
	if l == nil {
		l = slog.Default()
	}
	o := options{level: DefaultLevel}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			start := time.Now()
			resp, err := next(ctx, req)
			duration := time.Since(start)

			e := errorx.From(err)
			code := errorx.OK
			if e != nil {
				code = e.Code
			}
			level := o.level(code)
			if !l.Enabled(ctx, level) {
				return resp, err
			}
			info, _ := metadata.FromContext(ctx)
			attrs := make([]slog.Attr, 0, 7)
			attrs = append(attrs,
				slog.String(KeyKind, string(info.Kind)),
				slog.String(KeyProtocol, info.Protocol),
				slog.String(KeyOperation, info.Operation),
				slog.Duration(KeyDuration, duration),
				slog.String(KeyCode, code.String()),
			)
			if e != nil {
				if e.Reason != "" {
					attrs = append(attrs, slog.String(KeyReason, e.Reason))
				}
				attrs = append(attrs, slog.String(KeyError, err.Error()))
			}
			l.LogAttrs(ctx, level, Message, attrs...)
			return resp, err
		}
	}
}
