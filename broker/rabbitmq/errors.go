package rabbitmq

import (
	"context"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/nostosx/servex/errorx"
)

// invalid returns a non-retryable InvalidArgument error.
func invalid(msg string) error {
	return errorx.New(errorx.InvalidArgument, "RABBITMQ_INVALID_ARGUMENT", "rabbitmq: "+msg)
}

// classify converts an error returned by the AMQP client during op into an
// errorx error, keeping err as the cause. Errors caused by the end of ctx keep
// the context error so that errorx reports Canceled or DeadlineExceeded.
func classify(ctx context.Context, op string, err error) error {
	if ctx != nil && ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return fmt.Errorf("rabbitmq: %s: %w", op, ctx.Err())
	}
	if aerr, ok := errors.AsType[*amqp.Error](err); ok && aerr != nil {
		code, reason := errorx.Unavailable, "RABBITMQ_UNAVAILABLE"
		switch aerr.Code {
		case amqp.NotFound:
			code, reason = errorx.NotFound, "RABBITMQ_NOT_FOUND"
		case amqp.PreconditionFailed, amqp.ResourceLocked:
			code, reason = errorx.FailedPrecondition, "RABBITMQ_PRECONDITION_FAILED"
		case amqp.AccessRefused, amqp.NotAllowed:
			code, reason = errorx.PermissionDenied, "RABBITMQ_ACCESS_REFUSED"
		case amqp.FrameError:
			// The client library reports every failed read or write on the
			// socket, such as a reset connection or missed heartbeats, as a
			// frame error of its own; only one sent by the server is a
			// protocol violation.
			if aerr.Server {
				code, reason = errorx.Internal, "RABBITMQ_PROTOCOL_ERROR"
			}
		case amqp.SyntaxError, amqp.CommandInvalid, amqp.NotImplemented:
			code, reason = errorx.Internal, "RABBITMQ_PROTOCOL_ERROR"
		}
		return errorx.Newf(code, reason, "rabbitmq: %s: %s", op, aerr.Reason).WithCause(err)
	}
	return unavailable(op, err)
}

// unavailable returns a retryable Unavailable error for a transport failure.
func unavailable(op string, err error) error {
	return errorx.Newf(errorx.Unavailable, "RABBITMQ_UNAVAILABLE", "rabbitmq: %s: broker unavailable", op).WithCause(err)
}
