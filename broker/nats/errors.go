package nats

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/nostosx/servex/errorx"
)

// invalid returns a non-retryable InvalidArgument error.
func invalid(msg string) error {
	return errorx.New(errorx.InvalidArgument, "NATS_INVALID_ARGUMENT", "nats: "+msg)
}

// classify converts an error returned by the NATS client during op into an
// errorx error, keeping err as the cause. Errors caused by the end of ctx keep
// the context error so that errorx reports Canceled or DeadlineExceeded.
func classify(ctx context.Context, op string, err error) error {
	if ctx != nil && ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return fmt.Errorf("nats: %s: %w", op, ctx.Err())
	}
	switch {
	case errors.Is(err, jetstream.ErrStreamNotFound):
		return noStream(op, err)
	case errors.Is(err, jetstream.ErrNoStreamResponse), errors.Is(err, natsgo.ErrNoResponders):
		// Nobody answered: the stream or the JetStream API is temporarily
		// without a leader, for example while a server restarts. Publish
		// tells a subject without stream apart before classifying.
		return unavailable(op, err)
	case errors.Is(err, natsgo.ErrBadSubject), errors.Is(err, jetstream.ErrInvalidSubject),
		errors.Is(err, natsgo.ErrMaxPayload), errors.Is(err, jetstream.ErrInvalidConsumerName),
		errors.Is(err, jetstream.ErrInvalidStreamName):
		return errorx.Newf(errorx.InvalidArgument, "NATS_INVALID_ARGUMENT", "nats: %s: invalid argument", op).WithCause(err)
	case errors.Is(err, natsgo.ErrAuthorization), errors.Is(err, natsgo.ErrPermissionViolation):
		return errorx.Newf(errorx.PermissionDenied, "NATS_NOT_AUTHORIZED", "nats: %s: not authorized", op).WithCause(err)
	case errors.Is(err, natsgo.ErrConnectionClosed), errors.Is(err, jetstream.ErrConnectionClosed),
		errors.Is(err, jetstream.ErrServerShutdown), errors.Is(err, natsgo.ErrTimeout),
		errors.Is(err, natsgo.ErrNoServers), errors.Is(err, natsgo.ErrDisconnected):
		return unavailable(op, err)
	}
	if jerr, ok := errors.AsType[jetstream.JetStreamError](err); ok && jerr.APIError() != nil {
		api := jerr.APIError()
		code := errorx.Internal
		switch api.Code {
		case http.StatusBadRequest:
			code = errorx.InvalidArgument
		case http.StatusNotFound:
			code = errorx.NotFound
		case http.StatusConflict:
			code = errorx.FailedPrecondition
		case http.StatusServiceUnavailable:
			code = errorx.Unavailable
		}
		return errorx.Newf(code, "NATS_API_ERROR", "nats: %s: %s", op, api.Description).WithCause(err)
	}
	return unavailable(op, err)
}

// noStream returns a non-retryable FailedPrecondition error for a subject that
// no stream is bound to.
func noStream(op string, err error) error {
	return errorx.Newf(errorx.FailedPrecondition, "NATS_NO_STREAM", "nats: %s: no stream bound to the subject", op).WithCause(err)
}

// unavailable returns a retryable Unavailable error for a transport failure.
func unavailable(op string, err error) error {
	return errorx.Newf(errorx.Unavailable, "NATS_UNAVAILABLE", "nats: %s: server unavailable", op).WithCause(err)
}
