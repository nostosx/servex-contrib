package grpcx

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nostosx/servex/errorx"
)

// RetryableMetadataKey is the reserved google.rpc.ErrorInfo metadata key that
// carries an explicit errorx retryability decision across gRPC. Its value is
// "true" or "false". The server sends Error.PropagatedRetryable under it when
// that differs from the default of the transmitted code, for example after
// WithRetryable(false) on an Unavailable error, or for a remote error the
// handler returns unchanged, which its caller must not retry again. Any
// application metadata value under this key is dropped. The client restores
// the decision with Error.WithRetryable and removes the key from
// Error.Metadata; when the key is absent or not a boolean, retryability
// follows the code.
const RetryableMetadataKey = "servex.retryable"

// toStatus converts a handler error into a gRPC status. An *errorx.Error in
// the chain wins: its Code becomes the status code, its Message the status
// message, and its Reason, Metadata and a non-default propagated
// retryability (see RetryableMetadataKey) travel in a google.rpc.ErrorInfo
// detail. Errors that
// already carry a gRPC status keep it; any other error is converted with
// errorx.From. An error with code OK is reported as Unknown, because a status
// with code OK would turn the failure into a success.
func toStatus(err error) *status.Status {
	e, ok := errors.AsType[*errorx.Error](err)
	if !ok || e == nil {
		if st, ok := status.FromError(err); ok {
			return st
		}
		e = errorx.From(err)
	}
	code := codes.Code(e.Code)
	if code == codes.OK {
		code = codes.Unknown
	}
	st := status.New(code, e.Message)
	md := maps.Clone(e.Metadata)
	delete(md, RetryableMetadataKey)
	if retryable := e.PropagatedRetryable(); retryable != errorx.New(errorx.Code(code), "", "").Retryable() {
		if md == nil {
			md = make(map[string]string, 1)
		}
		md[RetryableMetadataKey] = strconv.FormatBool(retryable)
	}
	if e.Reason == "" && len(md) == 0 {
		return st
	}
	withInfo, derr := st.WithDetails(&errdetails.ErrorInfo{Reason: e.Reason, Metadata: md})
	if derr != nil {
		// ErrorInfo always marshals; keep the plain status just in case.
		return st
	}
	return withInfo
}

// statusError converts a handler error into the error returned to gRPC.
func statusError(err error) error {
	if err == nil {
		return nil
	}
	return toStatus(err).Err()
}

// fromStatus converts an error returned by a gRPC call into an
// *errorx.Error with the same Code and Message, and the Reason and Metadata
// of the first google.rpc.ErrorInfo detail. Retryability follows the code
// unless that detail carries RetryableMetadataKey, which is restored with
// WithRetryable and removed from Metadata. The result is marked remote with
// errorx.WithRemote: retry middleware of this client may still retry it, but
// a servex server returning it tells its own caller not to retry. That
// includes the statuses gRPC reports for connection failures, which a client
// cannot tell apart from statuses sent by the peer. Canceled and
// DeadlineExceeded caused by the end of ctx are local and not marked.
// The original error stays in the cause chain, so status.FromError and
// status.Code keep working on the result; when ctx has ended and the code is
// Canceled or DeadlineExceeded, the context error joins the chain too.
func fromStatus(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[*errorx.Error](err); ok {
		return err
	}
	st, ok := status.FromError(err)
	if !ok {
		return errorx.From(err)
	}
	code := errorx.Code(st.Code())
	if code == errorx.OK {
		code = errorx.Unknown
	}
	var reason string
	var md map[string]string
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			reason, md = info.GetReason(), info.GetMetadata()
			break
		}
	}
	raw, explicit := md[RetryableMetadataKey]
	if explicit {
		md = maps.Clone(md)
		delete(md, RetryableMetadataKey)
	}
	cause := err
	ctxErr := ctx.Err()
	local := ctxErr != nil && (code == errorx.Canceled || code == errorx.DeadlineExceeded)
	if local {
		cause = fmt.Errorf("%w: %w", ctxErr, err)
	}
	e := errorx.New(code, reason, st.Message()).WithMetadata(md).WithCause(cause)
	if retryable, perr := strconv.ParseBool(raw); explicit && perr == nil {
		e = e.WithRetryable(retryable)
	}
	if !local {
		// After WithRetryable, so that the peer's decision applies to this
		// hop only.
		e = e.WithRemote()
	}
	return e
}
