package grpcx

import (
	"context"
	"errors"
	"fmt"
	"maps"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nostosx/servex/errorx"
)

// toStatus converts a handler error into a gRPC status. An *errorx.Error in
// the chain wins: its Code becomes the status code, its Message the status
// message, and its Reason and Metadata travel in a google.rpc.ErrorInfo
// detail. Errors that already carry a gRPC status keep it; any other error
// is converted with errorx.From. An error with code OK is reported as
// Unknown, because a status with code OK would turn the failure into a
// success.
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
	if e.Reason == "" && len(e.Metadata) == 0 {
		return st
	}
	withInfo, derr := st.WithDetails(&errdetails.ErrorInfo{Reason: e.Reason, Metadata: maps.Clone(e.Metadata)})
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
// of the first google.rpc.ErrorInfo detail. Retryability follows the code.
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
	cause := err
	if ctxErr := ctx.Err(); ctxErr != nil && (code == errorx.Canceled || code == errorx.DeadlineExceeded) {
		cause = fmt.Errorf("%w: %w", ctxErr, err)
	}
	return errorx.New(code, reason, st.Message()).WithMetadata(md).WithCause(cause)
}
