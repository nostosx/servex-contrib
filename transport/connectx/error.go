package connectx

import (
	"context"
	"errors"
	"maps"
	"strconv"

	"connectrpc.com/connect"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/nostosx/servex/errorx"
)

// RetryableMetadataKey carries the servex retry decision in ErrorInfo details,
// using the same key as grpcx so errors preserve it across protocol bridges.
const RetryableMetadataKey = "servex.retryable"

// ToError preserves existing Connect errors and maps errorx codes, reasons,
// metadata and propagated retry decisions to wire errors. Unknown causes are
// hidden from clients. It does not retry calls or streams.
func ToError(err error) error {
	if err == nil {
		return nil
	}
	e, ok := errors.AsType[*errorx.Error](err)
	if !ok || e == nil {
		if _, ok := errors.AsType[*connect.Error](err); ok {
			return err
		}
		e = errorx.From(err)
	}
	code := connect.Code(e.Code)
	if code < connect.CodeCanceled || code > connect.CodeUnauthenticated {
		code = connect.CodeUnknown
	}
	out := connect.NewError(code, errors.New(e.Message))
	md := maps.Clone(e.Metadata)
	delete(md, RetryableMetadataKey)
	if retry := e.PropagatedRetryable(); retry != errorx.New(errorx.Code(code), "", "").Retryable() {
		if md == nil {
			md = make(map[string]string)
		}
		md[RetryableMetadataKey] = strconv.FormatBool(retry)
	}
	if e.Reason != "" || len(md) > 0 {
		detail, detailErr := connect.NewErrorDetail(&errdetails.ErrorInfo{Reason: e.Reason, Metadata: md})
		if detailErr == nil {
			out.AddDetail(detail)
		}
	}
	return out
}

// FromError decodes wire errors and marks them remote. Caller cancellation
// stays local and remains detectable with errors.Is. The original error is
// retained for SDK-specific diagnostics.
func FromError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if e, ok := errors.AsType[*errorx.Error](err); ok && e != nil {
		return err
	}
	wire, ok := errors.AsType[*connect.Error](err)
	if !ok {
		return errorx.From(err)
	}
	out := errorx.New(errorx.Code(wire.Code()), "", wire.Message())
	for _, detail := range wire.Details() {
		value, decodeErr := detail.Value()
		if decodeErr != nil {
			continue
		}
		info, ok := value.(*errdetails.ErrorInfo)
		if !ok {
			continue
		}
		out.Reason = info.GetReason()
		out.Metadata = maps.Clone(info.GetMetadata())
		if retry, parseErr := strconv.ParseBool(out.Metadata[RetryableMetadataKey]); parseErr == nil {
			out = out.WithRetryable(retry)
		}
		delete(out.Metadata, RetryableMetadataKey)
		break
	}
	cause := err
	if ctx.Err() != nil && (wire.Code() == connect.CodeCanceled || wire.Code() == connect.CodeDeadlineExceeded) {
		return out.WithCause(errors.Join(cause, ctx.Err()))
	}
	return out.WithCause(cause).WithRemote()
}
