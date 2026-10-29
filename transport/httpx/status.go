package httpx

import (
	"net/http"

	"github.com/nostosx/servex/errorx"
)

// statusClientClosedRequest is the non-standard status used for Canceled.
const statusClientClosedRequest = 499

// statusOf maps an errorx code to an HTTP status code. Unknown codes map to
// 500.
func statusOf(c errorx.Code) int {
	switch c {
	case errorx.OK:
		return http.StatusOK
	case errorx.Canceled:
		return statusClientClosedRequest
	case errorx.InvalidArgument, errorx.FailedPrecondition, errorx.OutOfRange:
		return http.StatusBadRequest
	case errorx.DeadlineExceeded:
		return http.StatusGatewayTimeout
	case errorx.NotFound:
		return http.StatusNotFound
	case errorx.AlreadyExists, errorx.Aborted:
		return http.StatusConflict
	case errorx.PermissionDenied:
		return http.StatusForbidden
	case errorx.ResourceExhausted:
		return http.StatusTooManyRequests
	case errorx.Unimplemented:
		return http.StatusNotImplemented
	case errorx.Unavailable:
		return http.StatusServiceUnavailable
	case errorx.Unauthenticated:
		return http.StatusUnauthorized
	default: // Unknown, Internal, DataLoss and undefined codes.
		return http.StatusInternalServerError
	}
}

// codeOf maps an HTTP status code back to an errorx code. Statuses shared by
// several codes map to the most conservative one: 400 to InvalidArgument,
// 409 to AlreadyExists and 500 to Unknown. Only statuses of the contract table
// are mapped; any other status (for example a 502 or 408 produced by a proxy)
// yields Unknown, whose default retry policy is false. This avoids recommending
// a retry when the request may already have executed; caller-defined middleware
// can still choose a different policy.
func codeOf(status int) errorx.Code {
	switch status {
	case http.StatusBadRequest:
		return errorx.InvalidArgument
	case http.StatusUnauthorized:
		return errorx.Unauthenticated
	case http.StatusForbidden:
		return errorx.PermissionDenied
	case http.StatusNotFound:
		return errorx.NotFound
	case http.StatusNotImplemented:
		return errorx.Unimplemented
	case http.StatusGatewayTimeout:
		return errorx.DeadlineExceeded
	case http.StatusConflict:
		return errorx.AlreadyExists
	case http.StatusTooManyRequests:
		return errorx.ResourceExhausted
	case statusClientClosedRequest:
		return errorx.Canceled
	case http.StatusServiceUnavailable:
		return errorx.Unavailable
	default:
		return errorx.Unknown
	}
}

// errorBody is the fixed JSON representation of an errorx.Error. Servers
// always write Retryable with errorx.Error.PropagatedRetryable, so an explicit
// WithRetryable setting survives the hop while an error received from another
// service is not retried again; it is absent from the bodies of servers that
// predate the field.
type errorBody struct {
	Code      string            `json:"code"`
	Reason    string            `json:"reason"`
	Message   string            `json:"message"`
	Metadata  map[string]string `json:"metadata"`
	Retryable *bool             `json:"retryable,omitempty"`
}
