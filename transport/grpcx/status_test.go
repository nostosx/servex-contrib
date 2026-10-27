package grpcx

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nostosx/servex/errorx"
)

func TestStatusRoundTripAllCodes(t *testing.T) {
	for c := errorx.Canceled; c <= errorx.Unauthenticated; c++ {
		t.Run(c.String(), func(t *testing.T) {
			in := errorx.New(c, "SOME_REASON", "some message").WithMetadata(map[string]string{"k": "v"})
			st := toStatus(in)
			if st.Code() != codes.Code(c) || st.Message() != "some message" {
				t.Fatalf("status = %v %q", st.Code(), st.Message())
			}
			out, ok := errors.AsType[*errorx.Error](fromStatus(t.Context(), st.Err()))
			if !ok {
				t.Fatal("fromStatus did not return *errorx.Error")
			}
			if out.Code != c || out.Reason != "SOME_REASON" || out.Message != "some message" || !maps.Equal(out.Metadata, in.Metadata) {
				t.Errorf("out = %+v", out)
			}
			if out.Retryable() != errorx.New(c, "", "").Retryable() {
				t.Errorf("Retryable() = %v, want default for %v", out.Retryable(), c)
			}
			if !errors.Is(out, in) {
				t.Error("errors.Is(out, in) = false")
			}
			// The original status stays reachable.
			if got := status.Code(out); got != codes.Code(c) {
				t.Errorf("status.Code(out) = %v", got)
			}
		})
	}
}

func TestToStatus(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		code    codes.Code
		msg     string
		details bool
	}{
		{"errorx without reason has no details", errorx.New(errorx.NotFound, "", "missing"), codes.NotFound, "missing", false},
		{"wrapped errorx", fmt.Errorf("wrap: %w", errorx.New(errorx.Aborted, "R", "m")), codes.Aborted, "m", true},
		{"metadata only", errorx.New(errorx.Internal, "", "m").WithMetadata(map[string]string{"a": "b"}), codes.Internal, "m", true},
		{"OK code becomes Unknown", errorx.New(errorx.OK, "", "odd"), codes.Unknown, "odd", false},
		{"undefined code is kept", errorx.New(errorx.Code(42), "", "x"), codes.Code(42), "x", false},
		{"status passes through", status.Error(codes.PermissionDenied, "denied"), codes.PermissionDenied, "denied", false},
		{"plain error", errors.New("boom"), codes.Unknown, "unknown error", false},
		{"canceled", context.Canceled, codes.Canceled, "operation canceled", false},
		{"deadline", fmt.Errorf("x: %w", context.DeadlineExceeded), codes.DeadlineExceeded, "deadline exceeded", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := toStatus(tt.err)
			if st.Code() != tt.code || st.Message() != tt.msg {
				t.Errorf("status = %v %q, want %v %q", st.Code(), st.Message(), tt.code, tt.msg)
			}
			if got := len(st.Details()) > 0; got != tt.details {
				t.Errorf("has details = %v, want %v", got, tt.details)
			}
		})
	}
	if statusError(nil) != nil {
		t.Error("statusError(nil) != nil")
	}
}

func TestFromStatus(t *testing.T) {
	if fromStatus(t.Context(), nil) != nil {
		t.Error("fromStatus(nil) != nil")
	}

	// Already converted errors are returned unchanged.
	e := errorx.New(errorx.Unavailable, "X", "y")
	if got := fromStatus(t.Context(), e); got != error(e) {
		t.Errorf("fromStatus(errorx) = %v", got)
	}

	// Non-status errors are classified with errorx.From.
	if got := errorx.CodeOf(fromStatus(t.Context(), errors.New("x"))); got != errorx.Unknown {
		t.Errorf("plain error code = %v", got)
	}

	// The first ErrorInfo detail wins; other details are ignored.
	st, err := status.New(codes.FailedPrecondition, "m").WithDetails(
		&errdetails.RetryInfo{},
		&errdetails.ErrorInfo{Reason: "FIRST", Metadata: map[string]string{"a": "1"}},
		&errdetails.ErrorInfo{Reason: "SECOND"},
	)
	if err != nil {
		t.Fatal(err)
	}
	out, _ := errors.AsType[*errorx.Error](fromStatus(t.Context(), st.Err()))
	if out == nil || out.Reason != "FIRST" || out.Metadata["a"] != "1" || out.Code != errorx.FailedPrecondition {
		t.Errorf("out = %+v", out)
	}

	// Foreign statuses without details keep code and message.
	out, _ = errors.AsType[*errorx.Error](fromStatus(t.Context(), status.Error(codes.Unavailable, "conn refused")))
	if out == nil || out.Code != errorx.Unavailable || out.Reason != "" || out.Message != "conn refused" || !out.Retryable() {
		t.Errorf("out = %+v", out)
	}

	// When the caller's context has ended, its error joins the chain.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	got := fromStatus(ctx, status.Error(codes.Canceled, "context canceled"))
	if !errors.Is(got, context.Canceled) || errorx.CodeOf(got) != errorx.Canceled || status.Code(got) != codes.Canceled {
		t.Errorf("canceled = %v", got)
	}
	// A server-side Canceled without a local cancellation does not claim it.
	if got := fromStatus(t.Context(), status.Error(codes.Canceled, "x")); errors.Is(got, context.Canceled) {
		t.Errorf("remote canceled = %v", got)
	}
}

// errorInfo returns the first ErrorInfo detail of st, or nil.
func errorInfo(st *status.Status) *errdetails.ErrorInfo {
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			return info
		}
	}
	return nil
}

func TestStatusRetryableRoundTrip(t *testing.T) {
	tests := []struct {
		name      string
		in        *errorx.Error
		wire      string // RetryableMetadataKey value on the wire; "" means absent
		retryable bool
		md        map[string]string
	}{
		{"explicit false on Unavailable", errorx.New(errorx.Unavailable, "", "draining").WithRetryable(false), "false", false, nil},
		{"explicit true on NotFound", errorx.New(errorx.NotFound, "R", "m").WithRetryable(true), "true", true, nil},
		{"explicit decision keeps metadata", errorx.New(errorx.Aborted, "R", "m").WithMetadata(map[string]string{"k": "v"}).WithRetryable(false), "false", false, map[string]string{"k": "v"}},
		{"explicit default is not sent", errorx.New(errorx.Unavailable, "", "m").WithRetryable(true), "", true, nil},
		{"code default is not sent", errorx.New(errorx.ResourceExhausted, "R", "m"), "", true, nil},
		{"application value under the key is dropped", errorx.New(errorx.NotFound, "R", "m").WithMetadata(map[string]string{RetryableMetadataKey: "true", "k": "v"}), "", false, map[string]string{"k": "v"}},
		{"application value cannot override the decision", errorx.New(errorx.Internal, "", "m").WithMetadata(map[string]string{RetryableMetadataKey: "false"}).WithRetryable(true), "true", true, nil},
		{"OK reported as Unknown keeps the decision", errorx.New(errorx.OK, "", "odd").WithRetryable(true), "true", true, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := toStatus(tt.in)
			info := errorInfo(st)
			got, ok := info.GetMetadata()[RetryableMetadataKey]
			if tt.wire == "" && ok || tt.wire != "" && got != tt.wire {
				t.Fatalf("wire %s = %q (present %v), want %q", RetryableMetadataKey, got, ok, tt.wire)
			}
			out, _ := errors.AsType[*errorx.Error](fromStatus(t.Context(), st.Err()))
			if out == nil {
				t.Fatal("fromStatus did not return *errorx.Error")
			}
			if out.Retryable() != tt.retryable || errorx.IsRetryable(out) != tt.retryable {
				t.Errorf("Retryable() = %v, want %v", out.Retryable(), tt.retryable)
			}
			if !maps.Equal(out.Metadata, tt.md) {
				t.Errorf("Metadata = %v, want %v", out.Metadata, tt.md)
			}
			if tt.md == nil && out.Metadata != nil {
				t.Errorf("Metadata = %v, want nil", out.Metadata)
			}
		})
	}
}

func TestFromStatusWithoutRetryableKey(t *testing.T) {
	// Statuses from servers that predate RetryableMetadataKey, or from other
	// implementations, keep the code-derived retryability.
	for _, c := range []errorx.Code{errorx.Unavailable, errorx.DeadlineExceeded, errorx.NotFound, errorx.Internal} {
		want := errorx.New(c, "", "").Retryable()
		plain := status.New(codes.Code(c), "m")
		withInfo, err := plain.WithDetails(&errdetails.ErrorInfo{Reason: "R", Metadata: map[string]string{"k": "v"}})
		if err != nil {
			t.Fatal(err)
		}
		invalid, err := plain.WithDetails(&errdetails.ErrorInfo{Metadata: map[string]string{RetryableMetadataKey: "maybe"}})
		if err != nil {
			t.Fatal(err)
		}
		for name, st := range map[string]*status.Status{"no details": plain, "ErrorInfo without key": withInfo, "invalid value": invalid} {
			out, _ := errors.AsType[*errorx.Error](fromStatus(t.Context(), st.Err()))
			if out == nil || out.Code != c || out.Retryable() != want {
				t.Errorf("%v %s: out = %+v, Retryable() = %v, want %v", c, name, out, out.Retryable(), want)
			}
			if _, ok := out.Metadata[RetryableMetadataKey]; ok {
				t.Errorf("%v %s: reserved key leaked into Metadata %v", c, name, out.Metadata)
			}
		}
		// The status detail itself is not modified by decoding.
		if errorInfo(invalid).GetMetadata()[RetryableMetadataKey] != "maybe" {
			t.Errorf("%v: decoding modified the status detail", c)
		}
	}
}
