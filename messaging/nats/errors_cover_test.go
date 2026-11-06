package nats

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/nostosx/servex/errorx"
)

func TestClassifyMapsErrors(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	expired, cancelExpired := context.WithTimeout(t.Context(), 0)
	defer cancelExpired()
	<-expired.Done()

	apiErr := func(status int, desc string) error {
		return &jetstream.APIError{Code: status, ErrorCode: jetstream.JSErrCodeBadRequest, Description: desc}
	}
	tests := []struct {
		name      string
		ctx       context.Context
		err       error
		code      errorx.Code
		reason    string // empty for context errors, which are not *errorx.Error
		retryable bool
	}{
		{"canceled context keeps the context error", canceled, fmt.Errorf("request: %w", context.Canceled),
			errorx.Canceled, "", false},
		{"expired context keeps the context error", expired, context.DeadlineExceeded,
			errorx.DeadlineExceeded, "", true},
		{"ended context with a server error is still classified", canceled, natsgo.ErrBadSubject,
			errorx.InvalidArgument, "NATS_INVALID_ARGUMENT", false},
		{"context error of a live context is a transport failure", t.Context(), context.DeadlineExceeded,
			errorx.Unavailable, "NATS_UNAVAILABLE", true},
		{"stream not found", nil, jetstream.ErrStreamNotFound, errorx.FailedPrecondition, "NATS_NO_STREAM", false},
		{"stream not found API error", nil,
			&jetstream.APIError{Code: http.StatusNotFound, ErrorCode: jetstream.JSErrCodeStreamNotFound, Description: "stream not found"},
			errorx.FailedPrecondition, "NATS_NO_STREAM", false},
		{"bad subject", nil, natsgo.ErrBadSubject, errorx.InvalidArgument, "NATS_INVALID_ARGUMENT", false},
		{"invalid subject", nil, jetstream.ErrInvalidSubject, errorx.InvalidArgument, "NATS_INVALID_ARGUMENT", false},
		{"max payload", nil, fmt.Errorf("publish: %w", natsgo.ErrMaxPayload), errorx.InvalidArgument, "NATS_INVALID_ARGUMENT", false},
		{"invalid consumer name", nil, jetstream.ErrInvalidConsumerName, errorx.InvalidArgument, "NATS_INVALID_ARGUMENT", false},
		{"invalid stream name", nil, jetstream.ErrInvalidStreamName, errorx.InvalidArgument, "NATS_INVALID_ARGUMENT", false},
		{"authorization", nil, natsgo.ErrAuthorization, errorx.PermissionDenied, "NATS_NOT_AUTHORIZED", false},
		{"permission violation", nil, natsgo.ErrPermissionViolation, errorx.PermissionDenied, "NATS_NOT_AUTHORIZED", false},
		{"connection closed", nil, natsgo.ErrConnectionClosed, errorx.Unavailable, "NATS_UNAVAILABLE", true},
		{"jetstream connection closed", nil, jetstream.ErrConnectionClosed, errorx.Unavailable, "NATS_UNAVAILABLE", true},
		{"server shutdown", nil, jetstream.ErrServerShutdown, errorx.Unavailable, "NATS_UNAVAILABLE", true},
		{"timeout", nil, natsgo.ErrTimeout, errorx.Unavailable, "NATS_UNAVAILABLE", true},
		{"no servers", nil, natsgo.ErrNoServers, errorx.Unavailable, "NATS_UNAVAILABLE", true},
		{"disconnected", nil, natsgo.ErrDisconnected, errorx.Unavailable, "NATS_UNAVAILABLE", true},
		{"API bad request", nil, apiErr(http.StatusBadRequest, "bad"), errorx.InvalidArgument, "NATS_API_ERROR", false},
		{"API not found", nil, fmt.Errorf("consumer: %w", &jetstream.APIError{
			Code: http.StatusNotFound, ErrorCode: jetstream.JSErrCodeConsumerNotFound, Description: "consumer not found"}),
			errorx.NotFound, "NATS_API_ERROR", false},
		{"API conflict", nil, apiErr(http.StatusConflict, "conflict"), errorx.FailedPrecondition, "NATS_API_ERROR", false},
		{"API unavailable", nil, apiErr(http.StatusServiceUnavailable, "no leader"), errorx.Unavailable, "NATS_API_ERROR", true},
		{"API other status", nil, apiErr(http.StatusInternalServerError, "boom"), errorx.Internal, "NATS_API_ERROR", false},
		// A JetStream error without an API error is a client side failure.
		{"jetstream error without API error", nil, jetstream.ErrKeyNotFound, errorx.Unavailable, "NATS_UNAVAILABLE", true},
		{"unknown error", nil, errors.New("write: broken pipe"), errorx.Unavailable, "NATS_UNAVAILABLE", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := classify(tt.ctx, "publish", tt.err)
			if got := errorx.CodeOf(err); got != tt.code {
				t.Fatalf("CodeOf(%v) = %v, want %v", err, got, tt.code)
			}
			if got := errorx.IsRetryable(err); got != tt.retryable {
				t.Errorf("IsRetryable(%v) = %v, want %v", err, got, tt.retryable)
			}
			if !strings.Contains(err.Error(), "nats: publish") {
				t.Errorf("error %q does not name the operation", err)
			}
			if tt.reason == "" {
				if tt.ctx == nil || !errors.Is(err, tt.ctx.Err()) {
					t.Errorf("error %v does not keep the context error", err)
				}
				return
			}
			xe, ok := errors.AsType[*errorx.Error](err)
			if !ok || xe.Reason != tt.reason {
				t.Errorf("error %v, want reason %s", err, tt.reason)
			}
			if !errors.Is(err, tt.err) {
				t.Errorf("error %v does not keep the cause %v", err, tt.err)
			}
		})
	}
}

func TestClassifyNamesAPIErrorDescription(t *testing.T) {
	err := classify(context.Background(), "subscribe", &jetstream.APIError{Code: http.StatusConflict, Description: "consumer name already in use"})
	if !strings.Contains(err.Error(), "nats: subscribe: consumer name already in use") {
		t.Fatalf("classify() = %q, want the API description in the message", err)
	}
}

func TestDefaultBackoff(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{-1, backoffBase},
		{0, backoffBase},
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{7, 6400 * time.Millisecond},
		{8, backoffCap},
		{9, backoffCap},
		// The shift is bounded, so huge attempts do not overflow.
		{1 << 30, backoffCap},
	}
	for _, tt := range tests {
		if got := defaultBackoff(tt.attempt); got != tt.want {
			t.Errorf("defaultBackoff(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
	// New uses it unless WithBackoff sets another function.
	for _, opts := range [][]Option{nil, {WithBackoff(nil)}} {
		if got := New(nil, opts...).opts.backoff(3); got != 400*time.Millisecond {
			t.Errorf("New(%d options) backoff(3) = %v, want the default", len(opts), got)
		}
	}
}
