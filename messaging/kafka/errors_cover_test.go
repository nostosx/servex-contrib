package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nostosx/servex/errorx"
)

func TestClassifyMapsErrors(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	expired, cancelExpired := context.WithTimeout(t.Context(), 0)
	defer cancelExpired()
	<-expired.Done()

	tests := []struct {
		name      string
		ctx       context.Context
		err       error
		code      errorx.Code
		reason    string // empty for context errors, which are not *errorx.Error
		retryable bool
	}{
		{"canceled context keeps the context error", canceled, fmt.Errorf("produce: %w", context.Canceled),
			errorx.Canceled, "", false},
		{"expired context keeps the context error", expired, context.DeadlineExceeded,
			errorx.DeadlineExceeded, "", true},
		{"ended context with a broker error is still classified", canceled, kerr.MessageTooLarge,
			errorx.InvalidArgument, "KAFKA_INVALID_RECORD", false},
		{"context error of a live context is a transport failure", t.Context(), context.Canceled,
			errorx.Unavailable, "KAFKA_UNAVAILABLE", true},
		{"closed client", nil, fmt.Errorf("wrapped: %w", kgo.ErrClientClosed),
			errorx.FailedPrecondition, "KAFKA_CLOSED", false},
		{"message too large", nil, kerr.MessageTooLarge, errorx.InvalidArgument, "KAFKA_INVALID_RECORD", false},
		{"record list too large", nil, kerr.RecordListTooLarge, errorx.InvalidArgument, "KAFKA_INVALID_RECORD", false},
		{"invalid topic", nil, kerr.InvalidTopicException, errorx.InvalidArgument, "KAFKA_INVALID_RECORD", false},
		{"invalid record", nil, kerr.InvalidRecord, errorx.InvalidArgument, "KAFKA_INVALID_RECORD", false},
		// CORRUPT_MESSAGE is retriable for Kafka, yet retrying the same
		// record cannot succeed.
		{"corrupt message", nil, kerr.CorruptMessage, errorx.InvalidArgument, "KAFKA_INVALID_RECORD", false},
		{"invalid required acks", nil, kerr.InvalidRequiredAcks, errorx.InvalidArgument, "KAFKA_INVALID_RECORD", false},
		{"topic authorization", nil, kerr.TopicAuthorizationFailed, errorx.PermissionDenied, "KAFKA_NOT_AUTHORIZED", false},
		{"group authorization", nil, kerr.GroupAuthorizationFailed, errorx.PermissionDenied, "KAFKA_NOT_AUTHORIZED", false},
		{"cluster authorization", nil, kerr.ClusterAuthorizationFailed, errorx.PermissionDenied, "KAFKA_NOT_AUTHORIZED", false},
		{"transactional id authorization", nil, kerr.TransactionalIDAuthorizationFailed,
			errorx.PermissionDenied, "KAFKA_NOT_AUTHORIZED", false},
		{"sasl authentication", nil, kerr.SaslAuthenticationFailed, errorx.Unauthenticated, "KAFKA_UNAUTHENTICATED", false},
		{"other non-retriable broker error", nil, fmt.Errorf("wrapped: %w", kerr.UnknownServerError),
			errorx.Internal, "KAFKA_ERROR", false},
		{"retriable broker error", nil, kerr.NotLeaderForPartition, errorx.Unavailable, "KAFKA_UNAVAILABLE", true},
		{"transport error", nil, errors.New("dial tcp: connection refused"), errorx.Unavailable, "KAFKA_UNAVAILABLE", true},
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
			if !strings.Contains(err.Error(), "kafka: publish") {
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

func TestClassifyNamesBrokerErrorMessage(t *testing.T) {
	err := classify(context.Background(), "commit", kerr.UnknownServerError)
	if !strings.Contains(err.Error(), "kafka: commit: "+kerr.UnknownServerError.Message) {
		t.Fatalf("classify() = %q, want the Kafka error code in the message", err)
	}
}

// fetchesWithError returns fetches holding a single partition error.
func fetchesWithError(topic string, partition int32, err error) kgo.Fetches {
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic:      topic,
		Partitions: []kgo.FetchPartition{{Partition: partition, Err: err}},
	}}}}
}

func TestFetchErrorSkipsRecoverableErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"canceled", context.Canceled},
		{"deadline exceeded", fmt.Errorf("poll: %w", context.DeadlineExceeded)},
		{"data loss", &kgo.ErrDataLoss{Topic: "orders", Partition: 1, ConsumedTo: 10, ResetTo: 5}},
		{"retriable broker error", kerr.NotLeaderForPartition},
		{"wrapped retriable broker error", fmt.Errorf("fetch: %w", kerr.UnknownTopicOrPartition)},
		{"group session", &kgo.ErrGroupSession{Err: errors.New("join failed")}},
		// The member was evicted, for example after a pause longer than the
		// session timeout; the client rejoins the group.
		{"unknown member", &kgo.ErrGroupSession{Err: kerr.UnknownMemberID}},
		{"illegal generation", &kgo.ErrGroupSession{Err: kerr.IllegalGeneration}},
		{"fenced instance", &kgo.ErrGroupSession{Err: kerr.FencedInstanceID}},
		{"fenced member epoch", &kgo.ErrGroupSession{Err: kerr.FencedMemberEpoch}},
		{"no error", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := fetchError(fetchesWithError("orders", 1, tt.err)); err != nil {
				t.Fatalf("fetchError(%v) = %v, want nil", tt.err, err)
			}
		})
	}
}

func TestFetchErrorReportsFatalErrors(t *testing.T) {
	// A group session error that rejoining cannot resolve is fatal.
	sessionWithFatalCause := &kgo.ErrGroupSession{Err: kerr.GroupAuthorizationFailed}
	tests := []struct {
		name   string
		err    error
		code   errorx.Code
		reason string
		text   string
	}{
		{"closed client", kgo.ErrClientClosed, errorx.FailedPrecondition, "KAFKA_CLOSED", "kafka: subscribe: client is closed"},
		{"non-retriable broker error", kerr.TopicAuthorizationFailed, errorx.PermissionDenied, "KAFKA_NOT_AUTHORIZED",
			`kafka: subscribe: topic "orders" partition 2: not authorized`},
		{"offset out of range", kerr.OffsetOutOfRange, errorx.Internal, "KAFKA_ERROR",
			`kafka: subscribe: topic "orders" partition 2: OFFSET_OUT_OF_RANGE`},
		{"group session with a fatal cause", sessionWithFatalCause, errorx.PermissionDenied, "KAFKA_NOT_AUTHORIZED",
			`topic "orders" partition 2`},
		{"unknown error", errors.New("decompress: corrupt"), errorx.DataLoss, "KAFKA_FETCH_FAILED",
			`kafka: subscribe: topic "orders" partition 2: fetch failed`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := fetchError(fetchesWithError("orders", 2, tt.err))
			if got := errorx.CodeOf(err); got != tt.code {
				t.Fatalf("fetchError(%v) = %v, want code %v", tt.err, err, tt.code)
			}
			if xe, ok := errors.AsType[*errorx.Error](err); !ok || xe.Reason != tt.reason {
				t.Errorf("fetchError(%v) = %v, want reason %s", tt.err, err, tt.reason)
			}
			if !errors.Is(err, tt.err) {
				t.Errorf("fetchError(%v) = %v, does not keep the cause", tt.err, err)
			}
			if errorx.IsRetryable(err) {
				t.Errorf("fetchError(%v) = %v, want non-retryable", tt.err, err)
			}
			if !strings.Contains(err.Error(), tt.text) {
				t.Errorf("fetchError(%v) = %q, want it to contain %q", tt.err, err, tt.text)
			}
		})
	}
}

func TestFetchErrorReturnsFirstFatalError(t *testing.T) {
	fetches := kgo.Fetches{
		{Topics: []kgo.FetchTopic{{Topic: "orders", Partitions: []kgo.FetchPartition{
			{Partition: 0, Err: kerr.NotLeaderForPartition},
			{Partition: 1, Err: errors.New("first")},
		}}}},
		{Topics: []kgo.FetchTopic{{Topic: "payments", Partitions: []kgo.FetchPartition{
			{Partition: 0, Err: kerr.TopicAuthorizationFailed},
		}}}},
	}
	err := fetchError(fetches)
	if errorx.CodeOf(err) != errorx.DataLoss || !strings.Contains(err.Error(), `topic "orders" partition 1`) {
		t.Fatalf("fetchError() = %v, want the fetch failure of orders/1", err)
	}
}

func TestCommitError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code errorx.Code // OK when the subscription goes on
	}{
		{"illegal generation", kerr.IllegalGeneration, errorx.OK},
		{"unknown member", kerr.UnknownMemberID, errorx.OK},
		{"rebalance in progress", fmt.Errorf("commit: %w", kerr.RebalanceInProgress), errorx.OK},
		{"fenced instance", kerr.FencedInstanceID, errorx.OK},
		{"stale member epoch", kerr.StaleMemberEpoch, errorx.OK},
		{"retriable broker error", kerr.CoordinatorNotAvailable, errorx.OK},
		{"connection failure", errors.New("dial tcp: connection refused"), errorx.OK},
		{"context error", context.DeadlineExceeded, errorx.OK},
		{"not authorized", kerr.GroupAuthorizationFailed, errorx.PermissionDenied},
		{"other non-retriable broker error", kerr.OffsetMetadataTooLarge, errorx.Internal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := commitError(tt.err)
			if got := errorx.CodeOf(err); got != tt.code {
				t.Fatalf("commitError(%v) = %v, want code %v", tt.err, err, tt.code)
			}
			if err == nil {
				return
			}
			if !errors.Is(err, tt.err) || !strings.Contains(err.Error(), "kafka: commit") {
				t.Errorf("commitError(%v) = %v, want the commit failure with its cause", tt.err, err)
			}
			if errorx.IsRetryable(err) {
				t.Errorf("commitError(%v) = %v, want non-retryable", tt.err, err)
			}
		})
	}
}
