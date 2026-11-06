package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nostosx/servex/errorx"
)

// errClosed is returned by Publish once the Broker is closed.
var errClosed = errorx.New(errorx.FailedPrecondition, "KAFKA_CLOSED", "kafka: broker is closed")

// invalid returns a non-retryable InvalidArgument error.
func invalid(format string, args ...any) error {
	return errorx.Newf(errorx.InvalidArgument, "KAFKA_INVALID_ARGUMENT", "kafka: "+format, args...)
}

// classify converts an error returned by the franz-go client during op into
// an errorx error, keeping err as the cause. Errors caused by the end of ctx
// keep the context error so that errorx reports Canceled or
// DeadlineExceeded.
func classify(ctx context.Context, op string, err error) error {
	if ctx != nil && ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return fmt.Errorf("kafka: %s: %w", op, ctx.Err())
	}
	if errors.Is(err, kgo.ErrClientClosed) {
		return errorx.Newf(errorx.FailedPrecondition, "KAFKA_CLOSED", "kafka: %s: client is closed", op).WithCause(err)
	}
	switch {
	case errors.Is(err, kerr.MessageTooLarge), errors.Is(err, kerr.RecordListTooLarge),
		errors.Is(err, kerr.InvalidTopicException), errors.Is(err, kerr.InvalidRecord),
		errors.Is(err, kerr.CorruptMessage), errors.Is(err, kerr.InvalidRequiredAcks):
		return errorx.Newf(errorx.InvalidArgument, "KAFKA_INVALID_RECORD", "kafka: %s: invalid record", op).WithCause(err)
	case errors.Is(err, kerr.TopicAuthorizationFailed), errors.Is(err, kerr.GroupAuthorizationFailed),
		errors.Is(err, kerr.ClusterAuthorizationFailed), errors.Is(err, kerr.TransactionalIDAuthorizationFailed):
		return errorx.Newf(errorx.PermissionDenied, "KAFKA_NOT_AUTHORIZED", "kafka: %s: not authorized", op).WithCause(err)
	case errors.Is(err, kerr.SaslAuthenticationFailed):
		return errorx.Newf(errorx.Unauthenticated, "KAFKA_UNAUTHENTICATED", "kafka: %s: authentication failed", op).WithCause(err)
	}
	if ke, ok := errors.AsType[*kerr.Error](err); ok && !ke.Retriable {
		return errorx.Newf(errorx.Internal, "KAFKA_ERROR", "kafka: %s: %s", op, ke.Message).WithCause(err)
	}
	// Everything else is a transport level failure: the cluster or the
	// partition leader is unreachable, or requests ran out of retries.
	return errorx.Newf(errorx.Unavailable, "KAFKA_UNAVAILABLE", "kafka: %s: cluster unavailable", op).WithCause(err)
}

// fetchError returns the first fatal error among the errors injected into
// fetches, or nil. Context errors, data loss notifications, retriable broker
// errors and transient group session errors are not fatal: the client
// recovers from them on its own, and the liveness check reports a cluster
// that stays unreachable.
func fetchError(fetches kgo.Fetches) error {
	for _, fe := range fetches.Errors() {
		err := fe.Err
		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			continue
		case errors.Is(err, kgo.ErrClientClosed):
			return classify(context.Background(), "subscribe", err)
		}
		if _, ok := errors.AsType[*kgo.ErrDataLoss](err); ok {
			continue
		}
		op := fmt.Sprintf("subscribe: topic %q partition %d", fe.Topic, fe.Partition)
		// A group session error wraps the broker error that ended the
		// session, so it is checked before broker errors: errors such as
		// UnknownMemberID or IllegalGeneration are not retriable requests,
		// yet the client rejoins the group on its own.
		if _, ok := errors.AsType[*kgo.ErrGroupSession](err); ok {
			if permanentGroupError(err) {
				return classify(context.Background(), op, err)
			}
			continue
		}
		if ke, ok := errors.AsType[*kerr.Error](err); ok {
			if ke.Retriable {
				continue
			}
			return classify(context.Background(), op, err)
		}
		return errorx.Newf(errorx.DataLoss, "KAFKA_FETCH_FAILED",
			"kafka: subscribe: topic %q partition %d: fetch failed", fe.Topic, fe.Partition).WithCause(err)
	}
	return nil
}

// permanentGroupError reports whether a group session error cannot be
// resolved by rejoining the group: the member is not authorized, or the group
// cannot accept it.
func permanentGroupError(err error) bool {
	for _, target := range []error{
		kerr.GroupAuthorizationFailed, kerr.TopicAuthorizationFailed, kerr.ClusterAuthorizationFailed,
		kerr.SaslAuthenticationFailed, kerr.InvalidGroupID, kerr.GroupMaxSizeReached,
	} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

// commitError reports whether a commit failure must end the subscription.
// Failures caused by a concurrent rebalance only lead to redelivery to the
// new owner of the partition, and connection failures are reported by the
// liveness check.
func commitError(err error) error {
	if err == nil {
		return nil
	}
	if ke, ok := errors.AsType[*kerr.Error](err); ok && !ke.Retriable {
		switch {
		case errors.Is(err, kerr.IllegalGeneration), errors.Is(err, kerr.UnknownMemberID),
			errors.Is(err, kerr.RebalanceInProgress), errors.Is(err, kerr.FencedInstanceID),
			errors.Is(err, kerr.StaleMemberEpoch):
			return nil
		}
		return classify(context.Background(), "commit", err)
	}
	return nil
}
