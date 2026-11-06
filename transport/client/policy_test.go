package client_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	client "github.com/nostosx/servex-contrib/transport/client"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/selector"
	"github.com/nostosx/servex/transport"
)

type countedSelector struct {
	selected, done atomic.Int64
	onSelect       func()
}

func (s *countedSelector) Select(_ context.Context, nodes []selector.Node) (selector.Node, selector.Done, error) {
	s.selected.Add(1)
	if s.onSelect != nil {
		s.onSelect()
	}
	return nodes[0], func(context.Context, error) { s.done.Add(1) }, nil
}

func TestCloseRejectsExistingAndNewEndpoints(t *testing.T) {
	for _, stream := range []bool{false, true} {
		name := "unary"
		if stream {
			name = "stream"
		}
		t.Run(name, func(t *testing.T) {
			s := &countedSelector{}
			c, err := client.New(t.Context(), "http://127.0.0.1:8080", client.WithSelector(s))
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			build := c.Endpoint
			if stream {
				build = c.StreamEndpoint
			}
			inv := transport.InvokerFunc(func(context.Context, string, any) (any, error) {
				t.Error("closed client invoked transport")
				return nil, nil
			})
			existing := build("users.get", inv)
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			for _, call := range []endpoint.Endpoint{existing, build("users.get", inv)} {
				_, err := call(t.Context(), nil)
				outcome := errorx.Classify(err)
				if outcome.Code != errorx.FailedPrecondition || outcome.Reason != "CLIENT_CLOSED" || outcome.Retryable {
					t.Fatalf("closed error=%v", err)
				}
			}
			if s.selected.Load() != 0 {
				t.Fatal("closed client selected a node")
			}
		})
	}
}

func TestCloseStopsRetryButPreservesRunningInvocation(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "retry"
		}
		t.Run(name, func(t *testing.T) {
			s := &countedSelector{}
			c, err := client.New(t.Context(), "http://127.0.0.1:8080", client.WithSelector(s), client.WithPolicy(client.Policy{
				MaxAttempts: 3, RetrySafe: true, RetryIf: func(error) bool { return true },
			}))
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			calls := 0
			call := c.Endpoint("users.get", transport.InvokerFunc(func(context.Context, string, any) (any, error) {
				calls++
				if err := c.Close(); err != nil {
					t.Error(err)
				}
				if fail {
					return nil, errors.New("retryable failure")
				}
				return 7, nil
			}))
			result, err := call(t.Context(), nil)
			if fail {
				if got := errorx.Classify(err); got.Code != errorx.FailedPrecondition || got.Retryable {
					t.Fatalf("retry after close=%v", err)
				}
			} else if result != 7 || err != nil {
				t.Fatalf("running invocation=%v, %v", result, err)
			}
			if calls != 1 || s.selected.Load() != 1 || s.done.Load() != 1 {
				t.Fatal("close replayed invocation or lost selector outcome")
			}
		})
	}
}

func TestCloseDuringSelectionSettlesWithoutInvoking(t *testing.T) {
	s := &countedSelector{}
	c, err := client.New(t.Context(), "http://127.0.0.1:8080", client.WithSelector(s))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s.onSelect = func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	}
	call := c.Endpoint("users.get", transport.InvokerFunc(func(context.Context, string, any) (any, error) {
		t.Error("invocation started after selection closed the client")
		return nil, nil
	}))
	_, err = call(t.Context(), nil)
	if got := errorx.Classify(err); got.Code != errorx.FailedPrecondition || got.Reason != "CLIENT_CLOSED" {
		t.Fatalf("closed selection=%v", err)
	}
	if s.selected.Load() != 1 || s.done.Load() != 1 {
		t.Fatal("selected node outcome was not settled once")
	}
}

func TestLogicalCallAndAttemptsHaveIndependentBoundaries(t *testing.T) {
	t.Parallel()
	s := &countedSelector{}
	cause := errors.New("transient")
	logical, attempts := 0, 0
	callPolicy := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, request any) (any, error) { logical++; return next(ctx, request) }
	}
	perAttempt := func(next transport.Invoker) transport.Invoker {
		return transport.InvokerFunc(func(ctx context.Context, address string, request any) (any, error) {
			attempts++
			info, ok := transport.AttemptFromContext(ctx)
			if !ok || info.Number != attempts || info.Address != address {
				t.Fatalf("attempt metadata: %+v", info)
			}
			return next.Invoke(ctx, address, request)
		})
	}
	c, err := client.New(t.Context(), "http://127.0.0.1:8080", client.WithSelector(s), client.WithMiddleware(callPolicy), client.WithAttemptMiddleware(perAttempt), client.WithPolicy(client.Policy{MaxAttempts: 3, RetrySafe: true, RetryIf: func(err error) bool { return errors.Is(err, cause) }}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	fn := c.Endpoint("catalog.get", transport.InvokerFunc(func(context.Context, string, any) (any, error) {
		if attempts < 3 {
			return nil, cause
		}
		return 7, nil
	}))
	if result, err := fn(t.Context(), nil); result != 7 || err != nil || logical != 1 || attempts != 3 || s.selected.Load() != 3 || s.done.Load() != 3 {
		t.Fatalf("result=%v error=%v logical=%d attempts=%d selection=%d done=%d", result, err, logical, attempts, s.selected.Load(), s.done.Load())
	}
}

func TestAttemptTimeoutAndTotalBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &countedSelector{}
		c, err := client.New(t.Context(), "http://127.0.0.1:8080", client.WithSelector(s), client.WithPolicy(client.Policy{Timeout: 250 * time.Millisecond, AttemptTimeout: 100 * time.Millisecond, MaxAttempts: 10, RetrySafe: true, RetryIf: func(error) bool { return true }}))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		start := time.Now()
		fn := c.Endpoint("catalog.get", transport.InvokerFunc(func(ctx context.Context, _ string, _ any) (any, error) { <-ctx.Done(); return nil, ctx.Err() }))
		_, err = fn(t.Context(), nil)
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != 250*time.Millisecond || s.selected.Load() != 3 {
			t.Fatalf("error=%v duration=%v attempts=%d", err, time.Since(start), s.selected.Load())
		}
	})
}

func TestUnsafeReplayRejectedAndStreamsBypassUnaryPolicies(t *testing.T) {
	t.Parallel()
	s := &countedSelector{}
	if c, err := client.New(t.Context(), "http://127.0.0.1:8080", client.WithSelector(s), client.WithPolicy(client.Policy{MaxAttempts: 2})); err == nil || c != nil {
		t.Fatal("unsafe replay policy accepted")
	}
	c, err := client.New(t.Context(), "http://127.0.0.1:8080", client.WithSelector(s), client.WithPolicy(client.Policy{MaxAttempts: 3, RetrySafe: true, RetryIf: func(error) bool { return true }}), client.WithAttemptMiddleware(func(transport.Invoker) transport.Invoker { t.Fatal("stream used unary attempt chain"); return nil }))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	calls := 0
	cause := errors.New("stream failed")
	fn := c.StreamEndpoint("chat.stream", transport.InvokerFunc(func(context.Context, string, any) (any, error) { calls++; return nil, cause }))
	if _, err := fn(t.Context(), nil); err != cause || calls != 1 {
		t.Fatalf("stream error=%v calls=%d", err, calls)
	}
}

func TestPolicyPreservesLastResultAndRespectsRetryClassification(t *testing.T) {
	retryable := errorx.New(errorx.Unavailable, "UPSTREAM_DOWN", "upstream unavailable").WithRetryable(true)
	terminal := errorx.New(errorx.PermissionDenied, "DENIED", "permission denied")
	for _, tt := range []struct {
		name        string
		failure     error
		succeedAt   int
		wantCalls   int
		wantFailure error
	}{
		{name: "transient failure recovers", failure: retryable, succeedAt: 2, wantCalls: 2},
		{name: "retry budget exhausted", failure: retryable, wantCalls: 3, wantFailure: retryable},
		{name: "terminal failure stops", failure: terminal, wantCalls: 1, wantFailure: terminal},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := &countedSelector{}
			c, err := client.New(t.Context(), "http://127.0.0.1:8080", client.WithSelector(s), client.WithPolicy(client.Policy{
				MaxAttempts: 3, RetrySafe: true, RetryIf: errorx.IsRetryable,
			}))
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			calls := 0
			request := &struct{ ID string }{ID: "original"}
			call := c.Endpoint("catalog.get", transport.InvokerFunc(func(_ context.Context, _ string, got any) (any, error) {
				calls++
				if got != request {
					t.Fatal("retry changed the request")
				}
				if calls == tt.succeedAt {
					return calls, nil
				}
				return calls, tt.failure
			}))
			result, err := call(t.Context(), request)
			if result != tt.wantCalls || err != tt.wantFailure || calls != tt.wantCalls {
				t.Fatalf("result=%v error=%v calls=%d", result, err, calls)
			}
			if s.selected.Load() != int64(calls) || s.done.Load() != int64(calls) {
				t.Fatalf("selection not settled per attempt: selected=%d done=%d", s.selected.Load(), s.done.Load())
			}
		})
	}
}

func TestPolicyCancellationStopsBackoffWithoutReplay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &countedSelector{}
		c, err := client.New(t.Context(), "http://127.0.0.1:8080", client.WithSelector(s), client.WithPolicy(client.Policy{
			MaxAttempts: 3, RetrySafe: true, RetryIf: errorx.IsRetryable,
			Backoff: func(int) time.Duration { return time.Hour },
		}))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		ctx, cancel := context.WithCancelCause(t.Context())
		defer cancel(nil)
		attempted := make(chan struct{})
		calls := 0
		call := c.Endpoint("catalog.get", transport.InvokerFunc(func(context.Context, string, any) (any, error) {
			calls++
			if calls == 1 {
				close(attempted)
			}
			return 7, errorx.New(errorx.Unavailable, "UPSTREAM_DOWN", "unavailable").WithRetryable(true)
		}))
		var result any
		var failure error
		done := make(chan struct{})
		go func() {
			defer close(done)
			result, failure = call(ctx, nil)
		}()
		<-attempted
		synctest.Wait()
		cause := errors.New("caller canceled during backoff")
		cancel(cause)
		<-done
		if result != 7 || calls != 1 || !errors.Is(failure, context.Canceled) || !errors.Is(failure, cause) {
			t.Fatalf("result=%v calls=%d error=%v", result, calls, failure)
		}
	})
}

func TestPolicyDoesNotSelectForCanceledCaller(t *testing.T) {
	s := &countedSelector{}
	c, err := client.New(t.Context(), "http://127.0.0.1:8080", client.WithSelector(s), client.WithPolicy(client.Policy{
		MaxAttempts: 3, RetrySafe: true, RetryIf: errorx.IsRetryable,
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithCancelCause(t.Context())
	cause := errors.New("request canceled")
	cancel(cause)
	call := c.Endpoint("catalog.get", transport.InvokerFunc(func(context.Context, string, any) (any, error) {
		t.Fatal("canceled caller reached the transport")
		return nil, nil
	}))
	_, err = call(ctx, nil)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, cause) || s.selected.Load() != 0 {
		t.Fatalf("error=%v selections=%d", err, s.selected.Load())
	}
}
