package retry_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nostosx/servex-contrib/middleware/circuitbreaker"
	"github.com/nostosx/servex-contrib/middleware/retry"
	"github.com/nostosx/servex/errorx"
)

var (
	errUnavailable = errorx.New(errorx.Unavailable, "DOWN", "backend down")
	errInvalid     = errorx.New(errorx.InvalidArgument, "BAD", "bad request")
)

// script is a fake endpoint returning errs[i] on its i-th call and "ok" once
// the script is exhausted. It records the call times.
type script struct {
	errs []error

	mu    sync.Mutex
	calls []time.Time
}

func (s *script) endpoint(context.Context, any) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := len(s.calls)
	s.calls = append(s.calls, time.Now())
	if i < len(s.errs) {
		return fmt.Sprintf("resp%d", i), s.errs[i]
	}
	return "ok", nil
}

func (s *script) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

// fixed returns a backoff with a constant delay that records the attempts it
// was asked about.
func fixed(d time.Duration, attempts *[]int) func(int) time.Duration {
	var mu sync.Mutex
	return func(attempt int) time.Duration {
		mu.Lock()
		defer mu.Unlock()
		*attempts = append(*attempts, attempt)
		return d
	}
}

func TestSuccessFirstAttempt(t *testing.T) {
	s := &script{}
	resp, err := retry.New()(s.endpoint)(t.Context(), nil)
	if err != nil || resp != "ok" || s.count() != 1 {
		t.Fatalf("ep = %v, %v after %d calls; want ok, nil after 1", resp, err, s.count())
	}
}

func TestRetriesUntilSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var attempts []int
		s := &script{errs: []error{errUnavailable, errUnavailable}}
		start := time.Now()
		resp, err := retry.New(retry.WithBackoff(fixed(time.Second, &attempts)))(s.endpoint)(t.Context(), nil)
		if err != nil || resp != "ok" {
			t.Fatalf("ep = %v, %v; want ok, nil", resp, err)
		}
		if s.count() != 3 {
			t.Errorf("calls = %d, want 3", s.count())
		}
		if !slices.Equal(attempts, []int{1, 2}) {
			t.Errorf("backoff attempts = %v, want [1 2]", attempts)
		}
		if elapsed := time.Since(start); elapsed != 2*time.Second {
			t.Errorf("elapsed = %v, want 2s", elapsed)
		}
	})
}

func TestMaxAttempts(t *testing.T) {
	tests := []struct {
		name string
		opts []retry.Option
		want int
	}{
		{"default", nil, retry.DefaultMaxAttempts},
		{"five", []retry.Option{retry.WithMaxAttempts(5)}, 5},
		{"one", []retry.Option{retry.WithMaxAttempts(1)}, 1},
		{"zero", []retry.Option{retry.WithMaxAttempts(0)}, 1},
		{"negative", []retry.Option{retry.WithMaxAttempts(-3)}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &script{errs: slices.Repeat([]error{errUnavailable}, 10)}
			opts := slices.Concat(tt.opts, []retry.Option{retry.WithBackoff(func(int) time.Duration { return 0 })})
			resp, err := retry.New(opts...)(s.endpoint)(t.Context(), nil)
			if s.count() != tt.want {
				t.Errorf("calls = %d, want %d", s.count(), tt.want)
			}
			// The response and error of the last attempt are returned.
			if err != errUnavailable || resp != fmt.Sprintf("resp%d", tt.want-1) {
				t.Errorf("ep = %v, %v; want resp%d, %v", resp, err, tt.want-1, errUnavailable)
			}
		})
	}
}

func TestNonRetryableStops(t *testing.T) {
	for _, err := range []error{errInvalid, errors.New("plain error"), errorx.New(errorx.Unavailable, "X", "y").WithRetryable(false)} {
		s := &script{errs: []error{err}}
		_, got := retry.New()(s.endpoint)(t.Context(), nil)
		if got != err || s.count() != 1 {
			t.Errorf("err %v: got %v after %d calls, want it after 1", err, got, s.count())
		}
	}
}

// TestRetriesRemoteErrors checks that the hop receiving a remote error
// retries it, while the error still reports to the next caller that it must
// not be retried again.
func TestRetriesRemoteErrors(t *testing.T) {
	remote := errUnavailable.WithRemote()
	s := &script{errs: slices.Repeat([]error{remote}, retry.DefaultMaxAttempts)}
	_, err := retry.New(retry.WithBackoff(func(int) time.Duration { return 0 }))(s.endpoint)(t.Context(), nil)
	if err != remote || s.count() != retry.DefaultMaxAttempts {
		t.Fatalf("err = %v after %d calls; want the remote error after %d", err, s.count(), retry.DefaultMaxAttempts)
	}
	if got := errorx.From(err); !got.Remote() || got.PropagatedRetryable() {
		t.Fatalf("remote error lost its mark: Remote %v, PropagatedRetryable %v", got.Remote(), got.PropagatedRetryable())
	}
}

func TestRetryIf(t *testing.T) {
	s := &script{errs: []error{errInvalid, errInvalid}}
	_, err := retry.New(
		retry.WithRetryIf(func(err error) bool { return errorx.CodeOf(err) == errorx.InvalidArgument }),
		retry.WithBackoff(func(int) time.Duration { return 0 }),
	)(s.endpoint)(t.Context(), nil)
	if err != nil || s.count() != 3 {
		t.Fatalf("err = %v after %d calls; want nil after 3", err, s.count())
	}
}

func TestNilOptionsKeepDefaults(t *testing.T) {
	s := &script{errs: []error{errInvalid}}
	_, err := retry.New(nil, retry.WithRetryIf(nil), retry.WithBackoff(nil))(s.endpoint)(t.Context(), nil)
	if err != errInvalid || s.count() != 1 {
		t.Fatalf("err = %v after %d calls; want %v after 1", err, s.count(), errInvalid)
	}
}

// TestNeverRetriesOpenCircuit checks that an open breaker stops retrying even
// when the predicate accepts every error.
func TestNeverRetriesOpenCircuit(t *testing.T) {
	for _, err := range []error{
		circuitbreaker.ErrOpen,
		fmt.Errorf("wrapped: %w", circuitbreaker.ErrOpen),
		errorx.New(errorx.Unavailable, circuitbreaker.ReasonOpen, "from a remote breaker"),
	} {
		s := &script{errs: []error{err, err}}
		_, got := retry.New(retry.WithRetryIf(func(error) bool { return true }))(s.endpoint)(t.Context(), nil)
		if got != err || s.count() != 1 {
			t.Errorf("err %v: got %v after %d calls, want it after 1", err, got, s.count())
		}
	}
}

func TestCancelDuringBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		time.AfterFunc(300*time.Millisecond, cancel)
		s := &script{errs: slices.Repeat([]error{errUnavailable}, 10)}
		start := time.Now()
		_, err := retry.New(retry.WithBackoff(func(int) time.Duration { return time.Hour }))(s.endpoint)(ctx, nil)
		if elapsed := time.Since(start); elapsed != 300*time.Millisecond {
			t.Errorf("elapsed = %v, want 300ms", elapsed)
		}
		if err != errUnavailable || s.count() != 1 {
			t.Errorf("err = %v after %d calls; want last error after 1", err, s.count())
		}
	})
}

func TestDeadlineBoundsBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 2500*time.Millisecond)
		defer cancel()
		s := &script{errs: slices.Repeat([]error{errUnavailable}, 10)}
		start := time.Now()
		_, err := retry.New(
			retry.WithMaxAttempts(10),
			retry.WithBackoff(func(int) time.Duration { return time.Second }),
		)(s.endpoint)(ctx, nil)
		if elapsed := time.Since(start); elapsed != 2500*time.Millisecond {
			t.Errorf("elapsed = %v, want 2.5s", elapsed)
		}
		if err != errUnavailable || s.count() != 3 {
			t.Errorf("err = %v after %d calls; want last error after 3", err, s.count())
		}
	})
}

// TestCancelDuringAttempt checks that an attempt failing because the
// caller's context ended is not retried, whatever its error.
func TestCancelDuringAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	calls := 0
	_, err := retry.New(retry.WithBackoff(func(int) time.Duration { return 0 }))(func(context.Context, any) (any, error) {
		calls++
		cancel()
		return nil, errUnavailable
	})(ctx, nil)
	if err != errUnavailable || calls != 1 {
		t.Fatalf("err = %v after %d calls; want %v after 1", err, calls, errUnavailable)
	}
}

func TestDefaultBackoffTiming(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &script{errs: slices.Repeat([]error{errUnavailable}, 10)}
		_, _ = retry.New(retry.WithMaxAttempts(7))(s.endpoint)(t.Context(), nil)
		if s.count() != 7 {
			t.Fatalf("calls = %d, want 7", s.count())
		}
		for i := 1; i < len(s.calls); i++ {
			d := min(retry.DefaultBaseDelay<<(i-1), retry.DefaultMaxDelay)
			if got := s.calls[i].Sub(s.calls[i-1]); got < d/2 || got > d {
				t.Errorf("delay before attempt %d = %v, want in [%v, %v]", i+1, got, d/2, d)
			}
		}
	})
}

func TestExponential(t *testing.T) {
	b := retry.Exponential(100*time.Millisecond, time.Second)
	want := []time.Duration{100, 200, 400, 800, 1000, 1000, 1000}
	for i, w := range want {
		w *= time.Millisecond
		for range 200 {
			if got := b(i + 1); got < w/2 || got > w {
				t.Fatalf("attempt %d: delay %v, want in [%v, %v]", i+1, got, w/2, w)
			}
		}
	}
	if got := b(1000); got < 500*time.Millisecond || got > time.Second {
		t.Errorf("attempt 1000: delay %v, want capped at 1s", got)
	}
	for _, bad := range []func(int) time.Duration{retry.Exponential(0, time.Second), retry.Exponential(time.Second, 0)} {
		if got := bad(3); got != 0 {
			t.Errorf("degenerate backoff = %v, want 0", got)
		}
	}
}

func TestConcurrent(t *testing.T) {
	ep := retry.New(retry.WithBackoff(func(int) time.Duration { return 0 }))(func(_ context.Context, req any) (any, error) {
		if req.(int)%2 == 0 {
			return nil, errUnavailable
		}
		return req, nil
	})
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			resp, err := ep(t.Context(), i)
			if i%2 == 0 && err != errUnavailable || i%2 == 1 && resp != i {
				t.Errorf("req %d: got %v, %v", i, resp, err)
			}
		})
	}
	wg.Wait()
}

func TestExponentialSaturatesWithoutOverflow(t *testing.T) {
	const limit = time.Duration(1<<63 - 1)
	for _, attempt := range []int{1, 2, 3, int(^uint(0) >> 1)} {
		b := retry.Exponential(limit/2+1, limit)
		upper := limit
		if attempt == 1 {
			upper = limit/2 + 1
		}
		for range 100 {
			if got := b(attempt); got < upper/2 || got > upper {
				t.Fatalf("attempt %d: delay %v, want in [%v, %v]", attempt, got, upper/2, upper)
			}
		}
	}
}
