package client

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/transport"
)

// Policy governs a complete unary call and each selected-node attempt. Zero
// values make one attempt with the caller's deadline. Operation policies replace
// the default completely; they are not merged. Callbacks must be concurrency-safe.
type Policy struct {
	Timeout        time.Duration
	AttemptTimeout time.Duration
	MaxAttempts    int
	RetrySafe      bool
	RetryIf        func(error) bool
	Backoff        func(attempt int) time.Duration
}

func (p Policy) validate() error {
	if p.Timeout < 0 || p.AttemptTimeout < 0 || p.MaxAttempts < 0 {
		return errors.New("client: negative governance policy value")
	}
	if p.MaxAttempts > 1 && (!p.RetrySafe || p.RetryIf == nil) {
		return errors.New("client: retries require RetrySafe and RetryIf")
	}
	return nil
}

// WithPolicy selects a default unary policy. Do not stack automatic SDK or
// endpoint retry middleware with it; there must be one retry owner per call.
func WithPolicy(policy Policy) Option {
	return func(o *clientOptions) { o.policy = policy }
}

// WithOperationPolicies captures exact stable operation policies at configuration.
// A new map is owned by the client; no request-time mutation or global state exists.
func WithOperationPolicies(policies map[string]Policy) Option {
	owned := maps.Clone(policies)
	return func(o *clientOptions) { o.policies = maps.Clone(owned) }
}

// WithAttemptMiddleware wraps native invocations after node selection, separately
// from logical-call WithMiddleware. Attempt middleware must not replay next.
func WithAttemptMiddleware(middleware ...transport.AttemptMiddleware) Option {
	owned := append([]transport.AttemptMiddleware(nil), middleware...)
	return func(o *clientOptions) { o.attempt = append(o.attempt, owned...) }
}

func validatePolicies(o clientOptions) error {
	if err := o.policy.validate(); err != nil {
		return err
	}
	for operation, p := range o.policies {
		if operation == "" {
			return errors.New("client: empty operation policy")
		}
		if err := p.validate(); err != nil {
			return fmt.Errorf("client: policy %q: %w", operation, err)
		}
	}
	return nil
}

func (p Policy) call(ctx context.Context, request any, attempt func(context.Context, any, int) (any, error)) (any, error) {
	var result any
	var err error
	for number := 1; number <= max(p.MaxAttempts, 1); number++ {
		if ctx.Err() != nil {
			return result, errors.Join(ctx.Err(), context.Cause(ctx))
		}
		result, err = func() (any, error) {
			if p.AttemptTimeout > 0 {
				bounded, cancel := context.WithTimeout(ctx, p.AttemptTimeout)
				defer cancel()
				return attempt(bounded, request, number)
			}
			return attempt(ctx, request, number)
		}()
		if err == nil || errors.Is(err, errClientClosed) || number == max(p.MaxAttempts, 1) || !p.RetrySafe || p.RetryIf == nil || !p.RetryIf(err) {
			return result, err
		}
		if p.Backoff != nil {
			timer := time.NewTimer(max(p.Backoff(number), 0))
			select {
			case <-ctx.Done():
				timer.Stop()
				return result, errors.Join(ctx.Err(), context.Cause(ctx))
			case <-timer.C:
			}
		}
	}
	return result, err
}

// Attempt adapts existing endpoint policies explicitly to the selected-node
// boundary. The address stays native to Invoker; transport attempt context lets
// policies choose a per-node key. Do not pass retry middleware here.
func Attempt(middleware ...endpoint.Middleware) transport.AttemptMiddleware {
	chain := endpoint.Chain(middleware...)
	return func(next transport.Invoker) transport.Invoker {
		wrapped := chain(func(ctx context.Context, request any) (any, error) {
			info, _ := transport.AttemptFromContext(ctx)
			return next.Invoke(ctx, info.Address, request)
		})
		return transport.InvokerFunc(func(ctx context.Context, address string, request any) (any, error) {
			info, _ := transport.AttemptFromContext(ctx)
			info.Address = address
			return wrapped(transport.WithAttempt(ctx, info), request)
		})
	}
}

// ChainAttempts composes native invoker middleware, first outermost. Nil entries
// are skipped and the slice is copied. Assemble before issuing calls.
func chainAttempts(middleware ...transport.AttemptMiddleware) transport.AttemptMiddleware {
	owned := slices.Clone(middleware)
	return func(next transport.Invoker) transport.Invoker {
		for _, decorate := range slices.Backward(owned) {
			if decorate != nil {
				next = decorate(next)
			}
		}
		return next
	}
}
