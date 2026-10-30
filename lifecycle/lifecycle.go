// Package lifecycle connects explicitly owned SDK resources to servex. Scope
// also releases resources if application assembly fails before App.Start.
package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nostosx/servex/app"
)

var (
	ErrStopped  = errors.New("lifecycle: resource stopped")
	ErrNotReady = errors.New("lifecycle: scope not running")
)

// Operations declares resource ownership explicitly. A nil Close borrows the
// value. Check and Close must be safe to overlap and honor context where possible.
// Close must attempt cleanup even if its context is already canceled.
type Operations[T any] struct {
	Check func(context.Context, T) error
	Close func(context.Context, T) error
}

type Resource[T any] struct {
	value      T
	name       string
	operations Operations[T]
	once       sync.Once
	done       chan struct{}
	mu         sync.Mutex
	closed     bool
	err        error
}

func New[T any](name string, value T, operations Operations[T]) (*Resource[T], error) {
	if strings.TrimSpace(name) == "" {
		return nil, errors.New("lifecycle: resource name required")
	}
	return &Resource[T]{name: name, value: value, operations: operations, done: make(chan struct{})}, nil
}

func (r *Resource[T]) Value() T                        { return r.value }
func (r *Resource[T]) Name() string                    { return r.name }
func (r *Resource[T]) Start(ctx context.Context) error { return r.Check(ctx) }
func (r *Resource[T]) Check(ctx context.Context) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return ErrStopped
	}
	if r.operations.Check != nil {
		if err := r.operations.Check(ctx, r.value); err != nil {
			return err
		}
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	closed = r.closed
	r.mu.Unlock()
	if closed {
		return ErrStopped
	}
	return nil
}

// Stop invokes Close exactly once, including before Start. ctx bounds waiting;
// an SDK Close that cannot be interrupted can continue in the cleanup goroutine.
func (r *Resource[T]) Stop(ctx context.Context) error {
	r.once.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.mu.Unlock()
		go func() {
			if r.operations.Close != nil {
				r.err = r.operations.Close(ctx, r.value)
			}
			close(r.done)
		}()
	})
	return waitStopped(ctx, r.done, func() error { return r.err })
}

// Shutdown and HealthCheck let the same explicit resource participate in do's
// native lifecycle, when do is its sole owner. Never give it two owners.
func (r *Resource[T]) Shutdown(ctx context.Context) error    { return r.Stop(ctx) }
func (r *Resource[T]) HealthCheck(ctx context.Context) error { return r.Check(ctx) }

// Scope owns resources added during assembly, starts them in order and releases
// them in reverse. Register it before the container, servers and workers.
type Scope struct {
	mu          sync.Mutex
	hooks       []app.Hook
	state       scopeState
	startDone   chan struct{}
	startCancel context.CancelFunc
	once        sync.Once
	done        chan struct{}
	err         error
	timeout     time.Duration
}

type scopeState uint8

const (
	assembling scopeState = iota
	starting
	running
	stopping
	stopped
)

var _ app.Hook = (*Scope)(nil)

func NewScope(timeout time.Duration) (*Scope, error) {
	if timeout <= 0 {
		return nil, errors.New("lifecycle: positive cleanup timeout required")
	}
	return &Scope{timeout: timeout, done: make(chan struct{}), startDone: make(chan struct{})}, nil
}

func (s *Scope) Name() string { return "resources" }

// Add transfers ownership immediately. Hooks must support idempotent Stop even
// if Start was never called. Borrowed values should have no Close operation.
func (s *Scope) Add(hook app.Hook) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != assembling {
		return errors.New("lifecycle: scope already started or stopped")
	}
	if nilHook(hook) {
		return errors.New("lifecycle: nil resource")
	}
	if strings.TrimSpace(hook.Name()) == "" {
		return errors.New("lifecycle: resource name required")
	}
	for _, existing := range s.hooks {
		if existing.Name() == hook.Name() {
			return fmt.Errorf("lifecycle: duplicate resource %s", hook.Name())
		}
	}
	s.hooks = append(s.hooks, hook)
	return nil
}

func (s *Scope) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.state != assembling {
		s.mu.Unlock()
		return errors.New("lifecycle: scope already started or stopped")
	}
	s.state = starting
	startCtx, cancel := context.WithCancel(ctx)
	s.startCancel = cancel
	hooks := slices.Clone(s.hooks)
	s.mu.Unlock()
	err := startHooks(startCtx, hooks)
	s.mu.Lock()
	if err == nil && s.state == starting {
		s.state = running
	} else if err == nil {
		err = context.Canceled
	}
	close(s.startDone)
	s.mu.Unlock()
	if err != nil {
		cleanup, finish := context.WithTimeout(context.Background(), s.timeout)
		defer finish()
		return errors.Join(err, s.Stop(cleanup))
	}
	return nil
}

func startHooks(ctx context.Context, hooks []app.Hook) error {
	for _, hook := range hooks {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if err := hook.Start(ctx); err != nil {
			return fmt.Errorf("lifecycle: start %s: %w", hook.Name(), err)
		}
	}
	return context.Cause(ctx)
}

func (s *Scope) Check(ctx context.Context) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	if s.state != running {
		s.mu.Unlock()
		return ErrNotReady
	}
	hooks := slices.Clone(s.hooks)
	s.mu.Unlock()
	for _, hook := range hooks {
		if checker, ok := hook.(interface{ Check(context.Context) error }); ok {
			if err := checker.Check(ctx); err != nil {
				return fmt.Errorf("lifecycle: check %s: %w", hook.Name(), err)
			}
		}
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != running {
		return ErrNotReady
	}
	return nil
}

// Stop cancels any startup, then releases all constructed resources in reverse
// order. ctx bounds this caller's wait, not other callers or the shared cleanup.
// The scope's timeout is sent to cleanup operations. A timed-out resource Stop
// is joined before closing its dependency, even if an SDK Close ignores deadlines.
func (s *Scope) Stop(ctx context.Context) error {
	s.once.Do(func() {
		s.mu.Lock()
		if s.state == assembling {
			close(s.startDone)
		}
		s.state = stopping
		if s.startCancel != nil {
			s.startCancel()
		}
		hooks := slices.Clone(s.hooks)
		s.mu.Unlock()
		go func() {
			// Never close an SDK while its startup callback is still running.
			<-s.startDone
			cleanup, cancel := context.WithTimeout(context.Background(), s.timeout)
			defer cancel()
			for _, hook := range slices.Backward(hooks) {
				err := hook.Stop(cleanup)
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					// Stop's result may be only a waiting deadline, while an
					// idempotent resource is still closing. Join actual cleanup
					// before releasing its dependency. All Add'ed hooks must
					// support repeat Stop calls, including after Start failure.
					err = hook.Stop(context.Background())
				}
				if err != nil {
					s.err = errors.Join(s.err, fmt.Errorf("lifecycle: stop %s: %w", hook.Name(), err))
				}
			}
			s.mu.Lock()
			s.state = stopped
			close(s.done)
			s.mu.Unlock()
		}()
	})
	return waitStopped(ctx, s.done, func() error { return s.err })
}

func waitStopped(ctx context.Context, done <-chan struct{}, result func() error) error {
	select {
	case <-done:
		return result()
	default:
	}
	select {
	case <-done:
		return result()
	case <-ctx.Done():
		// Prefer a completed result over a simultaneous waiting cancellation.
		select {
		case <-done:
			return result()
		default:
			return context.Cause(ctx)
		}
	}
}

func nilHook(hook app.Hook) bool {
	if hook == nil {
		return true
	}
	value := reflect.ValueOf(hook)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// Finish belongs in the composition root as defer scope.Finish(&err). Its fresh
// context also covers constructor errors, canceled startup and App rollback.
func (s *Scope) Finish(result *error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	if err := s.Stop(ctx); err != nil && !errors.Is(*result, err) {
		*result = errors.Join(*result, err)
	}
}
