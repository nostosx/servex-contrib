package do_test

import (
	"context"
	"errors"
	htmltemplate "html/template"
	"io"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	texttemplate "text/template"

	dox "github.com/nostosx/servex-contrib/di/do"
	"github.com/nostosx/servex/di"
)

func newContainer(t *testing.T) *dox.Container {
	t.Helper()
	c := dox.New()
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	return c
}

func mustRegister(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestContainerTypedGraph(t *testing.T) {
	t.Parallel()
	c := newContainer(t)
	var container di.Container = c
	mustRegister(t, di.Supply(container, "hello"))
	var calls atomic.Int32
	mustRegister(t, di.Provide(container, func(r di.Resolver) (*greeter, error) {
		calls.Add(1)
		return newGreeter(di.MustResolve[string](r)), nil
	}))
	if calls.Load() != 0 {
		t.Fatal("Provide eagerly ran constructor")
	}
	var wg sync.WaitGroup
	values := make([]*greeter, 16)
	for idx := range values {
		wg.Go(func() {
			value, err := di.Resolve[*greeter](container)
			if err != nil {
				t.Error(err)
			}
			values[idx] = value
		})
	}
	wg.Wait()
	for _, value := range values {
		if value == nil || value != values[0] || value.message != "hello" {
			t.Fatalf("singleton = %+v", value)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("constructor ran %d times", calls.Load())
	}
}

func TestContainerErrors(t *testing.T) {
	t.Parallel()
	c := newContainer(t)
	if _, err := di.Resolve[int](c); !errors.Is(err, di.ErrNotFound) {
		t.Fatalf("missing = %v", err)
	}
	if err := c.Register(nil, func(di.Resolver) (any, error) { return 0, nil }); !errors.Is(err, di.ErrInvalidArgument) {
		t.Fatalf("nil type = %v", err)
	}
	if _, err := c.Resolve(nil); !errors.Is(err, di.ErrInvalidArgument) {
		t.Fatalf("nil lookup type = %v", err)
	}
	if err := c.Register(reflect.TypeFor[int](), nil); !errors.Is(err, di.ErrInvalidArgument) {
		t.Fatalf("nil provider = %v", err)
	}
	mustRegister(t, di.Supply(c, 1))
	if err := di.Supply(c, 2); !errors.Is(err, di.ErrDuplicate) {
		t.Fatalf("duplicate = %v", err)
	}
	if got, err := di.Resolve[int](c); err != nil || got != 1 {
		t.Fatalf("duplicate replaced original: %d, %v", got, err)
	}
	mustRegister(t, c.Stop(t.Context()))
	if _, err := di.Resolve[int](c); !errors.Is(err, di.ErrClosed) {
		t.Fatalf("closed lookup = %v", err)
	}
	if err := di.Supply(c, "closed"); !errors.Is(err, di.ErrClosed) {
		t.Fatalf("closed registration = %v", err)
	}
}

func TestContainerInterfaceAndTypeIdentity(t *testing.T) {
	t.Parallel()
	c := newContainer(t)
	mustRegister(t, di.Supply[io.Reader](c, strings.NewReader("hello")))
	reader, err := di.Resolve[io.Reader](c)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil || string(got) != "hello" {
		t.Fatalf("interface = %q, %v", got, err)
	}
	// These types share a printed name but have distinct package identities.
	html := &htmltemplate.Template{}
	text := &texttemplate.Template{}
	mustRegister(t, di.Supply(c, html))
	mustRegister(t, di.Supply(c, text))
	if got, err := di.Resolve[*htmltemplate.Template](c); err != nil || got != html {
		t.Fatalf("html type = %v, %v", got, err)
	}
	if got, err := di.Resolve[*texttemplate.Template](c); err != nil || got != text {
		t.Fatalf("text type = %v, %v", got, err)
	}
	mustRegister(t, di.Supply[*int](c, nil))
	if got, err := di.Resolve[*int](c); err != nil || got != nil {
		t.Fatalf("typed nil = %v, %v", got, err)
	}
}

func TestContainerFailureRetryAndCycle(t *testing.T) {
	t.Parallel()
	c := newContainer(t)
	failure := errors.New("database unavailable")
	var attempts int
	mustRegister(t, di.Provide(c, func(di.Resolver) (int, error) {
		attempts++
		if attempts == 1 {
			return 0, failure
		}
		return 42, nil
	}))
	if _, err := di.Resolve[int](c); !errors.Is(err, failure) {
		t.Fatalf("constructor lost cause: %v", err)
	}
	if value, err := di.Resolve[int](c); err != nil || value != 42 {
		t.Fatalf("retry = %d, %v", value, err)
	}
	mustRegister(t, di.Provide(c, func(r di.Resolver) (string, error) {
		return di.MustResolve[string](r), nil
	}))
	if _, err := di.Resolve[string](c); !errors.Is(err, di.ErrCycle) {
		t.Fatalf("cycle = %v", err)
	}
}

func TestContainerInvalidProviderResults(t *testing.T) {
	t.Parallel()
	for _, value := range []any{nil, "not an int"} {
		c := newContainer(t)
		mustRegister(t, c.Register(reflect.TypeFor[int](), func(di.Resolver) (any, error) { return value, nil }))
		if _, err := di.Resolve[int](c); !errors.Is(err, di.ErrInvalidArgument) {
			t.Fatalf("invalid result %v: %v", value, err)
		}
	}
	c := newContainer(t)
	mustRegister(t, di.Supply[io.Writer](c, nil))
	if got, err := di.Resolve[io.Writer](c); err != nil || got != nil {
		t.Fatalf("nil interface = %v, %v", got, err)
	}
}

func TestContainerConcurrentCyclicRoots(t *testing.T) {
	t.Parallel()
	c := newContainer(t)
	type first struct{}
	type second struct{}
	mustRegister(t, di.Provide(c, func(r di.Resolver) (*first, error) {
		di.MustResolve[*second](r)
		return &first{}, nil
	}))
	mustRegister(t, di.Provide(c, func(r di.Resolver) (*second, error) {
		di.MustResolve[*first](r)
		return &second{}, nil
	}))
	ready := make(chan struct{})
	errs := make(chan error, 2)
	go func() { <-ready; _, err := di.Resolve[*first](c); errs <- err }()
	go func() { <-ready; _, err := di.Resolve[*second](c); errs <- err }()
	close(ready)
	for range 2 {
		if err := <-errs; !errors.Is(err, di.ErrCycle) {
			t.Fatalf("concurrent cycle = %v", err)
		}
	}
}

func TestContainerCleanupAndMissingDependency(t *testing.T) {
	t.Parallel()
	c := newContainer(t)
	var closed atomic.Bool
	// Supplied resources must be owned even without an explicit Resolve.
	mustRegister(t, di.Supply(c, &resource{shutdown: func(context.Context) error {
		closed.Store(true)
		return nil
	}}))
	mustRegister(t, di.Provide(c, func(r di.Resolver) (string, error) {
		di.MustResolve[int](r)
		return "unreachable", nil
	}))
	if _, err := di.Resolve[string](c); !errors.Is(err, di.ErrNotFound) {
		t.Fatalf("nested missing dependency lost cause: %v", err)
	}
	mustRegister(t, c.Shutdown(t.Context()))
	if !closed.Load() {
		t.Fatal("supplied resource leaked")
	}
}
