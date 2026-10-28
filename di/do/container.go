package do

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/samber/do/v2"

	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/di"
	"github.com/nostosx/servex/health"
)

// Container implements di.Container with samber/do lazy singletons and
// dependency tracking. It also implements app.Hook and health.Checker through
// the lifecycle adapter. Register it before server/worker hooks. New must be
// used; the zero value is not usable. Do not copy a Container.
type Container struct {
	*injectorHook
	types     map[reflect.Type]string
	resolveMu sync.Mutex
}

var (
	_ di.Container   = (*Container)(nil)
	_ app.Hook       = (*Container)(nil)
	_ health.Checker = (*Container)(nil)
)

// New creates an isolated container named "di" with no global registrations.
// Supply/Provide and Resolve can use the core di API exclusively. Ordinary
// constructors do not depend on samber/do. Call Stop even if assembly fails.
func New() *Container {
	return &Container{
		injectorHook: &injectorHook{name: "di", injector: do.New()},
		types:        make(map[reflect.Type]string),
	}
}

// Register installs a lazy singleton. The provider is run at most once after
// a successful construction; errors preserve their causes. Expected dependency
// lookup errors panicked by di.MustResolve are recovered without losing identity.
func (c *Container) Register(t reflect.Type, provider di.Provider) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return di.ErrClosed
	}
	if t == nil || provider == nil {
		return fmt.Errorf("%w: nil type or provider", di.ErrInvalidArgument)
	}
	if _, ok := c.types[t]; ok {
		return fmt.Errorf("%w: %v", di.ErrDuplicate, t)
	}
	name := fmt.Sprintf("servex.di.%d:%v", len(c.types), t)
	do.ProvideNamed(c.injector, name, func(i do.Injector) (any, error) {
		value, err := provider(resolver{container: c, injector: i})
		if err != nil {
			return nil, err
		}
		if value == nil {
			switch t.Kind() {
			case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice, reflect.UnsafePointer:
				return reflect.Zero(t).Interface(), nil
			}
		} else if reflect.TypeOf(value).AssignableTo(t) {
			return value, nil
		}
		return nil, fmt.Errorf("%w: provider for %v returned %T", di.ErrInvalidArgument, t, value)
	})
	c.types[t] = name
	return nil
}

// Resolve gets the exact registered type. It is safe to call concurrently
// after registration. The lifecycle lock excludes shutdown and new registrations
// while the complete provider chain is resolving.
func (c *Container) Resolve(t reflect.Type) (any, error) {
	// Serialize root chains. Otherwise two roots in a cyclic graph can each
	// hold one of do's singleton locks while waiting for the other.
	c.resolveMu.Lock()
	defer c.resolveMu.Unlock()
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.stopped {
		return nil, di.ErrClosed
	}
	return c.resolve(c.injector, t)
}

func (c *Container) resolve(i do.Injector, t reflect.Type) (any, error) {
	if t == nil {
		return nil, fmt.Errorf("%w: nil type", di.ErrInvalidArgument)
	}
	name, ok := c.types[t]
	if !ok {
		return nil, fmt.Errorf("%w: %v", di.ErrNotFound, t)
	}
	value, err := do.InvokeNamed[any](i, name)
	if err != nil {
		if errors.Is(err, do.ErrCircularDependency) {
			err = errors.Join(di.ErrCycle, err)
		}
		return nil, fmt.Errorf("di: resolve %v: %w", t, err)
	}
	return value, nil
}

// Shutdown is the explicit cleanup equivalent of the app.Hook Stop method.
func (c *Container) Shutdown(ctx context.Context) error { return c.Stop(ctx) }

// resolver carries do's invocation path so dependency lookups record edges
// and detect cycles. The outer Container.Resolve holds the lifecycle lock.
type resolver struct {
	container *Container
	injector  do.Injector
}

func (r resolver) Resolve(t reflect.Type) (any, error) {
	return r.container.resolve(r.injector, t)
}
