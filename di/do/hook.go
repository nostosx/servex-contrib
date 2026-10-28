package do

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/samber/do/v2"

	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/health"
)

// Hook adapts an injector to app.Hook and health.Checker. Register it before
// hooks that consume container-owned resources so those hooks stop first.
// Start is a no-op: resolve the application's entry points before app.Start
// to discover missing dependencies without serving traffic. Check delegates
// to the injector's health checks; it does not instantiate lazy services.
// As in do itself, checks cover this scope and its ancestors, not children.
// Stop shuts down this scope and its children in do's dependency order.
// Stop is idempotent; repeated calls return the first shutdown error. Checks
// through this hook are serialized against Stop and fail after it stops.
//
// Only services implementing a do Shutdown interface are cleaned up by do;
// Close and app.Hook.Stop are not called automatically. Give each resource
// one lifecycle owner. Do not register the App itself in the owned injector.
// The caller must call Shutdown if assembly fails before app.Start, since
// an unstarted App does not stop hooks. Hook panics if injector is nil.
func Hook(name string, injector do.Injector) app.Hook {
	if injector == nil {
		panic("do: Hook with nil injector")
	}
	return &injectorHook{name: name, injector: injector}
}

type injectorHook struct {
	name     string
	injector do.Injector
	mu       sync.RWMutex
	stopped  bool
	stopErr  error
}

var (
	_ app.Hook       = (*injectorHook)(nil)
	_ health.Checker = (*injectorHook)(nil)
)

func (h *injectorHook) Name() string { return h.name }

func (h *injectorHook) Start(ctx context.Context) error {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.stopped {
		return errors.New("do: injector hook stopped")
	}
	return ctx.Err()
}

func (h *injectorHook) Stop(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.stopped {
		h.stopped = true
		h.stopErr = Shutdown(ctx, h.injector)
	}
	return h.stopErr
}

func (h *injectorHook) Check(ctx context.Context) error {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.stopped {
		return errors.New("do: injector hook stopped")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	results := h.injector.HealthCheckWithContext(ctx)
	var errs []error
	for _, name := range slices.Sorted(maps.Keys(results)) {
		if err := results[name]; err != nil {
			errs = append(errs, fmt.Errorf("do: health check %q: %w", name, err))
		}
	}
	// do's health-check timeout wraps its own sentinel and formats ctx.Err
	// as text. Retain the caller's context error for errors.Is as well.
	if err := ctx.Err(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// Shutdown releases the injector's scope and children using ctx. It returns
// nil on success and joins individual shutdown errors with their service and
// scope names, preserving their identity through errors.Is and errors.As.
// Use it to clean up successfully constructed dependencies when assembly fails.
// Like do, it does not close plain io.Closers or retain transient instances.
// Do not resolve services or run health checks concurrently with shutdown.
// Shutdown panics if injector is nil; it never uses do's global container.
func Shutdown(ctx context.Context, injector do.Injector) error {
	if injector == nil {
		panic("do: Shutdown with nil injector")
	}
	report := injector.ShutdownWithContext(ctx)
	var errs []error
	for service, err := range report.Errors {
		if err != nil {
			errs = append(errs, fmt.Errorf("do: shutdown %q in scope %q: %w", service.Service, service.ScopeName, err))
		}
	}
	slices.SortFunc(errs, func(a, b error) int { return strings.Compare(a.Error(), b.Error()) })
	return errors.Join(errs...)
}
