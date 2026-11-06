package fx

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	uberfx "go.uber.org/fx"

	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/health"
)

// ExitError preserves the nonzero code supplied to native fx.Shutdowner.
// Applications can use errors.As to select their process exit code.
type ExitError struct{ Code int }

func (e *ExitError) Error() string { return fmt.Sprintf("fx: shutdown requested exit code %d", e.Code) }

// Bridge adapts one native Fx application. Register it before servers and workers
// that consume Fx-owned resources, so those consumers stop first. Construct it
// through Hook and do not copy it. Its zero value is not usable.
type Bridge struct {
	name        string
	native      *uberfx.App
	mu          sync.Mutex
	state       state
	startCancel context.CancelFunc
	startDone   chan struct{}
	stopOnce    sync.Once
	done        chan struct{}
	stopErr     error
	errs        chan error
	monitorStop chan struct{}
	monitorDone chan struct{}
	managed     bool
	exitErr     error
}

type state uint8

const (
	initial state = iota
	starting
	running
	stopping
	stopped
)

var (
	_ app.Hook       = (*Bridge)(nil)
	_ health.Checker = (*Bridge)(nil)
)

// Hook connects native Fx Start/Stop without starting goroutines or subscribing
// to signals. Start uses Fx's startup timeout; Stop shares one cleanup operation
// bounded by Fx's shutdown timeout. Each Stop caller can stop waiting independently.
// Use Run to forward native Shutdowner requests and OS signals into servex Run.
// Nil applications and empty names panic during assembly.
func Hook(name string, native *uberfx.App) *Bridge {
	if strings.TrimSpace(name) == "" || native == nil {
		panic("fx: hook name and native application required")
	}
	return &Bridge{name: name, native: native, startDone: make(chan struct{}),
		done: make(chan struct{}), errs: make(chan error, 1), monitorStop: make(chan struct{})}
}

func (h *Bridge) Name() string { return h.name }

// Err reports nonzero native Shutdowner requests when driven through Run.
// A normal request cancels Run's context instead of reporting a runtime failure.
func (h *Bridge) Err() <-chan error { return h.errs }

func (h *Bridge) Start(ctx context.Context) (err error) {
	h.mu.Lock()
	if h.state != initial {
		h.mu.Unlock()
		return errors.New("fx: bridge already started or stopped")
	}
	h.state = starting
	bounded, cancel := context.WithTimeout(ctx, h.native.StartTimeout())
	h.startCancel = cancel
	h.mu.Unlock()
	finished := false
	defer func() {
		cancel()
		h.mu.Lock()
		if !finished {
			err = errors.New("fx: startup interrupted")
		}
		if err == nil && h.state == starting {
			h.state = running
		} else if err == nil {
			err = context.Canceled
		}
		close(h.startDone)
		h.mu.Unlock()
		if err != nil {
			cleanup, finish := context.WithTimeout(context.WithoutCancel(ctx), h.native.StopTimeout())
			defer finish()
			err = errors.Join(err, h.Stop(cleanup))
		}
	}()
	err = h.native.Start(bounded)
	finished = true
	return err
}

// Check reports the bridge lifecycle state. Fx has no application health API;
// register resource-specific readiness hooks with servex separately.
func (h *Bridge) Check(ctx context.Context) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if err := h.native.Err(); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.state != running {
		return errors.New("fx: bridge not running")
	}
	return nil
}

// Stop releases Fx-owned hooks once. A caller timeout bounds waiting, while
// cleanup continues with the native Fx stop budget. Native hooks must honor their
// contexts; neither framework can forcibly stop an uncooperative resource.
func (h *Bridge) Stop(ctx context.Context) error {
	h.stopOnce.Do(func() {
		h.mu.Lock()
		if h.state == initial {
			close(h.startDone)
		}
		h.state = stopping
		if h.startCancel != nil {
			h.startCancel()
		}
		monitorDone := h.monitorDone
		close(h.monitorStop)
		h.mu.Unlock()
		go func() {
			<-h.startDone
			if monitorDone != nil {
				<-monitorDone
			}
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), h.native.StopTimeout())
			defer cancel()
			h.stopErr = h.native.Stop(cleanup)
			h.mu.Lock()
			h.state = stopped
			close(h.errs)
			close(h.done)
			h.mu.Unlock()
		}()
	})
	select {
	case <-h.done:
		return h.stopErr
	default:
	}
	select {
	case <-h.done:
		return h.stopErr
	case <-ctx.Done():
		select {
		case <-h.done:
			return h.stopErr
		default:
			return context.Cause(ctx)
		}
	}
}

// Run drives an explicitly assembled servex application containing this bridge.
// Register the bridge before its consumers. Fx Wait is the only OS signal receiver;
// do not combine this mode with fx.App.Run or app.SignalContext. A normal native
// Shutdowner request is a clean stop; a nonzero code reports ExitError through the
// bridge's Err channel and servex runtime failure handling. The caller retains all
// middleware, dependency and lifecycle assembly choices.
func (h *Bridge) Run(ctx context.Context, application *app.App) (err error) {
	if application == nil {
		panic("fx: servex application required")
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	h.mu.Lock()
	if h.managed || h.state != initial {
		h.mu.Unlock()
		return errors.New("fx: Run requires a fresh bridge")
	}
	h.managed = true
	h.monitorDone = make(chan struct{})
	// Wait only starts native signal reception; it invokes no application hooks.
	wait := h.native.Wait()
	runCtx, cancel := context.WithCancel(ctx)
	forward := func(signal uberfx.ShutdownSignal) {
		if signal.ExitCode == 0 {
			cancel()
			return
		}
		exitErr := &ExitError{Code: signal.ExitCode}
		h.mu.Lock()
		h.exitErr = exitErr
		h.mu.Unlock()
		h.errs <- exitErr
	}
	go func() {
		defer close(h.monitorDone)
		select {
		case signal := <-wait:
			forward(signal)
		case <-h.monitorStop:
			// Cancellation can win Run's select after a native request has
			// already been queued. Retain that exit code before joining.
			select {
			case signal := <-wait:
				forward(signal)
			default:
			}
		}
	}()
	h.mu.Unlock()
	defer func() {
		cancel()
		// Also release the receiver when startup fails, panics or calls Goexit
		// before reaching this bridge. Panic and Goexit continue unchanged.
		cleanup, finish := context.WithTimeout(context.WithoutCancel(ctx), h.native.StopTimeout())
		defer finish()
		stopErr := h.Stop(cleanup)
		if stopErr != nil && !errors.Is(err, stopErr) {
			err = errors.Join(err, stopErr)
		}
		h.mu.Lock()
		exitErr := h.exitErr
		h.mu.Unlock()
		if exitErr != nil && !errors.Is(err, exitErr) {
			err = errors.Join(err, exitErr)
		}
	}()
	return application.Run(runCtx)
}
