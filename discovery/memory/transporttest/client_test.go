package transport_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	sxclient "github.com/nostosx/servex-contrib/transport/client"

	"github.com/nostosx/servex-contrib/discovery/memory"
	"github.com/nostosx/servex/discovery"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/selector"
	"github.com/nostosx/servex/transport"
)

// roundRobin is a deterministic Selector that rotates through the nodes and
// records every node set it was offered and every reported outcome.
type roundRobin struct {
	next atomic.Int64

	mu      sync.Mutex
	offered [][]string
	done    []error
}

func (r *roundRobin) Select(_ context.Context, nodes []selector.Node) (selector.Node, selector.Done, error) {
	addrs := make([]string, 0, len(nodes))
	for _, n := range nodes {
		addrs = append(addrs, n.Addr)
	}
	r.mu.Lock()
	r.offered = append(r.offered, addrs)
	r.mu.Unlock()
	if len(nodes) == 0 {
		return selector.Node{}, nil, selector.ErrNoAvailable
	}
	i := int(r.next.Add(1) - 1)
	return nodes[i%len(nodes)], func(_ context.Context, err error) {
		r.mu.Lock()
		r.done = append(r.done, err)
		r.mu.Unlock()
	}, nil
}

// lastOffered returns the node addresses offered to the latest Select call.
func (r *roundRobin) lastOffered() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.offered) == 0 {
		return nil
	}
	return r.offered[len(r.offered)-1]
}

// addrInvoker returns the address it was invoked with.
func addrInvoker() transport.Invoker {
	return transport.InvokerFunc(func(_ context.Context, addr string, _ any) (any, error) {
		return addr, nil
	})
}

func register(t *testing.T, reg *memory.Registry, id string, endpoints ...string) *discovery.Instance {
	t.Helper()
	ins := &discovery.Instance{ID: id, Name: "svc", Endpoints: endpoints}
	if err := reg.Register(t.Context(), ins); err != nil {
		t.Fatalf("Register(%s): %v", id, err)
	}
	return ins
}

func TestInvokerFunc(t *testing.T) {
	f := transport.InvokerFunc(func(_ context.Context, addr string, req any) (any, error) {
		return fmt.Sprint(addr, req), nil
	})
	got, err := f.Invoke(t.Context(), "a:1", 7)
	if err != nil || got != "a:17" {
		t.Fatalf("Invoke = %v, %v; want a:17, nil", got, err)
	}
}

func TestNewClientInvalidTargets(t *testing.T) {
	reg := memory.New()
	tests := []struct {
		name   string
		target string
		opts   []sxclient.Option
	}{
		{"empty", "", nil},
		{"no scheme", "localhost:8000", nil},
		{"no host", "http://", nil},
		{"unparsable", "http://[::1", nil},
		{"discovery without name", "discovery:///", []sxclient.Option{sxclient.WithDiscoverer(reg), sxclient.WithScheme("http")}},
		{"discovery with host", "discovery://host/svc", []sxclient.Option{sxclient.WithDiscoverer(reg), sxclient.WithScheme("http")}},
		{"discovery nested name", "discovery:///a/b", []sxclient.Option{sxclient.WithDiscoverer(reg), sxclient.WithScheme("http")}},
		{"discovery without discoverer", "discovery:///svc", []sxclient.Option{sxclient.WithScheme("http")}},
		{"discovery without scheme", "discovery:///svc", []sxclient.Option{sxclient.WithDiscoverer(reg)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := newClient(t.Context(), tt.target, tt.opts...)
			if err == nil {
				c.Close()
				t.Fatalf("NewClient(%q) succeeded, want error", tt.target)
			}
		})
	}
}

func TestDirectTarget(t *testing.T) {
	// WithScheme is ignored for direct targets: the target scheme wins.
	c, err := newClient(t.Context(), "grpc://10.0.0.1:9000", sxclient.WithScheme("http"))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	var info metadata.Info
	ep := c.Endpoint("Greeter/SayHello", transport.InvokerFunc(func(ctx context.Context, addr string, req any) (any, error) {
		info, _ = metadata.FromContext(ctx)
		return addr, nil
	}))
	got, err := ep(t.Context(), nil)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if got != "10.0.0.1:9000" {
		t.Errorf("addr = %v, want 10.0.0.1:9000", got)
	}
	if info.Kind != metadata.KindClient || info.Protocol != "grpc" || info.Operation != "Greeter/SayHello" {
		t.Errorf("info = %+v", info)
	}
	if err := c.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func TestEndpointMetadataAndMiddlewareOrder(t *testing.T) {
	var order []string
	mark := func(name string) endpoint.Middleware {
		return func(next endpoint.Endpoint) endpoint.Endpoint {
			return func(ctx context.Context, req any) (any, error) {
				info, ok := metadata.FromContext(ctx)
				if !ok || info.Kind != metadata.KindClient || info.Protocol != "http" || info.Operation != "op" || info.Header == nil {
					t.Errorf("%s: info = %+v, %v", name, info, ok)
				}
				info.Header.Add("x-trace", name)
				order = append(order, name)
				return next(ctx, req)
			}
		}
	}
	c, err := newClient(t.Context(), "http://127.0.0.1:1",
		sxclient.WithMiddleware(mark("client1"), mark("client2")))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	var header []string
	ep := c.Endpoint("op", transport.InvokerFunc(func(ctx context.Context, _ string, _ any) (any, error) {
		info, _ := metadata.FromContext(ctx)
		header = info.Header.Values("X-Trace")
		return "ok", nil
	}), mark("ep"))

	// A server-side Info on the incoming context is replaced.
	ctx := metadata.NewContext(t.Context(), metadata.Info{Kind: metadata.KindServer, Header: metadata.Pairs("x-trace", "inbound")})
	if _, err := ep(ctx, nil); err != nil {
		t.Fatalf("call: %v", err)
	}
	want := []string{"client1", "client2", "ep"}
	if !slices.Equal(order, want) {
		t.Errorf("order = %v, want %v", order, want)
	}
	if !slices.Equal(header, want) {
		t.Errorf("header = %v, want %v", header, want)
	}

	// Every call starts with a fresh header.
	order = nil
	if _, err := ep(t.Context(), nil); err != nil {
		t.Fatalf("second call: %v", err)
	}
	if !slices.Equal(header, want) {
		t.Errorf("second call header = %v, want %v", header, want)
	}
}

func TestNoAvailableNode(t *testing.T) {
	reg := memory.New()
	sel := &roundRobin{}
	c, err := newClient(t.Context(), "discovery:///svc",
		sxclient.WithDiscoverer(reg), sxclient.WithScheme("http"), sxclient.WithSelector(sel))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	invoked := false
	ep := c.Endpoint("op", transport.InvokerFunc(func(context.Context, string, any) (any, error) {
		invoked = true
		return nil, nil
	}))
	_, err = ep(t.Context(), nil)
	if !errors.Is(err, selector.ErrNoAvailable) {
		t.Fatalf("err = %v, want ErrNoAvailable", err)
	}
	if errorx.CodeOf(err) != errorx.Unavailable {
		t.Errorf("code = %v, want Unavailable", errorx.CodeOf(err))
	}
	if invoked {
		t.Error("invoker called without nodes")
	}
}

func TestSelectionInsideMiddlewareChain(t *testing.T) {
	reg := memory.New()
	register(t, reg, "1", "http://10.0.0.1:80")
	register(t, reg, "2", "http://10.0.0.2:80")
	sel := &roundRobin{}
	c, err := newClient(t.Context(), "discovery:///svc",
		sxclient.WithDiscoverer(reg), sxclient.WithScheme("http"), sxclient.WithSelector(sel))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	errFirst := errorx.New(errorx.Unavailable, "BUSY", "busy")
	var hits []string
	inv := transport.InvokerFunc(func(_ context.Context, addr string, _ any) (any, error) {
		hits = append(hits, addr)
		if len(hits) == 1 {
			return nil, errFirst
		}
		return addr, nil
	})
	// retryOnce is a minimal retry middleware: it calls next a second time
	// when the first attempt fails.
	retryOnce := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			resp, err := next(ctx, req)
			if err != nil {
				return next(ctx, req)
			}
			return resp, nil
		}
	}
	got, err := c.Endpoint("op", inv, retryOnce)(t.Context(), nil)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if len(hits) != 2 || hits[0] == hits[1] {
		t.Fatalf("hits = %v, want two different nodes", hits)
	}
	if got != hits[1] {
		t.Errorf("reply = %v, want %v", got, hits[1])
	}
	sel.mu.Lock()
	defer sel.mu.Unlock()
	if len(sel.done) != 2 || !errors.Is(sel.done[0], errFirst) || sel.done[1] != nil {
		t.Errorf("done outcomes = %v, want [%v <nil>]", sel.done, errFirst)
	}
}

func TestDiscoveryUpdatesFilteredByScheme(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reg := memory.New()
		a := register(t, reg, "a", "http://10.0.0.1:80", "grpc://10.0.0.1:90")
		sel := &roundRobin{}
		c, err := newClient(t.Context(), "discovery:///svc",
			sxclient.WithDiscoverer(reg), sxclient.WithScheme("http"), sxclient.WithSelector(sel))
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		defer c.Close()
		ep := c.Endpoint("op", addrInvoker())
		call := func() ([]string, error) {
			_, err := ep(t.Context(), nil)
			return sel.lastOffered(), err
		}

		if nodes, err := call(); err != nil || !slices.Equal(nodes, []string{"10.0.0.1:80"}) {
			t.Fatalf("initial nodes = %v, %v", nodes, err)
		}

		b := register(t, reg, "b", "grpc://10.0.0.2:90", "http://10.0.0.2:80", "://bad")
		register(t, reg, "c", "grpc://10.0.0.3:90")
		synctest.Wait()
		if nodes, err := call(); err != nil || !slices.Equal(nodes, []string{"10.0.0.1:80", "10.0.0.2:80"}) {
			t.Fatalf("nodes after register = %v, %v", nodes, err)
		}

		for _, ins := range []*discovery.Instance{a, b} {
			if err := reg.Deregister(t.Context(), ins); err != nil {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		if _, err := call(); !errors.Is(err, selector.ErrNoAvailable) {
			t.Fatalf("err after deregister = %v, want ErrNoAvailable", err)
		}

		// Close stops the watch goroutine; synctest.Test fails on leaks.
		if err := c.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := c.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
		// After Close, updates are no longer applied.
		register(t, reg, "d", "http://10.0.0.4:80")
		synctest.Wait()
		if _, err := call(); !errors.Is(err, selector.ErrNoAvailable) {
			t.Fatalf("err after Close = %v, want ErrNoAvailable", err)
		}
	})
}

// watchResult is one scripted outcome of fakeWatcher.Next.
type watchResult struct {
	instances []*discovery.Instance
	err       error
}

// fakeWatcher delivers scripted results and blocks until Stop otherwise.
type fakeWatcher struct {
	results  chan watchResult
	stop     chan struct{}
	stopOnce sync.Once
	stops    atomic.Int32
	stopErr  error
}

func newFakeWatcher() *fakeWatcher {
	return &fakeWatcher{results: make(chan watchResult), stop: make(chan struct{})}
}

var errWatcherStopped = errors.New("fake: stopped")

func (w *fakeWatcher) Next() ([]*discovery.Instance, error) {
	select {
	case <-w.stop:
		return nil, errWatcherStopped
	default:
	}
	select {
	case r := <-w.results:
		return r.instances, r.err
	case <-w.stop:
		return nil, errWatcherStopped
	}
}

func (w *fakeWatcher) Stop() error {
	w.stops.Add(1)
	w.stopOnce.Do(func() { close(w.stop) })
	return w.stopErr
}

// fakeDiscoverer hands out a fixed watcher or a fixed error.
type fakeDiscoverer struct {
	w   *fakeWatcher
	err error
}

func (d *fakeDiscoverer) Watch(context.Context, string) (discovery.Watcher, error) {
	if d.err != nil {
		return nil, d.err
	}
	return d.w, nil
}

func instance(id, ep string) *discovery.Instance {
	return &discovery.Instance{ID: id, Name: "svc", Endpoints: []string{ep}}
}

// discovererFunc allows a test to control the setup of a discovery watch.
type discovererFunc func(context.Context, string) (discovery.Watcher, error)

func (f discovererFunc) Watch(ctx context.Context, name string) (discovery.Watcher, error) {
	return f(ctx, name)
}

func TestNewClientWatchSetupIsBoundedByContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := discovererFunc(func(ctx context.Context, _ string) (discovery.Watcher, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		})
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		start := time.Now()
		_, err := newClient(ctx, "discovery:///svc",
			sxclient.WithDiscoverer(d), sxclient.WithScheme("http"))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want DeadlineExceeded", err)
		}
		if d := time.Since(start); d != time.Second {
			t.Errorf("watch setup waited %v, want 1s", d)
		}
	})
}

func TestNewClientWaitIsBoundedByContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newFakeWatcher()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		start := time.Now()
		_, err := newClient(ctx, "discovery:///svc",
			sxclient.WithDiscoverer(&fakeDiscoverer{w: w}), sxclient.WithScheme("http"))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want DeadlineExceeded", err)
		}
		if d := time.Since(start); d != time.Second {
			t.Errorf("waited %v, want 1s", d)
		}
		if w.stops.Load() != 1 {
			t.Errorf("Stop called %d times, want 1", w.stops.Load())
		}
	})
}

func TestWatcherOutlivesNewClientContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newFakeWatcher()
		d := &fakeDiscoverer{w: w}
		go func() {
			w.results <- watchResult{instances: []*discovery.Instance{instance("a", "http://10.0.0.1:80")}}
		}()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		c, err := newClient(ctx, "discovery:///svc",
			sxclient.WithDiscoverer(d), sxclient.WithScheme("http"))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		cancel() // the usual defer cancel() after construction
		w.results <- watchResult{instances: []*discovery.Instance{instance("b", "http://10.0.0.2:80")}}
		synctest.Wait()
		if got, err := c.Endpoint("op", addrInvoker())(t.Context(), nil); err != nil || got != "10.0.0.2:80" {
			t.Fatalf("call after construction context ended = %v, %v; want 10.0.0.2:80, nil", got, err)
		}
		if w.stops.Load() != 0 {
			t.Fatalf("Stop called %d times before Close", w.stops.Load())
		}
	})
}

func TestNewClientWatchErrors(t *testing.T) {
	errWatch := errors.New("watch failed")
	errNext := errors.New("next failed")
	t.Run("watch", func(t *testing.T) {
		_, err := newClient(t.Context(), "discovery:///svc",
			sxclient.WithDiscoverer(&fakeDiscoverer{err: errWatch}), sxclient.WithScheme("http"))
		if !errors.Is(err, errWatch) {
			t.Fatalf("err = %v, want %v", err, errWatch)
		}
	})
	t.Run("first next", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			w := newFakeWatcher()
			go func() { w.results <- watchResult{err: errNext} }()
			_, err := newClient(t.Context(), "discovery:///svc",
				sxclient.WithDiscoverer(&fakeDiscoverer{w: w}), sxclient.WithScheme("http"))
			if !errors.Is(err, errNext) {
				t.Fatalf("err = %v, want %v", err, errNext)
			}
			if w.stops.Load() != 1 {
				t.Errorf("Stop called %d times, want 1", w.stops.Load())
			}
		})
	})
}

func TestWatchRetriesAfterError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newFakeWatcher()
		go func() { w.results <- watchResult{instances: []*discovery.Instance{instance("a", "http://a:1")}} }()
		sel := &roundRobin{}
		c, err := newClient(t.Context(), "discovery:///svc",
			sxclient.WithDiscoverer(&fakeDiscoverer{w: w}), sxclient.WithScheme("http"), sxclient.WithSelector(sel))
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		defer c.Close()
		ep := c.Endpoint("op", addrInvoker())

		// A transient error keeps the last known nodes.
		w.results <- watchResult{err: errors.New("transient")}
		synctest.Wait()
		if got, err := ep(t.Context(), nil); err != nil || got != "a:1" {
			t.Fatalf("call after error = %v, %v; want a:1", got, err)
		}

		// The next Next happens only after the backoff delay.
		delivered := make(chan struct{})
		go func() {
			w.results <- watchResult{instances: []*discovery.Instance{instance("b", "http://b:1")}}
			close(delivered)
		}()
		synctest.Sleep(99 * time.Millisecond)
		select {
		case <-delivered:
			t.Fatal("Next called again before the backoff elapsed")
		default:
		}
		synctest.Sleep(time.Millisecond)
		<-delivered
		synctest.Wait()
		if got, err := ep(t.Context(), nil); err != nil || got != "b:1" {
			t.Fatalf("call after recovery = %v, %v; want b:1", got, err)
		}

		// A failing Stop is reported by every Close call.
		w.stopErr = errors.New("stop failed")
		if err := c.Close(); !errors.Is(err, w.stopErr) {
			t.Fatalf("Close = %v, want %v", err, w.stopErr)
		}
		if err := c.Close(); !errors.Is(err, w.stopErr) {
			t.Fatalf("second Close = %v, want %v", err, w.stopErr)
		}
		if w.stops.Load() != 1 {
			t.Errorf("Stop called %d times, want 1", w.stops.Load())
		}
	})
}

func TestCloseDuringBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newFakeWatcher()
		go func() { w.results <- watchResult{} }()
		c, err := newClient(t.Context(), "discovery:///svc",
			sxclient.WithDiscoverer(&fakeDiscoverer{w: w}), sxclient.WithScheme("http"))
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		w.results <- watchResult{err: errors.New("transient")}
		synctest.Wait()
		start := time.Now()
		if err := c.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if d := time.Since(start); d != 0 {
			t.Errorf("Close waited %v for the backoff", d)
		}
	})
}

func TestConcurrentCallsDuringUpdates(t *testing.T) {
	reg := memory.New()
	register(t, reg, "seed", "http://10.0.0.1:80")
	c, err := newClient(t.Context(), "discovery:///svc",
		sxclient.WithDiscoverer(reg), sxclient.WithScheme("http"))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()
	ep := c.Endpoint("op", addrInvoker())

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			for range 200 {
				if _, err := ep(t.Context(), i); err != nil {
					t.Errorf("call: %v", err)
					return
				}
			}
		})
	}
	wg.Go(func() {
		for i := range 100 {
			ins := instance(fmt.Sprint("n", i), fmt.Sprintf("http://10.0.1.%d:80", i))
			if err := reg.Register(t.Context(), ins); err != nil {
				t.Errorf("Register: %v", err)
				return
			}
			if err := reg.Deregister(t.Context(), ins); err != nil {
				t.Errorf("Deregister: %v", err)
				return
			}
		}
	})
	wg.Wait()
}

func TestWatchBackoffCapsAndResets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newFakeWatcher()
		go func() { w.results <- watchResult{} }()
		c, err := newClient(t.Context(), "discovery:///svc",
			sxclient.WithDiscoverer(&fakeDiscoverer{w: w}), sxclient.WithScheme("http"))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		checkDelay := func(delay time.Duration) {
			t.Helper()
			w.results <- watchResult{err: errors.New("transient")}
			synctest.Wait()
			delivered := make(chan struct{})
			go func() {
				w.results <- watchResult{err: errors.New("transient")}
				close(delivered)
			}()
			synctest.Sleep(delay - time.Nanosecond)
			select {
			case <-delivered:
				t.Fatalf("Next retried before %v", delay)
			default:
			}
			synctest.Sleep(time.Nanosecond)
			<-delivered
			synctest.Wait()
		}
		// Each check delivers two failures; account for both retry delays.
		for _, delay := range []time.Duration{100 * time.Millisecond, 400 * time.Millisecond, 1600 * time.Millisecond, 5 * time.Second, 5 * time.Second} {
			checkDelay(delay)
		}
		w.results <- watchResult{}
		synctest.Wait()
		checkDelay(100 * time.Millisecond)
	})
}
