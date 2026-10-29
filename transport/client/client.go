package client

import (
	"context"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nostosx/servex/discovery"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/selector"
	"github.com/nostosx/servex/transport"

	"github.com/nostosx/kitx/backoff"
)

// discoveryScheme is the target scheme that selects service discovery.
const discoveryScheme = "discovery"

// Backoff bounds for retrying a failing Watcher.Next in the background.
const (
	minWatchRetry = 100 * time.Millisecond
	maxWatchRetry = 5 * time.Second
)

// Option configures a Client.
type Option func(*clientOptions)

// clientOptions holds the configuration collected from Options.
type clientOptions struct {
	discoverer discovery.Discoverer
	selector   selector.Selector
	scheme     string
	middleware []endpoint.Middleware
	observer   func([]selector.Node)
}

// WithDiscoverer sets the Discoverer used to resolve "discovery:///<name>"
// targets. It is required for such targets and ignored for direct ones.
func WithDiscoverer(d discovery.Discoverer) Option {
	return func(o *clientOptions) { o.discoverer = d }
}

// WithSelector sets the Selector used to pick a node for every call.
// A selector is required; the caller chooses its implementation explicitly.
func WithSelector(s selector.Selector) Option {
	return func(o *clientOptions) {
		if s != nil {
			o.selector = s
		}
	}
}

// WithScheme sets the protocol of a discovery target: only instance
// endpoints whose URL scheme equals scheme become nodes, and scheme is
// reported as the call protocol. It is required for discovery targets and
// ignored for direct targets, whose own scheme is used instead.
func WithScheme(scheme string) Option {
	return func(o *clientOptions) { o.scheme = scheme }
}

// WithMiddleware appends client-level middleware. It wraps every endpoint
// built by the Client, outside of the per-endpoint middleware.
func WithMiddleware(m ...endpoint.Middleware) Option {
	return func(o *clientOptions) { o.middleware = append(o.middleware, m...) }
}

// WithNodeObserver sets fn to be called with the node set of the Client
// whenever it changes, for example to keep a stateful selector or a metric in
// sync with service discovery. Two node sets are equal when they hold the
// same nodes, compared by Addr, Weight and Metadata, in the same order.
//
// The first call happens before New returns, with the initial node set,
// possibly empty. Later calls come from the background watch goroutine of a
// discovery target; a direct target has no later calls. Calls are serialized
// and never made after Close has returned. fn receives a deep copy it may
// keep. fn must return promptly, since it delays applying the next update,
// and must not call Close. A nil fn disables observation.
func WithNodeObserver(fn func([]selector.Node)) Option {
	return func(o *clientOptions) { o.observer = fn }
}

// Client resolves a target into a set of nodes and builds endpoints that
// select one node per call. All methods are safe for concurrent use when the
// supplied selector, invokers and middleware are. Create a Client with New;
// its zero value is not usable.
type Client struct {
	scheme     string
	selector   selector.Selector
	middleware []endpoint.Middleware

	// nodes is the current node set; it is replaced, never mutated.
	nodes atomic.Pointer[[]selector.Node]

	// observer and observed are used only by the goroutine applying updates:
	// New for direct targets, the watch goroutine otherwise.
	observer func([]selector.Node)
	observed *[]selector.Node // last set passed to observer; nil before the first call

	watcher   discovery.Watcher // nil for direct targets
	closed    chan struct{}     // closed by Close
	watchDone chan struct{}     // closed when the watch goroutine exits
	closeOnce sync.Once
	closeErr  error
}

// New creates a Client for target, which is either
//
//   - "<scheme>://host:port": a direct target with a single node, whose
//     scheme is the protocol; or
//   - "discovery:///<name>": the instances of service name, which requires
//     WithDiscoverer and WithScheme.
//
// For discovery targets New waits, bounded by ctx, for the first
// result of the Watcher and then keeps the nodes up to date in a background
// goroutine until Close is called. Only endpoints whose scheme matches the
// configured scheme become nodes.
func New(ctx context.Context, target string, opts ...Option) (*Client, error) {
	var o clientOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if o.selector == nil {
		return nil, fmt.Errorf("transport: WithSelector is required")
	}
	u, err := url.Parse(target)
	if err != nil {
		return nil, fmt.Errorf("transport: invalid target %q: %w", target, err)
	}
	c := &Client{
		selector:   o.selector,
		middleware: slices.Clone(o.middleware),
		observer:   o.observer,
		closed:     make(chan struct{}),
	}
	if u.Scheme != discoveryScheme {
		if u.Scheme == "" || u.Host == "" {
			return nil, fmt.Errorf("transport: invalid target %q: want <scheme>://host:port or discovery:///<name>", target)
		}
		c.scheme = u.Scheme
		c.store([]selector.Node{{Addr: u.Host}})
		return c, nil
	}

	name := strings.TrimPrefix(u.Path, "/")
	switch {
	case u.Host != "" || name == "" || strings.Contains(name, "/"):
		return nil, fmt.Errorf("transport: invalid discovery target %q: want discovery:///<name>", target)
	case o.discoverer == nil:
		return nil, fmt.Errorf("transport: discovery target %q requires WithDiscoverer", target)
	case o.scheme == "":
		return nil, fmt.Errorf("transport: discovery target %q requires WithScheme", target)
	}
	c.scheme = o.scheme
	c.nodes.Store(new([]selector.Node{}))

	// ctx bounds Watch setup and the wait for the first snapshot. The Watcher
	// owns its lifetime independently of ctx and is stopped by Close.
	w, err := o.discoverer.Watch(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("transport: watch %q: %w", name, err)
	}
	c.watcher = w
	c.watchDone = make(chan struct{})
	first := make(chan error, 1)
	go c.watch(first)

	select {
	case err := <-first:
		if err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("transport: watch %q: %w", name, err)
		}
		return c, nil
	case <-ctx.Done():
		_ = c.Close()
		return nil, fmt.Errorf("transport: wait for instances of %q: %w", name, context.Cause(ctx))
	}
}

// watch applies watcher snapshots until the Client is closed. The outcome of
// the first Next call is reported on first. Later errors are retried with an
// exponential backoff while the last known nodes stay in use.
func (c *Client) watch(first chan<- error) {
	defer close(c.watchDone)
	initialized := false
	retryDelay := backoff.Exponential(minWatchRetry, maxWatchRetry)
	attempt := 1
	for {
		instances, err := c.watcher.Next()
		if err == nil {
			c.update(instances)
			attempt = 1
			if !initialized {
				initialized = true
				first <- nil
			}
			continue
		}
		if !initialized {
			first <- err
			return
		}
		timer := time.NewTimer(retryDelay(attempt))
		select {
		case <-c.closed:
			timer.Stop()
			return
		case <-timer.C:
		}
		// Stop counting once the delay has saturated, avoiding counter overflow.
		if retryDelay(attempt) < maxWatchRetry {
			attempt++
		}
	}
}

// update replaces the node set with the endpoints of instances that match
// the client scheme, carrying each instance's Weight and Metadata.
func (c *Client) update(instances []*discovery.Instance) {
	nodes := make([]selector.Node, 0, len(instances))
	for _, ins := range instances {
		if ins == nil {
			continue
		}
		for _, ep := range ins.Endpoints {
			u, err := url.Parse(ep)
			if err != nil || u.Scheme != c.scheme || u.Host == "" {
				continue
			}
			nodes = append(nodes, selector.Node{Addr: u.Host, Weight: ins.Weight, Metadata: maps.Clone(ins.Metadata)})
		}
	}
	c.store(nodes)
}

// store publishes nodes and reports them to the observer if they differ from
// the set it saw last. It must only be called by the goroutine applying
// updates.
func (c *Client) store(nodes []selector.Node) {
	c.nodes.Store(&nodes)
	if c.observer == nil {
		return
	}
	if c.observed != nil && slices.EqualFunc(*c.observed, nodes, equalNodes) {
		return
	}
	select {
	case <-c.closed:
		return
	default:
	}
	c.observed = &nodes
	c.observer(cloneNodes(nodes))
}

// equalNodes reports whether a and b describe the same node.
func equalNodes(a, b selector.Node) bool {
	return a.Addr == b.Addr && a.Weight == b.Weight && maps.Equal(a.Metadata, b.Metadata)
}

// cloneNodes returns a deep copy of nodes.
func cloneNodes(nodes []selector.Node) []selector.Node {
	out := make([]selector.Node, len(nodes))
	for i, n := range nodes {
		out[i] = n
		out[i].Metadata = maps.Clone(n.Metadata)
	}
	return out
}

// Endpoint builds an endpoint for operation. A call
//
//  1. writes metadata.Info{Kind: client, Protocol: scheme, Operation:
//     operation, Header: a new MD} into the context;
//  2. runs the client-level middleware followed by mw;
//  3. selects a node, calls inv.Invoke and reports the outcome to the
//     selector's Done.
//
// Node selection is the innermost step, so middleware that calls next more
// than once, such as retry, selects a node for each attempt. When there is
// no node the call fails with selector.ErrNoAvailable.
func (c *Client) Endpoint(operation string, inv transport.Invoker, mw ...endpoint.Middleware) endpoint.Endpoint {
	chain := endpoint.Chain(slices.Concat(c.middleware, mw)...)
	ep := chain(func(ctx context.Context, req any) (any, error) {
		return c.invoke(ctx, inv, req)
	})
	return func(ctx context.Context, req any) (any, error) {
		ctx = metadata.NewContext(ctx, metadata.Info{
			Kind:      metadata.KindClient,
			Protocol:  c.scheme,
			Operation: operation,
			Header:    metadata.MD{},
		})
		return ep(ctx, req)
	}
}

// invoke selects a node and calls inv on it.
func (c *Client) invoke(ctx context.Context, inv transport.Invoker, req any) (any, error) {
	nodes := *c.nodes.Load()
	if len(nodes) == 0 {
		return nil, selector.ErrNoAvailable
	}
	node, done, err := c.selector.Select(ctx, nodes)
	if err != nil {
		return nil, err
	}
	resp, err := inv.Invoke(ctx, node.Addr, req)
	if done != nil {
		done(ctx, err)
	}
	return resp, err
}

// Close stops the Watcher of a discovery target and waits for the
// background goroutine to exit. Endpoints keep using the last known nodes.
// Close is idempotent; repeated calls return the result of the first one.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
		if c.watcher == nil {
			return
		}
		if err := c.watcher.Stop(); err != nil {
			c.closeErr = fmt.Errorf("transport: stop watcher: %w", err)
		}
		<-c.watchDone
	})
	return c.closeErr
}
