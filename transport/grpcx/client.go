package grpcx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"slices"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpcmd "google.golang.org/grpc/metadata"

	"github.com/nostosx/servex/discovery"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/selector"
	"github.com/nostosx/servex/transport"
)

// Compile-time interface check.
var _ grpc.ClientConnInterface = (*conn)(nil)

// scheme is the URL scheme of gRPC endpoints.
const scheme = "grpc"

// errClientClosed is returned by calls made after Client.Close.
var errClientClosed = errorx.New(errorx.FailedPrecondition, "CLIENT_CLOSED", "grpcx: client is closed")

// ClientOption configures a Client.
type ClientOption func(*clientOptions)

// clientOptions holds the configuration collected from ClientOptions.
type clientOptions struct {
	discoverer discovery.Discoverer
	selector   selector.Selector
	middleware []endpoint.Middleware
	dialOpts   []grpc.DialOption
}

// WithDiscoverer sets the Discoverer used to resolve "discovery:///<name>"
// targets. Only instance endpoints with the "grpc" scheme are used.
func WithDiscoverer(d discovery.Discoverer) ClientOption {
	return func(o *clientOptions) { o.discoverer = d }
}

// WithSelector sets the Selector that picks a node for every call. The
// default is selector.Random(); a nil s keeps the default.
func WithSelector(s selector.Selector) ClientOption {
	return func(o *clientOptions) { o.selector = s }
}

// WithClientMiddleware appends client middleware, which wraps every unary
// call made through Conn. Streams do not pass through it.
func WithClientMiddleware(m ...endpoint.Middleware) ClientOption {
	return func(o *clientOptions) { o.middleware = append(o.middleware, m...) }
}

// WithDialOptions appends options used to create the connection to every
// node. Connections use insecure transport credentials unless these options
// set others, for example with grpc.WithTransportCredentials.
func WithDialOptions(o ...grpc.DialOption) ClientOption {
	return func(co *clientOptions) { co.dialOpts = append(co.dialOpts, o...) }
}

// Client calls gRPC services through a transport.Client. It keeps one
// *grpc.ClientConn per node address, created on first use. When a node
// leaves the node set, its connection is closed as soon as the unary calls
// and streams using it have finished. Node set changes are noticed lazily,
// by the next call that selects a node. All methods are safe for concurrent
// use.
type Client struct {
	tc         *transport.Client
	middleware []endpoint.Middleware
	dialOpts   []grpc.DialOption
	cc         *conn

	mu     sync.Mutex
	conns  map[string]*nodeConn
	unary  map[string]endpoint.Endpoint // per method
	closed bool
	// live holds the addresses of the node set last offered to the
	// selector; it is nil until the first selection.
	live map[string]struct{}
	// seen and seenLen identify that node set. The transport.Client
	// replaces its node set on every change and never mutates it, so a
	// different first element or length means a new set.
	seen    *selector.Node
	seenLen int
}

// nodeConn is the pooled connection to one node.
type nodeConn struct {
	cc *grpc.ClientConn
	// refs counts the unary calls in progress and the streams not yet
	// finished on cc.
	refs int
	// stale reports that the node is not in the live node set, so cc is
	// closed when refs drops to zero.
	stale bool
}

// NewClient creates a Client on top of transport.NewClient with the scheme
// fixed to "grpc". target is either "grpc://host:port" or
// "discovery:///<name>", which requires WithDiscoverer; for discovery
// targets NewClient waits, bounded by ctx, for the first set of instances.
func NewClient(ctx context.Context, target string, opts ...ClientOption) (*Client, error) {
	o := clientOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	u, err := url.Parse(target)
	if err != nil {
		return nil, fmt.Errorf("grpcx: invalid target %q: %w", target, err)
	}
	if u.Scheme != scheme && u.Scheme != "discovery" {
		return nil, fmt.Errorf("grpcx: unsupported target scheme in %q: want grpc or discovery", target)
	}
	c := &Client{
		middleware: slices.Clone(o.middleware),
		dialOpts:   slices.Concat([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, o.dialOpts),
		conns:      make(map[string]*nodeConn),
		unary:      make(map[string]endpoint.Endpoint),
	}
	c.cc = &conn{c: c}
	sel := o.selector
	if sel == nil {
		sel = selector.Random()
	}
	topts := []transport.ClientOption{
		transport.WithScheme(scheme),
		transport.WithSelector(liveSelector{inner: sel, c: c}),
	}
	if o.discoverer != nil {
		topts = append(topts, transport.WithDiscoverer(o.discoverer))
	}
	tc, err := transport.NewClient(ctx, target, topts...)
	if err != nil {
		return nil, err
	}
	c.tc = tc
	return c, nil
}

// liveSelector passes every node set offered to the configured Selector to
// the Client first, so that connections to nodes that left the set can be
// closed.
type liveSelector struct {
	inner selector.Selector
	c     *Client
}

// Select implements selector.Selector.
func (s liveSelector) Select(ctx context.Context, nodes []selector.Node) (selector.Node, selector.Done, error) {
	s.c.observe(nodes)
	return s.inner.Select(ctx, nodes)
}

// observe records nodes as the live node set when it differs from the last
// one, and closes the idle connections to nodes outside it. Connections in
// use are closed when their last call or stream finishes.
func (c *Client) observe(nodes []selector.Node) {
	var first *selector.Node
	if len(nodes) > 0 {
		first = &nodes[0]
	}
	c.mu.Lock()
	if c.closed || (c.live != nil && first == c.seen && len(nodes) == c.seenLen) {
		c.mu.Unlock()
		return
	}
	c.seen, c.seenLen = first, len(nodes)
	c.live = make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		c.live[n.Addr] = struct{}{}
	}
	var idle []*grpc.ClientConn
	for addr, nc := range c.conns {
		_, ok := c.live[addr]
		nc.stale = !ok
		if nc.stale && nc.refs == 0 {
			delete(c.conns, addr)
			idle = append(idle, nc.cc)
		}
	}
	c.mu.Unlock()
	for _, cc := range idle {
		// Close only fails for a connection that is already closed.
		_ = cc.Close()
	}
}

// Conn returns a grpc.ClientConnInterface for generated clients, as in
// pb.NewGreeterClient(c.Conn()).
//
// Every unary call runs through transport.Client.Endpoint with the full
// method name as operation: the client middleware sees the request message
// and a context carrying metadata.Info, then a node is selected and the call
// is made on the connection of that node. metadata.Info.Header is sent as
// outgoing gRPC metadata, in addition to metadata already on the context.
// Failed calls return an *errorx.Error.
//
// Streams select a node the same way but skip the client middleware; errors
// returned by the stream are converted like unary errors, except io.EOF.
func (c *Client) Conn() grpc.ClientConnInterface { return c.cc }

// Close stops the transport.Client and closes the connections to all
// nodes. Calls made afterwards fail with a non-retryable
// errorx.FailedPrecondition (reason CLIENT_CLOSED). Close is idempotent.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	conns := c.conns
	c.conns = nil
	c.mu.Unlock()

	errs := []error{c.tc.Close()}
	for addr, nc := range conns {
		if err := nc.cc.Close(); err != nil {
			errs = append(errs, fmt.Errorf("grpcx: close connection to %s: %w", addr, err))
		}
	}
	return errors.Join(errs...)
}

// acquire returns the connection to addr, creating it on first use, and
// counts a use of it. release must be called once the use has finished; it
// is idempotent.
func (c *Client) acquire(addr string) (cc *grpc.ClientConn, release func(), err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, nil, errClientClosed
	}
	nc, ok := c.conns[addr]
	if !ok {
		cc, err := grpc.NewClient("passthrough:///"+addr, c.dialOpts...)
		if err != nil {
			return nil, nil, errorx.New(errorx.Unavailable, "", fmt.Sprintf("grpcx: connect to %s", addr)).WithCause(err)
		}
		nc = &nodeConn{cc: cc}
		c.conns[addr] = nc
	}
	// A concurrent call may already have seen a node set without addr; the
	// connection is then closed after this use.
	if c.live != nil {
		_, ok := c.live[addr]
		nc.stale = !ok
	}
	nc.refs++
	return nc.cc, sync.OnceFunc(func() { c.release(addr, nc) }), nil
}

// release ends a use of nc counted by acquire and closes nc when it was the
// last use of a node outside the live node set.
func (c *Client) release(addr string, nc *nodeConn) {
	c.mu.Lock()
	nc.refs--
	// After Close c.conns is nil and Close has closed nc already.
	evict := nc.refs == 0 && nc.stale && c.conns[addr] == nc
	if evict {
		delete(c.conns, addr)
	}
	c.mu.Unlock()
	if evict {
		// Close only fails for a connection that is already closed.
		_ = nc.cc.Close()
	}
}

// unaryEndpoint returns the cached endpoint for method.
func (c *Client) unaryEndpoint(method string) endpoint.Endpoint {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ep, ok := c.unary[method]; ok {
		return ep
	}
	ep := c.tc.Endpoint(method, transport.InvokerFunc(c.invokeUnary), c.middleware...)
	c.unary[method] = ep
	return ep
}

// unaryCall carries the per-call arguments of a unary call that do not
// travel as the endpoint request.
type unaryCall struct {
	method string
	reply  any
	opts   []grpc.CallOption
}

// unaryCallKey is the context key of the current *unaryCall.
type unaryCallKey struct{}

// invokeUnary is the transport.Invoker of unary calls.
func (c *Client) invokeUnary(ctx context.Context, addr string, req any) (any, error) {
	call, ok := ctx.Value(unaryCallKey{}).(*unaryCall)
	if !ok {
		return nil, errorx.New(errorx.Internal, "", "grpcx: middleware dropped the call context")
	}
	cc, release, err := c.acquire(addr)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := cc.Invoke(outgoing(ctx), call.method, req, call.reply, call.opts...); err != nil {
		return nil, fromStatus(ctx, err)
	}
	return call.reply, nil
}

// outgoing copies the header of the client metadata.Info into the outgoing
// gRPC metadata.
func outgoing(ctx context.Context) context.Context {
	if info, ok := metadata.FromContext(ctx); ok {
		return withOutgoingMD(ctx, info.Header)
	}
	return ctx
}

// conn implements grpc.ClientConnInterface for a Client.
type conn struct{ c *Client }

// Invoke performs a unary call through the client endpoint of method.
func (cn *conn) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	ctx = context.WithValue(ctx, unaryCallKey{}, &unaryCall{method: method, reply: reply, opts: opts})
	_, err := cn.c.unaryEndpoint(method)(ctx, args)
	return err
}

// NewStream selects a node and opens a stream on its connection. The client
// middleware is not applied.
func (cn *conn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	c := cn.c
	ep := c.tc.Endpoint(method, transport.InvokerFunc(func(ctx context.Context, addr string, _ any) (any, error) {
		cc, release, err := c.acquire(addr)
		if err != nil {
			return nil, err
		}
		s, err := cc.NewStream(outgoing(ctx), desc, method, opts...)
		if err != nil {
			release()
			return nil, fromStatus(ctx, err)
		}
		// The stream holds its connection until it finishes, which includes
		// the cancellation of ctx.
		stop := context.AfterFunc(ctx, release)
		return &clientStream{
			ClientStream:  s,
			ctx:           ctx,
			serverStreams: desc.ServerStreams,
			release: func() {
				stop()
				release()
			},
		}, nil
	}))
	s, err := ep(ctx, nil)
	if err != nil {
		return nil, err
	}
	return s.(grpc.ClientStream), nil
}

// clientStream converts the errors of a grpc.ClientStream into
// *errorx.Error values, leaving io.EOF untouched, and releases the node
// connection when the stream finishes.
type clientStream struct {
	grpc.ClientStream
	ctx           context.Context
	serverStreams bool
	release       func() // idempotent
}

// convert maps a stream error.
func (s *clientStream) convert(err error) error {
	if err == nil || errors.Is(err, io.EOF) {
		return err
	}
	return fromStatus(s.ctx, err)
}

// Header returns the header metadata received from the server.
func (s *clientStream) Header() (md grpcmd.MD, err error) {
	md, err = s.ClientStream.Header()
	if err != nil && !errors.Is(err, io.EOF) {
		s.release()
	}
	return md, s.convert(err)
}

// SendMsg sends m on the stream. io.EOF means the server ended the stream,
// whose status RecvMsg then returns; other errors abort the stream.
func (s *clientStream) SendMsg(m any) error {
	err := s.ClientStream.SendMsg(m)
	if err != nil && !errors.Is(err, io.EOF) {
		s.release()
	}
	return s.convert(err)
}

// RecvMsg receives the next message into m. The stream no longer needs its
// connection once RecvMsg fails, or after the single response of a stream
// without server streaming, which is the last message generated
// CloseAndRecv reads.
func (s *clientStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	if err != nil || !s.serverStreams {
		s.release()
	}
	return s.convert(err)
}

// CloseSend closes the send direction of the stream.
func (s *clientStream) CloseSend() error { return s.convert(s.ClientStream.CloseSend()) }
