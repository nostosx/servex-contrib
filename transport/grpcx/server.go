package grpcx

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/health"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/transport"
)

// Compile-time interface checks.
var (
	_ transport.Server                = (*Server)(nil)
	_ grpc.ServiceRegistrar           = (*Server)(nil)
	_ interface{ Err() <-chan error } = (*Server)(nil)
)

// defaultHealthInterval is how often a health Watch stream re-evaluates the
// checker.
const defaultHealthInterval = 5 * time.Second

// ServerOption configures a Server.
type ServerOption func(*serverOptions)

// serverOptions holds the configuration collected from ServerOptions.
type serverOptions struct {
	name             string
	addr             string
	middleware       []endpoint.Middleware
	streamMiddleware []endpoint.Middleware
	grpcOpts         []grpc.ServerOption
	health           health.Checker
	healthInterval   time.Duration
	listener         net.Listener
}

// WithName sets the hook name of the server, which identifies it in app logs
// and errors. The default is "grpc"; give each gRPC server of an App its own
// name. An empty name keeps the default.
func WithName(name string) ServerOption {
	return func(o *serverOptions) {
		if name != "" {
			o.name = name
		}
	}
}

// WithAddress sets the TCP address the server listens on. The default is
// ":9000"; ":0" picks a free port, which URL reports after Start.
func WithAddress(addr string) ServerOption {
	return func(o *serverOptions) { o.addr = addr }
}

// WithMiddleware appends server middleware. It wraps every unary call, in the
// order given; m[0] is the outermost layer. Streaming calls do not pass
// through it. The unary endpoint chain is composed once during NewServer;
// each request supplies its own handler and metadata through the context.
func WithMiddleware(m ...endpoint.Middleware) ServerOption {
	return func(o *serverOptions) { o.middleware = append(o.middleware, m...) }
}

// WithStreamMiddleware appends middleware run once around each complete
// streaming RPC, with a *StreamInfo as its request. It is separate from the
// unary middleware: auth, recovery, logging, metrics and tracing can be reused,
// but retry and request validation must not be applied to a stream.
// Health Watch streams bypass this chain, like unary health probes.
func WithStreamMiddleware(m ...endpoint.Middleware) ServerOption {
	return func(o *serverOptions) { o.streamMiddleware = append(o.streamMiddleware, m...) }
}

// WithGRPCOptions appends options for the underlying grpc.Server, such as
// credentials or keepalive parameters. Interceptors given here run inside
// the servex interceptors, except a grpc.UnaryInterceptor or
// grpc.StreamInterceptor, which grpc always places outermost.
func WithGRPCOptions(o ...grpc.ServerOption) ServerOption {
	return func(so *serverOptions) { so.grpcOpts = append(so.grpcOpts, o...) }
}

// WithHealth registers the grpc.health.v1.Health service. The status of the
// whole server (the empty service name) and of every registered service
// follows c.Check: SERVING when it returns nil, NOT_SERVING otherwise.
// Unknown service names yield NotFound from Check and SERVICE_UNKNOWN from
// Watch. An app.App satisfies health.Checker through its Check method.
func WithHealth(c health.Checker) ServerOption {
	return func(o *serverOptions) { o.health = c }
}

// Server is a gRPC server. It implements transport.Server and
// grpc.ServiceRegistrar, so generated code registers services directly with
// pb.RegisterXxxServer(srv, impl), and reports fatal serve errors through
// Err. All methods are safe for concurrent use.
//
// Every unary call runs the server middleware with a context carrying
// metadata.Info{Kind: server, Protocol: "grpc", Operation: full method,
// Header: incoming metadata}. Streaming calls run the separately configured
// stream middleware; health probes only get the Info. Errors are converted into gRPC
// statuses as described in the package documentation.
type Server struct {
	name             string
	addr             string
	unaryEndpoint    endpoint.Endpoint
	streamMiddleware endpoint.Middleware
	srv              *grpc.Server
	errc             chan error
	stopping         chan struct{} // closed when Stop begins
	listener         net.Listener  // preset listener, used instead of addr when non-nil
	// listen opens the listener.
	listen func(ctx context.Context, network, address string) (net.Listener, error)

	mu      sync.Mutex
	started bool
	stopped bool
	url     *url.URL
}

// NewServer creates a Server. Services must be registered before Start.
func NewServer(opts ...ServerOption) *Server {
	o := serverOptions{name: "grpc", addr: ":9000", healthInterval: defaultHealthInterval}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	s := &Server{
		name:             o.name,
		addr:             o.addr,
		streamMiddleware: endpoint.Chain(o.streamMiddleware...),
		errc:             make(chan error, 1),
		stopping:         make(chan struct{}),
		listener:         o.listener,
		listen:           new(net.ListenConfig).Listen,
	}
	if len(o.middleware) != 0 {
		s.unaryEndpoint = endpoint.Chain(o.middleware...)(callHandler)
	}
	gopts := slices.Concat([]grpc.ServerOption{
		grpc.ChainUnaryInterceptor(s.unaryInterceptor),
		grpc.ChainStreamInterceptor(s.streamInterceptor),
	}, o.grpcOpts)
	s.srv = grpc.NewServer(gopts...)
	if o.health != nil {
		grpc_health_v1.RegisterHealthServer(s.srv, &healthServer{s: s, checker: o.health, interval: o.healthInterval})
	}
	return s
}

// Name returns the name set with WithName, "grpc" by default.
func (s *Server) Name() string { return s.name }

// RegisterService registers a service and its implementation. It is called
// by generated code and must be invoked before Start; like grpc.Server it
// panics on duplicate services or registration after Start.
func (s *Server) RegisterService(desc *grpc.ServiceDesc, impl any) {
	s.srv.RegisterService(desc, impl)
}

// GetServiceInfo returns the registered services keyed by full service
// name. It lets the Server be used with grpc reflection.
func (s *Server) GetServiceInfo() map[string]grpc.ServiceInfo {
	return s.srv.GetServiceInfo()
}

// Start listens on the configured address synchronously and then serves in
// a background goroutine. A Server can be started only once.
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started || s.stopped {
		return errors.New("grpcx: server already started")
	}
	ln := s.listener
	if ln == nil {
		var err error
		ln, err = s.listen(ctx, "tcp", s.addr)
		if err != nil {
			return fmt.Errorf("grpcx: listen %q: %w", s.addr, err)
		}
	}
	s.started = true
	s.url = &url.URL{Scheme: "grpc", Host: ln.Addr().String()}
	go s.serve(ln)
	return nil
}

// serve runs the grpc.Server on ln and reports a fatal error on errc, which
// is closed once serving has ended.
func (s *Server) serve(ln net.Listener) {
	defer close(s.errc)
	if err := s.srv.Serve(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		s.errc <- fmt.Errorf("grpcx: serve: %w", err)
	}
}

// Stop gracefully stops the server: it stops accepting connections, ends
// health Watch streams and waits for pending calls until ctx ends, after
// which remaining calls are cancelled. Stopping a server that was never
// started, or stopping it again, does nothing.
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	started := s.started
	already := s.stopped
	s.stopped = true
	s.mu.Unlock()
	if already {
		return nil
	}
	close(s.stopping)
	if !started {
		s.srv.Stop()
		return nil
	}
	done := make(chan struct{})
	go func() {
		s.srv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.srv.Stop()
		<-done
		return fmt.Errorf("grpcx: graceful stop: %w", context.Cause(ctx))
	}
}

// Err returns a channel that receives the error that made serving fail.
// The channel is closed once serving has ended, including after Stop.
func (s *Server) Err() <-chan error { return s.errc }

// URL returns "grpc://<listen address>" with the actual port, or nil before
// a successful Start. The returned URL is a copy.
func (s *Server) URL() *url.URL {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.url == nil {
		return nil
	}
	return s.url.Clone()
}

// handlerKey is the context key of the grpc handler of the current unary
// call.
type handlerKey struct{}

// unaryInterceptor runs the server middleware around a unary call and
// converts its error into a gRPC status.
func (s *Server) unaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	ctx = metadata.NewContext(ctx, metadata.Info{
		Kind:      metadata.KindServer,
		Protocol:  "grpc",
		Operation: info.FullMethod,
		Header:    incomingMD(ctx),
	})
	if _, ok := info.Server.(*healthServer); ok {
		// Health probes bypass the middleware, like the HTTP probes of httpx.
		resp, err := handler(ctx, req)
		return resp, statusError(err)
	}
	ctx = context.WithValue(ctx, handlerKey{}, handler)
	var resp any
	var err error
	if s.unaryEndpoint == nil {
		resp, err = callHandler(ctx, req)
	} else {
		resp, err = s.unaryEndpoint(ctx, req)
	}
	if err != nil {
		return nil, statusError(err)
	}
	return resp, nil
}

// callHandler is the innermost endpoint of the server chain: it calls the
// grpc handler stored in ctx by unaryInterceptor.
func callHandler(ctx context.Context, req any) (any, error) {
	h, ok := ctx.Value(handlerKey{}).(grpc.UnaryHandler)
	if !ok {
		return nil, errors.New("grpcx: middleware dropped the call context")
	}
	return h(ctx, req)
}

// streamInterceptor runs the streaming chain for the complete RPC and
// converts its result into a gRPC status.
func (s *Server) streamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := metadata.NewContext(ss.Context(), metadata.Info{
		Kind:      metadata.KindServer,
		Protocol:  "grpc",
		Operation: info.FullMethod,
		Header:    incomingMD(ss.Context()),
	})
	if _, ok := srv.(*healthServer); ok {
		return statusError(handler(srv, &serverStream{ServerStream: ss, ctx: ctx}))
	}
	var called atomic.Bool
	streamInfo := &StreamInfo{ClientStreams: info.IsClientStream, ServerStreams: info.IsServerStream}
	_, err := s.streamMiddleware(func(ctx context.Context, _ any) (any, error) {
		if !called.CompareAndSwap(false, true) {
			return nil, errStreamReplayed
		}
		return nil, handler(srv, &serverStream{ServerStream: ss, ctx: ctx, info: streamInfo})
	})(ctx, streamInfo)
	return statusError(err)
}

// serverStream overrides the context of a grpc.ServerStream.
type serverStream struct {
	grpc.ServerStream
	ctx  context.Context
	info *StreamInfo
}

// Context returns the stream context carrying metadata.Info.
func (s *serverStream) Context() context.Context { return s.ctx }

// SendMsg records a message only after gRPC accepts it.
func (s *serverStream) SendMsg(message any) error {
	err := s.ServerStream.SendMsg(message)
	if err == nil && s.info != nil {
		s.info.sent.Add(1)
	}
	return err
}

// RecvMsg records each message received by the handler.
func (s *serverStream) RecvMsg(message any) error {
	err := s.ServerStream.RecvMsg(message)
	if err == nil && s.info != nil {
		s.info.received.Add(1)
	}
	return err
}
