package httpx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	jsoncodec "github.com/nostosx/servex-contrib/encoding/json"

	sxlog "github.com/nostosx/servex/logger"

	"github.com/nostosx/servex/encoding"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/health"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/transport"
)

// Compile-time interface checks.
var (
	_ transport.Server = (*Server)(nil)
	_ http.Handler     = (*Server)(nil)
)

// DefaultMaxBodyBytes is the request body limit for Handle and Route when
// WithMaxBodyBytes is not configured. It matches the default maximum message
// size of gRPC servers.
const DefaultMaxBodyBytes = 4 << 20

// maxMultipartMemory is the part of a multipart body kept in memory; the
// rest is stored in temporary files.
const maxMultipartMemory = 32 << 20

// Media types handled by the form binder instead of a codec.
const (
	mediaTypeForm      = "application/x-www-form-urlencoded"
	mediaTypeMultipart = "multipart/form-data"
)

// ServerOption configures a Server.
type ServerOption func(*serverOptions)

// serverOptions holds the configuration collected from ServerOptions.
type serverOptions struct {
	name              string
	addr              string
	middleware        []endpoint.Middleware
	httpMiddleware    []func(http.Handler) http.Handler
	codecs            []encoding.Codec
	health            health.Checker
	logger            sxlog.Logger
	readTimeout       time.Duration
	readHeaderTimeout time.Duration
	writeTimeout      time.Duration
	idleTimeout       time.Duration
	maxHeaderBytes    int
	maxBodyBytes      int64
	protocols         *http.Protocols
}

// WithProtocols configures net/http protocol negotiation, including h2c for
// native gRPC and bidirectional HTTP RPC streams. Nil keeps net/http defaults.
// The value is copied when the option is applied; callers retain ownership.
func WithProtocols(protocols *http.Protocols) ServerOption {
	return func(o *serverOptions) {
		o.protocols = nil
		if protocols != nil {
			copy := *protocols
			o.protocols = &copy
		}
	}
}

// WithName sets the hook name of the server, which identifies it in app
// logs and errors. The default is "http"; give each server of an App its own
// name.
func WithName(name string) ServerOption {
	return func(o *serverOptions) { o.name = name }
}

// WithAddress sets the TCP address the server listens on. The default is
// ":8000"; ":0" picks a free port, which URL reports after Start.
func WithAddress(addr string) ServerOption {
	return func(o *serverOptions) { o.addr = addr }
}

// WithReadTimeout sets net/http's timeout for reading the entire request,
// including the body. The default is zero, meaning no timeout.
func WithReadTimeout(d time.Duration) ServerOption {
	return func(o *serverOptions) { o.readTimeout = d }
}

// WithReadHeaderTimeout sets net/http's timeout for reading request headers.
// The default is zero, which falls back to ReadTimeout.
func WithReadHeaderTimeout(d time.Duration) ServerOption {
	return func(o *serverOptions) { o.readHeaderTimeout = d }
}

// WithWriteTimeout sets net/http's timeout for writing the response. The default
// is zero, meaning no timeout.
func WithWriteTimeout(d time.Duration) ServerOption {
	return func(o *serverOptions) { o.writeTimeout = d }
}

// WithIdleTimeout sets net/http's timeout for waiting for the next request on a
// keep-alive connection. The default is zero, which falls back to ReadTimeout.
func WithIdleTimeout(d time.Duration) ServerOption {
	return func(o *serverOptions) { o.idleTimeout = d }
}

// WithMaxHeaderBytes sets net/http's maximum request header size. The default is
// zero, which uses http.DefaultMaxHeaderBytes.
func WithMaxHeaderBytes(n int) ServerOption {
	return func(o *serverOptions) { o.maxHeaderBytes = n }
}

// WithMaxBodyBytes limits request bodies for Handle and Route, including unread
// body data. The default is DefaultMaxBodyBytes; values less than or equal
// to zero disable the limit. HandleHTTP handlers retain control over their
// request bodies.
func WithMaxBodyBytes(n int64) ServerOption {
	return func(o *serverOptions) { o.maxBodyBytes = n }
}

// WithMiddleware appends server middleware. It wraps every endpoint registered
// with Handle or Route, in the order given; m[0] is the outermost layer.
func WithMiddleware(m ...endpoint.Middleware) ServerOption {
	return func(o *serverOptions) { o.middleware = append(o.middleware, m...) }
}

// WithHTTPMiddleware appends net/http middleware that wraps the whole server
// handler, in the order given; m[0] is the outermost layer. It runs for
// every request the server receives, before routing and before the request
// body is read or bound, so it also observes unmatched routes, binding
// failures, HandleHTTP handlers and health probes. Use it for checks that
// must reject a request before its body is consumed, such as authentication
// or rate limiting, and for net/http instrumentation. Endpoint middleware
// configured with WithMiddleware still runs inside the matched route, after
// binding. Nil middleware is skipped.
func WithHTTPMiddleware(m ...func(http.Handler) http.Handler) ServerOption {
	return func(o *serverOptions) { o.httpMiddleware = append(o.httpMiddleware, m...) }
}

// WithCodecs appends codecs used to decode request bodies and encode responses.
// jsoncodec.Codec is always available; a codec with the same name as an
// earlier one replaces it.
func WithCodecs(c ...encoding.Codec) ServerOption {
	return func(o *serverOptions) { o.codecs = append(o.codecs, c...) }
}

// WithHealth mounts GET /livez, which always answers 200, and GET /readyz,
// which answers 200 when c.Check succeeds and 503 otherwise. An app.App
// satisfies health.Checker through its Check method.
func WithHealth(c health.Checker) ServerOption {
	return func(o *serverOptions) { o.health = c }
}

// WithLogger sets the logger for serve errors, failed readiness checks and
// requests that fail with a 5xx status. Logging is disabled unless a logger is injected.
func WithLogger(l sxlog.Logger) ServerOption {
	return func(o *serverOptions) {
		if l != nil {
			o.logger = l
		}
	}
}

// RouteOption configures a single route.
type RouteOption func(*routeOptions)

// routeOptions holds the configuration collected from RouteOptions.
type routeOptions struct {
	operation string
	encoder   func(http.ResponseWriter, *http.Request, any) error
}

// WithOperation sets the operation name reported in metadata.Info for the
// route. The default is the route pattern.
func WithOperation(name string) RouteOption {
	return func(o *routeOptions) { o.operation = name }
}

// Server is an HTTP server built on net/http. It implements
// transport.Server and http.Handler, and reports fatal serve errors through
// Err. All methods are safe for concurrent use.
type Server struct {
	name              string
	addr              string
	middleware        endpoint.Middleware
	codecs            codecs
	logger            sxlog.Logger
	mux               *http.ServeMux
	handler           http.Handler
	errc              chan error
	readTimeout       time.Duration
	readHeaderTimeout time.Duration
	writeTimeout      time.Duration
	idleTimeout       time.Duration
	maxHeaderBytes    int
	maxBodyBytes      int64
	protocols         *http.Protocols
	// listen opens the listener; tests replace it.
	listen func(ctx context.Context, network, address string) (net.Listener, error)

	mu      sync.Mutex
	srv     *http.Server
	url     *url.URL
	stopped bool
}

// NewServer creates a Server. Routes may be registered before or after
// Start.
func NewServer(opts ...ServerOption) *Server {
	o := serverOptions{name: "http", addr: ":8000", logger: nil, maxBodyBytes: DefaultMaxBodyBytes}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	s := &Server{
		name:              o.name,
		addr:              o.addr,
		middleware:        endpoint.Chain(o.middleware...),
		codecs:            newCodecs(o.codecs),
		logger:            o.logger,
		mux:               http.NewServeMux(),
		errc:              make(chan error, 1),
		readTimeout:       o.readTimeout,
		readHeaderTimeout: o.readHeaderTimeout,
		writeTimeout:      o.writeTimeout,
		idleTimeout:       o.idleTimeout,
		maxHeaderBytes:    o.maxHeaderBytes,
		maxBodyBytes:      o.maxBodyBytes,
		protocols:         o.protocols,
		listen:            new(net.ListenConfig).Listen,
	}
	s.handler = s.mux
	for _, m := range slices.Backward(o.httpMiddleware) {
		if m != nil {
			s.handler = m(s.handler)
		}
	}
	if o.health != nil {
		s.mountHealth(o.health)
	}
	return s
}

// Name returns the name set by WithName, "http" by default.
func (s *Server) Name() string { return s.name }

// Start listens on the configured address synchronously and then serves
// requests in a background goroutine. A Server can be started only once.
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.srv != nil || s.stopped {
		return errors.New("httpx: server already started")
	}
	ln, err := s.listen(ctx, "tcp", s.addr)
	if err != nil {
		return fmt.Errorf("httpx: listen %q: %w", s.addr, err)
	}
	srv := &http.Server{
		Handler:           s,
		ErrorLog:          log.New(errorLogWriter{s.logger}, "", 0),
		ReadTimeout:       s.readTimeout,
		ReadHeaderTimeout: s.readHeaderTimeout,
		WriteTimeout:      s.writeTimeout,
		IdleTimeout:       s.idleTimeout,
		MaxHeaderBytes:    s.maxHeaderBytes,
		Protocols:         s.protocols,
	}
	s.srv = srv
	s.url = &url.URL{Scheme: "http", Host: ln.Addr().String()}
	go s.serve(srv, ln)
	return nil
}

// serve runs srv on ln and reports a fatal error on errc, which is closed
// once serving has ended.
func (s *Server) serve(srv *http.Server, ln net.Listener) {
	defer close(s.errc)
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.log(context.Background(), sxlog.LevelError, "httpx: serve failed", "addr", ln.Addr().String(), "error", err)
		s.errc <- fmt.Errorf("httpx: serve: %w", err)
	}
}

// Stop gracefully shuts the server down, waiting for in-flight requests
// until ctx ends; remaining connections are then closed forcibly. Stopping a
// server that was never started, or stopping it again, does nothing.
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	srv := s.srv
	already := s.stopped
	s.stopped = true
	s.mu.Unlock()
	if srv == nil || already {
		return nil
	}
	if err := srv.Shutdown(ctx); err != nil {
		_ = srv.Close()
		return fmt.Errorf("httpx: shutdown: %w", err)
	}
	return nil
}

// Err returns a channel that receives the error that made serving fail.
// The channel is closed once serving has ended, including after Stop.
func (s *Server) Err() <-chan error { return s.errc }

// URL returns "http://<listen address>" with the actual port, or nil before
// a successful Start. The returned URL is a copy.
func (s *Server) URL() *url.URL {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.url == nil {
		return nil
	}
	return s.url.Clone()
}

// ServeHTTP passes the request through the WithHTTPMiddleware chain and
// dispatches it to the registered routes.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// HandleHTTP registers a plain http.Handler for pattern. Endpoint middleware
// configured with WithMiddleware is not applied; WithHTTPMiddleware is. Like
// http.ServeMux, it panics on invalid or conflicting patterns.
func (s *Server) HandleHTTP(pattern string, h http.Handler) {
	s.mux.Handle(pattern, h)
}

// Handle registers ep for pattern, which uses http.ServeMux syntax such as
// "POST /v1/users/{id}". For every request newReq creates the request
// object, normally a pointer to a struct, which is then bound in this order,
// later sources overriding earlier ones:
//
//  1. the body, decoded with the codec selected by Content-Type; form and
//     multipart bodies are bound field by field, including uploaded files
//     into *multipart.FileHeader and []*multipart.FileHeader fields;
//  2. the query string;
//  3. the path wildcards.
//
// Query, form and path values are matched to fields by JSON name and may be
// strings, integers, floats, bools, encoding.TextUnmarshaler values,
// pointers to them or slices of them. A nil newReq passes a nil request.
//
// The context carries metadata.Info with Kind server, Protocol "http", the
// operation name and the request header. Unless WithResponseEncoder is configured,
// a nil reply yields 204; any other reply is encoded with the codec negotiated
// from Accept, then the request Content-Type, defaulting to JSON. Errors are
// converted with errorx.From and written as a JSON body with the matching status.
// Like http.ServeMux, Handle panics on invalid or conflicting patterns.
func (s *Server) Handle(pattern string, ep endpoint.Endpoint, newReq func() any, opts ...RouteOption) {
	o := routeOptions{operation: pattern}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	rt := &route{
		s:         s,
		operation: o.operation,
		ep:        s.middleware(ep),
		newReq:    newReq,
		pathNames: pathNames(pattern),
		encoder:   o.encoder,
	}
	s.mux.Handle(pattern, rt)
}

// Route registers ep for pattern like Handle, creating each request object
// with new(Req).
func Route[Req any](s *Server, pattern string, ep endpoint.Endpoint, opts ...RouteOption) {
	s.Handle(pattern, ep, func() any { return new(Req) }, opts...)
}

// RouteFunc registers fn for pattern like Route, adapting it with
// endpoint.Typed: fn receives the bound *Req and its Res is the reply.
// Server middleware still sees the request as *Req and the reply as Res.
func RouteFunc[Req, Res any](s *Server, pattern string, fn func(context.Context, *Req) (Res, error), opts ...RouteOption) {
	Route[Req](s, pattern, endpoint.Typed(fn), opts...)
}

// route serves one registered endpoint.
type route struct {
	s         *Server
	operation string
	ep        endpoint.Endpoint
	newReq    func() any
	pathNames []string
	encoder   func(http.ResponseWriter, *http.Request, any) error
}

// ServeHTTP binds the request, calls the endpoint and writes the reply.
func (rt *route) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s := rt.s
	defer func() {
		if r.MultipartForm != nil {
			_ = r.MultipartForm.RemoveAll()
		}
	}()
	if s.maxBodyBytes > 0 {
		if r.ContentLength > s.maxBodyBytes {
			s.writeError(w, r, rt.operation, bodyTooLarge(&http.MaxBytesError{Limit: s.maxBodyBytes}))
			return
		}
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, s.maxBodyBytes)
		}
	}
	var req any
	if rt.newReq != nil {
		req = rt.newReq()
	}
	if err := rt.bind(r, req); err != nil {
		s.writeError(w, r, rt.operation, err)
		return
	}
	if s.maxBodyBytes > 0 && r.Body != nil {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			s.writeError(w, r, rt.operation, invalidBody(fmt.Errorf("httpx: read remaining body: %w", err)))
			return
		}
	}
	ctx := metadata.NewContext(r.Context(), metadata.Info{
		Kind:      metadata.KindServer,
		Protocol:  "http",
		Operation: rt.operation,
		Header:    headerMD(r.Header),
	})
	reply, err := rt.ep(ctx, req)
	if err != nil {
		s.writeError(w, r, rt.operation, err)
		return
	}
	if rt.encoder != nil {
		rt.encodeResponse(w, r.WithContext(ctx), reply)
		return
	}
	if isNil(reply) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	codec := s.codecs.forResponse(r)
	data, err := codec.Marshal(reply)
	if err != nil {
		s.writeError(w, r, rt.operation,
			errorx.New(errorx.Internal, "ENCODE_REPLY", "failed to encode the reply").WithCause(err))
		return
	}
	w.Header().Set("Content-Type", contentType(codec))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// bind fills req from the body, the query string and the path wildcards.
func (rt *route) bind(r *http.Request, req any) error {
	if req == nil {
		return nil
	}
	if err := rt.bindBody(r, req); err != nil {
		return err
	}
	if err := bindValues(req, r.URL.Query()); err != nil {
		return invalidParameter(fmt.Errorf("httpx: bind query: %w", err))
	}
	path := make(map[string][]string, len(rt.pathNames))
	for _, name := range rt.pathNames {
		if v := r.PathValue(name); v != "" {
			path[name] = []string{v}
		}
	}
	if err := bindValues(req, path); err != nil {
		return invalidParameter(fmt.Errorf("httpx: bind path: %w", err))
	}
	return nil
}

// bindBody decodes the request body into req according to Content-Type.
func (rt *route) bindBody(r *http.Request, req any) error {
	var mediaType string
	if ct := r.Header.Get("Content-Type"); ct != "" {
		mt, _, err := mime.ParseMediaType(ct)
		if err != nil {
			return errorx.New(errorx.InvalidArgument, "INVALID_CONTENT_TYPE",
				fmt.Sprintf("invalid content type %q", ct)).WithCause(err)
		}
		mediaType = mt
	}
	switch mediaType {
	case mediaTypeForm:
		if err := r.ParseForm(); err != nil {
			return invalidBody(fmt.Errorf("httpx: parse form: %w", err))
		}
		if err := bindValues(req, r.PostForm); err != nil {
			return invalidParameter(fmt.Errorf("httpx: bind form: %w", err))
		}
		return nil
	case mediaTypeMultipart:
		if err := r.ParseMultipartForm(maxMultipartMemory); err != nil {
			return invalidBody(fmt.Errorf("httpx: parse multipart form: %w", err))
		}
		if err := bindValues(req, r.MultipartForm.Value); err != nil {
			return invalidParameter(fmt.Errorf("httpx: bind form: %w", err))
		}
		return bindFiles(req, r.MultipartForm.File)
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return invalidBody(fmt.Errorf("httpx: read body: %w", err))
	}
	if len(body) == 0 {
		return nil
	}
	codec := rt.s.codecs.json()
	if mediaType != "" {
		c, ok := rt.s.codecs.lookup(mediaType)
		if !ok {
			return errorx.New(errorx.InvalidArgument, "UNSUPPORTED_MEDIA_TYPE",
				fmt.Sprintf("unsupported content type %q", mediaType))
		}
		codec = c
	}
	if err := codec.Unmarshal(body, req); err != nil {
		return invalidBody(fmt.Errorf("httpx: decode %s body: %w", codec.Name(), err))
	}
	return nil
}

// bodyTooLargeCause identifies a body-limit error without changing the
// status mapping for other InvalidArgument errors.
type bodyTooLargeCause struct{ err error }

func (e *bodyTooLargeCause) Error() string { return e.err.Error() }
func (e *bodyTooLargeCause) Unwrap() error { return e.err }

// bodyTooLarge reports a request exceeding the configured body limit.
func bodyTooLarge(err error) error {
	return errorx.New(errorx.InvalidArgument, "BODY_TOO_LARGE", "request body too large").
		WithCause(&bodyTooLargeCause{err: err})
}

// invalidBody reports a malformed request body or an exceeded body limit.
func invalidBody(err error) error {
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		return bodyTooLarge(err)
	}
	return errorx.New(errorx.InvalidArgument, "INVALID_BODY", err.Error()).WithCause(err)
}

// invalidParameter reports a query, form or path value that does not fit
// its field.
func invalidParameter(err error) error {
	return errorx.New(errorx.InvalidArgument, "INVALID_PARAMETER", err.Error()).WithCause(err)
}

// writeError writes err as the fixed JSON error body with the status mapped
// from its code. The retryable field is PropagatedRetryable, so a failure
// already retried by the client that received it from another service is
// not retried again by every caller up the chain.
func (s *Server) writeError(w http.ResponseWriter, r *http.Request, operation string, err error) {
	e := errorx.From(err)
	status := statusOf(e.Code)
	if _, ok := errors.AsType[*bodyTooLargeCause](err); ok {
		status = http.StatusRequestEntityTooLarge
	}
	if status >= http.StatusInternalServerError {
		s.log(r.Context(), sxlog.LevelError, "httpx: request failed",
			"operation", operation, "status", status, "error", err)
	}
	data, merr := jsoncodec.Codec.Marshal(errorBody{
		Code:      e.Code.String(),
		Reason:    e.Reason,
		Message:   e.Message,
		Metadata:  e.Metadata,
		Retryable: new(e.PropagatedRetryable()),
	})
	if merr != nil {
		// errorBody contains only strings and a bool, so this cannot happen
		// in practice.
		http.Error(w, http.StatusText(status), status)
		return
	}
	w.Header().Set("Content-Type", contentType(jsoncodec.Codec))
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

// mountHealth registers the liveness and readiness probes.
func (s *Server) mountHealth(c health.Checker) {
	s.mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) {
		writeText(w, http.StatusOK, "ok\n")
	})
	s.mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := c.Check(r.Context()); err != nil {
			s.log(r.Context(), sxlog.LevelWarn, "httpx: not ready", "error", err)
			writeText(w, http.StatusServiceUnavailable, "not ready\n")
			return
		}
		writeText(w, http.StatusOK, "ok\n")
	})
}

// writeText writes a plain text response.
func writeText(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

// headerMD converts an HTTP header into metadata with lower-case keys.
func headerMD(h http.Header) metadata.MD {
	md := make(metadata.MD, len(h))
	for k, vs := range h {
		for _, v := range vs {
			md.Add(k, v)
		}
	}
	return md
}

// isNil reports whether v is nil or a nil pointer wrapped in an interface.
func isNil(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	return rv.Kind() == reflect.Pointer && rv.IsNil()
}

func (s *Server) log(ctx context.Context, level sxlog.Level, message string, fields ...any) {
	if s.logger != nil {
		s.logger.Log(ctx, level, message, fields...)
	}
}

type errorLogWriter struct{ logger sxlog.Logger }

func (w errorLogWriter) Write(p []byte) (int, error) {
	if w.logger != nil {
		w.logger.Log(context.Background(), sxlog.LevelError, strings.TrimSpace(string(p)))
	}
	return len(p), nil
}
