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
	"sync"
	"time"

	jsoncodec "github.com/nostosx/servex-contrib/encoding/json"

	"github.com/nostosx/servex/observability/logger"
	"github.com/nostosx/servex/observability/tracing"

	"github.com/nostosx/servex/encoding"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/health"
	"github.com/nostosx/servex/metadata"
	httptransport "github.com/nostosx/servex/transport/http"
)

var (
	_ httptransport.Server         = (*Server)(nil)
	_ httptransport.Router         = (*Server)(nil)
	_ httptransport.EndpointRouter = (*Server)(nil)
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
	logger            logger.Logger
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
// Error records contain classification fields and available trace identifiers,
// without error messages, metadata or causes. Native net/http diagnostics use a
// fixed event because their text can contain request data and panic stacks.
func WithLogger(l logger.Logger) ServerOption {
	return func(o *serverOptions) {
		if l != nil {
			o.logger = l
		}
	}
}

// Server is an HTTP server built on net/http. It implements
// transport.Server and http.Handler, and reports fatal serve errors through
// Err. All methods are safe for concurrent use.
type Server struct {
	name              string
	addr              string
	middleware        endpoint.Middleware
	codecs            codecs
	logger            logger.Logger
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
	return newServer(o)
}

func newServer(o serverOptions) *Server {
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
		ErrorLog:          log.New(errorLogWriter{s}, "", 0),
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
		s.logError(context.Background(), logger.LevelError, "httpx: serve failed", err, "addr", ln.Addr().String())
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
// WithRequestDecoder replaces this binding pipeline, including when newReq is
// nil. The custom decoder receives transport metadata before endpoint middleware
// runs. Body limits and multipart cleanup still apply.
//
// The context carries metadata.Info with Kind server, Protocol "http", the
// operation name and the request header. Unless WithResponseEncoder is configured,
// a nil reply yields 204; any other reply is encoded with the codec negotiated
// from Accept, then the request Content-Type, defaulting to JSON. Errors are
// converted with errorx.From and written as a JSON body with the matching status.
// Like http.ServeMux, Handle panics on invalid or conflicting patterns.
//
// Route options come from the core transport/http package; register typed
// handlers with its Route and RouteFunc helpers. httptransport.WithOperation
// sets the operation name reported in metadata.Info (default: the pattern).
//
// httptransport.WithRequestDecoder replaces Body, Query, Path and Form binding;
// nil restores default binding. The decoder receives a fresh target and the
// matched request with transport metadata in its context, before endpoint
// middleware runs. It reads the size-limited Body without replacing it, honors
// request cancellation and must not retain the request or target after
// returning. Unread body bytes are checked against the limit before calling the
// endpoint. Multipart files remain available through response encoding and are
// removed when the route returns, including after decoder failure. Decoder
// errors bypass endpoint middleware and the successful response encoder:
// errorx errors and context cancellation/deadlines keep their classification,
// body overflow yields 413/BODY_TOO_LARGE, and other errors become a safe
// InvalidArgument/DECODE_REQUEST response without exposing their text.
//
// httptransport.WithResponseEncoder sets the successful response encoder. It
// runs after endpoint middleware, including for a nil reply, with request
// metadata in r.Context(); nil keeps the default codec and nil-reply behavior.
// The encoder owns the status, headers and body. Use http.ResponseController
// for optional writer operations such as flushing, deadlines and hijacking; the
// writer also provides Unwrap() http.ResponseWriter, whose writes bypass
// commitment tracking and must not be followed by an encoder error. The writer
// and request must not be used after the encoder returns. An error before a
// final response restores the original headers and produces the fixed
// Internal/ENCODE_REPLY JSON error. After a final status, body write, supported
// flush or hijack attempt, an error is logged without writing another response.
// Informational statuses other than 101 do not commit the final response.
func (s *Server) Handle(pattern string, ep endpoint.Endpoint, newReq func() any, opts ...httptransport.RouteOption) {
	o := httptransport.RouteOptions{Operation: pattern}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	rt := &route{
		s:         s,
		operation: o.Operation,
		ep:        s.middleware(ep),
		newReq:    newReq,
		pathNames: pathNames(pattern),
		decoder:   o.DecodeRequest,
		encoder:   o.EncodeResponse,
	}
	s.mux.Handle(pattern, rt)
}

// route serves one registered endpoint.
type route struct {
	s         *Server
	operation string
	ep        endpoint.Endpoint
	newReq    func() any
	pathNames []string
	decoder   func(*http.Request, any) error
	encoder   func(http.ResponseWriter, *http.Request, any) error
	bindings  bindingCache
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
	ctx := metadata.NewContext(r.Context(), metadata.Info{
		Kind:      metadata.KindServer,
		Protocol:  "http",
		Operation: rt.operation,
		Header:    headerMD(r.Header),
	})
	var req any
	if rt.newReq != nil {
		req = rt.newReq()
	}
	if err := rt.decodeRequest(ctx, r, req); err != nil {
		s.writeError(w, r, rt.operation, err)
		return
	}
	if s.maxBodyBytes > 0 && r.Body != nil {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			s.writeError(w, r, rt.operation, invalidBody(fmt.Errorf("httpx: read remaining body: %w", err)))
			return
		}
	}
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
	if err := rt.bindings.bindValues(req, r.URL.Query()); err != nil {
		return invalidParameter(fmt.Errorf("httpx: bind query: %w", err))
	}
	path := make(map[string][]string, len(rt.pathNames))
	for _, name := range rt.pathNames {
		if v := r.PathValue(name); v != "" {
			path[name] = []string{v}
		}
	}
	if err := rt.bindings.bindValues(req, path); err != nil {
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
		if err := rt.bindings.bindValues(req, r.PostForm); err != nil {
			return invalidParameter(fmt.Errorf("httpx: bind form: %w", err))
		}
		return nil
	case mediaTypeMultipart:
		if err := r.ParseMultipartForm(maxMultipartMemory); err != nil {
			return invalidBody(fmt.Errorf("httpx: parse multipart form: %w", err))
		}
		if err := rt.bindings.bindValues(req, r.MultipartForm.Value); err != nil {
			return invalidParameter(fmt.Errorf("httpx: bind form: %w", err))
		}
		return rt.bindings.bindFiles(req, r.MultipartForm.File)
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
		s.logError(r.Context(), logger.LevelError, "httpx: request failed", err,
			"operation", operation, "status", status)
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
			s.logError(r.Context(), logger.LevelWarn, "httpx: not ready", err)
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

// logError records only the shared payload-free error classification. A span
// belongs to the supplied context; endpoint middleware child contexts are not
// implicitly recovered after the endpoint returns.
func (s *Server) logError(ctx context.Context, level logger.Level, message string, err error, fields ...any) {
	if s.logger == nil || !s.logger.Enabled(ctx, level) {
		return
	}
	classification := errorx.Classify(err)
	fields = append(fields,
		"code", classification.Code.String(),
		"retryable", classification.Retryable,
		"remote", classification.Remote,
	)
	if classification.Reason != "" {
		fields = append(fields, "reason", classification.Reason)
	}
	if span := tracing.SpanFromContext(ctx); span != nil {
		traceID, spanID := span.TraceID(), span.SpanID()
		if traceID != "" && spanID != "" {
			fields = append(fields, "trace_id", traceID, "span_id", spanID)
		}
	}
	s.logger.Log(ctx, level, message, fields...)
}

type errorLogWriter struct{ server *Server }

func (w errorLogWriter) Write(p []byte) (int, error) {
	// net/http diagnostics can contain client data, panic values and stacks.
	// They have no typed error or request context, so retain only a fixed event.
	if w.server != nil {
		w.server.logError(context.Background(), logger.LevelError, "httpx: server diagnostic",
			errorx.New(errorx.Unknown, "HTTP_SERVER_DIAGNOSTIC", ""))
	}
	return len(p), nil
}
