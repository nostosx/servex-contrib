package httpx

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync/atomic"

	sxclient "github.com/nostosx/servex-contrib/transport/client"

	jsoncodec "github.com/nostosx/servex-contrib/encoding/json"
	"github.com/nostosx/servex-contrib/selector/random"

	"github.com/nostosx/servex/discovery"
	"github.com/nostosx/servex/encoding"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
	"github.com/nostosx/servex/selector"
	"github.com/nostosx/servex/transport"
)

// Limits applied when turning an error response into an *errorx.Error.
const (
	maxErrorBody    = 64 << 10
	maxErrorMessage = 512
)

// ClientOption configures a Client.
type ClientOption func(*clientOptions)

// clientOptions holds the configuration collected from ClientOptions.
type clientOptions struct {
	discoverer discovery.Discoverer
	selector   selector.Selector
	middleware []endpoint.Middleware
	httpClient *http.Client
	codec      encoding.Codec
}

// WithDiscoverer sets the Discoverer used to resolve "discovery:///<name>"
// targets. Only instance endpoints with the "http" scheme are used.
func WithDiscoverer(d discovery.Discoverer) ClientOption {
	return func(o *clientOptions) { o.discoverer = d }
}

// WithSelector sets the Selector that picks a node for every call. The
// default is random.New().
func WithSelector(s selector.Selector) ClientOption {
	return func(o *clientOptions) {
		if s != nil {
			o.selector = s
		}
	}
}

// WithClientMiddleware appends client-level middleware, which wraps every
// endpoint built by the Client outside of its per-endpoint middleware.
func WithClientMiddleware(m ...endpoint.Middleware) ClientOption {
	return func(o *clientOptions) { o.middleware = append(o.middleware, m...) }
}

// WithHTTPClient sets the http.Client used to send requests. The default is
// &http.Client{}; a nil c keeps the default.
func WithHTTPClient(c *http.Client) ClientOption {
	return func(o *clientOptions) {
		if c != nil {
			o.httpClient = c
		}
	}
}

// WithCodec sets the codec used to encode request bodies; it is also
// advertised in Accept and used for replies of the same media type. The
// default is jsoncodec.Codec; a nil c keeps the default.
func WithCodec(c encoding.Codec) ClientOption {
	return func(o *clientOptions) {
		if c != nil {
			o.codec = c
		}
	}
}

// Client calls HTTP endpoints through a sxclient.Client. All methods are
// safe for concurrent use when supplied codecs, selectors and middleware are.
// Create a Client with NewClient; its zero value is not usable.
type Client struct {
	tc       *sxclient.Client
	hc       *http.Client
	codec    encoding.Codec
	scheme   string
	basePath string
}

// NewClient creates a Client on top of sxclient.New. target is either
// "http://host:port" (or "https://host:port"),
// optionally followed by a base path that prefixes every request path, or
// "discovery:///<name>", which requires WithDiscoverer and selects only http
// endpoints. Direct targets retain their http or https scheme. The default
// http.Client has no timeout; callers must bound requests through ctx or
// WithHTTPClient.
func NewClient(ctx context.Context, target string, opts ...ClientOption) (*Client, error) {
	o := clientOptions{httpClient: &http.Client{}, codec: jsoncodec.Codec, selector: random.New()}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	u, err := url.Parse(target)
	if err != nil {
		return nil, fmt.Errorf("httpx: invalid target %q: %w", target, err)
	}
	c := &Client{hc: o.httpClient, codec: o.codec, scheme: "http"}
	switch u.Scheme {
	case "discovery":
	case "http", "https":
		c.scheme = u.Scheme
		c.basePath = strings.TrimSuffix(u.EscapedPath(), "/")
	default:
		return nil, fmt.Errorf("httpx: unsupported target scheme in %q: want http, https or discovery", target)
	}
	topts := []sxclient.Option{
		sxclient.WithScheme("http"),
		sxclient.WithSelector(o.selector),
		sxclient.WithMiddleware(o.middleware...),
	}
	if o.discoverer != nil {
		topts = append(topts, sxclient.WithDiscoverer(o.discoverer))
	}
	tc, err := sxclient.New(ctx, target, topts...)
	if err != nil {
		return nil, err
	}
	c.tc = tc
	return c, nil
}

// Endpoint builds an endpoint that calls method on path, for example
// Endpoint("GET", "/v1/users/{id}", ...). The operation name is
// "METHOD path".
//
// Every {name} (or {name...}) wildcard in path is filled from the request
// field with the same JSON name. For GET, DELETE and HEAD the remaining JSON
// fields become the query string; other methods send the whole request
// encoded with the client codec as the body. The outbound
// metadata.Info.Header is copied into the request header.
//
// A 2xx reply is decoded into the value returned by newReply (an empty body
// leaves it untouched); a nil newReply discards the body and yields a nil
// reply. Other statuses yield a remote *errorx.Error (see errorx.WithRemote),
// decoded from the errorx JSON body when present and otherwise derived from
// the status code; the body's retryable flag, when present, overrides the
// code's default. Retry middleware of this client may retry such an error, but
// a server returning it tells its own caller not to retry unless the handler
// sets WithRetryable again. A request that
// cannot be exchanged with the node yields errorx.Unavailable, unless the
// failure comes from ctx cancellation or a deadline. That error is retryable
// only when the request cannot have reached the node, because the transport
// failed before obtaining a connection, or when the request is replayable by
// the net/http rules: GET, HEAD, OPTIONS and TRACE requests and requests
// carrying an Idempotency-Key or X-Idempotency-Key header. Other requests may
// already have executed, so their error is explicitly not retryable.
func (c *Client) Endpoint(method, path string, newReply func() any, mw ...endpoint.Middleware) endpoint.Endpoint {
	method = strings.ToUpper(method)
	tmpl := parseTemplate(path)
	inv := transport.InvokerFunc(func(ctx context.Context, addr string, req any) (any, error) {
		return c.do(ctx, method, tmpl, addr, req, newReply)
	})
	return c.tc.Endpoint(method+" "+path, inv, mw...)
}

// Call builds an endpoint like Client.Endpoint whose replies are *Reply.
func Call[Reply any](c *Client, method, path string, mw ...endpoint.Middleware) endpoint.Endpoint {
	return c.Endpoint(method, path, func() any { return new(Reply) }, mw...)
}

// Close stops service discovery through the underlying sxclient.Client. It is
// idempotent; existing endpoints remain callable. It does not close idle HTTP
// connections or take ownership of a supplied http.Client.
func (c *Client) Close() error { return c.tc.Close() }

// do performs a single HTTP exchange with the node at addr.
func (c *Client) do(ctx context.Context, method string, tmpl template, addr string, req any, newReply func() any) (any, error) {
	fields, err := jsonFields(req, usesQuery(method) || tmpl.hasParameters())
	if err != nil {
		return nil, err
	}
	rawPath, used, err := tmpl.expand(fields)
	if err != nil {
		return nil, err
	}
	rawPath = c.basePath + rawPath
	path, err := url.PathUnescape(rawPath)
	if err != nil {
		return nil, fmt.Errorf("httpx: invalid path %q: %w", rawPath, err)
	}
	u := &url.URL{Scheme: c.scheme, Host: addr, Path: path, RawPath: rawPath}

	var body io.Reader
	if usesQuery(method) {
		q := make(url.Values)
		for name, v := range fields {
			if !used[name] {
				addQuery(q, name, v)
			}
		}
		u.RawQuery = q.Encode()
	} else if req != nil {
		data, err := c.codec.Marshal(req)
		if err != nil {
			return nil, fmt.Errorf("httpx: encode request: %w", err)
		}
		body = bytes.NewReader(data)
	}

	var progress exchangeProgress
	hreq, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, progress.trace()), method, u.String(), body)
	if err != nil {
		return nil, fmt.Errorf("httpx: build request: %w", err)
	}
	if info, ok := metadata.FromContext(ctx); ok {
		for k, vs := range info.Header.All() {
			for _, v := range vs {
				hreq.Header.Add(k, v)
			}
		}
	}
	if body != nil && hreq.Header.Get("Content-Type") == "" {
		hreq.Header.Set("Content-Type", contentType(c.codec))
	}
	if hreq.Header.Get("Accept") == "" {
		hreq.Header.Set("Accept", contentType(c.codec))
	}

	resp, err := c.hc.Do(hreq)
	if err != nil {
		return nil, transportError(method, u, err, progress.unsent() || replayable(hreq))
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, decodeError(resp)
	}
	if newReply == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, nil
	}
	reply := newReply()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("httpx: read reply: %w", err)
	}
	if len(data) == 0 || reply == nil {
		return reply, nil
	}
	if err := c.replyCodec(resp.Header.Get("Content-Type")).Unmarshal(data, reply); err != nil {
		return nil, fmt.Errorf("httpx: decode reply: %w", err)
	}
	return reply, nil
}

// replyCodec selects the codec for a reply: JSON for JSON media types, the
// client codec otherwise.
func (c *Client) replyCodec(ct string) encoding.Codec {
	if mt, _, err := mime.ParseMediaType(ct); err == nil {
		if name, ok := codecName(mt); ok && name != c.codec.Name() && name == jsoncodec.Codec.Name() {
			return jsoncodec.Codec
		}
	}
	return c.codec
}

// decodeError converts a non-2xx response into a remote *errorx.Error, so a
// server returning it reports PropagatedRetryable to its own caller.
func decodeError(resp *http.Response) error {
	data, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	var body errorBody
	if json.Unmarshal(data, &body) == nil {
		if code, ok := errorx.ParseCode(body.Code); ok && code != errorx.OK {
			e := errorx.New(code, body.Reason, body.Message).WithMetadata(body.Metadata)
			if body.Retryable != nil {
				e = e.WithRetryable(*body.Retryable)
			}
			return e.WithRemote()
		}
	}
	msg := resp.Status
	if text := strings.TrimSpace(string(data)); text != "" {
		if len(text) > maxErrorMessage {
			text = text[:maxErrorMessage]
		}
		msg += ": " + text
	}
	return errorx.New(codeOf(resp.StatusCode), "", msg).WithRemote()
}

// transportError classifies a failure to exchange a request with a node.
// Cancellation and deadline errors keep their context error in the chain,
// so errorx.From reports Canceled or DeadlineExceeded. Every other failure
// (refused connection, reset, TLS or protocol error) becomes an
// errorx.Unavailable whose cause is the original error. It is retryable, so
// that retry middleware can move on to another node, only when retrySafe
// reports that repeating the request cannot execute it twice.
func transportError(method string, u *url.URL, err error, retrySafe bool) error {
	err = fmt.Errorf("httpx: %s %s: %w", method, u.Redacted(), err)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if retrySafe {
		return errorx.New(errorx.Unavailable, "", "node unavailable").WithCause(err)
	}
	return errorx.New(errorx.Unavailable, "", "node unavailable after the request may have been sent").
		WithCause(err).WithRetryable(false)
}

// exchangeProgress records how far the transport got with a request.
type exchangeProgress struct {
	getConn atomic.Bool
	gotConn atomic.Bool
}

// trace returns the client trace that feeds p.
func (p *exchangeProgress) trace() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		GetConn: func(string) { p.getConn.Store(true) },
		GotConn: func(httptrace.GotConnInfo) { p.gotConn.Store(true) },
	}
}

// unsent reports whether the transport started acquiring a connection but
// never obtained one, so no part of the request reached the node. A
// transport that does not report connection events is never treated as
// unsent.
func (p *exchangeProgress) unsent() bool {
	return p.getConn.Load() && !p.gotConn.Load()
}

// replayable reports whether r may be sent again after a failure that
// happened once it may have reached the node, following net/http's rules for
// retrying requests on a broken connection.
func replayable(r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return true
	}
	_, key := r.Header["Idempotency-Key"]
	_, xKey := r.Header["X-Idempotency-Key"]
	return key || xKey
}

// usesQuery reports whether requests of method carry their fields in the
// query string instead of the body.
func usesQuery(method string) bool {
	switch method {
	case http.MethodGet, http.MethodDelete, http.MethodHead:
		return true
	}
	return false
}

// jsonFields returns the top-level members of the JSON encoding of req.
// Requests that do not encode as a JSON object have no fields. When extract is
// false, retain the JSON encoding check and its error precedence without
// decoding an unused field map. Body encoding still goes through the codec:
// custom marshalers may be stateful and custom codecs need not emit JSON.
func jsonFields(req any, extract bool) (map[string]jsontext.Value, error) {
	if req == nil {
		return nil, nil
	}
	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("httpx: encode request fields: %w", err)
	}
	if !extract || jsontext.Value(data).Kind() != '{' {
		return nil, nil
	}
	var fields map[string]jsontext.Value
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, fmt.Errorf("httpx: encode request fields: %w", err)
	}
	return fields, nil
}

// addQuery adds the JSON value v under name: scalars as their text, arrays
// as repeated values and objects as compact JSON. Nulls are skipped.
func addQuery(q url.Values, name string, v jsontext.Value) {
	if v.Kind() == '[' {
		var items []jsontext.Value
		if json.Unmarshal(v, &items) == nil {
			for _, item := range items {
				if s, ok := parameterText(item); ok {
					q.Add(name, s)
				}
			}
			return
		}
	}
	if s, ok := parameterText(v); ok {
		q.Add(name, s)
	}
}

// parameterText renders a JSON value as a query or path value. It reports
// false for null.
func parameterText(v jsontext.Value) (string, bool) {
	switch v.Kind() {
	case 'n':
		return "", false
	case '"':
		var s string
		if json.Unmarshal(v, &s) == nil {
			return s, true
		}
	}
	_ = v.Compact()
	return string(v), true
}

// template is a parsed request path such as "/v1/users/{id}".
type template []segment

func (t template) hasParameters() bool {
	for _, seg := range t {
		if seg.name != "" {
			return true
		}
	}
	return false
}

// segment is a literal path segment or a wildcard.
type segment struct {
	literal string
	name    string // non-empty for wildcards
	rest    bool   // {name...}: the value may contain slashes
}

// parseTemplate splits path into literal and wildcard segments.
func parseTemplate(path string) template {
	var t template
	for seg := range strings.SplitSeq(path, "/") {
		if inner, ok := strings.CutPrefix(seg, "{"); ok {
			if inner, ok := strings.CutSuffix(inner, "}"); ok && inner != "" && inner != "$" {
				name, rest := strings.CutSuffix(inner, "...")
				t = append(t, segment{name: name, rest: rest})
				continue
			}
		}
		t = append(t, segment{literal: seg})
	}
	return t
}

// expand returns the escaped path with every wildcard filled from fields,
// along with the names it consumed.
func (t template) expand(fields map[string]jsontext.Value) (string, map[string]bool, error) {
	parts := make([]string, 0, len(t))
	used := make(map[string]bool)
	for _, seg := range t {
		if seg.name == "" {
			parts = append(parts, seg.literal)
			continue
		}
		v, ok := fields[seg.name]
		if !ok {
			return "", nil, fmt.Errorf("httpx: path parameter %q not found in request", seg.name)
		}
		s, ok := parameterText(v)
		if !ok {
			return "", nil, fmt.Errorf("httpx: path parameter %q is null", seg.name)
		}
		used[seg.name] = true
		if !seg.rest {
			parts = append(parts, url.PathEscape(s))
			continue
		}
		var escaped []string
		for p := range strings.SplitSeq(s, "/") {
			escaped = append(escaped, url.PathEscape(p))
		}
		parts = append(parts, strings.Join(escaped, "/"))
	}
	return strings.Join(parts, "/"), used, nil
}
