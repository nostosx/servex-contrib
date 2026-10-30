package profile

import (
	"bufio"
	"context"
	"net"
	"net/http"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
)

// HTTP observes the entire native HTTP request, including routing failures,
// preflight, CSRF and Session persistence. operation is one bounded configured
// label, never a raw URL/user identifier. Use HTTP or Server observability for a
// transport, unless both request and endpoint measurement are explicitly wanted.
func (p *Profile) HTTP(operation string) func(http.Handler) http.Handler {
	if operation == "" {
		operation = "http.request"
	}
	return func(next http.Handler) http.Handler {
		run := endpoint.Chain(p.Server()...)(func(ctx context.Context, input any) (any, error) {
			call := input.(httpCall)
			next.ServeHTTP(call.writer, call.request.WithContext(ctx))
			return nil, statusError(call.writer.status)
		})
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			md := make(metadata.MD, len(r.Header))
			for name, values := range r.Header {
				for _, value := range values {
					md.Add(name, value)
				}
			}
			ctx := metadata.NewContext(r.Context(), metadata.Info{Kind: metadata.KindServer, Protocol: "http", Operation: operation, Header: md})
			writer := &httpWriter{ResponseWriter: w}
			_, err := run(ctx, httpCall{writer: writer, request: r})
			if err != nil && writer.status == 0 {
				http.Error(w, "request failed", http.StatusInternalServerError)
			}
		})
	}
}

type httpCall struct {
	writer  *httpWriter
	request *http.Request
}
type httpWriter struct {
	http.ResponseWriter
	status int
}

func (w *httpWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	if status >= 200 || status == http.StatusSwitchingProtocols {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}
func (w *httpWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(data)
}
func (w *httpWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *httpWriter) FlushError() error {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return http.NewResponseController(w.ResponseWriter).Flush()
}
func (w *httpWriter) Flush() { _ = w.FlushError() }
func (w *httpWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, reader, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err == nil {
		w.status = http.StatusSwitchingProtocols
	}
	return conn, reader, err
}

func statusError(status int) error {
	if status < 400 {
		return nil
	}
	var code errorx.Code
	switch status {
	case http.StatusUnauthorized:
		code = errorx.Unauthenticated
	case http.StatusForbidden:
		code = errorx.PermissionDenied
	case http.StatusNotFound:
		code = errorx.NotFound
	case http.StatusTooManyRequests:
		code = errorx.ResourceExhausted
	default:
		if status >= 500 {
			code = errorx.Internal
		} else {
			code = errorx.InvalidArgument
		}
	}
	return errorx.New(code, "HTTP_STATUS", "HTTP request failed")
}
