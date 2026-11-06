package httpx

import (
	"context"
	"net/http"
	"slices"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/metadata"
)

// EndpointFilter adapts an explicitly supplied endpoint chain to native HTTP
// middleware. It surrounds the whole request, including routing, preflight,
// CSRF and session persistence. No observation, recovery or retry is installed.
// Middleware must not replay the request or depend on a bound business payload.
// The first middleware is outermost; nil entries are ignored. An empty chain
// returns the original handler unchanged.
//
// operation is one stable request-scope name, never a raw URL or user ID. Empty
// uses "http.request". Native response errors are classified for outer middleware;
// an error before response commitment produces a fixed public HTTP error.
// ResponseController operations remain available through writer unwrapping.
func EndpointFilter(operation string, middleware ...endpoint.Middleware) func(http.Handler) http.Handler {
	owned := slices.DeleteFunc(slices.Clone(middleware), func(m endpoint.Middleware) bool { return m == nil })
	if len(owned) == 0 {
		return func(next http.Handler) http.Handler { return next }
	}
	if operation == "" {
		operation = "http.request"
	}
	chain := endpoint.Chain(owned...)
	return func(next http.Handler) http.Handler {
		run := chain(func(ctx context.Context, input any) (any, error) {
			call := input.(filterCall)
			next.ServeHTTP(call.writer.writer(), call.request.WithContext(ctx))
			return nil, filterStatusError(call.status.status)
		})
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			md := make(metadata.MD, len(r.Header))
			for name, values := range r.Header {
				for _, value := range values {
					md.Add(name, value)
				}
			}
			ctx := metadata.NewContext(r.Context(), metadata.Info{Kind: metadata.KindServer, Protocol: "http", Operation: operation, Header: md})
			status := &filterWriter{ResponseWriter: w}
			writer := &responseWriter{ResponseWriter: status}
			_, err := run(ctx, filterCall{writer: writer, status: status, request: r})
			if err != nil && !writer.committed {
				code := statusOf(errorx.Classify(err).Code)
				http.Error(w, http.StatusText(code), code)
			}
		})
	}
}

type filterCall struct {
	writer  *responseWriter
	status  *filterWriter
	request *http.Request
}

type filterWriter struct {
	http.ResponseWriter
	status int
}

func (w *filterWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *filterWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	if status >= http.StatusOK || status == http.StatusSwitchingProtocols {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *filterWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(data)
}

func filterStatusError(status int) error {
	if status < http.StatusBadRequest {
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
		if status >= http.StatusInternalServerError {
			code = errorx.Internal
		} else {
			code = errorx.InvalidArgument
		}
	}
	return errorx.New(code, "HTTP_STATUS", "HTTP request failed")
}
