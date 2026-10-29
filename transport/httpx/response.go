package httpx

import (
	"bufio"
	"maps"
	"net"
	"net/http"

	sxlog "github.com/nostosx/servex/logger"

	"github.com/nostosx/servex/errorx"
)

// WithResponseEncoder sets the successful response encoder for a route. It runs
// after endpoint middleware, including for a nil reply, with request metadata
// in r.Context(). A nil encoder keeps the default codec and nil-reply behavior.
//
// The encoder owns the status, headers and body. Use http.ResponseController
// for optional writer operations such as flushing, deadlines and hijacking.
// The writer also provides Unwrap() http.ResponseWriter. Writes through the
// unwrapped writer bypass commitment tracking and must not be followed by an
// encoder error. The writer and request must not be used after the encoder
// returns.
//
// An error before a final response restores the original response headers and
// produces the fixed Internal/ENCODE_REPLY JSON error. After a final status,
// body write, supported flush or hijack attempt, an error is logged without
// writing another response. Informational statuses other than 101 do not
// commit the final response. Endpoint and binding errors bypass the encoder.
func WithResponseEncoder(fn func(http.ResponseWriter, *http.Request, any) error) RouteOption {
	return func(o *routeOptions) { o.encoder = fn }
}

// encodeResponse invokes the configured custom encoder without buffering its
// output. The caller supplies the request with the endpoint's metadata context.
func (rt *route) encodeResponse(w http.ResponseWriter, r *http.Request, reply any) {
	headers := w.Header().Clone()
	tracked := &responseWriter{ResponseWriter: w}
	if err := rt.encoder(tracked.writer(), r, reply); err != nil {
		if tracked.committed {
			rt.s.log(r.Context(), sxlog.LevelError, "httpx: response encoding failed after commitment",
				"operation", rt.operation, "error", err)
			return
		}
		clear(w.Header())
		maps.Copy(w.Header(), headers)
		rt.s.writeError(w, r, rt.operation,
			errorx.New(errorx.Internal, "ENCODE_REPLY", "failed to encode the reply").WithCause(err))
	}
}

// responseWriter tracks attempts to commit a final response. Optional methods
// are exposed only when the underlying writer or its unwrap chain supports
// them. Other ResponseController operations traverse Unwrap directly.
type responseWriter struct {
	http.ResponseWriter
	committed bool
}

func (w *responseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *responseWriter) WriteHeader(status int) {
	if status >= http.StatusOK || status == http.StatusSwitchingProtocols {
		w.committed = true
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *responseWriter) Write(p []byte) (int, error) {
	// Even an empty write implicitly sends 200. A failed write may already
	// have sent headers, so mark the attempt before calling the writer.
	w.committed = true
	return w.ResponseWriter.Write(p)
}

func (w *responseWriter) flush() error {
	// A flush may send headers before returning a transport error.
	w.committed = true
	return http.NewResponseController(w.ResponseWriter).Flush()
}

func (w *responseWriter) hijack() (net.Conn, *bufio.ReadWriter, error) {
	// The connection can already be taken over when Hijack returns an error.
	// Conservatively suppress fallback writes after a supported attempt.
	w.committed = true
	return http.NewResponseController(w.ResponseWriter).Hijack()
}

func (w *responseWriter) writer() http.ResponseWriter {
	var flush, hijack bool
	for underlying := w.ResponseWriter; underlying != nil; {
		if _, ok := underlying.(interface{ FlushError() error }); ok {
			flush = true
		}
		if _, ok := underlying.(http.Flusher); ok {
			flush = true
		}
		if _, ok := underlying.(http.Hijacker); ok {
			hijack = true
		}
		unwrapper, ok := underlying.(interface{ Unwrap() http.ResponseWriter })
		if !ok || (flush && hijack) {
			break
		}
		underlying = unwrapper.Unwrap()
	}
	switch {
	case flush && hijack:
		return &responseFlushHijacker{responseFlusher{w}}
	case flush:
		return &responseFlusher{w}
	case hijack:
		return &responseHijacker{w}
	default:
		return w
	}
}

type responseFlusher struct{ *responseWriter }

func (w *responseFlusher) FlushError() error { return w.flush() }

type responseHijacker struct{ *responseWriter }

func (w *responseHijacker) Hijack() (net.Conn, *bufio.ReadWriter, error) { return w.hijack() }

type responseFlushHijacker struct{ responseFlusher }

func (w *responseFlushHijacker) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.hijack()
}
