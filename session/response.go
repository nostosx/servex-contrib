package session

import (
	"bufio"
	"errors"
	"net"
	"net/http"
)

// ErrPendingChanges rejects hijacking a connection before session cookie changes
// can be delivered. Authenticate upgrades using an already established session.
var ErrPendingChanges = errors.New("session: cannot hijack with pending changes")

type responseWriter struct {
	http.ResponseWriter
	request   *http.Request
	session   *Session
	committed bool
	written   bool
	err       error
}

func (w *responseWriter) ensure() error {
	if w.committed {
		return w.err
	}
	w.committed = true
	w.err = w.session.manager.commit(w.request.Context(), w.ResponseWriter, w.session)
	if w.err != nil {
		w.written = true
		w.Header().Del("Content-Length")
		w.Header().Del("Content-Type")
		w.Header().Del("Content-Encoding")
		w.session.manager.options.onError(w.ResponseWriter, w.request, w.err)
	}
	return w.err
}

func (w *responseWriter) WriteHeader(code int) {
	if w.written {
		return
	}
	if code >= 100 && code < 200 && code != http.StatusSwitchingProtocols {
		w.ResponseWriter.WriteHeader(code)
		return
	}
	if w.ensure() == nil {
		w.written = true
		w.ResponseWriter.WriteHeader(code)
	}
}

func (w *responseWriter) Write(data []byte) (int, error) {
	if err := w.ensure(); err != nil {
		return 0, err
	}
	w.written = true
	return w.ResponseWriter.Write(data)
}

func (w *responseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// FlushError lets ResponseController commit the session before flushing through
// to the original writer. Unsupported capabilities return http.ErrNotSupported.
func (w *responseWriter) FlushError() error {
	if err := w.ensure(); err != nil {
		return err
	}
	err := http.NewResponseController(w.ResponseWriter).Flush()
	if err == nil {
		w.written = true
	}
	return err
}

func (w *responseWriter) Flush() { _ = w.FlushError() }

func (w *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	s := w.session
	s.mu.Lock()
	pending := s.dirty || s.rotate || s.destroyed || s.clearCookie
	s.mu.Unlock()
	if pending {
		return nil, nil, ErrPendingChanges
	}
	if err := w.ensure(); err != nil {
		return nil, nil, err
	}
	connection, reader, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err == nil {
		w.written = true
	}
	return connection, reader, err
}
