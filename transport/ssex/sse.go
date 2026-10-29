// Package ssex implements the servex SSE contracts on net/http. It does not
// reconnect automatically or retain events for replay.
package ssex

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/transport/sse"
)

var (
	_ sse.Acceptor = (*Acceptor)(nil)
	_ sse.Sender   = (*sender)(nil)
	_ sse.Client   = (*Client)(nil)
	_ sse.Receiver = (*receiver)(nil)
)

// DefaultMaxEventBytes bounds a received event block, including comments.
const DefaultMaxEventBytes = 1 << 20

// Acceptor starts unbuffered SSE responses. Mount its handler through
// httpx.Server.HandleHTTP; the application owns stream cancellation.
type Acceptor struct{}

// Accept starts a GET event stream, flushing headers immediately. Unsupported
// writers and invalid methods are rejected before committing any response.
func (*Acceptor) Accept(w http.ResponseWriter, r *http.Request) (sse.Sender, error) {
	if err := r.Context().Err(); err != nil {
		return nil, err
	}
	if r.Method != http.MethodGet {
		return nil, errorx.New(errorx.InvalidArgument, "SSE_METHOD", "SSE requires GET")
	}
	if !canFlush(w) {
		return nil, errorx.New(errorx.FailedPrecondition, "SSE_FLUSH", "response writer cannot flush")
	}
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	if err := http.NewResponseController(w).Flush(); err != nil {
		return nil, fmt.Errorf("ssex: start stream: %w", err)
	}
	return &sender{ctx: r.Context(), w: w}, nil
}

func canFlush(w http.ResponseWriter) bool {
	for w != nil {
		if _, ok := w.(interface{ FlushError() error }); ok {
			return true
		}
		if _, ok := w.(http.Flusher); ok {
			return true
		}
		u, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return false
		}
		w = u.Unwrap()
	}
	return false
}

type sender struct {
	ctx context.Context
	w   http.ResponseWriter
}

func (s *sender) Send(event sse.Event) error {
	if !utf8.ValidString(event.ID) || !utf8.ValidString(event.Type) || !utf8.ValidString(event.Data) ||
		strings.ContainsAny(event.ID, "\r\n\x00") || strings.ContainsAny(event.Type, "\r\n") ||
		(event.Retry != nil && (*event.Retry < 0 || *event.Retry%time.Millisecond != 0)) {
		return errorx.New(errorx.InvalidArgument, "SSE_EVENT", "invalid SSE event fields")
	}
	var b strings.Builder
	b.WriteString("id: " + event.ID + "\n")
	if event.Type != "" {
		b.WriteString("event: " + event.Type + "\n")
	}
	if event.Retry != nil {
		b.WriteString("retry: " + strconv.FormatInt(event.Retry.Milliseconds(), 10) + "\n")
	}
	for line := range strings.SplitSeq(normalizeLines(event.Data), "\n") {
		b.WriteString("data: " + line + "\n")
	}
	b.WriteByte('\n')
	return s.write(b.String())
}

func (s *sender) Comment(comment string) error {
	if !utf8.ValidString(comment) {
		return errorx.New(errorx.InvalidArgument, "SSE_COMMENT", "comment must be UTF-8")
	}
	var b strings.Builder
	for line := range strings.SplitSeq(normalizeLines(comment), "\n") {
		b.WriteString(": " + line + "\n")
	}
	b.WriteByte('\n')
	return s.write(b.String())
}

func normalizeLines(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\r\n", "\n"), "\r", "\n")
}

func (s *sender) write(frame string) error {
	if err := s.ctx.Err(); err != nil {
		return err
	}
	if _, err := io.WriteString(s.w, frame); err != nil {
		return fmt.Errorf("ssex: write: %w", err)
	}
	if err := http.NewResponseController(s.w).Flush(); err != nil {
		return fmt.Errorf("ssex: flush: %w", err)
	}
	return nil
}

// Client opens individual streams. HTTP owns its transport and remains owned
// by the caller. A zero Client uses http.DefaultClient and a 1 MiB event limit.
type Client struct {
	HTTP          *http.Client
	MaxEventBytes int
}

// Open requests an event stream with the supplied headers, including any
// Last-Event-ID. It rejects non-200 responses and non-SSE content types.
func (c *Client) Open(ctx context.Context, url string, headers http.Header) (sse.Receiver, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header = headers.Clone()
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	req.Header.Set("Accept", "text/event-stream")
	client := c.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ssex: open: %w", err)
	}
	media, _, parseErr := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if resp.StatusCode != http.StatusOK || parseErr != nil || media != "text/event-stream" {
		_ = resp.Body.Close()
		return nil, errorx.Newf(errorx.FailedPrecondition, "SSE_RESPONSE", "expected SSE response, received HTTP %d", resp.StatusCode)
	}
	limit := c.MaxEventBytes
	if limit <= 0 {
		limit = DefaultMaxEventBytes
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, min(4096, limit)), limit)
	scanner.Split(splitLine)
	return &receiver{body: resp.Body, scanner: scanner, limit: limit, first: true, id: req.Header.Get("Last-Event-ID")}, nil
}

type receiver struct {
	body    io.ReadCloser
	scanner *bufio.Scanner
	limit   int
	first   bool
	id      string
	retry   *time.Duration
	err     error
}

func (r *receiver) Close() error { return r.body.Close() }

func (r *receiver) Receive() (sse.Event, error) {
	if r.err != nil {
		return sse.Event{}, r.err
	}
	var data strings.Builder
	var eventType string
	var hasData bool
	size := 0
	for r.scanner.Scan() {
		line := r.scanner.Text()
		size += len(line) + 1
		if size > r.limit {
			r.err = errorx.New(errorx.ResourceExhausted, "SSE_EVENT_SIZE", "SSE event exceeds byte limit")
			return sse.Event{}, r.err
		}
		if r.first {
			line = strings.TrimPrefix(line, "\ufeff")
			r.first = false
		}
		if line == "" {
			if hasData {
				return sse.Event{ID: r.id, Type: defaultType(eventType), Data: strings.TrimSuffix(data.String(), "\n"), Retry: copyRetry(r.retry)}, nil
			}
			eventType = ""
			size = 0
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "data":
			hasData = true
			data.WriteString(strings.ToValidUTF8(value, "\ufffd"))
			data.WriteByte('\n')
		case "event":
			eventType = strings.ToValidUTF8(value, "\ufffd")
		case "id":
			if !strings.ContainsRune(value, 0) {
				r.id = strings.ToValidUTF8(value, "\ufffd")
			}
		case "retry":
			if value != "" && strings.IndexFunc(value, func(c rune) bool { return c < '0' || c > '9' }) < 0 {
				ms, err := strconv.ParseInt(value, 10, 64)
				if err == nil && ms <= int64((1<<63-1)/time.Millisecond) {
					r.retry = new(time.Duration(ms) * time.Millisecond)
				}
			}
		}
	}
	r.err = r.scanner.Err()
	if errors.Is(r.err, bufio.ErrTooLong) {
		r.err = errorx.New(errorx.ResourceExhausted, "SSE_EVENT_SIZE", "SSE line exceeds byte limit").WithCause(r.err)
	}
	if r.err == nil {
		r.err = io.EOF // Incomplete events at EOF are discarded, as EventSource requires.
	}
	return sse.Event{}, r.err
}

func defaultType(t string) string {
	if t == "" {
		return "message"
	}
	return t
}

func copyRetry(d *time.Duration) *time.Duration {
	if d == nil {
		return nil
	}
	return new(*d)
}

// splitLine accepts LF, CRLF and CR, including a CRLF split across reads.
func splitLine(data []byte, atEOF bool) (int, []byte, error) {
	for i, b := range data {
		if b == '\n' {
			return i + 1, data[:i], nil
		}
		if b == '\r' {
			if i+1 == len(data) && !atEOF {
				return 0, nil, nil
			}
			n := i + 1
			if n < len(data) && data[n] == '\n' {
				n++
			}
			return n, data[:i], nil
		}
	}
	if atEOF && len(data) > 0 {
		return len(data), data, nil
	}
	return 0, nil, nil
}
