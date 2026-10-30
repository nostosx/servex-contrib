// Package session implements server-side browser sessions using only the standard
// library. Cookies carry opaque random tokens; application values remain in Store.
package session

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
)

var (
	// ErrCommitted means the response has already committed its session snapshot.
	ErrCommitted = errors.New("session: response already committed")
	// ErrClosed means the request has completed or the session was destroyed.
	ErrClosed = errors.New("session: closed or destroyed")
	// ErrDataTooLarge means JSON session values exceed the manager's limit.
	ErrDataTooLarge = errors.New("session: data limit exceeded")
)

type contextKey struct{}

// FromContext returns the request's session installed by Manager.Middleware.
func FromContext(ctx context.Context) (*Session, bool) {
	if ctx == nil {
		return nil, false
	}
	s, ok := ctx.Value(contextKey{}).(*Session)
	return s, ok
}

// Session is a request-owned snapshot. Its methods are concurrency-safe, but
// callers must join work before returning or writing/flushing a response.
// Mutations after response commitment fail. Do not retain it beyond the request.
type Session struct {
	mu          sync.Mutex
	manager     *Manager
	record      Record
	key         string
	clearCookie bool
	dirty       bool
	rotate      bool
	destroyed   bool
	committed   bool
	closed      bool
}

// Get decodes a value into destination. Missing values return false, nil.
// Mutable destinations never alias stored values. JSON type errors are returned.
func (s *Session) Get(key string, destination any) (bool, error) {
	s.mu.Lock()
	if s.closed || s.destroyed {
		s.mu.Unlock()
		return false, ErrClosed
	}
	value, ok := s.record.Values[key]
	value = append(json.RawMessage(nil), value...)
	s.mu.Unlock()
	if !ok {
		return false, nil
	}
	return true, json.Unmarshal(value, destination)
}

// Put stores a JSON-serializable value. JSON encoding and size validation happen
// before changing the snapshot. Values should be small identifiers and settings.
func (s *Session) Put(key string, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mutable(); err != nil {
		return err
	}
	if key == "" {
		return errors.New("session: empty value key")
	}
	candidate := cloneValues(s.record.Values)
	candidate[key] = encoded
	data, err := json.Marshal(candidate)
	if err != nil {
		return err
	}
	if len(data) > s.manager.options.maxDataBytes {
		return ErrDataTooLarge
	}
	s.record.Values = candidate
	s.dirty = true
	return nil
}

// Remove deletes a value if present.
func (s *Session) Remove(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mutable(); err != nil {
		return err
	}
	if _, ok := s.record.Values[key]; ok {
		delete(s.record.Values, key)
		s.dirty = true
	}
	return nil
}

// RenewToken requests atomic token rotation while preserving values and the
// absolute deadline. Call after login or privilege changes, before writing.
func (s *Session) RenewToken() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mutable(); err != nil {
		return err
	}
	s.rotate = true
	return nil
}

// Destroy requests server-side invalidation and cookie deletion before the first
// response write. Later mutations fail. In-flight requests may finish, but stale
// snapshots cannot recreate the destroyed key when Store follows its contract.
func (s *Session) Destroy() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mutable(); err != nil {
		return err
	}
	s.destroyed = true
	s.record.Values = nil
	return nil
}

func (s *Session) mutable() error {
	if s.closed || s.destroyed {
		return ErrClosed
	}
	if s.committed {
		return ErrCommitted
	}
	return nil
}
