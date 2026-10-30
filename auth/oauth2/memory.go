package oauth2

import (
	"context"
	"errors"
	"net/netip"
	"sync"
	"time"
)

// MemoryStore is bounded and process-local. It uses lazy expiry, has no worker,
// and loses pending logins on restart. Use a shared StateStore across replicas.
type MemoryStore struct {
	mu       sync.Mutex
	capacity int
	states   map[string]Attempt
}

var _ StateStore = (*MemoryStore)(nil)

func NewMemoryStore(capacity int) (*MemoryStore, error) {
	if capacity <= 0 {
		return nil, errors.New("oauth2: positive state capacity required")
	}
	return &MemoryStore{capacity: capacity, states: make(map[string]Attempt)}, nil
}

func (s *MemoryStore) Save(ctx context.Context, key string, attempt Attempt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := context.Cause(ctx); err != nil {
		return err
	}
	now := time.Now()
	if key == "" || !now.Before(attempt.ExpiresAt) {
		return ErrInvalidState
	}
	for key, state := range s.states {
		if !now.Before(state.ExpiresAt) {
			delete(s.states, key)
		}
	}
	if _, exists := s.states[key]; exists {
		return ErrInvalidState
	}
	if len(s.states) >= s.capacity {
		return errors.New("oauth2: pending login capacity reached")
	}
	s.states[key] = attempt
	return nil
}

func (s *MemoryStore) Take(ctx context.Context, key string) (Attempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := context.Cause(ctx); err != nil {
		return Attempt{}, err
	}
	attempt, exists := s.states[key]
	delete(s.states, key)
	if !exists || !time.Now().Before(attempt.ExpiresAt) {
		return Attempt{}, ErrInvalidState
	}
	return attempt, nil
}

func loopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip, err := netip.ParseAddr(host)
	return err == nil && ip.IsLoopback()
}
