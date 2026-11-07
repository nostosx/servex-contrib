package saml

import (
	"context"
	"errors"
	"sync"
	"time"
)

// MemoryStore is bounded, lazily expires state and is safe for one process only.
// Replicas and restart persistence require a shared atomic StateStore.
type MemoryStore struct {
	mu       sync.Mutex
	capacity int
	states   map[string]Attempt
}

func NewMemoryStore(capacity int) (*MemoryStore, error) {
	if capacity <= 0 {
		return nil, errors.New("saml: positive state capacity required")
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
	if key == "" || attempt.RequestID == "" || !now.Before(attempt.ExpiresAt) {
		return ErrInvalidResponse
	}
	for key, record := range s.states {
		if !now.Before(record.ExpiresAt) {
			delete(s.states, key)
		}
	}
	if _, exists := s.states[key]; exists {
		return ErrInvalidResponse
	}
	if len(s.states) >= s.capacity {
		return errors.New("saml: pending login capacity reached")
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
		return Attempt{}, ErrInvalidResponse
	}
	return attempt, nil
}
