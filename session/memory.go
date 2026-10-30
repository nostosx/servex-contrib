package session

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

// MemoryStore is a bounded, process-local Store with lazy expiry. It starts no
// cleanup goroutines. Expired entries are removed on access or capacity pressure;
// restart loses all sessions. Multiple replicas need a shared Store implementation.
type MemoryStore struct {
	mu       sync.Mutex
	records  map[string]Record
	capacity int
}

var _ Store = (*MemoryStore)(nil)

// NewMemoryStore requires a positive maximum number of stored sessions. It does
// not evict live sessions when full; Save returns ErrCapacity instead.
func NewMemoryStore(capacity int) (*MemoryStore, error) {
	if capacity <= 0 {
		return nil, errors.New("session: capacity must be positive")
	}
	return &MemoryStore{records: make(map[string]Record), capacity: capacity}, nil
}

func (s *MemoryStore) Load(ctx context.Context, key string) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := context.Cause(ctx); err != nil {
		return Record{}, err
	}
	record, ok := s.live(key, time.Now())
	if !ok {
		return Record{}, ErrNotFound
	}
	return cloneRecord(record), nil
}

func (s *MemoryStore) Save(ctx context.Context, key string, expected uint64, record Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := context.Cause(ctx); err != nil {
		return err
	}
	now := time.Now()
	previous, exists := s.live(key, now)
	if key == "" || expired(record, now) || record.ExpiresAt.After(record.Deadline) || expected == math.MaxUint64 {
		return ErrConflict
	}
	if (expected == 0 && exists) || (expected != 0 && (!exists || previous.Version != expected)) {
		return ErrConflict
	}
	if !exists && len(s.records) >= s.capacity {
		for storedKey := range s.records {
			s.live(storedKey, now)
		}
		if len(s.records) >= s.capacity {
			return ErrCapacity
		}
	}
	record.Version = expected + 1
	s.records[key] = cloneRecord(record)
	return nil
}

func (s *MemoryStore) Rotate(ctx context.Context, oldKey, newKey string, expected uint64, record Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := context.Cause(ctx); err != nil {
		return err
	}
	now := time.Now()
	previous, exists := s.live(oldKey, now)
	_, collision := s.live(newKey, now)
	if !exists || expected == 0 || previous.Version != expected || newKey == "" || collision || expired(record, now) || record.ExpiresAt.After(record.Deadline) {
		return ErrConflict
	}
	record.Version = 1
	s.records[newKey] = cloneRecord(record)
	delete(s.records, oldKey)
	return nil
}

func (s *MemoryStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := context.Cause(ctx); err != nil {
		return err
	}
	delete(s.records, key)
	return nil
}

// live is called only with mu held.
func (s *MemoryStore) live(key string, now time.Time) (Record, bool) {
	record, ok := s.records[key]
	if ok && expired(record, now) {
		delete(s.records, key)
		return Record{}, false
	}
	return record, ok
}
