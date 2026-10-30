package session

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	// ErrNotFound means a session is absent or expired.
	ErrNotFound = errors.New("session: not found")
	// ErrConflict means a concurrent write, rotation or deletion invalidated a
	// snapshot. Retrying an authenticated mutation requires application judgment.
	ErrConflict = errors.New("session: concurrent change or expiration")
	// ErrCapacity means the store cannot admit another live session.
	ErrCapacity = errors.New("session: store capacity reached")
)

// Record is a server-side snapshot. Deadline is the absolute expiry; ExpiresAt
// is the idle expiry, never later than Deadline. Version is assigned by Store.
// Values are JSON values; stores must copy the map and each byte slice.
type Record struct {
	Values    map[string]json.RawMessage
	Deadline  time.Time
	ExpiresAt time.Time
	Version   uint64
}

// Store owns session persistence. All operations must be concurrency-safe,
// atomic and honor ctx. Keys supplied by Manager are SHA-256 hashes of random
// cookie tokens; raw tokens must never be persisted or logged.
type Store interface {
	// Load returns an isolated snapshot or ErrNotFound for missing/expired keys.
	Load(ctx context.Context, key string) (Record, error)
	// Save with expected=0 inserts only if no live key exists. Otherwise it
	// updates only a live key whose Version equals expected, or ErrConflict.
	// Assign version expected+1; never recreate a deleted or expired snapshot.
	Save(ctx context.Context, key string, expected uint64, record Record) error
	// Rotate atomically replaces a live old key with an unused new key, checking
	// expected Version. Failure must leave the old session unchanged. The new
	// record starts at version 1; missing/expired/conflicting keys return ErrConflict.
	Rotate(ctx context.Context, oldKey, newKey string, expected uint64, record Record) error
	// Delete invalidates a key. Missing keys succeed; later stale Save/Rotate
	// calls must fail rather than resurrecting the session.
	Delete(ctx context.Context, key string) error
}

func cloneRecord(record Record) Record {
	record.Values = cloneValues(record.Values)
	return record
}

func cloneValues(values map[string]json.RawMessage) map[string]json.RawMessage {
	copyValues := make(map[string]json.RawMessage, len(values))
	for key, value := range values {
		copyValues[key] = append(json.RawMessage(nil), value...)
	}
	return copyValues
}

func expired(record Record, now time.Time) bool {
	return !now.Before(record.Deadline) || !now.Before(record.ExpiresAt)
}
