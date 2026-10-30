// Package redis implements session.Store using the official Redis Go client.
// Scripts provide version-checked writes and atomic token rotation.
package redis

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/nostosx/servex-contrib/session"
)

// ErrInvalidRecord identifies malformed stored data or oversized record input.
var ErrInvalidRecord = errors.New("session/redis: invalid record")

// Option configures a Store before concurrent use.
type Option func(*options)
type options struct {
	prefix   string
	maxBytes int
}

// WithPrefix selects an application namespace. The default is servex:session:.
func WithPrefix(prefix string) Option { return func(o *options) { o.prefix = prefix } }

// WithMaxRecordBytes bounds serialized records, including their metadata.
// The default 128 KiB accommodates session.Manager's default 64 KiB values.
func WithMaxRecordBytes(size int) Option { return func(o *options) { o.maxBytes = size } }

// Store supports standalone Redis and Sentinel failover clients. ClusterClient
// is intentionally not accepted: rotation requires two keys on one Redis node.
// Store owns neither the client nor background tasks. Redis TTLs use milliseconds.
type Store struct {
	client  *goredis.Client
	options options
}

var _ session.Store = (*Store)(nil)

// New performs no network I/O. Configure the client with finite I/O timeouts and
// ContextTimeoutEnabled. For session mutations, disable automatic transport
// retries (MaxRetries=-1); a lost response can leave a write's outcome uncertain.
func New(client *goredis.Client, opts ...Option) (*Store, error) {
	if client == nil {
		return nil, errors.New("session/redis: client is required")
	}
	o := options{prefix: "servex:session:", maxBytes: 128 << 10}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if o.prefix == "" || len(o.prefix) > 256 || o.maxBytes <= 0 {
		return nil, errors.New("session/redis: invalid namespace or record limit")
	}
	return &Store{client: client, options: o}, nil
}

func (s *Store) key(key string) (string, error) {
	if len(key) != 64 {
		return "", errors.New("session/redis: expected a SHA-256 store key")
	}
	if _, err := hex.DecodeString(key); err != nil {
		return "", errors.New("session/redis: invalid SHA-256 store key")
	}
	return s.options.prefix + key, nil
}

func (s *Store) Load(ctx context.Context, key string) (session.Record, error) {
	if err := context.Cause(ctx); err != nil {
		return session.Record{}, err
	}
	name, err := s.key(key)
	if err != nil {
		return session.Record{}, err
	}
	values, err := s.client.HMGet(ctx, name, "version", "record").Result()
	if err != nil {
		return session.Record{}, operationError(ctx, "load", err)
	}
	if values[0] == nil && values[1] == nil {
		return session.Record{}, session.ErrNotFound
	}
	version, ok := values[0].(string)
	if !ok {
		return session.Record{}, ErrInvalidRecord
	}
	data, ok := values[1].(string)
	if !ok || len(data) > s.options.maxBytes {
		return session.Record{}, ErrInvalidRecord
	}
	var record session.Record
	if json.Unmarshal([]byte(data), &record) != nil || record.Version == 0 || strconv.FormatUint(record.Version, 10) != version || record.ExpiresAt.After(record.Deadline) {
		return session.Record{}, ErrInvalidRecord
	}
	if err := context.Cause(ctx); err != nil {
		return session.Record{}, err
	}
	if !time.Now().Before(record.ExpiresAt) || !time.Now().Before(record.Deadline) {
		return session.Record{}, session.ErrNotFound
	}
	return record, nil
}

func (s *Store) Save(ctx context.Context, key string, expected uint64, record session.Record) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	name, err := s.key(key)
	if err != nil {
		return err
	}
	if expected == math.MaxUint64 {
		return session.ErrConflict
	}
	data, expiry, err := s.encode(record, expected+1)
	if err != nil {
		return err
	}
	result, err := saveScript.Run(ctx, s.client, []string{name}, strconv.FormatUint(expected, 10), strconv.FormatUint(expected+1, 10), data, expiry).Int()
	return mutationResult(ctx, result, err)
}

func (s *Store) Rotate(ctx context.Context, oldKey, newKey string, expected uint64, record session.Record) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	oldName, err := s.key(oldKey)
	if err != nil {
		return err
	}
	newName, err := s.key(newKey)
	if err != nil {
		return err
	}
	if expected == 0 || oldName == newName {
		return session.ErrConflict
	}
	data, expiry, err := s.encode(record, 1)
	if err != nil {
		return err
	}
	result, err := rotateScript.Run(ctx, s.client, []string{oldName, newName}, strconv.FormatUint(expected, 10), data, expiry).Int()
	return mutationResult(ctx, result, err)
}

func (s *Store) Delete(ctx context.Context, key string) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	name, err := s.key(key)
	if err != nil {
		return err
	}
	if err := s.client.Del(ctx, name).Err(); err != nil {
		return operationError(ctx, "delete", err)
	}
	return nil
}

func (s *Store) encode(record session.Record, version uint64) ([]byte, int64, error) {
	// Round down rather than extend the security deadline to the next millisecond.
	record.Deadline = record.Deadline.Truncate(time.Millisecond)
	record.ExpiresAt = record.ExpiresAt.Truncate(time.Millisecond)
	if !time.Now().Before(record.ExpiresAt) || record.ExpiresAt.After(record.Deadline) {
		return nil, 0, session.ErrConflict
	}
	record.Version = version
	data, err := json.Marshal(record)
	if err != nil || len(data) > s.options.maxBytes {
		return nil, 0, ErrInvalidRecord
	}
	return data, record.ExpiresAt.UnixMilli(), nil
}

func mutationResult(ctx context.Context, result int, err error) error {
	if err != nil {
		return operationError(ctx, "mutate", err)
	}
	if result == 0 {
		return session.ErrConflict
	}
	return nil
}

func operationError(ctx context.Context, operation string, err error) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return fmt.Errorf("session/redis: %s: %w", operation, err)
}

// Versions and payloads remain strings. Lua numbers cannot represent all uint64
// versions or arbitrary JSON integers; scripts never decode/re-encode records.
var saveScript = goredis.NewScript(`
local now = redis.call('TIME')
if tonumber(ARGV[4]) <= tonumber(now[1])*1000 + math.floor(tonumber(now[2])/1000) then return 0 end
if ARGV[1] == '0' then
  if redis.call('EXISTS', KEYS[1]) ~= 0 then return 0 end
else
  if redis.call('HGET', KEYS[1], 'version') ~= ARGV[1] then return 0 end
end
redis.call('HSET', KEYS[1], 'version', ARGV[2], 'record', ARGV[3])
redis.call('PEXPIREAT', KEYS[1], ARGV[4])
return 1
`)

var rotateScript = goredis.NewScript(`
local now = redis.call('TIME')
if tonumber(ARGV[3]) <= tonumber(now[1])*1000 + math.floor(tonumber(now[2])/1000) then return 0 end
if redis.call('HGET', KEYS[1], 'version') ~= ARGV[1] then return 0 end
if redis.call('EXISTS', KEYS[2]) ~= 0 then return 0 end
redis.call('HSET', KEYS[2], 'version', '1', 'record', ARGV[2])
redis.call('PEXPIREAT', KEYS[2], ARGV[3])
redis.call('DEL', KEYS[1])
return 1
`)
