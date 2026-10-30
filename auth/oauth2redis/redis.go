// Package oauth2redis atomically stores and consumes OAuth2 login attempts using
// the official Redis client. Share its connection pool with browser sessions.
package oauth2redis

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nostosx/servex-contrib/auth/oauth2"
)

type Store struct {
	client *redis.Client
	prefix string
}

var _ oauth2.StateStore = (*Store)(nil)

// New borrows a single-node/Sentinel client. Disable automatic transport retries
// (MaxRetries=-1) because a lost GETDEL response has an uncertain consumption result.
func New(client *redis.Client, prefix string) (*Store, error) {
	if client == nil || prefix == "" || len(prefix) > 256 {
		return nil, errors.New("oauth2redis: client and bounded prefix required")
	}
	return &Store{client: client, prefix: prefix}, nil
}

func (s *Store) key(key string) (string, error) {
	if len(key) != 64 {
		return "", oauth2.ErrInvalidState
	}
	if _, err := hex.DecodeString(key); err != nil {
		return "", oauth2.ErrInvalidState
	}
	return s.prefix + key, nil
}

func (s *Store) Save(ctx context.Context, key string, attempt oauth2.Attempt) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	name, err := s.key(key)
	if err != nil {
		return err
	}
	if !time.Now().Before(attempt.ExpiresAt) {
		return oauth2.ErrInvalidState
	}
	data, err := json.Marshal(attempt)
	if err != nil {
		return err
	}
	if len(data) > 4096 {
		return oauth2.ErrInvalidState
	}
	err = s.client.SetArgs(ctx, name, data, redis.SetArgs{Mode: "NX", ExpireAt: attempt.ExpiresAt}).Err()
	if errors.Is(err, redis.Nil) {
		return oauth2.ErrInvalidState
	}
	return err
}

func (s *Store) Take(ctx context.Context, key string) (oauth2.Attempt, error) {
	if err := context.Cause(ctx); err != nil {
		return oauth2.Attempt{}, err
	}
	name, err := s.key(key)
	if err != nil {
		return oauth2.Attempt{}, err
	}
	data, err := s.client.GetDel(ctx, name).Bytes()
	if errors.Is(err, redis.Nil) {
		return oauth2.Attempt{}, oauth2.ErrInvalidState
	}
	if err != nil {
		return oauth2.Attempt{}, err
	}
	var attempt oauth2.Attempt
	if len(data) > 4096 || json.Unmarshal(data, &attempt) != nil {
		return oauth2.Attempt{}, oauth2.ErrInvalidState
	}
	if !time.Now().Before(attempt.ExpiresAt) {
		return oauth2.Attempt{}, oauth2.ErrInvalidState
	}
	return attempt, nil
}
