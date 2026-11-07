// Package redis atomically stores and consumes SAML login attempts using
// the official Redis client. Share its connection pool with browser sessions.
package redis

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nostosx/servex-contrib/auth/saml"
)

type Store struct {
	client *redis.Client
	prefix string
}

var _ saml.StateStore = (*Store)(nil)

// New borrows a single-node/Sentinel client. Disable automatic transport retries
// (MaxRetries=-1) because a lost GETDEL response has an uncertain consumption result.
func New(client *redis.Client, prefix string) (*Store, error) {
	if client == nil || prefix == "" || len(prefix) > 256 {
		return nil, errors.New("saml redis: client and bounded prefix required")
	}
	return &Store{client: client, prefix: prefix}, nil
}

func (s *Store) key(key string) (string, error) {
	if len(key) != 64 {
		return "", saml.ErrInvalidResponse
	}
	if _, err := hex.DecodeString(key); err != nil {
		return "", saml.ErrInvalidResponse
	}
	return s.prefix + key, nil
}

func (s *Store) Save(ctx context.Context, key string, attempt saml.Attempt) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	name, err := s.key(key)
	if err != nil {
		return err
	}
	if !time.Now().Before(attempt.ExpiresAt) {
		return saml.ErrInvalidResponse
	}
	data, err := json.Marshal(attempt)
	if err != nil {
		return err
	}
	if len(data) > 4096 {
		return saml.ErrInvalidResponse
	}
	err = s.client.SetArgs(ctx, name, data, redis.SetArgs{Mode: "NX", ExpireAt: attempt.ExpiresAt}).Err()
	if errors.Is(err, redis.Nil) {
		return saml.ErrInvalidResponse
	}
	return err
}

func (s *Store) Take(ctx context.Context, key string) (saml.Attempt, error) {
	if err := context.Cause(ctx); err != nil {
		return saml.Attempt{}, err
	}
	name, err := s.key(key)
	if err != nil {
		return saml.Attempt{}, err
	}
	data, err := s.client.GetDel(ctx, name).Bytes()
	if errors.Is(err, redis.Nil) {
		return saml.Attempt{}, saml.ErrInvalidResponse
	}
	if err != nil {
		return saml.Attempt{}, err
	}
	var attempt saml.Attempt
	if len(data) > 4096 || json.Unmarshal(data, &attempt) != nil {
		return saml.Attempt{}, saml.ErrInvalidResponse
	}
	if !time.Now().Before(attempt.ExpiresAt) {
		return saml.Attempt{}, saml.ErrInvalidResponse
	}
	return attempt, nil
}
