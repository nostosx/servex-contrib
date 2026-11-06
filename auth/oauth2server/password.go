package oauth2server

import (
	"context"
	"errors"
)

// Passwords is satisfied by the contrib password adapter. It lets applications
// select the same bounded password policy without coupling contrib modules.
type Passwords interface {
	Hash(context.Context, []byte) (string, error)
	Verify(context.Context, []byte, string) (bool, error)
}

type PasswordHasher struct {
	passwords Passwords
	slots     chan struct{}
}

func NewPasswordHasher(passwords Passwords, parallelism int) (*PasswordHasher, error) {
	if passwords == nil || parallelism < 1 {
		return nil, errors.New("oauth2server: password adapter and positive concurrency required")
	}
	return &PasswordHasher{passwords: passwords, slots: make(chan struct{}, parallelism)}, nil
}
func (h *PasswordHasher) acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case h.slots <- struct{}{}:
		return nil
	default:
		return errors.New("oauth2server: client authentication busy")
	}
}
func (h *PasswordHasher) Hash(ctx context.Context, data []byte) ([]byte, error) {
	if err := h.acquire(ctx); err != nil {
		return nil, err
	}
	defer func() { <-h.slots }()
	hash, err := h.passwords.Hash(ctx, data)
	return []byte(hash), err
}
func (h *PasswordHasher) Compare(ctx context.Context, hash, data []byte) error {
	// Prevent callers from using the KDF as an unbounded input/memory sink.
	if len(data) > 256 || len(hash) > 1024 {
		return errors.New("oauth2server: invalid client credential")
	}
	if err := h.acquire(ctx); err != nil {
		return err
	}
	defer func() { <-h.slots }()
	valid, err := h.passwords.Verify(ctx, data, string(hash))
	if err != nil {
		return err
	}
	if !valid {
		return errors.New("oauth2server: invalid client credential")
	}
	return nil
}
