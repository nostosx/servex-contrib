// Package password stores passwords as salted Argon2id PHC strings using the
// Go project's x/crypto implementation. It does not manage users or credentials.
package password

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/crypto/argon2"
)

var (
	ErrInvalidHash     = errors.New("password: invalid or unsupported hash")
	ErrPasswordTooLong = errors.New("password: password exceeds 1024 bytes")
)

// Parameters are encoded with every hash. Memory is measured in KiB. Benchmark
// production settings and bound concurrent calls: Argon2 cannot be interrupted.
type Parameters struct {
	Memory      uint32
	Iterations  uint32
	Parallelism uint8
	SaltLength  uint32
	KeyLength   uint32
}

// DefaultParameters uses Argon2id with 64 MiB, three passes and four lanes.
func DefaultParameters() Parameters {
	return Parameters{Memory: 64 * 1024, Iterations: 3, Parallelism: 4, SaltLength: 16, KeyLength: 32}
}

// Hasher is immutable and safe for concurrent use. Verification accepts bounded
// PHC parameters, including older settings; NeedsRehash identifies upgrades.
type Hasher struct{ parameters Parameters }

func New(parameters Parameters) (*Hasher, error) {
	if !valid(parameters) {
		return nil, errors.New("password: invalid Argon2id parameters")
	}
	return &Hasher{parameters: parameters}, nil
}

func valid(p Parameters) bool {
	validCost := p.Parallelism > 0 && p.Parallelism <= 8 && p.Iterations > 0 && p.Iterations <= 10
	validMemory := p.Memory >= 8*uint32(p.Parallelism) && p.Memory <= 256*1024
	validLengths := p.SaltLength >= 16 && p.SaltLength <= 64 && p.KeyLength >= 16 && p.KeyLength <= 64
	return validCost && validMemory && validLengths
}

// Hash generates a fresh salt. Cancellation is checked around the synchronous
// KDF; cancellation cannot reclaim CPU or memory after the KDF has started.
func (h *Hasher) Hash(ctx context.Context, password []byte) (string, error) {
	if err := check(ctx, password); err != nil {
		return "", err
	}
	p := h.parameters
	salt := make([]byte, p.SaltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("password: salt: %w", err)
	}
	key := argon2.IDKey(password, salt, p.Iterations, p.Memory, p.Parallelism, p.KeyLength)
	defer clear(key)
	if err := context.Cause(ctx); err != nil {
		return "", err
	}
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s", argon2.Version, p.Memory,
		p.Iterations, p.Parallelism, base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(key)), nil
}

// Verify returns false for a password mismatch and ErrInvalidHash for malformed
// or excessive costs. Stored hashes are sensitive configuration, not user input.
func (h *Hasher) Verify(ctx context.Context, password []byte, encoded string) (bool, error) {
	if err := check(ctx, password); err != nil {
		return false, err
	}
	p, salt, expected, err := decode(encoded)
	if err != nil {
		return false, err
	}
	actual := argon2.IDKey(password, salt, p.Iterations, p.Memory, p.Parallelism, p.KeyLength)
	defer clear(actual)
	if err := context.Cause(ctx); err != nil {
		return false, err
	}
	return subtle.ConstantTimeCompare(actual, expected) == 1, nil
}

func (h *Hasher) NeedsRehash(encoded string) (bool, error) {
	p, _, _, err := decode(encoded)
	if err != nil {
		return false, err
	}
	return p != h.parameters, nil
}

func check(ctx context.Context, password []byte) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if len(password) > 1024 {
		return ErrPasswordTooLong
	}
	return nil
}

func decode(encoded string) (Parameters, []byte, []byte, error) {
	invalid := func() (Parameters, []byte, []byte, error) { return Parameters{}, nil, nil, ErrInvalidHash }
	if len(encoded) > 512 {
		return invalid()
	}
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 {
		return invalid()
	}
	if parts[0] != "" || parts[1] != "argon2id" || parts[2] != "v=19" {
		return invalid()
	}
	costs := strings.Split(parts[3], ",")
	if len(costs) != 3 {
		return invalid()
	}
	values := [3]uint32{}
	for i, prefix := range []string{"m=", "t=", "p="} {
		text, ok := strings.CutPrefix(costs[i], prefix)
		if !ok {
			return invalid()
		}
		value, err := strconv.ParseUint(text, 10, 32)
		if err != nil {
			return invalid()
		}
		values[i] = uint32(value)
	}
	if values[2] > 8 {
		return invalid()
	}
	salt, err := base64.RawStdEncoding.Strict().DecodeString(parts[4])
	if err != nil {
		return invalid()
	}
	key, err := base64.RawStdEncoding.Strict().DecodeString(parts[5])
	if err != nil {
		return invalid()
	}
	p := Parameters{Memory: values[0], Iterations: values[1], Parallelism: uint8(values[2]),
		SaltLength: uint32(len(salt)), KeyLength: uint32(len(key))}
	if !valid(p) {
		return invalid()
	}
	return p, salt, key, nil
}
