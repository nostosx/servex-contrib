package password

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestHashVerifyAndUpgrade(t *testing.T) {
	p := Parameters{Memory: 64, Iterations: 1, Parallelism: 1, SaltLength: 16, KeyLength: 32}
	h, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	one, err := h.Hash(t.Context(), []byte("correct horse"))
	if err != nil {
		t.Fatal(err)
	}
	two, err := h.Hash(t.Context(), []byte("correct horse"))
	if err != nil || one == two {
		t.Fatalf("fresh salt: %v", err)
	}
	for _, test := range []struct {
		password string
		want     bool
	}{{"correct horse", true}, {"wrong", false}, {"", false}} {
		ok, err := h.Verify(t.Context(), []byte(test.password), one)
		if err != nil || ok != test.want {
			t.Fatalf("verify=%v err=%v", ok, err)
		}
	}
	if rehash, err := h.NeedsRehash(one); err != nil || rehash {
		t.Fatalf("rehash=%v err=%v", rehash, err)
	}
	p.Iterations++
	newHasher, _ := New(p)
	if rehash, err := newHasher.NeedsRehash(one); err != nil || !rehash {
		t.Fatalf("upgrade=%v err=%v", rehash, err)
	}
	if _, err := New(DefaultParameters()); err != nil {
		t.Fatal(err)
	}
}

func TestRejectMalformedAndExcessiveCosts(t *testing.T) {
	h, _ := New(Parameters{Memory: 64, Iterations: 1, Parallelism: 1, SaltLength: 16, KeyLength: 32})
	valid, _ := h.Hash(t.Context(), []byte("x"))
	for _, encoded := range []string{"", strings.Repeat("x", 513), strings.Replace(valid, "v=19", "v=16", 1), strings.Replace(valid, "m=64", "m=999999999", 1), strings.Replace(valid, "p=1", "p=256", 1), valid + "$extra"} {
		if _, err := h.Verify(t.Context(), nil, encoded); !errors.Is(err, ErrInvalidHash) {
			t.Fatalf("invalid accepted: %v", err)
		}
	}
	ctx, cancel := context.WithCancelCause(t.Context())
	cause := errors.New("stopped")
	cancel(cause)
	if _, err := h.Hash(ctx, nil); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	if _, err := h.Verify(ctx, nil, valid); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	if _, err := h.Hash(t.Context(), make([]byte, 1025)); !errors.Is(err, ErrPasswordTooLong) {
		t.Fatal(err)
	}
	if _, err := New(Parameters{}); err == nil {
		t.Fatal("invalid parameters accepted")
	}
}
