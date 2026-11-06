package loader

import (
	"errors"
	"testing"
	"time"
)

func TestLoaderRequiresExplicitCodec(t *testing.T) {
	loader, err := New[int](t.Context(), newFakeCache(), time.Second)
	if loader != nil || !errors.Is(err, ErrCodecRequired) {
		t.Fatalf("NewLoader=%v,%v", loader, err)
	}
}
