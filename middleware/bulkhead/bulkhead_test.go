package bulkhead_test

import (
	"context"
	"testing"
	"time"

	"github.com/nostosx/servex-contrib/middleware/bulkhead"
	"github.com/nostosx/servex-contrib/middleware/endpoint"
	"github.com/nostosx/servex/errorx"
)

func TestAdmissionRejectsOverlapAndReleasesAfterPanic(t *testing.T) {
	t.Parallel()
	limiter, err := bulkhead.New(1)
	if err != nil {
		t.Fatal(err)
	}
	entered, leave, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	call := endpoint.Bulkhead(limiter)(func(context.Context, any) (any, error) { close(entered); <-leave; return 7, nil })
	go func() { defer close(done); _, _ = call(t.Context(), nil) }()
	<-entered
	if _, err := call(t.Context(), nil); errorx.CodeOf(err) != errorx.ResourceExhausted {
		t.Fatalf("overlap admitted: %v", err)
	}
	close(leave)
	<-done
	func() {
		defer func() { _ = recover() }()
		_, _ = endpoint.Bulkhead(limiter)(func(context.Context, any) (any, error) { panic("failure") })(t.Context(), nil)
	}()
	if result, err := endpoint.Bulkhead(limiter)(func(context.Context, any) (any, error) { return 9, nil })(t.Context(), nil); result != 9 || err != nil {
		t.Fatal("panic leaked concurrency slot")
	}
}

func TestReleaseIsIdempotentAndKeepsCapacityExact(t *testing.T) {
	t.Parallel()
	// Capacity two, both slots held. Releasing one holder must free exactly one
	// slot: a non-idempotent release also withdraws the other holder's slot and
	// admits a fourth caller. Extra releases run in a goroutine so the defect
	// surfaces as a failed assertion instead of a blocked test.
	limiter, err := bulkhead.New(2)
	if err != nil {
		t.Fatal(err)
	}
	first, err := limiter.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	second, err := limiter.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	first()
	repeated := make(chan struct{})
	go func() { defer close(repeated); first(); first() }()
	select {
	case <-repeated:
	case <-time.After(5 * time.Second):
		t.Fatal("repeated release blocked instead of being ignored")
	}
	third, err := limiter.Acquire(t.Context())
	if err != nil {
		t.Fatalf("the released slot is unavailable: %v", err)
	}
	if _, err := limiter.Acquire(t.Context()); errorx.CodeOf(err) != errorx.ResourceExhausted {
		t.Fatalf("repeated release withdrew another holder's slot: %v", err)
	}
	second()
	third()
	for range 2 {
		release, err := limiter.Acquire(t.Context())
		if err != nil {
			t.Fatalf("released capacity is unavailable: %v", err)
		}
		release()
	}
}
