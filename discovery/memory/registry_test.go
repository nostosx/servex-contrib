package memory

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/nostosx/servex/discovery"
)

func inst(name, id string, endpoints ...string) *discovery.Instance {
	return &discovery.Instance{ID: id, Name: name, Endpoints: endpoints}
}

func ids(list []*discovery.Instance) []string {
	out := make([]string, 0, len(list))
	for _, ins := range list {
		out = append(out, ins.ID)
	}
	return out
}

// result is the outcome of an asynchronous Next call.
type result struct {
	list []*discovery.Instance
	err  error
}

// nextAsync calls w.Next in a new goroutine and returns its result channel.
func nextAsync(w discovery.Watcher) <-chan result {
	ch := make(chan result, 1)
	go func() {
		list, err := w.Next()
		ch <- result{list, err}
	}()
	return ch
}

// mustWatch starts a watcher and consumes its initial snapshot.
func mustWatch(t *testing.T, r *Registry, name string) (discovery.Watcher, []*discovery.Instance) {
	t.Helper()
	w, err := r.Watch(t.Context(), name)
	if err != nil {
		t.Fatalf("Watch error = %v", err)
	}
	t.Cleanup(func() {
		if err := w.Stop(); err != nil {
			t.Errorf("Stop() = %v", err)
		}
	})
	list, err := w.Next()
	if err != nil {
		t.Fatalf("first Next error = %v", err)
	}
	return w, list
}

func TestValidation(t *testing.T) {
	r := New()
	tests := []struct {
		name string
		ins  *discovery.Instance
	}{
		{"nil", nil},
		{"empty name", inst("", "1")},
		{"empty id", inst("svc", "")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := r.Register(t.Context(), tt.ins); err == nil || !strings.HasPrefix(err.Error(), "memory: ") {
				t.Errorf("Register error = %v, want memory error", err)
			}
			if err := r.Deregister(t.Context(), tt.ins); err == nil || !strings.HasPrefix(err.Error(), "memory: ") {
				t.Errorf("Deregister error = %v, want memory error", err)
			}
		})
	}
}

func TestCanceledContext(t *testing.T) {
	r := New()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := r.Register(ctx, inst("svc", "1")); !errors.Is(err, context.Canceled) {
		t.Errorf("Register error = %v, want context.Canceled", err)
	}
	if err := r.Deregister(ctx, inst("svc", "1")); !errors.Is(err, context.Canceled) {
		t.Errorf("Deregister error = %v, want context.Canceled", err)
	}
	if _, err := r.Watch(ctx, "svc"); !errors.Is(err, context.Canceled) {
		t.Errorf("Watch error = %v, want context.Canceled", err)
	}
}

func TestFirstNextReturnsSnapshot(t *testing.T) {
	r := New()
	_, list := mustWatch(t, r, "svc")
	if len(list) != 0 {
		t.Fatalf("empty service snapshot = %v, want empty", ids(list))
	}

	for _, id := range []string{"c", "a", "b"} {
		if err := r.Register(t.Context(), inst("svc", id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Register(t.Context(), inst("other", "z")); err != nil {
		t.Fatal(err)
	}
	_, list = mustWatch(t, r, "svc")
	if got := ids(list); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("snapshot = %v, want [a b c]", got)
	}
}

func TestNextBlocksUntilChange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := New()
		w, _ := mustWatch(t, r, "svc")

		ch := nextAsync(w)
		synctest.Wait()
		select {
		case res := <-ch:
			t.Fatalf("Next returned %v, %v before any change", ids(res.list), res.err)
		default:
		}

		// Changes to another service or no-op deregistrations do not wake it.
		if err := r.Register(t.Context(), inst("other", "x")); err != nil {
			t.Fatal(err)
		}
		if err := r.Deregister(t.Context(), inst("svc", "unknown")); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		select {
		case res := <-ch:
			t.Fatalf("Next returned %v, %v after an unrelated change", ids(res.list), res.err)
		default:
		}

		if err := r.Register(t.Context(), inst("svc", "1", "http://10.0.0.1:80")); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		select {
		case res := <-ch:
			if res.err != nil {
				t.Fatalf("Next error = %v", res.err)
			}
			if got := ids(res.list); !slices.Equal(got, []string{"1"}) {
				t.Fatalf("Next = %v, want [1]", got)
			}
		default:
			t.Fatal("Next did not return after a change")
		}

		// Deregistration is a change too.
		ch = nextAsync(w)
		if err := r.Deregister(t.Context(), inst("svc", "1")); err != nil {
			t.Fatal(err)
		}
		res := <-ch
		if res.err != nil || len(res.list) != 0 {
			t.Fatalf("Next after Deregister = %v, %v, want empty", ids(res.list), res.err)
		}
		if err := w.Stop(); err != nil {
			t.Errorf("Stop() = %v", err)
		}
	})
}

func TestChangesCoalesce(t *testing.T) {
	r := New()
	w, _ := mustWatch(t, r, "svc")
	for _, id := range []string{"1", "2", "3"} {
		if err := r.Register(t.Context(), inst("svc", id)); err != nil {
			t.Fatal(err)
		}
	}
	list, err := w.Next()
	if err != nil {
		t.Fatal(err)
	}
	if got := ids(list); !slices.Equal(got, []string{"1", "2", "3"}) {
		t.Fatalf("Next = %v, want [1 2 3]", got)
	}
}

func TestRegisterReplaces(t *testing.T) {
	r := New()
	w, _ := mustWatch(t, r, "svc")
	if err := r.Register(t.Context(), inst("svc", "1", "http://a:1")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Next(); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(t.Context(), inst("svc", "1", "http://b:2")); err != nil {
		t.Fatal(err)
	}
	list, err := w.Next()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || !slices.Equal(list[0].Endpoints, []string{"http://b:2"}) {
		t.Fatalf("Next = %+v, want single instance with http://b:2", list)
	}
}

func TestSnapshotsAreCopies(t *testing.T) {
	r := New()
	in := &discovery.Instance{
		ID:        "1",
		Name:      "svc",
		Metadata:  map[string]string{"k": "v"},
		Endpoints: []string{"http://a:1"},
	}
	if err := r.Register(t.Context(), in); err != nil {
		t.Fatal(err)
	}
	// Mutating the registered value must not affect the registry.
	in.Metadata["k"] = "changed"
	in.Endpoints[0] = "changed"

	_, list := mustWatch(t, r, "svc")
	got := list[0]
	if got.Metadata["k"] != "v" || got.Endpoints[0] != "http://a:1" {
		t.Fatalf("registry kept caller's mutations: %+v", got)
	}
	// Mutating a snapshot must not affect later snapshots.
	got.Metadata["k"] = "changed"
	got.Endpoints[0] = "changed"
	_, list = mustWatch(t, r, "svc")
	if list[0].Metadata["k"] != "v" || list[0].Endpoints[0] != "http://a:1" {
		t.Fatalf("snapshot mutation leaked: %+v", list[0])
	}
}

func TestMultipleWatchers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := New()
		var chans []<-chan result
		for range 3 {
			w, _ := mustWatch(t, r, "svc")
			chans = append(chans, nextAsync(w))
		}
		synctest.Wait()
		if err := r.Register(t.Context(), inst("svc", "1")); err != nil {
			t.Fatal(err)
		}
		for i, ch := range chans {
			res := <-ch
			if res.err != nil || !slices.Equal(ids(res.list), []string{"1"}) {
				t.Errorf("watcher %d: Next = %v, %v, want [1]", i, ids(res.list), res.err)
			}
		}
	})
}

// TestStopUnblocksNext stops a watcher while Next is blocked. synctest.Test
// fails if any goroutine started in the bubble is left blocked, so this also
// verifies that no goroutine leaks.
func TestStopUnblocksNext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := New()
		w, err := r.Watch(t.Context(), "svc")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Next(); err != nil {
			t.Fatal(err)
		}
		ch := nextAsync(w)
		synctest.Wait()

		if err := w.Stop(); err != nil {
			t.Fatalf("Stop error = %v", err)
		}
		res := <-ch
		if !errors.Is(res.err, errStopped) {
			t.Fatalf("Next after Stop error = %v, want errStopped", res.err)
		}
		if _, err := w.Next(); !errors.Is(err, errStopped) {
			t.Fatalf("later Next error = %v, want errStopped", err)
		}
		if err := w.Stop(); err != nil {
			t.Fatalf("second Stop error = %v", err)
		}
		// Changes after Stop do not revive the watcher.
		if err := r.Register(t.Context(), inst("svc", "1")); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Next(); !errors.Is(err, errStopped) {
			t.Fatalf("Next after change error = %v, want errStopped", err)
		}
	})
}

func TestStopBeforeFirstNext(t *testing.T) {
	r := New()
	w, err := r.Watch(t.Context(), "svc")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Stop(); err != nil {
		t.Errorf("Stop() = %v", err)
	}
	if _, err := w.Next(); !errors.Is(err, errStopped) {
		t.Fatalf("Next error = %v, want errStopped", err)
	}
}

func TestConcurrentUse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := New()
		const n = 50
		w, _ := mustWatch(t, r, "svc")
		defer func() {
			if err := w.Stop(); err != nil {
				t.Errorf("cleanup Stop() = %v", err)
			}
		}()

		seen := make(chan error, 1)
		var readers sync.WaitGroup
		readers.Go(func() {
			for {
				list, err := w.Next()
				if err != nil {
					seen <- err
					return
				}
				if len(list) == n {
					seen <- nil
					return
				}
			}
		})

		var writers sync.WaitGroup
		for i := range n {
			writers.Go(func() {
				if err := r.Register(t.Context(), inst("svc", fmt.Sprint(i))); err != nil {
					t.Error(err)
				}
				// Watchers on other services and churn must not interfere.
				other, err := r.Watch(t.Context(), "other")
				if err != nil {
					t.Error(err)
					return
				}
				if _, err := other.Next(); err != nil {
					t.Errorf("Next() = %v", err)
				}
				if err := other.Stop(); err != nil {
					t.Errorf("Stop() = %v", err)
				}
				if err := r.Register(t.Context(), inst("other", fmt.Sprint(i))); err != nil {
					t.Error(err)
				}
				if err := r.Deregister(t.Context(), inst("other", fmt.Sprint(i))); err != nil {
					t.Error(err)
				}
			})
		}
		writers.Wait()
		if err := <-seen; err != nil {
			t.Fatalf("Next() before seeing all %d instances: %v", n, err)
		}
		if err := w.Stop(); err != nil {
			t.Errorf("Stop() = %v", err)
		}
		readers.Wait()
	})
}
