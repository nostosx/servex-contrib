package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nostosx/servex/discovery"
)

// Timeouts used by the contract tests. quiet is how long Next must stay
// blocked to conclude that it does not return; patience bounds waits for
// changes that must arrive.
const (
	quiet    = 300 * time.Millisecond
	patience = 15 * time.Second
)

// contractEnv is a registry under test.
type contractEnv struct {
	reg discovery.Registrar
	dis discovery.Discoverer
	// rich reports whether Version and Metadata round-trip.
	rich bool
}

// inst returns an instance with one endpoint whose host is derived from
// name and id, so that distinct instances never share an address.
func inst(name, id string) *discovery.Instance {
	h := fnv.New32a()
	h.Write([]byte(name + "/" + id))
	s := h.Sum32()
	ep := fmt.Sprintf("http://10.%d.%d.%d:8080", byte(s>>16), byte(s>>8), byte(s)|1)
	return &discovery.Instance{ID: id, Name: name, Endpoints: []string{ep}}
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

// await waits for a result on ch.
func await(t *testing.T, ch <-chan result) result {
	t.Helper()
	select {
	case res := <-ch:
		return res
	case <-time.After(patience):
		t.Fatal("Next did not return in time")
		return result{}
	}
}

// assertBlocked checks that ch yields nothing for a while.
func assertBlocked(t *testing.T, ch <-chan result, why string) {
	t.Helper()
	select {
	case res := <-ch:
		t.Fatalf("Next returned %v, %v %s", ids(res.list), res.err, why)
	case <-time.After(quiet):
	}
}

// waitFor calls Next until ok accepts the snapshot and returns it.
func waitFor(t *testing.T, w discovery.Watcher, ok func([]*discovery.Instance) bool) []*discovery.Instance {
	t.Helper()
	deadline := time.After(patience)
	for {
		select {
		case res := <-nextAsync(w):
			if res.err != nil {
				t.Fatalf("Next error = %v", res.err)
			}
			if ok(res.list) {
				return res.list
			}
		case <-deadline:
			t.Fatal("expected snapshot did not arrive in time")
		}
	}
}

// hasIDs returns a predicate matching snapshots with exactly want.
func hasIDs(want ...string) func([]*discovery.Instance) bool {
	return func(list []*discovery.Instance) bool { return slices.Equal(ids(list), want) }
}

// mustWatch starts a watcher and consumes its initial snapshot.
func mustWatch(t *testing.T, d discovery.Discoverer, name string) (discovery.Watcher, []*discovery.Instance) {
	t.Helper()
	w, err := d.Watch(t.Context(), name)
	if err != nil {
		t.Fatalf("Watch error = %v", err)
	}
	// Stop is idempotent and returns the result of the first call, which
	// tests that care about it check themselves; some expect it to fail.
	t.Cleanup(func() { _ = w.Stop() })
	res := await(t, nextAsync(w))
	if res.err != nil {
		t.Fatalf("first Next error = %v", res.err)
	}
	return w, res.list
}

func mustRegister(t *testing.T, r discovery.Registrar, ins *discovery.Instance) {
	t.Helper()
	if err := r.Register(t.Context(), ins); err != nil {
		t.Fatalf("Register(%s/%s) error = %v", ins.Name, ins.ID, err)
	}
}

func mustDeregister(t *testing.T, r discovery.Registrar, ins *discovery.Instance) {
	t.Helper()
	if err := r.Deregister(t.Context(), ins); err != nil {
		t.Fatalf("Deregister(%s/%s) error = %v", ins.Name, ins.ID, err)
	}
}

// runContract checks the discovery.Discoverer contract, mirroring the
// behaviour of discovery/memory. newEnv returns a fresh, empty registry.
func runContract(t *testing.T, newEnv func(t *testing.T) contractEnv) {
	t.Run("CanceledContext", func(t *testing.T) {
		env := newEnv(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := env.reg.Register(ctx, inst("svc", "1")); !errors.Is(err, context.Canceled) {
			t.Errorf("Register error = %v, want context.Canceled", err)
		}
		if err := env.reg.Deregister(ctx, inst("svc", "1")); !errors.Is(err, context.Canceled) {
			t.Errorf("Deregister error = %v, want context.Canceled", err)
		}
		if _, err := env.dis.Watch(ctx, "svc"); !errors.Is(err, context.Canceled) {
			t.Errorf("Watch error = %v, want context.Canceled", err)
		}
	})

	t.Run("FirstNextReturnsSnapshot", func(t *testing.T) {
		env := newEnv(t)
		_, list := mustWatch(t, env.dis, "svc")
		if len(list) != 0 {
			t.Fatalf("empty service snapshot = %v, want empty", ids(list))
		}
		for _, id := range []string{"c", "a", "b"} {
			mustRegister(t, env.reg, inst("svc", id))
		}
		mustRegister(t, env.reg, inst("other", "z"))
		_, list = mustWatch(t, env.dis, "svc")
		if got := ids(list); !slices.Equal(got, []string{"a", "b", "c"}) {
			t.Fatalf("snapshot = %v, want [a b c]", got)
		}
		for _, ins := range list {
			if ins.Name != "svc" || !slices.Equal(ins.Endpoints, inst("svc", ins.ID).Endpoints) {
				t.Errorf("instance = %+v, want endpoints %v", ins, inst("svc", ins.ID).Endpoints)
			}
		}
	})

	t.Run("NextBlocksUntilChange", func(t *testing.T) {
		env := newEnv(t)
		w, _ := mustWatch(t, env.dis, "svc")
		ch := nextAsync(w)
		assertBlocked(t, ch, "before any change")

		// Changes to another service or no-op deregistrations do not wake it.
		mustRegister(t, env.reg, inst("other", "x"))
		mustDeregister(t, env.reg, inst("svc", "unknown"))
		assertBlocked(t, ch, "after an unrelated change")

		mustRegister(t, env.reg, inst("svc", "1"))
		res := await(t, ch)
		if res.err != nil || !slices.Equal(ids(res.list), []string{"1"}) {
			t.Fatalf("Next = %v, %v, want [1]", ids(res.list), res.err)
		}

		// Deregistration is a change too.
		ch = nextAsync(w)
		mustDeregister(t, env.reg, inst("svc", "1"))
		res = await(t, ch)
		if res.err != nil || len(res.list) != 0 {
			t.Fatalf("Next after Deregister = %v, %v, want empty", ids(res.list), res.err)
		}
	})

	t.Run("RegisterReplaces", func(t *testing.T) {
		env := newEnv(t)
		w, _ := mustWatch(t, env.dis, "svc")
		first := inst("svc", "1")
		mustRegister(t, env.reg, first)
		waitFor(t, w, hasIDs("1"))
		second := inst("svc", "1")
		second.Endpoints = []string{"http://10.9.9.9:9090"}
		mustRegister(t, env.reg, second)
		waitFor(t, w, func(list []*discovery.Instance) bool {
			return len(list) == 1 && slices.Equal(list[0].Endpoints, second.Endpoints)
		})
	})

	t.Run("SnapshotsAreCopies", func(t *testing.T) {
		env := newEnv(t)
		in := inst("svc", "1")
		if env.rich {
			in.Version = "v1"
			in.Metadata = map[string]string{"k": "v"}
		}
		want := in.Endpoints[0]
		mustRegister(t, env.reg, in)
		// Mutating the registered value must not affect the registry.
		in.Endpoints[0] = "changed"
		if env.rich {
			in.Metadata["k"] = "changed"
		}

		_, list := mustWatch(t, env.dis, "svc")
		if len(list) != 1 {
			t.Fatalf("snapshot = %v, want [1]", ids(list))
		}
		got := list[0]
		if got.Endpoints[0] != want || env.rich && (got.Version != "v1" || got.Metadata["k"] != "v") {
			t.Fatalf("snapshot = %+v", got)
		}
		// Mutating a snapshot must not affect later snapshots.
		got.Endpoints[0] = "changed"
		if got.Metadata != nil {
			got.Metadata["k"] = "changed"
		}
		w, list := mustWatch(t, env.dis, "svc")
		if list[0].Endpoints[0] != want || env.rich && list[0].Metadata["k"] != "v" {
			t.Fatalf("snapshot mutation leaked: %+v", list[0])
		}
		// The same holds for snapshots delivered by later Next calls.
		mustRegister(t, env.reg, inst("svc", "2"))
		list = waitFor(t, w, hasIDs("1", "2"))
		list[0].Endpoints[0] = "changed"
		_, list = mustWatch(t, env.dis, "svc")
		if list[0].Endpoints[0] != want {
			t.Fatalf("snapshot mutation leaked: %+v", list[0])
		}
	})

	t.Run("MultipleWatchers", func(t *testing.T) {
		env := newEnv(t)
		var chans []<-chan result
		for range 3 {
			w, _ := mustWatch(t, env.dis, "svc")
			chans = append(chans, nextAsync(w))
		}
		mustRegister(t, env.reg, inst("svc", "1"))
		for i, ch := range chans {
			res := await(t, ch)
			if res.err != nil || !slices.Equal(ids(res.list), []string{"1"}) {
				t.Errorf("watcher %d: Next = %v, %v, want [1]", i, ids(res.list), res.err)
			}
		}
	})

	t.Run("StopUnblocksNext", func(t *testing.T) {
		env := newEnv(t)
		w, err := env.dis.Watch(t.Context(), "svc")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = w.Stop() })
		if res := await(t, nextAsync(w)); res.err != nil {
			t.Fatal(res.err)
		}
		ch := nextAsync(w)
		assertBlocked(t, ch, "before Stop")
		if err := w.Stop(); err != nil {
			t.Fatalf("Stop error = %v", err)
		}
		if res := await(t, ch); !errors.Is(res.err, errStopped) {
			t.Fatalf("Next after Stop error = %v, want errStopped", res.err)
		}
		// Stop waits for the background goroutine of the watcher.
		select {
		case <-w.(*watcher).done:
		default:
			t.Fatal("background goroutine still running after Stop")
		}
		if err := w.Stop(); err != nil {
			t.Fatalf("second Stop error = %v", err)
		}
		// Changes after Stop do not revive the watcher.
		mustRegister(t, env.reg, inst("svc", "1"))
		if _, err := w.Next(); !errors.Is(err, errStopped) {
			t.Fatalf("Next after change error = %v, want errStopped", err)
		}
	})

	t.Run("StopBeforeFirstNext", func(t *testing.T) {
		env := newEnv(t)
		w, err := env.dis.Watch(t.Context(), "svc")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = w.Stop() })
		if err := w.Stop(); err != nil {
			t.Fatalf("Stop error = %v", err)
		}
		if _, err := w.Next(); !errors.Is(err, errStopped) {
			t.Fatalf("Next error = %v, want errStopped", err)
		}
	})

	t.Run("ContextBoundsOnlyWatch", func(t *testing.T) {
		env := newEnv(t)
		ctx, cancel := context.WithCancel(t.Context())
		w, err := env.dis.Watch(ctx, "svc")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := w.Stop(); err != nil {
				t.Errorf("Stop error = %v", err)
			}
		}()
		cancel()
		if res := await(t, nextAsync(w)); res.err != nil {
			t.Fatalf("first Next error = %v", res.err)
		}
		mustRegister(t, env.reg, inst("svc", "1"))
		waitFor(t, w, hasIDs("1"))
	})

	t.Run("ConcurrentUse", func(t *testing.T) {
		env := newEnv(t)
		const n = 20
		w, _ := mustWatch(t, env.dis, "svc")
		var writers sync.WaitGroup
		for i := range n {
			writers.Go(func() {
				if err := env.reg.Register(t.Context(), inst("svc", fmt.Sprint(i))); err != nil {
					t.Error(err)
				}
				// Watchers on other services and churn must not interfere.
				other, err := env.dis.Watch(t.Context(), "other")
				if err != nil {
					t.Error(err)
					return
				}
				if _, err := other.Next(); err != nil {
					t.Errorf("Next error = %v", err)
				}
				if err := other.Stop(); err != nil {
					t.Errorf("Stop error = %v", err)
				}
				o := inst("other", fmt.Sprint(i))
				if err := env.reg.Register(t.Context(), o); err != nil {
					t.Error(err)
				}
				if err := env.reg.Deregister(t.Context(), o); err != nil {
					t.Error(err)
				}
			})
		}
		writers.Wait()
		waitFor(t, w, func(list []*discovery.Instance) bool { return len(list) == n })
	})
}
