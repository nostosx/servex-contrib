//go:build integration

package consul

import (
	"context"
	"errors"
	"maps"
	"os"
	"slices"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/hashicorp/consul/api"

	"github.com/nostosx/servex/discovery"
)

// itClient returns a client for the agent at SERVEX_IT_CONSUL_ADDR.
func itClient(t *testing.T) *api.Client {
	t.Helper()
	addr := os.Getenv("SERVEX_IT_CONSUL_ADDR")
	if addr == "" {
		t.Skip("SERVEX_IT_CONSUL_ADDR is not set")
	}
	client, err := api.NewClient(&api.Config{Address: addr})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestIntegrationDistinctServiceIdentities(t *testing.T) {
	client := itClient(t)
	r := New(client)
	s := newSyncRegistrar(t, r)
	prefix := "it-identity-" + uuid.NewV7().String() + "-"
	instances := []*discovery.Instance{
		{Name: prefix + "foo-bar", ID: "baz"},
		{Name: prefix + "foo", ID: "bar-baz"},
		{Name: prefix + "foo%2Dbar", ID: "baz"},
	}
	for _, ins := range instances {
		if err := s.Register(t.Context(), ins); err != nil {
			t.Fatal(err)
		}
	}
	check := func(ins *discovery.Instance) {
		t.Helper()
		reg, _, err := client.Agent().Service(serviceID(ins), (&api.QueryOptions{}).WithContext(t.Context()))
		if err != nil || reg.Service != ins.Name || !slices.Contains(reg.Tags, tagID+ins.ID) {
			t.Fatalf("service %+v was overwritten: %+v, %v", ins, reg, err)
		}
	}
	for _, ins := range instances {
		check(ins)
	}
	if err := s.Deregister(t.Context(), instances[0]); err != nil {
		t.Fatal(err)
	}
	for _, ins := range instances[1:] {
		check(ins)
	}
}

// syncRegistrar waits after every call until the change is visible in the
// catalog, which a real agent updates asynchronously, and removes all
// instances it registered when the test ends.
type syncRegistrar struct {
	r *Registry

	mu   sync.Mutex
	seen map[string]*discovery.Instance
}

func newSyncRegistrar(t *testing.T, r *Registry) *syncRegistrar {
	s := &syncRegistrar{r: r, seen: make(map[string]*discovery.Instance)}
	t.Cleanup(func() {
		s.mu.Lock()
		seen := slices.Collect(maps.Values(s.seen))
		s.mu.Unlock()
		for _, ins := range seen {
			ctx, cancel := context.WithTimeout(context.Background(), patience)
			err := s.r.Deregister(ctx, ins)
			cancel()
			if err != nil {
				t.Errorf("cleanup: %v", err)
			}
		}
	})
	return s
}

func (s *syncRegistrar) Register(ctx context.Context, ins *discovery.Instance) error {
	if err := s.r.Register(ctx, ins); err != nil {
		return err
	}
	s.mu.Lock()
	s.seen[serviceID(ins)] = clone(ins)
	s.mu.Unlock()
	return s.await(ctx, ins, true)
}

func (s *syncRegistrar) Deregister(ctx context.Context, ins *discovery.Instance) error {
	if err := s.r.Deregister(ctx, ins); err != nil {
		return err
	}
	return s.await(ctx, ins, false)
}

// await polls the catalog until ins is (or is not) a passing instance.
func (s *syncRegistrar) await(ctx context.Context, ins *discovery.Instance, present bool) error {
	ctx, cancel := context.WithTimeout(ctx, patience)
	defer cancel()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		entries, _, err := s.r.client.Health().Service(ins.Name, "", true, (&api.QueryOptions{}).WithContext(ctx))
		if err != nil {
			return err
		}
		found := slices.ContainsFunc(entries, func(e *api.ServiceEntry) bool { return e.Service.ID == serviceID(ins) })
		if found == present {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func TestIntegrationContract(t *testing.T) {
	client := itClient(t)
	runContract(t, func(t *testing.T) contractEnv {
		r := New(client)
		t.Cleanup(func() { stopAll(r) })
		return contractEnv{reg: newSyncRegistrar(t, r), dis: r, rich: true}
	})
}

func TestIntegrationEndToEnd(t *testing.T) {
	r := New(itClient(t))
	t.Cleanup(func() { stopAll(r) })
	runEndToEnd(t, newSyncRegistrar(t, r), r)
}

func TestIntegrationHeartbeat(t *testing.T) {
	r := New(itClient(t), WithTTL(2*time.Second))
	t.Cleanup(func() { stopAll(r) })
	reg := newSyncRegistrar(t, r)
	name := "it-heartbeat-" + uuid.NewV7().String()
	mustRegister(t, reg, inst(name, "1"))
	w, list := mustWatch(t, r, name)
	if !slices.Equal(ids(list), []string{"1"}) {
		t.Fatalf("snapshot = %v, want [1]", ids(list))
	}
	// The real agent turns the TTL check critical without heartbeats.
	ch := nextAsync(w)
	select {
	case res := <-ch:
		t.Fatalf("instance changed while heartbeating: %v, %v", ids(res.list), res.err)
	case <-time.After(5 * time.Second):
	}
	stopAll(r)
	if res := await(t, ch); res.err != nil || len(res.list) != 0 {
		t.Fatalf("Next after heartbeats stopped = %v, %v, want empty", ids(res.list), res.err)
	}
}

// A visibility timeout must fail the registrar rather than silently succeed.
func TestSyncRegistrarAwaitDeadline(t *testing.T) {
	_, client := newFakeConsul(t)
	s := newSyncRegistrar(t, New(client))
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := s.await(ctx, inst("missing", "1"), true); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("await missing instance = %v, want DeadlineExceeded", err)
	}
}
