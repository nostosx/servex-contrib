package consul

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"

	"github.com/nostosx/servex/discovery"
	"github.com/nostosx/servex/errorx"
)

func TestContract(t *testing.T) {
	runContract(t, func(t *testing.T) contractEnv {
		_, client := newFakeConsul(t)
		r := New(client)
		t.Cleanup(func() { stopAll(r) })
		return contractEnv{reg: r, dis: r, rich: true}
	})
}

// stopAll stops all heartbeats of r.
func stopAll(r *Registry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, hb := range r.beats {
		delete(r.beats, id)
		hb.stop()
	}
}

func TestEndToEnd(t *testing.T) {
	_, client := newFakeConsul(t)
	r := New(client)
	t.Cleanup(func() { stopAll(r) })
	runEndToEnd(t, r, r)
}

func TestValidation(t *testing.T) {
	_, client := newFakeConsul(t)
	r := New(client)
	tests := []struct {
		name string
		ins  *discovery.Instance
	}{
		{"nil", nil},
		{"empty name", &discovery.Instance{ID: "1"}},
		{"empty id", &discovery.Instance{Name: "svc"}},
		{"slash", &discovery.Instance{ID: "a/b", Name: "svc"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := r.Register(t.Context(), tt.ins); errorx.CodeOf(err) != errorx.InvalidArgument {
				t.Errorf("Register error = %v, want InvalidArgument", err)
			}
			if err := r.Deregister(t.Context(), tt.ins); errorx.CodeOf(err) != errorx.InvalidArgument {
				t.Errorf("Deregister error = %v, want InvalidArgument", err)
			}
		})
	}
	bad := &discovery.Instance{ID: "1", Name: "svc", Endpoints: []string{"10.0.0.1:80"}}
	if err := r.Register(t.Context(), bad); errorx.CodeOf(err) != errorx.InvalidArgument {
		t.Errorf("Register(invalid endpoint) error = %v, want InvalidArgument", err)
	}
	if _, err := r.Watch(t.Context(), ""); errorx.CodeOf(err) != errorx.InvalidArgument {
		t.Errorf("Watch(\"\") error = %v, want InvalidArgument", err)
	}
	// Consul rejects meta keys it does not accept.
	meta := &discovery.Instance{ID: "1", Name: "svc", Metadata: map[string]string{"a.b": "c"}}
	err := r.Register(t.Context(), meta)
	if errorx.CodeOf(err) != errorx.InvalidArgument {
		t.Errorf("Register(invalid meta) error = %v, want InvalidArgument", err)
	}
	if _, ok := errors.AsType[api.StatusError](err); !ok {
		t.Errorf("Register(invalid meta) error lost its cause: %v", err)
	}
}

func TestRegistrationLayout(t *testing.T) {
	f, client := newFakeConsul(t)
	r := New(client, WithTTL(30*time.Second), WithDeregisterCriticalServiceAfter(2*time.Minute))
	t.Cleanup(func() { stopAll(r) })
	ins := &discovery.Instance{
		ID:        "1",
		Name:      "svc",
		Version:   "v1",
		Metadata:  map[string]string{"zone": "a"},
		Endpoints: []string{"http://10.0.0.1:80", "grpc://10.0.0.1:90"},
	}
	mustRegister(t, r, ins)
	reg, ok := f.service("svc-1")
	if !ok {
		t.Fatal("service svc-1 not registered")
	}
	wantTags := []string{"servex.id=1", "servex.version=v1", "servex.endpoint=http://10.0.0.1:80", "servex.endpoint=grpc://10.0.0.1:90"}
	if reg.Name != "svc" || reg.Address != "10.0.0.1" || reg.Port != 80 || !slices.Equal(reg.Tags, wantTags) || reg.Meta["zone"] != "a" {
		t.Errorf("registration = %+v", reg)
	}
	if c := reg.Check; c == nil || c.CheckID != "service:svc-1" || c.TTL != "30s" || c.Status != api.HealthPassing || c.DeregisterCriticalServiceAfter != "2m0s" {
		t.Errorf("check = %+v", reg.Check)
	}
	if reg.Weights != nil {
		t.Errorf("weights = %+v, want Consul defaults for weight 0", reg.Weights)
	}
	_, list := mustWatch(t, r, "svc")
	want := clone(ins)
	want.Weight = 1 // Consul's default passing weight
	if len(list) != 1 || !equalInstance(list[0], want) {
		t.Fatalf("snapshot = %+v, want %+v", list, want)
	}
}

// TestWeight checks that a positive weight becomes the passing weight of the
// Consul service and is reported back by Watch, and that changing it
// updates watchers.
func TestWeight(t *testing.T) {
	f, client := newFakeConsul(t)
	r := New(client)
	t.Cleanup(func() { stopAll(r) })
	ins := &discovery.Instance{ID: "1", Name: "svc", Weight: 5, Endpoints: []string{"http://10.0.0.1:80"}}
	mustRegister(t, r, ins)
	reg, ok := f.service("svc-1")
	if !ok {
		t.Fatal("service svc-1 not registered")
	}
	if reg.Weights == nil || *reg.Weights != (api.AgentWeights{Passing: 5, Warning: 1}) {
		t.Fatalf("weights = %+v, want passing 5, warning 1", reg.Weights)
	}
	w, list := mustWatch(t, r, "svc")
	if len(list) != 1 || list[0].Weight != 5 {
		t.Fatalf("snapshot = %+v, want weight 5", list)
	}
	ins.Weight = 2
	mustRegister(t, r, ins)
	waitFor(t, w, func(list []*discovery.Instance) bool { return len(list) == 1 && list[0].Weight == 2 })
}

func TestDistinctServiceIdentities(t *testing.T) {
	f, client := newFakeConsul(t)
	r := New(client)
	t.Cleanup(func() { stopAll(r) })
	instances := []*discovery.Instance{
		{Name: "foo-bar", ID: "baz"},
		{Name: "foo", ID: "bar-baz"},
		{Name: "foo%2Dbar", ID: "baz"},
	}
	for _, ins := range instances {
		mustRegister(t, r, ins)
	}
	for _, ins := range instances {
		reg, ok := f.service(serviceID(ins))
		if !ok || reg.Name != ins.Name || !slices.Contains(reg.Tags, tagID+ins.ID) {
			t.Fatalf("registration for %+v was overwritten: %+v", ins, reg)
		}
	}
	if len(r.beats) != len(instances) {
		t.Fatalf("heartbeats = %d, want %d", len(r.beats), len(instances))
	}
	if err := r.Deregister(t.Context(), instances[0]); err != nil {
		t.Fatal(err)
	}
	for _, ins := range instances[1:] {
		if _, ok := f.service(serviceID(ins)); !ok {
			t.Fatalf("deregister removed another service: %+v", ins)
		}
	}
}

func TestHeartbeat(t *testing.T) {
	f, client := newFakeConsul(t)
	r := New(client, WithTTL(300*time.Millisecond))
	t.Cleanup(func() { stopAll(r) })
	mustRegister(t, r, inst("svc", "1"))
	w, list := mustWatch(t, r, "svc")
	if !slices.Equal(ids(list), []string{"1"}) {
		t.Fatalf("snapshot = %v, want [1]", ids(list))
	}
	// The check would have expired several times without heartbeats.
	ch := nextAsync(w)
	select {
	case res := <-ch:
		t.Fatalf("instances changed: %v, %v", ids(res.list), res.err)
	case <-time.After(1500 * time.Millisecond):
	}
	if _, updates := f.counts("svc-1"); updates < 5 {
		t.Errorf("heartbeats = %d, want at least 5", updates)
	}

	// Deregister stops the heartbeat.
	mustDeregister(t, r, inst("svc", "1"))
	if res := await(t, ch); res.err != nil || len(res.list) != 0 {
		t.Fatalf("Next after Deregister = %v, %v, want empty", ids(res.list), res.err)
	}
	_, before := f.counts("svc-1")
	time.Sleep(500 * time.Millisecond)
	if _, after := f.counts("svc-1"); after != before {
		t.Errorf("heartbeats after Deregister: %d -> %d", before, after)
	}
}

func TestExpiresWithoutHeartbeat(t *testing.T) {
	_, client := newFakeConsul(t)
	r := New(client, WithTTL(200*time.Millisecond))
	t.Cleanup(func() { stopAll(r) })
	mustRegister(t, r, inst("svc", "1"))
	w, _ := mustWatch(t, r, "svc")
	// A process that stops heartbeating (here: stopped behind the
	// registry's back) turns critical and disappears from watchers, which
	// only see passing instances.
	stopAll(r)
	waitFor(t, w, hasIDs())
}

func TestReregisterAfterAgentReset(t *testing.T) {
	f, client := newFakeConsul(t)
	r := New(client, WithTTL(300*time.Millisecond))
	t.Cleanup(func() { stopAll(r) })
	mustRegister(t, r, inst("svc", "1"))
	w, _ := mustWatch(t, r, "svc")
	f.reset()
	waitFor(t, w, hasIDs())
	// The next heartbeat is rejected and the registry registers again.
	waitFor(t, w, hasIDs("1"))
	if regs, _ := f.counts("svc-1"); regs < 2 {
		t.Errorf("registrations = %d, want at least 2", regs)
	}
}

func TestWatchSurvivesOutage(t *testing.T) {
	f, client := newFakeConsul(t)
	r := New(client)
	t.Cleanup(func() { stopAll(r) })
	w, _ := mustWatch(t, r, "svc")
	// Interrupt the blocking query, fail for a while, then recover.
	f.unavailable.Store(true)
	f.srv.CloseClientConnections()
	time.Sleep(300 * time.Millisecond)
	f.unavailable.Store(false)
	mustRegister(t, r, inst("svc", "1"))
	waitFor(t, w, hasIDs("1"))
}

func TestUnavailable(t *testing.T) {
	f, client := newFakeConsul(t)
	r := New(client)
	f.unavailable.Store(true)
	check := func(op string, err error) {
		t.Helper()
		e, ok := errors.AsType[*errorx.Error](err)
		if !ok || e.Code != errorx.Unavailable || !e.Retryable() {
			t.Errorf("%s error = %v, want retryable Unavailable", op, err)
		}
		if se, ok := errors.AsType[api.StatusError](err); !ok || se.Code != http.StatusInternalServerError {
			t.Errorf("%s error lost its cause: %v", op, err)
		}
	}
	err := r.Register(t.Context(), inst("svc", "1"))
	check("Register", err)
	err = r.Deregister(t.Context(), inst("svc", "1"))
	check("Deregister", err)
	_, err = r.Watch(t.Context(), "svc")
	check("Watch", err)

	// Connection failures are Unavailable too.
	f.srv.Close()
	_, err = r.Watch(t.Context(), "svc")
	if e, ok := errors.AsType[*errorx.Error](err); !ok || e.Code != errorx.Unavailable {
		t.Errorf("Watch on a closed agent error = %v, want Unavailable", err)
	}

	err = backendErr("op", api.StatusError{Code: http.StatusForbidden})
	if errorx.CodeOf(err) != errorx.PermissionDenied {
		t.Errorf("backendErr(403) = %v, want PermissionDenied", err)
	}
	err = backendErr("op", context.Canceled)
	if !errors.Is(err, context.Canceled) || errorx.CodeOf(err) != errorx.Canceled {
		t.Errorf("backendErr(Canceled) = %v, want context.Canceled", err)
	}
}
