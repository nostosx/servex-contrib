package nacos

import (
	"errors"
	"maps"
	"slices"
	"testing"

	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"

	"github.com/nostosx/servex/discovery"
	"github.com/nostosx/servex/errorx"
)

func TestContract(t *testing.T) {
	runContract(t, func(t *testing.T) contractEnv {
		r := New(newFakeNaming())
		return contractEnv{reg: r, dis: r, rich: true}
	})
}

func TestEndToEnd(t *testing.T) {
	r := New(newFakeNaming())
	runEndToEnd(t, r, r)
}

func TestValidation(t *testing.T) {
	f := newFakeNaming()
	r := New(f)
	tests := []struct {
		name string
		ins  *discovery.Instance
	}{
		{"nil", nil},
		{"empty name", &discovery.Instance{ID: "1", Endpoints: []string{"http://10.0.0.1:80"}}},
		{"empty id", &discovery.Instance{Name: "svc", Endpoints: []string{"http://10.0.0.1:80"}}},
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
	for _, eps := range [][]string{nil, {"10.0.0.1:80"}, {"http://10.0.0.1"}, {"http://10.0.0.1:0"}, {"http://:80"}} {
		ins := &discovery.Instance{ID: "1", Name: "svc", Endpoints: eps}
		if err := r.Register(t.Context(), ins); errorx.CodeOf(err) != errorx.InvalidArgument {
			t.Errorf("Register(endpoints %v) error = %v, want InvalidArgument", eps, err)
		}
	}
	if _, err := r.Watch(t.Context(), ""); errorx.CodeOf(err) != errorx.InvalidArgument {
		t.Errorf("Watch(\"\") error = %v, want InvalidArgument", err)
	}
	if n := f.count("RegisterInstance") + f.count("BatchRegisterInstance"); n != 0 {
		t.Errorf("invalid instances reached the client %d times", n)
	}
}

func TestPublishedLayout(t *testing.T) {
	f := newFakeNaming()
	r := New(f, WithGroup("G"), WithCluster("C"))
	ins := &discovery.Instance{
		ID:        "1",
		Name:      "svc",
		Version:   "v1",
		Metadata:  map[string]string{"zone": "a", "servex.id": "spoofed"},
		Endpoints: []string{"http://10.0.0.1:80", "grpc://10.0.0.1:90"},
	}
	mustRegister(t, r, ins)
	hosts := f.published["G@@svc"]
	if len(hosts) != 2 || f.count("BatchRegisterInstance") != 1 {
		t.Fatalf("published = %+v, want one batch of two hosts", hosts)
	}
	h := hosts[0]
	want := map[string]string{"zone": "a", "servex.id": "1", "servex.version": "v1", "servex.endpoint": "http://10.0.0.1:80", "servex.index": "0"}
	if h.Ip != "10.0.0.1" || h.Port != 80 || h.ClusterName != "C" || !h.Ephemeral || !h.Healthy || !h.Enable || h.Weight != 1 || !maps.Equal(h.Metadata, want) {
		t.Errorf("host = %+v", h)
	}

	w, list := mustWatch(t, r, "svc")
	wantIns := &discovery.Instance{ID: "1", Name: "svc", Version: "v1", Metadata: map[string]string{"zone": "a"}, Endpoints: ins.Endpoints}
	if len(list) != 1 || !equalInstance(list[0], wantIns) {
		t.Fatalf("snapshot = %+v, want %+v", list, wantIns)
	}
	if f.subscribers("G", "svc") != 1 {
		t.Errorf("subscribers = %d, want 1", f.subscribers("G", "svc"))
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	if f.subscribers("G", "svc") != 0 {
		t.Errorf("subscribers after Stop = %d, want 0", f.subscribers("G", "svc"))
	}

	// Other groups and clusters are separate.
	other := New(f, WithGroup("G"), WithCluster("D"))
	_, list = mustWatch(t, other, "svc")
	if len(list) != 0 {
		t.Errorf("other cluster snapshot = %v, want empty", ids(list))
	}
}

func TestPublishesServiceSetTogether(t *testing.T) {
	f := newFakeNaming()
	r := New(f)
	mustRegister(t, r, inst("svc", "1"))
	if f.count("RegisterInstance") != 1 {
		t.Fatalf("a single host must use RegisterInstance")
	}
	mustRegister(t, r, inst("svc", "2"))
	if got := len(f.published[serviceKey("", "svc")]); got != 2 || f.count("BatchRegisterInstance") != 1 {
		t.Fatalf("published %d hosts, want both instances in one batch", got)
	}
	// Removing one instance republishes the rest ...
	mustDeregister(t, r, inst("svc", "1"))
	hosts := f.published[serviceKey("", "svc")]
	if len(hosts) != 1 || hosts[0].Metadata[metaID] != "2" || f.count("DeregisterInstance") != 0 {
		t.Fatalf("published = %+v, want only instance 2", hosts)
	}
	// ... and removing the last one deregisters the service.
	mustDeregister(t, r, inst("svc", "2"))
	if _, ok := f.published[serviceKey("", "svc")]; ok || f.count("DeregisterInstance") != 1 {
		t.Fatalf("service still published after the last Deregister")
	}
	// Unknown instances are not sent to the client.
	mustDeregister(t, r, inst("svc", "3"))
	if f.count("DeregisterInstance") != 1 {
		t.Errorf("unknown instance reached the client")
	}
}

func TestFiltersUnusableHosts(t *testing.T) {
	f := newFakeNaming()
	r := New(f)
	mustRegister(t, r, inst("svc", "1"))
	mustRegister(t, r, inst("svc", "2"))
	w, list := mustWatch(t, r, "svc")
	if !slices.Equal(ids(list), []string{"1", "2"}) {
		t.Fatalf("snapshot = %v", ids(list))
	}
	setFor := func(id string, mutate func(*model.Instance)) func(*model.Instance) {
		return func(h *model.Instance) {
			if h.Metadata[metaID] == id {
				mutate(h)
			}
		}
	}
	f.update("", "svc", setFor("1", func(h *model.Instance) { h.Healthy = false }))
	waitFor(t, w, hasIDs("2"))
	f.update("", "svc", setFor("1", func(h *model.Instance) { h.Healthy = true }))
	waitFor(t, w, hasIDs("1", "2"))
	f.update("", "svc", setFor("2", func(h *model.Instance) { h.Enable = false }))
	waitFor(t, w, hasIDs("1"))
	f.update("", "svc", setFor("1", func(h *model.Instance) { h.Weight = 0 }))
	waitFor(t, w, hasIDs())
	// Hosts registered by other frameworks carry no servex metadata.
	f.update("", "svc", func(h *model.Instance) {
		h.Weight, h.Enable = 1, true
		delete(h.Metadata, metaID)
	})
	ch := nextAsync(w)
	assertBlocked(t, ch, "for hosts without servex metadata")
	if err := w.Stop(); err != nil {
		t.Errorf("Stop error = %v", err)
	}
}

func TestNotificationErrorsKeepSnapshot(t *testing.T) {
	f := newFakeNaming()
	r := New(f)
	mustRegister(t, r, inst("svc", "1"))
	w, _ := mustWatch(t, r, "svc")
	f.mu.Lock()
	f.notifyErr = true
	f.mu.Unlock()
	f.update("", "svc", func(*model.Instance) {})
	assertBlocked(t, nextAsync(w), "after a failed notification")
}

func TestNotificationWinsOverInitialRead(t *testing.T) {
	f := newFakeNaming()
	mustRegister(t, New(f), inst("svc", "1"))
	// The initial read returns a list that a notification, delivered while
	// the read was in flight, already superseded.
	r := New(&staleRead{fakeNaming: f})
	_, list := mustWatch(t, r, "svc")
	if len(list) != 0 {
		t.Fatalf("snapshot = %v, want the newer empty list", ids(list))
	}
}

// staleRead is a naming client whose SelectAllInstances result is
// overtaken by a notification that marks every host unhealthy.
type staleRead struct {
	*fakeNaming
}

func (s *staleRead) SelectAllInstances(p vo.SelectAllInstancesParam) ([]model.Instance, error) {
	list, err := s.fakeNaming.SelectAllInstances(p)
	s.update(p.GroupName, p.ServiceName, func(h *model.Instance) { h.Healthy = false })
	return list, err
}

func TestStateIgnoresEqualLists(t *testing.T) {
	st := newState()
	st.set([]*discovery.Instance{inst("svc", "2")})
	st.set([]*discovery.Instance{inst("svc", "2")})
	if st.version != 1 {
		t.Fatalf("version = %d, want 1", st.version)
	}
}

func TestUnavailable(t *testing.T) {
	f := newFakeNaming()
	r := New(f)
	mustRegister(t, r, inst("svc", "1"))
	w, _ := mustWatch(t, r, "svc")
	f.fail.Store(true)

	check := func(op string, err error) {
		t.Helper()
		e, ok := errors.AsType[*errorx.Error](err)
		if !ok || e.Code != errorx.Unavailable || !e.Retryable() || !errors.Is(err, errFake) {
			t.Errorf("%s error = %v, want retryable Unavailable caused by errFake", op, err)
		}
	}
	check("Register", r.Register(t.Context(), inst("svc", "2")))
	check("Deregister", r.Deregister(t.Context(), inst("svc", "1")))
	_, err := r.Watch(t.Context(), "svc")
	check("Watch", err)
	check("Stop", w.Stop())
	if err := w.Stop(); err == nil {
		t.Error("second Stop did not return the first error")
	}

	// Failed calls leave the local state unchanged.
	f.fail.Store(false)
	if got := slices.Sorted(maps.Keys(r.services["svc"])); !slices.Equal(got, []string{"1"}) {
		t.Errorf("local instances = %v, want [1]", got)
	}
	// A failed initial read does not leave a subscription behind.
	before := f.subscribers("", "svc")
	failRead := &failingRead{fakeNaming: f}
	if _, err := New(failRead).Watch(t.Context(), "svc"); errorx.CodeOf(err) != errorx.Unavailable {
		t.Errorf("Watch with a failing read error = %v, want Unavailable", err)
	}
	if got := f.subscribers("", "svc"); got != before {
		t.Errorf("subscribers = %d, want %d", got, before)
	}
}

// failingRead is a naming client whose SelectAllInstances fails.
type failingRead struct {
	*fakeNaming
}

func (f *failingRead) SelectAllInstances(vo.SelectAllInstancesParam) ([]model.Instance, error) {
	return nil, errFake
}
