package etcd

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/nostosx/servex/discovery"
	"github.com/nostosx/servex/errorx"
)

// errInjected is returned by the calls that faults makes fail.
var errInjected = errors.New("injected failure")

// faults switches failures of a client's Put and KeepAlive on and off.
type faults struct {
	put, keepAlive atomic.Bool
}

// faultKV fails Put while its faults say so.
type faultKV struct {
	clientv3.KV
	f *faults
}

func (kv faultKV) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	if kv.f.put.Load() {
		return nil, errInjected
	}
	return kv.KV.Put(ctx, key, val, opts...)
}

// faultLease fails KeepAlive while its faults say so.
type faultLease struct {
	clientv3.Lease
	f *faults
}

func (l faultLease) KeepAlive(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	if l.f.keepAlive.Load() {
		return nil, errInjected
	}
	return l.Lease.KeepAlive(ctx, id)
}

// faultyRegistry returns a Registry whose client fails on demand, and the
// client itself for inspecting the store.
func faultyRegistry(t *testing.T) (*Registry, *clientv3.Client, *faults) {
	t.Helper()
	cli := startEtcd(t)
	f := &faults{}
	// The client is new and not shared yet, so its fields can be replaced.
	cli.KV = faultKV{KV: cli.KV, f: f}
	cli.Lease = faultLease{Lease: cli.Lease, f: f}
	r := New(cli, WithTTL(time.Second))
	t.Cleanup(func() { deregisterAll(r) })
	return r, cli, f
}

// stored returns the lease and version of the instance stored under key.
func stored(t *testing.T, cli *clientv3.Client, name, key string) (clientv3.LeaseID, string) {
	t.Helper()
	resp, err := cli.Get(t.Context(), key)
	if err != nil || len(resp.Kvs) != 1 {
		t.Fatalf("Get(%s) = %v, %v, want one key", key, resp, err)
	}
	kv := resp.Kvs[0]
	ins, ok := decode(name, kv.Key, kv.Value)
	if !ok {
		t.Fatalf("stored value %s is not an instance", kv.Value)
	}
	return clientv3.LeaseID(kv.Lease), ins.Version
}

// failRegister re-registers instance svc/1 as version v2 and checks that it
// fails with the injected error.
func failRegister(t *testing.T, r *Registry) {
	t.Helper()
	ins := inst("svc", "1")
	ins.Version = "v2"
	err := r.Register(t.Context(), ins)
	if errorx.CodeOf(err) != errorx.Unavailable || !errors.Is(err, errInjected) {
		t.Fatalf("Register error = %v, want Unavailable caused by the injected failure", err)
	}
}

// v1 returns instance svc/1 at version v1.
func v1() *discovery.Instance {
	ins := inst("svc", "1")
	ins.Version = "v1"
	return ins
}

func TestRegisterFailureResumesLiveLease(t *testing.T) {
	r, cli, f := faultyRegistry(t)
	key := r.key("svc", "1")
	mustRegister(t, r, v1())
	old := r.keepers[key].leaseID()

	f.put.Store(true)
	failRegister(t, r)
	f.put.Store(false)

	// The previous registration is kept on its own lease ...
	if got := r.keepers[key].leaseID(); got != old {
		t.Fatalf("tracked lease = %x, want the previous lease %x", got, old)
	}
	ttl, err := cli.TimeToLive(t.Context(), old)
	if err != nil {
		t.Fatal(err)
	}
	// ... and that lease is still kept alive after it would have expired.
	time.Sleep(time.Duration(ttl.GrantedTTL+1) * time.Second)
	lease, version := stored(t, cli, "svc", key)
	if lease != old || version != "v1" {
		t.Errorf("stored = lease %x version %q, want lease %x version v1", lease, version, old)
	}
	if got := r.keepers[key].leaseID(); got != old {
		t.Errorf("tracked lease = %x, want %x", got, old)
	}
}

func TestRegisterFailureRestoresLostLease(t *testing.T) {
	r, cli, f := faultyRegistry(t)
	key := r.key("svc", "1")
	mustRegister(t, r, v1())
	w, _ := mustWatch(t, r, "svc")
	old := r.keepers[key].leaseID()

	// The lease is lost while writes fail, so it cannot be replaced yet.
	f.put.Store(true)
	if _, err := cli.Revoke(t.Context(), old); err != nil {
		t.Fatal(err)
	}
	waitFor(t, w, hasIDs())
	failRegister(t, r)

	// The resumed keeper finds the lease gone and writes the previous
	// registration again once writes succeed.
	f.put.Store(false)
	list := waitFor(t, w, hasIDs("1"))
	if list[0].Version != "v1" {
		t.Errorf("restored version = %q, want v1", list[0].Version)
	}
	lease, _ := stored(t, cli, "svc", key)
	if lease == old || lease != r.keepers[key].leaseID() {
		t.Errorf("stored lease = %x, want a new lease tracked by the registry (old %x)", lease, old)
	}
}

func TestRegisterFailureWithoutKeepAlive(t *testing.T) {
	r, cli, f := faultyRegistry(t)
	key := r.key("svc", "1")
	mustRegister(t, r, v1())
	old := r.keepers[key].leaseID()

	// Neither a new registration nor the old lease's keep-alive can start.
	f.put.Store(true)
	f.keepAlive.Store(true)
	failRegister(t, r)

	// The keeper keeps trying in the background and registers the previous
	// instance with a new lease once the client recovers.
	f.put.Store(false)
	f.keepAlive.Store(false)
	eventually(t, func() (any, error) {
		lease, version := stored(t, cli, "svc", key)
		if lease == old || lease != r.keepers[key].leaseID() || version != "v1" {
			return nil, fmt.Errorf("stored lease %x version %q, want a new tracked lease with v1 (old %x)", lease, version, old)
		}
		return nil, nil
	})

	// Deregister still stops the keeper and removes the instance.
	mustDeregister(t, r, v1())
	if resp, err := cli.Get(t.Context(), key); err != nil || len(resp.Kvs) != 0 {
		t.Fatalf("Get = %v, %v, want no key", resp, err)
	}
}
