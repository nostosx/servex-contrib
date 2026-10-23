package etcd

import (
	"context"
	"encoding/json/v2"
	"errors"
	"net/url"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"

	"github.com/nostosx/servex/discovery"
	"github.com/nostosx/servex/errorx"
)

// startEtcd starts an embedded single-node etcd and returns a client for it.
func startEtcd(t *testing.T) *clientv3.Client {
	t.Helper()
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(zap.NewNop())
	client := url.URL{Scheme: "http", Host: "127.0.0.1:0"}
	peer := url.URL{Scheme: "http", Host: "127.0.0.1:0"}
	cfg.ListenClientUrls, cfg.AdvertiseClientUrls = []url.URL{client}, []url.URL{client}
	cfg.ListenPeerUrls, cfg.AdvertisePeerUrls = []url.URL{peer}, []url.URL{peer}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("start etcd: %v", err)
	}
	t.Cleanup(e.Close)
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(30 * time.Second):
		t.Fatal("etcd did not become ready")
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{e.Clients[0].Addr().String()},
		DialTimeout: 5 * time.Second,
		Logger:      zap.NewNop(),
	})
	if err != nil {
		t.Fatalf("etcd client: %v", err)
	}
	t.Cleanup(func() { cli.Close() })
	return cli
}

func TestContract(t *testing.T) {
	cli := startEtcd(t)
	n := 0
	runContract(t, func(t *testing.T) contractEnv {
		// Every subtest gets its own namespace on the shared server.
		n++
		r := New(cli, WithNamespace("/contract/"+string(rune('a'+n))))
		t.Cleanup(func() { deregisterAll(r) })
		return contractEnv{reg: r, dis: r, rich: true}
	})
}

// deregisterAll stops all keepers of r and revokes their leases.
func deregisterAll(r *Registry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, k := range r.keepers {
		delete(r.keepers, key)
		k.stop()
		_, _ = r.client.Revoke(context.Background(), k.leaseID())
	}
}

func TestValidation(t *testing.T) {
	r := New(nil)
	tests := []struct {
		name string
		ins  *discovery.Instance
	}{
		{"nil", nil},
		{"empty name", &discovery.Instance{ID: "1"}},
		{"empty id", &discovery.Instance{Name: "svc"}},
		{"slash in name", &discovery.Instance{ID: "1", Name: "a/b"}},
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
	for _, name := range []string{"", "a/b"} {
		if _, err := r.Watch(t.Context(), name); errorx.CodeOf(err) != errorx.InvalidArgument {
			t.Errorf("Watch(%q) error = %v, want InvalidArgument", name, err)
		}
	}
}

func TestStorageLayout(t *testing.T) {
	cli := startEtcd(t)
	r := New(cli, WithNamespace("/ns/"), WithTTL(30*time.Second))
	t.Cleanup(func() { deregisterAll(r) })
	ins := &discovery.Instance{
		ID:        "1",
		Name:      "svc",
		Version:   "v1",
		Metadata:  map[string]string{"zone": "a"},
		Endpoints: []string{"http://10.0.0.1:80", "grpc://10.0.0.1:90"},
	}
	mustRegister(t, r, ins)
	resp, err := cli.Get(t.Context(), "/ns/svc/1")
	if err != nil || len(resp.Kvs) != 1 {
		t.Fatalf("Get = %v, %v, want one key", resp, err)
	}
	var got map[string]any
	if err := json.Unmarshal(resp.Kvs[0].Value, &got); err != nil {
		t.Fatal(err)
	}
	if got["id"] != "1" || got["name"] != "svc" || got["version"] != "v1" {
		t.Errorf("stored value = %s", resp.Kvs[0].Value)
	}
	ttl, err := cli.TimeToLive(t.Context(), clientv3.LeaseID(resp.Kvs[0].Lease))
	if err != nil {
		t.Fatal(err)
	}
	if ttl.GrantedTTL != 30 {
		t.Errorf("granted TTL = %d, want 30", ttl.GrantedTTL)
	}

	// Values that are not instances are ignored by watchers.
	if _, err := cli.Put(t.Context(), "/ns/svc/garbage", "{"); err != nil {
		t.Fatal(err)
	}
	w, list := mustWatch(t, r, "svc")
	if len(list) != 1 || !equalInstance(list[0], ins) {
		t.Fatalf("snapshot = %+v, want %+v", list, ins)
	}
	// A value without an id takes it from the key.
	if _, err := cli.Put(t.Context(), "/ns/svc/2", `{"name":"svc"}`); err != nil {
		t.Fatal(err)
	}
	waitFor(t, w, hasIDs("1", "2"))

	// Deregister revokes the lease, which deletes the key.
	mustDeregister(t, r, ins)
	ttl, err = cli.TimeToLive(t.Context(), clientv3.LeaseID(resp.Kvs[0].Lease))
	if err != nil {
		t.Fatal(err)
	}
	if ttl.TTL != -1 {
		t.Errorf("lease TTL after Deregister = %d, want -1 (revoked)", ttl.TTL)
	}
	waitFor(t, w, hasIDs("2"))
}

func TestKeepAlive(t *testing.T) {
	cli := startEtcd(t)
	r := New(cli, WithTTL(time.Second))
	t.Cleanup(func() { deregisterAll(r) })
	mustRegister(t, r, inst("svc", "1"))
	w, _ := mustWatch(t, r, "svc")

	// The lease (at least 1s, raised to the server minimum) would have
	// expired several times without keep-alive.
	ch := nextAsync(w)
	select {
	case res := <-ch:
		t.Fatalf("instance changed while kept alive: %v, %v", ids(res.list), res.err)
	case <-time.After(5 * time.Second):
	}
	resp, err := cli.Get(t.Context(), r.key("svc", "1"))
	if err != nil || len(resp.Kvs) != 1 {
		t.Fatalf("Get = %v, %v, want the instance", resp, err)
	}
}

func TestReregisterWhenLeaseLost(t *testing.T) {
	cli := startEtcd(t)
	// Keep-alive requests go out every TTL/3; a short TTL detects the lost
	// lease quickly.
	r := New(cli, WithTTL(3*time.Second))
	t.Cleanup(func() { deregisterAll(r) })
	mustRegister(t, r, inst("svc", "1"))
	w, _ := mustWatch(t, r, "svc")

	resp, err := cli.Get(t.Context(), r.key("svc", "1"))
	if err != nil || len(resp.Kvs) != 1 {
		t.Fatalf("Get = %v, %v", resp, err)
	}
	lost := clientv3.LeaseID(resp.Kvs[0].Lease)
	// Revoking the lease behind the registry's back deletes the key ...
	if _, err := cli.Revoke(t.Context(), lost); err != nil {
		t.Fatal(err)
	}
	waitFor(t, w, hasIDs())
	// ... and the registry writes it again with a new lease.
	waitFor(t, w, hasIDs("1"))
	resp, err = cli.Get(t.Context(), r.key("svc", "1"))
	if err != nil || len(resp.Kvs) != 1 {
		t.Fatalf("Get = %v, %v", resp, err)
	}
	if got := clientv3.LeaseID(resp.Kvs[0].Lease); got == lost || got != r.keepers[r.key("svc", "1")].leaseID() {
		t.Errorf("lease = %x, want a new lease tracked by the registry", got)
	}
}

func TestRegisterReplacesLease(t *testing.T) {
	cli := startEtcd(t)
	r := New(cli)
	t.Cleanup(func() { deregisterAll(r) })
	mustRegister(t, r, inst("svc", "1"))
	old := r.keepers[r.key("svc", "1")].leaseID()
	mustRegister(t, r, inst("svc", "1"))
	cur := r.keepers[r.key("svc", "1")].leaseID()
	if cur == old {
		t.Fatal("Register did not grant a new lease")
	}
	ttl, err := cli.TimeToLive(t.Context(), old)
	if err != nil {
		t.Fatal(err)
	}
	if ttl.TTL != -1 {
		t.Errorf("old lease TTL = %d, want -1 (revoked)", ttl.TTL)
	}
	resp, err := cli.Get(t.Context(), r.key("svc", "1"))
	if err != nil || len(resp.Kvs) != 1 || clientv3.LeaseID(resp.Kvs[0].Lease) != cur {
		t.Fatalf("Get = %v, %v, want the key on the new lease", resp, err)
	}
}

func TestDeregisterForeignKey(t *testing.T) {
	cli := startEtcd(t)
	a, b := New(cli), New(cli)
	t.Cleanup(func() { deregisterAll(a) })
	mustRegister(t, a, inst("svc", "1"))
	// Another registry can remove the instance; it deletes the key.
	mustDeregister(t, b, inst("svc", "1"))
	resp, err := cli.Get(t.Context(), a.key("svc", "1"))
	if err != nil || len(resp.Kvs) != 0 {
		t.Fatalf("Get = %v, %v, want no key", resp, err)
	}
}

func TestUnavailable(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:1"},
		DialTimeout: time.Second,
		Logger:      zap.NewNop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	r := New(cli)
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	if err := r.Register(ctx, inst("svc", "1")); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Register error = %v, want context.DeadlineExceeded", err)
	}
	if _, err := r.Watch(ctx, "svc"); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Watch error = %v, want context.DeadlineExceeded", err)
	}

	err = backendErr("register", rpctypes.ErrGRPCNoLeader)
	if e, ok := errors.AsType[*errorx.Error](err); !ok || e.Code != errorx.Unavailable || !e.Retryable() {
		t.Errorf("backendErr = %v, want retryable Unavailable", err)
	}
	if !errors.Is(err, rpctypes.ErrGRPCNoLeader) {
		t.Errorf("backendErr lost its cause: %v", err)
	}
}

func TestWatchRecoversAfterCompaction(t *testing.T) {
	cli := startEtcd(t)
	r := New(cli)
	t.Cleanup(func() { deregisterAll(r) })
	w, err := r.Watch(t.Context(), "svc")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.Stop(); err != nil {
			t.Errorf("Stop error = %v", err)
		}
	}()
	if res := await(t, nextAsync(w)); res.err != nil {
		t.Fatal(res.err)
	}

	// Stop the background follower, advance and compact the store, then
	// follow again from the stale revision as if the watch had been
	// interrupted: the follower must resynchronize with a full read.
	ww := w.(*watcher)
	ww.cancel()
	<-ww.done
	resp, err := cli.Get(t.Context(), "/")
	if err != nil {
		t.Fatal(err)
	}
	stale := resp.Header.Revision
	mustRegister(t, r, inst("svc", "1"))
	put, err := cli.Put(t.Context(), "/unrelated", "x")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Compact(t.Context(), put.Header.Revision); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	w2 := newWatcher(ww.st, cancel)
	defer func() {
		if err := w2.Stop(); err != nil {
			t.Errorf("Stop error = %v", err)
		}
	}()
	go func() {
		defer close(w2.done)
		r.follow(ctx, "svc", r.namespace+"/svc/", stale, map[string]*discovery.Instance{}, ww.st)
	}()
	waitFor(t, w2, hasIDs("1"))
}

func TestEndToEnd(t *testing.T) {
	r := New(startEtcd(t))
	runEndToEnd(t, r, r)
}
