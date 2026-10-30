package etcd

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

func embedded(t *testing.T) *clientv3.Client {
	t.Helper()
	config := embed.NewConfig()
	config.Dir = t.TempDir()
	config.ZapLoggerBuilder = embed.NewZapLoggerBuilder(zap.NewNop())
	address := url.URL{Scheme: "http", Host: "127.0.0.1:0"}
	config.ListenClientUrls, config.AdvertiseClientUrls = []url.URL{address}, []url.URL{address}
	config.ListenPeerUrls, config.AdvertisePeerUrls = []url.URL{address}, []url.URL{address}
	config.InitialCluster = config.InitialClusterFromName(config.Name)
	server, err := embed.StartEtcd(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	select {
	case <-server.Server.ReadyNotify():
	case <-time.After(30 * time.Second):
		t.Fatal("embedded etcd not ready")
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{server.Clients[0].Addr().String()}, DialTimeout: 3 * time.Second, Logger: zap.NewNop()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestLeaseLockAndElection(t *testing.T) { exercise(t, embedded(t), "/test-coordination/") }

func exercise(t *testing.T, client *clientv3.Client, prefix string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	c, err := New(client, prefix, 5)
	if err != nil {
		t.Fatal(err)
	}
	first, err := c.Acquire(ctx, "resource")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Release(context.Background()) })
	snapshot, err := client.Get(ctx, prefix+"locks/resource", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	watch := client.Watch(ctx, prefix+"locks/resource", clientv3.WithPrefix(), clientv3.WithRev(snapshot.Header.Revision+1))
	result := make(chan *Lock, 1)
	failures := make(chan error, 1)
	go func() {
		second, err := c.Acquire(ctx, "resource")
		if err != nil {
			failures <- err
			return
		}
		result <- second
	}()
	// Observing the contender's own key proves it entered etcd's wait queue.
	select {
	case response := <-watch:
		if response.Err() != nil || len(response.Events) == 0 {
			t.Fatal("contender not registered", response.Err())
		}
	case err := <-failures:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(context.Cause(ctx))
	}
	select {
	case <-result:
		t.Fatal("overlapping lock owners")
	default:
	}
	if err := first.Release(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case second := <-result:
		if second.Revision <= first.Revision {
			t.Fatal("fencing revision did not increase")
		}
		if err := second.Release(ctx); err != nil {
			t.Fatal(err)
		}
	case err := <-failures:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(context.Cause(ctx))
	}
	leader, err := c.Campaign(ctx, "scheduler", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	if leader.Revision <= 0 {
		t.Fatal("missing election revision")
	}
	if err := leader.Resign(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-leader.Done():
	case <-ctx.Done():
		t.Fatal("lease did not stop")
	}
	if _, err := c.Acquire(ctx, "../escape"); err == nil {
		t.Fatal("unsafe name")
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := c.Acquire(canceled, "cancel"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestConfiguration(t *testing.T) {
	if _, err := New(nil, "/test/", 5); err == nil {
		t.Fatal("nil client")
	}
}
