//go:build integration

package etcd

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
	"uuid"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// itClient connects to the etcd cluster named by SERVEX_IT_ETCD_ENDPOINTS
// (comma separated) and returns a namespace private to the test.
func itClient(t *testing.T) (*clientv3.Client, string) {
	t.Helper()
	endpoints := os.Getenv("SERVEX_IT_ETCD_ENDPOINTS")
	if endpoints == "" {
		t.Skip("SERVEX_IT_ETCD_ENDPOINTS is not set")
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(endpoints, ","),
		DialTimeout: 5 * time.Second,
		Logger:      zap.NewNop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ns := "/servex-it/" + uuid.NewV7().String()
	t.Cleanup(func() {
		if _, err := cli.Delete(context.Background(), ns+"/", clientv3.WithPrefix()); err != nil {
			t.Errorf("cleanup: %v", err)
		}
		cli.Close()
	})
	return cli, ns
}

func TestIntegrationContract(t *testing.T) {
	cli, ns := itClient(t)
	n := 0
	runContract(t, func(t *testing.T) contractEnv {
		n++
		r := New(cli, WithNamespace(ns+"/"+string(rune('a'+n))))
		t.Cleanup(func() { deregisterAll(r) })
		return contractEnv{reg: r, dis: r, rich: true}
	})
}

func TestIntegrationEndToEnd(t *testing.T) {
	cli, ns := itClient(t)
	r := New(cli, WithNamespace(ns))
	runEndToEnd(t, r, r)
}

func TestIntegrationReregisterWhenLeaseLost(t *testing.T) {
	cli, ns := itClient(t)
	r := New(cli, WithNamespace(ns), WithTTL(3*time.Second))
	t.Cleanup(func() { deregisterAll(r) })
	mustRegister(t, r, inst("svc", "1"))
	w, _ := mustWatch(t, r, "svc")
	resp, err := cli.Get(t.Context(), r.key("svc", "1"))
	if err != nil || len(resp.Kvs) != 1 {
		t.Fatalf("Get = %v, %v", resp, err)
	}
	if _, err := cli.Revoke(t.Context(), clientv3.LeaseID(resp.Kvs[0].Lease)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, w, hasIDs())
	waitFor(t, w, hasIDs("1"))
}
