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

func TestIntegrationEtcd(t *testing.T) {
	endpoints := os.Getenv("SERVEX_IT_ETCD_ENDPOINTS")
	if endpoints == "" {
		t.Skip("SERVEX_IT_ETCD_ENDPOINTS not set")
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(endpoints, ","), DialTimeout: 3 * time.Second, Logger: zap.NewNop()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	prefix := "/servex-it-coordination/" + uuid.NewV7().String() + "/"
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := client.Delete(ctx, prefix, clientv3.WithPrefix()); err != nil {
			t.Error(err)
		}
	})
	exercise(t, client, prefix)
}
