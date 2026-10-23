//go:build integration

package nacos

import (
	"context"
	"errors"
	"net"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"
	"uuid"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"

	"github.com/nostosx/servex/discovery"
)

// itClient connects a naming client to the server at SERVEX_IT_NACOS_ADDR
// (host:port of the HTTP API). The gRPC port defaults to port+1000 and can
// be set with SERVEX_IT_NACOS_GRPC_PORT.
func itClient(t *testing.T) naming_client.INamingClient {
	t.Helper()
	addr := os.Getenv("SERVEX_IT_NACOS_ADDR")
	if addr == "" {
		t.Skip("SERVEX_IT_NACOS_ADDR is not set")
	}
	host, p, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.ParseUint(p, 10, 16)
	if err != nil {
		t.Fatal(err)
	}
	grpc := port + 1000
	if v := os.Getenv("SERVEX_IT_NACOS_GRPC_PORT"); v != "" {
		if grpc, err = strconv.ParseUint(v, 10, 16); err != nil {
			t.Fatal(err)
		}
	}
	// The client keeps writing logs after CloseClient, which t.TempDir
	// cleanup would report; remove the directory on a best effort basis.
	dir, err := os.MkdirTemp("", "servex-nacos-it")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	client, err := clients.NewNamingClient(vo.NacosClientParam{
		ClientConfig: constant.NewClientConfig(
			constant.WithTimeoutMs(5000),
			constant.WithNotLoadCacheAtStart(true),
			// Required for watchers to see a service become empty.
			constant.WithUpdateCacheWhenEmpty(true),
			constant.WithLogDir(dir),
			constant.WithCacheDir(dir),
			constant.WithLogLevel("error"),
		),
		ServerConfigs: []constant.ServerConfig{*constant.NewServerConfig(host, port, constant.WithGrpcPort(grpc))},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.CloseClient)
	return client
}

// syncRegistrar waits after every call until the change reached the
// client's service cache, which the server updates asynchronously.
type syncRegistrar struct {
	r *Registry
}

func (s syncRegistrar) Register(ctx context.Context, ins *discovery.Instance) error {
	if err := s.r.Register(ctx, ins); err != nil {
		return err
	}
	return s.await(ctx, ins, true)
}

func (s syncRegistrar) Deregister(ctx context.Context, ins *discovery.Instance) error {
	if err := s.r.Deregister(ctx, ins); err != nil {
		return err
	}
	return s.await(ctx, ins, false)
}

func (s syncRegistrar) await(ctx context.Context, ins *discovery.Instance, present bool) error {
	ctx, cancel := context.WithTimeout(ctx, patience)
	defer cancel()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		list, err := s.r.client.SelectAllInstances(vo.SelectAllInstancesParam{
			ServiceName: ins.Name,
			GroupName:   s.r.group,
			Clusters:    []string{s.r.cluster},
		})
		if err != nil {
			return err
		}
		found := slices.ContainsFunc(list, func(n model.Instance) bool { return n.Metadata[metaID] == ins.ID })
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

func newSyncRegistrar(t *testing.T, r *Registry) syncRegistrar {
	t.Helper()
	t.Cleanup(func() {
		r.mu.Lock()
		var registered []*discovery.Instance
		for name, instances := range r.services {
			for id := range instances {
				registered = append(registered, &discovery.Instance{Name: name, ID: id})
			}
		}
		r.mu.Unlock()
		for _, ins := range registered {
			ctx, cancel := context.WithTimeout(context.Background(), patience)
			err := r.Deregister(ctx, ins)
			cancel()
			if err != nil {
				t.Errorf("cleanup registration: %v", err)
			}
		}
	})
	return syncRegistrar{r}
}

func TestIntegrationContract(t *testing.T) {
	client := itClient(t)
	runContract(t, func(t *testing.T) contractEnv {
		// A fresh group isolates every subtest.
		r := New(client, WithGroup("servex-it-"+uuid.NewV7().String()))
		return contractEnv{reg: newSyncRegistrar(t, r), dis: r, rich: true}
	})
}

func TestIntegrationEndToEnd(t *testing.T) {
	r := New(itClient(t), WithGroup("servex-it-"+uuid.NewV7().String()))
	runEndToEnd(t, newSyncRegistrar(t, r), r)
}

func TestSyncRegistrarAwait(t *testing.T) {
	f := newFakeNaming()
	s := syncRegistrar{New(f)}
	ins := inst("missing", "1")
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := s.await(ctx, ins, true); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("await missing instance = %v, want DeadlineExceeded", err)
	}
	f.fail.Store(true)
	if err := s.await(t.Context(), ins, true); !errors.Is(err, errFake) {
		t.Fatalf("await unavailable registry = %v, want errFake", err)
	}
}
