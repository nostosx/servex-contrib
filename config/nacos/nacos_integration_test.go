//go:build integration

package nacos_test

import (
	"context"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"

	"github.com/nostosx/servex-contrib/config/nacos"
	"github.com/nostosx/servex-contrib/config/viper"
	"github.com/nostosx/servex/config"
)

// integrationClient connects to the Nacos server at SERVEX_IT_NACOS_ADDR
// (host:port of the HTTP API) and skips the test when it is unset. The gRPC
// port defaults to the HTTP port plus 1000 and can be overridden with
// SERVEX_IT_NACOS_GRPC_PORT.
func integrationClient(t *testing.T) config_client.IConfigClient {
	t.Helper()
	addr := os.Getenv("SERVEX_IT_NACOS_ADDR")
	if addr == "" {
		t.Skip("SERVEX_IT_NACOS_ADDR not set")
	}
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.ParseUint(portStr, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	server := *constant.NewServerConfig(host, port)
	if g := os.Getenv("SERVEX_IT_NACOS_GRPC_PORT"); g != "" {
		if server.GrpcPort, err = strconv.ParseUint(g, 10, 64); err != nil {
			t.Fatal(err)
		}
	}
	// The client's logger keeps writing after CloseClient, which defeats
	// t.TempDir's strict cleanup; remove the directory on a best-effort basis.
	dir, err := os.MkdirTemp("", "servex-nacos-it-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	c, err := clients.NewConfigClient(vo.NacosClientParam{
		ClientConfig: constant.NewClientConfig(
			constant.WithTimeoutMs(5000),
			constant.WithNotLoadCacheAtStart(true),
			constant.WithCacheDir(dir),
			constant.WithLogDir(dir),
			constant.WithLogLevel("error"),
		),
		ServerConfigs: []constant.ServerConfig{server},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.CloseClient)
	return c
}

func TestIntegrationLoadAndWatch(t *testing.T) {
	c := integrationClient(t)
	dataID := "servex-it-" + strconv.FormatInt(time.Now().UnixNano(), 36) + ".json"
	const group = "SERVEX_IT"
	publish := func(content string) {
		t.Helper()
		if ok, err := c.PublishConfig(vo.ConfigParam{DataId: dataID, Group: group, Content: content}); err != nil || !ok {
			t.Fatalf("publish = %v, %v", ok, err)
		}
	}
	t.Cleanup(func() {
		if _, err := c.DeleteConfig(vo.ConfigParam{DataId: dataID, Group: group}); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})

	// Wait until the published item is readable.
	publish(`{"http":{"timeout":"1s"},"v":0}`)
	src := nacos.New(c, dataID, group)
	deadline := time.Now().Add(30 * time.Second)
	var cfg config.Config
	for {
		var err error
		cfg, err = viper.New(t.Context(), []config.Source{src})
		if err == nil && cfg.Duration("http.timeout") == time.Second {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("published item not readable: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reloaded := make(chan int, 100)
	done := make(chan error, 1)
	go func() {
		done <- cfg.Watch(ctx, func(c config.Config) { reloaded <- c.Int("v") })
	}()
	// The listener needs a moment to register; publish until a reload
	// observes the new value.
	timeout := time.After(60 * time.Second)
	for i := 1; ; i++ {
		publish(`{"http":{"timeout":"2s"},"v":` + strconv.Itoa(i) + `}`)
		select {
		case v := <-reloaded:
			if v < 1 {
				continue
			}
			if got := cfg.Duration("http.timeout"); got != 2*time.Second {
				t.Fatalf("http.timeout = %v after reload", got)
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatalf("Watch = %v", err)
			}
			return
		case <-time.After(2 * time.Second):
		case <-timeout:
			t.Fatal("no reload")
		}
	}
}
