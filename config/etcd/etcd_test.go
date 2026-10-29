package etcd_test

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/nostosx/servex-contrib/config/etcd"
	"github.com/nostosx/servex-contrib/config/viper"
	"github.com/nostosx/servex/config"
	"github.com/nostosx/servex/errorx"
)

// kvCodec decodes "key=value" lines into a flat map.
type kvCodec struct{}

func (kvCodec) Name() string { return "kv" }

func (kvCodec) Marshal(any) ([]byte, error) { return nil, errors.New("kv: marshal unsupported") }

func (kvCodec) Unmarshal(data []byte, v any) error {
	m, ok := v.(*map[string]any)
	if !ok {
		return fmt.Errorf("kv: unsupported target %T", v)
	}
	for line := range strings.Lines(string(data)) {
		k, val, ok := strings.Cut(strings.TrimSpace(line), "=")
		if !ok {
			return fmt.Errorf("kv: malformed line %q", line)
		}
		(*m)[k] = val
	}
	return nil
}

// waitTimeout bounds every wait on the embedded server.
const waitTimeout = 10 * time.Second

// startEtcd starts an embedded single-node etcd on random loopback ports in
// a temporary directory and returns a client connected to it.
func startEtcd(t *testing.T) *clientv3.Client {
	t.Helper()
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	local := url.URL{Scheme: "http", Host: "127.0.0.1:0"}
	cfg.ListenClientUrls = []url.URL{local}
	cfg.AdvertiseClientUrls = []url.URL{local}
	cfg.ListenPeerUrls = []url.URL{local}
	cfg.AdvertisePeerUrls = []url.URL{local}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(waitTimeout):
		t.Fatal("embedded etcd did not become ready")
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{e.Clients[0].Addr().String()},
		DialTimeout: waitTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func put(t *testing.T, c *clientv3.Client, key, value string) {
	t.Helper()
	if _, err := c.Put(t.Context(), key, value); err != nil {
		t.Fatal(err)
	}
}

func TestLoad(t *testing.T) {
	c := startEtcd(t)
	put(t, c, "/app/config", `{"http":{"addr":":8000","port":8080},"tags":["a"]}`)
	put(t, c, "/app/blank", " \n")
	put(t, c, "/app/bad", `{"a":`)
	put(t, c, "/app/list", `[1]`)

	t.Run("single key", func(t *testing.T) {
		got, err := etcd.New(c, "/app/config").Load(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]any{"http": map[string]any{"addr": ":8000", "port": 8080.0}, "tags": []any{"a"}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("Load = %#v, want %#v", got, want)
		}
	})
	t.Run("blank value", func(t *testing.T) {
		got, err := etcd.New(c, "/app/blank").Load(t.Context())
		if err != nil || got == nil || len(got) != 0 {
			t.Fatalf("Load = %#v, %v; want empty map", got, err)
		}
	})
	t.Run("missing key", func(t *testing.T) {
		_, err := etcd.New(c, "/app/missing").Load(t.Context())
		if errorx.CodeOf(err) != errorx.NotFound || !strings.Contains(err.Error(), `etcd: load "/app/missing"`) {
			t.Fatalf("err = %v, want NotFound", err)
		}
	})
	for _, key := range []string{"/app/bad", "/app/list"} {
		t.Run("undecodable "+key, func(t *testing.T) {
			_, err := etcd.New(c, key).Load(t.Context())
			if errorx.CodeOf(err) != errorx.InvalidArgument || errorx.IsRetryable(err) {
				t.Fatalf("err = %v, want non-retryable InvalidArgument", err)
			}
			if !strings.Contains(err.Error(), "with json codec") {
				t.Fatalf("err = %v, want naming the codec", err)
			}
		})
	}
	t.Run("canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := etcd.New(c, "/app/config").Load(ctx)
		if !errors.Is(err, context.Canceled) || errorx.CodeOf(err) != errorx.Canceled {
			t.Fatalf("err = %v, want Canceled", err)
		}
	})
	t.Run("custom codec", func(t *testing.T) {
		put(t, c, "/app/kv", "name=svc\nport=80\n")
		got, err := etcd.New(c, "/app/kv", etcd.WithCodec(kvCodec{}), etcd.WithCodec(nil)).Load(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if want := map[string]any{"name": "svc", "port": "80"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("Load = %#v, want %#v", got, want)
		}
		put(t, c, "/app/kvbad", "novalue")
		_, err = etcd.New(c, "/app/kvbad", etcd.WithCodec(kvCodec{})).Load(t.Context())
		if errorx.CodeOf(err) != errorx.InvalidArgument || !strings.Contains(err.Error(), "with kv codec") {
			t.Fatalf("err = %v, want InvalidArgument naming the kv codec", err)
		}
	})
}

func TestLoadPrefix(t *testing.T) {
	c := startEtcd(t)
	put(t, c, "/svc/", `{"name":"svc","db":{"primary":{"dsn":"old","pool":5}}}`)
	put(t, c, "/svc/db/primary", `{"dsn":"new"}`)
	put(t, c, "/svc/db//replica/", `{"dsn":"r"}`)
	put(t, c, "/svc/http/port", `8080`)
	put(t, c, "/svc/http/addr", `":80"`)
	put(t, c, "/svc/empty", ``)
	put(t, c, "/svc/name/first", `"replaces a scalar"`)
	put(t, c, "/other/x", `1`)

	got, err := etcd.New(c, "/svc/", etcd.WithPrefix()).Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"name": map[string]any{"first": "replaces a scalar"},
		"db": map[string]any{
			"primary": map[string]any{"dsn": "new", "pool": 5.0},
			"replica": map[string]any{"dsn": "r"},
		},
		"http":  map[string]any{"port": 8080.0, "addr": ":80"},
		"empty": map[string]any{},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Load = %#v, want %#v", got, want)
	}

	t.Run("no keys", func(t *testing.T) {
		got, err := etcd.New(c, "/nothing/", etcd.WithPrefix()).Load(t.Context())
		if err != nil || got == nil || len(got) != 0 {
			t.Fatalf("Load = %#v, %v; want empty map", got, err)
		}
	})
	t.Run("non-object at prefix", func(t *testing.T) {
		put(t, c, "/scalar/", `1`)
		_, err := etcd.New(c, "/scalar/", etcd.WithPrefix()).Load(t.Context())
		if errorx.CodeOf(err) != errorx.InvalidArgument || !strings.Contains(err.Error(), "must be an object") {
			t.Fatalf("err = %v, want InvalidArgument", err)
		}
	})
	t.Run("undecodable", func(t *testing.T) {
		put(t, c, "/broken/a", `{`)
		_, err := etcd.New(c, "/broken/", etcd.WithPrefix()).Load(t.Context())
		if errorx.CodeOf(err) != errorx.InvalidArgument || !strings.Contains(err.Error(), `"/broken/a"`) {
			t.Fatalf("err = %v, want InvalidArgument naming the key", err)
		}
	})
}

func TestLoadUnavailable(t *testing.T) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:1"},
		DialTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	// The client retries until the caller's deadline; the failure is then
	// a retryable deadline error that keeps the transport cause.
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	_, err = etcd.New(client, "/k").Load(ctx)
	if err == nil {
		t.Fatal("Load against an unreachable endpoint succeeded")
	}
	if !errorx.IsRetryable(err) || errorx.From(err).Reason != etcd.ReasonUnavailable {
		t.Fatalf("err = %v, want retryable %s", err, etcd.ReasonUnavailable)
	}
	if errors.Unwrap(err) == nil {
		t.Fatalf("err = %v, want the cause kept", err)
	}
}

// watch starts s.Watch in the background and returns a channel receiving
// one value per notification, plus a function that stops it and returns
// the result of Watch.
func watch(t *testing.T, s *etcd.Source) (<-chan struct{}, <-chan error, func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	notified := make(chan struct{}, 100)
	done := make(chan error, 1)
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		done <- s.Watch(ctx, func() {
			select {
			case notified <- struct{}{}:
			case <-ctx.Done():
			}
		})
	}()
	stop := func() error {
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(waitTimeout):
			t.Fatal("Watch did not return after cancel")
			return nil
		}
	}
	t.Cleanup(func() {
		cancel()
		select {
		case <-exited:
		case <-time.After(waitTimeout):
			t.Error("Watch did not return during cleanup")
		}
	})
	return notified, done, stop
}

// putUntilNotified writes key until a notification arrives, covering the
// window in which a watch started at the current revision is not yet
// established.
func putUntilNotified(t *testing.T, c *clientv3.Client, key string, notified <-chan struct{}) {
	t.Helper()
	deadline := time.After(waitTimeout)
	for i := 0; ; i++ {
		put(t, c, key, fmt.Sprintf(`{"v":%d}`, i))
		select {
		case <-notified:
			return
		case <-time.After(50 * time.Millisecond):
		case <-deadline:
			t.Fatal("no change notification")
		}
	}
}

func waitNotified(t *testing.T, notified <-chan struct{}) {
	t.Helper()
	select {
	case <-notified:
	case <-time.After(waitTimeout):
		t.Fatal("no change notification")
	}
}

func expectSilence(t *testing.T, notified <-chan struct{}, d time.Duration) {
	t.Helper()
	select {
	case <-notified:
		t.Fatal("unexpected change notification")
	case <-time.After(d):
	}
}

func TestWatchSingleKey(t *testing.T) {
	c := startEtcd(t)
	put(t, c, "/app/config", `{"v":0}`)
	s := etcd.New(c, "/app/config")
	notified, _, stop := watch(t, s)

	putUntilNotified(t, c, "/app/config", notified)
	// Other keys, including ones sharing the key as a prefix, are ignored.
	put(t, c, "/app/config2", `{}`)
	put(t, c, "/app/other", `{}`)
	expectSilence(t, notified, 200*time.Millisecond)
	// A deletion is a change.
	if _, err := c.Delete(t.Context(), "/app/config"); err != nil {
		t.Fatal(err)
	}
	waitNotified(t, notified)

	if err := stop(); err != nil {
		t.Fatalf("Watch = %v, want nil", err)
	}
}

func TestWatchPrefix(t *testing.T) {
	c := startEtcd(t)
	s := etcd.New(c, "/svc/", etcd.WithPrefix())
	notified, _, stop := watch(t, s)

	putUntilNotified(t, c, "/svc/a", notified)
	put(t, c, "/svc/deep/b", `1`)
	waitNotified(t, notified)
	put(t, c, "/elsewhere", `1`)
	expectSilence(t, notified, 200*time.Millisecond)

	// One transaction touching several keys is one change.
	_, err := c.Txn(t.Context()).Then(
		clientv3.OpPut("/svc/x", `1`),
		clientv3.OpPut("/svc/y", `2`),
	).Commit()
	if err != nil {
		t.Fatal(err)
	}
	waitNotified(t, notified)
	expectSilence(t, notified, 200*time.Millisecond)

	if err := stop(); err != nil {
		t.Fatalf("Watch = %v, want nil", err)
	}
}

func TestWatchResumesAfterLoad(t *testing.T) {
	c := startEtcd(t)
	put(t, c, "/app/config", `{"v":0}`)
	s := etcd.New(c, "/app/config")
	if _, err := s.Load(t.Context()); err != nil {
		t.Fatal(err)
	}
	// A change made between Load and Watch is not missed.
	put(t, c, "/app/config", `{"v":1}`)
	notified, _, stop := watch(t, s)
	waitNotified(t, notified)
	expectSilence(t, notified, 200*time.Millisecond)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}

func TestWatchCompacted(t *testing.T) {
	c := startEtcd(t)
	put(t, c, "/app/config", `{"v":0}`)
	s := etcd.New(c, "/app/config")
	if _, err := s.Load(t.Context()); err != nil {
		t.Fatal(err)
	}
	put(t, c, "/unrelated", `1`)
	resp, err := c.Put(t.Context(), "/unrelated", `2`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Compact(t.Context(), resp.Header.GetRevision(), clientv3.WithCompactPhysical()); err != nil {
		t.Fatal(err)
	}

	notified, _, stop := watch(t, s)
	// The compacted gap is reported once, then watching continues.
	waitNotified(t, notified)
	putUntilNotified(t, c, "/app/config", notified)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}

func TestWatchClientClosed(t *testing.T) {
	c := startEtcd(t)
	notified, done, _ := watch(t, etcd.New(c, "/app/config"))
	putUntilNotified(t, c, "/app/config", notified)
	c.Close()
	select {
	case err := <-done:
		if !errorx.IsRetryable(err) || errorx.CodeOf(err) != errorx.Unavailable {
			t.Fatalf("Watch = %v, want retryable Unavailable", err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("Watch did not return after the client was closed")
	}
}

func TestConfigWatchReloads(t *testing.T) {
	c := startEtcd(t)
	put(t, c, "/svc/http/addr", `":8000"`)
	put(t, c, "/svc/http/timeout", `"1s"`)
	put(t, c, "/svc/v", `0`)

	cfg, err := viper.New(t.Context(), []config.Source{etcd.New(c, "/svc/", etcd.WithPrefix())})
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Duration("http.timeout"); got != time.Second {
		t.Fatalf("http.timeout = %v", got)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reloaded := make(chan int, 100)
	done := make(chan error, 1)
	go func() {
		done <- cfg.Watch(ctx, func(c config.Config) { reloaded <- c.Int("v") })
	}()
	// The source resumes after the revision of the initial Load, so a
	// single write is enough.
	put(t, c, "/svc/http/timeout", `"2s"`)
	put(t, c, "/svc/v", `1`)
	deadline := time.After(waitTimeout)
	for {
		select {
		case v := <-reloaded:
			if v != 1 {
				continue
			}
			if got := cfg.Duration("http.timeout"); got != 2*time.Second {
				t.Fatalf("http.timeout = %v after reload", got)
			}
			if got := cfg.String("http.addr"); got != ":8000" {
				t.Fatalf("http.addr = %q after reload", got)
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatalf("Watch = %v", err)
			}
			return
		case <-deadline:
			t.Fatal("no reload")
		}
	}
}
