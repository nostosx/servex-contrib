package consul_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"

	"github.com/nostosx/servex-contrib/config/consul"
	"github.com/nostosx/servex/config"
	"github.com/nostosx/servex/errorx"
)

// waitTimeout bounds every wait on the fake server.
const waitTimeout = 5 * time.Second

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

func TestLoad(t *testing.T) {
	f := newFakeConsul(t)
	c := f.client(t)
	f.put("app/config", `{"http":{"addr":":8000","port":8080},"tags":["a"]}`)
	f.put("app/blank", " \n")
	f.put("app/bad", `{"a":`)
	f.put("app/list", `[1]`)
	f.put("app/kv", "name=svc\nport=80\n")

	t.Run("single key", func(t *testing.T) {
		got, err := consul.New(c, "app/config").Load(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]any{"http": map[string]any{"addr": ":8000", "port": 8080.0}, "tags": []any{"a"}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("Load = %#v, want %#v", got, want)
		}
	})
	t.Run("blank value", func(t *testing.T) {
		got, err := consul.New(c, "app/blank").Load(t.Context())
		if err != nil || got == nil || len(got) != 0 {
			t.Fatalf("Load = %#v, %v; want empty map", got, err)
		}
	})
	t.Run("missing key", func(t *testing.T) {
		_, err := consul.New(c, "app/missing").Load(t.Context())
		if errorx.CodeOf(err) != errorx.NotFound || !strings.Contains(err.Error(), `consul: load "app/missing"`) {
			t.Fatalf("err = %v, want NotFound", err)
		}
	})
	for _, key := range []string{"app/bad", "app/list"} {
		t.Run("undecodable "+key, func(t *testing.T) {
			_, err := consul.New(c, key).Load(t.Context())
			if errorx.CodeOf(err) != errorx.InvalidArgument || errorx.IsRetryable(err) {
				t.Fatalf("err = %v, want non-retryable InvalidArgument", err)
			}
			if !strings.Contains(err.Error(), "with json codec") {
				t.Fatalf("err = %v, want naming the codec", err)
			}
		})
	}
	t.Run("custom codec", func(t *testing.T) {
		got, err := consul.New(c, "app/kv", consul.WithCodec(kvCodec{}), consul.WithCodec(nil)).Load(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if want := map[string]any{"name": "svc", "port": "80"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("Load = %#v, want %#v", got, want)
		}
	})
	t.Run("canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := consul.New(c, "app/config").Load(ctx)
		if !errors.Is(err, context.Canceled) || errorx.CodeOf(err) != errorx.Canceled {
			t.Fatalf("err = %v, want Canceled", err)
		}
	})
}

func TestLoadPrefix(t *testing.T) {
	f := newFakeConsul(t)
	c := f.client(t)
	f.folder("svc/")
	f.put("svc/name/first", `"replaces a scalar"`)
	f.put("svc/db//replica/", `{"dsn":"r"}`)
	f.put("svc/db/primary", `{"dsn":"new"}`)
	f.put("svc/http/port", `8080`)
	f.put("svc/http/addr", `":80"`)
	f.folder("svc/empty/")
	f.put("svc", `{"ignored":"sibling key without the slash"}`)
	f.put("other/x", `1`)

	got, err := consul.New(c, "svc/", consul.WithPrefix()).Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"name": map[string]any{"first": "replaces a scalar"},
		"db": map[string]any{
			"primary": map[string]any{"dsn": "new"},
			"replica": map[string]any{"dsn": "r"},
		},
		"http":  map[string]any{"port": 8080.0, "addr": ":80"},
		"empty": map[string]any{},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Load = %#v, want %#v", got, want)
	}

	t.Run("object at prefix merged with deeper keys", func(t *testing.T) {
		f.put("root/", `{"name":"svc","db":{"primary":{"dsn":"old","pool":5}}}`)
		f.put("root/db/primary", `{"dsn":"new"}`)
		got, err := consul.New(c, "root/", consul.WithPrefix()).Load(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]any{"name": "svc", "db": map[string]any{"primary": map[string]any{"dsn": "new", "pool": 5.0}}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("Load = %#v, want %#v", got, want)
		}
	})
	t.Run("no keys", func(t *testing.T) {
		got, err := consul.New(c, "nothing/", consul.WithPrefix()).Load(t.Context())
		if err != nil || got == nil || len(got) != 0 {
			t.Fatalf("Load = %#v, %v; want empty map", got, err)
		}
	})
	t.Run("non-object at prefix", func(t *testing.T) {
		f.put("scalar/", `1`)
		_, err := consul.New(c, "scalar/", consul.WithPrefix()).Load(t.Context())
		if errorx.CodeOf(err) != errorx.InvalidArgument || !strings.Contains(err.Error(), "must be an object") {
			t.Fatalf("err = %v, want InvalidArgument", err)
		}
	})
	t.Run("undecodable", func(t *testing.T) {
		f.put("broken/a", `{`)
		_, err := consul.New(c, "broken/", consul.WithPrefix()).Load(t.Context())
		if errorx.CodeOf(err) != errorx.InvalidArgument || !strings.Contains(err.Error(), `"broken/a"`) {
			t.Fatalf("err = %v, want InvalidArgument naming the key", err)
		}
	})
}

func TestLoadErrors(t *testing.T) {
	f := newFakeConsul(t)
	c := f.client(t)
	f.put("k", `{}`)
	tests := []struct {
		status    int
		code      errorx.Code
		retryable bool
	}{
		{http.StatusBadRequest, errorx.InvalidArgument, false},
		{http.StatusUnauthorized, errorx.Unauthenticated, false},
		{http.StatusForbidden, errorx.PermissionDenied, false},
		{http.StatusTooManyRequests, errorx.ResourceExhausted, true},
		{http.StatusInternalServerError, errorx.Unavailable, true},
		{http.StatusServiceUnavailable, errorx.Unavailable, true},
	}
	for _, tt := range tests {
		t.Run(http.StatusText(tt.status), func(t *testing.T) {
			f.failNext(tt.status)
			_, err := consul.New(c, "k").Load(t.Context())
			if errorx.CodeOf(err) != tt.code || errorx.IsRetryable(err) != tt.retryable {
				t.Fatalf("err = %v, want %v retryable=%v", err, tt.code, tt.retryable)
			}
			if _, ok := errors.AsType[api.StatusError](err); !ok {
				t.Fatalf("err = %v, want the status error kept as cause", err)
			}
		})
	}
	t.Run("unreachable", func(t *testing.T) {
		dead, err := api.NewClient(&api.Config{Address: "127.0.0.1:1", Scheme: "http"})
		if err != nil {
			t.Fatal(err)
		}
		_, err = consul.New(dead, "k").Load(t.Context())
		if errorx.CodeOf(err) != errorx.Unavailable || !errorx.IsRetryable(err) || errors.Unwrap(err) == nil {
			t.Fatalf("err = %v, want retryable Unavailable with cause", err)
		}
	})
}

// watch starts s.Watch in the background and returns a channel receiving
// one value per notification, a channel receiving the result of Watch and a
// function that stops it and returns that result.
func watch(t *testing.T, s *consul.Source) (<-chan struct{}, <-chan error, func() error) {
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

// waitBlocked waits until n queries are blocking on the fake server, which
// shows that Watch has taken its baseline and is waiting for changes.
func waitBlocked(t *testing.T, f *fakeConsul, n int) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for {
		if _, blocked := f.stats(); blocked >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("Watch never issued a blocking query")
		}
		time.Sleep(time.Millisecond)
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

// expectSilence checks that no notification arrives while the watcher
// handles the current state: it waits for Watch to block again.
func expectSilence(t *testing.T, f *fakeConsul, notified <-chan struct{}) {
	t.Helper()
	waitBlocked(t, f, 1)
	select {
	case <-notified:
		t.Fatal("unexpected change notification")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestWatchSingleKey(t *testing.T) {
	f := newFakeConsul(t)
	c := f.client(t)
	f.put("app/config", `{"v":0}`)
	notified, _, stop := watch(t, consul.New(c, "app/config"))
	waitBlocked(t, f, 1)

	f.put("app/config", `{"v":1}`)
	waitNotified(t, notified)
	// Writing the same value is a change, as in etcd.
	f.put("app/config", `{"v":1}`)
	waitNotified(t, notified)
	// Writes to other keys wake the blocking query but are not reported.
	f.put("app/config2", `{}`)
	f.put("app/other", `{}`)
	expectSilence(t, f, notified)
	f.remove("app/config")
	waitNotified(t, notified)
	// The key reappearing is a change as well.
	f.put("app/config", `{"v":2}`)
	waitNotified(t, notified)

	if err := stop(); err != nil {
		t.Fatalf("Watch = %v, want nil", err)
	}
}

func TestWatchPrefix(t *testing.T) {
	f := newFakeConsul(t)
	c := f.client(t)
	notified, _, stop := watch(t, consul.New(c, "svc/", consul.WithPrefix()))
	waitBlocked(t, f, 1)

	f.put("svc/a", `1`)
	waitNotified(t, notified)
	f.put("svc/deep/b", `1`)
	waitNotified(t, notified)
	f.put("elsewhere", `1`)
	expectSilence(t, f, notified)
	f.remove("svc/a")
	waitNotified(t, notified)

	if err := stop(); err != nil {
		t.Fatalf("Watch = %v, want nil", err)
	}
}

func TestWatchResumesAfterLoad(t *testing.T) {
	f := newFakeConsul(t)
	c := f.client(t)
	f.put("app/config", `{"v":0}`)
	s := consul.New(c, "app/config")
	if _, err := s.Load(t.Context()); err != nil {
		t.Fatal(err)
	}
	// A change made between Load and Watch is not missed.
	f.put("app/config", `{"v":1}`)
	notified, _, stop := watch(t, s)
	waitNotified(t, notified)
	expectSilence(t, f, notified)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}

func TestWatchWaitTimeExpires(t *testing.T) {
	f := newFakeConsul(t)
	c := f.client(t)
	f.put("app/config", `{}`)
	notified, _, stop := watch(t, consul.New(c, "app/config", consul.WithWaitTime(10*time.Millisecond)))
	// Expired blocking queries return the unchanged state and are retried
	// without notifications.
	deadline := time.Now().Add(waitTimeout)
	for queries, _ := f.stats(); queries < 5; queries, _ = f.stats() {
		if time.Now().After(deadline) {
			t.Fatalf("only %d queries issued", queries)
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-notified:
		t.Fatal("unexpected change notification")
	default:
	}
	f.put("app/config", `{"v":1}`)
	waitNotified(t, notified)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}

func TestWatchIndexReset(t *testing.T) {
	f := newFakeConsul(t)
	c := f.client(t)
	f.put("app/config", `{}`)
	f.setIndex(100)
	notified, _, stop := watch(t, consul.New(c, "app/config"))
	waitBlocked(t, f, 1)

	// The index goes backwards, as after a snapshot restore. The watcher
	// must start over instead of blocking until index 100 is reached again.
	f.setIndex(5)
	expectSilence(t, f, notified)
	f.put("app/config", `{"v":1}`)
	waitNotified(t, notified)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}

func TestWatchRetriesTransientErrors(t *testing.T) {
	f := newFakeConsul(t)
	c := f.client(t)
	f.put("app/config", `{}`)
	s := consul.New(c, "app/config", consul.WithRetryDelay(time.Millisecond, 4*time.Millisecond))
	notified, _, stop := watch(t, s)
	waitBlocked(t, f, 1)

	f.failNext(http.StatusInternalServerError, http.StatusServiceUnavailable, http.StatusTooManyRequests, http.StatusBadGateway)
	f.put("app/config", `{"v":1}`)
	waitNotified(t, notified)
	if err := stop(); err != nil {
		t.Fatalf("Watch = %v, want nil", err)
	}
}

func TestWatchStopsOnPermanentError(t *testing.T) {
	f := newFakeConsul(t)
	c := f.client(t)
	f.put("app/config", `{}`)
	f.failNext(http.StatusForbidden)
	_, done, _ := watch(t, consul.New(c, "app/config"))
	select {
	case err := <-done:
		if errorx.CodeOf(err) != errorx.PermissionDenied || !strings.Contains(err.Error(), `consul: watch "app/config"`) {
			t.Fatalf("Watch = %v, want PermissionDenied", err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("Watch did not return")
	}
}

func TestWatchCanceledWhileRetrying(t *testing.T) {
	f := newFakeConsul(t)
	c := f.client(t)
	f.failNext(http.StatusInternalServerError)
	s := consul.New(c, "app/config", consul.WithRetryDelay(time.Hour, time.Hour))
	_, _, stop := watch(t, s)
	deadline := time.Now().Add(waitTimeout)
	for queries, _ := f.stats(); queries < 1; queries, _ = f.stats() {
		if time.Now().After(deadline) {
			t.Fatal("no query issued")
		}
		time.Sleep(time.Millisecond)
	}
	if err := stop(); err != nil {
		t.Fatalf("Watch = %v, want nil", err)
	}
}

func TestConfigWatchReloads(t *testing.T) {
	f := newFakeConsul(t)
	c := f.client(t)
	f.put("svc/http/addr", `":8000"`)
	f.put("svc/http/timeout", `"1s"`)
	f.put("svc/v", `0`)

	cfg, err := config.New(t.Context(), []config.Source{consul.New(c, "svc/", consul.WithPrefix())})
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
		done <- cfg.Watch(ctx, func(c *config.Config) { reloaded <- c.Int("v") })
	}()
	f.put("svc/http/timeout", `"2s"`)
	f.put("svc/v", `1`)
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
