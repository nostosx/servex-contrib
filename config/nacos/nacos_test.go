package nacos_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nostosx/servex-contrib/config/nacos"
	"github.com/nostosx/servex/config"
	"github.com/nostosx/servex/errorx"
)

// waitTimeout bounds every wait on listener callbacks.
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
	f := newFakeClient()
	f.publish("app.json", "G", `{"http":{"addr":":8000","port":8080},"tags":["a"]}`)
	f.publish("blank.json", "G", " \n")
	f.publish("bad.json", "G", `{"a":`)
	f.publish("list.json", "G", `[1]`)
	f.publish("app.kv", "G", "name=svc\nport=80\n")
	f.publish("default.json", nacos.DefaultGroup, `{"group":"default"}`)

	t.Run("item", func(t *testing.T) {
		got, err := nacos.New(f, "app.json", "G").Load(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]any{"http": map[string]any{"addr": ":8000", "port": 8080.0}, "tags": []any{"a"}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("Load = %#v, want %#v", got, want)
		}
	})
	t.Run("default group", func(t *testing.T) {
		got, err := nacos.New(f, "default.json", "").Load(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if want := map[string]any{"group": "default"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("Load = %#v, want %#v", got, want)
		}
	})
	for _, id := range []string{"blank.json", "missing.json"} {
		t.Run("empty "+id, func(t *testing.T) {
			got, err := nacos.New(f, id, "G").Load(t.Context())
			if err != nil || got == nil || len(got) != 0 {
				t.Fatalf("Load = %#v, %v; want empty map", got, err)
			}
		})
	}
	for _, id := range []string{"bad.json", "list.json"} {
		t.Run("undecodable "+id, func(t *testing.T) {
			_, err := nacos.New(f, id, "G").Load(t.Context())
			if errorx.CodeOf(err) != errorx.InvalidArgument || errorx.IsRetryable(err) {
				t.Fatalf("err = %v, want non-retryable InvalidArgument", err)
			}
			if !strings.Contains(err.Error(), "nacos: decode G/"+id+" with json codec") {
				t.Fatalf("err = %v, want naming the item and codec", err)
			}
		})
	}
	t.Run("custom codec", func(t *testing.T) {
		got, err := nacos.New(f, "app.kv", "G", nacos.WithCodec(kvCodec{}), nacos.WithCodec(nil), nil).Load(t.Context())
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
		_, err := nacos.New(f, "app.json", "G").Load(ctx)
		if !errors.Is(err, context.Canceled) || errorx.CodeOf(err) != errorx.Canceled {
			t.Fatalf("err = %v, want Canceled", err)
		}
	})
	t.Run("empty data ID", func(t *testing.T) {
		_, err := nacos.New(f, "", "G").Load(t.Context())
		if errorx.CodeOf(err) != errorx.InvalidArgument {
			t.Fatalf("err = %v, want InvalidArgument", err)
		}
	})
	t.Run("client failure", func(t *testing.T) {
		g := newFakeClient()
		cause := errors.New("read config from both server and cache fail")
		g.getErr = cause
		_, err := nacos.New(g, "app.json", "G").Load(t.Context())
		if errorx.CodeOf(err) != errorx.Unavailable || !errorx.IsRetryable(err) || !errors.Is(err, cause) {
			t.Fatalf("err = %v, want retryable Unavailable wrapping the cause", err)
		}
		if !strings.Contains(err.Error(), "nacos: load G/app.json") {
			t.Fatalf("err = %v, want naming the item", err)
		}
	})
}

// watch starts s.Watch in the background and returns a channel receiving
// one value per notification, plus a function that stops it and returns
// the result of Watch.
func watch(t *testing.T, s *nacos.Source) (<-chan struct{}, func() error) {
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
	return notified, stop
}

func waitListening(t *testing.T, f *fakeClient, dataID, group string, want bool) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for f.listening(dataID, group) != want {
		if time.Now().After(deadline) {
			t.Fatalf("listening = %v, want %v", !want, want)
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

func TestWatch(t *testing.T) {
	f := newFakeClient()
	f.publish("app.json", "G", `{"v":0}`)
	notified, stop := watch(t, nacos.New(f, "app.json", "G"))
	waitListening(t, f, "app.json", "G", true)

	f.publish("app.json", "G", `{"v":1}`)
	waitNotified(t, notified)
	f.publish("other.json", "G", `{}`)
	f.publish("app.json", "H", `{}`)
	f.callbacks.Wait()
	select {
	case <-notified:
		t.Fatal("unexpected notification for another item")
	default:
	}

	if err := stop(); err != nil {
		t.Fatalf("Watch = %v, want nil", err)
	}
	if f.listening("app.json", "G") {
		t.Fatal("listener not canceled")
	}
	if listens, cancels := f.counts(); listens != 1 || cancels != 1 {
		t.Fatalf("ListenConfig/CancelListenConfig calls = %d/%d, want 1/1", listens, cancels)
	}
}

func TestWatchSharesOneListener(t *testing.T) {
	f := newFakeClient()
	s := nacos.New(f, "app.json", "")
	first, stopFirst := watch(t, s)
	waitListening(t, f, "app.json", nacos.DefaultGroup, true)
	second, stopSecond := watch(t, s)
	// Wait for the second Watch to register by observing a notification.
	deadline := time.After(waitTimeout)
	for registered := false; !registered; {
		f.publish("app.json", nacos.DefaultGroup, `{}`)
		waitNotified(t, first)
		select {
		case <-second:
			registered = true
		case <-time.After(10 * time.Millisecond):
		case <-deadline:
			t.Fatal("second Watch never notified")
		}
	}

	if err := stopFirst(); err != nil {
		t.Fatal(err)
	}
	// The listener survives while a Watch is still running.
	f.callbacks.Wait()
	for len(second) > 0 {
		<-second
	}
	f.publish("app.json", nacos.DefaultGroup, `{"v":2}`)
	waitNotified(t, second)
	if err := stopSecond(); err != nil {
		t.Fatal(err)
	}
	if listens, cancels := f.counts(); listens != 1 || cancels != 1 {
		t.Fatalf("ListenConfig/CancelListenConfig calls = %d/%d, want 1/1", listens, cancels)
	}
	if f.listening("app.json", nacos.DefaultGroup) {
		t.Fatal("listener not canceled")
	}
}

func TestWatchNoNotifyAfterReturn(t *testing.T) {
	f := newFakeClient()
	s := nacos.New(f, "app.json", "G")
	ctx, cancel := context.WithCancel(t.Context())
	var returned, late atomic.Bool
	done := make(chan error, 1)
	go func() {
		done <- s.Watch(ctx, func() {
			if returned.Load() {
				late.Store(true)
			}
		})
	}()
	waitListening(t, f, "app.json", "G", true)
	// Capture the listener and call it after Watch has returned, as the
	// client may do with a callback already in flight.
	f.mu.Lock()
	listener := f.listeners[itemKey("app.json", "G")]
	f.mu.Unlock()
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	returned.Store(true)
	listener("public", "G", "app.json", "{}")
	if late.Load() {
		t.Fatal("notify called after Watch returned")
	}
}

func TestWatchErrors(t *testing.T) {
	t.Run("listen failure", func(t *testing.T) {
		f := newFakeClient()
		cause := errors.New("[checkConfigInfo.GetClientConfig] failed")
		f.listenErr = cause
		err := nacos.New(f, "app.json", "G").Watch(t.Context(), func() {})
		if errorx.CodeOf(err) != errorx.Unavailable || !errors.Is(err, cause) {
			t.Fatalf("Watch = %v, want Unavailable wrapping the cause", err)
		}
		// A failed registration leaves no watcher behind: a later Watch
		// registers again.
		f.listenErr = nil
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- nacos.New(f, "app.json", "G").Watch(ctx, func() {}) }()
		waitListening(t, f, "app.json", "G", true)
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
	t.Run("empty data ID", func(t *testing.T) {
		err := nacos.New(newFakeClient(), "", "G").Watch(t.Context(), func() {})
		if errorx.CodeOf(err) != errorx.InvalidArgument {
			t.Fatalf("Watch = %v, want InvalidArgument", err)
		}
	})
	t.Run("canceled", func(t *testing.T) {
		f := newFakeClient()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := nacos.New(f, "app.json", "G").Watch(ctx, func() {}); err != nil {
			t.Fatalf("Watch = %v, want nil", err)
		}
		if listens, _ := f.counts(); listens != 0 {
			t.Fatal("canceled Watch registered a listener")
		}
	})
}

func TestConfigWatchReloads(t *testing.T) {
	f := newFakeClient()
	f.publish("app.json", "G", `{"http":{"addr":":8000","timeout":"1s"},"v":0}`)
	cfg, err := config.New(t.Context(), []config.Source{nacos.New(f, "app.json", "G")})
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
	waitListening(t, f, "app.json", "G", true)
	f.publish("app.json", "G", `{"http":{"addr":":8000","timeout":"2s"},"v":1}`)
	select {
	case v := <-reloaded:
		if v != 1 {
			t.Fatalf("v = %d after reload", v)
		}
		if got := cfg.Duration("http.timeout"); got != 2*time.Second {
			t.Fatalf("http.timeout = %v after reload", got)
		}
	case <-time.After(waitTimeout):
		t.Fatal("no reload")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Watch = %v", err)
	}
}
