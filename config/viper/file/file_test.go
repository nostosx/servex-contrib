package file_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nostosx/servex-contrib/config/viper"
	"github.com/nostosx/servex-contrib/config/viper/file"
	"github.com/nostosx/servex/config"
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

// waitTimeout bounds every wait on real file system polling.
const waitTimeout = 5 * time.Second

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestLoadJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.json")
	writeFile(t, path, `{"http":{"addr":":8000","port":8080},"tags":["a"]}`)
	got, err := file.New(path).Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"http": map[string]any{"addr": ":8000", "port": 8080.0},
		"tags": []any{"a"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Load = %#v, want %#v", got, want)
	}
}

func TestLoadEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.json")
	writeFile(t, path, " \n\t")
	got, err := file.New(path).Load(t.Context())
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("Load = %#v, %v; want empty map", got, err)
	}
}

func TestLoadErrors(t *testing.T) {
	dir := t.TempDir()
	invalid := filepath.Join(dir, "bad.json")
	writeFile(t, invalid, `{"a":`)
	yaml := filepath.Join(dir, "app.unsupported")
	writeFile(t, yaml, "a: 1")
	plain := filepath.Join(dir, "app.json")
	writeFile(t, plain, `{}`)

	canceled, cancel := context.WithCancel(t.Context())
	cancel()

	tests := []struct {
		name   string
		src    *file.Source
		ctx    context.Context
		want   string
		target error
	}{
		{"missing file", file.New(filepath.Join(dir, "missing.json")), t.Context(), "file: load ", fs.ErrNotExist},
		{"invalid json", file.New(invalid), t.Context(), "file: decode " + invalid + ": ", nil},
		{"unknown extension", file.New(yaml), t.Context(), `no codec for extension ".unsupported"`, nil},
		{"removed codec", file.New(plain, file.WithCodec(".json", nil)), t.Context(), `no codec for extension ".json"`, nil},
		{"canceled", file.New(plain), canceled, "file: load ", context.Canceled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.src.Load(tt.ctx)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want containing %q", err, tt.want)
			}
			if tt.target != nil && !errors.Is(err, tt.target) {
				t.Fatalf("err = %v, want wrapping %v", err, tt.target)
			}
		})
	}
}

func TestWithCodec(t *testing.T) {
	dir := t.TempDir()
	tests := []struct {
		name string
		file string
		ext  string
	}{
		{"with dot", "app.kv", ".kv"},
		{"without dot", "app.kv", "kv"},
		{"case insensitive", "APP.KV", ".Kv"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(dir, tt.file)
			writeFile(t, path, "name=svc\nport=80\n")
			got, err := file.New(path, file.WithCodec(tt.ext, kvCodec{})).Load(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if want := map[string]any{"name": "svc", "port": "80"}; !reflect.DeepEqual(got, want) {
				t.Fatalf("Load = %#v, want %#v", got, want)
			}
		})
	}
	// Adding a codec keeps the JSON default.
	path := filepath.Join(dir, "x.json")
	writeFile(t, path, `{"a":1}`)
	if _, err := file.New(path, file.WithCodec(".kv", kvCodec{})).Load(t.Context()); err != nil {
		t.Fatalf("json after WithCodec: %v", err)
	}
}

// watch starts s.Watch in the background and returns a channel receiving
// one value per notification, plus a function that stops it and returns
// the result of Watch.
func watch(t *testing.T, s *file.Source) (<-chan struct{}, func() error) {
	t.Helper()
	notified := make(chan struct{}, 100)
	stop := startWatch(t, func(ctx context.Context) error {
		return s.Watch(ctx, func() {
			select {
			case notified <- struct{}{}:
			case <-ctx.Done():
			}
		})
	})
	return notified, stop
}

// startWatch owns cancellation and joins the watcher on every exit path.
func startWatch(t *testing.T, watch func(context.Context) error) func() error {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	var err error
	go func() {
		err = watch(ctx)
		close(done)
	}()
	stop := func() error {
		cancel()
		select {
		case <-done:
			return err
		case <-time.After(waitTimeout):
			return errors.New("Watch did not return after cancel")
		}
	}
	t.Cleanup(func() {
		if err := stop(); err != nil {
			t.Error(err)
		}
	})
	return stop
}

// touchUntilNotified rewrites path with a new size and modification time
// until a notification arrives. Repeating covers the window in which Watch
// may not yet have taken its initial snapshot.
func touchUntilNotified(t *testing.T, path string, notified <-chan struct{}) {
	t.Helper()
	deadline := time.After(waitTimeout)
	base := time.Now().Add(time.Hour)
	for i := 1; ; i++ {
		writeFile(t, path, fmt.Sprintf(`{"v":%d,"pad":%q}`, i, strings.Repeat("x", i)))
		mtime := base.Add(time.Duration(i) * time.Second)
		if err := os.Chtimes(path, mtime, mtime); err != nil {
			t.Fatal(err)
		}
		select {
		case <-notified:
			return
		case <-time.After(20 * time.Millisecond):
		case <-deadline:
			t.Fatal("no change notification")
		}
	}
}

func TestWatchDetectsChanges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.json")
	writeFile(t, path, `{"v":0}`)
	s := file.New(path, file.WithPollInterval(5*time.Millisecond))
	notified, stop := watch(t, s)

	touchUntilNotified(t, path, notified)

	// Same size, different modification time.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	settle(t, notified)
	mtime := info.ModTime().Add(time.Minute)
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	waitNotified(t, notified)

	// Removal is not reported; reappearance is.
	settle(t, notified)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	expectSilence(t, notified, 50*time.Millisecond)
	writeFile(t, path, `{"v":"back"}`)
	waitNotified(t, notified)

	if err := stop(); err != nil {
		t.Fatalf("Watch = %v, want nil", err)
	}
}

func TestWatchUnchangedFileIsSilent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.json")
	writeFile(t, path, `{}`)
	notified, stop := watch(t, file.New(path, file.WithPollInterval(time.Millisecond)))
	expectSilence(t, notified, 50*time.Millisecond)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}

func TestConfigWatchReloadsFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.json")
	writeFile(t, path, `{"http":{"addr":":8000","timeout":"1s"},"v":0}`)
	t.Setenv("SVXFILETEST_HTTP__ADDR", ":9000")

	c, err := viper.New(t.Context(), []config.Source{file.New(path, file.WithPollInterval(5*time.Millisecond)), viper.Env("SVXFILETEST_")})
	if err != nil {
		t.Fatal(err)
	}
	if got := c.String("http.addr"); got != ":9000" {
		t.Fatalf("env must override file: http.addr = %q", got)
	}
	if got := c.Duration("http.timeout"); got != time.Second {
		t.Fatalf("http.timeout = %v", got)
	}

	reloaded := make(chan int, 100)
	stop := startWatch(t, func(ctx context.Context) error {
		return c.Watch(ctx, func(c config.Config) {
			select {
			case reloaded <- c.Int("v"):
			case <-ctx.Done():
			}
		})
	})

	deadline := time.After(waitTimeout)
	base := time.Now().Add(time.Hour)
	for i := 1; ; i++ {
		// Publish a complete revision; an in-place truncate/write may expose a
		// valid empty configuration before the new contents are written.
		replacement := filepath.Join(filepath.Dir(path), "app.next")
		writeFile(t, replacement, fmt.Sprintf(`{"http":{"addr":":8000","timeout":"2s"},"v":%d}`, i))
		mtime := base.Add(time.Duration(i) * time.Second)
		if err := os.Chtimes(replacement, mtime, mtime); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(replacement, path); err != nil {
			t.Fatal(err)
		}
		select {
		case v := <-reloaded:
			if v < 1 {
				t.Fatalf("fn saw stale v = %d", v)
			}
			if got := c.Duration("http.timeout"); got != 2*time.Second {
				t.Fatalf("http.timeout = %v after reload", got)
			}
			if got := c.String("http.addr"); got != ":9000" {
				t.Fatalf("http.addr = %q after reload", got)
			}
			if err := stop(); err != nil {
				t.Fatalf("Watch = %v", err)
			}
			return
		case <-time.After(20 * time.Millisecond):
		case <-deadline:
			t.Fatal("no reload")
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

// settle discards notifications until none arrive for several poll
// intervals, so later assertions are not confused by stale ones.
func settle(t *testing.T, c <-chan struct{}) {
	t.Helper()
	quiet := time.NewTimer(30 * time.Millisecond)
	defer quiet.Stop()
	deadline := time.NewTimer(waitTimeout)
	defer deadline.Stop()
	for {
		select {
		case _, ok := <-c:
			if !ok {
				t.Fatal("notification channel closed")
			}
			quiet.Reset(30 * time.Millisecond)
		case <-quiet.C:
			return
		case <-deadline.C:
			t.Fatal("notifications did not settle")
		}
	}
}
