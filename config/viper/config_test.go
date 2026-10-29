package viper_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nostosx/servex-contrib/config/viper"
	"github.com/nostosx/servex/config"
)

// mapSource is a mutable in-memory Source.
type mapSource struct {
	mu  sync.Mutex
	m   map[string]any
	err error
}

func (s *mapSource) Load(context.Context) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m, s.err
}

func (s *mapSource) set(m map[string]any, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m, s.err = m, err
}

// watchSource is a Watchable whose changes are triggered by the test.
type watchSource struct {
	mapSource
	changes chan struct{}
	err     error // returned by Watch after the first change when set
}

func (s *watchSource) Watch(ctx context.Context, notify func()) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-s.changes:
			if s.err != nil {
				return s.err
			}
			notify()
		}
	}
}

func mustNew(t *testing.T, sources ...config.Source) config.Config {
	t.Helper()
	c, err := viper.New(t.Context(), sources)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func src(m map[string]any) *mapSource { return &mapSource{m: m} }

type sourceFunc func(context.Context) (map[string]any, error)

func (f sourceFunc) Load(ctx context.Context) (map[string]any, error) { return f(ctx) }

func TestNewWithValidatorsRejectsInitialCandidate(t *testing.T) {
	t.Parallel()
	invalid := errors.New("port must be positive")
	var candidate config.Reader
	c, err := viper.New(t.Context(), []config.Source{src(map[string]any{"port": -1})},
		viper.WithValidator(func(_ context.Context, s config.Reader) error {
			candidate = s
			if s.Int("port") <= 0 {
				return invalid
			}
			return nil
		}))
	if c != nil || !errors.Is(err, invalid) || !strings.Contains(err.Error(), "config: validate 0") {
		t.Fatalf("New = %v, %v", c, err)
	}
	if candidate == nil || candidate.Int("port") != -1 {
		t.Fatalf("validator did not receive the rejected candidate: %v", candidate)
	}
}

func TestNewWithValidatorsValidatesMergedTreeInOrder(t *testing.T) {
	t.Parallel()
	type key struct{}
	ctx := context.WithValue(t.Context(), key{}, "request")
	base := src(map[string]any{"server": map[string]any{"port": -1, "host": "localhost"}})
	override := src(map[string]any{"SERVER": map[string]any{"PORT": "8080"}})
	sources := []config.Source{base, override}
	var order []int
	var retained config.Reader
	c, err := viper.New(ctx, sources, nil, viper.WithValidator(nil),
		viper.WithValidator(func(got context.Context, s config.Reader) error {
			order = append(order, 1)
			retained = s
			if got.Value(key{}) != "request" {
				t.Error("validator did not receive the load context")
			}
			var server struct {
				Host string
				Port int
			}
			if err := s.Unmarshal("server", &server); err != nil {
				return err
			}
			if server.Host != "localhost" || server.Port != 8080 {
				return errors.New("validator did not see the repaired merged tree")
			}
			return nil
		}),
		viper.WithValidator(func(context.Context, config.Reader) error {
			order = append(order, 2)
			return nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(order, []int{1, 2}) {
		t.Fatalf("validator order = %v", order)
	}
	// Changing the caller's slice must not replace a Config source.
	sources[0] = src(map[string]any{"server": map[string]any{"host": "other"}})
	if err := c.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	if c.String("server.host") != "localhost" || retained.Int("server.port") != 8080 {
		t.Fatal("configuration sources or retained candidate changed")
	}
	if !reflect.DeepEqual(order, []int{1, 2, 1, 2}) {
		t.Fatalf("validator order after reload = %v", order)
	}
}

func TestReloadRejectsCandidateAndRecovers(t *testing.T) {
	t.Parallel()
	invalid := errors.New("invalid revision")
	s := src(map[string]any{"revision": 1})
	var candidates []config.Reader
	var after int
	c, err := viper.New(t.Context(), []config.Source{s},
		viper.WithValidator(func(_ context.Context, candidate config.Reader) error {
			candidates = append(candidates, candidate)
			// Reads and mutation of copies cannot alter validation or publication.
			v, _ := candidate.Value("")
			v.(map[string]any)["revision"] = 99
			if candidate.Int("revision") == 2 {
				return invalid
			}
			return nil
		}),
		viper.WithValidator(func(context.Context, config.Reader) error {
			after++
			return nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	before := c.Snapshot()
	s.set(map[string]any{"revision": 2}, nil)
	if err := c.Reload(t.Context()); !errors.Is(err, invalid) {
		t.Fatalf("Reload = %v, want validation error", err)
	}
	if after != 1 || c.Int("revision") != 1 || before.Int("revision") != 1 {
		t.Fatal("rejected candidate was published or later validator ran")
	}
	s.set(map[string]any{"revision": 3}, nil)
	if err := c.Reload(t.Context()); err != nil {
		t.Fatal(err)
	}
	if after != 2 || c.Int("revision") != 3 || before.Int("revision") != 1 {
		t.Fatal("reload did not recover or changed the pinned revision")
	}
	for i, candidate := range candidates {
		if candidate.Int("revision") != i+1 {
			t.Errorf("retained candidate %d changed", i)
		}
	}
}

func TestReloadCancellationPreservesPublishedValues(t *testing.T) {
	for _, stage := range []string{"before load", "last source", "last validator"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var loading bool
			s := sourceFunc(func(context.Context) (map[string]any, error) {
				if loading && stage == "last source" {
					cancel() // Deliberately return success despite cancellation.
				}
				if loading {
					return map[string]any{"v": 2}, nil
				}
				return map[string]any{"v": 1}, nil
			})
			c, err := viper.New(t.Context(), []config.Source{s},
				viper.WithValidator(func(context.Context, config.Reader) error {
					if loading && stage == "last validator" {
						cancel()
					}
					return nil
				}))
			if err != nil {
				t.Fatal(err)
			}
			before := c.Snapshot()
			loading = true
			if stage == "before load" {
				cancel()
			}
			if err := c.Reload(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("Reload = %v, want cancellation", err)
			}
			if c.Int("v") != 1 || before.Int("v") != 1 {
				t.Fatal("canceled reload changed published values")
			}
		})
	}
	t.Run("no sources", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if c, err := viper.New(ctx, nil); c != nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("New = %v, %v", c, err)
		}
	})
}

func TestReloadValidationDoesNotBlockReaders(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var loads atomic.Int32
		release := make(chan struct{})
		s := sourceFunc(func(context.Context) (map[string]any, error) {
			v := int(loads.Add(1))
			return map[string]any{"a": v, "b": v}, nil
		})
		var c config.Config
		c, err := viper.New(t.Context(), []config.Source{s},
			viper.WithValidator(func(_ context.Context, candidate config.Reader) error {
				if candidate.Int("a") == 2 {
					if c.Int("a") != 1 {
						t.Error("validator read a candidate through the live Config")
					}
					<-release
				}
				return nil
			}))
		if err != nil {
			t.Fatal(err)
		}
		before := c.Snapshot()
		first := make(chan error, 1)
		go func() { first <- c.Reload(t.Context()) }()
		synctest.Wait() // The candidate is waiting for validation.
		var readers sync.WaitGroup
		var reads atomic.Int32
		for range 4 {
			readers.Go(func() {
				snapshot := c.Snapshot()
				var out struct{ A, B int }
				if err := c.Unmarshal("", &out); err != nil || out.A != 1 || out.B != 1 {
					t.Errorf("read during validation = %+v, %v", out, err)
				}
				if c.Int("a") != 1 || snapshot.Int("a") != 1 || snapshot.Int("b") != 1 {
					t.Error("read during validation observed unpublished candidate")
				}
				reads.Add(1)
			})
		}
		synctest.Wait()
		if reads.Load() != 4 || loads.Load() != 2 {
			t.Errorf("readers completed = %d, loads = %d; want 4, 2", reads.Load(), loads.Load())
		}
		// Start a competing reload after Wait: a mutex waiter is not durably
		// blocked for synctest, so another Wait here would stall the bubble.
		second := make(chan error, 1)
		go func() { second <- c.Reload(t.Context()) }()
		close(release)
		readers.Wait()
		if err := <-first; err != nil {
			t.Fatal(err)
		}
		if err := <-second; err != nil {
			t.Fatal(err)
		}
		if c.Int("a") != 3 || c.Int("b") != 3 || before.Int("a") != 1 {
			t.Fatal("reloads did not publish in order or changed the pinned snapshot")
		}
	})
}

func TestNewMerge(t *testing.T) {
	base := src(map[string]any{
		"http": map[string]any{"addr": ":8000", "timeout": "1s"},
		"name": "base",
		"list": []any{"a", "b"},
		"db":   "scalar",
		"Mode": "dev",
	})
	override := src(map[string]any{
		"http": map[string]any{"addr": ":9000", "tls": true},
		"list": []any{"c"},
		"db":   map[string]any{"dsn": "x"},
		"mode": "prod",
	})
	c := mustNew(t, base, override)

	got, _ := c.Value("")
	want := map[string]any{
		"http": map[string]any{"addr": ":9000", "timeout": "1s", "tls": true},
		"name": "base",
		"list": []any{"c"},
		"db":   map[string]any{"dsn": "x"},
		"mode": "prod",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("merged tree = %#v, want %#v", got, want)
	}
	// The merged tree must not alias the sources.
	base.m["http"].(map[string]any)["addr"] = "mutated"
	if s := c.String("http.addr"); s != ":9000" {
		t.Fatalf("http.addr = %q after mutating source", s)
	}
	if s := c.String("http.timeout"); s != "1s" {
		t.Fatalf("http.timeout = %q", s)
	}
}

func TestNewMergeMapReplacedByScalar(t *testing.T) {
	c := mustNew(t,
		src(map[string]any{"a": map[string]any{"b": 1.0}}),
		src(map[string]any{"a": "flat"}),
	)
	if got := c.String("a"); got != "flat" {
		t.Fatalf("a = %q, want flat", got)
	}
	if _, ok := c.Value("a.b"); ok {
		t.Fatal("a.b still present")
	}
}

func TestNewNormalizesTypedContainers(t *testing.T) {
	c := mustNew(t, src(map[string]any{
		"ports":  []int{1, 2},
		"labels": map[string]string{"k": "v"},
		"ids":    map[int]bool{7: true},
		"ptr":    new(42),
	}))
	tests := map[string]any{
		"ports":  []any{1, 2},
		"labels": map[string]any{"k": "v"},
		"ids":    map[string]any{"7": true},
		"ptr":    42,
	}
	for path, want := range tests {
		if got, _ := c.Value(path); !reflect.DeepEqual(got, want) {
			t.Errorf("Value(%q) = %#v, want %#v", path, got, want)
		}
	}
	if got := c.Int("ports.1"); got != 2 {
		t.Errorf("ports.1 = %d", got)
	}
}

func TestNewErrors(t *testing.T) {
	if _, err := viper.New(t.Context(), []config.Source{src(nil), nil}); err == nil || !strings.HasPrefix(err.Error(), "config: ") {
		t.Fatalf("nil source: err = %v", err)
	}
	boom := errors.New("boom")
	_, err := viper.New(t.Context(), []config.Source{src(nil), &mapSource{err: boom}})
	if !errors.Is(err, boom) || !strings.Contains(err.Error(), "config: load source 1") {
		t.Fatalf("load error = %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := viper.New(ctx, []config.Source{src(nil)}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ctx: err = %v", err)
	}
}

func TestNewWithoutSources(t *testing.T) {
	c := mustNew(t)
	got, ok := c.Value("")
	if !ok || !reflect.DeepEqual(got, map[string]any{}) {
		t.Fatalf("Value(\"\") = %#v, %v", got, ok)
	}
}

func TestValue(t *testing.T) {
	c := mustNew(t, src(map[string]any{
		"broker": map[string]any{"kafka": map[string]any{"brokers": []any{"a", "b"}}},
		"Camel":  map[string]any{"Key": "v"},
		"nil":    nil,
		"a.b":    "dotted",
	}))
	tests := []struct {
		path string
		want any
		ok   bool
	}{
		{"broker.kafka.brokers", []any{"a", "b"}, true},
		{"broker.kafka.brokers.0", "a", true},
		{"broker.kafka.brokers.2", nil, false},
		{"broker.kafka.brokers.-1", nil, false},
		{"broker.kafka.brokers.x", nil, false},
		{"broker.kafka.missing", nil, false},
		{"broker.kafka.brokers.0.deeper", nil, false},
		{"camel.key", "v", true},
		{"Camel.Key", "v", true},
		{"nil", nil, false},
		{"a.b", "dotted", true},
		{"broker.", nil, false},
	}
	for _, tt := range tests {
		got, ok := c.Value(tt.path)
		if ok != tt.ok || !reflect.DeepEqual(got, tt.want) {
			t.Errorf("Value(%q) = %#v, %v; want %#v, %v", tt.path, got, ok, tt.want, tt.ok)
		}
	}
}

func TestValueReturnsCopy(t *testing.T) {
	c := mustNew(t, src(map[string]any{"m": map[string]any{"list": []any{"x"}}}))
	v, _ := c.Value("m")
	v.(map[string]any)["list"].([]any)[0] = "mutated"
	v.(map[string]any)["new"] = 1
	root, _ := c.Value("")
	root.(map[string]any)["m"] = nil
	if got := c.String("m.list.0"); got != "x" {
		t.Fatalf("m.list.0 = %q after mutating copy", got)
	}
	if _, ok := c.Value("m.new"); ok {
		t.Fatal("copy mutation leaked into config")
	}
}

func TestTypedGetters(t *testing.T) {
	c := mustNew(t, src(map[string]any{
		"str":      "hello",
		"intStr":   " 42 ",
		"floatStr": "2.5",
		"intFloat": 8080.0,
		"frac":     1.5,
		"negative": -3,
		"big":      uint64(1 << 63),
		"boolStr":  "true",
		"bool":     true,
		"one":      "1",
		"dur":      "1m30s",
		"durNum":   1e9,
		"durInt":   "250",
		"durVal":   3 * time.Second,
		"map":      map[string]any{"k": "v"},
		"list":     []any{1},
		"nil":      nil,
		"bad":      "nope",
	}))
	tests := []struct {
		path     string
		str      string
		integer  int
		boolean  bool
		float    float64
		duration time.Duration
	}{
		{path: "str", str: "hello"},
		{path: "intStr", str: " 42 "},
		{path: "floatStr", str: "2.5", integer: 2, float: 2.5, duration: 2},
		{path: "intFloat", str: "8080", integer: 8080, boolean: true, float: 8080, duration: 8080},
		{path: "frac", str: "1.5", integer: 1, boolean: true, float: 1.5, duration: 1},
		{path: "negative", str: "-3", integer: -3, boolean: true, float: -3, duration: -3},
		{path: "big", str: "9223372036854775808", integer: -1 << 63, boolean: true, float: 1 << 63, duration: -1 << 63},
		{path: "boolStr", str: "true", boolean: true},
		{path: "bool", str: "true", integer: 1, boolean: true, float: 1},
		{path: "one", str: "1", integer: 1, boolean: true, float: 1, duration: 1},
		{path: "dur", str: "1m30s", duration: 90 * time.Second},
		{path: "durNum", str: "1000000000", integer: 1e9, boolean: true, float: 1e9, duration: time.Second},
		{path: "durInt", str: "250", integer: 250, float: 250, duration: 250},
		{path: "durVal", str: "3s", integer: 3e9, boolean: true, float: 3e9, duration: 3 * time.Second},
		{path: "map"},
		{path: "list"},
		{path: "nil"},
		{path: "bad", str: "nope"},
		{path: "missing"},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			if got := c.String(tt.path); got != tt.str {
				t.Errorf("String = %q, want %q", got, tt.str)
			}
			if got := c.Int(tt.path); got != tt.integer {
				t.Errorf("Int = %d, want %d", got, tt.integer)
			}
			if got := c.Bool(tt.path); got != tt.boolean {
				t.Errorf("Bool = %v, want %v", got, tt.boolean)
			}
			if got := c.Float64(tt.path); got != tt.float {
				t.Errorf("Float64 = %v, want %v", got, tt.float)
			}
			if got := c.Duration(tt.path); got != tt.duration {
				t.Errorf("Duration = %v, want %v", got, tt.duration)
			}
		})
	}
}

func TestReload(t *testing.T) {
	s := src(map[string]any{"v": "1"})
	c := mustNew(t, s)
	s.set(map[string]any{"v": "2"}, nil)
	if err := c.Reload(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := c.String("v"); got != "2" {
		t.Fatalf("v = %q after reload", got)
	}
	boom := errors.New("boom")
	s.set(map[string]any{"v": "3"}, boom)
	if err := c.Reload(t.Context()); !errors.Is(err, boom) {
		t.Fatalf("Reload err = %v", err)
	}
	if got := c.String("v"); got != "2" {
		t.Fatalf("failed reload replaced values: v = %q", got)
	}
}

func TestConcurrentReloadAndRead(t *testing.T) {
	s := src(map[string]any{"n": 0, "m": map[string]any{"k": "v"}})
	c := mustNew(t, s)
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Go(func() {
			for j := range 100 {
				if i == 0 {
					s.set(map[string]any{"n": j, "m": map[string]any{"k": "v"}}, nil)
					if err := c.Reload(t.Context()); err != nil {
						t.Error(err)
					}
					continue
				}
				_ = c.Int("n")
				_, _ = c.Value("")
				var out struct{ M map[string]string }
				if err := c.Unmarshal("", &out); err != nil || out.M["k"] != "v" {
					t.Errorf("Unmarshal = %+v, %v", out, err)
				}
			}
		})
	}
	wg.Wait()
	if got := c.Int("n"); got != 99 {
		t.Fatalf("n = %d, want 99", got)
	}
}

func TestEnv(t *testing.T) {
	t.Setenv("SVXTEST_BROKER__KAFKA__BROKERS", "a,b")
	t.Setenv("SVXTEST_HTTP__ADDR", ":9000")
	t.Setenv("SVXTEST_NAME", "svc")
	t.Setenv("SVXTEST_Mixed__CaSe", "x")
	t.Setenv("SVXTEST_DB", "scalar")
	t.Setenv("SVXTEST_DB__DSN", "dsn")
	t.Setenv("SVXTEST_BAD____KEY", "ignored")
	t.Setenv("SVXTEST_TRAILING__", "ignored")
	t.Setenv("SVXTEST_", "ignored")
	t.Setenv("OTHER_NAME", "ignored")

	m, err := viper.Env("SVXTEST_").Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"broker": map[string]any{"kafka": map[string]any{"brokers": "a,b"}},
		"http":   map[string]any{"addr": ":9000"},
		"name":   "svc",
		"mixed":  map[string]any{"case": "x"},
		"db":     map[string]any{"dsn": "dsn"},
	}
	if !reflect.DeepEqual(m, want) {
		t.Fatalf("Env = %#v, want %#v", m, want)
	}
}

func TestEnvOverridesFileLikeSource(t *testing.T) {
	t.Setenv("SVXTEST2_HTTP__MAXCONNS", "64")
	file := src(map[string]any{"http": map[string]any{"addr": ":8000", "maxConns": 16.0}})
	c := mustNew(t, file, viper.Env("SVXTEST2_"))
	if got := c.Int("http.maxConns"); got != 64 {
		t.Fatalf("http.maxConns = %d, want 64", got)
	}
	var out struct {
		Addr     string `json:"addr"`
		MaxConns int    `json:"maxConns"`
	}
	if err := c.Unmarshal("http", &out); err != nil {
		t.Fatal(err)
	}
	if out.Addr != ":8000" || out.MaxConns != 64 {
		t.Fatalf("Unmarshal = %+v", out)
	}
}

func TestWatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ws := &watchSource{m: map[string]any{"v": "1"}, changes: make(chan struct{})}
		static := src(map[string]any{"static": "s"})
		c := mustNew(t, static, ws)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		seen := make(chan string, 10)
		done := make(chan error, 1)
		go func() {
			done <- c.Watch(ctx, func(got config.Config) {
				if got != c {
					t.Error("fn received a different *Config")
				}
				seen <- got.String("v") + "/" + got.String("static")
			})
		}()

		ws.set(map[string]any{"v": "2"}, nil)
		ws.changes <- struct{}{}
		synctest.Wait()
		if got := <-seen; got != "2/s" {
			t.Fatalf("fn saw %q, want 2/s", got)
		}

		// A failed reload keeps the old values and skips fn.
		ws.set(map[string]any{"v": "3"}, errors.New("broken"))
		ws.changes <- struct{}{}
		synctest.Wait()
		if len(seen) != 0 {
			t.Fatalf("fn called after failed reload: %q", <-seen)
		}
		if got := c.String("v"); got != "2" {
			t.Fatalf("v = %q after failed reload", got)
		}

		ws.set(map[string]any{"v": "4"}, nil)
		ws.changes <- struct{}{}
		synctest.Wait()
		if got := <-seen; got != "4/s" {
			t.Fatalf("fn saw %q, want 4/s", got)
		}

		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Watch = %v, want nil", err)
		}
	})
}

func TestWatchReloadErrorHandlerReportsFailuresAndCallbacksMayReload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		invalid := errors.New("invalid value")
		ws := &watchSource{m: map[string]any{"v": 1}, changes: make(chan struct{})}
		c, err := viper.New(t.Context(), []config.Source{ws},
			viper.WithValidator(func(_ context.Context, s config.Reader) error {
				if s.Int("v") < 1 {
					return invalid
				}
				return nil
			}))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		var reported []error
		var changes []int
		go func() {
			done <- c.Watch(ctx, func(got config.Config) {
				if got != c {
					t.Error("change callback lost Config identity")
				}
				changes = append(changes, got.Int("v"))
				ws.set(map[string]any{"v": 5}, nil)
				if err := got.Reload(ctx); err != nil {
					t.Error(err)
				}
			}, nil, viper.WithReloadErrorHandler(nil), viper.WithReloadErrorHandler(func(err error) {
				reported = append(reported, err)
				if c.Int("v") != 1 {
					t.Error("failed watch reload replaced the published tree")
				}
				ws.set(map[string]any{"v": 3}, nil)
				if err := c.Reload(ctx); err != nil {
					t.Error(err)
				}
			}))
		}()
		ws.set(map[string]any{"v": 0}, nil)
		ws.changes <- struct{}{}
		synctest.Wait()
		if len(reported) != 1 || !errors.Is(reported[0], invalid) || len(changes) != 0 || c.Int("v") != 3 {
			t.Fatalf("reported = %v, changes = %v, value = %d", reported, changes, c.Int("v"))
		}
		// A manual reload does not invoke either watch callback.
		ws.set(map[string]any{"v": 0}, nil)
		if err := c.Reload(ctx); !errors.Is(err, invalid) {
			t.Fatalf("manual Reload = %v", err)
		}
		if len(reported) != 1 || len(changes) != 0 {
			t.Fatal("manual Reload invoked watch callbacks")
		}
		ws.set(map[string]any{"v": 4}, nil)
		ws.changes <- struct{}{}
		synctest.Wait()
		if !reflect.DeepEqual(changes, []int{4}) || c.Int("v") != 5 {
			t.Fatalf("change callback or its Reload failed: changes = %v, value = %d", changes, c.Int("v"))
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestWatchReloadErrorHandlerErrorHandlerIsSynchronousAndCoalesces(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		boom := errors.New("load failure")
		ws := &watchSource{m: map[string]any{"v": 1}, changes: make(chan struct{})}
		c := mustNew(t, ws)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		release := make(chan struct{})
		done := make(chan error, 1)
		var reports, changes atomic.Int32
		go func() {
			done <- c.Watch(ctx, func(config.Config) { changes.Add(1) },
				viper.WithReloadErrorHandler(func(err error) {
					if !errors.Is(err, boom) {
						t.Errorf("error callback = %v", err)
					}
					reports.Add(1)
					<-release
				}))
		}()
		ws.set(nil, boom)
		ws.changes <- struct{}{}
		synctest.Wait()
		ws.set(map[string]any{"v": 2}, nil)
		for range 5 {
			ws.changes <- struct{}{}
		}
		synctest.Wait()
		if reports.Load() != 1 || changes.Load() != 0 || c.Int("v") != 1 {
			t.Error("watch continued reloading while its error handler was blocked")
		}
		// The handler is blocked, but a manual reload must still be possible.
		if err := c.Reload(ctx); err != nil || c.Int("v") != 2 {
			t.Errorf("manual Reload while callback blocked = %v", err)
		}
		close(release)
		synctest.Wait()
		if reports.Load() != 1 || changes.Load() != 1 {
			t.Errorf("reports = %d, changes = %d; want 1, 1", reports.Load(), changes.Load())
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestWatchReloadErrorHandlerSuppressesShutdownErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ws := &watchSource{m: map[string]any{"v": 1}, changes: make(chan struct{})}
		var validating atomic.Bool
		c, err := viper.New(t.Context(), []config.Source{ws},
			viper.WithValidator(func(ctx context.Context, _ config.Reader) error {
				if validating.Load() {
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			}))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			done <- c.Watch(ctx, func(config.Config) { t.Error("shutdown invoked change callback") },
				viper.WithReloadErrorHandler(func(error) { t.Error("shutdown invoked error callback") }))
		}()
		validating.Store(true)
		ws.set(map[string]any{"v": 2}, nil)
		ws.changes <- struct{}{}
		synctest.Wait()
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Watch = %v", err)
		}
		if c.Int("v") != 1 {
			t.Fatal("shutdown published canceled candidate")
		}
	})
}

func TestWatchCoalescesBursts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ws := &watchSource{m: map[string]any{}, changes: make(chan struct{})}
		c := mustNew(t, ws)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		done := make(chan error, 1)
		var calls atomic.Int32
		go func() {
			done <- c.Watch(ctx, func(config.Config) {
				calls.Add(1)
				<-release
			})
		}()
		ws.changes <- struct{}{}
		synctest.Wait() // fn is now blocked in its first call
		for range 5 {
			ws.changes <- struct{}{}
		}
		synctest.Wait() // every notify has run while fn was blocked
		unblock()
		synctest.Wait()
		if n := calls.Load(); n != 2 {
			t.Fatalf("fn called %d times, want 2", n)
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Watch = %v, want nil", err)
		}
	})
}

func TestWatchReturnsSourceError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		boom := errors.New("watch failed")
		failing := &watchSource{m: map[string]any{}, changes: make(chan struct{}), err: boom}
		other := &watchSource{m: map[string]any{}, changes: make(chan struct{})}
		c := mustNew(t, other, failing)
		done := make(chan error, 1)
		go func() {
			done <- c.Watch(t.Context(), nil,
				viper.WithReloadErrorHandler(func(error) { t.Error("terminal error reported as a reload failure") }))
		}()
		failing.changes <- struct{}{}
		err := <-done
		if !errors.Is(err, boom) || !strings.HasPrefix(err.Error(), "config: watch source 1") {
			t.Fatalf("Watch = %v", err)
		}
	})
}

func TestWatchWithoutWatchableSources(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := mustNew(t, src(map[string]any{"a": 1}))
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		start := time.Now()
		if err := c.Watch(ctx, func(config.Config) { t.Error("fn called") }); err != nil {
			t.Fatalf("Watch = %v", err)
		}
		if elapsed := time.Since(start); elapsed != time.Minute {
			t.Fatalf("Watch returned after %v, want 1m", elapsed)
		}
	})
}

func TestValueSnapshotSurvivesReload(t *testing.T) {
	s := src(map[string]any{"a": map[string]any{"b": "1"}})
	c := mustNew(t, s)
	before, _ := c.Value("")
	s.set(map[string]any{"a": map[string]any{"b": "2"}}, nil)
	if err := c.Reload(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := before.(map[string]any)["a"].(map[string]any)["b"]; got != "1" {
		t.Fatalf("old snapshot changed: a.b = %v", got)
	}
	if got := c.String("a.b"); got != "2" {
		t.Fatalf("a.b = %q after reload", got)
	}
}
