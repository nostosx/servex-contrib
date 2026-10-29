package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging/broker"
	"github.com/nostosx/servex/metadata"
)

const (
	waitTimeout = 30 * time.Second
	stream      = "EVENTS"
)

// fixture is an embedded JetStream server with a Broker connected to it.
type fixture struct {
	srv    *server.Server
	nc     *natsgo.Conn
	js     jetstream.JetStream
	broker *Broker
	ready  chan string
}

func startServer(t *testing.T) *server.Server {
	t.Helper()
	srv, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	})
	if err != nil {
		t.Fatalf("server.NewServer() = %v", err)
	}
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats server not ready")
	}
	return srv
}

func newFixture(t *testing.T, opts ...Option) *fixture {
	t.Helper()
	srv := startServer(t)
	nc, err := natsgo.Connect(srv.ClientURL(), natsgo.MaxReconnects(0))
	if err != nil {
		t.Fatalf("nats.Connect() = %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateStream(t.Context(), jetstream.StreamConfig{
		Name:     stream,
		Subjects: []string{"orders", "orders.>", "payments"},
	}); err != nil {
		t.Fatalf("CreateStream() = %v", err)
	}
	f := &fixture{srv: srv, nc: nc, js: js, ready: make(chan string, 16)}
	opts = append(slices.Clone(opts), withSubscribed(func(topic, group string) {
		f.ready <- topic + "/" + group
	}))
	f.broker = New(js, opts...)
	return f
}

// testSub runs Subscribe in the background.
type testSub struct {
	cancel context.CancelFunc
	done   chan error
	exited chan struct{}
}

func (f *fixture) subscribe(t *testing.T, topic, group string, h endpoint.Endpoint) *testSub {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	s := &testSub{cancel: cancel, done: make(chan error, 1), exited: make(chan struct{})}
	go func() {
		defer close(s.exited)
		s.done <- f.broker.Subscribe(ctx, topic, group, h)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-s.exited:
		case <-time.After(waitTimeout):
			t.Error("Subscribe did not exit during cleanup")
		}
	})
	select {
	case <-f.ready:
	case err := <-s.done:
		t.Fatalf("Subscribe() returned early: %v", err)
	case <-time.After(waitTimeout):
		t.Fatal("subscription not created")
	}
	return s
}

func (s *testSub) stop(t *testing.T) error {
	t.Helper()
	s.cancel()
	select {
	case err := <-s.done:
		return err
	case <-time.After(waitTimeout):
		t.Fatal("Subscribe did not return after cancellation")
		return nil
	}
}

func (f *fixture) publish(t *testing.T, topic string, keys ...string) {
	t.Helper()
	for _, k := range keys {
		if err := f.broker.Publish(t.Context(), &broker.Message{Topic: topic, Key: k}); err != nil {
			t.Fatalf("Publish(%s) = %v", k, err)
		}
	}
}

// collector records handled messages.
type collector struct {
	mu   sync.Mutex
	msgs []*broker.Message
}

func (c *collector) handle(_ context.Context, req any) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgs = append(c.msgs, req.(*broker.Message))
	return nil, nil
}

func (c *collector) keys() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var keys []string
	for _, m := range c.msgs {
		keys = append(keys, m.Key)
	}
	return keys
}

func (c *collector) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.msgs)
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitFor(t *testing.T, n int, cs ...*collector) {
	t.Helper()
	eventually(t, fmt.Sprintf("%d messages", n), func() bool {
		total := 0
		for _, c := range cs {
			total += c.len()
		}
		return total >= n
	})
}

// settledConsumer reports whether the durable consumer of group has no
// pending or unacknowledged messages.
func (f *fixture) settledConsumer(t *testing.T, topic, group string) bool {
	t.Helper()
	cons, err := f.js.Consumer(t.Context(), stream, DurableName(topic, group))
	if err != nil {
		t.Fatal(err)
	}
	info, err := cons.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return info.NumPending == 0 && info.NumAckPending == 0 && info.NumWaiting >= 0
}

func TestMessageMapping(t *testing.T) {
	f := newFixture(t)
	var c collector
	s := f.subscribe(t, "orders", "g", c.handle)
	in := &broker.Message{
		Topic:  "orders",
		Key:    "k1",
		Header: metadata.Join(metadata.Pairs("trace-id", "abc", "multi", "1"), metadata.Pairs("multi", "2")),
		Body:   []byte("payload"),
	}
	if err := f.broker.Publish(t.Context(), in); err != nil {
		t.Fatal(err)
	}
	if err := f.broker.Publish(t.Context(), &broker.Message{Topic: "orders"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2, &c)
	if err := s.stop(t); err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}
	got, empty := c.msgs[0], c.msgs[1]
	if got.Topic != "orders" || got.Key != "k1" || string(got.Body) != "payload" ||
		got.Header.Get("trace-id") != "abc" || !slices.Equal(got.Header.Values("multi"), []string{"1", "2"}) ||
		got.Header.Get(HeaderKey) != "" {
		t.Fatalf("message = %+v", got)
	}
	if empty.Key != "" || empty.Header != nil || len(empty.Body) != 0 {
		t.Fatalf("empty message = %+v", empty)
	}
}

func TestGroupDelivery(t *testing.T) {
	f := newFixture(t)
	var a1, a2, billing, other collector
	f.subscribe(t, "orders", "audit", a1.handle)
	f.subscribe(t, "orders", "audit", a2.handle)
	f.subscribe(t, "orders", "billing", billing.handle)
	f.subscribe(t, "payments", "audit", other.handle)

	var keys []string
	for i := range 20 {
		keys = append(keys, fmt.Sprintf("m%02d", i))
	}
	f.publish(t, "orders", keys...)
	waitFor(t, len(keys), &a1, &a2)
	waitFor(t, len(keys), &billing)
	eventually(t, "audit settled", func() bool { return f.settledConsumer(t, "orders", "audit") })

	audit := append(a1.keys(), a2.keys()...)
	slices.Sort(audit)
	if !slices.Equal(audit, keys) {
		t.Errorf("audit group got %v, want each of %v once", audit, keys)
	}
	if a1.len() == 0 || a2.len() == 0 {
		t.Errorf("audit members got %d and %d messages, want both to share the work", a1.len(), a2.len())
	}
	if got := billing.keys(); !slices.Equal(got, keys) {
		t.Errorf("billing got %v, want %v", got, keys)
	}
	if n := other.len(); n != 0 {
		t.Errorf("other topic got %d messages", n)
	}
}

func TestBroadcastDelivery(t *testing.T) {
	f := newFixture(t)
	f.publish(t, "orders", "old")
	var b1, b2 collector
	s1 := f.subscribe(t, "orders", "", b1.handle)
	s2 := f.subscribe(t, "orders", "", b2.handle)
	f.publish(t, "orders", "m0", "m1", "m2")
	waitFor(t, 3, &b1)
	waitFor(t, 3, &b2)
	for _, c := range []*collector{&b1, &b2} {
		if got := c.keys(); !slices.Equal(got, []string{"m0", "m1", "m2"}) {
			t.Errorf("broadcast subscriber got %v", got)
		}
	}
	for _, s := range []*testSub{s1, s2} {
		if err := s.stop(t); err != nil {
			t.Errorf("Subscribe() = %v", err)
		}
	}
	// The ephemeral consumers are deleted.
	st, err := f.js.Stream(t.Context(), stream)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for range st.ConsumerNames(t.Context()).Name() {
		n++
	}
	if n != 0 {
		t.Errorf("%d consumers left after broadcast subscribers ended", n)
	}
}

func TestAckNackAndDeadLetter(t *testing.T) {
	unavailable := errorx.New(errorx.Unavailable, "DOWN", "down")
	tests := []struct {
		name         string
		opts         []Option
		failures     int
		err          error
		wantAttempts int
		wantDead     bool
	}{
		{"ack", nil, 0, nil, 1, false},
		{"retryable then ack", nil, 2, unavailable, 3, false},
		{"retryable exhausts default redeliveries", nil, 100, unavailable, 4, true},
		{"retryable exhausts custom redeliveries", []Option{WithMaxRedeliveries(1)}, 100, unavailable, 2, true},
		{"negative redeliveries", []Option{WithMaxRedeliveries(-1)}, 100, unavailable, 1, true},
		{"plain error is not retryable", nil, 100, errors.New("bad"), 1, true},
		{"explicitly non-retryable", nil, 100, unavailable.WithRetryable(false), 1, true},
		{"explicitly retryable", []Option{WithMaxRedeliveries(2)}, 100,
			errorx.New(errorx.Internal, "X", "x").WithRetryable(true), 3, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				mu       sync.Mutex
				backoffs []int
				dead     []*broker.Message
				deadErrs []error
			)
			opts := append(slices.Clone(tt.opts),
				WithBackoff(func(attempt int) time.Duration {
					mu.Lock()
					defer mu.Unlock()
					backoffs = append(backoffs, attempt)
					return time.Millisecond
				}),
				WithDeadLetter(func(msg *broker.Message, err error) {
					mu.Lock()
					defer mu.Unlock()
					dead = append(dead, msg)
					deadErrs = append(deadErrs, err)
				}))
			f := newFixture(t, opts...)
			var attempts atomic.Int32
			s := f.subscribe(t, "orders", "g", func(_ context.Context, req any) (any, error) {
				if n := int(attempts.Add(1)); n <= tt.failures {
					return nil, tt.err
				}
				return nil, nil
			})
			if err := f.broker.Publish(t.Context(), &broker.Message{Topic: "orders", Key: "k", Header: metadata.Pairs("x-id", "1")}); err != nil {
				t.Fatal(err)
			}
			eventually(t, "message settled", func() bool {
				return int(attempts.Load()) >= tt.wantAttempts && f.settledConsumer(t, "orders", "g")
			})
			time.Sleep(50 * time.Millisecond) // no further redelivery
			if err := s.stop(t); err != nil {
				t.Fatalf("Subscribe() = %v", err)
			}
			if got := int(attempts.Load()); got != tt.wantAttempts {
				t.Errorf("attempts = %d, want %d", got, tt.wantAttempts)
			}
			mu.Lock()
			defer mu.Unlock()
			if want := tt.wantAttempts - 1; len(backoffs) != want {
				t.Errorf("backoff calls = %v, want %d", backoffs, want)
			}
			for i, a := range backoffs {
				if a != i+1 {
					t.Errorf("backoff attempts = %v, want 1, 2, ...", backoffs)
				}
			}
			if tt.wantDead != (len(dead) == 1) || len(dead) > 1 {
				t.Fatalf("dead letters = %d, want dead=%v", len(dead), tt.wantDead)
			}
			if tt.wantDead && (dead[0].Key != "k" || dead[0].Header.Get("x-id") != "1" || !errors.Is(deadErrs[0], tt.err)) {
				t.Errorf("dead letter = %+v, %v", dead[0], deadErrs[0])
			}
		})
	}
}

func TestConsumerConfigUsesUnlimitedDelivery(t *testing.T) {
	f := newFixture(t, WithMaxRedeliveries(5), WithAckWait(time.Minute))
	f.subscribe(t, "orders", "g", (&collector{}).handle)
	cons, err := f.js.Consumer(t.Context(), stream, DurableName("orders", "g"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := cons.CachedInfo().Config
	if cfg.MaxDeliver != -1 || cfg.AckWait != time.Minute || cfg.AckPolicy != jetstream.AckExplicitPolicy ||
		cfg.FilterSubject != "orders" {
		t.Fatalf("consumer config = %+v", cfg)
	}
}

func TestInterruptedMessageIsRedelivered(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  func(ctx context.Context) error
	}{
		{"context error", func(ctx context.Context) error { return ctx.Err() }},
		{"retryable error while stopping", func(context.Context) error { return errorx.New(errorx.Unavailable, "DOWN", "down") }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, WithAckWait(time.Minute))
			handling := make(chan struct{})
			var once sync.Once
			s := f.subscribe(t, "orders", "g", func(ctx context.Context, _ any) (any, error) {
				once.Do(func() { close(handling) })
				<-ctx.Done()
				return nil, tt.err(ctx)
			})
			f.publish(t, "orders", "m0")
			<-handling
			if err := s.stop(t); err != nil {
				t.Fatalf("Subscribe() = %v", err)
			}
			var c collector
			f.subscribe(t, "orders", "g", c.handle)
			// Redelivered right away, not after the one minute ack wait.
			waitFor(t, 1, &c)
			if got := c.keys(); !slices.Equal(got, []string{"m0"}) {
				t.Fatalf("redelivered %v, want [m0]", got)
			}
		})
	}
}

func TestNonRetryableErrorWhileStoppingIsDeadLettered(t *testing.T) {
	var dead atomic.Int32
	f := newFixture(t, WithDeadLetter(func(*broker.Message, error) { dead.Add(1) }))
	handling := make(chan struct{})
	s := f.subscribe(t, "orders", "g", func(ctx context.Context, _ any) (any, error) {
		close(handling)
		<-ctx.Done()
		return nil, errorx.New(errorx.InvalidArgument, "BAD", "bad message")
	})
	f.publish(t, "orders", "m0")
	<-handling
	if err := s.stop(t); err != nil {
		t.Fatal(err)
	}
	if got := dead.Load(); got != 1 {
		t.Fatalf("dead letters = %d, want 1", got)
	}
	eventually(t, "message terminated", func() bool { return f.settledConsumer(t, "orders", "g") })
}

func TestKeepAliveDuringLongHandler(t *testing.T) {
	f := newFixture(t, WithAckWait(200*time.Millisecond))
	var calls atomic.Int32
	s := f.subscribe(t, "orders", "g", func(context.Context, any) (any, error) {
		calls.Add(1)
		time.Sleep(time.Second) // five ack waits
		return nil, nil
	})
	f.publish(t, "orders", "slow")
	eventually(t, "handled", func() bool { return calls.Load() == 1 && f.settledConsumer(t, "orders", "g") })
	if err := s.stop(t); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1 (no redelivery while in progress)", got)
	}
}

func TestNoStream(t *testing.T) {
	f := newFixture(t)
	err := f.broker.Publish(t.Context(), &broker.Message{Topic: "unbound"})
	if errorx.CodeOf(err) != errorx.FailedPrecondition {
		t.Errorf("Publish(unbound) = %v, want FailedPrecondition", err)
	}
	err = f.broker.Subscribe(t.Context(), "unbound", "g", (&collector{}).handle)
	if errorx.CodeOf(err) == errorx.OK || errorx.IsRetryable(err) {
		t.Errorf("Subscribe(unbound) = %v, want a non-retryable error", err)
	}
	b := New(f.js, WithStream("MISSING"))
	if err := b.Subscribe(t.Context(), "orders", "g", (&collector{}).handle); errorx.CodeOf(err) != errorx.FailedPrecondition {
		t.Errorf("Subscribe(missing stream) = %v, want FailedPrecondition", err)
	}
}

func TestWithStream(t *testing.T) {
	f := newFixture(t)
	ready := make(chan struct{}, 1)
	b := New(f.js, WithStream(stream), withSubscribed(func(string, string) { ready <- struct{}{} }))
	got := make(chan string, 1)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- b.Subscribe(ctx, "orders.created", "g", func(_ context.Context, req any) (any, error) {
			got <- req.(*broker.Message).Topic
			return nil, nil
		})
	}()
	<-ready
	f.publish(t, "orders.created", "k")
	if topic := <-got; topic != "orders.created" {
		t.Fatalf("topic = %q", topic)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestValidation(t *testing.T) {
	f := newFixture(t)
	h := (&collector{}).handle
	for name, err := range map[string]error{
		"publish nil":           f.broker.Publish(t.Context(), nil),
		"publish empty topic":   f.broker.Publish(t.Context(), &broker.Message{}),
		"subscribe empty topic": f.broker.Subscribe(t.Context(), "", "g", h),
		"subscribe nil handler": f.broker.Subscribe(t.Context(), "orders", "g", nil),
	} {
		if errorx.CodeOf(err) != errorx.InvalidArgument {
			t.Errorf("%s: err = %v, want InvalidArgument", name, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := f.broker.Publish(ctx, &broker.Message{Topic: "orders"}); errorx.CodeOf(err) != errorx.Canceled {
		t.Errorf("Publish(canceled) = %v, want Canceled", err)
	}
	if err := f.broker.Subscribe(ctx, "orders", "g", h); err != nil {
		t.Errorf("Subscribe(canceled) = %v, want nil", err)
	}
}

func TestDurableName(t *testing.T) {
	if got, want := DurableName("orders", "billing"), "62696c6c696e67_6f7264657273"; got != want {
		t.Fatalf("DurableName() = %q, want %q", got, want)
	}
	seen := map[string]bool{}
	for _, pair := range [][2]string{
		{"orders.created", "billing_ops"}, {"orders.created", "billing.ops"},
		{"orders_created", "billing.ops"}, {"b_c", "a"}, {"c", "a_b"},
		{"orders.*.created", "my group/x"}, {"orders.>", "账单"},
	} {
		name := DurableName(pair[0], pair[1])
		if seen[name] {
			t.Fatalf("consumer name collision for %q: %s", pair, name)
		}
		seen[name] = true
	}
}

func TestDistinctGroupsWithPreviouslyCollidingNames(t *testing.T) {
	f := newFixture(t)
	var first, second collector
	a := f.subscribe(t, "orders.created", "billing_ops", first.handle)
	b := f.subscribe(t, "orders.created", "billing.ops", second.handle)
	f.publish(t, "orders.created", "one", "two")
	waitFor(t, 2, &first)
	waitFor(t, 2, &second)
	if err := a.stop(t); err != nil {
		t.Fatal(err)
	}
	if err := b.stop(t); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(first.keys(), []string{"one", "two"}) || !slices.Equal(second.keys(), []string{"one", "two"}) {
		t.Fatalf("independent groups received %v and %v", first.keys(), second.keys())
	}
}

func TestConnectionLoss(t *testing.T) {
	for _, tt := range []struct {
		name  string
		group string
		cut   func(f *fixture)
	}{
		{"server shutdown group", "g", func(f *fixture) { f.srv.Shutdown() }},
		{"server shutdown broadcast", "", func(f *fixture) { f.srv.Shutdown() }},
		{"connection closed", "g", func(f *fixture) { f.nc.Close() }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			s := f.subscribe(t, "orders", tt.group, (&collector{}).handle)
			tt.cut(f)
			select {
			case err := <-s.done:
				if errorx.CodeOf(err) != errorx.Unavailable || !errorx.IsRetryable(err) {
					t.Fatalf("Subscribe() = %v, want retryable Unavailable", err)
				}
			case <-time.After(waitTimeout):
				t.Fatal("Subscribe did not report the connection loss")
			}
			if err := f.broker.Publish(t.Context(), &broker.Message{Topic: "orders"}); errorx.CodeOf(err) != errorx.Unavailable {
				t.Fatalf("Publish() after connection loss = %v, want Unavailable", err)
			}
		})
	}
}

func TestWithConsumerInApp(t *testing.T) {
	f := newFixture(t)
	got := make(chan metadata.Info, 1)
	consumer := broker.NewConsumer(f.broker, "orders", "app", func(ctx context.Context, req any) (any, error) {
		info, _ := metadata.FromContext(ctx)
		got <- info
		return nil, nil
	})
	a := app.New(app.WithLogger(testLogger(slog.New(slog.DiscardHandler))), app.WithHooks(consumer))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()
	<-f.ready
	if err := f.broker.Publish(t.Context(), &broker.Message{Topic: "orders", Header: metadata.Pairs("x-id", "42")}); err != nil {
		t.Fatal(err)
	}
	info := <-got
	if info.Kind != metadata.KindServer || info.Protocol != "broker" || info.Operation != "orders" ||
		info.Header.Get("x-id") != "42" {
		t.Fatalf("Info = %+v", info)
	}
	// Consumers in a group are named after topic and group.
	if name := consumer.Name(); name != "consumer:orders:app" {
		t.Errorf("Name() = %q, want consumer:orders:app", name)
	}
	// The consumer runs over SubscribeReady and takes part in readiness.
	if err := consumer.(interface{ Check(context.Context) error }).Check(t.Context()); err != nil {
		t.Errorf("Check() = %v, want nil while subscribed", err)
	}
	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if s := a.State(); s != app.StateStopped {
		t.Fatalf("State() = %v, want stopped", s)
	}
}

func TestConnectionLossFailsApp(t *testing.T) {
	f := newFixture(t)
	consumer := broker.NewConsumer(f.broker, "orders", "app", (&collector{}).handle)
	a := app.New(app.WithLogger(testLogger(slog.New(slog.DiscardHandler))), app.WithHooks(consumer))
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(t.Context()) }()
	<-f.ready
	// Start waits for the subscription, so the loss happens while running.
	eventually(t, "app running", func() bool { return a.State() == app.StateRunning })
	f.srv.Shutdown()
	select {
	case err := <-runErr:
		if errorx.CodeOf(err) != errorx.Unavailable {
			t.Fatalf("Run() = %v, want Unavailable", err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("App kept running after the connection was lost")
	}
	if s := a.State(); s != app.StateFailed {
		t.Fatalf("State() = %v, want failed", s)
	}
}

// TestSubscribeReady checks that SubscribeReady reports readiness once, and
// that a message published right after it is delivered, for group and
// broadcast subscriptions.
func TestSubscribeReady(t *testing.T) {
	for _, group := range []string{"g", ""} {
		t.Run(fmt.Sprintf("group=%q", group), func(t *testing.T) {
			f := newFixture(t)
			var readyCalls atomic.Int32
			ready := make(chan struct{})
			got := make(chan string, 16)
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() {
				done <- f.broker.SubscribeReady(ctx, "orders", group, endpoint.Typed(func(_ context.Context, msg *broker.Message) (any, error) {
					got <- msg.Key
					return nil, nil
				}), func() {
					if readyCalls.Add(1) == 1 {
						close(ready)
					}
				})
			}()
			select {
			case <-ready:
			case err := <-done:
				t.Fatalf("SubscribeReady() = %v before ready", err)
			case <-time.After(waitTimeout):
				t.Fatal("subscription not ready")
			}
			f.publish(t, "orders", "k0", "k1")
			received := map[string]bool{}
			for len(received) < 2 {
				select {
				case k := <-got:
					received[k] = true
				case <-time.After(waitTimeout):
					t.Fatalf("received %v after ready, want k0 and k1", received)
				}
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatalf("SubscribeReady() = %v, want nil", err)
			}
			if n := readyCalls.Load(); n != 1 {
				t.Fatalf("ready called %d times, want 1", n)
			}
		})
	}
}

// TestSubscribeReadyFailures checks that a consumer hook over the Broker
// fails its Start when the subscription cannot be set up.
func TestSubscribeReadyFailures(t *testing.T) {
	notReady := func() { t.Error("ready called") }
	f := newFixture(t)
	if err := f.broker.SubscribeReady(t.Context(), "", "g", (&collector{}).handle, notReady); errorx.CodeOf(err) != errorx.InvalidArgument {
		t.Fatalf("SubscribeReady(empty topic) = %v, want InvalidArgument", err)
	}
	c := broker.NewConsumer(f.broker, "unbound", "g", (&collector{}).handle)
	if err := c.Start(t.Context()); errorx.CodeOf(err) == errorx.OK || errorx.IsRetryable(err) {
		t.Fatalf("consumer Start(unbound) = %v, want a non-retryable error", err)
	}
	f.nc.Close()
	c = broker.NewConsumer(f.broker, "orders", "", (&collector{}).handle)
	if err := c.Start(t.Context()); err == nil {
		t.Fatal("consumer Start() on a closed connection = nil, want an error")
	}
}
