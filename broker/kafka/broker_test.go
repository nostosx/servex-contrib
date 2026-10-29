package kafka

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

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging/broker"
	"github.com/nostosx/servex/metadata"
)

const waitTimeout = 30 * time.Second

// fixture is a fake cluster with a Broker connected to it.
type fixture struct {
	cluster *kfake.Cluster
	broker  *Broker
	ready   chan string // receives "topic/group" for every new subscription
}

func newFixture(t *testing.T, opts ...Option) *fixture {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, "orders", "payments", "dead"))
	if err != nil {
		t.Fatalf("kfake.NewCluster() = %v", err)
	}
	t.Cleanup(c.Close)
	f := &fixture{cluster: c, ready: make(chan string, 16)}
	opts = append(slices.Clone(opts), withSubscribed(func(topic, group string) {
		f.ready <- topic + "/" + group
	}))
	b, err := New([]kgo.Opt{
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.FetchMaxWait(100 * time.Millisecond),
	}, opts...)
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	t.Cleanup(func() { b.Close() })
	f.broker = b
	return f
}

// testSub tracks a background Subscribe call.
type testSub struct {
	cancel context.CancelFunc
	done   chan error
	exited chan struct{}
}

// subscribe starts a subscription and waits for its consumer client to be
// created. For a non-empty group, it also waits for the requested number of
// members to form a stable group.
func (f *fixture) subscribe(t *testing.T, topic, group string, members int, h endpoint.Endpoint) *testSub {
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
	if group != "" {
		wctx, wcancel := context.WithTimeout(t.Context(), waitTimeout)
		defer wcancel()
		if _, err := f.cluster.WaitGroupStable(wctx, group, members); err != nil {
			t.Fatalf("group %q not stable with %d members: %v", group, members, err)
		}
	}
	return s
}

// stop cancels the subscription and returns the Subscribe result.
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
		if err := f.broker.Publish(t.Context(), &broker.Message{Topic: topic, Key: k, Body: []byte("body-" + k)}); err != nil {
			t.Fatalf("Publish(%s) = %v", k, err)
		}
	}
}

// collector records handled messages.
type collector struct {
	mu   sync.Mutex
	msgs []*broker.Message
	got  chan struct{}
}

func newCollector() *collector { return &collector{got: make(chan struct{}, 1024)} }

func (c *collector) handle(_ context.Context, req any) (any, error) {
	c.mu.Lock()
	c.msgs = append(c.msgs, req.(*broker.Message))
	c.mu.Unlock()
	c.got <- struct{}{}
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

// waitFor waits until the total number of messages handled by cs reaches n.
func waitFor(t *testing.T, n int, cs ...*collector) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		total := 0
		for _, c := range cs {
			total += c.len()
		}
		if total >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d messages", n)
}

// consumeAll reads every record of topic from the start until n records were
// read.
func consumeAll(t *testing.T, f *fixture, topic string, n int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(f.cluster.ListenAddrs()...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchMaxWait(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()
	var recs []*kgo.Record
	for len(recs) < n {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("read %d records of %q, want %d", len(recs), topic, n)
		}
		recs = append(recs, fetches.Records()...)
	}
	return recs
}

func TestMessageMapping(t *testing.T) {
	f := newFixture(t)
	c := newCollector()
	s := f.subscribe(t, "orders", "g", 1, c.handle)
	in := &broker.Message{
		Topic:  "orders",
		Key:    "k1",
		Header: metadata.Join(metadata.Pairs("trace-id", "abc", "multi", "1"), metadata.Pairs("multi", "2")),
		Body:   []byte("payload"),
	}
	if err := f.broker.Publish(t.Context(), in); err != nil {
		t.Fatal(err)
	}
	// A message without a key or headers.
	if err := f.broker.Publish(t.Context(), &broker.Message{Topic: "orders"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2, c)
	if err := s.stop(t); err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}
	var got, empty *broker.Message
	for _, m := range c.msgs {
		if m.Key == "k1" {
			got = m
		} else {
			empty = m
		}
	}
	if got == nil || empty == nil {
		t.Fatalf("messages = %+v", c.msgs)
	}
	if got.Topic != "orders" || string(got.Body) != "payload" || got.Header.Get("trace-id") != "abc" ||
		!slices.Equal(got.Header.Values("multi"), []string{"1", "2"}) {
		t.Fatalf("message = %+v", got)
	}
	if empty.Key != "" || empty.Header != nil || len(empty.Body) != 0 {
		t.Fatalf("empty message = %+v", empty)
	}
}

func TestGroupDelivery(t *testing.T) {
	f := newFixture(t)
	a1, a2, billing, other := newCollector(), newCollector(), newCollector(), newCollector()
	f.subscribe(t, "orders", "audit", 1, a1.handle)
	f.subscribe(t, "orders", "audit", 2, a2.handle)
	f.subscribe(t, "orders", "billing", 1, billing.handle)
	f.subscribe(t, "payments", "audit-payments", 1, other.handle)

	var keys []string
	for i := range 20 {
		keys = append(keys, fmt.Sprintf("m%02d", i))
	}
	f.publish(t, "orders", keys...)
	waitFor(t, len(keys), a1, a2)
	waitFor(t, len(keys), billing)
	time.Sleep(200 * time.Millisecond) // give duplicates a chance to show up

	audit := append(a1.keys(), a2.keys()...)
	slices.Sort(audit)
	if !slices.Equal(audit, keys) {
		t.Errorf("audit group got %v, want each of %v once", audit, keys)
	}
	if len(a1.keys()) == 0 || len(a2.keys()) == 0 {
		t.Errorf("audit members got %d and %d messages, want both to share the work", len(a1.keys()), len(a2.keys()))
	}
	b := billing.keys()
	slices.Sort(b)
	if !slices.Equal(b, keys) {
		t.Errorf("billing got %v, want %v", b, keys)
	}
	if n := other.len(); n != 0 {
		t.Errorf("other topic got %d messages", n)
	}
}

func TestBroadcastDelivery(t *testing.T) {
	f := newFixture(t)
	// A message published before the subscribers exist is not received.
	f.publish(t, "orders", "old")
	time.Sleep(5 * time.Millisecond)
	b1, b2 := newCollector(), newCollector()
	keys := []string{"m0", "m1", "m2"}
	// Client creation is not a partition-positioning barrier. Probe every
	// data key so both consumers are fetching every partition used below.
	type receipt struct {
		consumer int
		key      string
	}
	probed := make(chan receipt, 32)
	wrap := func(consumer int, c *collector) endpoint.Endpoint {
		return func(ctx context.Context, req any) (any, error) {
			msg := req.(*broker.Message)
			if string(msg.Body) == "probe" {
				select {
				case probed <- receipt{consumer, msg.Key}:
				default:
				}
				return nil, nil
			}
			return c.handle(ctx, req)
		}
	}
	s1 := f.subscribe(t, "orders", "", 0, wrap(0, b1))
	s2 := f.subscribe(t, "orders", "", 0, wrap(1, b2))
	ready := make(map[receipt]bool)
	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for len(ready) < 2*len(keys) {
		for _, key := range keys {
			if err := f.broker.Publish(ctx, &broker.Message{Topic: "orders", Key: key, Body: []byte("probe")}); err != nil {
				t.Fatal(err)
			}
		}
		select {
		case r := <-probed:
			ready[r] = true
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("broadcast consumers not positioned: %v", ready)
		}
	}
	f.publish(t, "orders", keys...)
	waitFor(t, len(keys), b1)
	waitFor(t, len(keys), b2)
	for _, c := range []*collector{b1, b2} {
		got := c.keys()
		slices.Sort(got)
		if !slices.Equal(got, keys) {
			t.Errorf("broadcast subscriber got %v", got)
		}
	}
	for _, s := range []*testSub{s1, s2} {
		if err := s.stop(t); err != nil {
			t.Errorf("Subscribe() = %v", err)
		}
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
			var backoffs []int
			opts := append(slices.Clone(tt.opts), WithDeadLetterTopic("dead"),
				WithBackoff(func(attempt int) time.Duration {
					backoffs = append(backoffs, attempt)
					return time.Millisecond
				}))
			f := newFixture(t, opts...)
			var attempts atomic.Int32
			settled := make(chan struct{}, 1)
			h := func(_ context.Context, req any) (any, error) {
				n := int(attempts.Add(1))
				if n == tt.wantAttempts {
					settled <- struct{}{}
				}
				if n <= tt.failures {
					return nil, tt.err
				}
				return nil, nil
			}
			s := f.subscribe(t, "orders", "g", 1, h)
			msg := &broker.Message{Topic: "orders", Key: "k", Header: metadata.Pairs("x-id", "1"), Body: []byte("v")}
			if err := f.broker.Publish(t.Context(), msg); err != nil {
				t.Fatal(err)
			}
			select {
			case <-settled:
			case <-time.After(waitTimeout):
				t.Fatal("message not settled")
			}
			if tt.wantDead {
				recs := consumeAll(t, f, "dead", 1)
				dl := toMessage(recs[0])
				if dl.Key != "k" || string(dl.Body) != "v" || dl.Header.Get("x-id") != "1" ||
					dl.Header.Get(HeaderOriginalTopic) != "orders" || dl.Header.Get(HeaderError) != tt.err.Error() {
					t.Errorf("dead letter = %+v", dl)
				}
			}
			if err := s.stop(t); err != nil {
				t.Fatalf("Subscribe() = %v", err)
			}
			if got := int(attempts.Load()); got != tt.wantAttempts {
				t.Errorf("attempts = %d, want %d", got, tt.wantAttempts)
			}
			if want := tt.wantAttempts - 1; len(backoffs) != want {
				t.Errorf("backoff calls = %v, want %d", backoffs, want)
			}
			// The settled message was committed: a new member of the group
			// does not receive it again.
			again := newCollector()
			s2 := f.subscribe(t, "orders", "g", 1, again.handle)
			f.publish(t, "orders", "marker")
			waitFor(t, 1, again)
			if err := s2.stop(t); err != nil {
				t.Fatal(err)
			}
			if got := again.keys(); !slices.Equal(got, []string{"marker"}) {
				t.Errorf("after restart got %v, want only the marker", got)
			}
		})
	}
}

func TestDeadLetterIsOptional(t *testing.T) {
	f := newFixture(t)
	var calls atomic.Int32
	done := make(chan struct{}, 2)
	s := f.subscribe(t, "orders", "g", 1, func(context.Context, any) (any, error) {
		calls.Add(1)
		done <- struct{}{}
		return nil, errors.New("bad")
	})
	f.publish(t, "orders", "a", "b")
	<-done
	<-done
	if err := s.stop(t); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("handler calls = %d, want 2", got)
	}
}

func TestInterruptedMessageIsRedelivered(t *testing.T) {
	f := newFixture(t)
	handling := make(chan struct{})
	s := f.subscribe(t, "orders", "g", 1, func(ctx context.Context, req any) (any, error) {
		if req.(*broker.Message).Key == "m0" {
			close(handling)
		}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	f.publish(t, "orders", "m0")
	<-handling
	if err := s.stop(t); err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}
	c := newCollector()
	f.subscribe(t, "orders", "g", 1, c.handle)
	waitFor(t, 1, c)
	if got := c.keys(); !slices.Equal(got, []string{"m0"}) {
		t.Fatalf("redelivered %v, want [m0]", got)
	}
}

func TestRetryableErrorWhileStoppingIsNotSettled(t *testing.T) {
	f := newFixture(t, WithDeadLetterTopic("dead"))
	handling := make(chan struct{})
	var once sync.Once
	s := f.subscribe(t, "orders", "g", 1, func(ctx context.Context, _ any) (any, error) {
		once.Do(func() { close(handling) })
		<-ctx.Done()
		return nil, errorx.New(errorx.Unavailable, "DOWN", "down")
	})
	f.publish(t, "orders", "m0")
	<-handling
	if err := s.stop(t); err != nil {
		t.Fatal(err)
	}
	c := newCollector()
	f.subscribe(t, "orders", "g", 1, c.handle)
	waitFor(t, 1, c)
	if got := c.keys(); !slices.Equal(got, []string{"m0"}) {
		t.Fatalf("redelivered %v, want [m0]", got)
	}
}

func TestNonRetryableErrorWhileStoppingIsDeadLettered(t *testing.T) {
	f := newFixture(t, WithDeadLetterTopic("dead"))
	handling := make(chan struct{})
	s := f.subscribe(t, "orders", "g", 1, func(ctx context.Context, _ any) (any, error) {
		close(handling)
		<-ctx.Done()
		// The handler rejects the message explicitly after stop began.
		return nil, errorx.New(errorx.InvalidArgument, "BAD", "bad message")
	})
	f.publish(t, "orders", "m0")
	<-handling
	if err := s.stop(t); err != nil {
		t.Fatal(err)
	}
	recs := consumeAll(t, f, "dead", 1)
	if got := string(recs[0].Key); got != "m0" {
		t.Fatalf("dead letter key = %q, want m0", got)
	}
	// It was committed as well.
	c := newCollector()
	f.subscribe(t, "orders", "g", 1, c.handle)
	f.publish(t, "orders", "marker")
	waitFor(t, 1, c)
	time.Sleep(100 * time.Millisecond)
	if got := c.keys(); !slices.Equal(got, []string{"marker"}) {
		t.Fatalf("after restart got %v, want only the marker", got)
	}
}

func TestConnectionLoss(t *testing.T) {
	for _, group := range []string{"g", ""} {
		t.Run(fmt.Sprintf("group=%q", group), func(t *testing.T) {
			f := newFixture(t, WithPingInterval(20*time.Millisecond))
			s := f.subscribe(t, "orders", group, 1, newCollector().handle)
			f.cluster.Close()
			select {
			case err := <-s.done:
				if errorx.CodeOf(err) != errorx.Unavailable || !errorx.IsRetryable(err) {
					t.Fatalf("Subscribe() = %v, want retryable Unavailable", err)
				}
			case <-time.After(waitTimeout):
				t.Fatal("Subscribe did not report the connection loss")
			}
		})
	}
}

func TestClose(t *testing.T) {
	f := newFixture(t)
	s := f.subscribe(t, "orders", "g", 1, newCollector().handle)
	if err := f.broker.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-s.done:
		if err != nil {
			t.Fatalf("Subscribe() after Close = %v, want nil", err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("Subscribe did not return after Close")
	}
	if err := f.broker.Publish(t.Context(), &broker.Message{Topic: "orders"}); errorx.CodeOf(err) != errorx.FailedPrecondition {
		t.Fatalf("Publish() after Close = %v, want FailedPrecondition", err)
	}
	if err := f.broker.Subscribe(t.Context(), "orders", "g", newCollector().handle); err != nil {
		t.Fatalf("Subscribe() after Close = %v, want nil", err)
	}
	if err := f.broker.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}
}

func TestNewFromClient(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "orders"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.FetchMaxWait(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ready := make(chan struct{}, 1)
	b := NewFromClient(cl, withSubscribed(func(string, string) { ready <- struct{}{} }))
	got := make(chan string, 1)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- b.Subscribe(ctx, "orders", "", func(_ context.Context, req any) (any, error) {
			got <- req.(*broker.Message).Key
			return nil, nil
		})
	}()
	<-ready
	if err := b.Publish(t.Context(), &broker.Message{Topic: "orders", Key: "k"}); err != nil {
		t.Fatal(err)
	}
	if k := <-got; k != "k" {
		t.Fatalf("key = %q", k)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	b.Close()
	// The user's client stays usable.
	if err := cl.ProduceSync(t.Context(), &kgo.Record{Topic: "orders"}).FirstErr(); err != nil {
		t.Fatalf("client unusable after Close: %v", err)
	}
}

func TestValidation(t *testing.T) {
	f := newFixture(t)
	tests := []struct {
		name string
		err  error
	}{
		{"publish nil", f.broker.Publish(t.Context(), nil)},
		{"publish empty topic", f.broker.Publish(t.Context(), &broker.Message{})},
		{"subscribe empty topic", f.broker.Subscribe(t.Context(), "", "g", newCollector().handle)},
		{"subscribe nil handler", f.broker.Subscribe(t.Context(), "orders", "g", nil)},
	}
	for _, tt := range tests {
		if errorx.CodeOf(tt.err) != errorx.InvalidArgument {
			t.Errorf("%s: err = %v, want InvalidArgument", tt.name, tt.err)
		}
	}
	if _, err := New([]kgo.Opt{kgo.SeedBrokers()}); errorx.CodeOf(err) != errorx.InvalidArgument {
		t.Errorf("New() without seeds = %v, want InvalidArgument", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := f.broker.Publish(ctx, &broker.Message{Topic: "orders"}); errorx.CodeOf(err) != errorx.Canceled {
		t.Errorf("Publish(canceled) = %v, want Canceled", err)
	}
}

func TestClassify(t *testing.T) {
	if err := classify(context.Background(), "op", errors.New("dial tcp: refused")); errorx.CodeOf(err) != errorx.Unavailable ||
		!errorx.IsRetryable(err) {
		t.Errorf("connection error = %v, want retryable Unavailable", err)
	}
	if err := classify(context.Background(), "op", kgo.ErrClientClosed); errorx.CodeOf(err) != errorx.FailedPrecondition {
		t.Errorf("closed client = %v", err)
	}
	if err := fetchError(kgo.Fetches{}); err != nil {
		t.Errorf("fetchError(empty) = %v", err)
	}
	if err := commitError(nil); err != nil {
		t.Errorf("commitError(nil) = %v", err)
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
	select {
	case <-f.ready:
	case <-time.After(waitTimeout):
		t.Fatal("consumer did not subscribe")
	}
	wctx, wcancel := context.WithTimeout(t.Context(), waitTimeout)
	defer wcancel()
	if _, err := f.cluster.WaitGroupStable(wctx, "app", 1); err != nil {
		t.Fatal(err)
	}
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
	f := newFixture(t, WithPingInterval(20*time.Millisecond))
	consumer := broker.NewConsumer(f.broker, "orders", "app", newCollector().handle)
	a := app.New(app.WithLogger(testLogger(slog.New(slog.DiscardHandler))), app.WithHooks(consumer))
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(t.Context()) }()
	<-f.ready
	f.cluster.Close()
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
			f.publish(t, "orders", "k0", "k1", "k2", "k3")
			received := map[string]bool{}
			for len(received) < 4 {
				select {
				case k := <-got:
					received[k] = true
				case <-time.After(waitTimeout):
					t.Fatalf("received %v after ready, want k0..k3", received)
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

// TestSubscribeReadyFailures checks that SubscribeReady returns without
// calling ready when the subscription cannot start.
func TestSubscribeReadyFailures(t *testing.T) {
	notReady := func() { t.Error("ready called") }
	t.Run("invalid", func(t *testing.T) {
		f := newFixture(t)
		if err := f.broker.SubscribeReady(t.Context(), "", "g", newCollector().handle, notReady); errorx.CodeOf(err) != errorx.InvalidArgument {
			t.Fatalf("SubscribeReady(empty topic) = %v, want InvalidArgument", err)
		}
	})
	t.Run("closed", func(t *testing.T) {
		f := newFixture(t)
		f.broker.Close()
		if err := f.broker.SubscribeReady(t.Context(), "orders", "g", newCollector().handle, notReady); err != nil {
			t.Fatalf("SubscribeReady() after Close = %v, want nil", err)
		}
	})
	for _, group := range []string{"g", ""} {
		t.Run(fmt.Sprintf("unreachable group=%q", group), func(t *testing.T) {
			f := newFixture(t, WithPingInterval(20*time.Millisecond))
			f.cluster.Close()
			c := broker.NewConsumer(f.broker, "orders", group, newCollector().handle)
			ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
			defer cancel()
			if err := c.Start(ctx); errorx.CodeOf(err) != errorx.Unavailable {
				t.Fatalf("consumer Start() = %v, want Unavailable", err)
			}
		})
	}
}
