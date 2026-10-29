//go:build integration

package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging/broker"
	"github.com/nostosx/servex/metadata"
)

// Run with: SERVEX_IT_KAFKA_BROKERS=127.0.0.1:9092 go test -tags integration ./...
// Every test creates its own topics and groups and deletes them afterwards.

const probeKey = "servex-probe"

// itFixture is a Broker on a real cluster with test specific topics.
type itFixture struct {
	brokers []string
	broker  *Broker
	prefix  string
}

func newITFixture(t *testing.T, opts ...func(prefix string) Option) *itFixture {
	t.Helper()
	env := os.Getenv("SERVEX_IT_KAFKA_BROKERS")
	if env == "" {
		t.Skip("SERVEX_IT_KAFKA_BROKERS is not set")
	}
	f := &itFixture{brokers: strings.Split(env, ","), prefix: fmt.Sprintf("servex-it-%d", time.Now().UnixNano())}
	admin, err := kgo.NewClient(kgo.SeedBrokers(f.brokers...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	adm := kadm.NewClient(admin)
	topics := []string{f.topic("orders"), f.topic("dead")}
	resp, err := adm.CreateTopics(t.Context(), 2, 1, nil, topics...)
	if err != nil || resp.Error() != nil {
		t.Fatalf("CreateTopics() = %v, %v", err, resp.Error())
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = adm.DeleteGroups(ctx, f.group("g"), f.group("app"))
		_, _ = adm.DeleteTopics(ctx, topics...)
	})
	var bopts []Option
	for _, o := range opts {
		bopts = append(bopts, o(f.prefix))
	}
	b, err := New([]kgo.Opt{kgo.SeedBrokers(f.brokers...), kgo.FetchMaxWait(200 * time.Millisecond)}, bopts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	f.broker = b
	return f
}

func (f *itFixture) topic(name string) string { return f.prefix + "-" + name }
func (f *itFixture) group(name string) string { return f.prefix + "-" + name }

// subscribe starts a subscription and waits until it receives a probe
// message, which proves that partitions are assigned and positioned.
func (f *itFixture) subscribe(t *testing.T, topic, group string, h endpoint.Endpoint) *testSub {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	s := &testSub{cancel: cancel, done: make(chan error, 1)}
	probed := make(chan struct{}, 1)
	go func() {
		s.done <- f.broker.Subscribe(ctx, topic, group, func(ctx context.Context, req any) (any, error) {
			if req.(*broker.Message).Key == probeKey {
				select {
				case probed <- struct{}{}:
				default:
				}
				return nil, nil
			}
			return h(ctx, req)
		})
	}()
	t.Cleanup(cancel)
	deadline := time.After(waitTimeout)
	for {
		if err := f.broker.Publish(t.Context(), &broker.Message{Topic: topic, Key: probeKey}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-probed:
			return s
		case err := <-s.done:
			t.Fatalf("Subscribe() returned early: %v", err)
		case <-deadline:
			t.Fatal("subscription not ready")
		case <-time.After(300 * time.Millisecond):
		}
	}
}

func TestIntegrationRoundTrip(t *testing.T) {
	f := newITFixture(t)
	group, broadcast := newCollector(), newCollector()
	f.subscribe(t, f.topic("orders"), f.group("g"), group.handle)
	f.subscribe(t, f.topic("orders"), "", broadcast.handle)
	in := &broker.Message{
		Topic:  f.topic("orders"),
		Key:    "k1",
		Header: metadata.Join(metadata.Pairs("trace-id", "abc"), metadata.Pairs("trace-id", "def")),
		Body:   []byte("payload"),
	}
	if err := f.broker.Publish(t.Context(), in); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 1, group)
	waitFor(t, 1, broadcast)
	for _, c := range []*collector{group, broadcast} {
		c.mu.Lock()
		got := c.msgs[0]
		c.mu.Unlock()
		if got.Key != "k1" || string(got.Body) != "payload" || !slices.Equal(got.Header.Values("trace-id"), []string{"abc", "def"}) {
			t.Errorf("message = %+v", got)
		}
	}
}

func TestIntegrationDeadLetterAndCommit(t *testing.T) {
	f := newITFixture(t, func(prefix string) Option { return WithDeadLetterTopic(prefix + "-dead") },
		func(string) Option { return WithMaxRedeliveries(1) })
	var mu sync.Mutex
	attempts := map[string]int{}
	s := f.subscribe(t, f.topic("orders"), f.group("g"), func(_ context.Context, req any) (any, error) {
		key := req.(*broker.Message).Key
		mu.Lock()
		attempts[key]++
		mu.Unlock()
		switch key {
		case "retry":
			return nil, errorx.New(errorx.Unavailable, "DOWN", "down")
		case "reject":
			return nil, errorx.New(errorx.InvalidArgument, "BAD", "bad")
		}
		return nil, nil
	})
	dead := newCollector()
	f.subscribe(t, f.topic("dead"), "", dead.handle)
	f.publish(t, f.topic("orders"), "ok", "retry", "reject")
	waitFor(t, 2, dead)
	if err := s.stop(t); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if attempts["ok"] != 1 || attempts["retry"] != 2 || attempts["reject"] != 1 {
		t.Errorf("attempts = %v", attempts)
	}
	mu.Unlock()
	keys := dead.keys()
	slices.Sort(keys)
	if !slices.Equal(keys, []string{"reject", "retry"}) {
		t.Errorf("dead letters = %v", keys)
	}
	for _, m := range dead.msgs {
		if m.Header.Get(HeaderOriginalTopic) != f.topic("orders") || m.Header.Get(HeaderError) == "" {
			t.Errorf("dead letter headers = %v", m.Header)
		}
	}
	// Everything was committed: a restarted member sees only new messages.
	again := newCollector()
	f.subscribe(t, f.topic("orders"), f.group("g"), again.handle)
	f.publish(t, f.topic("orders"), "marker")
	waitFor(t, 1, again)
	time.Sleep(500 * time.Millisecond)
	if got := again.keys(); !slices.Equal(got, []string{"marker"}) {
		t.Errorf("after restart got %v, want only the marker", got)
	}
}

func TestIntegrationInterruptedMessageIsRedelivered(t *testing.T) {
	f := newITFixture(t)
	handling := make(chan struct{})
	var once sync.Once
	s := f.subscribe(t, f.topic("orders"), f.group("g"), func(ctx context.Context, _ any) (any, error) {
		once.Do(func() { close(handling) })
		<-ctx.Done()
		return nil, ctx.Err()
	})
	f.publish(t, f.topic("orders"), "m0")
	<-handling
	if err := s.stop(t); err != nil {
		t.Fatal(err)
	}
	c := newCollector()
	f.subscribe(t, f.topic("orders"), f.group("g"), c.handle)
	waitFor(t, 1, c)
	if got := c.keys(); !slices.Equal(got, []string{"m0"}) {
		t.Fatalf("redelivered %v, want [m0]", got)
	}
}

func TestIntegrationWithConsumerInApp(t *testing.T) {
	f := newITFixture(t)
	got := make(chan metadata.Info, 16)
	consumer := broker.NewConsumer(f.broker, f.topic("orders"), f.group("app"), func(ctx context.Context, req any) (any, error) {
		info, _ := metadata.FromContext(ctx)
		got <- info
		return nil, nil
	})
	a := app.New(app.WithLogger(testLogger(slog.New(slog.DiscardHandler))), app.WithHooks(consumer))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()
	deadline := time.After(waitTimeout)
	var info metadata.Info
	for info.Header.Get("x-id") != "42" {
		if err := f.broker.Publish(t.Context(), &broker.Message{Topic: f.topic("orders"), Header: metadata.Pairs("x-id", "42")}); err != nil {
			t.Fatal(err)
		}
		select {
		case info = <-got:
		case <-time.After(300 * time.Millisecond):
		case <-deadline:
			t.Fatal("message not consumed")
		}
	}
	if info.Kind != metadata.KindServer || info.Protocol != "broker" || info.Operation != f.topic("orders") {
		t.Fatalf("Info = %+v", info)
	}
	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run() = %v", err)
	}
}

func (f *itFixture) publish(t *testing.T, topic string, keys ...string) {
	t.Helper()
	for _, k := range keys {
		if err := f.broker.Publish(t.Context(), &broker.Message{Topic: topic, Key: k}); err != nil {
			t.Fatalf("Publish(%s) = %v", k, err)
		}
	}
}
