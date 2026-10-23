//go:build integration

package nats

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging/broker"
	"github.com/nostosx/servex/metadata"
)

// Run with: SERVEX_IT_NATS_URL=nats://127.0.0.1:4222 go test -tags integration ./...
// The server must have JetStream enabled. Every test uses its own stream.

func newITFixture(t *testing.T, opts ...Option) (*fixture, string) {
	t.Helper()
	url := os.Getenv("SERVEX_IT_NATS_URL")
	if url == "" {
		t.Skip("SERVEX_IT_NATS_URL is not set")
	}
	nc, err := natsgo.Connect(url, natsgo.MaxReconnects(0))
	if err != nil {
		t.Fatalf("nats.Connect() = %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("SERVEX_IT_%d", time.Now().UnixNano())
	prefix := "it" + name[len("SERVEX_IT_"):]
	if _, err := js.CreateStream(t.Context(), jetstream.StreamConfig{Name: name, Subjects: []string{prefix + ".>"}}); err != nil {
		t.Fatalf("CreateStream() = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c, err := natsgo.Connect(url)
		if err != nil {
			return
		}
		defer c.Close()
		if j, err := jetstream.New(c); err == nil {
			_ = j.DeleteStream(ctx, name)
			_ = j.DeleteKeyValue(ctx, name)
		}
	})
	f := &fixture{nc: nc, js: js, ready: make(chan string, 16)}
	opts = append([]Option{WithRedeliveryBucket(name)}, opts...)
	opts = append(opts, withSubscribed(func(topic, group string) { f.ready <- topic + "/" + group }))
	f.broker = New(js, opts...)
	return f, prefix
}

func TestIntegrationDelivery(t *testing.T) {
	f, p := newITFixture(t)
	var g1, g2, b1 collector
	f.subscribe(t, p+".orders", "g", g1.handle)
	f.subscribe(t, p+".orders", "g", g2.handle)
	f.subscribe(t, p+".orders", "", b1.handle)
	in := &broker.Message{
		Topic:  p + ".orders",
		Key:    "k0",
		Header: metadata.Join(metadata.Pairs("trace-id", "abc"), metadata.Pairs("trace-id", "def")),
		Body:   []byte("payload"),
	}
	if err := f.broker.Publish(t.Context(), in); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for i := 1; i < 10; i++ {
		keys = append(keys, fmt.Sprintf("k%d", i))
	}
	f.publish(t, p+".orders", keys...)
	keys = append([]string{"k0"}, keys...)
	waitFor(t, len(keys), &g1, &g2)
	waitFor(t, len(keys), &b1)
	time.Sleep(100 * time.Millisecond)
	group := append(g1.keys(), g2.keys()...)
	slices.Sort(group)
	if !slices.Equal(group, keys) {
		t.Errorf("group got %v, want %v once each", group, keys)
	}
	if got := b1.keys(); !slices.Equal(got, keys) {
		t.Errorf("broadcast got %v, want %v", got, keys)
	}
	first := b1.msgs[0]
	if string(first.Body) != "payload" || !slices.Equal(first.Header.Values("trace-id"), []string{"abc", "def"}) {
		t.Errorf("message = %+v", first)
	}
}

func TestIntegrationRedeliveryAndDeadLetter(t *testing.T) {
	var mu sync.Mutex
	var dead []string
	f, p := newITFixture(t,
		WithMaxRedeliveries(2),
		WithBackoff(func(int) time.Duration { return 10 * time.Millisecond }),
		WithDeadLetter(func(msg *broker.Message, _ error) {
			mu.Lock()
			defer mu.Unlock()
			dead = append(dead, msg.Key)
		}))
	attempts := map[string]int{}
	f.subscribe(t, p+".orders", "g", func(_ context.Context, req any) (any, error) {
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
	f.publish(t, p+".orders", "ok", "retry", "reject")
	eventually(t, "dead letters", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(dead) == 2
	})
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if attempts["ok"] != 1 || attempts["retry"] != 3 || attempts["reject"] != 1 {
		t.Errorf("attempts = %v", attempts)
	}
	slices.Sort(dead)
	if !slices.Equal(dead, []string{"reject", "retry"}) {
		t.Errorf("dead letters = %v", dead)
	}
}

func TestIntegrationInterruptedDeliveryDoesNotCount(t *testing.T) {
	dead := make(chan string, 1)
	f, p := newITFixture(t,
		WithMaxRedeliveries(0),
		WithAckWait(time.Minute),
		WithDeadLetter(func(msg *broker.Message, _ error) { dead <- msg.Key }))
	handling := make(chan struct{})
	var once sync.Once
	s := f.subscribe(t, p+".orders", "g", func(ctx context.Context, _ any) (any, error) {
		once.Do(func() { close(handling) })
		<-ctx.Done()
		return nil, ctx.Err()
	})
	f.publish(t, p+".orders", "m0")
	<-handling
	if err := s.stop(t); err != nil {
		t.Fatal(err)
	}
	f.subscribe(t, p+".orders", "g", func(context.Context, any) (any, error) {
		return nil, errorx.New(errorx.Unavailable, "DOWN", "down")
	})
	select {
	case key := <-dead:
		if key != "m0" {
			t.Fatalf("dead letter = %q, want m0", key)
		}
	case <-time.After(waitTimeout):
		t.Fatal("interrupted message was never dead lettered")
	}
}

func TestIntegrationConnectionLossFailsApp(t *testing.T) {
	f, p := newITFixture(t)
	got := make(chan metadata.Info, 1)
	consumer := broker.NewConsumer(f.broker, p+".orders", "app", func(ctx context.Context, _ any) (any, error) {
		info, _ := metadata.FromContext(ctx)
		got <- info
		return nil, nil
	})
	a := app.New(app.WithLogger(slog.New(slog.DiscardHandler)), app.WithHooks(consumer))
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(t.Context()) }()
	<-f.ready
	if err := f.broker.Publish(t.Context(), &broker.Message{Topic: p + ".orders", Header: metadata.Pairs("x-id", "42")}); err != nil {
		t.Fatal(err)
	}
	if info := <-got; info.Protocol != "broker" || info.Operation != p+".orders" || info.Header.Get("x-id") != "42" {
		t.Fatalf("Info = %+v", info)
	}
	f.nc.Close()
	select {
	case err := <-runErr:
		if errorx.CodeOf(err) != errorx.Unavailable {
			t.Fatalf("Run() = %v, want Unavailable", err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("App kept running after the connection was closed")
	}
}
