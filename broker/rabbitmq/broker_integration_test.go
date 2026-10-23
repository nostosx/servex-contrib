//go:build integration

package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging/broker"
	"github.com/nostosx/servex/metadata"
)

// Run with: SERVEX_IT_RABBITMQ_URL=amqp://guest:guest@127.0.0.1:5672/ go test -tags integration ./...

const waitTimeout = 30 * time.Second

// fixture is a connection with a Broker on a test specific exchange.
type fixture struct {
	conn     *amqp.Connection
	broker   *Broker
	exchange string
	ready    chan string
	extra    []string // further queues to delete
}

func dial(t *testing.T) *amqp.Connection {
	t.Helper()
	url := os.Getenv("SERVEX_IT_RABBITMQ_URL")
	if url == "" {
		t.Skip("SERVEX_IT_RABBITMQ_URL is not set")
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("amqp.Dial() = %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func newFixture(t *testing.T, opts ...Option) *fixture {
	t.Helper()
	conn := dial(t)
	exchange := fmt.Sprintf("servex-it-%d", time.Now().UnixNano())
	f := &fixture{conn: conn, exchange: exchange, ready: make(chan string, 16)}
	opts = append([]Option{WithExchange(exchange)}, opts...)
	opts = append(opts, withSubscribed(func(topic, group string) { f.ready <- topic + "/" + group }))
	f.broker = New(conn, opts...)
	t.Cleanup(func() {
		f.broker.Close()
		// Remove what the test declared, through a separate connection in
		// case the test closed its own.
		c := dial(t)
		// A missing queue/exchange closes its AMQP channel. Give each delete
		// its own channel so one absent resource cannot skip all later ones.
		remove := func(name string, deleteResource func(*amqp.Channel) error) {
			ch, err := c.Channel()
			if err != nil {
				t.Errorf("cleanup %q channel: %v", name, err)
				return
			}
			defer ch.Close()
			if err := deleteResource(ch); err != nil {
				var ae *amqp.Error
				if !errors.As(err, &ae) || ae.Code != amqp.NotFound {
					t.Errorf("cleanup %q: %v", name, err)
				}
			}
		}
		for _, q := range append(f.queues(), f.extra...) {
			remove(q, func(ch *amqp.Channel) error {
				_, err := ch.QueueDelete(q, false, false, false)
				return err
			})
		}
		for _, name := range []string{exchange, exchange + ".dlx"} {
			remove(name, func(ch *amqp.Channel) error { return ch.ExchangeDelete(name, false, false) })
		}
	})
	return f
}

// queues returns the group queues a test may have declared.
func (f *fixture) queues() []string {
	var qs []string
	for _, g := range []string{"g", "audit", "billing", "app"} {
		for _, topic := range []string{"orders", "payments"} {
			g := f.exchange + "-" + g
			qs = append(qs, QueueName(topic, g), DeadLetterQueueName(topic, g))
		}
	}
	return qs
}

// track registers the group queues of (topic, group) for deletion.
func (f *fixture) track(topic, group string) {
	f.extra = append(f.extra, QueueName(topic, group), DeadLetterQueueName(topic, group))
}

// group returns a group name unique to the fixture.
func (f *fixture) group(name string) string { return f.exchange + "-" + name }

type testSub struct {
	cancel context.CancelFunc
	done   chan error
}

func (f *fixture) subscribe(t *testing.T, topic, group string, h endpoint.Endpoint) *testSub {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	s := &testSub{cancel: cancel, done: make(chan error, 1)}
	b := f.broker
	go func() {
		s.done <- b.Subscribe(ctx, topic, group, h)
		close(s.done)
	}()
	t.Cleanup(func() {
		if err := s.stop(t); err != nil {
			t.Errorf("Subscribe() during cleanup = %v", err)
		}
	})
	select {
	case <-f.ready:
	case err := <-s.done:
		t.Fatalf("Subscribe() returned early: %v", err)
	case <-time.After(waitTimeout):
		t.Fatal("subscription not ready")
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
		time.Sleep(10 * time.Millisecond)
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

// dead reads the messages in the dead letter queue of group.
func (f *fixture) dead(t *testing.T, topic, group string) []amqp.Delivery {
	t.Helper()
	ch, err := f.conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	var out []amqp.Delivery
	for {
		d, ok, err := ch.Get(DeadLetterQueueName(topic, group), true)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			return out
		}
		out = append(out, d)
	}
}

// queueDepth returns the number of ready messages in queue.
func (f *fixture) queueDepth(t *testing.T, queue string) int {
	t.Helper()
	ch, err := f.conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	return q.Messages
}

func TestIntegrationMessageMapping(t *testing.T) {
	f := newFixture(t)
	var c collector
	s := f.subscribe(t, "orders", f.group("g"), c.handle)
	in := &broker.Message{
		Topic:  "orders",
		Key:    "k1",
		Header: metadata.Join(metadata.Pairs("trace-id", "abc", "multi", "1"), metadata.Pairs("multi", "2")),
		Body:   []byte("payload"),
	}
	if err := f.broker.Publish(t.Context(), in); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 1, &c)
	if err := s.stop(t); err != nil {
		t.Fatal(err)
	}
	got := c.msgs[0]
	if got.Topic != "orders" || got.Key != "k1" || string(got.Body) != "payload" || got.Header.Get("trace-id") != "abc" ||
		!slices.Equal(got.Header.Values("multi"), []string{"1", "2"}) {
		t.Fatalf("message = %+v", got)
	}
}

func TestIntegrationGroupAndBroadcastDelivery(t *testing.T) {
	f := newFixture(t)
	var a1, a2, billing, other, b1, b2 collector
	f.subscribe(t, "orders", f.group("audit"), a1.handle)
	f.subscribe(t, "orders", f.group("audit"), a2.handle)
	f.subscribe(t, "orders", f.group("billing"), billing.handle)
	f.subscribe(t, "payments", f.group("audit"), other.handle)
	f.subscribe(t, "orders", "", b1.handle)
	f.subscribe(t, "orders", "", b2.handle)

	var keys []string
	for i := range 20 {
		keys = append(keys, fmt.Sprintf("m%02d", i))
	}
	f.publish(t, "orders", keys...)
	waitFor(t, len(keys), &a1, &a2)
	waitFor(t, len(keys), &billing)
	waitFor(t, len(keys), &b1)
	waitFor(t, len(keys), &b2)
	time.Sleep(100 * time.Millisecond)

	audit := append(a1.keys(), a2.keys()...)
	slices.Sort(audit)
	if !slices.Equal(audit, keys) {
		t.Errorf("audit group got %v, want each of %v once", audit, keys)
	}
	if a1.len() == 0 || a2.len() == 0 {
		t.Errorf("audit members got %d and %d messages, want both to share the work", a1.len(), a2.len())
	}
	for name, c := range map[string]*collector{"billing": &billing, "broadcast 1": &b1, "broadcast 2": &b2} {
		if got := c.keys(); !slices.Equal(got, keys) {
			t.Errorf("%s got %v, want %v", name, got, keys)
		}
	}
	if n := other.len(); n != 0 {
		t.Errorf("other topic got %d messages", n)
	}
}

func TestIntegrationAckNackAndDeadLetter(t *testing.T) {
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
		{"plain error is not retryable", nil, 100, errors.New("bad"), 1, true},
		{"explicitly non-retryable", nil, 100, unavailable.WithRetryable(false), 1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				mu       sync.Mutex
				backoffs []int
				counts   []string
			)
			opts := append(slices.Clone(tt.opts), WithBackoff(func(attempt int) time.Duration {
				mu.Lock()
				defer mu.Unlock()
				backoffs = append(backoffs, attempt)
				return time.Millisecond
			}))
			f := newFixture(t, opts...)
			group := f.group("g")
			var attempts atomic.Int32
			s := f.subscribe(t, "orders", group, func(_ context.Context, req any) (any, error) {
				msg := req.(*broker.Message)
				mu.Lock()
				counts = append(counts, msg.Key+"/"+msg.Topic)
				mu.Unlock()
				if n := int(attempts.Add(1)); n <= tt.failures {
					return nil, tt.err
				}
				return nil, nil
			})
			if err := f.broker.Publish(t.Context(), &broker.Message{Topic: "orders", Key: "k", Header: metadata.Pairs("x-id", "1")}); err != nil {
				t.Fatal(err)
			}
			eventually(t, "attempts", func() bool { return int(attempts.Load()) >= tt.wantAttempts })
			var dead []amqp.Delivery
			if tt.wantDead {
				eventually(t, "dead letter", func() bool {
					dead = append(dead, f.dead(t, "orders", group)...)
					return len(dead) > 0
				})
			}
			time.Sleep(200 * time.Millisecond) // no further redelivery
			if err := s.stop(t); err != nil {
				t.Fatalf("Subscribe() = %v", err)
			}
			dead = append(dead, f.dead(t, "orders", group)...)
			if got := int(attempts.Load()); got != tt.wantAttempts {
				t.Errorf("attempts = %d, want %d", got, tt.wantAttempts)
			}
			mu.Lock()
			defer mu.Unlock()
			if want := tt.wantAttempts - 1; len(backoffs) != want {
				t.Errorf("backoff calls = %v, want %d", backoffs, want)
			}
			for _, c := range counts {
				if c != "k/orders" {
					t.Errorf("redelivered message lost key or topic: %v", counts)
					break
				}
			}
			if tt.wantDead != (len(dead) == 1) || len(dead) > 1 {
				t.Fatalf("dead letters = %d, want dead=%v", len(dead), tt.wantDead)
			}
			if tt.wantDead {
				msg := toMessage(dead[0])
				if msg.Key != "k" || msg.Header.Get("x-id") != "1" || msg.Topic != "orders" && msg.Topic != QueueName("orders", group) {
					t.Errorf("dead letter = %+v", msg)
				}
			}
			if n := f.queueDepth(t, QueueName("orders", group)); n != 0 {
				t.Errorf("queue depth = %d, want 0", n)
			}
		})
	}
}

func TestIntegrationInterruptedMessageIsRequeued(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  func(ctx context.Context) error
	}{
		{"context error", func(ctx context.Context) error { return ctx.Err() }},
		{"retryable error while stopping", func(context.Context) error { return errorx.New(errorx.Unavailable, "DOWN", "down") }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			group := f.group("g")
			handling := make(chan struct{})
			var once sync.Once
			s := f.subscribe(t, "orders", group, func(ctx context.Context, _ any) (any, error) {
				once.Do(func() { close(handling) })
				<-ctx.Done()
				return nil, tt.err(ctx)
			})
			f.publish(t, "orders", "m0")
			<-handling
			if err := s.stop(t); err != nil {
				t.Fatal(err)
			}
			var c collector
			f.subscribe(t, "orders", group, c.handle)
			waitFor(t, 1, &c)
			if got := c.keys(); !slices.Equal(got, []string{"m0"}) {
				t.Fatalf("redelivered %v, want [m0]", got)
			}
			if d := f.dead(t, "orders", group); len(d) != 0 {
				t.Fatalf("dead letters = %d, want 0", len(d))
			}
		})
	}
}

func TestIntegrationBrokerCloseRequeuesExhaustedHandler(t *testing.T) {
	f := newFixture(t, WithMaxRedeliveries(0))
	group := f.group("g")
	closedBroker := f.broker
	s := f.subscribe(t, "orders", group, func(context.Context, any) (any, error) {
		_ = closedBroker.Close()
		return nil, errorx.New(errorx.Unavailable, "DOWN", "down")
	})
	// Use another Broker for the initial publication: the handler closes the
	// consumer's Broker, which must leave the shared connection usable.
	publisher := New(f.conn, WithExchange(f.exchange))
	defer publisher.Close()
	if err := publisher.Publish(t.Context(), &broker.Message{Topic: "orders", Key: "m0"}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-s.done:
		if err != nil {
			t.Fatalf("Subscribe() = %v, want nil after Close", err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("Subscribe did not return after the handler closed the Broker")
	}
	f.broker = New(f.conn, WithExchange(f.exchange), withSubscribed(func(topic, group string) {
		f.ready <- topic + "/" + group
	}))
	var c collector
	next := f.subscribe(t, "orders", group, c.handle)
	waitFor(t, 1, &c)
	if err := next.stop(t); err != nil {
		t.Fatal(err)
	}
	if got := c.keys(); !slices.Equal(got, []string{"m0"}) {
		t.Errorf("redelivered messages = %v, want [m0]", got)
	}
	if dead := f.dead(t, "orders", group); len(dead) != 0 {
		t.Errorf("dead letters = %d, want 0", len(dead))
	}
}

func TestIntegrationNonRetryableErrorWhileStoppingIsDeadLettered(t *testing.T) {
	f := newFixture(t)
	group := f.group("g")
	handling := make(chan struct{})
	s := f.subscribe(t, "orders", group, func(ctx context.Context, _ any) (any, error) {
		close(handling)
		<-ctx.Done()
		return nil, errorx.New(errorx.InvalidArgument, "BAD", "bad message")
	})
	f.publish(t, "orders", "m0")
	<-handling
	if err := s.stop(t); err != nil {
		t.Fatal(err)
	}
	var dead []amqp.Delivery
	eventually(t, "dead letter", func() bool {
		dead = append(dead, f.dead(t, "orders", group)...)
		return len(dead) > 0
	})
	if k := toMessage(dead[0]).Key; k != "m0" || len(dead) != 1 {
		t.Fatalf("dead letters = %d, first key %q", len(dead), k)
	}
}

func TestIntegrationConnectionLoss(t *testing.T) {
	for _, group := range []string{"g", ""} {
		t.Run(fmt.Sprintf("group=%q", group), func(t *testing.T) {
			f := newFixture(t)
			if group != "" {
				group = f.group(group)
			}
			s := f.subscribe(t, "orders", group, (&collector{}).handle)
			f.conn.Close()
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

func TestIntegrationQueueArgumentMismatch(t *testing.T) {
	f := newFixture(t)
	group := f.group("g")
	ch, err := f.conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	// A queue with the same name but without dead lettering.
	if _, err := ch.QueueDeclare(QueueName("orders", group), true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	ch.Close()
	err = f.broker.Subscribe(t.Context(), "orders", group, (&collector{}).handle)
	if errorx.CodeOf(err) != errorx.FailedPrecondition {
		t.Fatalf("Subscribe() = %v, want FailedPrecondition", err)
	}
}

func TestIntegrationWithConsumerInApp(t *testing.T) {
	f := newFixture(t)
	got := make(chan metadata.Info, 1)
	consumer := broker.NewConsumer(f.broker, "orders", f.group("app"), func(ctx context.Context, req any) (any, error) {
		info, _ := metadata.FromContext(ctx)
		got <- info
		return nil, nil
	})
	a := app.New(app.WithLogger(slog.New(slog.DiscardHandler)), app.WithHooks(consumer))
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
	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if s := a.State(); s != app.StateStopped {
		t.Fatalf("State() = %v, want stopped", s)
	}
}

func TestIntegrationConnectionLossFailsApp(t *testing.T) {
	f := newFixture(t)
	consumer := broker.NewConsumer(f.broker, "orders", f.group("app"), (&collector{}).handle)
	a := app.New(app.WithLogger(slog.New(slog.DiscardHandler)), app.WithHooks(consumer))
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(t.Context()) }()
	<-f.ready
	f.conn.Close()
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

// keysOf returns the message keys of deliveries.
func keysOf(ds []amqp.Delivery) []string {
	var keys []string
	for _, d := range ds {
		keys = append(keys, toMessage(d).Key)
	}
	return keys
}

// reject is a handler that dead letters every message.
func reject(context.Context, any) (any, error) {
	return nil, errorx.New(errorx.InvalidArgument, "BAD", "bad message")
}

func TestIntegrationGroupQueuesDoNotCollide(t *testing.T) {
	t.Run("dots in group and topic", func(t *testing.T) {
		f := newFixture(t)
		// Both pairs were mapped to the queue "<prefix>-a.b.c".
		g1, g2 := f.group("a.b"), f.group("a")
		f.track("c", g1)
		f.track("b.c", g2)
		var c1, c2 collector
		f.subscribe(t, "c", g1, c1.handle)
		f.subscribe(t, "b.c", g2, c2.handle)
		f.publish(t, "c", "c0", "c1", "c2")
		f.publish(t, "b.c", "bc0", "bc1", "bc2")
		waitFor(t, 3, &c1)
		waitFor(t, 3, &c2)
		time.Sleep(200 * time.Millisecond)
		if got := c1.keys(); !slices.Equal(got, []string{"c0", "c1", "c2"}) {
			t.Errorf("group %q on c got %v", g1, got)
		}
		if got := c2.keys(); !slices.Equal(got, []string{"bc0", "bc1", "bc2"}) {
			t.Errorf("group %q on b.c got %v", g2, got)
		}
	})
	t.Run("dead letter queue and group queue", func(t *testing.T) {
		f := newFixture(t)
		g := f.group("g")
		// The dead letter queue of (orders, g) was the queue of (orders.dlq, g).
		f.track("orders.dlq", g)
		var other collector
		f.subscribe(t, "orders.dlq", g, other.handle)
		f.subscribe(t, "orders", g, reject)
		f.publish(t, "orders", "m0")
		var dead []amqp.Delivery
		eventually(t, "dead letter", func() bool {
			dead = append(dead, f.dead(t, "orders", g)...)
			return len(dead) > 0
		})
		time.Sleep(200 * time.Millisecond)
		if got := keysOf(dead); !slices.Equal(got, []string{"m0"}) {
			t.Errorf("dead letters = %v, want [m0]", got)
		}
		if got := other.keys(); len(got) != 0 {
			t.Errorf("group queue of orders.dlq got dead letters %v", got)
		}
	})
}

func TestIntegrationWildcardTopicDeadLetters(t *testing.T) {
	f := newFixture(t)
	g := f.group("g")
	topics := []string{"orders.created", "orders.*", "#"}
	for _, topic := range topics {
		f.track(topic, g)
		f.subscribe(t, topic, g, reject)
	}
	// Every queue receives the message and dead letters it once.
	f.publish(t, "orders.created", "m0")
	dead := make(map[string][]amqp.Delivery)
	eventually(t, "dead letters", func() bool {
		for _, topic := range topics {
			dead[topic] = append(dead[topic], f.dead(t, topic, g)...)
		}
		for _, topic := range topics {
			if len(dead[topic]) == 0 {
				return false
			}
		}
		return true
	})
	time.Sleep(300 * time.Millisecond)
	for _, topic := range topics {
		dead[topic] = append(dead[topic], f.dead(t, topic, g)...)
		if got := keysOf(dead[topic]); !slices.Equal(got, []string{"m0"}) {
			t.Errorf("dead letter queue of %q holds %v, want [m0]", topic, got)
		}
	}
}

func TestIntegrationBroadcastDeadLettersStayOutOfGroupQueues(t *testing.T) {
	f := newFixture(t)
	g := f.group("g")
	// A broadcast topic equal to the name of a group queue.
	topic := QueueName("orders", g)
	f.subscribe(t, "orders", g, (&collector{}).handle)
	ch, err := f.conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	probe, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.QueueBind(probe.Name, QueueName(topic, ""), f.exchange+".dlx", false, nil); err != nil {
		t.Fatal(err)
	}
	f.subscribe(t, topic, "", reject)
	f.publish(t, topic, "m0")
	eventually(t, "broadcast dead letter", func() bool {
		return f.queueDepth(t, probe.Name) == 1
	})
	if got := keysOf(f.dead(t, "orders", g)); len(got) != 0 {
		t.Errorf("dead letter queue of group %q got broadcast dead letters %v", g, got)
	}
}
