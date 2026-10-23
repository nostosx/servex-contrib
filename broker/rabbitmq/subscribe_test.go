package rabbitmq

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging/broker"
)

// newFakeBroker returns a Broker on a new fake server. Its Subscribe calls
// report the topic and group they consume to the returned channel.
func newFakeBroker(t *testing.T, opts ...Option) (*fakeServer, *Broker, <-chan string) {
	t.Helper()
	srv := newFakeServer()
	started := make(chan string, 16)
	opts = append(opts, withSubscribed(func(topic, group string) { started <- topic + "/" + group }))
	b := newBroker(srv.open, opts...)
	t.Cleanup(func() { _ = b.Close() })
	return srv, b, started
}

// running is a Subscribe call running in the background of a synctest
// bubble.
type running struct {
	cancel context.CancelFunc
	done   chan error
}

// subscribe starts b.Subscribe and waits until it consumes or returns.
func subscribe(t *testing.T, b *Broker, topic, group string, h endpoint.Endpoint) *running {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	r := &running{cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- b.Subscribe(ctx, topic, group, h) }()
	t.Cleanup(func() {
		cancel()
		synctest.Wait()
	})
	synctest.Wait()
	return r
}

// result returns what Subscribe returned; it fails if Subscribe still runs.
func (r *running) result(t *testing.T) error {
	t.Helper()
	synctest.Wait()
	select {
	case err := <-r.done:
		return err
	default:
		t.Fatal("Subscribe is still running")
		return nil
	}
}

// stop cancels the Subscribe context and returns what Subscribe returned.
func (r *running) stop(t *testing.T) error {
	t.Helper()
	r.cancel()
	return r.result(t)
}

// ack is a handler that accepts every message.
func ack(context.Context, any) (any, error) { return nil, nil }

var errDown = errorx.New(errorx.Unavailable, "DOWN", "down")

// reasonOf returns the errorx reason of err.
func reasonOf(err error) string {
	if e := errorx.From(err); e != nil {
		return e.Reason
	}
	return ""
}

func TestPublishConfirms(t *testing.T) {
	srv, b, _ := newFakeBroker(t)
	msg := &broker.Message{Topic: "orders", Key: "k", Body: []byte("x")}
	for range 2 {
		if err := b.Publish(t.Context(), msg); err != nil {
			t.Fatalf("Publish() = %v", err)
		}
	}
	if len(srv.channels) != 1 || srv.exchanges["servex"] != amqp.ExchangeTopic {
		t.Fatalf("channels = %d, exchanges = %v, want one channel and the topic exchange", len(srv.channels), srv.exchanges)
	}
	pubs := srv.publications()
	if len(pubs) != 2 {
		t.Fatalf("publications = %d, want 2", len(pubs))
	}
	if p := pubs[0]; p.exchange != "servex" || p.key != "orders" || p.msg.Headers[HeaderKey] != "k" ||
		p.msg.DeliveryMode != amqp.Persistent || string(p.msg.Body) != "x" {
		t.Errorf("publication = %+v", p)
	}

	// A channel closed by the server is replaced by the next Publish.
	srv.channels[0].shutdown(&amqp.Error{Code: amqp.ChannelError, Reason: "gone"})
	if err := b.Publish(t.Context(), msg); err != nil {
		t.Fatalf("Publish() after channel loss = %v", err)
	}
	if len(srv.channels) != 2 {
		t.Fatalf("channels = %d, want a new channel", len(srv.channels))
	}

	srv.nack = true
	if err := b.Publish(t.Context(), msg); errorx.CodeOf(err) != errorx.Unavailable || reasonOf(err) != "RABBITMQ_NACKED" {
		t.Errorf("Publish() of a nacked message = %v, want Unavailable RABBITMQ_NACKED", err)
	}
	srv.nack = false
	// Confirmations pending on a closing channel resolve as nacks; the
	// channel loss is reported, not a refusal by the broker.
	srv.dropConfirms = true
	if err := b.Publish(t.Context(), msg); errorx.CodeOf(err) != errorx.Unavailable ||
		reasonOf(err) != "RABBITMQ_UNAVAILABLE" || !errors.Is(err, amqp.ErrClosed) {
		t.Errorf("Publish() on a channel closed before the confirmation = %v, want Unavailable caused by amqp.ErrClosed", err)
	}
	// The same race with Close reports the closed Broker.
	srv.onPublish = func() { b.cancel() }
	if err := b.Publish(t.Context(), msg); errorx.CodeOf(err) != errorx.FailedPrecondition || reasonOf(err) != "RABBITMQ_CLOSED" {
		t.Errorf("Publish() racing Close = %v, want FailedPrecondition RABBITMQ_CLOSED", err)
	}
	b = newBroker(srv.open)
	t.Cleanup(func() { _ = b.Close() })
	srv.dropConfirms, srv.onPublish = false, nil
	srv.fail("publish", amqp.ErrClosed)
	if err := b.Publish(t.Context(), msg); errorx.CodeOf(err) != errorx.Unavailable || !errors.Is(err, amqp.ErrClosed) {
		t.Errorf("Publish() on a failing channel = %v, want Unavailable caused by amqp.ErrClosed", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := b.Publish(ctx, msg); errorx.CodeOf(err) != errorx.Canceled {
		t.Errorf("Publish() with a canceled context = %v, want Canceled", err)
	}

	// Close closes the publishing channel.
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if n := srv.openChannels(); n != 0 {
		t.Errorf("open channels after Close = %d, want 0", n)
	}
}

func TestPublishChannelSetupFailures(t *testing.T) {
	for _, tt := range []struct {
		op   string
		err  error
		want errorx.Code
	}{
		{"open", amqp.ErrClosed, errorx.Unavailable},
		{"confirm", &amqp.Error{Code: amqp.NotImplemented, Reason: "no confirms"}, errorx.Internal},
		{"exchange servex", &amqp.Error{Code: amqp.AccessRefused, Reason: "denied"}, errorx.PermissionDenied},
	} {
		t.Run(tt.op, func(t *testing.T) {
			srv, b, _ := newFakeBroker(t)
			srv.fail(tt.op, tt.err)
			msg := &broker.Message{Topic: "orders"}
			if err := b.Publish(t.Context(), msg); errorx.CodeOf(err) != tt.want || !errors.Is(err, tt.err) {
				t.Fatalf("Publish() = %v, want %v caused by %v", err, tt.want, tt.err)
			}
			if n := srv.openChannels(); n != 0 {
				t.Errorf("open channels = %d, want the failed channel closed", n)
			}
			// The next Publish opens a new channel.
			srv.fail(tt.op, nil)
			if err := b.Publish(t.Context(), msg); err != nil {
				t.Fatalf("Publish() after recovery = %v", err)
			}
		})
	}
}

func TestSubscribeDeclaresTopology(t *testing.T) {
	t.Run("group", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			srv, b, started := newFakeBroker(t, WithPrefetch(5))
			r := subscribe(t, b, "orders.*", "billing", ack)
			if got := <-started; got != "orders.*/billing" {
				t.Errorf("started = %q", got)
			}
			queue, dlq := QueueName("orders.*", "billing"), DeadLetterQueueName("orders.*", "billing")
			want := map[string]fakeQueue{
				queue: {durable: true, args: amqp.Table{"x-dead-letter-exchange": "servex.dlx", "x-dead-letter-routing-key": queue}},
				dlq:   {durable: true},
			}
			checkTopology(t, srv, want, []string{"servex.dlx " + queue + " -> " + dlq, "servex orders.* -> " + queue})
			srv.mu.Lock()
			prefetch := srv.prefetch
			srv.mu.Unlock()
			if prefetch != 5 {
				t.Errorf("prefetch = %d, want 5", prefetch)
			}
			if err := r.stop(t); err != nil {
				t.Fatalf("Subscribe() = %v", err)
			}
			if n := srv.openChannels(); n != 0 {
				t.Errorf("open channels = %d, want 0", n)
			}
		})
	})
	t.Run("broadcast", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			srv, b, _ := newFakeBroker(t)
			r := subscribe(t, b, "orders", "", ack)
			want := map[string]fakeQueue{
				"amq.gen-1": {autoDelete: true, exclusive: true, args: amqp.Table{
					"x-dead-letter-exchange": "servex.dlx", "x-dead-letter-routing-key": QueueName("orders", ""),
				}},
			}
			checkTopology(t, srv, want, []string{"servex orders -> amq.gen-1"})
			if err := r.stop(t); err != nil {
				t.Fatalf("Subscribe() = %v", err)
			}
		})
	})
}

// checkTopology compares the exchanges, queues and bindings declared on srv.
func checkTopology(t *testing.T, srv *fakeServer, queues map[string]fakeQueue, bindings []string) {
	t.Helper()
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if want := map[string]string{"servex": amqp.ExchangeTopic, "servex.dlx": amqp.ExchangeDirect}; !maps.Equal(srv.exchanges, want) {
		t.Errorf("exchanges = %v, want %v", srv.exchanges, want)
	}
	if !maps.EqualFunc(srv.queues, queues, func(a, b fakeQueue) bool {
		return a.durable == b.durable && a.autoDelete == b.autoDelete && a.exclusive == b.exclusive && maps.Equal(a.args, b.args)
	}) {
		t.Errorf("queues = %+v, want %+v", srv.queues, queues)
	}
	if !slices.Equal(srv.bindings, bindings) {
		t.Errorf("bindings = %q, want %q", srv.bindings, bindings)
	}
}

// delayedCancellation keeps the broker's AfterFunc callback pending after Close,
// so tests exercise shutdown without relying on goroutine scheduling.
type delayedCancellation struct {
	context.Context
	release <-chan struct{}
}

func (c delayedCancellation) Value(any) any { return nil }

func (c delayedCancellation) AfterFunc(f func()) func() bool {
	return context.AfterFunc(c.Context, func() {
		<-c.release
		f()
	})
}

func delayBrokerCancellation(t *testing.T, b *Broker) {
	t.Helper()
	release := make(chan struct{})
	b.ctx = delayedCancellation{Context: b.ctx, release: release}
	t.Cleanup(func() { close(release) })
}

// closeDuringSubscribeChannel closes the broker at a specified setup step.
type closeDuringSubscribeChannel struct {
	amqpChannel
	qos     func() error
	consume func() error
}

func (ch closeDuringSubscribeChannel) Qos(count, size int, global bool) error {
	if ch.qos != nil {
		if err := ch.qos(); err != nil {
			return err
		}
	}
	return ch.amqpChannel.Qos(count, size, global)
}

func (ch closeDuringSubscribeChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	if ch.consume != nil {
		if err := ch.consume(); err != nil {
			return nil, err
		}
	}
	return ch.amqpChannel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

func TestSubscribeCloseDuringSetup(t *testing.T) {
	for _, step := range []string{"open", "qos failure", "qos success", "consume"} {
		t.Run(step, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				srv, b, started := newFakeBroker(t)
				delayBrokerCancellation(t, b)
				b.open = func() (amqpChannel, error) {
					if step == "open" {
						_ = b.Close()
						return nil, amqp.ErrClosed
					}
					ch, err := srv.open()
					if err != nil {
						return nil, err
					}
					wrapped := closeDuringSubscribeChannel{amqpChannel: ch}
					if step == "consume" {
						wrapped.consume = func() error {
							_ = b.Close()
							return amqp.ErrClosed
						}
					} else {
						wrapped.qos = func() error {
							_ = b.Close()
							if step == "qos failure" {
								return amqp.ErrClosed
							}
							return nil
						}
					}
					return wrapped, nil
				}
				r := subscribe(t, b, "orders", "g", ack)
				if err := r.result(t); err != nil {
					t.Errorf("Subscribe() = %v, want nil after Close", err)
				}
				select {
				case <-started:
					t.Error("subscription started after Close")
				default:
				}
				if n := srv.openChannels(); n != 0 {
					t.Errorf("open channels = %d, want 0", n)
				}
			})
		})
	}
}

func TestSubscribeSetupFailures(t *testing.T) {
	notFound := &amqp.Error{Code: amqp.NotFound, Reason: "no exchange"}
	locked := &amqp.Error{Code: amqp.PreconditionFailed, Reason: "inequivalent arg"}
	queue, dlq := QueueName("orders", "g"), DeadLetterQueueName("orders", "g")
	for _, tt := range []struct {
		op, group string
		err       error
		want      errorx.Code
	}{
		{"open", "g", amqp.ErrClosed, errorx.Unavailable},
		{"exchange servex", "g", notFound, errorx.NotFound},
		{"exchange servex.dlx", "g", notFound, errorx.NotFound},
		{"queue " + queue, "g", locked, errorx.FailedPrecondition},
		{"queue " + dlq, "g", locked, errorx.FailedPrecondition},
		{"bind " + dlq, "g", notFound, errorx.NotFound},
		{"bind " + queue, "g", notFound, errorx.NotFound},
		{"queue ", "", locked, errorx.FailedPrecondition},
		{"bind amq.gen-1", "", notFound, errorx.NotFound},
		{"qos", "g", amqp.ErrClosed, errorx.Unavailable},
		{"consume", "g", &amqp.Error{Code: amqp.AccessRefused, Reason: "exclusive consumer"}, errorx.PermissionDenied},
	} {
		t.Run(tt.op, func(t *testing.T) {
			srv, b, _ := newFakeBroker(t)
			srv.fail(tt.op, tt.err)
			err := b.Subscribe(t.Context(), "orders", tt.group, ack)
			if errorx.CodeOf(err) != tt.want || !errors.Is(err, tt.err) {
				t.Fatalf("Subscribe() = %v, want %v caused by %v", err, tt.want, tt.err)
			}
			if n := srv.openChannels(); n != 0 {
				t.Errorf("open channels = %d, want 0", n)
			}

			// A failure after the subscription ended is not reported.
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if err := b.Subscribe(ctx, "orders", tt.group, ack); err != nil {
				t.Errorf("Subscribe() with a canceled context = %v, want nil", err)
			}
		})
	}
}

// delivery returns a delivery of topic carrying key and, if redelivered is
// positive, the redelivery counter.
func delivery(topic, key string, redelivered int) amqp.Delivery {
	h := amqp.Table{HeaderKey: key}
	if redelivered > 0 {
		h[HeaderRedeliveries] = int64(redelivered)
	}
	return amqp.Delivery{RoutingKey: topic, Headers: h, DeliveryMode: amqp.Persistent, Body: []byte(key)}
}

func TestSubscribeSettlesDeliveries(t *testing.T) {
	for _, tt := range []struct {
		name        string
		redelivered int
		err         error
		settled     string
		republished bool
	}{
		{"ack", 0, nil, "ack 1", false},
		{"plain error is dead lettered", 0, errors.New("bad"), "dead 1", false},
		{"non-retryable is dead lettered", 0, errDown.WithRetryable(false), "dead 1", false},
		{"retryable is republished", 1, errDown, "ack 1", true},
		{"retryable exhausted is dead lettered", 3, errDown, "dead 1", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				srv, b, _ := newFakeBroker(t)
				handled := make(chan *broker.Message, 1)
				r := subscribe(t, b, "orders", "g", func(_ context.Context, req any) (any, error) {
					handled <- req.(*broker.Message)
					return nil, tt.err
				})
				srv.consumer().deliver(delivery("orders", "k", tt.redelivered))
				synctest.Wait()
				if got := <-handled; got.Topic != "orders" || got.Key != "k" || string(got.Body) != "k" {
					t.Fatalf("handled message = %+v", got)
				}
				if s := srv.settlements(); !slices.Equal(s, []string{tt.settled}) {
					t.Errorf("settlements = %q, want [%s]", s, tt.settled)
				}
				pubs := srv.publications()
				if !tt.republished {
					if len(pubs) != 0 {
						t.Errorf("publications = %+v, want none", pubs)
					}
				} else if len(pubs) != 1 || pubs[0].exchange != "" || pubs[0].key != QueueName("orders", "g") ||
					pubs[0].msg.Headers[HeaderRedeliveries] != int64(tt.redelivered+1) ||
					pubs[0].msg.Headers[HeaderTopic] != "orders" || pubs[0].msg.Headers[HeaderKey] != "k" {
					t.Errorf("publications = %+v, want one redelivery to the queue", pubs)
				}
				if err := r.stop(t); err != nil {
					t.Fatalf("Subscribe() = %v", err)
				}
			})
		})
	}
}

func TestSubscribeRedeliveryBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			mu       sync.Mutex
			attempts []int
		)
		srv, b, _ := newFakeBroker(t, WithBackoff(func(attempt int) time.Duration {
			mu.Lock()
			defer mu.Unlock()
			attempts = append(attempts, attempt)
			return time.Minute
		}))
		r := subscribe(t, b, "orders", "g", func(context.Context, any) (any, error) { return nil, errDown })
		ch := srv.consumer()

		ch.deliver(delivery("orders", "k", 1))
		synctest.Wait()
		if n := len(srv.publications()); n != 0 {
			t.Fatalf("republished before the backoff elapsed: %d", n)
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		if pubs := srv.publications(); len(pubs) != 1 || pubs[0].msg.Headers[HeaderRedeliveries] != int64(2) {
			t.Fatalf("publications = %+v, want redelivery 2", pubs)
		}

		// Stopping during the backoff requeues the message instead.
		ch.deliver(delivery("orders", "k", 1))
		synctest.Wait()
		if err := r.stop(t); err != nil {
			t.Fatalf("Subscribe() = %v", err)
		}
		if s := srv.settlements(); !slices.Equal(s, []string{"ack 1", "requeue 2"}) {
			t.Errorf("settlements = %q, want [ack 1 requeue 2]", s)
		}
		if n := len(srv.publications()); n != 1 {
			t.Errorf("publications = %d, want 1", n)
		}
		mu.Lock()
		defer mu.Unlock()
		if !slices.Equal(attempts, []int{2, 2}) {
			t.Errorf("backoff attempts = %v, want [2 2]", attempts)
		}
	})
}

func TestSubscribeRedeliveryFailure(t *testing.T) {
	t.Run("ends the subscription", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			srv, b, _ := newFakeBroker(t)
			r := subscribe(t, b, "orders", "g", func(context.Context, any) (any, error) { return nil, errDown })
			srv.fail("publish", amqp.ErrClosed)
			srv.consumer().deliver(delivery("orders", "k", 0))
			if err := r.result(t); errorx.CodeOf(err) != errorx.Unavailable || !errors.Is(err, amqp.ErrClosed) {
				t.Fatalf("Subscribe() = %v, want Unavailable caused by amqp.ErrClosed", err)
			}
			if s := srv.settlements(); !slices.Equal(s, []string{"requeue 1"}) {
				t.Errorf("settlements = %q, want [requeue 1]", s)
			}
		})
	})
	t.Run("while stopping", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			srv, b, _ := newFakeBroker(t)
			r := subscribe(t, b, "orders", "g", func(context.Context, any) (any, error) { return nil, errDown })
			// The subscription ends while the message is republished.
			srv.onPublish = r.cancel
			srv.fail("publish", amqp.ErrClosed)
			srv.consumer().deliver(delivery("orders", "k", 0))
			if err := r.result(t); err != nil {
				t.Fatalf("Subscribe() = %v, want nil", err)
			}
			if s := srv.settlements(); !slices.Equal(s, []string{"requeue 1"}) {
				t.Errorf("settlements = %q, want [requeue 1]", s)
			}
		})
	})
	t.Run("after Close", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			srv, b, _ := newFakeBroker(t)
			// A handler may close the Broker before returning a retryable
			// rejection. The message must remain available for a new consumer.
			r := subscribe(t, b, "orders", "g", func(context.Context, any) (any, error) {
				_ = b.Close()
				return nil, errDown
			})
			srv.consumer().deliver(delivery("orders", "k", 0))
			if err := r.result(t); err != nil {
				t.Fatalf("Subscribe() = %v, want nil after Close", err)
			}
			if s := srv.settlements(); !slices.Equal(s, []string{"requeue 1"}) {
				t.Errorf("settlements = %q, want [requeue 1]", s)
			}
		})
	})
}

func TestSubscribeInterruptedHandler(t *testing.T) {
	for _, tt := range []struct {
		name    string
		err     func(ctx context.Context) error
		settled string
	}{
		{"context error is requeued", func(ctx context.Context) error { return ctx.Err() }, "requeue 1"},
		{"retryable error is requeued", func(context.Context) error { return errDown }, "requeue 1"},
		{"non-retryable error is dead lettered", func(context.Context) error {
			return errorx.New(errorx.InvalidArgument, "BAD", "bad")
		}, "dead 1"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				srv, b, _ := newFakeBroker(t)
				r := subscribe(t, b, "orders", "g", func(ctx context.Context, _ any) (any, error) {
					<-ctx.Done()
					return nil, tt.err(ctx)
				})
				srv.consumer().deliver(delivery("orders", "k", 0))
				synctest.Wait()
				if err := r.stop(t); err != nil {
					t.Fatalf("Subscribe() = %v", err)
				}
				if s := srv.settlements(); !slices.Equal(s, []string{tt.settled}) {
					t.Errorf("settlements = %q, want [%s]", s, tt.settled)
				}
				if n := len(srv.publications()); n != 0 {
					t.Errorf("publications = %d, want 0", n)
				}
			})
		})
	}
}

func TestSubscribeCloseBeforeCancellationCallback(t *testing.T) {
	for _, end := range []string{"channel failure", "buffered delivery", "retryable at limit", "context error", "non-retryable error"} {
		t.Run(end, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				srv, b, _ := newFakeBroker(t)
				delayBrokerCancellation(t, b)
				handled := false
				r := subscribe(t, b, "orders", "g", func(context.Context, any) (any, error) {
					handled = true
					_ = b.Close()
					switch end {
					case "context error":
						return nil, context.Canceled
					case "non-retryable error":
						return nil, errDown.WithRetryable(false)
					default:
						return nil, errDown
					}
				})
				ch := srv.consumer()
				want := []string{"requeue 1"}
				switch end {
				case "channel failure":
					_ = b.Close()
					ch.shutdown(&amqp.Error{Code: amqp.FrameError, Reason: "EOF"})
					want = nil
				case "buffered delivery":
					_ = b.Close()
					ch.deliver(delivery("orders", "k", 0))
					want = nil
				default:
					ch.deliver(delivery("orders", "k", b.opts.maxRedeliveries))
					if end == "non-retryable error" {
						want = []string{"dead 1"}
					}
				}
				if err := r.result(t); err != nil {
					t.Errorf("Subscribe() = %v, want nil after Close", err)
				}
				if end == "buffered delivery" && handled {
					t.Error("handler called after Close")
				}
				if got := srv.settlements(); !slices.Equal(got, want) {
					t.Errorf("settlements = %q, want %q", got, want)
				}
				if n := len(srv.publications()); n != 0 {
					t.Errorf("publications = %d, want 0", n)
				}
				if n := srv.openChannels(); n != 0 {
					t.Errorf("open channels = %d, want 0", n)
				}
			})
		})
	}
}

func TestSubscribeConcurrentClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := newFakeServer()
		b := newBroker(srv.open)
		t.Cleanup(func() { _ = b.Close() })
		const subscribers = 16
		errs := make(chan error, subscribers*2)
		var wg sync.WaitGroup
		for range subscribers {
			wg.Go(func() { errs <- b.Subscribe(t.Context(), "orders", "g", ack) })
		}
		synctest.Wait()
		b.mu.Lock()
		n := len(b.subs)
		b.mu.Unlock()
		if n != subscribers {
			t.Fatalf("registered subscriptions = %d, want %d", n, subscribers)
		}
		// Race more registrations with Close while the initial subscribers
		// are active. Every registration must either observe the closed
		// Broker or be canceled by Close.
		start := make(chan struct{})
		for range subscribers {
			wg.Go(func() {
				<-start
				errs <- b.Subscribe(t.Context(), "orders", "g", ack)
			})
		}
		wg.Go(func() {
			<-start
			_ = b.Close()
		})
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Errorf("Subscribe() = %v, want nil", err)
			}
		}
		b.mu.Lock()
		n = len(b.subs)
		b.mu.Unlock()
		if n != 0 {
			t.Errorf("registered subscriptions after return = %d, want 0", n)
		}
		if n := srv.openChannels(); n != 0 {
			t.Errorf("open channels after return = %d, want 0", n)
		}
	})
}

func TestSubscribeEnds(t *testing.T) {
	for _, tt := range []struct {
		name   string
		end    func(b *Broker, ch *fakeChannel)
		code   errorx.Code // the zero Code means that Subscribe returns nil
		reason string
		cause  error
	}{
		{"broker closed", func(b *Broker, _ *fakeChannel) { _ = b.Close() }, 0, "", nil},
		{"channel closed by the server", func(_ *Broker, ch *fakeChannel) {
			ch.shutdown(&amqp.Error{Code: amqp.ConnectionForced, Reason: "shutdown"})
		}, errorx.Unavailable, "RABBITMQ_UNAVAILABLE", nil},
		{"queue deleted", func(_ *Broker, ch *fakeChannel) {
			ch.shutdown(&amqp.Error{Code: amqp.NotFound, Reason: "queue deleted"})
		}, errorx.NotFound, "RABBITMQ_NOT_FOUND", nil},
		{"consumer canceled", func(_ *Broker, ch *fakeChannel) { ch.cancelConsumer("ctag-1") },
			errorx.Unavailable, "RABBITMQ_CONSUMER_CANCELED", nil},
		{"connection lost", func(_ *Broker, ch *fakeChannel) {
			// A reset socket or missed heartbeats, as amqp091-go reports them.
			ch.shutdown(&amqp.Error{Code: amqp.FrameError, Reason: "read tcp: connection reset by peer"})
		}, errorx.Unavailable, "RABBITMQ_UNAVAILABLE", nil},
		{"channel closed without error", func(_ *Broker, ch *fakeChannel) { ch.shutdown(nil) },
			errorx.Unavailable, "RABBITMQ_UNAVAILABLE", amqp.ErrClosed},
		{"deliveries ended", func(_ *Broker, ch *fakeChannel) { ch.endDeliveries() },
			errorx.Unavailable, "RABBITMQ_UNAVAILABLE", amqp.ErrClosed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				srv, b, _ := newFakeBroker(t)
				r := subscribe(t, b, "orders", "g", ack)
				tt.end(b, srv.consumer())
				err := r.result(t)
				if tt.code == 0 {
					if err != nil {
						t.Fatalf("Subscribe() = %v, want nil", err)
					}
				} else if errorx.CodeOf(err) != tt.code || reasonOf(err) != tt.reason ||
					tt.cause != nil && !errors.Is(err, tt.cause) {
					t.Fatalf("Subscribe() = %v, want %v %s", err, tt.code, tt.reason)
				}
				if tt.code == errorx.Unavailable && !errorx.IsRetryable(err) {
					t.Errorf("Subscribe() = %v, want a retryable error", err)
				}
				if n := srv.openChannels(); n != 0 {
					t.Errorf("open channels = %d, want 0", n)
				}
			})
		})
	}
}
