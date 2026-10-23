package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging/broker"
	"github.com/nostosx/servex/metadata"
)

// deliver simulates the delivery of p with routing key key.
func deliver(p amqp.Publishing, key string) amqp.Delivery {
	return amqp.Delivery{
		Headers:      p.Headers,
		DeliveryMode: p.DeliveryMode,
		MessageId:    p.MessageId,
		Timestamp:    p.Timestamp,
		RoutingKey:   key,
		Body:         p.Body,
	}
}

func TestMessageRoundTrip(t *testing.T) {
	in := &broker.Message{
		Topic:  "orders.created",
		Key:    "k1",
		Header: metadata.Join(metadata.Pairs("trace-id", "abc", "multi", "1"), metadata.Pairs("multi", "2")),
		Body:   []byte("payload"),
	}
	p := toPublishing(in)
	if p.DeliveryMode != amqp.Persistent {
		t.Errorf("DeliveryMode = %d, want persistent", p.DeliveryMode)
	}
	if err := p.Headers.Validate(); err != nil {
		t.Fatalf("headers are not a valid AMQP table: %v", err)
	}
	out := toMessage(deliver(p, "orders.created"))
	if out.Topic != in.Topic || out.Key != "k1" || string(out.Body) != "payload" ||
		out.Header.Get("trace-id") != "abc" || !slices.Equal(out.Header.Values("multi"), []string{"1", "2"}) ||
		out.Header.Get(HeaderKey) != "" {
		t.Fatalf("message = %+v", out)
	}
	in.Body[0] = 'X'
	if string(out.Body) != "payload" {
		t.Fatal("message body shares memory with the input")
	}

	empty := toMessage(deliver(toPublishing(&broker.Message{Topic: "t"}), "t"))
	if empty.Key != "" || empty.Header != nil || len(empty.Body) != 0 {
		t.Fatalf("empty message = %+v", empty)
	}
}

func TestHeaderValues(t *testing.T) {
	d := amqp.Delivery{RoutingKey: "t", Headers: amqp.Table{
		"str":     "s",
		"bytes":   []byte("b"),
		"int":     int32(7),
		"bool":    true,
		"list":    []any{"a", int64(2)},
		"time":    time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		"x-death": []any{amqp.Table{"count": int64(1)}},
		"table":   amqp.Table{"a": "b"},
		"nil":     nil,
	}}
	h := toMessage(d).Header
	want := map[string][]string{
		"str":   {"s"},
		"bytes": {"b"},
		"int":   {"7"},
		"bool":  {"true"},
		"list":  {"a", "2"},
		"time":  {"2026-01-02T03:04:05Z"},
	}
	for k, vs := range want {
		if got := h.Values(k); !slices.Equal(got, vs) {
			t.Errorf("header %s = %v, want %v", k, got, vs)
		}
	}
	for _, k := range []string{"x-death", "table", "nil"} {
		if h.Values(k) != nil {
			t.Errorf("header %s = %v, want it skipped", k, h.Values(k))
		}
	}
}

func TestRedeliveries(t *testing.T) {
	for _, tt := range []struct {
		v    any
		want int
	}{
		{nil, 0}, {int(2), 2}, {int8(3), 3}, {int16(4), 4}, {int32(5), 5}, {int64(6), 6},
		{uint8(7), 7}, {uint16(8), 8}, {uint32(9), 9}, {"10", 10}, {"bad", 0}, {int64(-1), 0}, {1.5, 0},
	} {
		h := amqp.Table{}
		if tt.v != nil {
			h[HeaderRedeliveries] = tt.v
		}
		if got := redeliveries(h); got != tt.want {
			t.Errorf("redeliveries(%#v) = %d, want %d", tt.v, got, tt.want)
		}
	}
}

func TestRedelivery(t *testing.T) {
	orig := toPublishing(&broker.Message{Topic: "orders.created", Key: "k", Header: metadata.Pairs("x-id", "1"), Body: []byte("v")})
	orig.MessageId = "id-1"
	d := deliver(orig, "orders.created")
	p := redelivery(d, "orders.created", 2)
	if err := p.Headers.Validate(); err != nil {
		t.Fatal(err)
	}
	if redeliveries(p.Headers) != 2 || p.MessageId != "id-1" || string(p.Body) != "v" || p.DeliveryMode != amqp.Persistent {
		t.Fatalf("redelivery = %+v", p)
	}
	if _, ok := d.Headers[HeaderRedeliveries]; ok {
		t.Fatal("redelivery modified the original headers")
	}
	// Republished through the default exchange, the routing key is the queue
	// name; the topic comes from the header.
	msg := toMessage(deliver(p, "g.orders.created"))
	if msg.Topic != "orders.created" || msg.Key != "k" || msg.Header.Get("x-id") != "1" ||
		msg.Header.Get(HeaderRedeliveries) != "" || msg.Header.Get(HeaderTopic) != "" {
		t.Fatalf("redelivered message = %+v", msg)
	}
	if p := redelivery(amqp.Delivery{}, "t", 1); redeliveries(p.Headers) != 1 {
		t.Fatalf("redelivery without headers = %+v", p.Headers)
	}
}

func TestDecide(t *testing.T) {
	active := t.Context()
	stopped, cancel := context.WithCancel(t.Context())
	cancel()
	unavailable := errorx.New(errorx.Unavailable, "DOWN", "down")
	for _, tt := range []struct {
		name         string
		ctx          context.Context
		err          error
		redeliveries int
		max          int
		want         action
	}{
		{"ack", active, nil, 0, 3, actAck},
		{"ack while stopping", stopped, nil, 0, 3, actAck},
		{"retryable", active, unavailable, 0, 3, actRetry},
		{"retryable last redelivery", active, unavailable, 2, 3, actRetry},
		{"retryable exhausted", active, unavailable, 3, 3, actDead},
		{"zero redeliveries", active, unavailable, 0, 0, actDead},
		{"wrapped retryable", active, fmt.Errorf("wrap: %w", unavailable), 0, 1, actRetry},
		{"explicitly retryable", active, errorx.New(errorx.Internal, "X", "x").WithRetryable(true), 0, 1, actRetry},
		{"plain error", active, errors.New("bad"), 0, 3, actDead},
		{"explicitly non-retryable", active, unavailable.WithRetryable(false), 0, 3, actDead},
		{"non-retryable while stopping", stopped, errorx.New(errorx.InvalidArgument, "BAD", "bad"), 0, 3, actDead},
		{"retryable while stopping", stopped, unavailable, 0, 3, actRequeue},
		{"interrupted", stopped, context.Canceled, 0, 3, actRequeue},
		{"interrupted deadline", stopped, fmt.Errorf("x: %w", context.DeadlineExceeded), 0, 3, actRequeue},
		{"interrupted errorx canceled", stopped, errorx.New(errorx.Canceled, "", "canceled"), 0, 3, actRequeue},
		{"context error while active", active, context.Canceled, 0, 3, actDead},
	} {
		if got := decide(tt.ctx, tt.err, tt.redeliveries, tt.max); got != tt.want {
			t.Errorf("%s: decide() = %d, want %d", tt.name, got, tt.want)
		}
	}
}

func TestClassify(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, tt := range []struct {
		name string
		ctx  context.Context
		err  error
		want errorx.Code
	}{
		{"closed", nil, amqp.ErrClosed, errorx.Unavailable},
		{"io", nil, io.EOF, errorx.Unavailable},
		{"connection forced", nil, &amqp.Error{Code: amqp.ConnectionForced, Reason: "shutdown"}, errorx.Unavailable},
		// amqp091-go reports socket failures as frame errors of its own.
		{"network failure", nil, &amqp.Error{Code: amqp.FrameError, Reason: "EOF"}, errorx.Unavailable},
		{"frame error from the server", nil, &amqp.Error{Code: amqp.FrameError, Reason: "bad frame", Server: true}, errorx.Internal},
		{"protocol error", nil, &amqp.Error{Code: amqp.CommandInvalid, Reason: "unexpected", Server: true}, errorx.Internal},
		{"not found", nil, &amqp.Error{Code: amqp.NotFound, Reason: "no queue"}, errorx.NotFound},
		{"precondition", nil, &amqp.Error{Code: amqp.PreconditionFailed, Reason: "args"}, errorx.FailedPrecondition},
		{"locked", nil, &amqp.Error{Code: amqp.ResourceLocked, Reason: "exclusive"}, errorx.FailedPrecondition},
		{"access", nil, &amqp.Error{Code: amqp.AccessRefused, Reason: "denied"}, errorx.PermissionDenied},
		{"canceled", ctx, fmt.Errorf("x: %w", context.Canceled), errorx.Canceled},
	} {
		err := classify(tt.ctx, "op", tt.err)
		if got := errorx.CodeOf(err); got != tt.want {
			t.Errorf("%s: code = %v, want %v (%v)", tt.name, got, tt.want, err)
		}
		if tt.ctx == nil && !errors.Is(err, tt.err) {
			t.Errorf("%s: cause lost: %v", tt.name, err)
		}
	}
	if !errorx.IsRetryable(classify(context.Background(), "op", amqp.ErrClosed)) {
		t.Error("connection failures must be retryable")
	}
}

func TestOptions(t *testing.T) {
	b := New(nil)
	if b.opts.exchange != "servex" || b.opts.deadLetterExchange != "servex.dlx" || b.opts.maxRedeliveries != 3 ||
		b.opts.prefetch != 1 || b.opts.backoff != nil {
		t.Fatalf("defaults = %+v", b.opts)
	}
	b = New(nil, WithExchange("ex"), WithMaxRedeliveries(-1), WithPrefetch(0), nil)
	if b.opts.deadLetterExchange != "ex.dlx" || b.opts.maxRedeliveries != 0 || b.opts.prefetch != 1 {
		t.Fatalf("options = %+v", b.opts)
	}
	b = New(nil, WithDeadLetterExchange("dead"), WithPrefetch(10))
	if b.opts.deadLetterExchange != "dead" || b.opts.prefetch != 10 {
		t.Fatalf("options = %+v", b.opts)
	}
	if got := QueueName("orders.created", "billing"); got != "billing.orders.created" {
		t.Fatalf("QueueName() = %q", got)
	}
}

func TestQueueNamesAreUnambiguous(t *testing.T) {
	pairs := [][2]string{ // topic, group
		{"c", "a.b"}, {"b.c", "a"}, {"a.b.c", ""},
		{"orders", "g"}, {"orders.dlq", "g"}, {"orders%dlq", "g"}, {"orders.%dlq", "g"},
		{"orders.*", "g"}, {"orders.#", "g"}, {"#", "g"}, {"*", "g"},
		{"x", "g%2E"}, {"x", "g."}, {"2E.x", "g%"}, {"25.x", "g%"}, {"x", "%"}, {"x", "."},
		{".x", ""}, {"x", ""}, {"", "x"},
	}
	seen := make(map[string]string)
	for _, p := range pairs {
		topic, group := p[0], p[1]
		names := map[string]string{"dead letter key": QueueName(topic, group)}
		if group != "" {
			names = map[string]string{"queue": QueueName(topic, group), "dead letter queue": DeadLetterQueueName(topic, group)}
		}
		for kind, name := range names {
			who := fmt.Sprintf("%s of (%q, %q)", kind, topic, group)
			if prev, ok := seen[name]; ok {
				t.Errorf("%s and %s are both %q", prev, who, name)
			}
			seen[name] = who
		}
	}
}

func TestValidationAndClose(t *testing.T) {
	b := New(nil)
	h := func(context.Context, any) (any, error) { return nil, nil }
	for name, err := range map[string]error{
		"publish nil":           b.Publish(t.Context(), nil),
		"publish empty topic":   b.Publish(t.Context(), &broker.Message{}),
		"subscribe empty topic": b.Subscribe(t.Context(), "", "g", h),
		"subscribe nil handler": b.Subscribe(t.Context(), "t", "g", nil),
	} {
		if errorx.CodeOf(err) != errorx.InvalidArgument {
			t.Errorf("%s: err = %v, want InvalidArgument", name, err)
		}
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(t.Context(), &broker.Message{Topic: "t"}); errorx.CodeOf(err) != errorx.FailedPrecondition {
		t.Errorf("Publish() after Close = %v, want FailedPrecondition", err)
	}
	if err := b.Subscribe(t.Context(), "t", "g", h); err != nil {
		t.Errorf("Subscribe() after Close = %v, want nil", err)
	}
	if err := b.Close(); err != nil {
		t.Errorf("second Close() = %v", err)
	}
}

func TestSleep(t *testing.T) {
	if !sleep(t.Context(), 0) || !sleep(t.Context(), time.Millisecond) {
		t.Fatal("sleep() = false for an active context")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if sleep(ctx, 0) || sleep(ctx, time.Hour) {
		t.Fatal("sleep() = true for a canceled context")
	}
}
