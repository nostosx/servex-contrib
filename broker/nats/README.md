# broker/nats

`github.com/nostosx/servex-contrib/broker/nats` implements the servex
`broker.Publisher` and `broker.ReadySubscriber` contracts on
[NATS JetStream](https://github.com/nats-io/nats.go).

- Topics are subjects bound to a JetStream stream that the application manages;
  the broker never creates streams.
- A non-empty group is a durable pull consumer named `DurableName(topic, group)`,
  shared by the group's members. An empty group creates an ephemeral consumer
  per subscriber that receives messages published after it subscribed.
  Durable names encode each component's UTF-8 bytes as hexadecimal, separated
  by `_`, so different groups and topics never alias.
- `SubscribeReady` reports readiness once the consumer exists and its pull
  subscription is active. `broker.NewConsumer` uses it, so the consumer hook's
  `Start` fails when, for example, no stream is bound to the topic. Group
  consumers are named `consumer:<topic>:<group>` in app logs and errors.
- `Message.Header` maps to the NATS headers; `Message.Key` travels in the
  `Servex-Key` header (`HeaderKey`).
- A retryable rejection is `Nak`ed with a delay. A non-retryable rejection, or
  one past the redelivery limit, is terminated and passed to the dead letter
  function. Group members share handler-rejection counts in a JetStream
  key-value bucket; interrupted deliveries do not count while that bucket is
  available. If it cannot be used, the fallback counts server deliveries,
  including interrupted ones. Broadcast subscribers count rejections in memory.

## Install

Requires Go 1.27.3. Uses published servex v1.2.0 and kitx v1.0.0 dependencies.

Earlier pre-release checkouts replaced punctuation with `_` in durable names.
The new names create new consumers starting at the stream's oldest message;
existing deployments must stop old subscribers and migrate their acknowledged
position explicitly before switching if replay is unacceptable. Remove old
consumers and their redelivery state after migration. There is no automatic
fallback to ambiguous old names.

Run this in your application module (create one with `go mod init` if needed):

```sh
go get github.com/nostosx/servex-contrib/broker/nats@v1.2.0
```

The example version corresponds to the tag `broker/nats/v1.1.0`;
see [module versioning](../../README.md#versioning).

## Usage

Save a complete program below as `main.go`, then run `go mod tidy` and
`go run .` from that application directory.

Start a dedicated NATS server with JetStream enabled (`nats-server -js`) at
`127.0.0.1:4222`. The program creates the `ORDERS` stream, publishes an order
and consumes until Ctrl+C. It also creates a durable consumer and, when
needed, the redelivery KV bucket. Closing the application does not delete
these persistent resources; use a disposable server or remove them after use.

```go
package main

import (
	"context"
	"log"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/nostosx/servex-contrib/broker/nats"
	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/messaging/broker"
)

func handleOrder(_ context.Context, msg *broker.Message) (any, error) {
	log.Printf("order: %s", msg.Body)
	return nil, nil
}

func run() error {
	nc, err := natsgo.Connect("nats://127.0.0.1:4222")
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	ctx, stop := app.SignalContext(context.Background())
	defer stop()

	// The stream is managed by the application.
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "ORDERS",
		Subjects: []string{"orders.>"},
	}); err != nil {
		return err
	}

	b := nats.New(js, nats.WithStream("ORDERS"), nats.WithDeadLetter(func(msg *broker.Message, err error) {
		log.Printf("dead letter %s: %v", msg.Topic, err)
	}))
	if err := b.Publish(ctx, &broker.Message{Topic: "orders.created", Body: []byte(`{"id":42}`)}); err != nil {
		return err
	}

	consumer := broker.NewConsumer(b, "orders.created", "billing", endpoint.Typed(handleOrder))
	return app.New(app.WithHooks(consumer)).Run(ctx)
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
```

The broker does not own the connection; closing it ends active subscriptions
with an error.

## Options

| Option | Default | Description |
|---|---|---|
| `WithMaxRedeliveries(n)` | 3 | Redeliveries after retryable handler errors before the message is terminated. |
| `WithBackoff(fn)` | 100ms doubling, max 10s | Redelivery delay requested with the `Nak`. |
| `WithDeadLetter(fn)` | none | Receives terminated messages and the last handler error. |
| `WithStream(name)` | look up by subject | Stream holding the subscribed topics. |
| `WithAckWait(d)` | 30s | Server ack deadline; extended while a handler runs. |
| `WithRedeliveryBucket(name)` | `SERVEX_REDELIVERIES` | Key-value bucket for the per-group redelivery count (created on demand, 24h TTL). |

## Testing

Unit tests run against an embedded NATS server with JetStream. Integration
tests need a server started with JetStream enabled (`nats-server -js`):

```sh
SERVEX_IT_NATS_URL=nats://127.0.0.1:4222 GOWORK=off go test -race -count=1 -tags integration ./...
```

## License

Licensed under the [MIT License](../../LICENSE).

## Release

Version v1.2.0, released 2026-10-29. Requires servex v1.4.0. Install from the public Go proxy without source replacements:

```sh
go get github.com/nostosx/servex-contrib/broker/nats@v1.2.0
```
