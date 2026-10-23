# broker/rabbitmq

`github.com/nostosx/servex-contrib/broker/rabbitmq` implements the servex
`broker.Publisher` and `broker.Subscriber` contracts on RabbitMQ (AMQP 0-9-1)
through [amqp091-go](https://github.com/rabbitmq/amqp091-go).

- Messages are published to a durable topic exchange with the topic as routing
  key, on a channel in confirm mode.
- A non-empty group consumes from the durable queue `QueueName(topic, group)`
  (`<group>.<topic>`, escaped). An empty group consumes from an exclusive,
  server-named queue that lives as long as the subscription.
- `Message.Header` maps to the AMQP headers; `Message.Key` travels in the
  `Servex-Key` header (`HeaderKey`).
- A retryable rejection is republished to the queue with an incremented
  `Servex-Redeliveries` counter. A non-retryable rejection, or one past the
  limit, is rejected without requeue and routed by the direct dead letter
  exchange into `DeadLetterQueueName(topic, group)`.

## Install

Requires Go 1.27.3. This module is unreleased; use the
[local checkout instructions](../../README.md#development), including a
replacement for the core in your application's `go.mod`.

After a release is available:

```sh
go get github.com/nostosx/servex-contrib/broker/rabbitmq@v1.0.0
```

The example version corresponds to the tag `broker/rabbitmq/v1.0.0`;
see [module versioning](../../README.md#versioning).

## Usage

```go
package main

import (
	"context"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/nostosx/servex-contrib/broker/rabbitmq"
	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/messaging/broker"
)

func handleOrder(_ context.Context, req any) (any, error) {
	log.Printf("order: %s", req.(*broker.Message).Body)
	return nil, nil
}

func run() error {
	conn, err := amqp.Dial("amqp://guest:guest@127.0.0.1:5672/")
	if err != nil {
		return err
	}
	defer conn.Close() // the Broker does not close the connection

	b := rabbitmq.New(conn, rabbitmq.WithExchange("shop"), rabbitmq.WithPrefetch(16))
	defer b.Close()

	ctx, stop := app.SignalContext(context.Background())
	defer stop()

	consumer := broker.NewConsumer(b, "orders.created", "billing", handleOrder)
	return app.New(app.WithHooks(consumer)).Run(ctx)
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
```

Publishing uses the same Broker:

```go
err := b.Publish(ctx, &broker.Message{Topic: "orders.created", Key: "42", Body: []byte(`{"id":42}`)})
```

The exchange routes a message only to queues that exist at that moment. A
group queue is durable, so it keeps receiving messages once its first
subscriber has declared it, also while no member is running.

The lifecycle tests use the default `amqp.Dial` connection mode. Connection
loss ends the subscription and can fail its application Hook. Behavior with
the SDK's optional automatic connection recovery has not been verified;
do not assume the Broker resumes a subscription after recovery.

## Options

| Option | Default | Description |
|---|---|---|
| `WithExchange(name)` | `servex` | Topic exchange messages are published to. |
| `WithDeadLetterExchange(name)` | `<exchange>.dlx` | Direct exchange receiving dead letters; must differ from the exchange. |
| `WithMaxRedeliveries(n)` | 3 | Redeliveries after retryable handler errors. |
| `WithBackoff(fn)` | immediate | Delay before republishing for redelivery number `attempt`. |
| `WithPrefetch(n)` | 1 | Unacknowledged messages delivered ahead of the handler. |

## Testing

Unit tests use an in-memory fake of the AMQP channel. Integration tests:

```sh
SERVEX_IT_RABBITMQ_URL=amqp://guest:guest@127.0.0.1:5672/ GOWORK=off go test -race -count=1 -tags integration ./...
```

## License

Licensed under the [MIT License](../../LICENSE).
