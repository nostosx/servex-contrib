# broker/kafka

`github.com/nostosx/servex-contrib/broker/kafka` implements the servex
`broker.Publisher` and `broker.Subscriber` contracts on Apache Kafka through
[franz-go](https://github.com/twmb/franz-go).

- A non-empty group is a Kafka consumer group. Offsets are committed only after
  the handler settled the records before them.
- An empty group consumes every partition directly (broadcast): each subscriber
  receives every record produced after it subscribed, and no group is left on
  the cluster.
- `Message.Key` maps to the record key, `Message.Header` to the record headers.
- Kafka has no per-message nack: a record rejected with a retryable error is
  redelivered to the handler in-process. Records rejected with a non-retryable
  error, or that ran out of redeliveries, go to the dead letter topic (if
  configured) and are then committed.

## Install

Requires Go 1.27.3. Uses published servex and kitx v1.0.0 dependencies.

Run this in your application module (create one with `go mod init` if needed):

```sh
go get github.com/nostosx/servex-contrib/broker/kafka@v1.0.0
```

The example version corresponds to the tag `broker/kafka/v1.0.0`;
see [module versioning](../../README.md#versioning).

## Usage

Save a complete program below as `main.go`, then run `go mod tidy` and
`go run .` from that application directory.

Start a dedicated Kafka broker at `127.0.0.1:9092`. Create the `orders` and
`orders.dead` topics before running this example; do not rely on broker-side
automatic topic creation. For example, using the Kafka distribution's CLI:

```sh
kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --if-not-exists --topic orders --partitions 1 --replication-factor 1
kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --if-not-exists --topic orders.dead --partitions 1 --replication-factor 1
```

The program publishes one record and consumes with the `billing` group until
Ctrl+C. On a fresh topic/group it logs that order; subsequent runs resume the
group's committed position. Remove the example topics and group after use.

```go
package main

import (
	"context"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nostosx/servex-contrib/broker/kafka"
	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/messaging/broker"
)

func handleOrder(_ context.Context, req any) (any, error) {
	msg := req.(*broker.Message)
	log.Printf("order %s: %s", msg.Key, msg.Body)
	return nil, nil // nil acknowledges; an error rejects the record
}

func run() error {
	b, err := kafka.New(
		[]kgo.Opt{kgo.SeedBrokers("127.0.0.1:9092")},
		kafka.WithDeadLetterTopic("orders.dead"),
	)
	if err != nil {
		return err
	}
	defer b.Close()

	ctx, stop := app.SignalContext(context.Background())
	defer stop()

	msg := &broker.Message{Topic: "orders", Key: "42", Body: []byte(`{"id":42}`)}
	if err := b.Publish(ctx, msg); err != nil {
		return err
	}

	// The consumer is an app.Hook: it subscribes on Start and stops on Stop.
	consumer := broker.NewConsumer(b, "orders", "billing", handleOrder)
	return app.New(app.WithHooks(consumer)).Run(ctx)
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
```

`NewFromClient(client *kgo.Client, opts...)` uses an existing producing client
instead; consumer clients are created from `client.Opts()`, so the client must
not be configured to consume. `Close` does not close a client passed this way.

## Options

| Option | Default | Description |
|---|---|---|
| `WithMaxRedeliveries(n)` | 3 | In-process redeliveries of a record rejected with a retryable error. |
| `WithBackoff(fn)` | immediate | Delay before redelivery number `attempt`. |
| `WithDeadLetterTopic(topic)` | none (dropped) | Topic for dead letters; adds the `HeaderOriginalTopic` and `HeaderError` headers. |
| `WithPingInterval(d)` | 10s | Reachability check; after three failures `Subscribe` returns a retryable `Unavailable`. `<= 0` disables it. |

## Testing

Unit tests run against an in-process cluster (`kfake`). Integration tests:

```sh
SERVEX_IT_KAFKA_BROKERS=127.0.0.1:9092 GOWORK=off go test -race -count=1 -tags integration ./...
```

`SERVEX_IT_KAFKA_BROKERS` is a comma-separated seed list; the tests create and
delete their own topics.

## License

Licensed under the [MIT License](../../LICENSE).
