# database/mongodb

`github.com/nostosx/servex-contrib/database/mongodb` plugs the official
[MongoDB Go driver](https://github.com/mongodb/mongo-go-driver) (v2) into
servex, in the same way the core `storage/database` package does for
`database/sql`. It does not wrap the driver: you keep using `*mongo.Client`.

- `Monitor(tracer, meter)` returns an `*event.CommandMonitor` that creates a
  client span per command (`db.system`, `db.operation`, `db.name`,
  `db.mongodb.collection`) and records `db_client_operations_total` and
  `db_client_operation_duration_seconds`, the metrics the core records for SQL.
  Metrics receive the command span context, so exemplars link to the span.
  Command documents are never recorded. A nil tracer or meter disables that
  part.
- `Hook(name, client)` is an `app.Hook`: Start pings the deployment (an
  unreachable deployment fails the start), Stop disconnects. It also
  implements `Check(ctx) error`, so it takes part in `App.Check` and can be
  used as a `health.Checker`.

## Install

Requires Go 1.27.3. Uses published servex and kitx v1.0.0 dependencies.
Run this in your application module (create one with `go mod init` if needed):

```sh
go get github.com/nostosx/servex-contrib/database/mongodb@v1.0.0
```

The example version corresponds to the tag `database/mongodb/v1.0.0`;
see [module versioning](../../README.md#versioning).

## Usage

Save a complete program below as `main.go`, then run `go mod tidy` and
`go run .` from that application directory.

Start a dedicated MongoDB deployment at `127.0.0.1:27017`. This program pings
it on startup and waits until Ctrl+C, then disconnects. It creates no database
or collection and does not start an HTTP server.

```go
package main

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/nostosx/servex-contrib/database/mongodb"
	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/observability/metrics"
	"github.com/nostosx/servex/observability/tracing"
)

func run() error {
	// Use real backends, for example servex-contrib/tracing/otel and
	// servex-contrib/metrics/prometheus.
	tracer, meter := tracing.Noop(), metrics.Noop()

	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://127.0.0.1:27017").
		SetMonitor(mongodb.Monitor(tracer, meter)))
	if err != nil {
		return err
	}

	// Register database hooks before the servers: they stop in reverse
	// order, so the client disconnects after in-flight requests drained.
	a := app.New(app.WithHooks(mongodb.Hook("mongodb", client) /*, servers... */))

	ctx, stop := app.SignalContext(context.Background())
	defer stop()
	return a.Run(ctx)
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
```

A failed ping is a retryable `errorx.Unavailable` (reason
`MONGODB_UNAVAILABLE`); a ping on a disconnected client is a non-retryable
`FailedPrecondition` (reason `MONGODB_CLIENT_DISCONNECTED`).

## Testing

Unit tests need no server. Integration tests:

```sh
SERVEX_IT_MONGODB_URI=mongodb://127.0.0.1:27017 GOWORK=off go test -race -count=1 -tags integration ./...
```

## License

Licensed under the [MIT License](../../LICENSE).
