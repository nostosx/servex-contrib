# storage/cache/redis

Current source is unreleased. Use Go 1.27.3; core integration targets
published servex v1.7.0. See the
[source-validation commands](../../../CONTRIBUTING.md#checks).

`github.com/nostosx/servex-contrib/storage/cache/redis` implements the servex
`cache.Cache` contract on a [go-redis](https://github.com/redis/go-redis)
`redis.UniversalClient`, so a standalone, sentinel or cluster deployment can
back contrib `loader.Loader` and any other `cache.Cache` consumer.

- Values are stored as plain Redis strings under the (optionally prefixed) key.
- A missing or expired key is `cache.ErrNotFound`.
- `ttl <= 0` stores the entry without expiry; a positive ttl below 1ms is
  rounded up to 1ms, the resolution of Redis.
- `New(client)` borrows a client. `Open(config)` owns its client and releases it through `Close` or `Hook`.

## Configuration

`Config`, `DefaultConfig`, `Validate` and `Open` provide file-driven construction.
Initialize defaults, decode with config/viper, then validate before opening:

```go
cfg := redis.DefaultConfig()
if err := settings.Snapshot().Unmarshal("redis", &cfg); err != nil { return err }
c, err := redis.Open(cfg)
if err != nil { return err }
defer c.Close() // Covers assembly failures and an application that never starts.
redisHook := redis.Hook("redis", c)
```

Register redisHook before components that borrow its client. Start and readiness
ping Redis; failed startup releases an owned client. Stop's context bounds waiting,
while native cleanup continues and later callers join it. `Client()` exposes a
borrowed native client for compatible Session/state/rate-limit adapters; give the
pool one lifecycle owner. `New(existingClient, ...)` keeps caller ownership.

```toml
[redis]
addrs = ["127.0.0.1:6379"]
db = 0
prefix = "app:"
pool_size = 10
min_idle_conns = 0
dial_timeout = "5s"
read_timeout = "5s"
write_timeout = "5s"
pool_timeout = "6s"
conn_max_idle_time = "5m"
conn_max_lifetime = "0s"
max_retries = 0
max_redirects = 3
tls = false
```

Use environment sources for `username`, `password`, `sentinel_username` and
`sentinel_password`. A `master_name` selects Sentinel. Multiple seed addresses
without it select Cluster, which only supports DB zero. These construction options
are not real Cluster/Sentinel acceptance; use dedicated service fixtures for that.

Timeouts must be positive; zero expiry disables that expiry. `max_retries=0`
disables node-command retries. Cluster has a separate `max_redirects` budget,
which covers MOVED/ASK and some network retries; zero disables that loop too.
The SDK can still retry connection establishment. Caller cancellation/deadlines
are enabled. Open performs no readiness ping; the SDK may initialize pools during
construction, so register cleanup immediately.

`tls=true` uses verified TLS with a TLS 1.2 minimum; `server_name` can select the
verified hostname. Custom certificate pools/callbacks belong in runtime
`WithTLSConfig`, which snapshots the TLS policy. Config is pure data and contains
no SDK clients, callbacks or TLS objects. Invalid settings are rejected without
printing addresses or credentials.

## Usage

The complete programs below use the current source API. Use matching peer
sources during development; install real module versions after publication.

Start a dedicated Redis instance at `127.0.0.1:6379`. The program prints
`alice` and exits. It creates `users:42` with a ten-minute TTL; remove that
example key afterwards or discard the instance. No database is needed for
the loader callback used here.

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	goredis "github.com/redis/go-redis/v9"

	cacheloader "github.com/nostosx/servex-contrib/storage/cache/loader"
	"github.com/nostosx/servex-contrib/storage/cache/redis"
	jsoncodec "github.com/nostosx/servex-contrib/encoding/json"
)

type User struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func main() {
	rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:6379"})
	defer rdb.Close()

	c := redis.New(rdb, redis.WithPrefix("users:"))

	// Loader adds read-through loading, JSON encoding and duplicate-load
	// suppression on top of any cache.Cache.
	owner, stop := context.WithCancel(context.Background())
	defer stop()
	users, err := cacheloader.New[User](
		owner,
		c,
		5*time.Second,
		cacheloader.WithTTL(10*time.Minute),
		cacheloader.WithCodec(jsoncodec.Codec),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer users.Close()
	u, err := users.Get(context.Background(), "42", func(ctx context.Context) (User, error) {
		return User{ID: "42", Name: "alice"}, nil // load from the database
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(u.Name)
}
```

Shared loads inherit the owner context, rather than the initiating request's
values. Use tenant/authorization-scoped keys and callbacks that honor cancellation.
Close the Loader before closing its Redis client; closing a Loader does not close
the backend.

## Options

| Option | Default | Description |
|---|---|---|
| `WithPrefix(s)` | none | Prepended to every key, to namespace entries in a shared database. |

Errors carry the reasons `REDIS_UNAVAILABLE` (retryable), `REDIS_CLIENT_CLOSED`
(`FailedPrecondition`, not retryable) and `REDIS_SERVER_ERROR`.

## Testing

Unit tests use miniredis. Integration tests:

```sh
SERVEX_IT_REDIS_ADDR=127.0.0.1:6379 GOWORK=off go test -race -count=1 -tags integration ./...
```

## License

Licensed under the [MIT License](../../../LICENSE).
