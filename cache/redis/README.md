# cache/redis

`github.com/nostosx/servex-contrib/cache/redis` implements the servex
`cache.Cache` contract on a [go-redis](https://github.com/redis/go-redis)
`redis.UniversalClient`, so a standalone, sentinel or cluster deployment can
back `cache.Loader` and any other `cache.Cache` consumer.

- Values are stored as plain Redis strings under the (optionally prefixed) key.
- A missing or expired key is `cache.ErrNotFound`.
- `ttl <= 0` stores the entry without expiry; a positive ttl below 1ms is
  rounded up to 1ms, the resolution of Redis.
- The Cache does not own the client; close it yourself.

## Install

Requires Go 1.27.3. Uses published servex and kitx v1.0.0 dependencies.

Install this module in your application:

```sh
go get github.com/nostosx/servex-contrib/cache/redis@v1.0.0
```

The example version corresponds to the tag `cache/redis/v1.0.0`;
see [module versioning](../../README.md#versioning).

## Usage

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/nostosx/servex-contrib/cache/redis"
	"github.com/nostosx/servex/storage/cache"
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
	users, err := cache.NewLoader[User](owner, c, 5*time.Second, cache.WithTTL(10*time.Minute))
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

Licensed under the [MIT License](../../LICENSE).
