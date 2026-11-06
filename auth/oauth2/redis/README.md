# auth/oauth2/redis

Module version for this revision: **v1.0.0**.

Use Go 1.27.3; core integration requires published servex v1.7.0. See the
[validation commands](../../../CONTRIBUTING.md#checks).

`redis` implements `client.StateStore` using the official
`github.com/redis/go-redis/v9` client. It borrows a client and never closes it.
Share that pool with Session and cache adapters and give each a distinct prefix.
Its import path is `github.com/nostosx/servex-contrib/auth/oauth2/redis`;
the package name is `redis`.

```go
// Import go-redis/v9 as goredis and auth/oauth2/redis as redis.
client := goredis.NewClient(&goredis.Options{
    Addr: "127.0.0.1:6379",
    MaxRetries: -1,
    DialTimeout: 3 * time.Second,
    ReadTimeout: 3 * time.Second,
    WriteTimeout: 3 * time.Second,
})
states, err := redis.New(client, "myapp:oauth-state:")
```

This assembly fragment belongs in the application composition root, which owns
`client.Close`. Pass `states` to OAuth2 `client.New` or `client.NewProvider`; use
the same backend on all replicas. The browser flow is provided by
[`auth/login`](../../login).

`Save` uses insert-only `SET NX` with absolute expiry. `Take` atomically reads
and deletes with `GETDEL`, then checks payload size and expiry again. Only hashed
OAuth state identifiers become Redis keys. State contains sensitive PKCE and
browser-binding data. Configure SDK authentication/TLS for your deployment.

Disable automatic transport retries: after a lost `GETDEL` response, the client
cannot determine whether consumption happened. Begin a fresh login after an
uncertain callback. Supports single-node/Sentinel `*redis.Client`.

This adapter moves from auth/oauth2redis to auth/oauth2/redis. Update its module
requirement and imports together. From the repository root, run real acceptance
against an explicitly configured disposable service:

```sh
task check lint vulncheck MODULES=auth/oauth2/redis
SERVEX_IT_REDIS_ADDR=127.0.0.1:6379 \
  task test INTEGRATION=true MODULES=auth/oauth2/redis
```

The backend requires Redis 6.2 or newer for GETDEL. Unit tests use miniredis;
real acceptance uses two independent clients and a random namespace to verify
insert-only saves, exactly one concurrent consumer, key expiry and expired-payload
rejection. It removes only its own keys and closes both borrowed test clients.
