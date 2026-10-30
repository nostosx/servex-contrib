# Redis OAuth2 state

`oauth2redis` implements `oauth2.StateStore` using the official
`github.com/redis/go-redis/v9` client. It borrows a client and never closes it.
Share that pool with Session and cache adapters and give each a distinct prefix.

```go
client := redis.NewClient(&redis.Options{
    Addr: "127.0.0.1:6379",
    MaxRetries: -1,
    DialTimeout: 3 * time.Second,
    ReadTimeout: 3 * time.Second,
    WriteTimeout: 3 * time.Second,
})
states, err := oauth2redis.New(client, "myapp:oauth-state:")
```

This assembly fragment belongs in the application composition root, which owns
`client.Close`. Pass `states` to `oauth2.New`; use the same backend on all
replicas. The complete browser flow is provided by [`auth/login`](../login).

`Save` uses insert-only `SET NX` with absolute expiry. `Take` atomically reads
and deletes with `GETDEL`, then checks payload size and expiry again. Only hashed
OAuth state identifiers become Redis keys. State contains sensitive PKCE and
browser-binding data. Configure SDK authentication/TLS for your deployment.

Disable automatic transport retries: after a lost `GETDEL` response, the client
cannot determine whether consumption happened. Begin a fresh login after an
uncertain callback. Supports single-node/Sentinel `*redis.Client`.

The OAuth2 peer is unreleased. From the contrib root, run:

```sh
bash .github/scripts/check-source.sh check lint vulncheck MODULES='auth/oauth2redis'
```

The helper resolves matching source in an external temporary modfile, without
modifying canonical requirements or inventing public checksums. Tests exercise
concurrent one-time consumption using miniredis. The `integration` build adds
real Redis acceptance with two independent clients sharing a random namespace:
insert-only saves, exactly one concurrent consumer, Redis key expiry, and
rejection of expired payloads. It cleans up its own keys and closes both clients.

With a disposable Redis 6.2 or newer already running, select its address:

```sh
SERVEX_IT_REDIS_ADDR=127.0.0.1:6379 \
    bash .github/scripts/check-source.sh test INTEGRATION=true MODULES='auth/oauth2redis'
```
