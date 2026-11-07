# auth/saml/redis

Atomic shared SAML request-correlation state using the official go-redis client.
Target module **v1.0.0**, Go 1.27.3, MIT. Implements auth/saml.StateStore.

`New(client, prefix)` borrows a single-node/Sentinel Redis client and never closes
it. Share its pool with sessions, but use a distinct non-empty prefix. Save uses
SET NX and absolute expiry; Take uses GETDEL and rejects oversized/expired state.
Only a SHA-256 RelayState digest is used as a key. Stored request IDs and browser
binding hashes are sensitive; configure authentication/TLS and avoid logging.

Requires Redis 6.2+. Select finite SDK timeouts and `MaxRetries: -1`. A lost
GETDEL response has an uncertain result: start a new SAML login instead of
retrying. There is no automatic retry or ownership of the pool.

```go
states, err := redis.New(client, "accounts:saml-state:")
if err != nil { return err }
provider, err := saml.New(config, states)
```

Import auth/saml/redis with package name redis (alias the Redis SDK when needed).
The service entrypoint chooses provider/session/CSRF composition. See the
[SAML package](../README.md) for metadata, certificates, browser cookie and
exact ACS-route policy. Session state and SAML correlation are separate records.

```sh
task check lint vulncheck MODULES=auth/saml/redis
SERVEX_IT_REDIS_ADDR=127.0.0.1:6379 task test INTEGRATION=true MODULES=auth/saml/redis
```

Unit tests use miniredis. Real integration uses two independent Redis clients,
a random namespace and owned disposable services; verifies insert-only save,
exactly one concurrent consumer, expiry and expired-payload rejection, with
cleanup restricted to the fixture's own keys.
