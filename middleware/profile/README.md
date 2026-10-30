# Shared observability profile

Construct one `profile.New(profile.Config{Logger: log, Tracer: tracer,
Propagator: propagator, Meter: meter})` in the composition root. It shares signal
configuration across protocols while returning independent middleware slices:

- `Server()` observes unary endpoints and worker invocations.
- `Stream()` observes the entire gRPC stream lifetime.
- `Client()` observes outgoing calls without selecting retries or timeouts.
- `HTTP("http.request")` wraps native `net/http` handlers, including routing,
  CORS, CSRF and Session persistence failures.

Server/Stream order is tracing → logging → metrics → recovery → explicit extras.
Recovery converts a panic into an error before the outer signals record it.
Authentication, validation, rate limits and timeouts are explicit additions;
stream policies must be suitable for a whole stream. No automatic replay occurs.

Use `HTTP` as the outer HTTP wrapper and keep Session/CSRF inside it. Choose a
fixed bounded operation label; never put raw URLs, user IDs or query strings into
metric labels. Select HTTP request observation or endpoint `Server` observation
for a transport unless both measurement levels are intentional. The HTTP wrapper
preserves response-controller access, flushing and hijacking for SSE/WebSocket.

The module keeps SDKs outside middleware. Nil signals preserve individual
middleware behavior: disabled logging and no-op metrics/tracing. Supply concrete
SDK adapters once from the application. See the complete composition in
[`servex-examples`](https://github.com/nostosx/servex-examples).
