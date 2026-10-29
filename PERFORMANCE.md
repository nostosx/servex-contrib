# Transport, codec and middleware performance

Measured on 2026-10-29 with Go 1.27.3, darwin/arm64, Apple M5. This is a
baseline investigation of the current matching core/contrib source, including
the unreleased backend extraction. No production optimization was applied.

This document records the pre-optimization baseline. Subsequent changes and
matched-binary comparisons are in [optimization results](PERFORMANCE-OPTIMIZATION.md).

## Findings

The measured warm unary paths do not show a universal framework slowdown.
There are specific avoidable costs worth addressing before changing core
interfaces: HTTP client field extraction, HTTP binding metadata reconstruction,
gRPC middleware reconstruction and logging attribute construction.

1. **HTTP client field extraction:** even a fixed-path POST calls `jsonFields`,
   marshals the whole request and unmarshals its fields, then marshals the body
   again. The in-memory client benchmark isolates a size-dependent cost from
   TCP/server work. See [Client.do and jsonFields](transport/httpx/client.go).
   Evaluate extracting fields only when query/path binding requires them,
   preserving existing encoding/error semantics.
2. **HTTP query/path binding:** `bindValues` rebuilds reflected field indexes
   and sorts names on each call. A request with both query and path values
   takes this path twice. The allocation profile attributes about 28% of sampled
   objects to `fieldIndexes` and its callees. Evaluate an immutable per-type
   binding plan, preserving embedding, tags and errors and considering cache
   lifetime. See [binding implementation](transport/httpx/bind.go).
3. **gRPC unary middleware:** `unaryInterceptor` applies the chain to
   `callHandler` on each RPC. Five passthrough middleware add five allocations
   and 80 bytes over the no-middleware interceptor. The allocation profile
   attributes about 55% of objects in that isolated case to rebuilding the
   closures. HTTP composes endpoint middleware at route registration. Evaluate
   composing the gRPC endpoint once, retaining per-call handler context.
   See [server interceptor](transport/grpcx/server.go).
4. **Logging:** enabled logging allocates an attribute slice with capacity
   seven, then immediately appends ten key/value elements. Attribute preparation
   and boxing cost 400 B/successful call even with a discard Logger. Nil/disabled
   logging still reads the clock twice. These costs precede real log encoding
   and I/O. See [logging middleware](middleware/logging/logging.go).
5. **Shared-key admission:** local rate limiting and circuit breaking allocate
   no objects on successful calls, but contend under parallel access to a shared
   key/operation. Mutex profiles locate waiting in the rate bucket and breaker
   allow/record paths. This matters for very small handlers; it is not evidence
   of a connection-pool or DI failure.

## Method

There are 101 final benchmark cases, each with ten samples. Serial cases and
middleware parallel cases use 100ms/sample. Final network parallel cases use
500ms/sample after initial 100ms runs showed scheduling variance. Serial cases
use GOMAXPROCS=1; parallel cases also run with GOMAXPROCS=8. Tables show medians.
Runs were sequential, without the race detector, tests or lint competing for
CPU. Profiling ran separately. Darwin CPU samples were heavily attributed to
runtime wait/syscall paths; no business-CPU percentages are inferred from them.
Allocation and mutex profiles substantiate the structural findings above.

HTTP uses HTTP/1.1 keep-alive and owned ephemeral loopback listeners. gRPC uses
both bufconn and cleartext loopback TCP. Connections are reused and prewarmed;
setup is excluded. Responses are checked. HTTP has id/name/active/data fields;
gRPC uses a generated protobuf payload. Data bytes means ASCII field length,
not exact wire size. Neither transport benchmark installs DI or optional
endpoint middleware unless a case explicitly says otherwise.

Native endpoints do not implement all servex metadata, error, negotiation and
binding contracts. Their measured difference is a feature cost, not automatically
an avoidable regression. These are local baselines, not hosted CI or production
capacity measurements.

## HTTP

Native uses handwritten net/http, ServeMux, JSON v2, the same echo schema and
body limit. ServexServer uses the native client against httpx; ServexBoth uses
httpx on both ends.

| Data bytes | Native TCP us/op | Servex server us/op | Servex both ends us/op |
|---|---:|---:|---:|
| 128 | 28.60 | 29.88 | 31.83 |
| 4096 | 40.82 | 41.96 | 46.93 |

The handler case includes equal httptest request/recorder creation on both
sides. The client-only case uses the same deterministic RoundTripper, consumes
the outgoing body and decodes a fixed echo response, excluding server/TCP.

| Isolated case | Native us/op | Servex us/op | Native allocs/op | Servex allocs/op |
|---|---:|---:|---:|---:|
| POST handler, 128 B | 1.63 | 2.09 | 26 | 36 |
| POST handler, 4 KiB | 6.01 | 6.51 | 35 | 45 |
| Query + path binding | 1.33 | 2.94 | 26 | 65 |
| Client only, 128 B | 1.30 | 2.55 | 25 | 51 |
| Client only, 4 KiB | 5.42 | 9.80 | 34 | 60 |

The isolated 4 KiB client uses about 22.4 KB/op natively versus 33.2 KB/op through
httpx. The difference includes field extraction, metadata, request-progress
tracking and other general-purpose client behavior; not all of it is attributed
to duplicate JSON processing.

## gRPC

All modes use the same generated grpc_testing service and response body.
Native uses grpc-go directly; the other modes add grpcx on one or both ends.
Health methods are not used, since they bypass middleware.

| Connection | Data bytes | Native us/op | Servex server us/op | Servex both ends us/op |
|---|---:|---:|---:|---:|
| Bufconn | 128 | 9.03 | 9.23 | 9.44 |
| Bufconn | 4096 | 10.43 | 10.79 | 11.45 |
| TCP | 128 | 33.50 | 33.75 | 34.06 |
| TCP | 4096 | 37.29 | 36.07 | 36.00 |

Network variance is material: native 4 KiB TCP has a 12% confidence range in
benchstat. The slightly lower servex 4 KiB medians do not establish an
optimization. At 128 B, native/full-servex serial cases allocate 150/173 objects
and 9760/11256 B per round trip. Both reuse their connections.

| Unary interceptor only | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Native handler | 1.75 | 0 | 0 |
| Servex, no middleware | 78.73 | 208 | 4 |
| Servex, one passthrough | 90.06 | 224 | 5 |
| Servex, five passthrough | 133.30 | 288 | 9 |

## Codecs

JSON/YAML fixtures have one value string; Proto uses a generated StringValue.
Adapters are compared against their owning SDK with identical decoded values.
This does not compare arbitrary schemas or equal wire lengths across formats.

| Codec | Data bytes | Native marshal us/op | Adapter marshal us/op | Native unmarshal us/op | Adapter unmarshal us/op |
|---|---:|---:|---:|---:|---:|
| JSON v2 | 128 | 0.138 | 0.139 | 0.126 | 0.130 |
| JSON v2 | 4096 | 1.788 | 1.786 | 1.628 | 1.507 |
| Proto | 128 | 0.047 | 0.049 | 0.064 | 0.068 |
| Proto | 4096 | 0.344 | 0.343 | 0.349 | 0.355 |
| YAML v3 | 128 | 2.243 | 2.264 | 2.936 | 3.039 |
| YAML v3 | 4096 | 33.520 | 32.733 | 31.159 | 32.054 |

JSON and Proto wrappers add no allocations in these fixtures. Small timing
differences are not evidence that forwarding through a wrapper speeds up its
SDK. YAML normalization adds three allocations (72 B) for the typed fixture
and ten (184 B) for a nested string-key dynamic map, preserving identical values.
Most YAML cost is in its parser/emitter. It should not be treated as equivalent
to JSON/Proto request-path defaults.

## Middleware

All endpoints are composed before timing. The handler is a noinline identity
endpoint. Authentication uses a static verifier, logging a discard core Logger,
and metrics/tracing Noop backends. JWT cryptography, SDK log encoding/output
and telemetry export are excluded. All calls succeed; retry does not retry.

| Success path | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Direct | 1.68 | 0 | 0 |
| Recovery | 5.29 | 0 | 0 |
| Validate | 3.10 | 0 | 0 |
| Auth, static verifier | 45.91 | 64 | 2 |
| Logging, nil | 42.67 | 0 | 0 |
| Logging, disabled | 42.04 | 0 | 0 |
| Logging, enabled/discard | 138.50 | 400 | 6 |
| Metrics, Noop | 78.38 | 112 | 2 |
| Tracing, Noop | 50.69 | 48 | 3 |
| Timeout | 220.95 | 272 | 4 |
| Local rate limit, one key | 43.59 | 0 | 0 |
| Circuit breaker | 89.62 | 0 | 0 |
| Retry | 2.50 | 0 | 0 |
| Server chain | 382.10 | 624 | 13 |
| Client chain | 331.40 | 272 | 4 |

ServerChain is recovery/auth/validate/logging/metrics/tracing/local rate limit.
ClientChain is timeout/retry/circuitbreaker. Combined costs are not the sum of
independent medians. Noop observability does not imply zero-cost middleware:
labels and attributes are still prepared. Timeout's context/timer allocation
implements a real deadline and is not an incidental bug.

## Parallel scaling

RunParallel ns/op is total wall time divided by completed calls, not individual
latency, p95 or p99. Network rows use 128 B and eight workers on reused
connections. Client and server run in the same process.

| Eight-worker case | us/op | B/op | allocs/op |
|---|---:|---:|---:|
| HTTP native | 10.72 | 8921 | 94 |
| HTTP servex server | 11.31 | 9767 | 112 |
| HTTP servex both ends | 12.10 | 11186 | 138 |
| gRPC native | 11.86 | 10207 | 140 |
| gRPC servex server | 13.06 | 11400 | 155 |
| gRPC servex both ends | 13.86 | 11816 | 163 |

| Shared middleware operation | One worker ns/op | Eight workers ns/op | allocs/op |
|---|---:|---:|---:|
| Rate limit, one key | 43.56 | 196.80 | 0 |
| Circuit breaker, one operation | 90.84 | 386.00 | 0 |
| Server chain | 384.80 | 410.30 | 13 |
| Client chain | 346.50 | 500.25 | 4 |

Shared keys expose lock contention; this does not model many shards or slow
business handlers. Mutex profiles locate waiting in those lock paths. Their
percentages describe accumulated goroutine blocking, not application CPU or
request latency. High key cardinality and long-running idle sweeps were not
measured by the single-key fixtures.

## Reproduction and verification

Run from each owning module; the repository root is not a Go module. Use
matching peers through external temporary modfiles when needed, as described
in CONTRIBUTING.md. Do not commit sibling replacements.

```sh
# From transport/httpx or transport/grpcx:
GOWORK=off go test -run='^$' -bench='^Benchmark' -benchmem \
  -benchtime=100ms -count=10 -cpu=1 . > benchmark.txt
benchstat benchmark.txt
# Repeat the RoundTripParallel cases with -cpu=8 -benchtime=500ms.

# From encoding/json, encoding/proto or encoding/yaml:
GOWORK=off go test -run='^$' -bench='^BenchmarkCodec' -benchmem \
  -benchtime=100ms -count=10 -cpu=1 .

# From middleware:
GOWORK=off go test -run='^$' -bench='^BenchmarkMiddleware' -benchmem \
  -benchtime=100ms -count=10 -cpu=1,8 ./internal/benchmarks
```

All six touched modules passed task check and default/integration lint.
Transport benchmarks also passed a one-iteration race smoke; its timings are
excluded from performance results. No production implementation or module
dependency changed. DI results are documented separately in
[di/do/PERFORMANCE.md](di/do/PERFORMANCE.md).

This is warm success-path measurement, not a production capacity/SLO test.
It excludes TLS, streaming, cold connections, real JWT validation, log I/O,
telemetry exporters, live discovery, multi-node balancing and database work.
Latency percentiles and overload behavior require a separate load test.
