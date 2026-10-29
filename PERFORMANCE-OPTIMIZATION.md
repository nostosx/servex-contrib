# Behavior-preserving performance optimizations

Implemented and measured on 2026-10-29, following the
[transport and middleware baseline](PERFORMANCE.md). Public interfaces and
module requirements are unchanged. Core source is unchanged.

## Changes and compatibility

### HTTP client field extraction

Fixed-path body requests no longer decode the initial JSON object into an
unused field map. Query methods and wildcard paths retain field extraction.
The initial JSON encoding check still runs before path expansion and body
encoding. The configured codec still encodes the body separately.

This is deliberate: skipping the initial JSON check changes custom-codec
acceptance/errors, and reusing its bytes changes stateful JSON marshaler
behavior. Compatibility tests verify both marshal calls and their order,
path values from the first call, body values from the second, custom codec
invocation and both error stages/causes. This optimization removes unused map
decoding; it does not claim to eliminate both existing encoding calls.

### HTTP binding plans

Each route lazily retains immutable value/file binding plans keyed by the
actual request type. Values use the same sorted field order as before. Field
indexes are built with the unchanged promotion, recursion and tag rules.
Plans contain type information and indexes, never request values or uploaded
file objects. Request factories are not called during registration.

The route retains at most 128 type plans. New types beyond the bound are still
bound normally without retention. There is no global registry, eviction policy
or background goroutine. RWMutex protects the cache; cached plans are immutable.

Regressions cover concurrent cold/warm use with distinct requests/files,
recursive embedding, text decoding, error order, type-capacity fallback,
dynamic factory types and query/path precedence. Existing form, multipart,
body-limit and error tests also pass.

### gRPC unary middleware

The unary endpoint chain is composed once in NewServer. Each request retains
its own handler in the context and continues to receive its own metadata.
An empty chain uses the direct callHandler path. Health bypass, middleware
order, short-circuiting, dropped-context errors and status conversion remain.
Streaming middleware behavior is unchanged.

A concurrent regression verifies one wrapper construction and isolation of
64 request handlers, payloads, operations and metadata sets. Wrapping middleware
once matches the HTTP endpoint composition model; it intentionally removes
per-RPC wrapper construction.

### Logging attributes and nil timing

Allocate capacity for all ten base attribute elements, plus optional reason
and error pairs, instead of allocating seven slots and immediately growing the
slice. Records still own their attribute slices; no pooling or retention rule
was introduced. Keys, values, order and error identity remain unchanged.

A nil logger skips clock reads but still invokes the configured level callback
after the handler. A disabled non-nil logger retains timing and its post-handler
Enabled call: moving these checks would change callback order or include level/
Enabled work in the recorded duration. A synctest regression confirms that the
duration excludes time spent in both callbacks.

## Measurement

Environment: Go 1.27.3, darwin/arm64, Apple M5. Before binaries were compiled
before editing production source; after binaries use the same benchmark
fixtures. Runs use matching local core/contrib sources through external
modfiles, without committed replacements.

The broad comparison has 73 cases per version with ten samples each. Serial
cases use GOMAXPROCS=1 and 100ms/sample; network parallel cases use
GOMAXPROCS=8 and 300ms/sample. Before/after order alternates each round.
Because the first broad run showed substantial time variance even in native
controls, 15 targeted cases were also measured at 500ms/sample: five samples
before, five after, five after, five before. Benchmarks run sequentially,
without concurrent checks or profiling. Statistics use benchstat.

### Targeted time results

| Case | Before | After | benchstat result |
|---|---:|---:|---|
| HTTP query/path binding | 2.877 us/op | 1.919 us/op | -33.28%, p<0.001 |
| HTTP client only, 128 B | 2.524 us/op | 2.002 us/op | -20.65%, p=0.002 |
| HTTP client only, 4 KiB | 10.047 us/op | 8.054 us/op | -19.84%, p=0.023 |
| gRPC interceptor, one wrapper | 104.85 ns/op | 90.66 ns/op | -13.53%, p<0.001 |
| gRPC interceptor, five wrappers | 152.40 ns/op | 96.35 ns/op | -36.78%, p<0.001 |
| gRPC interceptor, empty chain | 91.03 ns/op | 89.12 ns/op | No significant difference, p=0.165 |
| Logging with nil logger | 72.935 ns/op | 5.012 ns/op | -93.13%, p<0.001 |
| Logging, enabled/discard | 173.1 ns/op | 189.5 ns/op | Inconclusive, p=0.853 |

These are local microbenchmarks, not production latency reductions. Host
variance remains visible: the 4 KiB client after interval is wide, and the
unchanged native controls also move in some cases. Logging's allocation win
is reliable; a stable enabled-logging time improvement is not established.
The server-chain time result is also inconclusive in the longer run.

### Allocation results

| Case | B/op before -> after | allocs/op before -> after |
|---|---:|---:|
| HTTP query/path binding, including equal harness costs | 8977 -> 7537 | 65 -> 33 |
| HTTP client only, 128 B | 4400 -> 3784 | 51 -> 42 |
| HTTP client only, 4 KiB | about 33219 -> 27882 | 60 -> 51 |
| gRPC interceptor, five wrappers | 288 -> 208 | 9 -> 4 |
| gRPC interceptor, empty chain | 208 -> 208 | 4 -> 4 |
| Logging, enabled/discard | 400 -> 224 | 6 -> 5 |
| Seven-middleware server chain | 624 -> 448 | 13 -> 12 |

All listed allocation reductions are statistically significant, p<0.001 with
ten samples. They isolate framework success-path work. Real log encoding/I/O,
JWT verification, telemetry export, retries and other failure work are excluded.

### Network results

The eight-worker, 128 B, warmed HTTP full-client/server case improves from
18.63 to 17.01 us/op (-8.69%, p<0.001). Its native control is unchanged within
noise (16.17 vs 16.16 us/op, p=0.971). Allocations fall from 138 to 129 per
operation. RunParallel measures aggregate elapsed time per completed operation,
not individual request latency or percentiles.

The initial serial network comparisons have wide intervals and do not support
a general end-to-end latency improvement claim. The final gRPC network results
likewise do not establish an overall latency improvement from removing small
wrapper costs. Raw native controls and all measured cases are retained with
the evidence rather than filtered to favorable outcomes.

## Verification and reproduction

The three affected modules pass task check, including formatting, modernization,
tidy, vet, both builds, dependency checks and shuffled race tests. Default and
integration-tag lint and vulnerability checks pass. The vulnerability scan
finds no reachable vulnerabilities; gRPC's existing module graph has two
module-level advisories in packages it does not import. Requirements/checksums
and the core remain unchanged.

Use the same fixtures and Go toolchain on both versions. For each affected
module, compile its test binary before and after the production edits:

```sh
# transport/httpx or transport/grpcx:
go test -c -o /tmp/before.test .
# After applying the source changes:
go test -c -o /tmp/after.test .

# Example: select a named microbenchmark and alternate versions.
/tmp/before.test -test.run='^$' -test.bench='^BenchmarkHTTPQueryBinding$' \
  -test.benchmem -test.benchtime=500ms -test.count=5 -test.cpu=1
/tmp/after.test -test.run='^$' -test.bench='^BenchmarkHTTPQueryBinding$' \
  -test.benchmem -test.benchtime=500ms -test.count=5 -test.cpu=1
# Repeat after then before, append outputs and compare with benchstat.
```

For middleware compile ./internal/benchmarks. Respect the repository's matching
source/modfile rules and measure serially. CPU frequency, scheduling, unrelated
host load and very short handlers affect time measurements; allocation changes
are the most stable evidence from this investigation. Deployment and publication
are separate from these measurements.
