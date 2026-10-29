# DI performance measurements

Measured on 2026-10-29 with Go 1.27.3, darwin/arm64, Apple M5 and
samber/do/v2 v2.1.0. The benchmarks use the current local core and contrib
sources. Each case ran ten times for 200ms with GOMAXPROCS=1 and 8;
benchstat reports the medians below. Profiling was a separate run.

## Request path

Resolve services during application assembly and inject the resulting values
into handlers. The prewired benchmark calls the same handler method as manual
assembly, without entering the container during requests. Neither core App nor
Endpoint automatically resolves dependencies. Current CLI service templates and
example assembly also use ordinary constructors and do not require a container.

Serial results with GOMAXPROCS=1:

| Operation | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Core typed Resolve helper with a static, non-lookup Resolver | 1.862 | 0 | 0 |
| Cached native do.Invoke | 94.33 | 144 | 5 |
| Cached servex di.Resolve through the do adapter | 76.39 | 96 | 4 |
| Request, manually assembled endpoint | 1.722 | 0 | 0 |
| Request, endpoint resolved once before serving | 1.765 | 0 | 0 |
| Request, native do.Invoke on every call | 101.4 | 144 | 5 |
| Request, servex di.Resolve on every call | 82.47 | 96 | 4 |

The serial manual/prewired difference is not statistically significant
(p=0.060, n=10). Both allocate zero objects per request. The static Resolver
case isolates the core helper; it does not represent a real container lookup.
The adapter does not add allocations over native do in this fixture. Its named,
any-valued registrations take a different SDK invocation path, so these results
must not be generalized to every type or native do configuration.

Parallel results:

| Request mode | GOMAXPROCS=1 ns/op | GOMAXPROCS=8 ns/op | B/op | allocs/op |
|---|---:|---:|---:|---:|
| Manual | 1.823 | 0.3523 | 0 | 0 |
| Servex prewired | 1.861 | 0.3890 | 0 | 0 |
| Native lookup on every call | 108.0 | 173.3 | 144 | 5 |
| Servex lookup on every call | 86.16 | 143.8 | 96 | 4 |

RunParallel's ns/op measures aggregate wall time per completed operation, not
individual request latency. Manual/prewired cases are near the measurement
floor: the 8-CPU serial comparison favored prewired by 7.68%, while the parallel
comparison favored manual by 10.42% (only 0.0367ns/op). Both execute the same
handler with no DI calls. These tiny, opposing differences do not establish a
DI request-path regression or improvement.

Repeated lookup has measurable synchronization and allocation costs even
after singleton construction. A separate 8-CPU mutex profile attributed 98.5%
of sampled mutex delay to call stacks through Container.Resolve. This is
accumulated goroutine waiting time, not a percentage of application CPU time or
request latency. The container serializes root resolution to prevent concurrent
cyclic graphs from deadlocking; the lifecycle lock excludes shutdown and new
registration. The SDK also enters its invocation and singleton machinery for
cached values. Keep that machinery out of request handlers instead of removing
the correctness locks.

At 100,000 lookups/second, this fixture's 96 B and four allocations per lookup
would produce 9.6 MB/second of allocation traffic and 400,000 allocations/second.
That is allocation churn, not retained heap size. Prewired handlers incur none
of this DI lookup traffic.

## Assembly and lifecycle

The fixture is a four-node in-memory graph: config, repository, service and
endpoint. The timed operation includes registration, first resolution and
shutdown, without network connections or external resources.

| Assembly mode, GOMAXPROCS=1 | Time/op | B/op | allocs/op |
|---|---:|---:|---:|
| Manual constructors | 29.88ns | 48 | 4 |
| Native do | 14.28us | 22666 | 256 |
| Servex do adapter | 14.61us | 22162 | 255 |

Container assembly costs more than direct constructor calls. Here that cost
is about 15 microseconds for the complete graph and lifecycle, paid during
assembly rather than on each request. These figures do not predict startup
time for larger dependency graphs or constructors performing I/O.

## Reproduction and limits

Run from this module's directory with the same Go toolchain:

```sh
GOWORK=off go test -run='^$' -bench='^Benchmark' -benchmem \
  -benchtime=200ms -count=10 -cpu=1,8 ./... > benchmark.txt
benchstat benchmark.txt
```

For unreleased matching peers, use an external temporary modfile as described
in the repository's CONTRIBUTING.md. Do not commit local replacement directives.
Run performance measurements serially, without the race detector or concurrent
test/lint workloads. Profiling changes timing and must be kept separate from
the uninstrumented results above.

The endpoint does only an in-memory field read and returns an existing pointer.
These are DI and endpoint microbenchmarks, not HTTP/gRPC throughput, latency
percentiles, serialization, middleware, database or production load tests.
Use an application workload and service profiles before optimizing those paths.
The benchmark implementation is in container_bench_test.go. Production DI code
was not changed for this investigation.
