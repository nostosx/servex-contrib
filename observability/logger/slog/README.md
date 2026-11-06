# observability/logger/slog

Implements the published servex v1.7.0 Logger contract with log/slog. Configuration
and construction belong to this contrib module; the application combines Config
with its HTTP/database settings. Current source is unreleased; use Go 1.27.3 and
[source validation](../../../CONTRIBUTING.md#checks).

## Configuration

```go
config := slog.DefaultConfig()
// Decode the application's log section into config with config/viper.
log, err := slog.New(config)
if err != nil { return err }
defer log.Close()
application := app.New(app.WithLogger(log))
```

The log section can be represented as TOML:

```toml
[log]
level = "info"
format = "json"
output = "stderr"
add_source = false
```

Config has JSON tags consumed by the Viper adapter. DefaultConfig selects
info/json/stderr without source locations; Config{} has the same defaults.
Level accepts debug, info, warn and error. Format accepts json or text. Empty
strings select defaults. Validate checks fields without opening files or changing
global state. Invalid settings return ErrInvalidConfig with a field name; raw
configuration values are not included in validation errors.

Output selects stdout, stderr or a file path. New opens files for append with
creation mode 0600; existing permissions remain unchanged. Parent directories
must already exist. Close releases an owned file once; stop log producers first.
Standard streams stay open. A process reloader must explicitly replace a logger;
loading a new configuration snapshot does not mutate an existing handler.

Runtime dependencies are passed separately:

```go
log, err := slog.New(config, slog.WithWriter(writer))
```

WithWriter borrows an io.Writer and overrides the configured output. Close never
closes that writer. Configure a custom native handler through Wrap:

```go
backend := standardslog.New(standardslog.NewJSONHandler(writer, handlerOptions))
log := slog.Wrap(backend)
```

Wrap is the previous New(native) entry point. Wrap(nil) captures slog.Default
without changing it. Native handler enrichment/filtering and context remain
intact, and Close never closes a wrapped backend. AddSource records the caller of
the servex Log method. Fields are alternating string keys and values. Logger must
not be copied and is safe for concurrent use when its handler/output is safe.

## Checks

```sh
bash .github/scripts/check-source.sh check lint vulncheck MODULES=observability/logger/slog
```

Run from the repository root. Tests exercise actual JSON/text records, level and
source fields, validation before output creation, file append/close behavior,
borrowed writers, native wrapping, context and concurrent calls.
