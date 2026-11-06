# observability/logger/zap

Module version for this revision: **v1.1.0**.

Implements the published servex v1.7.0 Logger contract with go.uber.org/zap.
Configuration and construction belong to this contrib module; the application
combines Config with its HTTP/database settings. Use Go 1.27.3 and [validation commands](../../../CONTRIBUTING.md#checks).

## Configuration

```go
config := zap.DefaultConfig()
// Decode the application's log section into config with config/viper.
log, err := zap.New(config)
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
Level accepts debug, info, warn and error. Format accepts json or text (Zap console
encoding). Empty strings select defaults. Validate checks fields before opening
files or changing global state. Invalid settings return ErrInvalidConfig with a
field name; raw configuration values are omitted from validation errors.

Output selects stdout, stderr or an append-only file. New creates files with
mode 0600; existing permissions remain unchanged. Parent directories must already
exist. Close flushes a New-created logger and closes its own file once; stop
producers first. Standard streams stay open and are not fsynced. Sync/file-close
errors are preserved. Loading a configuration snapshot does not reconfigure a
running logger automatically.

Runtime dependencies are supplied separately:

```go
log, err := zap.New(config, zap.WithWriter(writer))
```

WithWriter borrows an io.Writer and overrides the configured output. Close never
closes it, but calls its Sync method when implemented and retains that error.
Use Wrap for a custom native Zap core, fields or sampling:

```go
backend, err := upstreamzap.NewProduction()
if err != nil { return err }
defer backend.Sync()
log := zap.Wrap(backend)
```

Wrap is the previous New(native) entry point. Wrap(nil) selects a no-op backend.
Close does not call Sync or close resources on a wrapped backend: those remain
caller-owned. No path changes Zap globals. Debug/Info/Warn/Error are mapped by
severity; higher levels remain Error and never panic or exit. Context is accepted
by the core contract; Zap receives explicit fields. AddSource records the caller
of the servex Log method. Logger must not be copied and supports concurrent calls
when its backend/output does.

## Checks

```sh
task check lint vulncheck MODULES=observability/logger/zap
```

Run from the repository root. Tests exercise actual JSON/text records, level and
caller fields, validation before output creation, file append/close, retained
Sync errors, borrowed resources, native wrapping and concurrent calls.


## v1.1.0: readable output and rotating files

Format readable emits a single escaped line with component and bracketed fields.
Labels optionally maps structured keys to presentation labels; it never renames
JSON fields. It uses second-precision timestamp and caller keys. Existing json
and text formats keep their encoder semantics.

File is an additional JSON output, configured with path, max_size (MiB),
max_backups, max_age (days), and compress. Empty path disables it. Zero size uses
lumberjack's 100 MiB default; zero backup/age values disable those retention
limits. Configuration validation precedes output acquisition. Close flushes the
main output and closes owned primary/rotating files once; WithWriter remains
borrowed. File rotation reuses lumberjack v2.2.1, previously owned by the consumer.
