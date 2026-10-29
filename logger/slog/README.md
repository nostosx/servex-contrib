# logger/slog

Implements `servex/logger.Logger` with `log/slog`. This module requires
servex v1.4.0 or later.

```go
import (
 "log/slog"
 "os"
 slogadapter "github.com/nostosx/servex-contrib/logger/slog"
 "github.com/nostosx/servex/app"
)
log := slogadapter.New(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
application := app.New(app.WithLogger(log))
```

New(nil) captures slog.Default without changing the process default.
Levels and context are passed directly to slog. Handler configuration,
enrichment, redaction, level changes and output resource ownership remain
with the caller.


Fields are alternating string keys and values. The adapter is safe for
concurrent use when its caller-owned backend is. It never sets global loggers.
Use repository Task check/lint/vulncheck gates with matching core sources.

## Release

Version v1.0.0, released 2026-10-29. Requires servex v1.4.0. Install from the public Go proxy without source replacements:

```sh
go get github.com/nostosx/servex-contrib/logger/slog@v1.0.0
```
