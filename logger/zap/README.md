# logger/zap

Implements `servex/logger.Logger` with `go.uber.org/zap`. This module requires
servex v1.4.0 or later.

```go
import (
 "go.uber.org/zap"
 zapadapter "github.com/nostosx/servex-contrib/logger/zap"
 "github.com/nostosx/servex/app"
)
backend, err := zap.NewProduction()
if err != nil { return err }
defer backend.Sync()
application := app.New(app.WithLogger(zapadapter.New(backend)))
```

New(nil) selects a no-op Zap backend. The caller owns backend construction,
sampling, fields, level configuration and Sync. Debug/Info/Warn/Error are
mapped by severity; higher levels remain Error and never panic or exit.
Context is accepted for the core contract; Zap receives only explicit fields.


Fields are alternating string keys and values. The adapter is safe for
concurrent use when its caller-owned backend is. It never sets global loggers.
Use repository Task check/lint/vulncheck gates with matching core sources.

## Release

Version v1.0.0, released 2026-10-29. Requires servex v1.4.0. Install from the public Go proxy without source replacements:

```sh
go get github.com/nostosx/servex-contrib/logger/zap@v1.0.0
```
