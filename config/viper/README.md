# config/viper

Implements the contract-only `github.com/nostosx/servex/config` package with
`github.com/spf13/viper`. This module implements the configuration contracts in servex v1.4.0.

## Usage

```go
package main

import (
 "context"
 "errors"
 "log"

 "github.com/nostosx/servex/config"
 cfgviper "github.com/nostosx/servex-contrib/config/viper"
 "github.com/nostosx/servex-contrib/config/viper/file"
)

func main() {
 ctx := context.Background()
 cfg, err := cfgviper.New(ctx, []config.Source{
  file.New("config.yaml"),
  cfgviper.Env("APP_"),
 }, cfgviper.WithValidator(func(_ context.Context, candidate config.Reader) error {
  port := candidate.Int("http.port")
  if port < 1 || port > 65535 { return errors.New("http.port is out of range") }
  return nil
 }))
 if err != nil { log.Fatal(err) }
 view := cfg.Snapshot()
 log.Println(view.Int("http.port"))
 if err := cfg.Watch(ctx, func(current config.Config) {
  log.Println(current.Snapshot().Int("http.port"))
 }, cfgviper.WithReloadErrorHandler(func(err error) { log.Println("reload rejected:",err) })); err != nil {
  log.Fatal(err)
 }
}
```

Later Sources override earlier ones. Nested objects retain earlier sibling
keys; scalar and list values replace the previous value, including object to
scalar transitions. Viper owns setting storage/case folding and decoding;
the adapter does not expose a mutable Viper instance or use its global registry.
The ordered Source API deliberately controls precedence rather than combining
Viper's implicit flag/env/remote priority layers.

Each Reload constructs a private candidate, validates the complete tree,
checks cancellation and atomically publishes an immutable snapshot.
Failed loads or validations retain the old revision. Snapshot reads and
decoded containers are isolated; a validator cannot replace another view.
Watch subscribes only to `config.Watchable` Sources, coalesces notifications,
serializes callbacks and joins all watchers on cancellation. Callbacks may
Reload; validators must not reenter Reload. Applying new values to running
components is the application's responsibility.

## Sources and formats

`Env("APP_")` snapshots process environment. Prefix matching is case-sensitive,
keys are lowercased and double underscores separate levels:
`APP_HTTP__PORT=8080` becomes `http.port`. Values are strings; Env has no watcher.

`file.New(path)` supports Viper's file formats by default, including JSON,
YAML/YML, TOML and env/dotenv. No YAML codec registration is
required. Custom `file.WithCodec(extension, codec)` overrides a format;
nil removes it. `WithPollInterval` defaults to two seconds. Polling can miss
changes between polls; removal retains the old config and reappearance triggers
reload. Use absolute paths to pin file locations.

Existing `config/consul`, `config/etcd` and `config/nacos` adapters remain
independent Sources: applications explicitly pass them to this engine.

## Decoding and migration

The core `config` package has no constructors, Env, file package, decoder,
merge algorithm or snapshot implementation.

- Replace core constructors/options with `cfgviper.New`, `Env`,
  `WithValidator` and `WithReloadErrorHandler`.
- Replace `*config.Config` with `config.Config`; replace
  `*config.Snapshot` with `config.Reader`.
- Unmarshal uses JSON tags, mapstructure weak conversion, embedded-field
  squashing, duration and TextUnmarshaler hooks. Error messages follow Viper.
- Viper treats null as unset. Defaults in destinations remain unchanged.
  Embedded fields sharing names can all be populated; nil embedded pointers
  are not automatically allocated. Scalar getters use Viper's cast rules.
- Weak numeric conversions may truncate fractions or overflow narrow types.
  Use a validator on the candidate before decoding into narrow business types.
  The adapter does not maintain a second custom reflection decoder.

## Validation

Use repository Task gates with an isolated module override pointing at the
matching unpublished core. No sibling replacements are stored in go.mod.
Tests cover ordered layers, mutation isolation, accepted/rejected revisions,
concurrent reload/read, synchronous callbacks, cancellation, watcher failures,
Viper decoding and file formats. Real backend services are verified in their
own source modules.

## Release

Version v1.0.0, released 2026-10-29. Requires servex v1.4.0. Install from the public Go proxy without source replacements:

```sh
go get github.com/nostosx/servex-contrib/config/viper@v1.0.0
```
