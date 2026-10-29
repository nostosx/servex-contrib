# selector/random

Provides a weighted random implementation of the core Selector contract inside
the existing `servex-contrib/selector` module.

```go
import "github.com/nostosx/servex-contrib/selector/random"

selector := random.New()
```

Each call chooses a node with probability proportional to Node.Weight; weights
less than one count as one. An empty set returns selector.ErrNoAvailable.
Returned Done callbacks are non-nil on success and have no state to update.
The default source is math/rand/v2 and supports concurrent calls.

This package is available in selector v1.2.0 with servex v1.4.0. The original
deterministic weight-mapping, distribution and concurrency regression tests
move with the implementation; the core no longer constructs an algorithm.

## Release

Version v1.2.0, released 2026-10-29. Requires servex v1.4.0. Install from the public Go proxy without source replacements:

```sh
go get github.com/nostosx/servex-contrib/selector@v1.2.0
```
