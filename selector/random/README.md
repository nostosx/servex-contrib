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

This package is unreleased and requires matching core sources. The original
deterministic weight-mapping, distribution and concurrency regression tests
move with the implementation; the core no longer constructs an algorithm.
