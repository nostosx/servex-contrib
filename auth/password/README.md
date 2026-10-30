# password

**v1.0.0 (2026-10-31).** Argon2id password hashing using the Go project's official
`golang.org/x/crypto/argon2` v0.58.0. Go 1.27.3; MIT. No user database or login
handler is imposed. Algorithms come from x/crypto; PHC encoding is local glue.

## Usage

This complete program hashes a supplied password and verifies it without logging
the password or its stored hash. Set DEMO_PASSWORD before running it.

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"

    "github.com/nostosx/servex-contrib/auth/password"
)

func run() error {
    hasher, err := password.New(password.DefaultParameters())
    if err != nil { return err }
    input := []byte(os.Getenv("DEMO_PASSWORD"))
    defer clear(input)
    encoded, err := hasher.Hash(context.Background(), input)
    if err != nil { return err }
    // Persist encoded in the application's credential table.
    matched, err := hasher.Verify(context.Background(), input, encoded)
    if err != nil { return err }
    fmt.Println("password matched:", matched)
    return nil
}

func main() { if err := run(); err != nil { log.Fatal(err) } }
```

## Contract

Each hash uses a fresh cryptographic salt and stores its algorithm, version and
parameters in a PHC string. Verify uses constant-time key comparison. A wrong
password returns false; a malformed/unsupported hash returns ErrInvalidHash.
NeedsRehash compares stored parameters with this hasher's policy; rehash only
after successful authentication and persist through the application's transaction.

Defaults are 64 MiB, three passes, four lanes, a 16-byte salt and a 32-byte key.
Benchmark and tune for your host. Verification rejects excessive parameters
(over 256 MiB, ten passes or eight lanes), unsupported versions and invalid
encodings before running the KDF. Passwords are limited to 1024 bytes. Passwords
are byte strings: no implicit trimming, Unicode normalization or truncation.

Argon2 is synchronous and cannot be interrupted midway. Context cancellation is
checked before and after it, not a CPU/memory cancellation guarantee. Bound
concurrent password operations and login attempts in the application. The caller
owns password buffers; the adapter clears its derived-key buffers only.

## Checks

From the repository root: `task check lint vulncheck MODULES=auth/password`.
Tests cover matching, mismatch, fresh salts, policy upgrades, malformed/excessive
parameters and cancellation. Install this module at v1.0.0.
