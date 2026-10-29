# validation/playground

Struct validation middleware backed by `github.com/go-playground/validator/v10`.
This is an independent module; core and generic middleware do not depend on it.
Requires servex v1.4.0. No local source replacements are needed.

## Usage

Construct and configure the validator explicitly, before serving requests:

```go
package main

import (
    "context"
    "fmt"

    "github.com/go-playground/validator/v10"
    "github.com/nostosx/servex-contrib/validation/playground"
)

type CreateUser struct {
    Name  string `json:"name" validate:"required"`
    Email string `json:"email" validate:"required,email"`
}

func main() {
    engine := validator.New(validator.WithRequiredStructEnabled())
    handler := playground.New(engine)(func(_ context.Context, req any) (any, error) {
        return req.(*CreateUser).Name, nil
    })
    name, err := handler(context.Background(), &CreateUser{Name: "alice", Email: "alice@example.com"})
    fmt.Println(name, err)
}
```

Use the returned endpoint.Middleware in HTTP, RPC, CQRS or consumer middleware
chains when the request is a struct or struct pointer. Requests without a
Validate method are still checked. This does not replace the SDK-free
middleware/validate adapter for request-owned Validate methods.

## Errors and concurrency

Field violations become InvalidArgument/VALIDATION_FAILED with the public
message `request validation failed`. The original validator.ValidationErrors
remain available through errors.As. Nil, typed-nil and non-struct requests
produce Internal/VALIDATION_ERROR with their InvalidValidationError cause;
they indicate a mismatched endpoint or middleware setup. Validator tag/config
panics remain SDK programming errors; configure and check rules at startup.

The validator is caller-owned. Register tags, field-name mapping and custom
validators before sharing it; do not reconfigure it during concurrent calls.
StructCtx receives the caller's context. Cancellation causes are returned
unchanged before or after validation, and failed validation never calls next.
The adapter cannot interrupt synchronous validation; custom validators must
observe context themselves when they perform work. Successful requests and
downstream results/errors pass through unchanged. Public error messages are
generic; causes retain SDK details for deliberate inspection by application code.

## Checks

From the repository root, run `MODULES='validation/playground' task check lint
vulncheck` with the matching core source. Tests cover actual tag validation,
cause identity, cancellation, middleware short-circuiting and concurrent use.

## License

[MIT](LICENSE).

## Release

Version v1.0.0, released 2026-10-29. Requires servex v1.4.0. Install from the public Go proxy without source replacements:

```sh
go get github.com/nostosx/servex-contrib/validation/playground@v1.0.0
```
