# validation/protovalidate

Protobuf validation middleware backed by `buf.build/go/protovalidate`.
It uses an explicitly supplied Buf Validator and owns its SDK dependencies;
core and generic middleware remain SDK-free. This module is unreleased and
requires matching source validation through temporary external modfiles.

## Schema and usage

Keep validation rules in your Proto schema:

```protobuf
syntax = "proto3";
package user.v1;
import "buf/validate/validate.proto";

message CreateUserRequest {
  string name = 1 [(buf.validate.field).string.min_len = 3];
}
```

Use your existing Buf/protobuf generation pipeline for those messages. Construct
the validator and use the returned middleware with endpoint.Chain or the
HTTP/gRPC/Connect adapter's endpoint middleware options:

```go
import (
    bufvalidate "buf.build/go/protovalidate"
    "github.com/nostosx/servex-contrib/validation/protovalidate"
)

engine, err := bufvalidate.New()
if err != nil {
    return err
}
middleware := protovalidate.New(engine)
```

The endpoint request must itself implement proto.Message. For Connect handlers,
use this inside the servex endpoint adapter where the protobuf payload is
available, rather than passing a transport envelope. Streaming applications
choose where to validate each message; this adapter does not automatically
read streams or replay messages. Application-owned wrapper types need an
explicit adapter to their protobuf payload.

## Error and ownership contracts

Constraint violations become InvalidArgument/VALIDATION_FAILED with a generic
public message. The original *bufvalidate.ValidationError remains available
through errors.As. Invalid request types, nil messages, invalid schemas/CEL
compilation and evaluation failures are Internal/VALIDATION_ERROR. Schema and
runtime causes are retained; their details are not copied into public messages.

The supplied Validator is safe for concurrent calls. Construct it and configure
schema preloading or other options at the application composition root. Nil
validators panic. Cancellation is checked before and after validation and its
cause is preserved. Buf's synchronous Validate API has no context parameter;
this middleware does not interrupt an in-flight CEL evaluation. Validation
failures do not call the next endpoint. Successful payload/context and all
downstream results/errors are passed through unchanged.

## Checks

Use `MODULES='validation/protovalidate' task check lint vulncheck` with matching
core sources. Tests construct annotated descriptors dynamically, run real Buf
validation and verify schema compilation failures without requiring protoc,
Buf binaries, generated test artifacts or external services.

## License

[MIT](LICENSE); the upstream Buf SDK is Apache-2.0 licensed.
