# messaging/memory

Module version for this revision: **v1.0.0**.

Use Go 1.27.3; core integration requires published servex v1.7.0. See the
[validation commands](../../CONTRIBUTING.md#checks).

In-memory messaging.

Implements messaging.Publisher, Subscriber and ReadySubscriber. Consumer groups,
broadcast delivery, retry/dead-letter policy, cancellation and copied message
data retain the original regression coverage. consumertest checks core
consumer lifecycle composition against this explicit backend.

## Validation

Run repository `task check MODULES='messaging/memory'`, `task lint` and
`task vulncheck` against published dependencies. Source overrides are an explicit development mode;
see the root CONTRIBUTING guide.
Original unit, race and composition regressions moved with the backend.
These tests do not require a shared or production service.
