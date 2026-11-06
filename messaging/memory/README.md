# messaging/memory

Current source is unreleased. Use Go 1.27.3; core integration targets
published servex v1.7.0. See the
[source-validation commands](../../CONTRIBUTING.md#checks).

In-memory messaging.

Implements messaging.Publisher, Subscriber and ReadySubscriber. Consumer groups,
broadcast delivery, retry/dead-letter policy, cancellation and copied message
data retain the original regression coverage. consumertest checks core
consumer lifecycle composition against this explicit backend.

## Validation

Run repository `task check MODULES='messaging/memory'`, `task lint` and
`task vulncheck` with matching core/contrib sources. During development,
throwaway modfiles may resolve unpublished peers locally; do not commit local
replacements or claim public-proxy acceptance before coordinated publication.
Original unit, race and composition regressions moved with the backend.
These tests do not require a shared or production service.
