# Security policy

Starting with v1.0.0, security fixes are provided for the latest minor release
of each module's v1 series. Modules are versioned independently. Upgrade to
the latest patch release before reporting a vulnerability. Reports about the
current development code are also welcome.

## Reporting a vulnerability

Use the repository's **Security → Report a vulnerability** form when private
reporting is enabled. If it is unavailable, open an issue asking for a private
reporting channel without disclosing the vulnerability or exploit details.
Do not include credentials, personal data or access-bearing URLs.

Include the affected module and revision, prerequisites, a minimal
reproduction, expected behavior and impact. Coordinate public disclosure
with the maintainers after a fix and regression test are available.

## Scope

Report failures in this project's security boundaries, including cancellation
or shutdown behavior that leaks resources, unsafe input handling and accidental
credential disclosure. Application authentication, authorization, transport
security and backend permissions must still be configured by the caller.
Use dedicated disposable services for reproductions; never send a test against
shared or production infrastructure without authorization.
