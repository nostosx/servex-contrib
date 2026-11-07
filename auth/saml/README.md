# auth/saml

SAML 2.0 enterprise browser SSO as a Service Provider (SP), using
[gosaml2 v0.12.0](https://github.com/russellhaering/gosaml2) and its XML signature
validation, canonicalization and decryption. crewjam is used only as an independent
IdP fixture in tests; its response-validation functions are not imported by runtime code. Target module version: **v1.0.0**.
Go 1.27.3; MIT. There is no account database, session policy or implicit router.

## Supported flow

- SP-initiated SSO: signed RSA-SHA256 HTTP-Redirect AuthnRequest, HTTP-POST ACS.
- Trusted IdP EntityDescriptor metadata, including multiple signing certificates
  for rollover; SP metadata export with its signing/decryption certificate.
- Signed responses or signed assertions, including encrypted assertions.
- Signature/issuer, request correlation, audience, recipient, assertion validity
  and SubjectConfirmation validation. SHA-1 signatures/signing digests are rejected.
- Browser-bound one-time RelayState, opaque random values of at most 80 bytes,
  consumed atomically before parsing the response. No arbitrary redirect URL.
- Verified NameID, format/qualifiers, attributes, SessionIndex and assertion/session
  expiry returned for explicit account mapping.

Configure a compatible IdP, such as Entra ID, Okta, Keycloak or AD FS, with the
exact SP EntityID, ACS URL and SP certificate. Those products are interoperability
targets, not claims of live vendor acceptance. This package implements the SP
role. SAML IdP issuance, unsolicited/IdP-initiated login, Artifact/SOAP, SLO,
dynamic browser-selected metadata and transient account identifiers are excluded.
Existing local sessions are revoked by the application's own session policy;
SAML SessionIndex alone does not implement single logout.

## Assembly

```go
states, err := saml.NewMemoryStore(10000) // One process only.
if err != nil { return err }
provider, err := saml.New(saml.Config{
    EntityID: "https://accounts.example.com/saml/entity",
    ACSURL: "https://accounts.example.com/saml/acs",
    IDPMetadata: trustedMetadataXML,
    Key: rsaKey,
    Certificate: certificate,
}, states)
if err != nil { return err }
metadataXML, err := provider.Metadata()
if err != nil { return err }
authorization, err := provider.Begin(ctx, browserSessionBinding)
if err != nil { return err }
// Persist the binding in the browser session before redirecting.
identity, err := provider.Complete(ctx, browserSessionBinding, relayState, samlResponse)
if err != nil { return err }
// Resolve the verified identity against this tenant's account policy.
```

`Key` and `Certificate` are borrowed, immutable matching RSA material of at least
2048 bits. Store private keys outside source and secret-bearing config files.
Metadata is supplied through a trusted administrative channel; the adapter never
downloads a URL from a browser/assertion. Select one explicit EntityDescriptor,
not an ambiguous EntitiesDescriptor aggregate. Constructors parse/copy metadata
and require current IdP signing certificates. Expiring metadata and keys need
explicit refresh/reconstruction by the operator. No global SDK clocks or
verification settings are changed.

Use `provider.Metadata()` on a separately selected public metadata GET route.
Use [`auth/login`](../login) `Config.SAML` for Session composition. The ACS is a
cross-site POST: use Secure, HttpOnly, SameSite=None sessions and exclude only
the exact ACS route from generic cross-origin POST/CSRF rejection. Continue to
validate RelayState, browser binding and the signed assertion. Other mutations
retain CSRF protection. If writing your own handler, require exactly one
SAMLResponse and RelayState from a bounded application/x-www-form-urlencoded
POST body; never merge query values, accept SAMLart or use raw XML in logs/errors.
Complete caps the encoded SAMLResponse at 1 MiB; login caps the form at 2 MiB.

Account identity is scoped by the configured tenant/IdP and the tuple
Issuer, Subject, NameIDFormat, NameQualifier, SPNameQualifier. Do not key on
email or grant local roles from attribute names without explicit application
policy. The default requested NameID is persistent; transient NameIDs are
rejected. Applications must constrain their local session lifetime and permissions
using identity.ExpiresAt and their own policy. The returned SessionIndex is data
for application policy, not an automatically active logout subscription.

## State storage and verification

MemoryStore is bounded, lazily expires entries and loses attempts on restart.
Replicas need [`auth/saml/redis`](redis) or another atomic StateStore. Save must
be insert-only; Take must delete/read atomically. An uncertain take or failed
response requires a new login. Multi-instance assertions cannot be accepted
without the original outstanding request and original browser binding.

```sh
task check lint vulncheck MODULES='auth/saml auth/saml/redis auth/login'
SERVEX_IT_REDIS_ADDR='...' task test INTEGRATION=true MODULES=auth/saml/redis
```

Tests use fresh keys and actual SDK-signed/encrypted XML, validate correlation,
browser/provider binding, signature trust, audience/recipient/expiry, tampering,
unsigned/unsolicited responses, SHA-1 rejection and single consumption. They do
not prove live Entra/Okta/AD FS setup. Metadata/API boundaries follow the
[SAML 2.0 specifications](https://docs.oasis-open.org/security/saml/v2.0/).
