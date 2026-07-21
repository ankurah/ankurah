# ankurah-jwt-auth

JWT policy agent for Ankurah. It supports the original local RSA signing-key
mode and an issuer-trust mode for resource servers that accept externally
issued access tokens.

## Issuer trust

Enable the native-only `issuer-http` feature to resolve an OpenID Connect
discovery document and keep its JWKS cached in the background:

```rust,no_run
use ankurah_jwt_auth::{IssuerVerifier, JwtAgent};
use std::{collections::HashSet, time::Duration};

# async fn example() -> anyhow::Result<()> {
let verifier = IssuerVerifier::discover(
    "https://id.example/.well-known/openid-configuration",
    "https://id.example",
    HashSet::from(["resource-client".to_string()]),
    vec!["user".to_string()],
    Duration::from_secs(30 * 60),
)
.await?;
let agent = JwtAgent::new_durable_issuer(verifier, "policy.json")?;
# let _ = agent;
# Ok(())
# }
```

The discovery URL is validated before network I/O and must return the exact
expected HTTPS issuer. Redirects and non-success responses are rejected. The
resolved HTTPS `jwks_uri` is serialized in `IssuerTrustDescriptor`; it is not
configured as a pinned key URL. Construction performs the initial JWKS fetch,
then a background task refreshes on the configured TTL and through a
single-flight, rate-limited unknown-`kid` trigger. HTTP connection and request
deadlines prevent a stalled issuer from wedging construction or the refresh
worker. Verification itself is synchronous and snapshots the cached key.

Issuer mode accepts only RS256 tokens with `kid`, `typ: at+jwt`, no `crit`
header, a configured issuer and audience, `exp`, `sub`, and
`token_use: access`. Its clock tolerance defaults to 60 seconds. Token-supplied
roles are discarded and replaced with the descriptor's policy-owned default
roles; `roles` and `email` may be absent from an issuer token.

The optional `JwtPolicy.trust_json` field keeps previously stored policy
entities readable. Durable watchers publish exactly one trust alternative:
issuer descriptor or legacy public-key PEM. Ephemeral agents treat a valid
descriptor as policy-ready without fetching JWKS, because they do not verify
inbound server state.
