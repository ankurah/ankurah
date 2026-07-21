# JWT Auth Extension Specification

## Overview

The JWT auth extension provides role-based access control (RBAC) for ankurah nodes via RS256 JSON Web Tokens. It implements the `PolicyAgent` trait, intercepting all read, write, and query operations to enforce a declarative policy configuration.

The extension supports two node modes and two durable trust sources:

- **Durable nodes** load a policy config file and either hold local signing keys or verify access tokens against an HTTPS issuer discovered through OpenID Connect. A filesystem watcher detects policy changes and hot-reloads them without restart.
- **Ephemeral nodes** (WASM/mobile clients) start with deny-all defaults and receive the policy plus either a public key or an issuer trust descriptor from the durable node via a LiveQuery on the `jwtpolicy` collection. They do not fetch issuer JWKS.

## Permission Model

```
User JWT → Role(s) → Privilege(s) → Collection Access Rules
                                        ↓
                                  read / write + scope filters
```

Three layers:

1. **Roles** -- Assigned to users in their JWT `roles` claim (e.g. `"Admin"`, `"Dispatcher"`).
2. **Privileges** -- Named capabilities granted by roles (e.g. `"manage_jobs"`, `"view_users"`). The wildcard `"*"` grants all privileges.
3. **Collection rules** -- Per-collection mappings that specify which privilege is required for `read` and `write` access, plus optional row-level scope filters.

## Context Types

`JwtContext` is the `ContextData` type used by `JwtAgent`:

| Variant | Description | Wire serialization |
|---------|-------------|-------------------|
| `User { claims, token }` | Authenticated user. Claims extracted from a verified JWT. | Raw JWT bytes as `AuthData` |
| `Root` | Privileged system context. Bypasses all RBAC checks. | Cannot be serialized -- local-only |
| `NoUser` | Unauthenticated. Can only access the `jwtpolicy` collection. | Empty `AuthData` |

## JWT Claims

Tokens use RS256 with a 4096-bit RSA key pair. The claims structure:

| Claim | JWT field | Type | Description |
|-------|-----------|------|-------------|
| Subject | `sub` (standard) | `String` | User entity ID |
| Roles | `roles` (custom) | `Vec<String>` | Role names from the policy config |
| Email | `email` (custom) | `String` | User's email address |
| Name | `name` (custom) | `Option<String>` | User's display name |
| Custom | (any other field) | `Map<String, Value>` | Arbitrary extra claims, captured via `#[serde(flatten)]` |

Standard JWT timing claims (`iat`, `exp`, `nbf`) are handled by the `jwt_simple` library.

### Trust Modes

Legacy local-key mode verifies RS256 tokens against one configured public key. It expects the application-defined `JwtClaims` fields and can sign tokens when a full `SigningKeys` keypair is present.

Issuer mode is native-only and requires the `issuer-http` feature for discovery and background key refresh. It accepts only tokens meeting all of these conditions:

- RS256 signature, a non-empty `kid`, `typ: at+jwt`, and no `crit` header.
- Exact configured HTTPS issuer and at least one configured audience.
- Required `exp` and `sub` standard claims.
- `token_use: access`.
- A signature from the current last-good JWKS snapshot.

Issuer-token `roles` are untrusted and discarded. The verifier installs policy-owned `default_roles`; `roles` and `email` may therefore be absent from the token. The default clock tolerance is 60 seconds.

### Unverified Parsing

`parse_claims_unverified(token)` decodes the payload without signature verification, for use on clients that only need to read claim data (e.g. displaying the current user).

## Policy Configuration

The policy is a JSON file with two top-level fields: `roles` and `collections`.

### Format

```json
{
  "roles": {
    "Admin": ["*"],
    "Dispatcher": ["view_jobs", "manage_jobs", "view_users"],
    "Technician": ["view_jobs", "update_own_jobs"]
  },
  "collections": {
    "job": {
      "read": "view_jobs",
      "write": "manage_jobs",
      "scope": [
        {
          "filter": "assigned_to = $jwt.sub",
          "unless_privilege": "manage_jobs"
        }
      ]
    },
    "user": {
      "read": "view_users",
      "write": "manage_users"
    }
  }
}
```

### `roles`

Type: `Map<String, Vec<String>>`

Maps each role name to its list of privilege strings. A role with `["*"]` is granted all privileges (wildcard).

### `collections`

Type: `Map<String, CollectionRules>`

Each entry defines access rules for one collection:

| Field | Type | Description |
|-------|------|-------------|
| `read` | `Option<String>` | Privilege name required for read access. `None` = no read access. |
| `write` | `Option<String>` | Privilege name required for write access. `None` = no write access. |
| `scope` | `Vec<ScopeRule>` | Row-level filters injected into queries (default: empty). |

### Scope Rules

Scope rules restrict query results at the row level. Each rule has:

| Field | Type | Description |
|-------|------|-------------|
| `filter` | `String` | AnkQL predicate with `$jwt.*` variable placeholders |
| `unless_privilege` | `Option<String>` | If the user holds this privilege, skip this filter |

Multiple scope rules are AND-ed together. If no scope rules are defined for a collection, queries are unfiltered (beyond the collection-level access check).

### Variable Substitution

Scope filter strings support `$jwt.*` variables that are resolved from the authenticated user's claims before parsing with the AnkQL parser:

| Variable | Resolves to |
|----------|------------|
| `$jwt.sub` | User entity ID (`claims.sub`) |
| `$jwt.email` | User email (`claims.email`) |
| `$jwt.name` | User display name (`claims.name`) -- fails if absent |
| `$jwt.custom.<field>` | Custom claim field (string values only) |

Each `$jwt.*` token is replaced with a `?` placeholder before parsing; the resolved claim values are then populated into the parsed AST as literal expressions. Claim values never appear in the query text, so they cannot alter the filter's structure regardless of content. A literal `?` in a filter string has no corresponding claim value and fails closed as a placeholder count mismatch.

Example: with `claims.sub = "user123"`, the filter `"assigned_to = $jwt.sub"` parses as `assigned_to = ?` and is populated with the literal `"user123"`.

**Literal typing:** a claim value that parses as a base64 EntityId is populated as a typed `EntityId` literal rather than a string. Ref-field property values collate as raw EntityId bytes in the reactor's watcher index while string literals collate as text, so an untyped comparison fetches correctly but never matches commit-time lookups — the scoped LiveQuery silently stops receiving live updates ([ankurah#259](https://github.com/ankurah/ankurah/issues/259)). Typing by value shape is a workaround: it guesses wrong (fails closed) for a String field whose value happens to parse as an EntityId. When #259 is fixed at the watcher index, this heuristic should be removed and values populated as plain strings.

### Fail-Closed Defaults

- An empty `PolicyConfig` (no roles, no collections) denies all access.
- Collections not listed in the config are inaccessible to non-privileged contexts.
- Unknown roles grant no privileges.
- Missing claim variables in scope filters cause the query to be denied.

## JwtAgent

`JwtAgent` implements `PolicyAgent` and holds its state behind `Arc<RwLock<AgentState>>`, where `AgentState` combines the `PolicyConfig` and optional `JwtKeys`.

### Construction

**Durable node:**
```rust
let keys = SigningKeys::generate()?;  // or SigningKeys::from_pem(pem)
let agent = JwtAgent::new_durable(keys, "path/to/policy.json")?;
```
Reads and parses the policy file synchronously. Fails fast on missing/invalid file. Stores the path for the filesystem watcher.

**Durable issuer resource server (native with `issuer-http`):**
```rust
let verifier = IssuerVerifier::discover(
    "https://id.example/.well-known/openid-configuration",
    "https://id.example",
    HashSet::from(["resource-client".to_string()]),
    vec!["user".to_string()],
    Duration::from_secs(30 * 60),
).await?;
let agent = JwtAgent::new_durable_issuer(verifier, "path/to/policy.json")?;
```
Discovery validates the supplied discovery URL and expected issuer before network I/O, requires an exact issuer match, rejects redirects and non-success responses, validates the discovered HTTPS JWKS URL, and performs an initial JWKS fetch before returning.

**Ephemeral node:**
```rust
let agent = JwtAgent::new_ephemeral();
```
Starts with deny-all config and no keys. Policy and keys arrive via LiveQuery.

### Key Types

| Type | Description |
|------|-------------|
| `SigningKeys` | Full RSA key pair. Can sign and verify JWTs. |
| `JwtKeys::Signing(SigningKeys)` | Wraps a full key pair. |
| `JwtKeys::VerifyOnly(RS256PublicKey)` | Public key only. Can verify but not sign. |
| `JwtKeys::Issuer(IssuerVerifier)` | Synchronous verifier backed by an atomically replaced JWKS cache. |
| `IssuerTrustDescriptor` | Serializable issuer, JWKS URL, required audiences/token shape, default roles, refresh TTL, and clock tolerance. |

### PolicyAgent Trait Implementation

#### `on_node_ready`

Called after the `Node` is fully constructed.

- **Durable mode:** Spawns a `PolicyWatcher` that monitors the config file for changes using filesystem notifications (`notify` crate). The watcher runs under a `Root` context and publishes exactly one trust source: local public-key PEM or an issuer descriptor.
- **Ephemeral mode:** Creates a weak-node LiveQuery (`EntityLiveQuery::new_weak_node`) on the `jwtpolicy` collection with `NoUser` context. Subscribes to changes and updates config + keys when policy entities arrive.

#### `sign_request`

Serializes each `JwtContext` into `AuthData`:
- `User` -- the raw JWT token bytes
- `NoUser` -- empty bytes
- `Root` -- returns an error (Root cannot be sent over the wire)

#### `check_request`

Deserializes `AuthData` back into `JwtContext`:
- Empty bytes -> `NoUser`
- Non-empty bytes -> verifies the JWT under the configured local-key or issuer trust mode, extracts normalized claims -> `User`

#### `can_access_collection`

- The `jwtpolicy` collection is always accessible (bootstrap carveout).
- `Root` context bypasses all checks.
- Otherwise checks if any of the user's roles have a privilege matching the collection's `read` or `write` requirement.

#### `filter_predicate`

- `Root` context: returns the predicate unchanged.
- Looks up scope rules for the collection. For each rule:
  - If `unless_privilege` is set and the user holds that privilege, the rule is skipped.
  - Otherwise, substitutes `$jwt.*` variables in the filter string and AND-s the resulting predicate with the query.
- No scope rules: returns the predicate unchanged.

#### `check_event` / `check_write`

- `Root` context: always allowed.
- `jwtpolicy` collection: only `Root` can write.
- `NoUser`: all writes denied.
- Otherwise: checks `can_write_collection` against the user's roles and the collection's `write` privilege.

#### `check_read` / `check_read_event`

Delegates to `can_access_collection`.

#### `validate_received_event` / `validate_received_state` / `attest_state` / `validate_causal_assertion`

Currently permissive (return `Ok(())` or `None`). Attestation and cross-node validation are planned for future implementation.

## Policy Sync: Durable to Ephemeral

### Durable Side: PolicyWatcher

`PolicyWatcher` watches the policy JSON file on disk using the `notify` crate (filesystem events, not polling). It watches the parent directory to handle atomic saves (temp file + rename).

On a detected change:
1. Debounce 100ms, drain queued events.
2. Read and parse the file as `PolicyConfig`.
3. On parse error: log warning, keep previous valid config.
4. On success: atomically update the in-memory `AgentState`.
5. Upsert the `JwtPolicy` entity (collection: `jwtpolicy`) with the new config JSON and exactly one trust source: public key PEM or serialized issuer descriptor.

The `JwtPolicy` entity serves as the bridge to ephemeral nodes -- changes propagate through ankurah's normal replication.

### Ephemeral Side: LiveQuery

On `on_node_ready`, the ephemeral agent creates a weak-node LiveQuery via `EntityLiveQuery::new_weak_node` (does not prevent the node from being dropped) on the `jwtpolicy` collection using `NoUser` context.

The `can_access_collection` method has a hardcoded carveout allowing any context to read `jwtpolicy`, enabling the bootstrap flow.

When policy entities arrive or change:
1. Parse `config_json` field as `PolicyConfig`.
2. Parse either `public_key_pem` as an RSA public key or `trust_json` as an `IssuerTrustDescriptor`.
3. Atomically update the `AgentState` (config + trust material) under a single write lock. A valid descriptor makes policy ready but does not cause an ephemeral node to fetch issuer keys.

With public-key PEM, the ephemeral node can verify legacy JWTs and enforce RBAC. With descriptor-only issuer trust, it is policy-ready and can enforce local policy, but intentionally cannot verify issuer tokens because it receives no JWKS.

### JwtPolicy Model

```rust
#[derive(Model)]
pub struct JwtPolicy {
    #[active_type(LWW)]
    pub config_json: String,       // Serialized PolicyConfig JSON
    #[active_type(LWW)]
    pub public_key_pem: String,    // PEM-encoded RSA public key
    #[active_type(LWW)]
    pub trust_json: Option<String>, // Serialized IssuerTrustDescriptor
}
```

Collection name: `jwtpolicy` (auto-derived from struct name). Uses Last-Writer-Wins (LWW) semantics for all fields. The optional descriptor field keeps policy entities written by older versions readable, although adding the public Rust struct field requires downstream struct literals to initialize `trust_json`.

## Security Properties

1. **Fail-closed** -- Empty config denies all access. Unknown collections are inaccessible. Missing variables deny the query.
2. **Root never crosses the wire** -- `JwtContext::Root` cannot be serialized into `AuthData`. It exists only within a local node process.
3. **Write-checked everywhere** -- Write operations are validated regardless of origin (local or remote).
4. **Injection prevention** -- Claim values are populated into the parsed filter AST as literal expressions, never spliced into query text. Metacharacters in claim values (quotes, operators) are inert and cannot alter the filter's structure.
5. **Token expiry** -- JWT expiration is enforced by the `jwt_simple` library during verification.
6. **Atomic config updates** -- Config and keys are updated together under a single write lock, preventing inconsistent state.
7. **Policy collection protected** -- Only `Root` contexts can write to `jwtpolicy`. Non-Root users can only read it.
8. **Issuer transport pinned to HTTPS** -- Discovery and JWKS URLs must be HTTPS without credentials, query, or fragment. Discovery is validated before I/O, redirects are rejected, and the returned issuer must exactly match the configured issuer.
9. **Strict access-token profile** -- Issuer mode requires RS256, `kid`, `typ: at+jwt`, configured issuer/audience, `exp`, `sub`, and `token_use: access`; token roles are never authoritative.
10. **Bounded JWKS refresh** -- Verification never blocks on network I/O. Periodic and unknown-`kid` refreshes share one single-flight gate, unknown-`kid` refreshes have a 30-second post-refresh cooldown, HTTP operations have hard deadlines, and failed/invalid refreshes retain the last-good non-empty key snapshot.

## Crate Structure

```
extensions/jwt-auth/src/
  lib.rs          -- Module declarations and public exports
  agent.rs        -- JwtAgent struct, PolicyAgent implementation
  agent_state.rs  -- AgentState, AgentStateReadGuard, policy sync helpers
  claims.rs       -- JwtClaims struct, unverified token parsing
  config.rs       -- PolicyConfig, CollectionRules, ScopeRule
  context.rs      -- JwtContext enum (User/Root/NoUser)
  error.rs        -- AuthError types
  issuer.rs       -- Issuer descriptor, discovery, strict verification, JWKS cache
  keys.rs         -- SigningKeys, JwtKeys (sign/verify)
  model.rs        -- JwtPolicy ankurah Model (for replication)
  variables.rs    -- $jwt.* variable resolution and substitution
  watcher.rs      -- PolicyWatcher (filesystem notification, feature-gated)
```

The `watcher` module is gated behind the `watcher` Cargo feature (not available on WASM targets). Issuer discovery and background refresh are gated behind `issuer-http` and are also unavailable on WASM targets.
