# JWT Auth Extension Specification

## Overview

The JWT auth extension provides role-based access control (RBAC) for ankurah nodes via RS256 JSON Web Tokens. It implements the `PolicyAgent` trait, intercepting all read, write, and query operations to enforce a declarative policy configuration.

The extension supports two node modes:

- **Durable nodes** hold signing keys and a policy config file on disk. A filesystem watcher detects changes and hot-reloads the policy without restart.
- **Ephemeral nodes** (WASM/mobile clients) start with deny-all defaults and receive the policy and public key from the durable node via a LiveQuery on the `jwtpolicy` collection.

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

Maps each role name to its list of privilege strings. A role with `["*"]`
satisfies every named privilege requirement, but still needs an explicit
collection operation rule. Unconfigured collections and absent operation grants
confer no access. Unconditional row scopes still apply; a scope with
`unless_privilege` is skipped when that named privilege is satisfied.

### `collections`

Type: `Map<String, CollectionRules>`

Each entry defines access rules for one collection:

| Field | Type | Description |
|-------|------|-------------|
| `read` | `Option<String>` | Privilege name required for read access. `None` = no read access. |
| `write` | `Option<String>` | Privilege name required for write access. `None` = no write access. |
| `scope` | `Vec<ScopeRule>` | Row-level filters injected into queries (default: empty). |

### Scope Rules

Scope rules restrict access at the row level: they filter what reads return and constrain what writes may touch. Each rule has:

| Field | Type | Description |
|-------|------|-------------|
| `filter` | `String` | AnkQL predicate with `$jwt.*` variable placeholders |
| `unless_privilege` | `Option<String>` | If the user holds this privilege, skip this filter |
| `applies_to` | `String` | Which operations the rule constrains: `"read_write"` (the default), `"read"`, or `"write"`. A write-only rule gates writes without hiding rows from reads; a read-only rule filters visibility without constraining writes. |

Multiple applicable rules are AND-ed together. If no scope rules are defined for a collection, queries are unfiltered and writes unconstrained (beyond the collection-level access check).

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
- A credential whose scope filter names a claim its token does not carry contributes nothing to reads; a caller with no resolvable, authorized credential is denied.
- A write whose scope filter names a claim the writer's token does not carry is refused; the read-side skip does not apply, because dropping a write filter would fail open.

## JwtAgent

`JwtAgent` implements `PolicyAgent` and holds its state behind `Arc<RwLock<AgentState>>`, where `AgentState` combines the `PolicyConfig` and optional `JwtKeys`.

### Construction

**Durable node:**
```rust
let keys = SigningKeys::generate()?;  // or SigningKeys::from_pem(pem)
let agent = JwtAgent::new_durable(keys, "path/to/policy.json")?;
```
Reads and parses the policy file synchronously. Fails fast on missing/invalid file. Stores the path for the filesystem watcher.

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

### PolicyAgent Trait Implementation

#### `on_node_ready`

Called after the `Node` is fully constructed.

- **Durable mode:** Spawns a `PolicyWatcher` that monitors the config file for changes using filesystem notifications (`notify` crate). The watcher runs under a `Root` context.
- **Ephemeral mode:** Creates a weak-node LiveQuery (`EntityLiveQuery::new_with_weak_node`) on the `jwtpolicy` collection with `NoUser` context. Subscribes to changes and updates config + keys when policy entities arrive.

#### `sign_request`

Serializes each `JwtContext` into `AuthData`:
- `User` -- the raw JWT token bytes
- `NoUser` -- empty bytes
- `Root` -- returns an error (Root cannot be sent over the wire)

#### `check_request`

Deserializes `AuthData` back into `JwtContext`:
- Empty bytes -> `NoUser`
- Non-empty bytes -> verifies JWT signature, extracts claims -> `User`

#### `can_access_collection`

- The `jwtpolicy` collection is always accessible (bootstrap carveout).
- `Root` context bypasses all checks.
- Otherwise checks if any of the user's roles have a privilege matching the collection's `read` or `write` requirement.

#### `query_predicate` / `retrieval_predicate`

Return the entities these credentials may access, independently of the caller's
selection or where core evaluates the predicate. Query grants require a named
`read` or `write` privilege; known-ID retrieval also accepts `retrieve`.

- `Root` returns `True`. Policy-record memberships remain readable for bootstrap.
- Each configured component contributes `MemberOf(id) AND scope`, where scope is
  the union of its authorized credentials' slices. One actual membership granting
  access is sufficient; an unconfigured membership contributes nothing.
- A credential's applicable scope rules are AND-ed after substituting its claims.
  A satisfied `unless_privilege` skips that rule. Missing or invalid claims and
  unresolved restrictions contribute no grant, without invalidating other grants.
- Core's `ReadPolicy` intersects the query predicate with the caller's selection.
  A successful composition does not imply any matching rows or usable grants for
  that selection. Known-ID reads use the retrieval predicate in storage, or
  against a resident entity. Both local and peer Get distinguish missing from
  denied entities; denied state is not returned. A peer denial does not fall
  back to cached state. Ordinary fetches continue filtering unauthorized rows.

#### `check_write_event` / `check_write`

The row-level half of write scoping: `check_write` gates local create/edit against
the current state. After each event is applied, `check_write_event` receives the event,
the transaction's original state, and the resulting entity state; creation has an empty original head.
An existing membership must authorize both states, so a newly added membership
cannot authorize its own addition. JWT adds no event-specific check or attestation;
other agents may do so in the same hook. Any event or state rejection rolls
back the storage transaction.

- `Root` context: always allowed.
- JWT policy entities: only privileged JWT contexts can write.
- Catalog entities: core permits only its internal privileged context to write them; schema-registration permissions are checked before that context is used.
- `NoUser`: all writes denied.
- Otherwise requires `can_write_collection` for the writer's roles, then evaluates the write-applicable scope rules (`applies_to` covering writes, minus any whose `unless_privilege` the writer holds), `$jwt.*`-substituted from the writer's claims, against the entity: every predicate must hold, or the write is refused.
- Asymmetry with the read side, on purpose: a write-scope filter naming a claim the token does not carry refuses the write rather than skipping the credential. On reads a skipped credential merely contributes nothing to the union; on writes skipping would drop the constraint and fail open.

#### `check_read_event`

Core first applies the retrieval predicate to the event's entity. PolicyAgent
may then impose additional event-specific restrictions; JwtAgent adds none.

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
5. Upsert the `JwtPolicy` entity (collection: `jwtpolicy`) with the new config JSON and public key PEM.

The `JwtPolicy` entity serves as the bridge to ephemeral nodes -- changes propagate through ankurah's normal replication.

### Ephemeral Side: LiveQuery

On `on_node_ready`, the ephemeral agent creates a weak-node LiveQuery via `EntityLiveQuery::new_with_weak_node` (does not prevent the node from being dropped) on the `jwtpolicy` collection using `NoUser` context.

The `can_access_collection` method has a hardcoded carveout allowing any context to read `jwtpolicy`, enabling the bootstrap flow.

When policy entities arrive or change:
1. Parse `config_json` field as `PolicyConfig`.
2. Parse `public_key_pem` field as an RSA public key.
3. Atomically update the `AgentState` (config + keys) under a single write lock.

After this point, the ephemeral node can verify JWTs and enforce RBAC.

### JwtPolicy Model

```rust
#[derive(Model)]
pub struct JwtPolicy {
    #[active_type(LWW)]
    pub config_json: String,       // Serialized PolicyConfig JSON
    #[active_type(LWW)]
    pub public_key_pem: String,    // PEM-encoded RSA public key
}
```

Collection name: `jwtpolicy` (auto-derived from struct name). Uses Last-Writer-Wins (LWW) semantics for both fields.

## Security Properties

1. **Fail-closed** -- Empty config denies all access. Unknown collections are inaccessible. Missing variables deny the query.
2. **Root never crosses the wire** -- `JwtContext::Root` cannot be serialized into `AuthData`. It exists only within a local node process.
3. **Write-checked everywhere** -- Write operations are validated regardless of origin (local or remote).
4. **Injection prevention** -- Claim values are populated into the parsed filter AST as literal expressions, never spliced into query text. Metacharacters in claim values (quotes, operators) are inert and cannot alter the filter's structure.
5. **Token expiry** -- JWT expiration is enforced by the `jwt_simple` library during verification.
6. **Atomic config updates** -- Config and keys are updated together under a single write lock, preventing inconsistent state.
7. **Policy collection protected** -- Only `Root` contexts can write to `jwtpolicy`. Non-Root users can only read it.

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
  keys.rs         -- SigningKeys, JwtKeys (sign/verify)
  model.rs        -- JwtPolicy ankurah Model (for replication)
  variables.rs    -- $jwt.* variable resolution and substitution
  watcher.rs      -- PolicyWatcher (filesystem notification, feature-gated)
```

The `watcher` module is gated behind the `watcher` Cargo feature (not available on WASM targets).
