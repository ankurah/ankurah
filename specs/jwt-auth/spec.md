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
- **Ephemeral mode:** Creates a weak-node LiveQuery (`EntityLiveQuery::new_weak_node`) on the `jwtpolicy` collection with `NoUser` context. Subscribes to changes and updates config + keys when policy entities arrive.

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

#### `filter_predicate`

Narrows a query so storage returns only rows the caller may read. A caller may present several credentials (its context set) and may read any row that any one of them admits, so the query narrows to the union of per-credential scope slices -- the same any-of admission `check_read` applies row by row.

- `Root` anywhere in the context set: returns the predicate unchanged.
- No scope rules for the collection: returns the predicate unchanged. This check runs first, so an unscoped collection requires no authorized credential.
- Otherwise walks every credential in the context set. A credential contributes nothing if it is `NoUser`, if its roles cannot access the collection, or if its scope variables cannot resolve (its filter names a claim the token does not carry -- skipped with a `tracing::warn`).
- Each surviving credential contributes one slice: its read-applicable scope rules (`applies_to` covering reads), minus any whose `unless_privilege` it holds, `$jwt.*`-substituted from its own claims and AND-ed together. Equal slices deduplicate. A credential no rule constrains may read every row the caller asked for, so the union collapses: the caller's predicate is returned unchanged.
- The query becomes `P AND (s1 OR s2 OR ...)` -- the caller's predicate stays factored in front of the union, where a storage planner reads indexable terms off the top-level conjunction.
- No credential contributed a slice (none authorized, or none resolvable): the query is refused with `AccessDenied`.

#### `check_event` / `check_write`

The row-level half of write scoping: a non-privileged writer may only touch rows inside its write scope. `check_write` gates a local write against the entity's current state; `check_event` gates an applied event against both the entity as it stood before (when it has history) and as the event leaves it, so an update can neither start from nor produce a row outside the writer's scope.

- `Root` context: always allowed.
- `jwtpolicy` collection: only `Root` can write.
- Catalog collections (`check_event` only): `NoUser` is refused; any authenticated caller passes, because core admits catalog writes only through the schema-registration executor, whose policy check already ran.
- `NoUser`: all writes denied.
- Otherwise requires `can_write_collection` for the writer's roles, then evaluates the write-applicable scope rules (`applies_to` covering writes, minus any whose `unless_privilege` the writer holds), `$jwt.*`-substituted from the writer's claims, against the entity: every predicate must hold, or the write is refused.
- Asymmetry with the read side, on purpose: a write-scope filter naming a claim the token does not carry refuses the write rather than skipping the credential. On reads a skipped credential merely contributes nothing to the union; on writes skipping would drop the constraint and fail open.

#### `check_read`

The row-level half of read scoping: admits or refuses one entity by its serialized state, where `filter_predicate` narrows the query up front. `check_read` and `check_read_event` run the same row-level check through one shared helper, so a caller refused an entity's state is also refused that entity's events (events replay the same content):

- Requires `can_access_collection`. `Root` passes unconditionally.
- No scope rules for the collection: allowed.
- Otherwise materializes the state into a `TemporaryEntity` (a state that cannot be evaluated is refused) and walks the caller's credentials: a credential admits the row when every one of its read-applicable, substituted scope predicates evaluates true, and the first admitting credential allows the read. Unauthorized and unresolvable credentials are skipped exactly as in `filter_predicate` (logged at `debug` -- the query half already warned once per query), so credential order cannot change the answer. A credential no rule constrains admits every row. A scope predicate that fails to evaluate against the row refuses the read; so does running out of credentials.
- `check_read_event` runs the same rules against the entity as it *currently* stands, fetched through a getter the serving node hands in. The getter is bound to the event's own entity at construction -- there is no way to hand the check another row -- and the verdict fetches only when it must: a privileged caller or an unscoped collection decides before any fetch happens. When nothing current exists for an event of a scoped collection, the event is refused (there is nothing to evaluate); an unscoped collection is unaffected. A fetch failure is surfaced as an error, not a refusal.

Accepted residual: the verdict is about the row as it stands now, so a caller who can read a row today reads that row's whole history, including the part written while the row sat outside its scope. Per-event historical evaluation is tracked in [issue #445](https://github.com/ankurah/ankurah/issues/445).

Engine-dependent limitation: on the IndexedDB engine events are stored without collection identity, so an event can be relabelled into a collection the caller may read and be judged by that collection's rules rather than its own ([issue #444](https://github.com/ankurah/ankurah/issues/444)).

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

On `on_node_ready`, the ephemeral agent creates a weak-node LiveQuery via `EntityLiveQuery::new_weak_node` (does not prevent the node from being dropped) on the `jwtpolicy` collection using `NoUser` context.

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
