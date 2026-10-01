# JWT Auth Extension Specification

## Overview

The JWT auth extension provides role-based access control (RBAC) for ankurah nodes via RS256 JSON Web Tokens. It implements the `PolicyAgent` trait (`core/src/policy.rs`), which core consults for requests between nodes, reads, writes, and schema registrations.

The policy is stored in the system as entities of nine policy models (labels prefixed `jwtagent_`) and replicates like any other data. Every node, durable or ephemeral, loads it through one livequery per policy model run as `NoUser`, so all nodes enforce the same stored policy. Enforcement reads only those stored records; it never reads a file or the catalog.

- **Durable nodes** hold the RSA signing key pair and are the only place policy is authored. `JwtAgent::set_policy` (or `set_policy_from_file`) turns a `PolicyConfig` into stored records and publishes the agent's public key as the system's verification key. The agent does not watch the policy file; an application may start `PolicyWatcher` to do so.
- **Ephemeral nodes** (WASM/mobile clients) start with no keys and a deny-all config. They become ready once the stored policy and the verification key have arrived from a durable peer.

## Permission Model

```
User JWT → Role(s) → Privilege(s) → Collection Access Rules
                                        ↓
                                  read / write + scope filters
```

Three layers:

1. **Roles** -- Assigned to users in their JWT `roles` claim (e.g. `"Admin"`, `"Dispatcher"`).
2. **Privileges** -- Named capabilities granted by roles (e.g. `"manage_jobs"`, `"view_users"`). The wildcard `"*"` grants all privileges.
3. **Collection rules** -- Per-collection mappings that specify which privilege is required for `read`, `retrieve`, and `write` access, plus optional row-level scope filters.

## Context Types

`JwtContext` is the `ContextData` type used by `JwtAgent`:

| Variant | Description | Wire serialization |
|---------|-------------|-------------------|
| `User { claims, token }` | Authenticated user. Claims extracted from a verified JWT. | Raw JWT bytes as `AuthData` |
| `Root` | Privileged system context, returned by `JwtContext::system()`. Bypasses every policy check. | Cannot be serialized; local-only |
| `NoUser` | Unauthenticated. May read policy records and, through core's catalog exemption, catalog rows. Nothing else: no writes and no catalog changes. | Empty `AuthData` |

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

`PolicyConfig` is the authoring input: a JSON document with two top-level fields, `roles` and `collections`. `set_policy` converts it into stored policy records (see Stored Policy below). After the stored policy loads, `JwtAgent::config()` returns a `PolicyConfig` rebuilt from those records.

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
      "read": "manage_users",
      "retrieve": "view_users",
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

Each entry defines access rules for one collection, keyed by the collection's model label:

| Field | Type | Description |
|-------|------|-------------|
| `read` | `Option<String>` | Privilege that admits scans of this collection: queries, subscriptions, and fetches. Holders of `write` may also scan. |
| `retrieve` | `Option<String>` | Privilege that admits reading rows the caller already names (`get`, a `Ref` follow, the by-id wire path) without admitting scans; every predicate query is a scan, even an id-shaped one. Holders of `read` or `write` may also retrieve. Absent: retrieval requires `read` or `write`. |
| `write` | `Option<String>` | Privilege required to create or edit entities in this collection. Absent: no writes. |
| `scope` | `Vec<ScopeRule>` | Row-level filters applied to reads and writes (default: empty). |

An absent privilege grants nothing, and a privilege name that no role grants is unreachable.

### Scope Rules

Scope rules restrict access at the row level: they filter what reads return and constrain what writes may touch. Each rule has:

| Field | Type | Description |
|-------|------|-------------|
| `filter` | `String` | AnkQL predicate with `$jwt.*` variable placeholders |
| `unless_privilege` | `Option<String>` | If the user holds this privilege, skip this filter |
| `applies_to` | `String` | Which operations the rule constrains: `"read_write"` (the default), `"read"`, or `"write"`. A write-only rule gates writes without hiding rows from reads; a read-only rule filters visibility without constraining writes. |

Multiple applicable rules are AND-ed together. If no scope rules are defined for a collection, queries are unfiltered and writes unconstrained (beyond the collection-level access check).

### Variable Substitution

Scope filters name claim values with `$jwt.*` variables, so one rule serves every user. The values are never part of the query text: `parse_template` (`variables.rs`) replaces each variable with a `?` placeholder, parses the result with the AnkQL parser, and keeps the variable names in placeholder order. A literal `?` in a filter has no variable behind it and fails as a placeholder count mismatch. Parsing happens when the policy is set. The stored `ResolvedPolicyScope` keeps the resolved predicate with its placeholders, and one `ClaimParameter` per placeholder records the variable name and, when the placeholder is compared with a property, that property's value type.

At evaluation, `BoundPredicate::populate` resolves each parameter from the credential's claims:

| Variable | Resolves to |
|----------|------------|
| `$jwt.sub` | User entity ID (`claims.sub`) |
| `$jwt.email` | User email (`claims.email`) |
| `$jwt.name` | User display name (`claims.name`); fails if absent |
| `$jwt.custom.<field>` | Custom claim field (string values only) |

A missing claim, a non-string custom claim, or an unknown variable fails that credential's evaluation. Each resolved value becomes a literal expression (`typed_expr`): a value that parses as a base64 `EntityId` becomes an `EntityId` literal, anything else a string. When the parameter records a property type, the literal is then cast to that type (`Value::cast_to`); a value that cannot be cast fails the evaluation (`claim value does not match policy property type`).

Example: with `claims.sub = "user123"`, the filter `"assigned_to = $jwt.sub"` is stored as `assigned_to = ?` with the parameter `$jwt.sub`, and evaluates with the literal `"user123"`.

**Literal typing:** `typed_expr` types by value shape because ref-field property values collate as raw `EntityId` bytes in the reactor's watcher index while string literals collate as text; an untyped comparison fetches correctly but never matches commit-time lookups, so a scoped livequery silently stops receiving live updates ([ankurah#259](https://github.com/ankurah/ankurah/issues/259)). The shape guess is wrong for a string property whose value happens to parse as an `EntityId`: when that property's type is recorded on the parameter, the cast converts the literal back to its base64 string form; when it is not, the comparison fails closed. When ankurah#259 is fixed at the watcher index, `typed_expr` can return plain string literals unconditionally.

### Fail-Closed Defaults

- An empty `PolicyConfig` (no roles, no collections) denies all access.
- Collections not listed in the config, and listed collections whose label has not been bound to a model, are inaccessible to non-privileged contexts.
- Unknown roles grant no privileges.
- A scope rule whose binding has not arrived, or a model policy whose scope rows have not all arrived, denies scoped access instead of granting it unscoped.
- A credential whose scope filter names a claim its token does not carry contributes nothing to reads; a caller with no resolvable, authorized credential gets no rows.
- A write whose scope filter names a claim the writer's token does not carry is refused; the read-side skip does not apply, because dropping a write filter would fail open.
- Until the stored policy has loaded, only policy records are readable and every non-privileged write is refused.
- A node without keys refuses every incoming request that carries credentials, including anonymous (empty) ones. Catalog reads bypass policy in core and are unaffected.

## JwtAgent

`JwtAgent` implements `PolicyAgent`. Its state is an `AgentState` in a `Mut` signal (`ankurah::signals::Mut`): the current `PolicyConfig`, the optional `JwtKeys`, the bound policy built from the stored records (`BoundPolicy`, crate-private), and the IDs of the policy models. Clones share the state, an authoring lock, and the handle of the background policy sync.

### Construction

**Durable node, first run:**
```rust
let keys = SigningKeys::generate()?;  // or SigningKeys::from_pem(pem)
let agent = JwtAgent::new_durable(keys, "path/to/policy.json")?;
let node = Node::new_durable(storage, agent.clone());
node.system.create().await?;
agent.set_policy(&node, &agent.config()).await?;  // or agent.set_policy_from_file(&node, path)
node.wait_ready().await?;
```

`new_durable` reads and parses the policy file once, synchronously, and fails on a missing or invalid file. The parsed config is held in memory for `set_policy`; nothing enforces it and nothing watches the file.

`set_policy(&node, &config)` requires a durable node and keys. It registers the nine policy models if they are missing (only policy installation registers them), records their IDs, and writes the stored policy under a weak `Root` context, using the agent's public key as the verification key. It does not wait for node readiness; on a fresh durable node the caller must not wait either, because `start` completes only after a stored policy exists. `set_policy_from_file(&node, path)` reads a JSON file with `tokio::fs`, parses it, and calls `set_policy` once; it is not compiled for `wasm32`. When replacing an existing stored policy, pass the new config explicitly or use `set_policy_from_file`: `config()` reflects the stored policy once it has loaded.

**Durable node, restart:** construct the agent the same way, or with `new_ephemeral`, and skip `set_policy`; `start` loads the stored policy and key from local storage. A policy file that differs from the stored policy changes nothing until `set_policy` is called.

**Ephemeral node:**
```rust
let agent = JwtAgent::new_ephemeral();
let node = Node::new(storage, agent.clone());
```
`new_ephemeral` gives an agent with no keys and a deny-all config. Once the node is connected to a durable peer, `start` completes when the stored policy and verification key have arrived; `node.wait_ready()` and `context_async` wait for it.

### Runtime Methods

| Method | Behavior |
|--------|----------|
| `config()` | The current `PolicyConfig`: the file's (or the deny-all default) until the stored policy loads, then the one rebuilt from the stored records. |
| `update_config(config)` | Replaces the in-memory config for a later `set_policy`. Active permissions are unchanged, and the next successful refresh replaces it again with the stored policy's config. |
| `set_keys(keys)` | Replaces the keys. The next refresh keeps a signing pair only if its public PEM matches the stored verification key. |
| `signing_keys()` | The signing pair, if the agent holds one. |
| `state_handle()` | A `Read<AgentState>` for observing config and keys. |
| `policy_ready()` | Whether a config has been loaded and keys are present. It does not say whether the stored policy has loaded; `Node::wait_ready` does. |
| `can_access_model(credentials, model)` | Helper, not a trait method: `Ok` for a privileged credential or a policy model; otherwise `Ok` when any credential holds `read`, `write`, or `retrieve` for the model's rules, else `ModelDenied` (or `policy has not loaded` before the stored policy loads). |
| `set_catalog(catalog)` | Feature `test-helpers`: binds the in-memory config against a `PolicyCatalog` fixture, without persistence or refresh. |

### Key Types

| Type | Description |
|------|-------------|
| `SigningKeys` | Full RSA key pair. Can sign and verify JWTs. |
| `JwtKeys::Signing(SigningKeys)` | Wraps a full key pair. |
| `JwtKeys::VerifyOnly(RS256PublicKey)` | Public key only. Can verify but not sign. |

`JwtKeys::from_public_pem` builds a verify-only key from a PEM, `JwtKeys::public_key_pem` exports the public key of either variant, and `SigningKeys::sign(claims, duration)` issues tokens.

### PolicyAgent Trait Implementation

#### `start`

Core calls `start` after the system and catalog have loaded, and the node does not report ready (`Node::wait_ready`, `context_async`) until it returns; an error halts the node. `JwtAgent::start` spawns `start_policy_sync` (see Loading under Stored Policy) and returns once the first policy load completes. The livequeries live as long as the agent; dropping the last `JwtAgent` clone cancels the task.

#### `preflight`

Core calls `preflight(node, model)` from `ensure_registered` for every non-privileged context before a model is used, so that synchronous access checks see current policy bindings. `JwtAgent` fetches every policy record (`PolicyGraph::fetch` as `NoUser` with `CachePolicy::Tracked`, which waits for a durable answer) when the policy models are known, the model is not itself a policy model, and the bound policy holds no fully bound rules for it (no rules, or a scope whose binding has not arrived). Applying the fetched records refreshes the policy livequeries before the access check runs. It resolves no names and grants nothing.

#### `sign_request`

Serializes each `JwtContext` into `AuthData`:
- `User` -- the raw JWT token bytes
- `NoUser` -- empty bytes
- `Root` -- returns an error (Root cannot be sent over the wire); one unsignable member fails the whole request ([ankurah#432](https://github.com/ankurah/ankurah/issues/432) records the skip-versus-fail question)

#### `check_request`

Requires keys; without them every request fails with `No keys configured for JWT verification`. Then deserializes each `AuthData` into a `JwtContext`:
- Empty bytes -> `NoUser`
- Non-empty bytes -> verifies the JWT signature with the current keys and extracts claims -> `User`

#### `check_schema_registration`

Core calls it with the registering credential and the complete plan, only for plans that change the catalog and only when a credential (not core's privileged context) registers. `JwtAgent`:
- allows `Root`;
- refuses `NoUser` (`Anonymous contexts cannot change the catalog`);
- refuses any other credential whose plan touches the policy schema (`Only privileged contexts may change JWT policy schema`): creating a model whose label starts with `jwtagent_`, creating a property minted for a policy model, creating a membership on a policy model, or updating a catalog row that belongs to one (a model-property row whose membership belongs to a policy model, or whose membership cannot be found; a policy model's own row; a property minted for a policy model);
- allows everything else.

#### `schema_registered`

Core calls it inside the registration transaction, after `check_schema_registration` passes and before commit, and holds the returned guard through the commit. `JwtAgent` fills the policy's pending bindings for the models and properties the plan creates; see Binding at schema registration under Stored Policy.

#### `query_predicate` / `retrieval_predicate`

Return the entities these credentials may access, independently of the caller's
selection or where core evaluates the predicate. Query grants require a named
`read` or `write` privilege; known-ID retrieval also accepts `retrieve`.

- `Root` returns `True`. Policy-record memberships remain readable for bootstrap.
- Until the stored policy has loaded, the predicate is the union of the policy
  models' memberships; if their IDs are not yet known, the call fails with
  `policy has not loaded`.
- Each configured component contributes `MemberOf(id) AND scope`, where scope is
  the union of its authorized credentials' slices. One actual membership granting
  access is sufficient; an unconfigured membership contributes nothing.
- A credential's applicable scope rules are AND-ed after substituting its claims.
  A satisfied `unless_privilege` skips that rule. Missing or invalid claims and
  unresolved restrictions contribute no grant, without invalidating other grants.
- Core's `ContextPolicy` (`core/src/policy/context.rs`) intersects the query
  predicate with the caller's selection. A successful composition does not imply
  any matching rows or usable grants for that selection. Known-ID reads use the
  retrieval predicate in storage, or against a resident entity. Both local and
  peer Get distinguish missing from denied entities; denied state is not
  returned. A peer denial does not fall back to cached state. Ordinary fetches
  continue filtering unauthorized rows.

#### `check_write_event` / `check_write`

The row-level half of write scoping: `check_write` gates a local create or edit against
the current state (core calls it from `Transaction::create`, `get`, and `edit`). After each
event is applied, `check_write_event` receives the event, the transaction's original state,
and the resulting entity state; creation has an empty original head. Core calls it in local
commits and in transactions received from peers. An existing membership must authorize both
states, so a newly added membership cannot authorize its own addition. JWT adds no
event-specific check or attestation; other agents may do so in the same hook. Any event or
state rejection rolls back the storage transaction.

- `Root` context: always allowed.
- Until the stored policy has loaded: refused (`policy has not loaded`).
- `NoUser`: all writes refused.
- An entity with a policy-model membership: only `Root` may write it.
- Catalog entities: core permits only its internal privileged context to write them; schema-registration permissions are checked before that context is used.
- Otherwise `BoundPolicy::check_write` walks the entity's memberships, skipping any the original state lacked. For each, the model's stored rules must grant the writer the `write` privilege, and every write-applicable scope rule (`applies_to` covering writes, minus any whose `unless_privilege` the writer holds), populated from the writer's claims, must evaluate true against the original state (when there is one) and the resulting state. The first membership that passes allows the write; otherwise the last denial is returned.
- Asymmetry with the read side, on purpose: a write-scope filter naming a claim the token does not carry refuses the write rather than skipping the credential. On reads a skipped credential merely contributes nothing to the union; on writes skipping would drop the constraint and fail open.

#### `check_reads` / `check_read_event`

Core first applies the retrieval predicate to the entity, or to the event's entity.
`PolicyAgent` may then impose additional restrictions in `check_reads` and
`check_read_event`; `JwtAgent` keeps the trait defaults, which add none.

#### `validate_received_event` / `validate_received_state` / `attest_state` / `validate_causal_assertion`

Currently permissive (return `Ok(())` or `None`).

## Stored Policy

Policy lives in the system as entities so that every node enforces the same rules and a policy change replicates like any other write. Authoring converts a `PolicyConfig` into records; loading rebuilds an evaluator from the records that have arrived; enforcement uses only that evaluator.

### Policy models (`model.rs`)

All nine models carry labels prefixed `jwtagent_` and are `no_ffi`.

| Model | Label | Fields |
|-------|-------|--------|
| `Role` | `jwtagent_role` | `name` |
| `Privilege` | `jwtagent_privilege` | `name` |
| `RolePrivilege` | `jwtagent_role_privilege` | `role: Ref<Role>`, `privilege: Ref<Privilege>`, `status` |
| `ModelPolicy` | `jwtagent_model_policy` | `label`, `model: Binding<ModelId>`, `read`, `retrieve`, `write` (each `Option<Ref<Privilege>>`), `scope_count`, `status` |
| `PolicyProperty` | `jwtagent_policy_property` | `policy: Ref<ModelPolicy>`, `label`, `property: Binding<PropertyId>`, `value_type: Option<String>` |
| `PolicyScope` | `jwtagent_policy_scope` | `policy: Ref<ModelPolicy>`, `filter`, `unless_privilege: Option<Ref<Privilege>>`, `applies_to: ScopeRuleOp`, `resolved: Option<Ref<ResolvedPolicyScope>>`, `status` |
| `ResolvedPolicyScope` | `jwtagent_resolved_policy_scope` | `predicate: ScopePredicate` |
| `ClaimParameter` | `jwtagent_claim_parameter` | `scope: Ref<ResolvedPolicyScope>`, `position: i32`, `variable`, `value_type: Option<String>` |
| `JwtVerificationKey` | `jwtagent_verification_key` | `public_key_pem` |

`RuleStatus` (`Active` or `Retired`) says whether a rule is configured, independently of whether its bindings are ready; the scopes of a retired `ModelPolicy` are ignored. `Binding<T>` records how a label was bound to a durable identity: `Pending` (no model or property with that label existed), `AtPolicySet(id)` (bound when the policy was set), or `AtRegistration(id)` (bound by the first matching schema registration, kept so a policy review can see it). `ScopePredicate` stores a `Predicate<Resolved>` whose placeholders stand for claim parameters. `ModelPolicy::scope_count` and `PolicyScope::resolved` exist so a partially arrived policy cannot grant unscoped access: a rule is unusable until that many active scope rows have arrived, and `resolved = None` means not yet bound, never an absent restriction.

`PolicyGraph` (`graph.rs`) is the in-memory snapshot of every record of the nine models, keyed by entity ID; `PolicyGraph::fetch` reads all nine through a context. `PolicyQueries` holds one livequery per model, combines their current results with `snapshot()`, and forwards their changes with `subscribe()`. `bind_models` binds all nine from the local catalog without registering; `register_models` registers any that are missing.

### Authoring (`authoring.rs`)

`set_policy(context, catalog, config, pem)` replaces the stored policy in one transaction:

1. Fetch the current graph with a durable answer.
2. Retire every active `RolePrivilege`, `ModelPolicy`, and `PolicyScope`. `Role` and `Privilege` records keep their identities.
3. Create any `Privilege` the config names (in role grants, `read`, `retrieve`, `write`, or `unless_privilege`) that does not exist yet, and any missing `Role`; create one active `RolePrivilege` per role and distinct granted privilege.
4. For each collection, bind its label to a model: a system model by label, else the one catalog model carrying that label (an ambiguous label fails the call), else `Pending`. Create the active `ModelPolicy` with its privilege references and `scope_count`. For each scope rule, parse the filter template (an unparseable filter fails the call), collect the property names it references (other than `id`), and create an active `PolicyScope` with `resolved = None`; create one `Pending` `PolicyProperty` per property name.
5. Bind what can be bound now (`bind_properties_and_scopes`): a `PolicyProperty` binds when its policy's model is bound and the catalog resolves the label, recording the property ID as `AtPolicySet` and its value type; a `PolicyScope` binds once all of its policy's properties are bound: `BoundPredicate::bind` resolves the filter against the stored property identities only (`StoredBindings`), and the result is written as a `ResolvedPolicyScope` plus one `ClaimParameter` per placeholder, referenced from `resolved`.
6. Require at most one `JwtVerificationKey`; set its PEM, or create it.
7. Commit. A failure at any step leaves the stored policy unchanged.

`PolicyCatalog` (`catalog.rs`) is the catalog lookup used while authoring (`model_labels`, `property`, `property_type`, `resolve_predicate`), implemented for `CatalogManager`. Enforcement never consults a catalog.

### Binding at schema registration

A policy may name a collection or property before its model is registered. `schema_registered` fills those bindings inside the registration transaction, so the rules are enforceable from the moment the schema exists:

- Core calls the hook on the registering durable node after `check_schema_registration` passes and before the transaction commits; replicas apply the committed records and do not run it.
- `JwtAgent` returns early when the system epoch is not available or the nine policy models do not bind locally (no policy installed yet). Otherwise it takes the authoring lock and calls `authoring::schema_registered` under a weak `Root` context.
- For each active `ModelPolicy` whose `model` is `Pending` and whose label matches a model the plan creates, it sets `Binding::AtRegistration(model)` and logs a warning, because the first registration with that label wins.
- It then binds pending properties and scopes exactly as `set_policy` does, resolving names against the plan's new rows overlaid on the committed catalog (`RegistrationCatalog`), and records those bindings as `AtRegistration`.
- A scope that cannot be bound against the new schema aborts the registration transaction; the catalog stays unchanged.
- The hook returns the authoring lock's guard and the executor holds it through commit, so `set_policy` cannot run between the binding and the commit and leave the new rules pending. `set_policy` takes the same lock after registering the policy models.

Bindings persist: a restart does not rebind, and setting a new policy creates new `ModelPolicy` rows that bind afresh.

### Loading (`agent_state.rs`)

`start_policy_sync` runs once per agent, spawned by `start`:

1. Wait until all nine policy models bind from the local catalog (`bind_models`), watching catalog changes while waiting. On a fresh durable node this completes only after `set_policy` registers the models, so installing the first policy is a bootstrap step that runs before the node is ready.
2. Record the policy model IDs in `AgentState::policy_models`.
3. Open one livequery per policy model (`PolicyQueries`), selection `true`, under a weak `NoUser` context, and wait for a durable answer from each. A durable node answers from its own storage; an ephemeral node waits for a durable peer.
4. Subscribe the refresh to all nine queries, run it once, and wait until the state holds a config, keys, and a bound policy. `start` then returns.

The refresh installs a consistent policy from the current query results, or none:

1. Snapshot all nine queries into a `PolicyGraph`.
2. Build a `BoundPolicy` from it (`BoundPolicy::from_graph`).
3. Require exactly one `JwtVerificationKey` record and parse its PEM as a verify-only key.
4. On success, in one state update: keep the local keys if they are a signing pair whose public PEM equals the stored PEM, otherwise install the verify-only key; set `config` to the config rebuilt from the records; install the bound policy; mark the config loaded.
5. On any failure (records still arriving, a missing privilege record, two active policies for one model, zero or several key records): clear the bound policy and the loaded flag, leave keys and config as they were, and log at debug level.

Refreshes run one at a time behind a lock that is released before listeners are notified, so a slower refresh cannot replace a newer policy.

### Enforcement (`bound_policy.rs`)

`BoundPolicy::from_graph` builds the synchronous evaluator from a graph snapshot:

- It rebuilds the `PolicyConfig` from active grants and active model policies, so `JwtAgent::config()` reflects the stored policy; a referenced privilege record that has not arrived fails the build.
- For each active `ModelPolicy` with a bound model it keeps the `read`, `retrieve`, and `write` privilege names and its scopes. If the number of active scope rows differs from `scope_count`, one unevaluable scope stands in for them (`policy scopes have not all arrived`). Otherwise each scope loads its stored predicate from `resolved`, the `ResolvedPolicyScope`, and its `ClaimParameter` rows at contiguous positions; anything missing makes that scope unevaluable (`policy scope binding has not arrived`). An unevaluable scope denies every read and write it applies to.
- A `ModelPolicy` whose model is still `Pending` installs no rules, so its collection is denied like an unconfigured one.
- Two active policies for one model fail the build.

Evaluation for one credential (`ModelRules`): `read` or `write` admits scans (queries and subscriptions), `retrieve` additionally admits known-ID reads, and `write` admits writes; a wildcard role satisfies any named privilege. The applicable scope rules for an operation are those whose `applies_to` covers it, minus any whose `unless_privilege` the credential holds; each is populated from the credential's claims, which requires a `User` context.

`read_predicate(credentials, operation)` returns the union of the policy models' memberships (always readable) and, for each bound model, `MemberOf(model) AND scope`, where scope is the union over admitted credentials of their AND-ed scope predicates; a credential with no applicable rules admits every row of the model, and a credential whose rules cannot be populated contributes nothing. `check_write` is described under `check_write_event` / `check_write`. `can_access_model` answers the collection-level question alone.

### Verification key

One `JwtVerificationKey` record carries the public PEM every node uses to verify tokens; private signing material never leaves the node that holds it. `set_policy` writes the calling agent's public key into that record. The refresh requires exactly one key record and installs it verify-only, except on a node whose local signing pair has the same public key, which keeps its pair. A durable node started with a signing key that does not match the stored key therefore verifies tokens with the stored key, logs a warning when its signing pair is replaced, and its `signing_keys()` returns `None`; tokens signed with its key fail verification until `set_policy` publishes it.

### PolicyWatcher (`watcher.rs`, feature `watcher`)

`PolicyWatcher::start(path, &node, agent)` spawns a tokio task that watches the file's parent directory with the `notify` crate, so atomic saves (temp file plus rename) are seen. After any event it waits 100 ms, drains queued events, and calls `agent.set_policy_from_file(&node, &path)`, logging success at info and failure at warn; a file that fails to parse leaves the stored policy untouched. `stop()` or dropping the watcher aborts the task. It does not install the initial policy, and nothing in the repository starts it; the agent never does.

## Security Properties

1. **Fail-closed** -- An empty config denies all access. Unconfigured, unbound, and partially arrived rules deny rather than grant. A missing claim removes a credential's read grant and refuses its write. Until the stored policy loads, only policy records are readable.
2. **Root never crosses the wire** -- `JwtContext::Root` cannot be serialized into `AuthData`. It exists only within a local node process.
3. **Write-checked everywhere** -- `check_write` runs before every local create or edit, and `check_write_event` runs on every applied event, in local commits and in transactions received from peers.
4. **Injection prevention** -- Claim values are populated into the stored predicate's placeholders as literal expressions, never spliced into query text. Metacharacters in claim values (quotes, operators) are inert and cannot alter the filter's structure.
5. **Token expiry** -- JWT expiration is enforced by the `jwt_simple` library during verification.
6. **Atomic policy updates** -- One refresh installs the bound policy, its config, and the verification key in a single state update, and refreshes are serialized so an older snapshot cannot replace a newer one.
7. **Policy records protected** -- Non-privileged contexts cannot write any policy record (`check_write`) or change the policy models' schema (`check_schema_registration`), and `NoUser` cannot change the catalog at all. Every context can read the policy records, so a node can load policy before it holds any credential.

## Crate Structure

```
extensions/jwt-auth/src/
  lib.rs              module declarations and public exports
  agent.rs            JwtAgent: construction, set_policy, and the PolicyAgent implementation
  agent_state.rs      AgentState and start_policy_sync, the livequery-driven load and refresh
  authoring.rs        set_policy and schema_registered: writing policy records and binding them to schema
  bound_policy.rs     BoundPolicy: the synchronous evaluator built from stored records
  bound_predicate.rs  BoundPredicate: a resolved scope predicate with typed claim parameters
  catalog.rs          PolicyCatalog: catalog lookups used only while authoring
  claims.rs           JwtClaims and parse_claims_unverified
  config.rs           PolicyConfig, CollectionRules, ScopeRule, ScopeRuleOp
  context.rs          JwtContext (User, Root, NoUser)
  error.rs            AuthError
  graph.rs            PolicyGraph, PolicyQueries, bind_models, register_models
  keys.rs             SigningKeys and JwtKeys
  model.rs            the nine policy models, RuleStatus, Binding, ScopePredicate
  variables.rs        $jwt.* template parsing and claim resolution
  watcher.rs          PolicyWatcher (feature `watcher`)
```

Cargo features: `watcher` adds the `notify` dependency and the `watcher` module; `test-helpers` exposes `JwtAgent::set_catalog`; `uniffi` sets up UniFFI scaffolding. `set_policy_from_file` is not compiled for `wasm32`.
