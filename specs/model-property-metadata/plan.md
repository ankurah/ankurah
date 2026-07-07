# Implementation plan: model and property metadata

Authority: rfc.md in this directory (RATIFIED rev 3, commit ff40a3bf;
AMENDED to rev 4 on 2026-07-06 -- the identity plane pivoted from
deterministic derivation to durable-allocated ids; ratification pending
on #289). This plan translates the RFC into a build order; it does not
re-argue design. Where the RFC delegated a decision to implementation,
the choice is made here and collected in "Decisions made in this plan"
at the bottom for async review. Rev 4 reworks A1/A2/A3/A5/A6 and amends
decisions 8, 16, and 18 (marked in place); decisions 20-24 are the
pivot's additions. Code citations verified against this branch (based on
main 05593d0d = 0.9.0).

Scope: Phase 0 (#294) and Phase A in full; Phase C outlined (engine-local,
no wire gate; detailed tasks written when A stabilizes). Phase D excluded
(tracked: #301 DDL, #302 binding codegen, #303 schema evolution).
Nomenclature per #305 applies to all new code and docs: Model = a named
data contract (a definition entity); Collection = a storage table name.

## Ladder and dependency DAG

```
Phase 0 (#294 protocol version)            <- gates every wire change
  |
Phase A, one protocol epoch (proto v1 -> v2), build order (rev 4):
  A1 lookup keys + allocation ------------------->  A5 upsert executor
                                                       + response
  A4 catalog collections + protection ----------->  A5, A6 receiver guard
  A5, A7 catalog subscription/map + response feed -> A8 LWW v2 fallback,
                                                    A9 resolution,
                                                    A11 lifecycle/macro
  A8 LWW v2 (testable early against a resolver trait)
  A9 ankql Identifier + resolution pass  ->  A10 read rules (+ #175 fix)
  A11 derive macro attributes + registration triggers
  A12 errors/guards/audits (cross-cutting, lands with its consumers)
  |
Phase C (sled rekey, SQL column binding + rename DDL, IndexedDB)

(Rev 4 deletions: A2 frozen genesis encoder is gone outright; A3
create-with-derived-id reduces to feeding the existing known-id create
path freshly allocated ids inside the executor.)
```

Phase A work packages are ordered so that the pure, heavily-testable core
(A1, A2, A3, A8 wire shapes) lands first as red-to-green commits, then the
catalog and the protocol operation, then resolution and read semantics,
then the macro surface.

## Phase 0: protocol version in the Presence handshake (#294)

Today `Presence { node_id, durable, system_root }` carries no version
(proto/src/peering.rs:5-10) and is the first `Message` variant exchanged
by every connector (proto/src/message.rs:12-17). Construction sites:
websocket-server sends its presence on upgrade
(connectors/websocket-server/src/server.rs:121), websocket-client after
the WS handshake (connectors/websocket-client/src/client.rs:276-282),
the wasm client (connectors/websocket-client-wasm/src/connection.rs:146),
and local-process passes typed structs straight to `register_peer` with
no serialization (connectors/local-process/src/lib.rs:59-63).
`Node::register_peer` accepts presence unconditionally
(core/src/node.rs:223-262), and a Presence that fails to decode is today
just a dropped frame with the connection left open
(server.rs:163,194-196; client.rs:352,361-364). Zero version constants
exist anywhere (the `version: u32` on SubscribeQuery is a per-query
generation counter, not a protocol version).

Design (flagged decisions, see bottom):

- `pub const PROTOCOL_VERSION: u32 = 1;` in proto (lib.rs). Version 1 =
  "0.9 wire shapes + versioned Presence". Phase A bumps it to 2. Pre-#294
  binaries are retroactively version 0, detectable only by decode
  failure or a Presence missing the field.
- `Presence` gains `protocol_version: u32` as the LAST field, so the
  pre-#294 encoding is a strict prefix of the new one: a new node reading
  an old Presence hits EOF at the version field and classifies the peer
  as version 0 (clean, non-panicking refusal with an actionable log);
  an old node reading a new Presence either ignores the trailing bytes or
  errors, but the new side refuses and closes regardless, so the
  encounter always terminates deterministically.
- Semantics: REFUSE on inequality, both directions, checked in core by
  making `register_peer` fallible, not in connectors, so every transport
  inherits it (including local-process, which never serializes).
  Rationale: rev 3 forbids an interim dual-encoding state (degrade would
  require maintaining two codecs for every changed message), and RFC
  section 4 already fixes the deployment answer as "durable nodes
  upgrade first". The comparison lives in one function so a future
  version can widen equality to a range without touching the handshake
  again.
- Connectors change from drop-frame to close-connection when a Presence
  fails to decode before establishment (the version-0 encounter) or when
  `register_peer` refuses: send the rejection best-effort, then tear
  down via the existing deregister path.
- Refusal is explicit where the vocabulary exists: a new
  `Message::PresenceRejected { expected, received }` variant sent
  best-effort before close. A version-0 peer cannot decode it; it just
  sees the close (unfixable: 0.9 binaries have no rejection vocabulary).
- Policy hook (the #294 checkbox): deferred. Version acceptance stays
  structural until a concrete use case appears; record on #294 at close.

Tests (tasks.md group 1): same-version connect (both node pairings),
mismatch refused in both directions with the rejection message observed,
hand-crafted version-0 Presence bytes refused without panic, and the
existing integration suite green on the bumped handshake.

## Phase A: the id-keyed epoch

### A1. Identity allocation (RFC 5.1; rev 4, was "Identity derivation")

The rev 3 derivation module (proto/src/schema_id.rs: the three
`*_entity_id` functions, golden vectors, the standalone-DDL zero scope)
is DELETED, together with its tests. Identity is allocated: the
registration executor (A5) mints `EntityId::new()` -- a true ULID -- on
lookup miss. What A1 contributes now is the normative LOOKUP KEYS, which
live with the executor rather than in proto:

- model: by `collection`
- property: by (model id, current name, backend, value_type) -- the type
  pair stays in the key so a retype mints a new identity and the RFC 5.4
  sibling gate keeps firing
- membership: by (model id, property id)

There is nothing to golden-vector: no byte surface participates in
identity. The Ulid audit (old RFC 11.1) is moot; allocated ids carry
real timestamps.

### A2. Frozen genesis encoder + self-certification -- DELETED (rev 4)

core/src/schema/genesis.rs (the frozen encoder, FrozenValue,
validate_catalog_genesis) and its golden/tamper suites are deleted with
derivation. Catalog entities are created via ORDINARY events through the
normal commit machinery; no identity-bearing byte surface exists to
freeze, and there is nothing for receivers to recompute (RFC 5.1,
section 4: single-allocator authority replaces self-certification).

### A3. Creation inside the executor (rev 4, was "Create-with-derived-id")

The known-id create path threaded through the phantom-entity guard
(Transaction::create recording, core/src/transaction.rs:62-72; enforced
in commit_local_trx, core/src/context.rs:84-98) survives, but the
registration executor now feeds it freshly allocated `EntityId::new()`
values instead of derived ones. No public API.

### A4. Catalog collections, protection constants, prefix reservation

- Collection ids `_ankurah_model`, `_ankurah_property`,
  `_ankurah_model_property` via `CollectionId::fixed_name`, beside the
  system collection's (core/src/system.rs:121).
- `PROTECTED_COLLECTIONS` (core/src/system.rs:21, currently read by
  nothing) becomes the four-entry set and finally gets readers: the
  receiver-side commit guard (A6) and `CollectionSet::get` prefix
  enforcement (`_ankurah_` reserved; reject for user collections, allow
  for system callers).
- Catalog entity accessors: raw Entity/backend read-write helpers in the
  SysRoot style (core/src/system.rs:124-127). SYSTEM MODELS, never
  derive(Model) (the ouroboros rule, RFC 4); enforce by comment and by
  the derive-time prefix rejection.

### A5. Registration as an upsert protocol operation (RFC 5.2; rev 4)

proto: language-agnostic descriptors, a request variant beside
Fetch/SubscribeQuery (proto/src/request.rs), and a response variant
carrying the resolved definitions:

```rust
pub struct ModelDescriptor      { pub collection: String, pub name: String,
                                  pub explicit_id: Option<EntityId> }
pub struct PropertyDescriptor   { pub minting_collection: String, pub name: String,
                                  pub backend: String, pub value_type: String,
                                  pub target_collection: Option<String>,
                                  pub renamed_from: Option<String>,
                                  pub explicit_id: Option<EntityId> }
pub struct MembershipDescriptor { pub collection: String, pub property_name: String,
                                  pub optional: bool }
NodeRequestBody::RegisterSchema { models, properties, memberships }
NodeResponseBody::SchemaRegistered { /* full resolved defs, ids included:
                                       models, properties, memberships */ }
```

(Descriptors reference models by collection string and properties by
NAME within the request; ids are the executor's to allocate or resolve,
so no descriptor carries one except `explicit_id` 5.9 bindings.
Reference-typed properties name their target model by collection,
resolved executor-side. Exact field spelling may shift during
implementation; the invariant is: everything the durable side needs, no
Rust types, no client-supplied ids except explicit bindings, and the
response returns every id the client needs to bind.)

Durable-side executor in core, under a process-local mutex end to end:

- Look up each definition by its lookup key (A1) against the executor's
  authoritative lookup state; allocate `EntityId::new()` on miss and
  create via ordinary events; on hit, emit head-parented follow-ups only
  where the requested metadata differs (decision 18's machinery,
  unchanged).
- Apply rename hints BEFORE lookup-or-create, guarded (RFC 5.8): only
  when the current-name lookup misses and the hinted lookup hits; a
  no-op otherwise. The hint write is an ordinary follow-up.
- Build the resolution plan (creates / metadata updates / resolved
  no-ops) and submit it to `check_schema_registration` (decision 26)
  BEFORE emitting anything; refusal fails the whole request.
- Explicit-id bindings (RFC 5.9): look up the entity, verify (backend,
  value_type), hard-fail on absence or mismatch, mint only the
  membership.
- Resolve target-model collection references; allocate the model entity
  on miss (RFC 5.2, preserves #236's circular-reference resolution).
- Every event passes `PolicyAgent::check_event` like any write (trait
  method, core/src/policy.rs:89-96; commit-path call site
  core/src/context.rs:129), which is the gate on who may define schema
  (data freedom: ephemeral nodes may define schema subject to policy).
- Persist and relay through the normal commit machinery; update the
  executor's lookup state SYNCHRONOUSLY post-commit, before releasing
  the mutex (the reactor-fed catalog map lags commit; RFC 5.1 executor
  discipline).
- Respond with SchemaRegistered carrying the full resolved definitions;
  idempotence is the upsert's (a repeat request finds every key, emits
  zero events, returns the same ids).
- A durable node that runs model code executes the same operation
  locally and consumes the same response shape.

Client side: on first mutating use of model M (create/edit), ensure
registration: check the local map, issue RegisterSchema if the binding
is absent, AWAIT the response, and upsert the returned definitions into
the CatalogManager map immediately on ack, so binding and id-keyed
writes proceed right behind it. This replaces cache_compiled's local id
derivation (impossible under allocation); cache_compiled reduces to
recording compiled_schemas for the commit-time-registration gap. Read
paths (fetch/query/subscribe) resolve through the map only. Explicit
`ctx.register::<M>().await` issues eagerly. OFFLINE (rev 4 ruling): a
never-registered collection is a strict error at create/commit
("connect once first"); already-registered collections keep writing
offline against the cached binding; the pending_registrations queue and
drain_pending are DELETED (no reconnect drain; residue stays the
representation for catalog lag and denied registrations, converged by
normalize/migrate-on-bind).

Tests: upsert idempotency (register twice -> same ids, zero events);
rename-hint application and its no-op guard; retype mints a distinct id
(lookup key includes the type pair); policy denial refuses cleanly; the
strict never-registered-offline error at create/commit.

### A6. Receiver-side structural protection (RFC 4; rev 4 trims it)

- Durable nodes reject ordinary CommitTransaction requests carrying
  events that target any protected collection, outright, regardless of
  sender version. The only mutation path is the registration executor.
- Catalog events arriving via peer replication are ordinary
  policy-trusted events originating from the allocator; the rev 3
  content self-certification (validate_catalog_genesis) is deleted with
  derivation (RFC 4: single-allocator authority replaces it).
- Tests: commit into `_ankurah_model` refused; legitimate registration
  relays and applies.

### A7. Catalog subscription and map (RFC 5.2, AC3)

- `CatalogMap` in core: by (minting model, name, backend, value_type)
  -> property id; by property id -> definition; by model id ->
  membership set (the contract); by collection -> model id; display-name
  index per collection for resolution (A9) and the sibling gate (A10).
- Warmed by three predicate-True subscriptions at system-ready, ordinary
  LiveQuery machinery (the catalog deliberately does NOT ride the
  Presence handshake the way the system collection does,
  core/src/node.rs:245-258), and fed IMMEDIATELY by SchemaRegistered
  responses (rev 4): ensure_registered upserts the returned definitions
  on ack, ahead of reactor delivery.
- `wait_catalog_ready` gate analogous to wait_system_ready
  (core/src/system.rs:97-101): consumers with no compiled schema defer
  resolution until the initial snapshot lands.
- `hard_reset` flushes the catalog map and every cached schema binding
  along with what it already clears (core/src/system.rs:208-234):
  allocated ids belong to one system and must not leak across a root
  change.
- Tests: map warms from a peer with existing catalog; updates arrive via
  subscription; response-fed upsert observed ahead of the subscription;
  hard_reset flush verified.

### A8. LWW diff v2 and state 0xA2, id-keyed (RFC 5.5)

Wire shapes (constants at core/src/property/backend/lww.rs:18, 30-31;
refusal behavior at lww.rs:176-180 stays intact for old buffers):

```rust
const LWW_DIFF_VERSION_2: u8 = 2;
const LWW_STATE_VERSION_2: u8 = 0xA2;   // LWW_STATE_VERSION_BASE + 2

// diff v2 payload and state 0xA2 payload both carry two maps:
struct V2Map<T> {
    by_id: BTreeMap<EntityId, T>,       // the normative id-keyed data
    residue: BTreeMap<PropertyName, T>, // ONLY carried-over legacy names
}                                        // not yet translatable to ids
```

- Two maps rather than an enum key: the id map is structurally id-only
  (writers cannot smuggle names), and the empty-residue common case
  costs 8 bytes. Writers MUST emit by_id for every property they can
  resolve; residue exists so that a legacy buffer with an untranslatable
  name (data from a model nobody re-registered) survives rewrite-on-save
  without data loss.
- In-memory keying becomes `PropertyKey::{Id(EntityId), Name(String)}`
  over the existing ValueEntry lifecycle (lww.rs:44-73): Name keys serve
  catalog/system collections (permanently name-keyed, RFC 4 bootstrap
  exemption) and untranslated residue; everything else keys by Id.
  Name-based public access (entity property lookup) resolves name -> id
  through the entity's schema binding before touching the map.
- Backends learn their keying mode and name<->id binding from the entity
  at construction: catalog/system collections run name-keyed v1/0xA1
  forever (the RFC 4 bootstrap exemption); user collections run id-keyed
  v2/0xA2.
- Read fallback: 0xA1 and pre-0.9 buffers (lww.rs:154-185) decode
  name-keyed, translate name -> id through (local schema, catalog),
  residue for misses; lazy rewrite-on-save emits 0xA2, exactly the 0.9
  legacy precedent. v1 diffs from old events apply the same way.
- Unknown property ids in incoming v2 payloads apply and persist
  opaquely (RFC 5.6 catalog lag; apply is already schema-blind,
  lww.rs:336-363); projection surfaces UnknownProperty until the catalog
  entry arrives.
- Field broadcasts stay name-keyed in Phase A (per-field addressing
  consumers are Phase D; signals are node-local).
- Yrs: NO rekeying, ratified (RFC 5.5). Roots stay property-name-keyed;
  the catalog binds root name to property id at the ankurah boundary.
- Tests: v2 round-trip (diff and state); v1 -> v2 translation with and
  without full resolution (residue preserved); pre-0.9 -> v2; simulated
  0.9 binary refuses 0xA2/v2 cleanly (the lww.rs:176-180 arm, pinned by
  test); catalog collections still write v1/0xA1.

### A9. ankql Identifier and the resolution pass (RFC 5.3, AC4, AC5)

- New AST node (resolved form; `PathExpr` stays the parse-time form):

```rust
pub struct Identifier { pub property: EntityId, pub name: String,
                        pub subpath: Vec<String> }
```

- A resolution pass Selection -> resolved Selection binds steps[0]
  against (local compiled schema, then catalog map), leaves the rest as
  subpath, and fails closed with UnknownProperty naming collection and
  property (AC5). It replaces/absorbs the TypeResolver pass per its own
  TODO (core/src/type_resolver.rs:24-26); call sites
  core/src/context.rs:344, core/src/node.rs:858, node.rs:916,
  core/src/livequery.rs:217.
- Nodes with no compiled schema defer behind wait_catalog_ready (A7)
  instead of failing during warm-up.
- Wire: Fetch/SubscribeQuery Selections carry Identifier
  (proto/src/request.rs:119-128; bincode, hard break, inside this
  epoch's version bump). Receivers evaluate predicates with unknown
  property ids as matching nothing (pass-through until #274).
- Consumers unify on one rule (kills the three inconsistent
  missing-property behaviors: filter hard error
  core/src/selection/filter.rs:59-91, reactor unwrap_or(false)
  core/src/reactor/subscription_state.rs:391, SQL assume_null
  storage/postgres/src/lib.rs:528-548): unresolvable fails at build;
  resolvable-but-absent evaluates per A10. The assume_null and
  referenced_columns first-vs-last-step inconsistency
  (ankql/src/ast.rs:241-333) collapses because resolution fixes which
  step is the property once.
- SQL engines consume Identifier.name for column addressing in Phase A
  (catalog-bound columns are Phase C).
- Tests: resolution against compiled schema, against catalog-only
  (dynamic consumer), UnknownProperty on neither; subpath preservation;
  cross-node query with mismatched display names resolves to the same
  property id (rename scenario).

### A10. Read-path rules and the #175 fix (RFC 5.4)

Rule ladder, implemented where property projection happens (View getters
keep compiled optionality; engines, predicate evaluation, and dynamic
access consult the catalog membership):

1. registered + present -> value.
2. registered + absent + optional membership -> None.
3. registered + absent + required -> the value type's default, gated by
   rule 4. Defaults: "" / 0 / 0.0 / false / empty binary / Json null.
   EXCEPTION (flagged decision): required `entityid` has no fabricable
   default; absent reads stay PropertyError::Missing.
4. Sibling gate: scan ACROSS contracts for a same-display-name sibling
   property id with data present on this entity; if found ->
   PropertyError::TypeSkew naming both ids, never a fabricated default.
5. Unregistered and not in local schema -> PropertyError::UnknownProperty.

Ambiguous or missing optionality resolves as optional (absent -> None,
never a default); membership whose `optional` follow-up has not arrived
is treated as optional.

Zero-op creation events: creation with an empty operation set generates
and persists an event instead of being skipped (EventId hashes fine over
empty operations, proto/src/data.rs:18-24). Un-ignores
tests/tests/yrs_backend.rs:303-332 (test_sequential_text_operations,
"blocked on #236"); integration test: create with empty string, reload,
read back "".

### A11. Derive macro, attributes, lifecycle glue

- The macro (derive/src/model/) emits a static ModelDescriptor: the
  local compiled schema (ModelSchema/FieldSchema per the section-7
  reconciliation), with per-field (name, backend, value_type) from the
  NORMATIVE mapping table (RFC 4); ephemeral fields excluded
  (derive/src/model/description.rs:31-40). No anchor field (rev 4).
- Attributes: `#[property(renamed_from = "...")]` (the rename hint,
  RFC 5.8; ruled 2026-07-06), `#[property(id = "...")]` and
  `#[model(id = "...")]` (explicit binding, RFC 5.9; parse and validate
  id format at compile time; verification and membership minting happen
  at registration; binding keeps working unchanged beside the hint).
- `_ankurah_` collection prefix -> compile error at derive time.
- Registration triggers threaded through context paths: mutating use
  auto-asserts (awaiting the response), read paths resolve via the map,
  `ctx.register::<M>()`.
- Tests: descriptor snapshot for a representative model (every table
  row); rename-hint application and no-op guard; reserved prefix fails
  compile (trybuild or equivalent); shared property by explicit id
  readable from two contracts with differing optional; retype mints a
  distinct property id (and the sibling gate fires on mixed data, A10
  test).

### A12. Errors, guards, audits (cross-cutting)

- New error variants: UnknownProperty, TypeSkew (PropertyError);
  registration refusals (explicit-id mismatch or absence, reserved
  prefix); the strict never-registered-offline error at create/commit
  (rev 4). CatalogGenesisError and the anchor-reuse refusal are deleted
  with the identity-plane pivot.
- PROTECTED_COLLECTIONS readers (with A4/A6); hard_reset flush (with
  A7). The ulid-timestamp audit is moot under allocation.
- Docs: brief internals note under docs/internals/ once shapes settle
  (the schema-evolution book chapter waits on #291).

## Phase A protocol bump

Phase A ships as PROTOCOL_VERSION = 2 (one epoch: LWW v2/0xA2, resolved
Identifier Selections, RegisterSchema). Durable nodes upgrade first
(receiver-side protection makes this a deployment order, not a
simultaneous upgrade). No interim name-keyed-with-catalog state ships.

## Phase C outline (engine-local; detailed after A)

- sled: property_config keyed by property entity id bytes instead of
  name bytes (storage/sled/src/property.rs:32-63); u32 compaction and
  row format untouched.
- postgres/sqlite: columns stay human-named; EACH ENGINE persists its
  own column <-> property-definition-id binding (the sled
  property_config precedent generalized), because names are mutable
  and collisions resolve engine-locally; renames become ALTER TABLE
  RENAME COLUMN driven by catalog changes; display-name collisions
  among live ids (retype lineage, policy-permitted fork, shared names
  across contracts) keep the first claimant bare and suffix newcomers
  from the property id (`name` / `name_bb`; RFC 5.5, mental model
  ratified 2026-07-06 -- materialization is indexing strategy plus
  admin legibility, never the data model). Discharges the TODO at
  storage/postgres/src/lib.rs:368-373. Keep canonical-write vs
  materialization-write seams clean inside set_state
  (storage/postgres/src/lib.rs:342, materialization loop at 364-391)
  per RFC 4a; #304 consumes that cleanliness.
- IndexedDB: name-bound via catalog, lazy re-materialization on rename
  (storage/indexeddb-wasm/src/collection.rs:73-81).

## Landing strategy

- Phase 0 goes up as its own small PR from a branch off main
  (`presence-protocol-version`), independently reviewable and mergeable
  ahead of everything (it is also #284's missing guard).
- Phase A lands on this branch (model-property-metadata), rebased over
  Phase 0 once merged; red-to-green commits per work package where a
  test can pin the change; adversarial review on A5 (upsert atomicity
  and allocation authority) and A8 (v2/v1 fallback) before the PR.
- Validation gate before any push: cargo test -p ankurah-core (lib),
  ankurah-tests, ankurah-derive, ankql, jwt-auth; isolated cargo check
  -p ankurah-core --features wasm; cargo fmt --all; taplo fmt after
  Cargo.toml changes.
- Version files untouched; releases go through RELEASES +
  .release/bump-version.sh only (CI auto-publishes on version change).

## Decisions made in this plan (RFC delegated; flag disagreements)

1. **Refuse, not degrade** (#294): version equality required; comparison
   isolated in one function so a future release can widen it to a range.
   Degrade contradicts rev 3's no-dual-encoding stance.
2. **Presence field appended last** so the old encoding is a prefix of
   the new one and a version-0 peer is detectable by EOF, not guesswork.
3. **PROTOCOL_VERSION u32, starts at 1**, Phase A bumps to 2; pre-#294
   binaries are implicitly version 0.
4. **v2 two-map shape** (by_id + residue) rather than an enum key:
   structurally id-only writes, lossless legacy carry-over.
5. **In-memory PropertyKey::{Id, Name}** with name-keyed mode reserved
   for catalog/system collections and untranslated residue.
6. **Required `entityid` has no default**: absent stays Missing rather
   than fabricating a reference (RFC 5.4 lists defaults only for
   scalars).
7. **Field broadcasts stay name-keyed in Phase A** (no consumer needs
   id-keyed signals yet; node-local, no wire impact).
8. **Registration descriptors reference by collection string + property
   name** within the request (rev 4; was "+ anchor"); the executor
   allocates or resolves all ids (except explicit-id bindings, which
   carry the literal id). Target-model references travel as collection
   strings, resolved executor-side.
9. **Phase 0 as a separate PR off main**; Phase A follows on this
   branch.
10. **Catalog subscriptions are policy-free on durable nodes** (the
    SystemManager precedent: the map is node infrastructure reading its
    own storage; mutation stays gated by check_event, remote access by
    the server-side subscription checks). Ephemeral nodes warm on the
    first context_async with that context's credentials: a node's
    catalog visibility follows the credentials it runs under.
11. **StorageEngine::list_collections** added (additive, default empty)
    so the catalog warm never materializes empty `_ankurah_*` trees;
    sled overrides it, the other engines' overrides are follow-up work
    (tracked in tasks.md group 4).
12. **Reactor event-batching fix folded in**: ReactorUpdateItem now
    appends events across same-entity changes within one notify batch
    instead of keeping only the first change's events. A multi-event
    commit (a registration's creation + follow-up events) previously relayed
    live with entity state ahead of its listed events, which receivers
    reject; latent for any multi-event commit, surfaced by the catalog.
13. **0xA2 state entries carry an optional display-name hint** for the
    A-to-C window: postgres/sqlite/sled parse state buffers UNBOUND
    (postgres lib.rs:366, sqlite engine.rs:279, sled collection.rs:118)
    to materialize columns/rows, so a pure id-keyed state buffer would
    black out engine querying until Phase C. Hints live only inside the
    opaque LWW state buffer; v2 DIFFS stay pure id-keyed, so identity
    and convergence are untouched; renames leave stale hints until
    rewrite-on-save; Phase C's catalog-bound engines remove the
    dependence. Rejected: an engine trait-seam change (Phase C
    territory) and 0xA1-emit from id-keyed memory (cannot encode
    unknown-id entries: data loss).
14. **Predicate-level required-defaults are out of Phase A** (RFC 5.4
    scoping): resolved-identifier predicate evaluation treats an absent
    property as NULL uniformly (IsNull matches, comparisons false),
    which unifies the three historical missing-property behaviors;
    rule 3's type default applies in the compiled View getters (which
    know required-ness at compile time), gated by the sibling check.
    Consulting per-membership optionality inside the filter is deferred
    with the Filterable follow-ups in tasks.md.
15. **Cross-root state transplant is unsupported** (maintainer ruling
    2026-07-05: "different roots means different systems"): a BOUND
    checked read has no foreign-id fallback; a same-display-name value
    under an unresolvable id fails visible as TypeSkew. Display-name
    hints are engine-projection only and never route writes or bound
    reads (the schema-blind projection regime is decision 17).
16. **Commit-time registration closes the edit-only gap**: the sync
    edit path cannot await a durable registration, so commit_local_trx
    ensure-registers any touched collection whose compiled schema is
    recorded but not yet ensured. AMENDED by rev 4: for a collection
    with NO binding at all (never registered on this system), a failed
    or impossible registration at create/commit is the STRICT
    never-registered-offline error, failing the write; for an
    already-bound collection, a denied or unreachable re-assert stays a
    warning and the write proceeds (unregistered fields land as
    residue). ctx.register::<M>() remains the eager strict form.
17. **The schema-blind projection regime of the checked read** (pre-PR
    review finding, 2026-07-05): a backend parsed with NO binding --
    the engines' post-filter TemporaryEntity and policy-agent state
    inspection, which have no contract by construction -- reads a
    display name as bare Name residue first, then a UNIQUE hint
    claimant among id-keyed entries; TWO claimants (a retype lineage)
    fail visible as TypeSkew, and the lenient projection read returns
    None rather than guessing. Bound backends never hint-route
    (decision 15 unchanged). Without this regime, engine post-filtering
    and policy inspection of 0xA2 states silently evaluated every
    id-keyed property as absent. This is the checked counterpart of the
    property_values materialization projection, and Phase C's
    catalog-bound engines subsume it together with the hints.
18. **Provenance-ordered follow-ups** (maintainer direction,
    2026-07-05; SPLIT by rev 4). The `anchored` descriptor bit and the
    rename-back admission it enabled die with the anchor apparatus.
    The follow-up machinery SURVIVES VERBATIM: executor follow-ups
    (property/model display name, target_model, membership optional)
    are emitted only when the current catalog value differs, and parent
    at the entity's CURRENT head instead of the genesis:
    genesis-parenting made every metadata write after the first
    mutually CONCURRENT, so a chained rename or an optional flip was
    decided by event-id tiebreak (hash luck) rather than recency.
    Head-parenting makes an unchanged re-registration a true no-op
    (zero events); rename-hint name updates (RFC 5.8) ride this same
    machinery. Mixed-fleet consequence: display names follow the most
    recent registration (an old binary re-asserts its compiled name on
    startup); addressing is unaffected because each node resolves
    through its own compiled overlay (RFC 5.8).
19. **`Property::VALUE_TYPE` (erratum 2 resolution; maintainer
    direction, 2026-07-05)**: the `Property` trait declares its
    normative value_type as an associated const (default "string"),
    and `#[derive(Model)]` emits `<Ty as Property>::VALUE_TYPE` for
    field types outside the built-in table instead of assuming
    "string" -- compile-time, zero-cost (const in the static
    initializer), no per-field annotation. `#[derive(Property)]` pins
    "string" to match its JSON-string serialization; hand-written
    impls declare the `Value` variant their `into_value` actually
    produces. Every existing type keeps its current string (no
    re-keying); shipped-type changes are retypes by definition.
    Rejected: keeping the always-string assumption (misdeclares
    hand impls, and retro-fitting the const post-release would re-key
    every custom property that then declared a non-string type);
    a required (defaultless) const (breaks every downstream hand
    impl for marginal gain -- the default is correct for the
    JSON-catch-all convention the ecosystem actually uses). Rev 4
    note: VALUE_TYPE now feeds the property LOOKUP KEY instead of a
    hash input; the decision and its invariant are otherwise
    unchanged.
20. **Upsert executor under a process-local mutex** (rev 4, RFC 5.1):
    the whole RegisterSchema execution serializes on one async mutex;
    ids are allocated with `EntityId::new()`; the executor's lookup
    state is updated synchronously post-commit BEFORE the mutex
    releases, because the reactor-fed catalog map lags commit and the
    executor must not race itself into double-allocation.
21. **SchemaRegistered response feeds the catalog map** (rev 4, RFC
    5.2): the response carries the full resolved definitions;
    ensure_registered upserts them into the CatalogManager map
    immediately on ack, ahead of reactor delivery, so binding and
    id-keyed writes proceed right behind registration. cache_compiled
    reduces to recording compiled_schemas for the
    commit-time-registration gap; its local id derivation is deleted.
22. **Strict never-registered-offline error** (rev 4 maintainer
    ruling, RFC 5.2): creating entities in a collection with no cached
    binding while no durable peer is reachable fails at create/commit
    with an actionable "connect once first" error. The offline
    pending_registrations queue and drain_pending are deleted; residue
    remains the representation for catalog lag and denied
    registrations, converged by normalize/migrate-on-bind.
23. **Rename hint semantics** (rev 4, RFC 5.8): transient, idempotent,
    guarded (applies only when the current-name lookup misses and the
    hinted lookup hits; never merges identities, never creates a
    second live property under one lookup key). Attribute form RULED
    2026-07-06: `#[property(renamed_from = "old")]`, a convenience
    beside explicit-id binding, which stays fully load-bearing (5.9).
24. **Single-allocator routing** (rev 4, RFC 5.1): one durable node
    per system executes RegisterSchema; multi-durable deployments
    route registration to it. Documented as a constraint on #309; a
    real allocator protocol (leases or consensus) is future work
    there.
25. **Stale-writer rename fork is policy-governed** (maintainer
    ruling, 2026-07-06; RFC 5.8): no former-names fallback or other
    dedicated convergence mechanism. A pre-rename binary
    re-registering the old name is an ordinary schema-definition
    request: permissive systems allocate (visible orphan lineage,
    tooling reconciles), restrictive systems refuse via the
    PolicyAgent in either style -- object-based (that property
    definition is not writable) or principal-based (that user may not
    define schema) -- and the denied field lands as name-keyed
    residue per decisions 16 and 22. The policy surface must support
    BOTH discrimination axes (RFC 5.2, 5.7).
25b. **First-use registration on read paths + the anticipated-collection
    fallback** (rev 4 corollary, RFC 5.2/5.3; surfaced by the
    subscribe-before-create tests, REVISED 2026-07-06 by the
    replica-lag flake -- REN 2 second ruling, ratified in session): a
    warm catalog REPLICA cannot prove a collection unregistered (it may
    lag the authority by a subscription hop), so resolution never
    renders that verdict alone. When the warm map cannot resolve a
    reference and the binary carries a compiled schema, the model
    REGISTERS AT FIRST USE via the ordinary idempotent upsert: an
    existing schema resolves to a no-op plan (zero events, policy verb
    skipped) whose response feeds the map synchronously, so reads warm
    against authoritative rows and query_wait initializes populated.
    The anticipated-collection rule survives as the FALLBACK when
    registration cannot run (policy denial, no durable peer): an
    unregistered collection provably holds no entities (creation
    requires registration), so the fetch answers EMPTY and a live query
    activates empty (Predicate::False placeholder) and DEFERS
    (wait_collection_registered), upgrading through the ordinary
    selection-update path when registration lands. The resolution
    deferral also KICKS the ephemeral catalog subscription inline when
    a sync-context node never subscribed (ensure_subscribed is
    idempotent and awaited), and fails closed when neither the kick nor
    registration can warm the catalog (offline). Fail-closed is
    unchanged for unknown properties in registered collections and for
    references no compiled schema anticipates (AC5). PropertyError
    gains UnregisteredCollection to carry the distinction.
26. **`check_schema_registration` PolicyAgent verb** (maintainer
    direction, 2026-07-06; RFC 5.7): a new trait method with a
    default-allow implementation, called by the executor after its
    lookup phase and before any event is emitted, still under the
    mutex: `fn check_schema_registration(&self, node, cdata, plan) ->
    Result<(), AccessDenied>`. The plan is a core-side type (never on
    the wire) listing what the request will actually do: definitions
    to CREATE (descriptors), metadata UPDATES (entity id, field,
    old -> new: renames, optional flips, retargets), and resolved
    no-ops. Rationale: check_request cannot know whether a descriptor
    exists (an agent gating "actual creation" there would duplicate
    the executor's lookup and race the mutex), and check_event fires
    per event mid-commit, where creation-ness must be
    reverse-engineered from is_entity_create + collection; the
    executor holds the answer for free at exactly the decision point.
    Refusal fails the whole registration (all-or-nothing, matching
    the single commit_remote_transaction); check_event still gates
    every emitted event underneath (defense in depth); sync, matching
    check_event. Registration-scoped: the broader per-property policy
    surface stays deferred to the #264/#274 consolidated design.
