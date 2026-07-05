# Implementation plan: model and property metadata

Authority: rfc.md in this directory (RATIFIED rev 3, commit ff40a3bf).
This plan translates the RFC into a build order; it does not re-argue
design. Where the RFC delegated a decision to implementation, the choice
is made here and collected in "Decisions made in this plan" at the bottom
for async review. Code citations verified against this branch (based on
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
Phase A, one protocol epoch (proto v1 -> v2), build order:
  A1 derivation  ->  A2 frozen genesis encoder  ->  A5 registration op
  A1            ->  A3 create-with-derived-id   ->  A5
  A4 catalog collections + protection ----------->  A5, A6 receiver guard
  A5, A7 catalog subscription/map -------------->  A8 LWW v2 fallback,
                                                    A9 resolution,
                                                    A11 lifecycle/macro
  A8 LWW v2 (testable early against a resolver trait)
  A9 ankql Identifier + resolution pass  ->  A10 read rules (+ #175 fix)
  A11 derive macro attributes + registration triggers
  A12 errors/guards/audits (cross-cutting, lands with its consumers)
  |
Phase C (sled rekey, SQL column binding + rename DDL, IndexedDB)
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

### A1. Identity derivation (RFC 5.1)

Pure functions, no I/O; home: proto (beside EntityId; sha2 is already a
proto dependency for EventId hashing) so core, derive-macro consumers,
and future bindings share one implementation.

```rust
// proto/src/schema_id.rs (new)
pub fn model_entity_id(root: &EntityId, collection: &str) -> EntityId;
pub fn property_entity_id(root: &EntityId, minting_model: &EntityId,
                          anchor: &str, backend: &str, value_type: &str) -> EntityId;
pub fn membership_entity_id(root: &EntityId, model: &EntityId,
                            property: &EntityId) -> EntityId;
```

- SHA-256 over domain tag ("ankurah.model.v1" / "ankurah.property.v1" /
  "ankurah.membership.v1") with every field length-prefixed as
  `(len as u64).to_le_bytes()`; first 16 bytes into
  `EntityId::from_bytes`. Full 16 bytes are hash output; never truncate
  entropy (RFC 5.1).
- Standalone-DDL scope (RFC 5.1): a zero model id constant for
  standalone shared declarations; define it now, used by Phase D.
- Golden-vector tests: fixed root and inputs, exact expected id bytes,
  so any accidental change to tags, prefixing, or truncation breaks red.
- Ulid audit (RFC 11.1): sweep for any code interpreting EntityId
  timestamp bits; fix dependents or record "none found" in the PR.

### A2. Frozen genesis encoder + self-certification (RFC 5.1)

Home: core (needs core's Value); a standalone module, expressly NOT
calling the live LWW encoder. Pinned forever to the v1 diff shape as it
exists today: `Operation { diff: bincode(LWWDiff { version: 1, data:
bincode(BTreeMap<PropertyName, Option<Value>>) }) }`
(core/src/property/backend/lww.rs:18, 204-206), scalar Values only.

```rust
// core/src/schema/genesis.rs (new)
pub fn model_genesis(root, collection) -> (EntityId, Event);      // sets: collection
pub fn property_genesis(root, minting_model, anchor, backend,
                        value_type) -> (EntityId, Event);          // sets: minted_for, name, backend, value_type
pub fn membership_genesis(root, model, property) -> (EntityId, Event); // sets: model, property
pub fn validate_catalog_genesis(collection, event) -> Result<(), CatalogGenesisError>;
```

- Genesis = exactly the identity fields, one LWW operation, empty parent
  clock; EventId computed the ordinary content-hash way.
- `validate_catalog_genesis` recomputes the entity id from the decoded
  payload and the event id from re-encoding; receivers and the executor
  both use it (self-certifying genesis, RFC 4).
- Tests: golden byte vectors for all three genesis kinds (encoder
  drift breaks red, independent of lww.rs changes); two derivations of
  the same definition produce identical EventIds; tampered payload
  (field value, field set, or operation count) fails validation.

### A3. Create-with-derived-id

`Entity::create(id, collection)` exists (core/src/entity.rs:145-154) but
`WeakEntitySet::create` mints ids internally (core/src/entity.rs:680-687)
and the commit path rejects foreign-id creates via the phantom-entity
guard (recorded in Transaction::create, core/src/transaction.rs:62-72;
enforced in commit_local_trx, core/src/context.rs:84-98). Add a pub(crate)
create-with-known-id path for the registration executor that registers
with the phantom guard exactly as create() does. No public API.

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

### A5. Registration as a protocol operation (RFC 5.2)

proto: language-agnostic descriptors and a new request variant beside
Fetch/SubscribeQuery (proto/src/request.rs):

```rust
pub struct ModelDescriptor      { pub collection: String, pub name: String }
pub struct PropertyDescriptor   { pub minting_collection: String, pub anchor: String,
                                  pub name: String, pub backend: String,
                                  pub value_type: String,
                                  pub target_model: Option<EntityId>,
                                  pub explicit_id: Option<EntityId> }
pub struct MembershipDescriptor { pub collection: String, pub property_anchor: String,
                                  pub optional: bool }
NodeRequestBody::RegisterSchema { models: Vec<ModelDescriptor>,
                                  properties: Vec<PropertyDescriptor>,
                                  memberships: Vec<MembershipDescriptor> }
```

(Descriptors reference models by collection string and properties by
anchor within the request, since root-scoped ids are the executor's to
derive; `explicit_id` carries a 5.9 binding. Exact field spelling may
shift during implementation; the invariant is: everything the durable
side needs, no Rust types, no pre-derived ids except explicit bindings.)

Durable-side executor in core:

- Derive all ids (A1); consult the catalog map (A7) for existing
  entities; run the anchor-reuse refusal (RFC 5.8: derived id exists
  with a different current display name -> refuse, demand an anchor).
- Explicit-id bindings (RFC 5.9): look up the entity, verify (backend,
  value_type), hard-fail on absence or mismatch, mint only the
  membership.
- Emit frozen genesis events (A2) for unknown entities and ordinary LWW
  follow-up events for non-identity fields (membership optional,
  target_model, display name when it differs from the anchor); every
  event passes `PolicyAgent::check_event` like any write (trait method,
  core/src/policy.rs:89-96; commit-path call site
  core/src/context.rs:129), which is the gate on who may define schema
  (data freedom: ephemeral nodes may define schema subject to policy).
- Persist and relay through the normal commit machinery; idempotence is
  structural (same derivation + frozen encoder = same EventId = no-op on
  redelivery, core/src/entity.rs:251-254).
- A durable node that runs model code executes the same operation
  locally; response is success/error only (clients can derive ids
  themselves).

Client side: on first mutating use of model M (create/edit), ensure
registration: derive ids, check the local map, issue RegisterSchema if
absent. Read paths (fetch/query/subscribe) derive and cache only.
Explicit `ctx.register::<M>().await` issues eagerly. Offline: derive
ids, write id-keyed data locally, queue the operation for reconnect
(drain on durable-peer connect, beside the subscription relay's
notify_peer_connected seam, core/src/node.rs:240-243).

Tests: two-node concurrent registration converges to identical genesis
EventIds; re-issued RegisterSchema and two durable executors are
idempotent; policy denial refuses cleanly; offline queue drains on
reconnect.

### A6. Receiver-side structural protection (RFC 4)

- Durable nodes reject ordinary CommitTransaction requests carrying
  events that target any protected collection, outright, regardless of
  sender version. The only mutation path is the registration executor.
- Catalog events arriving via peer replication validate by content:
  genesis events must pass `validate_catalog_genesis` (A2); follow-ups
  are policy-checked writes.
- Tests: commit into `_ankurah_model` refused; tampered genesis refused;
  legitimate registration relays and applies.

### A7. Catalog subscription and map (RFC 5.2, AC3)

- `CatalogMap` in core: by (minting model, anchor, backend, value_type)
  -> property id; by property id -> definition; by model id ->
  membership set (the contract); by collection -> model id; display-name
  index per collection for resolution (A9) and the sibling gate (A10).
- Warmed by three predicate-True subscriptions at system-ready, ordinary
  LiveQuery machinery (the catalog deliberately does NOT ride the
  Presence handshake the way the system collection does,
  core/src/node.rs:245-258).
- `wait_catalog_ready` gate analogous to wait_system_ready
  (core/src/system.rs:97-101): consumers with no compiled schema defer
  resolution until the initial snapshot lands.
- `hard_reset` flushes the catalog map and every derived-id cache along
  with what it already clears (core/src/system.rs:208-234): derived ids
  are root-scoped and must not leak across a root change.
- Tests: map warms from a peer with existing catalog; updates arrive via
  subscription; hard_reset flush verified.

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
  forever (genesis writes additionally pass through A2's frozen encoder,
  never this path); user collections run id-keyed v2/0xA2.
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
  reconciliation), with per-field (anchor = field name unless pinned,
  backend, value_type) from the NORMATIVE mapping table (RFC 4);
  ephemeral fields excluded (derive/src/model/description.rs:31-40).
- Attributes: `#[property(anchor = "...")]` (rename lineage, RFC 5.8),
  `#[property(id = "...")]` and `#[model(id = "...")]` (explicit
  binding, RFC 5.9; parse and validate id format at compile time;
  verification and membership minting happen at registration).
- `_ankurah_` collection prefix -> compile error at derive time.
- Registration triggers threaded through context paths: mutating use
  auto-asserts, read paths derive + cache, `ctx.register::<M>()`.
- Tests: descriptor snapshot for a representative model (every table
  row); anchor chain keeps the original anchor; reserved prefix fails
  compile (trybuild or equivalent); anchor-reuse refusal; shared
  property by explicit id readable from two contracts with differing
  optional; retype mints a distinct property id (and the sibling gate
  fires on mixed data, A10 test).

### A12. Errors, guards, audits (cross-cutting)

- New error variants: UnknownProperty, TypeSkew (PropertyError);
  CatalogGenesisError; registration refusals (anchor reuse, explicit-id
  mismatch, reserved prefix).
- PROTECTED_COLLECTIONS readers (with A4/A6); ulid-timestamp audit
  (with A1); hard_reset flush (with A7).
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
- postgres/sqlite: columns stay human-named; catalog binds property id
  -> column; renames become ALTER TABLE RENAME COLUMN driven by catalog
  changes; (name, type) collisions disambiguate the newcomer with a
  short id suffix (discharges the TODO at
  storage/postgres/src/lib.rs:368-373). Keep canonical-write vs
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
  test can pin the change; Opus adversarial review on A2 (frozen
  encoder), A8 (v2/v1 fallback), and A5 (registration convergence)
  before the PR.
- Validation gate before any push: cargo test -p ankurah-core (lib),
  ankurah-tests, jwt-auth; isolated cargo check -p ankurah-core
  --features wasm; cargo fmt --all; taplo fmt after Cargo.toml changes.
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
8. **Registration descriptors reference by collection string + anchor**
   within the request; the executor derives all ids (except explicit-id
   bindings, which carry the literal id).
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
    commit (a registration's genesis + follow-up) previously relayed
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
14a. **Predicate-level required-defaults are out of Phase A** (RFC 5.4
    scoping): resolved-identifier predicate evaluation treats an absent
    property as NULL uniformly (IsNull matches, comparisons false),
    which unifies the three historical missing-property behaviors;
    rule 3's type default applies in the compiled View getters (which
    know required-ness at compile time), gated by the sibling check.
    Consulting per-membership optionality inside the filter is deferred
    with the Filterable follow-ups in tasks.md.
14b. **Cross-root state transplant is unsupported** (maintainer ruling
    2026-07-05: "different roots means different systems"): the checked
    read has no foreign-id fallback; a same-display-name value under an
    unresolvable id fails visible as TypeSkew. Display-name hints are
    engine-projection only and never route reads or writes.
14. **Commit-time registration closes the edit-only gap**: the sync
    edit path cannot await a durable registration, so commit_local_trx
    ensure-registers any touched collection whose compiled schema is
    recorded but not yet ensured. Auto-assert triggers are best-effort
    (a denied registration warns and never fails a data write: policy
    gates schema definition, not data writes); ctx.register::<M>() is
    the strict form.
