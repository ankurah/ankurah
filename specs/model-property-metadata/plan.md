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
Phase A, one released protocol epoch (proto v1 -> v3), build order (rev 4):
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

Delivered as part of PR #307:

- `Presence` carries `protocol_version: u32` as its last field and the
  metadata epoch exports `PROTOCOL_VERSION = 3`. Pre-versioning binaries are
  treated as version 0. An ephemeral legacy Presence is a strict prefix that
  reaches EOF at the new field; a durable legacy Presence also contains the
  old collection-bearing EntityState, so the refusal classifier parses that
  mirrored legacy shape exactly and rejects trailing bytes.
- `Node::register_peer` is fallible and is the single compatibility
  enforcement point for every transport, including local-process. Version
  inequality refuses; the comparison remains isolated so a future release
  can deliberately widen it.
- Connectors own only transport mechanics: construct Presence, send
  `PresenceRejected` best-effort while the sink exists, then close. A native
  websocket refusal enters the ordinary exponential-backoff path rather than
  reconnecting in a tight loop.
- A version-policy hook remains deferred until there is a concrete use case.
- Tests cover same-version connection, mismatches in both directions,
  hand-crafted version-0 bytes, explicit rejection, and reconnect backoff.

AMENDED by the identity-attestation RFC (2026-07-11): protocol v6 changes
the Presence body and adds a symmetric challenge, so the original
prefix/suffix compatibility applies only to the v1 transition described
above. The outer `Message` discriminants from v5 remain fixed and new kinds
append; decoding is exact, bounded, and rejects trailing bytes. A frozen v5
Presence is therefore closed cleanly rather than being reinterpreted as a v6
challenge. Explicit `PresenceRejected` remains best-effort when the older
payload cannot be decoded far enough to name its version.

## Phase A: the id-keyed epoch

### A1. Identity allocation (RFC 5.1; rev 4, was "Identity derivation")

The rev 3 derivation module (proto/src/schema_id.rs: the three
`*_entity_id` functions, golden vectors, the standalone-DDL zero scope)
is DELETED, together with its tests. Identity is allocated: on lookup miss,
the registration executor (A5) emits an ordinary genesis with fresh entropy
and uses its full content hash as the EntityId. What A1 contributes now is
the normative LOOKUP KEYS, which
live with the executor rather than in proto:

- model: by `collection`
- property: by (model id, current name); backend and value_type are checked
  against the found property's fixed canonical pair and never enter identity
- membership: by (model id, property id)

There is no catalog-specific identity encoder to golden-vector. The shared
genesis encoder and its identity surface are owned by the later
identity-attestation RFC. The old ULID audit is superseded by fixed-width
32-byte content-hash EntityIds.

### A2. Frozen genesis encoder + self-certification -- DELETED (rev 4)

core/src/schema/genesis.rs (the frozen encoder, FrozenValue,
validate_catalog_genesis) and its golden/tamper suites are deleted with
derivation. Catalog entities are created via ORDINARY events through the
normal commit machinery; there is no catalog-specific byte surface to
freeze. Receivers apply the shared genesis validation from the
identity-attestation RFC, while this RFC's single-allocator authority still
decides the name-keyed upsert.

### A3. Creation inside the executor (rev 4, was "Create-with-derived-id")

The known-id create path threaded through the phantom-entity guard
(Transaction::create recording, core/src/transaction.rs:62-72; enforced
in commit_local_trx, core/src/context.rs:84-98) survives conceptually, but
the registration executor now creates an ordinary genesis and takes its
derived content-hash EntityId. No public API.

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
  authoritative lookup state; create an ordinary genesis on miss and use
  its content-hash EntityId; on hit, emit head-parented follow-ups only
  where the requested metadata differs (decision 18's machinery,
  unchanged).
- Apply rename hints BEFORE lookup-or-create, guarded (RFC 5.8): only
  when the current-name lookup misses and the hinted lookup hits; a
  no-op otherwise. The hint write is an ordinary follow-up.
- Build the resolution plan (creates / metadata updates / resolved
  no-ops) and submit it to `check_schema_registration` (decision 26)
  BEFORE emitting anything; refusal fails the whole request.
- Explicit-id bindings (RFC 5.9): look up the entity, require the exact
  backend and a mutually castable value_type, hard-fail on absence or
  incompatibility, and mint only the membership. The response carries the
  stored canonical pair.
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

Client side: on first mutating use of model M (create/edit) or first
predicate-resolution use (fetch/query/subscribe), ensure registration:
check the local map, issue RegisterSchema when the binding is absent or
doubtful, AWAIT the response, and upsert the returned definitions into the
CatalogManager map immediately on ack, so canonical id-keyed writes and
resolved predicates proceed behind the same binding. Direct entity-id gets
are the exception: they only record the compiled schema for a possible later
edit and load the entity; they do not mint identity. Explicit
`ctx.register::<M>().await` issues eagerly. OFFLINE (rev 4 ruling):
create/commit may proceed only when every field in the exact compiled schema
already has a compatible canonical binding in the local catalog. A merely
known collection, a missing field, or an incompatible declaration fails loud
("connect once first" when no durable allocator is reachable). The
pending_registrations queue and `drain_pending` are DELETED; failed or denied
registration creates no new user `Name` residue.

Tests: upsert idempotency (register twice -> same ids, zero events);
rename-hint application and its no-op guard; retype mints a distinct id
(lookup key includes the type pair) (SUPERSEDED 2026-07-10, decision 30:
the test now pins castable-retype-reuses-identity and the non-castable
refusal); policy denial refuses cleanly; the
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

- `CatalogMap` in core: by (minting model, current name) -> property id;
  by property id -> canonical definition; by model id -> membership set
  (the contract); by collection -> model id; display-name index per
  collection for resolution (A9). Backend and value_type are compatibility
  inputs on a lookup hit, never identity inputs.
- Warmed by three predicate-True subscriptions at system-ready, ordinary
  LiveQuery machinery (the catalog deliberately does NOT ride the
  Presence handshake the way the system collection does,
  core/src/node.rs:245-258), and fed IMMEDIATELY by SchemaRegistered
  responses (rev 4): ensure_registered upserts the returned definitions
  on ack, ahead of reactor delivery.
- `wait_catalog_ready` gate analogous to wait_system_ready
  (core/src/system.rs:97-101): consumers with no compiled schema defer
  resolution until the initial snapshot lands.
- `hard_reset` flushes the catalog map and every cached registration state
  along with what it already clears (core/src/system.rs:208-234):
  allocated ids belong to one system and must not leak across a root
  change.
- Tests: map warms from a peer with existing catalog; updates arrive via
  subscription; response-fed upsert observed ahead of the subscription;
  hard_reset flush verified.

### A8. Uniform PropertyKey wire and state (RFC 5.5)

- Every backend receives `PropertyKey::{Id(EntityId), Name(String)}` from
  the catalog-aware caller. Backends hold no schema binding or wire mode.
- LWW diff v2 and state 0xA2 each carry one tagged
  `BTreeMap<PropertyKey, ...>`. Legacy diff v1, state 0xA1, and pre-0.9
  buffers decode their string keys as `Name`; every save emits the current
  tagged form.
- Registered user commits resolve staged field names to `Id` before event
  generation. System/catalog fields and unresolved legacy residue remain
  `Name`. A present Id entry, including a tombstone, is authoritative; only
  an absent Id may fall back to Name residue.
- Unknown property ids apply and persist opaquely. Projection cannot name
  them until the catalog arrives; it never substitutes same-named data from
  another id.
- Yrs uses the same key contract for root names. New registered fields use
  id-named roots. Existing legacy name-root history cannot be re-keyed, so
  reads, edits, and observers continue using that root until an id root
  actually exists.
- Tests pin current round trips, legacy decoding and rewrite, unknown-id
  preservation, Id-over-Name authority, and legacy Yrs editing/observation.

### A9. ankql Identifier and the resolution pass (RFC 5.3, AC4, AC5)

- New AST node (resolved form; `PathExpr` stays the parse-time form):

```rust
pub struct Identifier { pub property: EntityId, pub name: String,
                        pub subpath: Vec<String> }
```

- A resolution pass Selection -> resolved Selection binds steps[0] against
  the catalog, leaves the rest as subpath, canonicalizes comparison literals,
  and fails closed with UnknownProperty naming collection and property (AC5).
  The existing TypeResolver remains complementary for JSON subpath literals;
  catalog resolution runs first and TypeResolver follows at the query origins.
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

Rule ladder, implemented where property projection happens:

1. Resolve the compiled display name to its property id; an unknown query
   reference fails closed with `UnknownProperty`.
2. An id-keyed entry is authoritative, including a cleared tombstone. Only
   an absent id may read legacy Name residue.
3. Typed View getters cast the canonical value to the compiled type, then
   apply compiled optionality and required defaults. A value that cannot be
   projected returns `PropertyError::NonCastable`; required `entityid` and
   custom types still have no fabricable default.
4. Predicate evaluation treats an absent or uncastable stored value as NULL
   with a warning. There are no sibling or foreign-data gates at read time.

Zero-op creation events: creation with an empty operation set generates
and persists an event instead of being skipped (EventId hashes fine over
empty operations, proto/src/data.rs:18-24). Un-ignores
tests/tests/yrs_backend.rs:303-332 (test_sequential_text_operations,
"blocked on #236"); integration test: create with empty string, reload,
read back "".

### A11. Derive macro, attributes, lifecycle glue

- The macro (derive/src/model/) emits a static ModelSchema: the
  local compiled schema (ModelSchema/FieldSchema per the section-7
  reconciliation), with per-field (name, backend, value_type) from the
  NORMATIVE mapping table (RFC 4), plus `target_collection` for
  `Ref<T>`/`Option<Ref<T>>`; ephemeral fields excluded
  (derive/src/model/description.rs:31-40). No anchor field (rev 4).
- Attributes: `#[property(renamed_from = "...")]` (the rename hint,
  RFC 5.8; ruled 2026-07-06), `#[property(id = "...")]` and
  `#[model(id = "...")]` (explicit binding, RFC 5.9; parse and validate
  id format at compile time; verification and membership minting happen
  at registration; binding keeps working unchanged beside the hint).
- `_ankurah_` collection prefix -> compile error at derive time.
- Registration triggers threaded through context paths: mutating use and
  predicate fetch/query/subscribe auto-assert while resolving; direct id gets
  only cache and load; `ctx.register::<M>()` asserts eagerly.
- Tests: descriptor snapshot for a representative model (every table
  row); rename-hint application and no-op guard; reserved prefix fails
  compile (trybuild or equivalent); shared property by explicit id
  readable from two contracts with differing optionality; a same-backend,
  mutually castable retype reuses the property id and returns its canonical
  type; incompatible declarations fail without writes.

### A12. Errors, guards, audits (cross-cutting)

- Current property errors: `UnknownProperty` for fail-closed resolution and
  `NonCastable` for a per-value write or typed projection failure.
  Registration refusals cover non-castable declarations, explicit-id
  mismatch or absence, policy denial, and the strict never-registered-offline
  create/commit path. `TypeSkew`, CatalogGenesisError, and the anchor-reuse
  refusal are deleted.
- PROTECTED_COLLECTIONS readers (with A4/A6); hard_reset flush (with
  A7). The later identity-attestation RFC owns the 32-byte content-hash
  EntityId migration and timestamp-locality audit.
- Docs: brief internals note under docs/internals/ once shapes settle
  (the schema-evolution book chapter waits on #291).

## Phase A protocol bump

PR #307 ships the whole epoch as PROTOCOL_VERSION = 3: LWW v2/0xA2,
uniform PropertyKey payloads, resolved predicate Identifiers and ORDER BY
property identities, RegisterSchema, model-id data envelopes, and
once-per-connection schema descriptors. Durable nodes upgrade first. Protocol
2 existed only as an intermediate branch value; no released or mergeable v2
epoch exists.

## Phase C outline (engine-local; detailed after A)

- sled: property_config keyed by property entity id bytes instead of
  name bytes (storage/sled/src/property.rs:32-63); u32 compaction and
  row format untouched.
- postgres/sqlite: columns stay human-named; EACH ENGINE persists its
  own column <-> property-definition-id binding (the sled
  property_config precedent generalized), because names are mutable
  and collisions resolve engine-locally; renames become ALTER TABLE
  RENAME COLUMN driven by catalog changes; display-name collisions
  among distinct property ids, including explicitly shared bindings,
  keep the first claimant bare and suffix newcomers
  from the property id (`name` / `name_bb`; RFC 5.5, mental model
  ratified 2026-07-06 -- materialization is indexing strategy plus
  admin legibility, never the data model). Discharges the TODO at
  storage/postgres/src/lib.rs:368-373. Keep canonical-write vs
  materialization-write seams clean inside set_state
  (storage/postgres/src/lib.rs:342, materialization loop at 364-391)
  per RFC 4a; #304 consumes that cleanliness.
- IndexedDB: name-bound via catalog, lazy re-materialization on rename
  (storage/indexeddb-wasm/src/collection.rs:73-81).

AMENDED (#289, 2026-07-09): the durable column maps, collision suffixes,
and the sled/IndexedDB keying above landed early on PR #307 (decision 28).
What remains in Phase C: rename DDL (ALTER TABLE RENAME COLUMN driven by
catalog changes), rename-driven IndexedDB re-materialization, and the
model <-> table map half deferred with #304.

## Landing strategy

- The standalone Phase 0 PR did not merge. Presence negotiation and Phase A
  are folded into `model-property-metadata` and release together as protocol
  v3, with red-to-green commits where tests pin each work package.
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
2. **Presence field appended last** for the original metadata transition, so
   an ephemeral old encoding is a prefix of the new one and detectable by EOF.
   Durable old encodings also carry the pre-v3 nested EntityState shape and are
   classified by an exact mirrored legacy decoder; trailing bytes distinguish
   versioned v1/v2 peers. Protocol v6 supersedes that compatibility shape with
   a symmetric challenge, stable v5 outer discriminants, and exact bounded
   decoding, so a v5 Presence cannot be reinterpreted as a v6 challenge.
3. **PROTOCOL_VERSION is u32; the released metadata epoch is v3 and the
   identity-attestation epoch is v6.** Pre-versioning binaries are implicitly
   version 0. Protocol 2 was an intermediate branch value before model-id
   envelopes joined the metadata epoch.
4. **One tagged PropertyKey map** in LWW diff v2/state 0xA2. `Id` and
   lossless legacy `Name` residue coexist without a parallel-map shape.
5. **Uniform PropertyKey::{Id, Name}** across every backend, with no
   per-instance keying mode, schema binding, or wire mode.
6. **Required `entityid` has no default**: absent stays Missing rather
   than fabricating a reference (RFC 5.4 lists defaults only for
   scalars).
7. **Field broadcasts key by the same PropertyKey as the value/root.**
   Registered fields observe Id keys; legacy Yrs history continues observing
   its Name root until an Id root exists.
8. **Registration descriptors reference by collection string + property
   name** within the request (rev 4; was "+ anchor"); the executor
   allocates or resolves all ids (except explicit-id bindings, which
   carry the literal id). Target-model references travel as collection
   strings, resolved executor-side.
9. **Phase 0 folded into PR #307.** The standalone PR closed unmerged; the
   handshake and metadata wire changes ship as one v3 epoch.
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
    AMENDED (review correction, #289, 2026-07-10): the pg/sqlite/idb overrides
    became load-bearing under the model-id envelope swap -- a durable restart on
    those engines warmed an EMPTY catalog and lost model routing -- so they are
    now implemented: non-creating discovery via the state / `{name}_event` table
    pairing (SQL) and the distinct `__collection` values over the compound index
    (idb). The warm now propagates a listing error instead of treating it as
    "no collections exist" (start still latches readiness on failure, so a warm
    error rejects loudly rather than hanging).
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
    AMENDED (PropertyKey amendment, #289, 2026-07-07): the display-name hint is
    WITHDRAWN from the property backend entirely. The committed entry drops its
    `name` field; the backend stores `{value, event_id}` only. Schema-blind
    engine materialization (the A-to-C window the hint served) becomes a
    catalog-side concern tracked with the engine work on #312. See decision 27.
14. **Predicate-level required-defaults are out of Phase A** (RFC 5.4
    scoping): resolved-identifier predicate evaluation treats an absent
    property as NULL uniformly (IsNull matches, comparisons false),
    which unifies the three historical missing-property behaviors;
    rule 3's type default applies in the compiled View getters (which
    know required-ness at compile time).
    Consulting per-membership optionality inside the filter is deferred
    with the Filterable follow-ups in tasks.md.
15. **Cross-root state transplant is unsupported** (maintainer ruling
    2026-07-05: "different roots means different systems"): a BOUND
    checked read has no foreign-id fallback; a same-display-name value
    under an unresolvable id fails visible as TypeSkew. Display-name
    hints are engine-projection only and never route writes or bound
    reads (the schema-blind projection regime is decision 17).
    AMENDED (canonical value_type ruling, #289, 2026-07-10): the READ-SIDE
    half of this decision (the foreign-data gate on bound getters) is
    VETOED and removed with the other read-time gates (decision 31). The
    transplant stance itself stands: cross-system data is refused at the
    wire ingress by the unknown-model-id rejection, and an out-of-band
    copied buffer now reads as ABSENT (never substituted) rather than
    erroring per-read.
16. **Commit-time registration closes the edit-only gap**: the sync
    edit path cannot await a durable registration, so each transaction records
    the exact compiled schema shapes it uses and commit_local_trx
    ensure-registers those shapes. Process-global compiled-schema history is
    never replayed into an unrelated transaction. AMENDED by rev 4: for a collection
    with NO binding at all (never registered on this system), a failed
    or impossible registration at create/commit is the STRICT
    never-registered-offline error, failing the write; for an
    already-bound collection, a denied or unreachable re-assert is deferrable
    only when every field in that compiled schema already resolves to a
    compatible canonical definition. A missing or incompatible field fails
    the write; new Name residue is never emitted from a registered user
    collection. `ctx.register::<M>()` remains the eager strict form.
    EXTENDED (PropertyKey amendment, #289, 2026-07-07): commit_local_trx now
    also RESOLVES each entity's transient uncommitted `Name` keys to their `Id`
    at this same point, after ensure-registration and before the commit event.
    Ordinary sync accessors stage a name-keyed value; an explicit-id accessor
    carries and stages its literal id directly, even when its local field name
    differs from the catalog display name. Both paths canonicalize values at
    commit, so registered-user committed and wire forms are always id-keyed.
    See decision 27.
17. **SUPERSEDED: schema-blind checked-read projection** (pre-PR review
    finding, 2026-07-05; withdrawn by decisions 27 and 31). Display-name
    hints, hint routing, sibling scans, and `TypeSkew` were removed before
    release. Engine post-filter and policy inspection bind a
    `TemporaryEntity` to the catalog when they need name resolution;
    unbound inspection reads only actual Name residue and never guesses which
    id owns a display name.
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
    re-keying); shipped-type changes are retypes by definition
    (AMENDED 2026-07-10, decision 30: a changed VALUE_TYPE no longer
    forks an identity -- it is admitted through the canonical-type
    compatibility gate when mutually castable, refused otherwise).
    Rejected: keeping the always-string assumption (misdeclares
    hand impls, and retro-fitting the const post-release would re-key
    every custom property that then declared a non-string type);
    a required (defaultless) const (breaks every downstream hand
    impl for marginal gain -- the default is correct for the
    JSON-catch-all convention the ecosystem actually uses). AMENDED by
    decision 30: VALUE_TYPE is a registration compatibility declaration,
    not a lookup-key or hash input; the found property's canonical type is
    fixed at first allocation.
20. **Upsert executor under a process-local mutex** (rev 4, RFC 5.1):
    the whole RegisterSchema execution serializes on one async mutex;
    ids are allocated by emitting an ordinary genesis and using its full
    content hash; the executor's lookup
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
    define schema) -- and the write fails because the stale field has no
    compatible canonical binding. The policy surface must support
    BOTH discrimination axes (RFC 5.2, 5.7).
25b. **First-use registration on predicate-query paths; unresolvable references
    fail loud** (rev 4 corollary, RFC 5.2/5.3; surfaced by the
    subscribe-before-create tests, REVISED 2026-07-06 by the
    replica-lag flake -- REN 2 second ruling, ratified in session;
    fail-loud corollary CONFIRMED by the maintainer 2026-07-07,
    recorded on #289): a
    warm catalog REPLICA cannot prove a collection unregistered (it may
    lag the authority by a subscription hop), so resolution never
    renders that verdict alone. When the warm map cannot resolve a
    reference and the binary carries a compiled schema, the model
    REGISTERS AT FIRST USE via the ordinary idempotent upsert: an
    existing schema resolves to a no-op plan (zero events, policy verb
    skipped) whose response feeds the map synchronously, so predicate reads warm
    against authoritative rows and query_wait initializes populated.
    When registration cannot run (policy denial, no durable peer), the
    reference FAILS LOUD as UnregisteredCollection on fetch and live
    query alike (second same-day ruling: the catalog subscription is a
    CACHE -- an accelerator and offline enabler, run cached: true --
    and registration is the sole doubt-resolver; a lagging replica
    cannot prove emptiness, and the earlier draft's Predicate::False
    placeholder subscription answered a question nobody asked). The
    placeholder and wait_collection_registered deferral machinery are
    deleted. The resolution path still KICKS the ephemeral catalog
    subscription inline when a sync-context node never subscribed
    (ensure_subscribed is idempotent and awaited), and fails closed
    when neither the kick nor registration can warm the catalog
    (offline). Fail-closed is unchanged for unknown properties in
    registered collections and for references no compiled schema
    anticipates (AC5). PropertyError carries UnregisteredCollection as
    the loud, actionable variant.
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
    Refusal fails the whole registration before anything is emitted;
    check_event still gates every emitted event underneath (defense in
    depth) -- individually, not transactionally: a mid-batch event
    denial leaves earlier catalog events durable (maintainer ruling
    2026-07-06: registration need not be atomic; the allocator's
    storage-checked lookups keep identity convergent across partials;
    #313 tracks the transactional upgrade). The executor also requires
    can_access_collection on every collection a request names (second
    2026-07-06 ruling: the verb-skipped no-op upsert must not serve as
    an existence oracle). Sync, matching check_event.
    Registration-scoped: the broader per-property policy surface stays
    deferred to the #264/#274 consolidated design.
27. **The PropertyKey backend contract** (the PropertyKey amendment, #289,
    2026-07-07; supersedes the rev-4 SchemaBinding / WireMode data-plane
    machinery and RFC 5.5). Every property backend keys by a uniform
    `enum PropertyKey { Id(EntityId), Name(String) }`
    (core/src/property/mod.rs). The backend is a dumb PropertyKey-keyed store: no
    binding, no per-instance wire mode, no display-name hint. Name-to-id
    resolution moves to the catalog-aware write path -- `commit_local_trx`
    re-keys an LWW entity's transient `Name` keys to `Id` after
    ensure-registration and before the commit event (LWW resolves at commit);
    the yrs write accessor resolves at write time because yrs cannot re-key CRDT
    history at commit. Reads use MAP-LEVEL presence: an `Id` entry present, even
    a cleared `None` tombstone, is authoritative and never falls back to a stale
    `Name` value; only an absent id consults the bare `Name` residue. The
    catalog-blind `Entity` carries a stamped `Weak<dyn PropertyResolver>` (the
    live catalog) for the sync read path, replacing the old `Node::bind_entity`
    backend flip. INVARIANT: writes are always id-keyed for a registered user
    collection; a `Name` key is emitted only for system and catalog collections
    and for legacy decode. yrs is uniform (id-named roots), overriding the RFC
    5.5 Phase C "yrs roots stay name-keyed" decision. The wire is one tagged map
    (the leading version byte still decodes legacy 0xA1 and pre-0.9 buffers, but
    only the current tagged v2/0xA2 form is emitted). ERRATA: LWW leaves a stale `Name` entry
    in place, safely shadowed by its `Id` entry (cleanup in an event-sourced
    backend is a convergence-sensitive tombstone event, not a local delete); yrs
    cannot tombstone (empty Y.Text equals None, erratum 1), so a cleared
    pre-epoch yrs field may resurrect a stale name value on a migrated old
    database (accepted, pre-1.0 development data only). Layering: the schema
    layer no longer imports a binding from a property backend, and node.rs no
    longer references any concrete backend type. Amends decisions 13 and 16;
    full rationale on #289.
    AMENDED (read-dispatch amendment, #289, 2026-07-09 and canonical-type
    ruling, 2026-07-10): backend-specific checked reads and every read-time
    gate are deleted. `PropertyBackend::entry` exposes three-way presence,
    `restage` supports commit-time canonicalization, and generic
    `property::read_resolved` performs Id-then-legacy-Name dispatch. A present
    Id entry, even a tombstone, never falls back. Typed getters report only a
    per-value `NonCastable` projection failure; predicate evaluation maps
    absent or uncastable values to NULL. Cross-system state is refused by
    model id at wire ingress, while an out-of-band copied foreign id reads as
    absence and never substitutes for the local property.
28. **Engine-owned durable property-id -> column maps, landed with Phase A**
    (engine column-map amendment, #289, 2026-07-09; implemented on PR #307,
    accelerating the Phase C column-binding bullet). Root cause: with
    display-name hints withdrawn (decision 13 amendment) the engines
    materialized columns named by the only stable thing they had, the
    property id, while queries addressed human names -- a systemic read
    regression. Resolution, per the ratified engine-ownership principle
    (each storage engine alone owns and remembers its internal id <-> name
    mappings; neither Node, System, nor CatalogManager bears it): every
    engine persists a property-id -> column map, seeded from an injected
    resolver (StorageEngine::set_property_resolver, wired at Node::build)
    at set_state and consulted on every read. Postgres/sqlite:
    ankurah_property_columns table, UNIQUE(collection, column_name),
    insert-if-absent plus winner read-back so concurrent claimants
    converge. Sled: property_columns tree under an assignment lock.
    IndexedDB: property_columns object store, assigned in a pre-pass
    before the entities transaction (IndexedDB auto-commits across foreign
    awaits). Naming rules (core/src/storage.rs naming module): sanitized
    display name; collisions append the trailing 4+ characters of an
    identifier-safe, injective base32 encoding of the property id, widening
    through all 256 bits until unique; unresolvable names use the same token
    behind a synthetic prefix, loudly (never expected to fire). Names remain
    within PostgreSQL's 63-byte identifier limit. Bare property
    ids never appear as column names; collisions DEDUPE rather than
    hard-error (hard-error is unmergeable: retype is a supported flow).
    Reads: fetch_states translates the resolved Selection into engine
    column space once at its top (selection_to_column_space; order-by ids go
    straight through the engine map, with name resolution as a raw/legacy
    fallback); residual bare Paths reject at the
    resolution pass (AC5), deliberately not at the engine seam
    (system-collection storage queries legitimately run name-addressed).
    Table creation and column creation timing are UNCHANGED (tables on
    first need, columns on first write). Envelope stamping is LAZY: model
    id resolution is checked on the first hydrated row, never up front, so
    an empty scan on a cold catalog returns empty instead of erroring (the
    ephemeral known_matches pre-fetch against a never-stored collection),
    and an absent-entity get_state surfaces EntityNotFound, never a
    model-resolution error (get_retrieve_or_create relies on that
    fallthrough). Rename DDL stays Phase C. The
    model <-> table half of the bidirectional-map mandate is DEFERRED with
    #304 (buckets stay collection-string-keyed until storage APIs are
    keyed by model) -- flagged for maintainer veto.
29. **The model-id wire envelope, well-known model ids, and descriptor
    shipping** (model-id envelope amendment, #289, 2026-07-09; implemented
    on PR #307; PROTOCOL_VERSION = 3). Event, EntityState, EntityDelta,
    and SubscriptionUpdateItem carry `model: EntityId` in place of
    `collection: CollectionId`; EventId already excluded collection from
    identity, so event ids are unchanged. Collection strings survive only
    in query/API surfaces and registration payloads (collection is the
    data there, #305). System and catalog collections use well-known model
    ids (domain-hashed virtual identities under
    `ankurah.well-known-model.v0`; core/src/schema/mod.rs); the receiver-side protection guard
    (A6) keys on them. Ingress: Node::resolve_model maps model id ->
    collection via well-knowns then the catalog; unknown model ids REJECT
    loud and retryable, never synthesize a collection. Cold-catalog
    policy is maintainer-ruled c + e + d: (c) NodeUpdate and NodeResponse
    carry `#[serde(default)] schema: Vec<StateWithGenesis>` -- the real
    attested catalog entity states plus their exact self-certifying genesis
    events for any non-well-known model the
    payload references, shipped once per connection per model
    (PeerState.announced_models), policy-validated and ingested by the
    receiver BEFORE body processing. Non-empty batches are founder-only,
    strict, and atomic; definitions never ride inside events
    or state buffers. (e) Catalog warmth participates in readiness via
    the existing ensure_subscribed / context_async gates, and on the
    INGRESS side via resolve_model_wait: a durable node whose startup
    storage warm has not finished awaits catalog readiness once and
    retries before rejecting (a restarted node receiving traffic
    immediately must not reject models it allocated pre-restart; the warm
    task latches readiness even on a failed scan so the wait is bounded).
    Ephemeral ingress never waits -- definitions arrive inline, so a miss
    is final. (d) The
    decision-28 fallback naming stays as the loud never-fired net.
    Storage engines stamp reconstructed envelopes via a shared
    bucket_model_id helper (well-knowns -> resolver -> loud retrieval
    error). Two policy-surface changes, both forced by the same fact (raw
    state is now id-keyed, so hooks must receive what they need to
    resolve): check_read_event gains a collection parameter (core
    resolves, agents keep authorizing by collection), and check_read gains
    the node's catalog resolver (Option<Weak<dyn PropertyResolver>>) --
    an agent that evaluates NAME-addressed policy predicates against
    entity state (scope filters) must bind its inspection view through it
    (TemporaryEntity::new_bound; the unbound TemporaryEntity remains the
    schema-blind engine post-filter tier), or every display name resolves
    to nothing on a post-epoch buffer and scope enforcement denies
    everything. This is the policy-inspection half of what decision 17's
    display-name-hint projection used to provide; the engine-projection
    half stays with #312. Entity unification (1-M
    entity <-> model, membership vec, single entity store) is the
    direction this preserves but is expressly DEFERRED: #330, with #305
    and #312 comments recording the boundary.
30. **Canonical value_type; registration is a compatibility check, never a
    mutation** (maintainer ruling in session, 2026-07-10; recorded on #289
    and #303; rfc.md 5.1/5.6 amendments). Backend and value_type leave the
    property lookup key -- the executor looks up by (model, name) in both
    the catalog map and the storage double-check -- and a property's
    (backend, value_type) is CANONICAL, fixed at first allocation. A hit
    declaring a different value_type never forks a second identity and
    never updates the definition: a mutually castable pair (per
    Value::cast_to; type-level twin ValueType::castable_to, kept in
    lockstep by test) admits the binary with a drift warning, anything
    else refuses loudly (`RegistrationError::NonCastable`), and a
    backend change always refuses. The SchemaRegistered response carries
    the CANONICAL (backend, value_type) -- the requester's cast target --
    on hits and explicit-id bindings alike, and explicit-id verification
    admits the same castable drift. The cast discipline: commit-time
    canonicalization at the Name->Id rekey choke point (a value the
    canonical type cannot represent fails that writer's commit); the bound
    getter casts canonical->compiled (per-value failure =
    `PropertyError::NonCastable`); the resolution pass casts comparison literals opposite a
    resolved whole-property reference (subpaths keep the TypeResolver Json
    heuristic; a literal the canonical type cannot represent fails the
    query loud at origin); the entity read/evaluation boundary
    (`read_lenient` / `value_resolved`) defensively canonicalizes, reading
    junk as NULL with a warning (decision 14's absent-as-NULL precedent),
    which keeps the reactor's byte-collated watcher index and predicate
    evaluation in one type. Motivation: exactly one stored type per
    property makes engine columns and index collation consistent (the
    lesson behind the old hardcoded-string collation). A struct-level
    retype is durably INERT; changing a canonical type is a deliberate
    migration operation, spilled to #303. One live property per (model,
    name) becomes an allocator invariant (CatalogMapInner::resolve warns
    if a map ever holds two). Supersedes the fork-identity semantics of
    rev 4's 5.6 and decision 19's "changing VALUE_TYPE is a retype" note.
31. **Read-time gates removed; backends addressed only through the
    PropertyBackend trait** (maintainer ruling in session, 2026-07-10,
    same thread as decision 30; rfc.md 5.4 rule-4 amendment). The sibling
    gate and the foreign-data gate are deleted from every read path:
    registration is the sole type/identity admission point. Operations that
    require catalog resolution register first; a direct id get does not, and
    neither path repeats compatibility checks while reading. TypeSkew is
    deleted with the gates (the surviving read-time failure is
    the per-value PropertyError::NonCastable projection error, renamed
    from CastError; the registration refusal is
    RegistrationError::NonCastable). Cross-system data is refused at wire
    ingress by the unknown-model-id rejection; an out-of-band copied
    buffer reads as absent, never substituted (decision 15's read-side
    half vetoed). Mechanically: PropertyBackend gains `entry` (three-way
    presence: absent / cleared tombstone / value, the primitive the
    anti-resurrection rule needs) and `restage` (commit-time
    canonicalization hook), both with defaults suiting tombstone-less
    backends; the dispatch collapses to the generic
    `property::read_resolved`; every by-name backend touch and downcast
    in generic core (entity.rs read paths, resolve_pending_keys) is
    removed -- LWW is an ordinary registered backend with no special
    status in core, and `backend_from_string` is the one sanctioned
    by-name seam (the registry constructor). PropertyResolver::siblings
    and the entity's catalog_knows_id helper die with the gates;
    Filterable::value_checked becomes the infallible value_resolved.
