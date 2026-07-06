# RFC: model and property metadata (defining entity per model and per property)

Status: RATIFIED (maintainer sign-off 2026-07-05, recorded on #289):
direction, all nine axes, and the rev-2 contract/membership model stand;
implementation ladder (plan.md/tasks.md) is the next deliverable.
Rev 3 (maintainer direction, same day): registration becomes a protocol
operation executed by durable nodes; catalog protection is enforced
receiver-side and structurally; the data contract is id-keyed from the
FIRST landing, which promotes #294 to Phase 0 of the ladder (sections 4,
5.2, 5.3, 5.5, 9). Originally: DRAFT for review on #289. Scope agreed 2026-07-05: each model gets its
own defining entity id, and each property gets its own defining entity id;
model and property metadata become first-class, replicated entities rather
than compile-time-only facts. This document decides the design axes and
proposes a phased landing order. Implementation is expressly out of scope
until the direction is signed off on #289.

Medium and convention: #289 asks for "a design doc in specs/ following the
phase-2 RFC conventions", and the phase-2 convention files strategy in RFC
issues. This document is the design doc in specs/; the decision summary is
posted to #289 for ratification there; per-phase implementation RFC issues
(and this directory's plan.md/tasks.md) follow after sign-off, matching the
spec/plan/tasks layout used by specs/property-registration and jwt-auth.

Requirements baseline: issue #85's acceptance criteria are treated as
requirements; where this design renegotiates one, the renegotiation is called
out inline and collected in section 10. Prior art: PR #236
(`specs/property-registration/{spec,plan,tasks}.md` on branch
`fix/175-empty-string-missing-property`), which this RFC absorbs and, in
three places, deliberately diverges from; and the in-repo schema-registry
plan at specs/unified-refs-edges-json/phase-3-schema.md, reconciled in
section 7. All code citations are against main at 05593d0d (0.9.0). This
draft has been through an adversarial review pass (three independent
reviewers: code-reality, distributed-behavior, requirements-coherence); their
surviving findings are integrated below, most visibly in 5.1's frozen
encoder, 5.4's sibling gate, and 5.8's anchor-reuse guard.

## 1. Problem statement

A property in ankurah today IS its name string, and a collection IS its name
string. There is no stable identity behind either. The inventory of what
keys on those names (the name-as-identity register):

- `PropertyName = String` is the sole property key type
  (core/src/property/mod.rs:12). Backend value maps are
  `BTreeMap<PropertyName, ...>` (core/src/property/backend/lww.rs:68-73), and
  field-change broadcasts are keyed by name in both backends
  (core/src/property/backend/lww.rs:72, core/src/property/backend/yrs.rs:24).
- Property names ride inside the opaque per-backend byte payloads: LWW
  operation diffs and state buffers serialize `BTreeMap<PropertyName, ...>`
  (core/src/property/backend/lww.rs:187-207, 136-152); Yrs stores each
  property as a Text root named by the property name inside the yrs update
  encoding (core/src/property/backend/yrs.rs:44-49). The proto layer never
  sees property names: `Operation { diff: Vec<u8> }` and
  `OperationSet(BTreeMap<String, Vec<Operation>>)` are keyed by backend name
  only ("lww"/"yrs") (proto/src/data.rs:156-181).
- Entity property lookup scans backends for a name match; the first backend
  that has the name wins, so a name collision across backends is silently
  ambiguous (core/src/entity.rs:552-563).
- Query ASTs reference properties as raw strings:
  `PathExpr { steps: Vec<String> }` (ankql/src/ast.rs:53-70). Fetch and
  SubscribeQuery carry the parsed `ast::Selection` on the wire
  (proto/src/request.rs:119-128). Nothing anywhere validates a property
  reference against a schema; the parser accepts any identifier
  (ankql/src/parser.rs:30-66).
- Postgres and sqlite create columns named by property name, on demand,
  inside set_state, and only when a value is present
  (storage/postgres/src/lib.rs:365-391, storage/sqlite/src/engine.rs:288-294,
  with the ALTER TABLE executors at postgres lib.rs:302-304 and sqlite
  engine.rs:239-241). Postgres carries a TODO wanting property ids to
  disambiguate same-named properties across backends
  (storage/postgres/src/lib.rs:368-373).
- Sled already compacts property names to engine-local u32 ids
  (storage/sled/src/property.rs:32-63); its materialized rows store
  `(u32, Value)` pairs, proving the name is not load-bearing below the
  materialization boundary (storage/sled/src/collection.rs:112-123).
- Collections: `CollectionId(String)` with no validation and no reserved
  prefix enforcement (proto/src/collection.rs:3-23); the collection id is the
  lowercased struct name (derive/src/model/description.rs:50); renaming a
  Rust struct silently re-homes the model to a fresh collection.
  `PROTECTED_COLLECTIONS` exists but is read by no code at all
  (core/src/system.rs:21).
- The TypeResolver that infers JSON semantics from path arity carries the
  comment "TODO(Phase 3): Replace heuristics with proper schema lookup from
  System tables" (core/src/type_resolver.rs:24-26), pointing at the
  schema-registry plan reconciled in section 7.

Concrete failures and frictions this produces, from #289's case list, each
validated against code:

1. **The empty-string bug family (#175, #236).** Yrs tracks operations, not
   state: inserting "" into a Text root creates no operations, so
   `to_operations` returns None (core/src/property/backend/yrs.rs:155-168).
   The root never materializes; `property_values()` omits the field; the
   Postgres column is never created; and the View getter errors
   `PropertyError::Missing` on read (core/src/property/value/yrs.rs:88-95).
   In the extreme case where every field of a create is an empty string, no
   creation event is generated at all and the entity is never persisted
   (tests/tests/yrs_backend.rs:300-305). The system cannot distinguish "set
   to empty" from "never existed" because there is no schema fact saying the
   property should exist.
2. **Undefined missing-property semantics.** Predicate evaluation returns a
   hard `Error::PropertyNotFound` for an absent property
   (core/src/selection/filter.rs:59-91); the reactor swallows that error into
   "does not match" via `unwrap_or(false)`
   (core/src/reactor/subscription_state.rs:391); the SQL engines rewrite
   references to unknown columns via ankql's `assume_null`
   (storage/postgres/src/lib.rs:528-548, mechanism in ankql/src/ast.rs:241-333).
   Three consumers, three different semantics, none schema-informed. A typo
   in a predicate is indistinguishable from an unpopulated property.
3. **SQL column identity.** Columns are keyed by name, created on first
   non-null value. A property rename orphans the old column and silently
   starts a new one; a name collision across backends is skipped by a
   `seen_properties` guard with an explicit TODO wanting property ids
   (storage/postgres/src/lib.rs:362-374).
4. **Cross-node merge-strategy agreement.** The merge strategy (LWW vs Yrs)
   is decided per field at compile time by the derive macro's backend
   registry: configs are consulted in first-refusal order with YrsString
   before LWW, yrs accepting exactly `^String$` and LWW accepting `.*`, so a
   bare String defaults to YrsString and everything else to LWW
   (derive/src/model/backend_registry.rs:16-25 for the ordering,
   backend_registry.rs:31-86 for resolution, patterns in
   core/src/property/value/{yrs,lww}.ron). Two nodes compiled with different
   `#[active_type]` choices for the same field name will write operations
   under different backends for the same property name; entity lookup then
   resolves the name to whichever backend answers first
   (core/src/entity.rs:552-563). The disagreement is silent and per-node.
5. **Per-field addressing has nothing to hang identity on.** Field signals
   are keyed by name string (core/src/property/backend/lww.rs:312-328,
   core/src/property/backend/yrs.rs:201-221); per-field subscription from a
   View is unsupported (tests/tests/basic.rs:51-54); policy has no
   per-property hooks: the finest granularity is per-entity and
   per-collection, and even the commented-out future hooks are per-event,
   per-collection, or per-node, none property-keyed
   (core/src/policy.rs:49-162, 155-161, 293-303); sled's index and property
   config are engine-local (storage/sled/src/database.rs:17-20).
6. **No introspection.** Which collections exist, which properties a
   collection has, and what types they carry are not queryable facts
   anywhere; collection existence is not even durably recorded as data
   (core/src/collectionset.rs:28-46, core/src/system.rs:83-91 TODO).

## 2. Requirements

From #85, treated as requirements (renegotiations marked "REN" and argued in
place):

- AC1: property definitions are entities in an `_ankurah_property`
  collection.
- AC2: definitions are upserted by default keyed by collection name +
  property name + type identifier, at usage time. (REN on trigger
  granularity and on read-path timing; sections 5.2 and 10.)
- AC3: entity lookup is built and maintained via a subscription.
- AC4: `ankql::ast::Identifier` carries the property entity id. (No
  `Identifier` node exists today; this RFC introduces it as the resolved
  form of `PathExpr`; section 5.3.)
- AC5: predicate building for properties no model defined fails (fail
  closed).
- Stretch: renames achievable by expressly referencing the property entity
  id on the model. (REN: reference the anchor name rather than the raw id;
  section 5.8.)

Direction added by the maintainer during #289 review (2026-07-05), treated
as requirements:

- Model definitions and property definitions are full-fledged entities in
  their own right (already the core of this design), with a lookup path by
  name AND an explicit binding path by known entity id via macro attribute,
  for models and for properties.
- Property backends record values under the property entity id, or a more
  space-efficient stand-in chosen per storage backend, rather than name
  strings (rev 3: wire encodings id-keyed from Phase A; engine-local
  stand-ins like sled's u32 compaction are the sanctioned pattern, rebound
  in Phase C).
- Properties are sharable between models, at least when addressed by
  explicit property id: a model is a data contract an entity is anticipated
  to meet; one entity can meet several contracts, and contracts can overlap
  (the unified-entity-storage trajectory; section 4a). Property identity
  must not be inseparable from one owning model.
- A path to a declarative DDL (unlike SQL's imperative DDL; possibly in
  ankql, possibly a separate surface) as an alternative to Rust structs as
  the definitive schema authority. Path required now; implementation not.

From the surrounding system, non-negotiable constraints:

- **No coordination requirement.** Registration must work offline-first: any
  node must be able to register a model without a round trip to a durable
  peer. Concurrent registration by independent nodes must converge without
  a canonicalization protocol.
- **Mixed fleets are the normal case.** Nodes running different model code
  versions (properties added, renamed, retyped) must not corrupt each other,
  and every wire or persisted format change must sequence behind #294
  (protocol version in the Presence handshake), which does not exist yet:
  Presence carries no version field (proto/src/peering.rs:5-10).
- **Bootstrap must not recurse.** Reading metadata entities must not require
  metadata entities.
- **Respect the #267 boundary.** The PropertyBackend trait, layer-view API,
  and conformance kit belong to #267. This RFC changes what identifies a
  property inside backend payloads, not the backend contract's shape.

## 3. Design overview

Three fixed-name catalog collections, `_ankurah_model`, `_ankurah_property`,
and `_ankurah_model_property` (contract membership), hold ordinary entities
whose properties are plain LWW values. Their entity
ids are minted deterministically from the system root and the natural key,
and their genesis events are produced by a frozen encoder, so any two nodes
that register the same thing mint byte-identical genesis events and converge
without coordination. Registration happens automatically at model first-use.
Resolution of a property reference consults the local compiled schema first
and the replicated catalog second, and fails closed when neither defines the
property. Registration travels as a dedicated protocol operation, not as
ordinary entity writes; durable nodes execute it after policy approval, and
catalog genesis events are self-certifying against the deterministic
derivation. The data contract is id-keyed from the FIRST landing: LWW
payloads and query identifiers carry property entity ids on the wire from
day one, which puts #294 (protocol version in the Presence handshake) at
the front of the ladder; engine-level binding (columns, rename DDL) and
tooling follow in later landings.

Two structural commitments shape the shapes below. First, a model is a DATA
CONTRACT: the model entity names a set of property entities it comprises,
via membership records, rather than owning its properties; properties are
independent entities that more than one model can include (by explicit id),
which is what the planned unified entity storage needs (section 4a).
Second, Rust structs are ONE binding to the catalog, not the definitive
schema: definitions can equally be authored declaratively (section 5.10)
or bound explicitly by id (section 5.9); the catalog entities themselves
are the definitive schema.

## 4. Catalog shape (where metadata entities live)

**Decision: dedicated fixed collections, per #85 AC1.** `_ankurah_model` and
`_ankurah_property`, ids constructed via `CollectionId::fixed_name` like the
system collection (core/src/system.rs:121). Metadata entities are ordinary
entities: LWW properties, normal events, normal state buffers, normal
subscriptions. Nothing about their storage or replication is special-cased.

Three catalog collections (the third is what makes models CONTRACTS rather
than owners; see the trajectory in 4a):

Property entity (`_ankurah_property`; standalone, shareable; see 5.1 for
which fields are identity-frozen):

- `name: String` (current display name; genesis writes the anchor name)
- `backend: String` (backend registry name, e.g. "lww", "yrs")
- `value_type: String` (language-agnostic value type, e.g. "string", "i64",
  mirroring core::value::ValueType's variants)
- `minted_for: EntityId` (the model entity in whose scope this property was
  first derived; PROVENANCE and derivation scope, expressly NOT ownership;
  a property bound by explicit id from another model is unaffected by it)
- `target_model: Option<EntityId>` (for reference-typed properties; mutable
  metadata, not identity)

Model entity (`_ankurah_model`; a named data contract):

- `collection: String` (the collection id this model's entities currently
  live in; the identity anchor for derivation. Under unified storage this
  demotes to a routing/view concern, 4a)
- `name: String` (display name, initially the struct name; mutable)

Membership entity (`_ankurah_model_property`; one per (model, property)
pair; the contract edge):

- `model: EntityId`
- `property: EntityId`
- `optional: bool` (PER CONTRACT: model A may require a property that model
  B, sharing it, treats as optional; this is why optionality lives here and
  not on the property)

Membership ids derive as f(model id, property id) (5.1), so concurrent
registration of different fields by nodes running different versions of one
model creates DISJOINT membership entities that union cleanly; a
membership list stored as one LWW vector on the model entity was considered
and rejected because concurrent adds would collide as a single register and
drop fields. Retiring a field is a tombstone flag on the membership (future
schema-evolution work), never deletion of the shared property entity.

**The Rust-type mapping is normative.** Because the derived property id
depends on (backend, value_type) (section 5.1), every node must map a given
Rust field type to the same descriptor pair, byte for byte. The mapping is
adopted from PR #236's plan.md table, restated against the actual active
types:

| Rust field type | backend | value_type | optional |
|---|---|---|---|
| `String` (default YrsString) | "yrs" | "string" | false |
| `Option<String>` (LWW; see errata) | "lww" | "string" | true |
| `#[active_type(LWW)] String` | "lww" | "string" | false |
| `LWW<i16> / <i32> / <i64>` | "lww" | "i16" / "i32" / "i64" | false |
| `LWW<f64>` | "lww" | "f64" | false |
| `LWW<bool>` | "lww" | "bool" | false |
| `LWW<Vec<u8>>` | "lww" | "binary" | false |
| `LWW<Json>` | "lww" | "json" | false |
| `Ref<T>` | "lww" | "entityid" | false |
| custom `Property` types | "lww" | `Property::VALUE_TYPE` (derive pins "string") | false |
| `Option<T>` of any above | same | same | true |

Errata (2026-07-05, implementation verification): #236's table said
`Option<String>` was yrs-backed; the shipped backend registry resolves ONLY
bare `String` to yrs (`accepts: "^String$"`, exactly as section 1 of this
RFC already describes) and `Option<String>` falls through to LWW's
catch-all. The row now records shipped behavior, which existing 0.9 data
already carries; changing the code to match the old row would have re-keyed
every deployed `Option<String>` field. RATIFIED by the maintainer
2026-07-05 with the deeper rationale: a yrs text CRDT cannot represent
`None` as distinct from the empty string (once a text root exists there is
no mergeable "unset" operation; deleting every character yields "", never
None), so an optional string REQUIRES a register that can hold an explicit
null -- which is exactly the LWW backend. LWW is not merely the shipped
fallback; it is the only backend with the right semantics for `Option`.
The second erratum (custom `Property` types) is RESOLVED by design change
(maintainer direction, 2026-07-05): rather than assuming "string" for
every custom type, the `Property` trait declares its own normative
value_type as an associated const (`Property::VALUE_TYPE`, default
"string"), which `#[derive(Model)]` reads at compile time for field types
outside the built-in table. `#[derive(Property)]` pins "string" explicitly
(matching its JSON-in-a-string serialization); a hand-written impl
producing another `Value` variant declares it (e.g. "i64"), and the
compiled schema, the registration request, the catalog, and the
property-id derivation all carry the declared type. The invariant is
documented on the const: VALUE_TYPE must equal the `Value` variant
`into_value` produces, and changing it for a shipped type is a retype
(a new property identity, RFC 5.8).

value_type strings are the lowercased core::value::ValueType variant names
(REN vs #236's plan.md prose, which listed a separate `ref` variant; the
PR's actual proto ValueType has EntityId and no ref, so references are
(value_type = "entityid", target_model = Some) here, following the diff over
the prose). The `optional` column feeds the MEMBERSHIP record, not the
property entity, and deliberately does NOT enter any identity key: making a
field Option<T> or not must not re-key the property or the membership. Future backends and
value types extend this table; extending it is a spec change, because two
nodes disagreeing on a mapping row mint different ids for the same field.

Rationale for dedicated collections over PR #236's `sys::Item` variants in
`_ankurah_system`:

1. **AC3 falls out for free.** The system collection does not replicate by
   subscription today; the root rides exclusively on the Presence handshake,
   consumed by ephemeral nodes joining a durable peer
   (proto/src/peering.rs:5-10, core/src/node.rs:245-258). Placing metadata
   there would mean extending the handshake or adding special replication.
   Dedicated collections use the ordinary peer-subscription path: each node
   subscribes to the three catalog collections with predicate True at
   system-ready, which is exactly AC3.
2. **Real properties are queryable and merge per-field.** `sys::Item` is a
   whole-enum JSON string inside a single LWW property "item"
   (core/src/system.rs:127, 269, 306-322): opaque to predicates, and
   concurrent updates to different conceptual fields of one item collide as
   one LWW register. Real LWW properties give
   `_ankurah_property WHERE model = {id}` introspection (a stated #289 case)
   and per-field merge of metadata updates.
3. **The catalog stays out of the root-discovery path.** load_system_catalog
   scans the whole system collection at startup
   (core/src/system.rs:246-303); hundreds of property items do not belong in
   that scan.

What we adopt from #236: the model and property entity kinds (extended
here with membership records so properties are contract-shareable), the
field vocabulary above, the language-agnostic type descriptors, the
Model-is-not-Collection stance (the model entity RECORDS its collection
binding as data; today the binding is the lowercased struct name,
derive/src/model/description.rs:50), and the read-path defaulting rule
(section 5.4). `sys::Item::Collection` stays untouched and unused by this
design; whether the model entity subsumes it later is an open question
(section 11).

### 4a. Trajectory: unified entity storage and models as contracts

The membership shape above is not speculative generality; it is the catalog
shape the maintainer's storage trajectory requires. Today an entity belongs
to exactly one collection and, in the SQL engines, its canonical state
lives inside that collection's table. The planned direction (maintainer,
2026-07-05): a Model is a data contract an entity is ANTICIPATED to meet;
one entity can meet several contracts, and contracts overlap. Concretely on
the storage side, unified entity storage means canonical state and head
move to a shared entities table while MATERIALIZED values remain per model
exactly as now (a named postgres table per model with its dynamic columns);
whether head is duplicated onto materialization tables is undecided. Sled
already has precisely this split: a global entities_tree for canonical
state, per-collection materialized trees for values
(storage/sled/src/database.rs:17-20, storage/sled/src/collection.rs:99-133);
the excision is, in essence, making postgres/sqlite look like sled. The
codebase already leans this way at the identity layer too: event identity
deliberately excludes the collection ("collection is getting excised from
identity", proto/src/data.rs:17), and PR #236's plan recorded "future
intent is unified collection with materialized views/indexes for queries"
(specs/property-registration/plan.md); the federated hypergraph aspiration
(LONG_TERM_ASPIRATION.md) sits behind both.

Scope decision: the collection excision is EXCLUDED from this project's
implementation (all phases; tracked separately as #304, with the
Collection-vs-Model terminology deconfliction as #305), because nothing in
the catalog design depends
on where canonical state is stored, and included as a compatibility
constraint: property entities are standalone and shareable NOW; contract
membership is a first-class record NOW; catalog semantics never assume
canonical state lives in a materialization table; and Phase C's engine work
(column creation, rename DDL, which runs inside set_state where canonical
and materialization writes are currently interleaved in one table,
storage/postgres/src/lib.rs:260-269, 342-391) keeps the canonical-write vs
materialization-write seams clean so the later table split stays mechanical
rather than a re-untangling. `Model::collection()` and collection-scoped
queries keep working unchanged throughout, with the model entity's
`collection` binding recorded as data so future remapping is a catalog
change, not code archaeology. Multi-contract entities in the public API are
likewise out of scope (section 6).

**Bootstrap non-recursion.** The catalog collections' own properties are
read by name, exactly like every property today. The meta-schema (the
property names above and their types) is defined by this spec and frozen;
catalog collections are permanently exempt from identity keying of their own
state (they stay name-keyed at the backend layer even in Phase C). This
terminates the recursion by fiat, the same move #236 made with its "implicit
data contract", but without inventing a second entity encoding. Concretely
(maintainer, 2026-07-05): catalog entities are SYSTEM MODELS, built and read
through the raw Entity/backend interface exactly as SysRoot is today
(core/src/system.rs:124-127), never through derive(Model); deriving a Model
for a catalog collection would be the self-description ouroboros this
exemption exists to forbid. If we later
want the meta-schema itself introspectable, we mint well-known property
entities for it; nothing is load-bearing on that.

**Protection (rev 3: structural and receiver-side).** `PROTECTED_COLLECTIONS`
is currently dead code: no reader exists (core/src/system.rs:21, exhaustive
grep). This RFC makes it real and structural: the catalog collections
(`_ankurah_system`, `_ankurah_model`, `_ankurah_property`,
`_ankurah_model_property`) are NOT mutable through ordinary transactions at
all. A receiving durable node rejects any CommitTransaction event targeting
them outright, regardless of the sender's software version; the only
mutation path is the registration operation (5.2), which the durable node
policy-checks and executes itself. Catalog events replicating between peers
are verifiable by CONTENT: a catalog genesis is self-certifying, because
its entity id must equal the derivation over its own payload and its event
id must equal the frozen encoding (5.1), so any node can validate one
without trusting the channel; follow-up events are policy-checked writes
arriving only from the registration path. The `_ankurah_` prefix is
reserved and rejected for user-model collection ids at derive time and at
CollectionSet::get. Because enforcement sits on the receiving durable node,
mixed-fleet safety is a deployment ORDER (upgrade durable nodes first), not
a simultaneous-upgrade requirement.

## 5. Design by axis

### 5.1 Identity derivation (the crux)

**Decision: deterministic entity ids, derived, never coordinated.**

- model entity id = first 16 bytes of
  SHA-256("ankurah.model.v1" || len(root) || root_entity_id_bytes ||
  len(collection) || collection_id_utf8)
- property entity id = first 16 bytes of
  SHA-256("ankurah.property.v1" || len(root) || root_bytes ||
  len(model_id) || minting_model_entity_id_bytes || len(anchor) ||
  anchor_name_utf8 || len(backend) || backend_utf8 || len(vt) ||
  value_type_utf8)
- membership entity id = first 16 bytes of
  SHA-256("ankurah.membership.v1" || len(root) || root_bytes ||
  len(model_id) || model_id_bytes || len(prop_id) || property_id_bytes)

The model id in the property derivation is the MINTING SCOPE, not
ownership: it namespaces default (by-name) derivation so that unrelated
models declaring the same field name do not accidentally converge on one
property entity. A model that intends to SHARE another model's property
binds it by explicit id (5.9), which bypasses derivation entirely; the
membership record then points at the shared property, and only the
membership id involves the sharing model. Explicit-id BINDING never mints:
it references an id that already exists. DDL-authored definitions (5.10)
use this same derivation when declared by name (under a model, that model's
scope; standalone shared declarations derive in a reserved standalone scope
with a zero model id), so a DDL document and a Rust struct declaring the
same thing converge on the same entity; explicit ids inside a DDL document
are references, exactly like the attribute form.

All fields length-prefixed; domain tags versioned; anchor name defined in
section 5.8 (it equals the current property name unless an anchor attribute
pinned an earlier one). The bytes go into `EntityId::from_bytes`
(proto/src/id.rs:19). The full 16 bytes are hash output: EntityId is a Ulid
newtype, so the nominal timestamp bits carry hash bytes. Adversarial review
confirmed 128 bits is sound at catalog scale (second-preimage at 2^128,
root-scoped, length-prefixed against boundary-sliding); truncating further
(e.g. zeroing timestamp bits down to 80 hash bits) is NOT acceptable against
a name-controlling adversary. If implementation verification finds anything
depending on EntityId timestamp semantics, the dependence gets fixed; the
id does not lose entropy.

The natural key follows #85 AC2: collection (via the model entity), property
name, and type identifier, where "type identifier" concretizes to the
(backend, value_type) pair under section 4's normative mapping.
Consequences, intended: a retype mints a NEW property identity (the String
"title" and an I64 "title" are different properties, which is the
collision-safety #85 asks for; the read-side consequences of retype are
governed by 5.4's sibling gate and 5.6); flipping `optional` or retargeting
a reference does NOT re-key.

**Why this converges without coordination.** EventId is a content hash:
SHA-256 over bincode(entity_id) || bincode(operations) || bincode(parent)
(proto/src/data.rs:15-24), attestations ride outside the hashed content
(proto/src/auth.rs Attested; proto/src/data.rs:116-127), and a creation
event is one with an empty parent clock (proto/src/data.rs:113). Therefore
two nodes that independently register the same property mint: the same
entity id (derived), the same genesis operations (frozen encoder, below),
the same empty parent. That is not two conflicting creations; it is
byte-for-byte THE SAME EVENT with the same EventId. Redelivery of an
already-known event id is a no-op on apply
(core/src/entity.rs:251-254), so two attested copies merge, not conflict.
Concurrent registration is idempotent by construction. No upsert race
exists, no canonicalization protocol is needed, and offline registration is
safe. This resolves the determinism question PR #236 explicitly deferred
("Entity ID stability - Runtime lookup by name for now; future annotation
for determinism", specs/property-registration/plan.md).

**The genesis freeze rule (normative, strengthened after adversarial
review).** The frozen surface is EVERY byte that feeds the EventId hash. The
genesis event of a metadata entity MUST be produced by a dedicated frozen
encoder, not by the generic backend `to_operations` path, and MUST contain
exactly the identity-key fields:

- model genesis: one LWW operation setting `collection`
- property genesis: one LWW operation setting `minted_for`, `name` (anchor),
  `backend`, `value_type`
- membership genesis: one LWW operation setting `model`, `property`

The frozen encoder emits the LWW operation encoding as it exists today
(LWWDiff version 1 wrapping a name-keyed bincode BTreeMap,
core/src/property/backend/lww.rs:18, 187-207) and never follows the node's
compiled operation-encoding version. This closes a hole the adversarial
review proved out: LWWDiff's version byte is inside `Operation.diff` and
therefore inside the EventId hash, so ANY general-encoding bump (v2 lands
with Phase A itself under rev 3; any v3 later) would make nodes on the two
sides of the bump mint DIFFERENT genesis bytes for the same derived id if
they used the generic path: two disjoint lineages for one entity id,
permanent divergence, in exactly the mixed-fleet window this design
serves. Pinning the genesis encoder (v1, name-keyed,
scalar Values only; determinism holds because the identity fields are
String/EntityId scalars whose bincode is unambiguous, never Json) makes the
genesis a pure function of the identity key across all phases, forever.
Everything else (membership `optional`, `target_model`, display-name
updates, any field a future version adds) MUST be written by follow-up
events, which merge as ordinary LWW updates and may use the node's current
encoding, since follow-up bytes do not participate in identity. All
authoring paths (derive macro and DDL) mint through the same derivation and
frozen encoder; explicit-id binding never mints at all (5.9). One
clarification the membership kind makes necessary: membership genesis
carries EntityId REFERENCES, not causal parents; a membership integrates
before, after, or without the entities it names, the same opacity argument
as data events referencing unknown property ids (5.6), so the registration
ordering in 5.2 remains hygiene, not a correctness requirement, and no
metadata-internal ordering dependency exists.

**Net-new plumbing, acknowledged.** No code path today creates an entity
under a caller-chosen id: `WeakEntitySet::create` mints internally
(core/src/entity.rs:681-687), and the commit path rejects creation events
for ids not minted via create() (the phantom-entity guard,
core/src/transaction.rs:86-98). The primitive exists
(`Entity::create(id, collection)`, core/src/entity.rs:146); Phase A adds a
create-with-derived-id constructor and threads registration through the
phantom guard. Called out so nobody reads the registration path as "zero
new machinery": once executed, registration events are validated,
policy-checked, attested, persisted, and relayed exactly like any commit;
entity-id minting, the genesis encoder, and the transport (a protocol
operation rather than a client transaction, 5.2) are what is special.

**System-root scoping.** The root entity id is part of the derivation, so
ids are per-system. Rationale: two unrelated systems never mint colliding
metadata ids (relevant the day federation arrives), and ids die with the
system on hard_reset, which is coherent with join semantics
(core/src/system.rs:156-234). The rejected alternative, root-free global
ids, would allow compile-time-constant ids but binds source code to nothing
(any system, same ids), which sounds convenient and is exactly the problem:
an id minted for the dev system and the prod system being equal means
catalog state from one is plausible in the other. Root scoping also forces
registration to wait for system_ready, which is already a real gate
(core/src/system.rs:94-101). The cost: source code can never embed a
metadata entity id, which constrains the rename design (section 5.8) to
anchor names instead of pinned ids. That constraint turned out to improve
the design.

### 5.2 Registration lifecycle

**Trigger: model first-use per process, durable write on first mutating
use.** When a context first touches a model M (create, edit, fetch, query,
subscribe), the node ensures registration of M, all its declared active
fields (ephemeral fields are excluded, derive/src/model/description.rs:31-40),
and the (M, property) membership records:
derive ids, check the local catalog map, and if absent, on a mutating path
(create/edit), issue the registration operation. Read-only paths derive and
cache ids without writing; the durable catalog entry appears when data
first appears, which is the moment other nodes start needing it. An
explicit `ctx.register::<M>().await` issues the same operation eagerly
(e.g. before first render).

**Transport (rev 3): registration is a protocol operation, not a client
transaction.** A dedicated request (a new NodeRequestBody variant beside
Fetch/SubscribeQuery, proto/src/request.rs:119-128; exact name is an
implementation choice, e.g. RegisterSchema) carries the language-agnostic
definitions: models (collection, name), properties (anchor, backend,
value_type, target_model), memberships (model, property, optional). The
receiving durable node policy-checks the request (the PolicyAgent decides
who may define schema; this is the data-freedom gate from above) and then
EXECUTES the registration itself: derives the ids, emits frozen-genesis
and follow-up events locally through check_event like any write
(core/src/context.rs:129), persists, and relays. Catalog collections are
not writable any other way (section 4 protection). Consequences: a durable
node needs no model code to serve registration (the request carries
everything); execution is idempotent because derivation and genesis are
deterministic, so re-issued or concurrently-issued registrations of the
same definitions converge on identical events, including across multiple
durable executors. A durable node that itself runs model code registers by
executing the same operation locally. OFFLINE ephemeral flow: derive ids
locally, write id-keyed data events offline immediately, queue the
registration operation for reconnect; the durable side applies the data
opaquely on arrival (5.6 catalog lag) and materializes fully once the
queued registration lands.

Two load-bearing rationales (maintainer, 2026-07-05) that any refinement
of this lifecycle must preserve: (a) DATA FREEDOM: authorized users define
whatever models and properties they see fit, subject only to PolicyAgent
approval; schema definition is an ephemeral-node capability, never a
durable-node privilege (collisions are handled by construction: identical
definitions converge to one entity, same-name different-type definitions
are distinct entities). (b) Durable nodes cannot be assumed to have model
definitions AT ALL: model code lives on clients; registration must
originate from ephemeral nodes and carry everything the durable side needs.

REN vs #85 AC2, twice, both disclosed (section 10): (a) granularity is per
MODEL, all properties in one transaction, not per accessor touch; the derive
macro statically enumerates fields (it already does for
initialize_new_entity, derive/src/model/model.rs:35-40), and per-accessor
granularity would leak partial schemas and buys nothing. (b) timing on READ
paths is cache-only rather than a durable upsert; AC2's "read/write accessor
usage time" had read usage registering durably, but a read-only node
performing catalog writes is a policy surprise and buys nothing until data
exists. Whether a query-only node may ALSO write durably stays open as a
policy question (section 11). REN vs PR #236's "on first trx.create only":
query paths must at least derive and cache, because predicate resolution
needs ids before any create happens on this node.

**Ordering within the registration transaction:** model entity, then
property entities, then memberships, extending #236's two-phase plan. With
derived ids the dependency is soft (a property id is derivable before the model
entity exists), so this is hygiene, not a hard ordering requirement; it also
means Ref<T> target model ids are derivable without registering the target
model first, eliminating #236's circular-reference concern.

**The catalog map (AC3).** Each node maintains an in-memory map, warmed by
subscribing to the three catalog collections (predicate True) once
system-ready: by (minting model, name, backend, value_type) to property
entity id, by id to definition, and by model id to its membership set (the
contract). Name-based lookup of models and properties, one of the
maintainer's required paths, is this map; it answers "the property named X
in collection C" through the model's memberships. This is an ordinary LiveQuery-backed subscription, the same
machinery applications use. Durable nodes have the catalog locally;
ephemeral nodes get initial state plus updates through the standard
subscription relay. Registration events arriving from peers update the map
like any changeset. Two normative obligations from adversarial review:
(a) a `wait_catalog_ready` gate, analogous to wait_system_ready
(core/src/system.rs:97-101): a consumer with no local compiled schema (a
relay, a dynamic binding) DEFERS resolution until the initial catalog
snapshot lands rather than failing UnknownProperty against a cold cache;
(b) `hard_reset` MUST flush the derived-id cache and catalog map along with
the state it already clears (core/src/system.rs:208-234), because derived
ids are root-scoped and a node that hard-resets into a different system must
re-derive everything against the new root; a stale cache would leak the old
system's ids into the new one.

**Idempotence across restarts and peers.** Ensure-registration re-derives
ids and re-emits the genesis event if the entity is unknown locally. If the
entity exists (locally or arrives later from a peer), the genesis event is
identical bytes, hence the same EventId, hence a no-op on merge. Follow-up
metadata (optional, target_model) re-asserted by a node that missed prior
state merges as LWW.

### 5.3 Resolution and failure semantics (AC4, AC5)

**ankql gains the resolved identifier.** Today a property reference is
`Expr::Path(PathExpr { steps: Vec<String> })` (ankql/src/ast.rs:24-70). This
RFC introduces

```
Identifier {
    property: EntityId,        // the defining property entity
    name: String,              // resolved-at name, for display and SQL
    subpath: Vec<String>,      // JSON sub-path steps, possibly empty
}
```

produced by a resolution pass that binds `steps[0]` against (local compiled
schema first, catalog map second) and leaves the remaining steps as the JSON
sub-path (to be typed against the phase-3 schema plan's StructuredKey model
when that lands; section 7). Resolution failure IS the AC5 fail-closed
behavior: predicate building returns an UnknownProperty error naming the
collection and property. This replaces today's three inconsistent
missing-property behaviors (hard evaluation error,
core/src/selection/filter.rs:59-91; reactor unwrap_or(false),
core/src/reactor/subscription_state.rs:391; SQL assume_null,
storage/postgres/src/lib.rs:528-548) with one rule: an UNRESOLVABLE
reference fails at build time; a resolvable reference whose property is
absent on a given entity evaluates as the registered default under 5.4's
rules, which is well-defined because resolution proved the property exists
in the schema. It also subsumes the known assume_null/referenced_columns
first-vs-last-step inconsistency on JSON paths (ankql/src/ast.rs:241-333
keying on path.property() vs path.first(); acknowledged in a sqlite comment,
storage/sqlite/src/engine.rs:462-468), because the resolution pass fixes
which step is the property once, in one place.

Where resolution runs: at predicate build on the querying node (parse or
programmatic construction, then resolve; the existing TypeResolver pass at
core/src/context.rs:344, core/src/node.rs:858, node.rs:916, and
core/src/livequery.rs:217 is the structural slot this replaces or absorbs,
per its own TODO, core/src/type_resolver.rs:24-26). On nodes with no
compiled schema, resolution defers behind wait_catalog_ready (5.2) instead
of rejecting during warm-up. Wire impact (rev 3): resolution formally
precedes serialization, and the wire carries resolved Selections from
Phase A (a bincode AST shape change on Fetch/SubscribeQuery,
proto/src/request.rs:119-128, sitting behind Phase 0's #294 version
negotiation like the rest of the id-keyed contract; there is no interim
name-form-on-the-wire state). Receiver-side handling of predicates referencing properties absent
from the receiver's catalog: PASS THROUGH (evaluate; unresolvable references
match nothing) until #274's validated-ingress seam exists, at which point
rejection becomes a policy option there. AC5's letter, "predicate
building... will fail", is a build-time client-side guarantee; this RFC
supplies the replicated data any future ingress check would consult.

### 5.4 Read-path semantics (the #175 fix)

Adopted from #236, generalized, gated after adversarial review, and grounded
in why it is semantically correct rather than a workaround:

1. Property registered, present in backend: return the value.
2. Property registered, absent from backend, optional: None. (Today
   Option<T> already swallows Missing, core/src/property/mod.rs:19-35.)
3. Property registered, absent from backend, required: the value type's
   default ("" for string, 0 for integers, false for bool), SUBJECT TO the
   sibling gate below. For an operation-based CRDT, "no operations for this
   field" is a legitimate encoding OF the default: yrs cannot and should not
   distinguish an empty Text from an untouched one
   (core/src/property/backend/yrs.rs:155-168). Declaring the default at the
   schema layer is the honest fix; it makes the empty-string case (#175)
   read back "" by definition instead of erroring PropertyError::Missing
   (core/src/property/value/yrs.rs:88-95). The same rule applies uniformly
   to LWW-backed required properties (decided, was draft open question):
   post-Phase-A a missing required LWW value means pre-metadata legacy data
   or a concurrent schema skew, and both read coherently as the declared
   default under the gate.
4. **The sibling gate (new, from adversarial review).** Rule 3's default is
   returned ONLY if no other live property entity sharing this display name
   has data present on this entity, scanned ACROSS contracts, not just the
   reading contract's membership set: if a retype lineage from another
   overlapping contract holds a real value here, fabricating a default over
   it is exactly the phantom-default hazard, whether or not the reader's
   contract knows that sibling. If a same-named
   sibling id (a retype lineage, 5.1) has data here, the read surfaces
   `PropertyError::TypeSkew` naming both property ids instead of fabricating
   a default. Without this gate, a String-to-i64 retype would read every
   pre-retype entity's real "30" as a phantom 0; with it, mid-migration
   reads are fail-visible, matching this RFC's rename philosophy (5.8).
5. Property not registered and not in the local schema:
   PropertyError::UnknownProperty (a new variant; today's Missing keeps its
   meaning for pre-Phase-A code paths).

Optionality sourcing (decided, from adversarial review; adjusted for
contracts): optionality is a property OF A MEMBERSHIP, so the flag consulted
is the one on the (model, property) membership for the contract in play:
the View getter's model, or the model bound to the queried collection for
predicate evaluation. When that resolution is ambiguous (several models
bound to the queried collection carry memberships for the property and
disagree, or none does), the property is treated as OPTIONAL: absent reads
as None, never a fabricated default, the same safe-direction bias as the
partial-metadata rule below. The same property shared by two models can be
required in one and optional in the other, coherently. A membership whose
`optional` follow-up has not yet arrived is treated as optional (absent
reads as None), never defaulted, because fabricating a default on partial
metadata is the one non-recoverable misread. The View getter continues to
obey its COMPILED optionality (its return type is fixed at compile time);
engines, predicate evaluation, and dynamic bindings obey the catalog
membership flag. A node whose compiled flag disagrees with the converged
membership flag is the mismatched-code case (5.6): each consumer is
internally coherent, the catalog makes the disagreement observable, and
`optional` stays out of every identity key because flipping optionality
must not re-key data.

Two notes. First, for the View getter specifically, required-ness and type
are compile-time knowledge, so rule 3 is implementable locally before the
catalog exists; the catalog extends the same rule to predicate evaluation,
engines, and non-Rust bindings, which is where schema truth must be data.
Second, the degenerate #175 case (every field empty, so zero operations, so
no creation event and no persisted entity, tests/tests/yrs_backend.rs:300-305)
is fixed by allowing creation events with an empty operation set rather than
skipping event generation; EventId hashes fine over empty operations
(proto/src/data.rs:15-24), and a zero-op genesis is exactly what "an entity
whose every field is default" means under rule 3. This piece is independent
of the catalog and could land with Phase A.

### 5.5 Wire and state impact (phased identity keying)

The load-bearing observation: property names appear on the wire in exactly
two places, inside opaque backend payloads (LWW diff/state maps, yrs update
root names) and inside query ASTs (grep-verified: no property-id or field-id
concept exists in shipped proto/core/derive code today; the phase-3 schema
PLAN's u32 PropertyId is unbuilt and reconciled in section 7). The proto
Event, OperationSet, EntityState, and StateBuffers shapes never change under
this design; they are keyed by backend name, not property name
(proto/src/data.rs:102-204). Rev 3 collapses what were three separately
gated changes into one protocol epoch behind #294:

- **Phase 0: #294.** Protocol version in the Presence handshake with
  decided refuse-vs-degrade semantics. Hard prerequisite for everything
  user-visible below; both the event/state encoding bump and the
  request-message AST change (#294's general "on-disk or wire format"
  clause covers request framing even though its motivating example is
  event encoding) ride the version negotiation it introduces.
- **Phase A: the id-keyed contract, the catalog, and registration, as one
  epoch.**
  - The catalog collections and the registration operation (4, 5.2).
  - LWW: diff version 2 (LWWDiff.version, currently 1,
    core/src/property/backend/lww.rs:18) and state buffer version 0xA2
    (currently 0xA1 with a pre-0.9 legacy fallback,
    core/src/property/backend/lww.rs:30-42, 154-185), maps keyed by
    EntityId (16 bytes) instead of name, from day one. Old 0.9 nodes
    REFUSE unknown versions cleanly rather than misreading them
    (core/src/property/backend/lww.rs:176-180), which is the right failure
    mode, and #294 turns it from an error into a negotiated capability.
    Reads of v1/legacy buffers translate name-to-id through (local schema,
    catalog), with lazy rewrite-on-save, exactly the 0.9 legacy-fallback
    precedent; that fallback runs from the first release rather than
    arriving in a later phase. Catalog-collection writes are exempt
    (section 4) and the genesis encoder is pinned (5.1), so metadata
    convergence is immune to this and every future bump.
  - Query ASTs carry the resolved Identifier (5.3); Fetch/SubscribeQuery
    Selections are bincode, not self-describing, so this is a hard wire
    break for those messages, absorbed by the same epoch.
  - Rationale for the collapse (maintainer, 2026-07-05): the data contract
    should pass model and property ids over the wire immediately; shipping
    an intermediate name-keyed-with-catalog state would mean migrating
    twice.
- **Phase C: engine-level identity binding.** (Engine storage is node-local,
  so this phase is NOT wire-gated; it consumes the catalog.)
  - Yrs (decided; timeless, not engine-specific): root names are embedded
    in the yrs update encoding and cannot be
    rekeyed without rewriting CRDT history. **Decision (was draft open
    question): defer yrs rekeying entirely.** Roots stay property-name-keyed
    for all entities; the catalog binds root name to property id at the
    ankurah boundary; renames keep working because the root name is just a
    key the binding maps to. Less code, no dual-root lookup, identical
    rename behavior. Revisit trigger: if per-field wire addressing ever
    needs the root itself to carry identity, id-named roots for new entities
    is the fallback design, and nothing here forecloses it.
  - Sled: swap the property_config key from name bytes to entity id bytes
    (storage/sled/src/property.rs:32-63); the u32 compaction and
    materialized row format are untouched.
  - Postgres/sqlite: columns REMAIN named by property name for human
    queryability. The catalog binds property id to column; a rename becomes
    ALTER TABLE RENAME COLUMN driven by observing the catalog change; a
    (name, type) collision (two live property ids sharing a name, e.g.
    post-retype) disambiguates the newcomer's column with a short id suffix.
    This finally gives the postgres TODO its property ids
    (storage/postgres/src/lib.rs:368-373).
  - IndexedDB: entity objects hold property-name fields
    (storage/indexeddb-wasm/src/collection.rs:73-81); same treatment as
    SQL: names bound via catalog, lazily re-materialized on rename.
- Size cost, estimated: a 16-byte id key vs a typically shorter name string
  in LWW maps. Negligible against event framing overhead; sled rows are
  already u32-compacted.

Sequencing note required by #296: Phase A is a wire/format change and MUST
NOT ship to mixed fleets before Phase 0 (#294) gives the Presence handshake
a protocol version with decided refuse-vs-degrade semantics. Within a
fleet, durable nodes upgrade first (receiver-side protection, section 4).
Phase C is engine-local and carries no wire dependency of its own.

### 5.6 Mismatched model code across nodes

- **Same name, different type or backend (the #289 strategy-agreement
  case):** different (backend, value_type) means different derived property
  ids. The two versions write to DIFFERENT properties that happen to share a
  display name. From Phase A the divergence is both VISIBLE (both entities
  queryable in the catalog, tooling can warn) and structurally
  un-collidable at the data layer, since payloads key by id from the same
  epoch (rev 3; previously these arrived in separate phases). During a retype rollout window, queries resolve
  per-node against each node's own schema (its own id), so the two fleets
  see different subsets for the affected property until the fleet converges;
  5.4's sibling gate keeps reads fail-visible rather than silently defaulted
  through that window. A declared-coercion migration ("retyped_from", where
  new code reads the old property id through a cast) is future
  schema-evolution work this design leaves room for but does not include.
- **Old node missing a property (fleet-version skew):** backend payloads are
  opaque; the old node applies and persists operations for ids it has no
  model field for, exactly as it does today for names it does not know
  (apply deserializes and stores without schema consultation,
  core/src/property/backend/lww.rs:336-363). It cannot project them, which
  is correct. Its catalog subscription still delivers the definition, so
  engine-level concerns (column creation) can proceed even without model
  code once Phase C lands.
- **Node predating this feature entirely:** the id-keyed encodings and the
  Identifier AST are unreadable to it BY DESIGN; Phase 0's version
  negotiation (refuse or degrade, decided in #294) governs the encounter,
  and durable nodes upgrade first (section 4).
- **Catalog lag (data before metadata):** events referencing an unknown
  property id apply fine (opacity, above), so ingest never blocks on the
  catalog; projection returns UnknownProperty until the catalog entry
  arrives via subscription. This deliberately avoids any causal dependency
  of data events on metadata events, so it composes with whatever #272
  decides about transactional visibility, and it needs nothing from #268's
  planner. Registration transactions are ordinary transactions; per #272's
  current reality, receivers integrate them per entity, and this design is
  correct under that weakest case.

### 5.7 Policy hooks

The property entity id becomes the stable key policy needs for per-property
rules; today nothing property-keyed exists at any granularity, including
among the commented-out future hooks (core/src/policy.rs:49-162, 155-161,
293-303). This RFC deliberately does NOT design per-property policy
semantics; it commits only to: (a) registration flows through check_event
like any write (core/src/context.rs:129), so agents can gate who may
register; (b) catalog collections are protected (section 4); (c) the
PolicyAgent surface for per-property gates should be designed ONCE together
with #264's commit-time hook and #274's admission seam, keyed by property
entity id, when one of those lands. Adding a use case to that consolidated
design, not a parallel surface, is the coordination contract with the
phase-2 track.

### 5.8 Renames and anchors (the stretch AC)

**Decision: pin the anchor, not the id.** The general mechanism is an
anchor attribute; a rename is its primary use:

```rust
#[property(anchor = "title")]   // reads as: renamed_from = "title"
pub headline: String,
```

The derivation (5.1) uses the anchor name, so the renamed field derives the
SAME property entity id on every node and every system. The registration
path then writes the new display name to the property entity's `name` (an
ordinary LWW update, provenance-ordered; concurrent different renames
converge like any LWW write, and each node's own queries keep resolving
through its compiled name to the same id, so display-name churn never
affects addressing). Consequences:

- Nodes still running pre-rename code derive the same id from the old name;
  both code versions address one property. Data written under either name
  converges; nothing migrates.
- ankql references resolve per-node against that node's compiled schema, to
  the same id; the wire carries the id from Phase A, so cross-version
  queries agree even though display names differ.
- SQL columns follow via the catalog-driven RENAME COLUMN (5.5).
- Chained renames keep the original anchor (a to b to c: anchor stays "a").
  The anchor is a permanent, self-documenting fact in the source.
- A rename WITHOUT the attribute is two independent properties (an add and
  an abandon), which is exactly today's failure mode, except now the catalog
  contains both entities plus provenance, so tooling can detect the likely
  mistake and suggest the attribute. Fail-visible instead of fail-silent.

**The anchor-reuse guard (normative, from adversarial review).** Anchors
are permanent derivation inputs, so a RETIRED display name must not be
silently re-minted: if release 1 renames `title` to `headline` (anchor
"title") and release 2 adds a brand-new field literally named `title`, the
new field's default anchor is its own name, "title", which derives the
EXACT id the headline lineage already owns; unrelated fields would silently
share one identity, and the id-keyed payloads would merge their writes into
one register.
Registration therefore REFUSES to mint a genesis for a field whose derived
id already exists in the catalog with a current display name different from
this field's name, with an error demanding an explicit anchor attribute
(the new field takes a fresh anchor, e.g.
`#[property(anchor = "title_2026")]`, or the author realizes it IS the
renamed property and anchors accordingly). The check runs against the
catalog map and the local schema at registration time; because derivation is
deterministic, any node with a warm catalog detects the collision, and a
node that registered blind before its catalog warmed converges into the SAME
id (never a corrupt third state), so the residual race window degrades to a
loudly-detectable semantic conflation, not silent divergence.

Relation to #85's stretch wording ("expressly referencing the property
entity id"): the anchor is the id-reference mechanism OF THE DERIVATION
PATH, one deterministic step removed from the literal id, which keeps one
binary valid against every system it deploys to. Literal id references
exist too (5.9) and satisfy the stretch AC verbatim; the anchor is the
recommended form wherever by-name derivation authored the property, the
explicit id wherever sharing or DDL did.

### 5.9 Explicit id binding and shared properties

By-name binding (derivation, 5.1) is the DEFAULT, not the only path. The
macro accepts explicit bindings to known definition entities:

```rust
#[model(id = "AZB64ULID...")]           // bind this struct to a known
struct Signal { ... }                    // model entity

#[property(id = "AZB64ULID...")]        // bind this field to a known,
pub label: String,                       // possibly shared, property entity
```

Semantics: an explicit id bypasses derivation entirely. Registration looks
the entity up (catalog map, then subscription-backed fetch); it verifies
the field's (backend, value_type) against the definition and fails
registration on mismatch (the same fail-closed posture as AC5); it ensures
the (model, property) MEMBERSHIP exists, minting it with the derived
membership id (5.1); it never mutates the bound property entity's identity
fields. Binding an id that does not exist in the catalog is an error, not
an implicit create: explicit binding is a REFERENCE to a definition
authored elsewhere (another model's registration, or DDL, 5.10). Two
consequences, stated as design rather than left implicit. Cold start: if
the authoring producer has never registered in this system, the binder's
registration HARD-FAILS; deployments using explicit binding must guarantee
the definition exists first (DDL application or eager registration of the
authoring model is the intended provisioning step). Drift: if a bound
definition is later retyped, every binder's verification fails until
rebuilt; retyping a shared property is a breaking change for its binders BY
DESIGN (fail-visible, consistent with 5.6; a declared-coercion escape hatch
is future schema-evolution work, section 6).

**Sharing.** This is how properties are shared between models: model B
binds model A's property id (or a DDL-authored property's id); B's
membership carries B's own `optional` stance; reads and predicates in B's
context resolve to the same property entity id, hence the same id-keyed
values on any entity both contracts touch (from Phase A under rev 3). Under
collection-delineated storage, sharing is visible only where entities of
both models coexist or migrate; under unified storage (4a) it is the
mechanism that makes overlapping contracts read the same data. Sharing by
NAME is deliberately not supported: accidental cross-model name equality
must not conflate semantics; sharing requires the express act of citing the
id.

**The per-system caveat, stated honestly.** Entity ids differ per system:
all minted ids are root-scoped derivations, whether authored by macro or by
DDL (5.1). A literal id in source therefore binds the source to one
system's catalog. Workable practices: per-deployment binding (the attribute
value sourced from build-time config or codegen against the target system's
catalog); or keeping cross-system portability by authoring shared
properties via a deterministic path and binding by anchor instead. Tooling
should make both easy (print ids per system; emit binding files from a
catalog). The mechanism is required and shipped; choosing a portability
practice is deployment policy, not RFC policy.

### 5.10 The declarative DDL path

The catalog entities are the definitive schema. Rust structs are one
authoring surface for them; this section commits to a second, declarative
one, without implementing it here.

A schema DOCUMENT declares desired catalog state: models (name,
collection), properties (name, backend, value_type), memberships (model,
property, optional), sharing (membership citing a property by name-within-
this-document or by explicit id), renames (anchor declarations). Applying
the document diffs declared state against the live catalog and emits the
SAME registration transactions the derive macro emits, through the same
frozen-genesis, derived-id, create-with-id machinery (5.1, 5.2): the
differ's output is a reviewable migration plan (create these entities,
update these display names, add these memberships), and application is
idempotent for the same reason registration is. Nothing in the write path
is DDL-specific; DDL is another producer of registration transactions.

Deliberate properties of this path: it is DECLARATIVE, desired-state,
diffable, unlike SQL's imperative DDL statements; it is language-neutral
(the descriptor vocabulary in section 4 was chosen language-agnostic
precisely so non-Rust bindings, e.g. TypeScript, author schema through this
same surface rather than through Rust); and it makes "schema without any
Rust struct" a first-class workflow, with structs optionally GENERATED from
the catalog rather than always being its source.

Format: a data document (RON, TOML, or JSON; pick during implementation),
applied via `ctx.apply_schema(doc)` and a CLI wrapper. Extending ankql with
schema statements is possible later but not required for the path: ankql's
grammar is selection-only today (ankql/src/ankql.pest), and desired-state
documents diff better than statement streams, which is the failure mode of
SQL DDL this direction avoids. Landing: the document format and differ are
Phase D deliverables; Phase A's job is to keep the registration machinery
shaped so the differ has nothing special to do.

## 6. What this design does NOT do (non-goals)

- No runtime-dynamic merge strategies (nodes obey their compiled active
  types; the catalog makes disagreement visible, not adjudicated).
- No change to the PropertyBackend trait, layer semantics, or conformance
  surface (#267's territory). The id-keyed encodings become laws in #267's
  conformance kit when both exist.
- No collection renames or model-to-collection remapping (the model entity
  records the binding as data, which is the prerequisite; acting on it is
  future schema-evolution work, kept out per #236's "divergence intended"
  note).
- No declared-coercion retype migrations (5.6's "retyped_from" note): retype
  is new identity plus fail-visible reads in this RFC; coercion is future
  schema-evolution work.
- No DDL implementation: the path, invariants, and landing phase are
  specified (5.10); the document format and differ are not built here.
- No collection excision / unified canonical-state storage and no
  multi-contract entities in the public API (4a records the trajectory,
  the concrete target shape, and the Phase C seam constraint that keeps
  the catalog and engine work compatible with it; that is all).
- No catalog-driven validation of incoming events at ingress (that is
  #274's seam; this RFC supplies the data it would consult).
- No federation-grade catalog trust model (root scoping is deliberate
  headroom for it).
- No yrs history rewriting, and no yrs root rekeying at all (5.5).
- No self-hosting of the meta-schema (the catalog describes user models; it
  does not describe itself; section 4).

## 7. Interactions and sequencing

- **#294 (Presence protocol version): Phase 0 of this ladder** (rev 3).
  The id-keyed data contract ships in Phase A, so #294 gates the entire
  initiative's first user-visible release, per #296's standing
  instruction. The implementation session designs and lands #294 first.
- **#175/#236:** Phase A plus the read-path rules (5.4) close #175. PR #236
  is superseded as an implementation (its sys::Item approach diverges from
  section 4) but its spec decisions are absorbed here with attribution;
  recommend closing the PR in favor of fresh Phase A work once this RFC is
  approved, cherry-picking its proto ValueType mirror if convenient
  (core/src/value/mod.rs conversions in the PR diff).
- **specs/unified-refs-edges-json/phase-3-schema.md (in-repo plan, unbuilt):
  reconciled, one registry.** That plan wants an in-process SchemaRegistry
  (RwLock HashMap of ModelSchema/FieldSchema) with a provisional
  `PropertyId = u32` ("manually assign... later, derive from field order or
  hashing") to make TypeResolver schema-aware, validate structured JSON
  paths (StructuredKey/StructuredNode), and normalize collation. This RFC
  supplies what that plan stubbed: property identity is the derived
  EntityId, not a u32 (u32s remain legitimate as engine-local compactions,
  sled-style); ModelSchema/FieldSchema become the concrete form of this
  RFC's "local compiled schema", generated by the derive macro, with
  FieldSchema carrying the property EntityId; Identifier.subpath (5.3)
  aligns with StructuredKey when structured typing lands; and the
  TypeResolver TODO (core/src/type_resolver.rs:24-26) is discharged by the
  resolution pass. The phase-3 plan should be revised against this RFC
  before implementation; its StructuredNode typing is complementary and
  untouched.
- **#267 (backend contract):** boundary stated in sections 2 and 6. One
  concrete renegotiation of #236: backend identity in the catalog is the
  open registry name string ("lww", "yrs",
  core/src/property/backend/mod.rs:114-137), not a closed BackendKind enum;
  #236's YrsText/YrsMap/YrsArray granularity is expressed as value_type
  under backend "yrs". A closed enum in proto would contradict #267's
  open-implementor direction.
- **#268/#274 (ingest, validated ingress):** no dependency either direction
  (5.6 catalog-lag design). The catalog becomes an input to #274's
  admission checks if and when policy wants it.
- **#272 (transactional visibility):** design is correct under per-entity
  integration (the weakest case), so it does not await #272's decision.
- **#265/#266 (comparison core, indexed causality):** no interaction found;
  generation numbers index the event DAG and never touch property identity,
  and this design adds no comparison-path work. Listed because #289 asked
  for the full precedent sweep.
- **#271 (history lifecycle):** catalog entities are low-churn; sealing
  interactions are nil near-term. One forward note: GDPR-style erasure of a
  property (not just its values) would be a catalog tombstone plus per-
  entity scrubbing, which belongs to #271's erasure question.
- **#291 (docs):** the schema-evolution chapter explicitly waits on this
  RFC's direction (per #296); sections 5.6 and 5.8 are its seed.
- **#295 (test gaps) and per-field addressing:** property entity ids give
  the View-side field subscription (tests/tests/basic.rs:51-54) and future
  per-field wire subscriptions a stable key; no commitment here beyond not
  foreclosing them.

## 8. Validation of #289's case list

| Case | Mechanism here | Lands |
|---|---|---|
| Empty-string family (#175, #236) | Read-path rules keyed by registration; zero-op creation events (5.4) | Phase A |
| Schema evolution | Stable identity under rename (5.8); retype = new identity, fail-visible reads (5.1, 5.4, 5.6); ids on the wire | Phase A (SQL rename DDL follows in Phase C) |
| SQL column identity | Catalog-bound columns, rename DDL, collision suffixes (5.5) | Phase C |
| Cross-node strategy agreement | (backend, value_type) in the identity key; divergence catalog-visible AND data-separated (id-keyed payloads) (5.6) | Phase A |
| Per-field addressing | Property entity id as the stable key for signals, policy, indexes (5.7, 7) | Foundation only; consumers later |
| Introspection | Catalog collections queryable with ordinary predicates (4) | Phase A |

## 9. Phasing (design-level; detailed ladder after sign-off)

- **Phase 0: #294.** Protocol version in the Presence handshake;
  refuse-vs-degrade semantics decided and implemented. Gates Phase A; the
  implementation session does this first.
- **Phase A (one protocol epoch; durable nodes upgrade first):** the three
  catalog collections (model, property, membership), deterministic ids
  with the frozen genesis encoder and create-with-derived-id plumbing
  (5.1), the registration protocol operation with policy gate and
  receiver-side catalog protection (4, 5.2), catalog subscription and map
  with wait_catalog_ready and hard_reset flushing (5.2), LWW diff v2 and
  state 0xA2 keyed by property id with the legacy name-keyed fallback and
  lazy rewrite (5.5), ankql Identifier on the wire with resolution
  preceding serialization and receiver-side validation pass-through
  pending #274 (5.3), read-path rules with the sibling gate (closes #175,
  including zero-op creation events), reserved-prefix enforcement,
  UnknownProperty and TypeSkew errors, anchor-reuse guard (5.8), explicit
  id-binding attributes for models and properties including
  shared-property membership (5.9; the attribute is parsed and enforced
  here, while portable AUTHORING of id values gets ergonomic with Phase
  D's codegen/DDL tooling). Yrs stays name-rooted by decision (5.5).
- **Phase C (engine-local, no wire gate):** sled property_config rekey;
  postgres/sqlite catalog-bound columns with rename DDL and collision
  suffixes; IndexedDB re-materialization on rename; all while keeping the
  canonical-vs-materialization write seams clean per 4a (#304's
  consumer).
- **Phase D:** the declarative schema document and differ (#301),
  catalog-to-binding codegen (#302), anchor-attribute tooling (rename
  detection, divergence warnings), per-property policy keys when the
  #264/#274 consolidated surface exists, per-field addressing consumers.
  Schema-evolution follow-ons tracked as #303.

(Phase B no longer exists: rev 3 folded the query-AST wire change into
Phase A.)

## 10. Renegotiations of #85, collected

1. AC2 trigger granularity: per model at first use, not per accessor
   (5.2). The upsert key itself is exactly AC2's (collection, name, type
   identifier), concretized as (model entity, anchor name, backend,
   value_type) under section 4's normative mapping.
2. AC2 read-path timing: read-only usage derives and caches but does not
   durably upsert; the durable write happens at first mutating use or
   explicit register() (5.2). AC2's parenthetical had read accessors
   registering durably.
3. AC4 vehicle: a new resolved `Identifier` node produced by a resolution
   pass, rather than mutating `PathExpr` in place (5.3); PathExpr remains
   the parse-time form.
4. Stretch AC vehicle: satisfied by two mechanisms rather than one. Literal
   entity-id references exist as required (`#[property(id = ...)]`, 5.9,
   also the sharing vehicle); the anchor attribute (5.8) is the recommended
   form for derivation-authored properties because it keeps one binary
   portable across systems whose root-scoped ids differ.

Everything else in #85 is adopted as written, including the
`_ankurah_property` collection name (AC1) that PR #236 had moved away from.

## 11. Open questions (for #289 discussion)

1. **Ulid timestamp bits in derived ids** (5.1): implementation must verify
   nothing interprets EntityId timestamp bits; if something does, fix the
   dependence. (Resolved in design: full 128 hash bits, never truncate.)
2. **Registration writes from read-only contexts** (5.2): RESOLVED
   2026-07-05, strictly cache-only for readers; durable entries appear at
   first mutating use or explicit register().
3. **sys::Item::Collection disposition** (4): superseded by the
   Collection-vs-Model terminology deconfliction, tracked as #305
   (maintainer direction: Collection becomes a storage-only concern;
   recommendation there is to delete the never-constructed variant rather
   than rename it to Model).
4. **Phase A rollout window** (5.5): RESOLVED 2026-07-05 and then
   superseded by rev 3's receiver-side structural protection (section 4):
   durable nodes upgrade first and reject ordinary transactions against
   catalog collections regardless of sender version, so the window reduces
   to deployment order.
5. **PR #236 disposition** (7): RESOLVED 2026-07-05, closed as
   superseded; Phase A implemented fresh.
6. **DDL document format** (5.10): deferred to Phase D, tracked as #301
   (format decision and the ankql query-surface question live there).
7. **Cross-system binding practice** (5.9): deferred to Phase D tooling,
   tracked as #302; anchors remain the recommended portable form
   meanwhile.
8. **Membership retirement** (4): tombstone semantics deferred to
   schema-evolution work, tracked as #303 (together with the
   declared-coercion retype escape hatch from 5.6/5.9/6). The collection
   excision, excluded from this project (4a), is tracked as #304.

(Resolved during adversarial review, recorded in their home sections: yrs
root strategy = defer rekeying, 5.5; LWW required-missing = uniform default
under the sibling gate, 5.4; relay validation = pass-through until #274,
5.3; optionality-unknown fallback = optional, 5.4.)

## 12. References

- Issues: #85 (requirements), #289 (umbrella), #175 (bug family), #294
  (protocol version), #296 (sequencing), #265/#266/#267/#268/#271/#272/#273/#274
  (phase-2 RFCs), #264 (commit-time hook), #291 (docs), #295 (test gaps).
- PR #236: specs/property-registration/{spec,plan,tasks}.md, proto sys::Item
  expansion, core ValueType conversions (branch
  fix/175-empty-string-missing-property).
- In-repo: specs/unified-refs-edges-json/phase-3-schema.md (schema registry
  plan, reconciled in section 7); specs/concurrency/phase-2.md
  (conventions); LONG_TERM_ASPIRATION.md (hypergraph trajectory, 4a).
- Code (all at 05593d0d): core/src/system.rs (catalog precedent);
  proto/src/data.rs (EventId content addressing, Event/OperationSet/State
  shapes); proto/src/id.rs (EntityId/Ulid); proto/src/sys.rs (Item);
  proto/src/auth.rs (Attested); proto/src/peering.rs (Presence, no version
  field); proto/src/request.rs (Fetch/SubscribeQuery carry ast::Selection);
  ankql/src/ast.rs (PathExpr, assume_null); core/src/selection/filter.rs and
  core/src/type_resolver.rs (evaluation and resolution semantics);
  core/src/entity.rs and core/src/transaction.rs (creation paths, phantom
  guard, apply dedup); core/src/property/backend/{lww,yrs}.rs (encodings,
  versioning, field broadcasts); core/src/property/mod.rs (PropertyName,
  Option projection); derive/src/model/{model,description,backend_registry}.rs
  and core/src/property/value/{lww,yrs}.ron (codegen, active-type
  selection); core/src/policy.rs (PolicyAgent surface);
  storage/{sled,postgres,sqlite,indexeddb-wasm} (engine keying).
- Book: concurrency section (event DAG, LWW versioning) at ankurah.org;
  internals source docs/internals/.
