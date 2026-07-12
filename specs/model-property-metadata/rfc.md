# RFC: model and property metadata (defining entity per model and per property)

Status: RATIFIED (maintainer sign-off 2026-07-05, recorded on #289):
direction, all nine axes, and the rev-2 contract/membership model stand;
implementation ladder (plan.md/tasks.md) is the next deliverable.
Rev 3 (maintainer direction, same day): registration becomes a protocol
operation executed by durable nodes; catalog protection is enforced
receiver-side and structurally; the data contract is id-keyed from the
FIRST landing, which promotes #294 to Phase 0 of the ladder (sections 4,
5.2, 5.3, 5.5, 9).
Rev 4 (maintainer ruling 2026-07-06; RATIFIED same day on #289 after the
full design walkthrough, and IMPLEMENTED on PR #307): the IDENTITY PLANE
is redesigned. Deterministic derivation, the frozen genesis encoder,
genesis self-certification, and the anchor apparatus are REMOVED; the
durable node ALLOCATES true EntityIds (real ULIDs) on first sighting, via
an upsert-by-current-name RegisterSchema whose response returns the
allocated definitions (sections 3, 4, 5.1, 5.2, 5.8). The delivered data
plane uses uniform `PropertyKey`, LWW v2/0xA2, model-id envelopes, resolved
query identifiers, and protocol v3. Schema-dependent predicate paths register
at first use; direct entity-id gets only cache and load. Unresolvable
references FAIL LOUD (25b second ruling; the defer-empty draft is superseded).
Originally: DRAFT for review on #289. Scope agreed 2026-07-05: each model
gets its own defining entity id, and each property gets its own defining
entity id; model and property metadata become first-class, replicated
entities rather than compile-time-only facts. This document decides the
design axes and proposes a phased landing order.

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
reviewers: code-reality, distributed-behavior, requirements-coherence).
Surviving findings are integrated below; the frozen genesis encoder,
anchor-reuse guard, and later the read-time sibling gate were removed as the
design moved to allocated identities and canonical value types.

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
  A protected-collection constant exists but is read by no code at all
  (the pre-implementation `core/src/system.rs:21`).
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
  id on the model. (REN in rev 3 via anchor names; rev 4 removes anchors --
  renames ship a transient migration hint, and literal-id references remain
  the 5.9 binding mechanism; section 5.8.)

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

- **Coordination confined to first contact (REN, rev 4).** Revs 1-3
  required registration to work offline-first with zero coordination, which
  is what forced derived identity. The maintainer ruling (2026-07-06)
  renegotiates the constraint: FIRST registration of a definition requires
  a round trip to the allocating durable node, and creating entities in a
  never-registered collection while offline is a strict error ("connect
  once first", 5.2). Data writes remain offline-capable only when every field
  in the exact compiled schema already has a compatible canonical binding in
  the local catalog. Concurrent registration by independent nodes converges
  because one allocator serializes it, not because the bytes are
  deterministic.
- **Mixed fleets are the normal case.** Nodes running different model code
  versions (properties added, renamed, retyped) must not corrupt each other.
  Protocol v3 now enforces the epoch boundary in the Presence handshake:
  incompatible peers exchange a refusal and do not register a connection.
- **Bootstrap must not recurse.** Reading metadata entities must not require
  metadata entities.
- **Respect the #267 boundary.** The PropertyBackend trait, layer-view API,
  and conformance kit belong to #267. This RFC changes what identifies a
  property inside backend payloads, not the backend contract's shape.

## 3. Design overview

Three fixed-name catalog collections, `_ankurah_model`, `_ankurah_property`,
and `_ankurah_model_property` (contract membership), hold ordinary entities
whose properties are plain LWW values. Their entity ids are ALLOCATED by the
durable node (true EntityIds, real ULIDs) the first time a definition is
sighted: registration is a lookup-or-create upsert by current name, and the
registration response returns the allocated definitions, which the client
folds into its catalog map immediately. Registration happens automatically
on schema-dependent first use. Resolution of a property reference consults
the catalog resolver (the compiled schema supplies names and types; ids exist
only in the catalog), and fails closed when the property is
undefined. Registration travels as a dedicated protocol operation, not as
ordinary entity writes; the durable node policy-checks it and executes it
as the system's single allocator (4, 5.1). The data contract is id-keyed
from the FIRST landing: LWW
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

One more framing governs everything engine-shaped below (maintainer,
2026-07-06): ankurah is a PROPERTY GRAPH system, and property declaration
works at the graph level -- a property entity is a graph fact, and entity
values are id-keyed data attached to graph nodes. Where the postgres and
sqlite engines materialize values into relational-looking tables, that is
a function of those engines' indexing strategy (and an intelligible
window for administrators peeking at the data), never the data model.
Tables, column names, and column-collision handling are engine-local
materialization concerns (5.5, Phase C); nothing in the catalog, the wire
contract, or resolution semantics depends on them.

## 4. Catalog shape (where metadata entities live)

**Decision: dedicated fixed collections, per #85 AC1.** `_ankurah_model` and
`_ankurah_property`, ids constructed via `CollectionId::fixed_name` like the
system collection (core/src/system.rs:121). Metadata entities are ordinary
entities: LWW properties, normal events, normal state buffers, normal
subscriptions. Nothing about their storage or replication is special-cased.

Three catalog collections (the third is what makes models CONTRACTS rather
than owners; see the trajectory in 4a):

Property entity (`_ankurah_property`; standalone, shareable; see 5.1 for
the upsert lookup key):

- `name: String` (current display name; part of the lookup key at
  registration time, mutable thereafter via renames)
- `backend: String` (backend registry name, e.g. "lww", "yrs")
- `value_type: String` (language-agnostic value type, e.g. "string", "i64",
  mirroring core::value::ValueType's variants)
- `minted_for: EntityId` (the model entity in whose scope this property was
  first allocated; PROVENANCE and lookup scope, expressly NOT ownership;
  a property bound by explicit id from another model is unaffected by it)
- `target_model: Option<EntityId>` (for reference-typed properties; mutable
  metadata, not identity)

Model entity (`_ankurah_model`; a named data contract):

- `collection: String` (the collection id this model's entities currently
  live in; the model lookup key at registration time. Under unified storage
  this demotes to a routing/view concern, 4a)
- `name: String` (display name, initially the struct name; mutable)

Membership entity (`_ankurah_model_property`; one per (model, property)
pair; the contract edge):

- `model: EntityId`
- `property: EntityId`
- `optional: bool` (PER CONTRACT: model A may require a property that model
  B, sharing it, treats as optional; this is why optionality lives here and
  not on the property)

Memberships are looked up by (model id, property id) and allocated on miss
(5.1), so registration of different fields by nodes running different
versions of one model creates DISJOINT membership entities that union
cleanly; a
membership list stored as one LWW vector on the model entity was considered
and rejected because concurrent adds would collide as a single register and
drop fields. Retiring a field is a tombstone flag on the membership (future
schema-evolution work), never deletion of the shared property entity.

**The Rust-type mapping is normative.** The first allocation fixes a
property's canonical (backend, value_type), and every later registration
declares the pair that the binary will actually read and write. The executor
requires an exact backend match and mutually castable value types; it never
changes the canonical pair or allocates a second identity for type drift.
The mapping is adopted from PR #236's plan.md table, restated against the
actual active types:

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
compiled schema, the registration request, and the catalog all carry the
declared type. The invariant is documented on the const: VALUE_TYPE must
equal the `Value` variant `into_value` produces. Changing it for a shipped
type is a compatibility-checked struct-level retype; it never changes the
property's canonical type or identity.

value_type strings are the lowercased core::value::ValueType variant names
(REN vs #236's plan.md prose, which listed a separate `ref` variant; the
PR's actual proto ValueType has EntityId and no ref, so references are
(value_type = "entityid", target_model = Some) here, following the diff over
the prose). The `optional` column feeds the MEMBERSHIP record, not the
property entity, and deliberately does NOT enter any identity key: making a
field Option<T> or not must not re-key the property or the membership. Future backends and
value types extend this table; extending it is a spec change, because two
nodes disagreeing on a mapping row may either be refused at registration or
operate through casts against the one canonical stored type.

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

AMENDED (PropertyKey amendment, #289, 2026-07-07): system and catalog
collections stay name-keyed through the `PropertyKey::Name` variant -- a data
fact about those collections' keys, not a per-instance wire mode. The deferred
upgrade path to hardcoded well-known constant ids for the meta-schema stays open
and non-load-bearing (this paragraph already records that nothing depends on
materializing it). See plan.md decision 27.

**Protection (rev 3: structural and receiver-side).** The pre-implementation
protected-collection constant is dead code: no reader exists
(`core/src/system.rs:21` at the time, exhaustive grep). This RFC makes the
classification real and structural: the catalog collections
(`_ankurah_system`, `_ankurah_model`, `_ankurah_property`,
`_ankurah_model_property`) are NOT mutable through ordinary transactions at
all. A receiving durable node rejects any CommitTransaction event targeting
them outright, regardless of the sender's software version; the only
mutation path is the registration operation (5.2), which the durable node
policy-checks and executes itself. Catalog events replicating between
peers are trusted FROM THE SERVING PEER the way every other served event
is: the write ban covers the transaction paths, while the
subscription/delta ingest paths carry no allocator-identity check -- in
the single-allocator topology the serving durable IS the allocator, so
the trust boundary is the peering relationship itself, and
allocator-identity enforcement for multi-peer topologies rides #309's
routing work (validate_received_event/state are the per-agent hooks for
deployments that want to gate ingest earlier). Rev 4 deleted the content
self-certification that rev 3 attached to genesis events (with derivation
gone there is nothing to recompute); single-allocator authority plus the
transaction-path write ban replaces it. The `_ankurah_` prefix is
reserved and rejected for user-model collection ids at derive time and at
CollectionSet::get. Because enforcement sits on the receiving durable node,
mixed-fleet safety is a deployment ORDER (upgrade durable nodes first), not
a simultaneous-upgrade requirement.
AMENDED (model-id envelope amendment, #289, 2026-07-09): with collection
names gone from the event envelope (5.5), the receiver guard keys on the
well-known model ids (core/src/schema/mod.rs, well_known_collection) instead
of collection strings; the protected set is unchanged.
AMENDED (review correction, #289, 2026-07-10): keying on the wire model id's
STATIC well-known-ness was found to be bypassable, because the actual write
target is the collection that id RESOLVES to through the (mutable) catalog map;
a poisoned map entry -- a non-reserved model id routed to a catalog collection
-- walked past the static check. The guard now resolves every CommitTransaction
event's model up front and rejects the transaction if any resolves to a
collection accepted by `schema::is_protected_collection`, before any event is
written (registration writes the catalog through a direct commit path that
bypasses this ingress guard). The
descriptor-ingest path was hardened to match: shipped catalog defs are ingested
only after the recipient/connection checks, a wire model def naming a reserved
collection is rejected, and a wire def cannot rebind a collection already mapped
to a different model id.

## 5. Design by axis

### 5.1 Identity allocation (the crux)

**Decision (rev 4, superseding rev 3's derivation): durable-allocated
ids, upserted by current name.** The durable node executing RegisterSchema
is the system's ALLOCATOR: for every definition it has never seen it mints
a fresh `EntityId::new()` -- a true ULID with real timestamp and
randomness -- and returns the allocated definitions in the response (5.2).
Identity is the allocated id, full stop; every name is metadata.

The lookup-or-create keys (the upsert keys, matching #85 AC2's
collection + name + type identifier verbatim):

- model: by `collection`
- property: by (model id, current `name`, `backend`, `value_type`) --
  keeping the type pair in the key preserves retype-mints-new-identity
  and the 5.4 sibling gate: a String "title" and an I64 "title" are
  different properties, which is the collision-safety #85 asks for.
  Flipping `optional` or retargeting a reference does NOT re-key.
- membership: by (model id, property id)

AMENDED (canonical value_type ruling, #289, 2026-07-10): backend and
value_type LEAVE the property lookup key -- the lookup is (model id,
current `name`) -- because keeping the type pair in the key made a
struct-level retype fork a second same-name identity (see the 5.6
amendment, which supersedes that design). Collision safety now comes from
the compatibility gate at the hit: a registration declaring a different
(backend, value_type) never mutates the found definition and never mints;
it is admitted when the backend matches and the value types are mutually
castable per the Value::cast_to relation (core/src/value/cast.rs), and
refused loudly otherwise. Flipping `optional` or retargeting a reference
still does not re-key.

A miss creates the entity via ORDINARY events through the normal commit
machinery (no frozen encoder, no special genesis; with nothing
byte-frozen, the creation event simply carries the full definition
state). A hit emits
provenance-ordered follow-ups ONLY where the requested metadata differs
from the catalog, parented at the entity's current head (the rev 3
follow-up machinery, plan decision 18, survives verbatim); an unchanged
re-registration is a true no-op, zero events.

The model id in the property lookup key is the MINTING SCOPE, not
ownership: it namespaces by-name registration so unrelated models
declaring the same field name never converge on one property entity by
accident (`minted_for` records it as provenance). A model that intends to
SHARE another model's property binds it by explicit id (5.9), which skips
the by-name lookup entirely; explicit-id BINDING never mints, it
references an id that must already exist. DDL-authored definitions (5.10)
flow through the same upsert.

**Executor discipline (normative).** The upsert runs under a process-local
mutex on the allocating node. The durable node's own catalog map is
reactor-fed (post-commit, asynchronous), so the executor MUST NOT race
itself: after committing an allocation it updates its lookup state
synchronously (a mutex-held map upsert or an allocator-side name-to-id
cache) BEFORE releasing the mutex, so the next request in line observes
the allocation regardless of reactor lag.

**Single-allocator authority.** One durable node per system executes
RegisterSchema. Two independent allocators could double-allocate a name
(two ids for one definition); systems with multiple durable nodes MUST
route RegisterSchema to one of them (documented on #309; a real allocator
protocol -- leases or consensus -- is future work there). This replaces
rev 3's convergence-by-construction: registrations converge because one
allocator serializes them, not because the bytes are deterministic.

**Why allocation over derivation (maintainer ruling, 2026-07-06).**

1. Derived ids were SHA-256 bytes shoehorned into the EntityId Ulid
   newtype: no timestamp, no randomness, a documented type fib. Allocated
   ids are honest ULIDs; the old open question about Ulid timestamp
   semantics (11.1) dies outright.
2. Derivation keyed identity on the ANCHOR, a fossil of the first field
   name, which is the sole reason the whole rev 3 5.8 apparatus existed
   (an anchor attribute carried in source forever, the anchor-reuse guard,
   the anchored descriptor bit, rename-back semantics). Allocation keys
   identity on nothing: names are lookup inputs at registration time and
   mutable metadata afterwards.
3. Derivation over-converges: re-registering a name after an excision
   silently RESURRECTS the old identity. An allocator never resurrects; a
   new sighting is a new entity.

**What allocation gives up, stated honestly.** First registration is a
coordination point: it requires the allocating durable node (section 2's
renegotiated constraint; the offline consequences and the strict
never-registered error are specified in 5.2). Ids remain per-system --
allocations by one system's durable node -- which derivation's root
scoping also imposed, so nothing is lost there; source code still cannot
portably embed a metadata entity id, and the portable rename form is the
by-name hint (5.8), with explicit-id binding (5.9) as the per-system
precision tool. PR #236's deferred determinism note ("Entity ID stability
- Runtime lookup by name for now; future annotation for determinism") is
resolved the other way: runtime lookup by name IS the design, executed
once per system by its allocator.

**Plumbing note.** Registration events are ordinary events: validated,
policy-checked, attested, persisted, and relayed exactly like any commit
(check_event gate, core/src/context.rs:129). What remains special is the
transport (a protocol operation rather than a client transaction, 5.2),
the executor's allocation authority, and the response carrying the
allocated definitions. Rev 3's create-with-derived-id plumbing reduces to
ordinary entity creation inside the executor; catalog entities still
integrate before, after, or without the entities their references name
(membership references are not causal parents), so registration ordering
in 5.2 remains hygiene, not a correctness requirement.

### 5.2 Registration lifecycle

**Trigger: model first-use per process on schema-dependent paths (REN 2,
as implemented).** Create, edit/commit, predicate fetch, query, and
subscribe ensure registration of M, all its declared active fields
(ephemeral fields are excluded, derive/src/model/description.rs:31-40),
and the (M, property) membership records. The node checks the local
catalog map and, if the binding is absent or doubtful, issues the
registration operation and AWAITS its response; the returned definitions
are upserted into the catalog map immediately on ack, so id-keyed writes
and predicate resolution proceed behind the same canonical binding.
Because the operation is an idempotent UPSERT (below), a path whose schema
the authority already carries resolves to a no-op plan: nothing is emitted,
the policy verb is skipped (5.7), and the response simply feeds the map.
This closes the replica-lag window in which a warm-but-lagging catalog
replica could otherwise answer against an incomplete binding. The catalog
subscription is a CACHE -- an accelerator and offline enabler (the
ephemeral catalog queries run cached) -- never the arbiter: registration is
the doubt-resolver.

A direct get by entity id is the deliberate exception. It does not need a
predicate binding to locate the row, so it loads the entity without minting
or awaiting schema registration. If that view is later edited, the transaction
records its exact compiled schema and commit ensures it before resolving staged
keys or writing. Typed projection follows the ordinary property rules,
including `Missing` or per-value `NonCastable` where applicable.
When registration cannot run on a schema-dependent path (policy denial, no
durable peer), that path FAILS LOUD with the registration error; mutating
paths enforce strictly before a write lands. An explicit
`ctx.register::<M>().await` issues the same operation eagerly.

**Transport: registration is a protocol operation, not a client
transaction (rev 3), and the operation is an UPSERT with a response
(rev 4).** A dedicated request (a new NodeRequestBody variant beside
Fetch/SubscribeQuery, proto/src/request.rs:119-128; RegisterSchema)
carries the language-agnostic definitions: models (collection, name),
properties (name, backend, value_type, rename hint per 5.8, explicit id
per 5.9), memberships (model, property, optional); reference-typed
properties name their target model by COLLECTION, resolved executor-side.
The receiving durable node policy-checks the request (the data-freedom
gate from above; PolicyAgent styles legitimately vary, maintainer note
2026-07-06 -- some discriminate on the PRINCIPAL, who may define schema,
others on the OBJECT, which model/property definitions are writable at
all, others both) and then EXECUTES the upsert itself under the 5.1
mutex: look up each definition by its lookup key, submit the resolved
plan to `check_schema_registration` (5.7: the exists-aware gate, so
agents judge actual creations without their own lookups), allocate
`EntityId::new()` on miss, emit
ordinary creation events and difference-only follow-ups through
check_event like any write (core/src/context.rs:129), persist, relay, and
respond with NodeResponseBody::SchemaRegistered carrying the FULL
resolved definitions (models, properties, memberships, each with its
allocated or existing id). Catalog collections are not writable any other
way (section 4 protection). A target-model reference whose collection has
never been sighted allocates the model entity on the spot (collection
set, display name arriving whenever that model properly registers), which
preserves #236's circular-reference resolution under allocation.
Consequences: a durable node needs no model code to serve registration
(the request carries everything); execution is idempotent because it is
an upsert (a repeat registration finds every key, emits zero events, and
returns the same ids); a durable node that itself runs model code
registers by executing the same operation locally. OFFLINE ephemeral
flow (rev 4, maintainer-ruled trade): a collection that has NEVER been
registered on this node cannot be created into while offline; create or
commit fails with a strict, actionable error ("connect once first").
Data writes keep working offline only when every field in the exact compiled
schema already has a compatible canonical binding: the ids are known and
events queue exactly as today. A merely known collection, a missing field, or
an incompatible declaration still fails loud. There is no offline
registration queue; the rev 3 queue-and-drain machinery is deleted with
derivation.

Two load-bearing rationales (maintainer, 2026-07-05) that any refinement
of this lifecycle must preserve: (a) DATA FREEDOM: authorized users define
whatever models and properties they see fit, subject only to PolicyAgent
approval; schema definition is an ephemeral-node capability, never a
durable-node privilege (collisions are handled by the upsert: identical
definitions resolve to one entity; a same-name declaration with a different
type is checked against that entity's canonical type and admitted only when
mutually castable). (b) Durable nodes cannot be
assumed to have model definitions AT ALL: model code lives on clients;
registration must originate from ephemeral nodes and carry everything the
durable side needs.

REN vs #85 AC2, twice, both disclosed (section 10): (a) granularity is per
MODEL, all properties in one transaction, not per accessor touch; the derive
macro statically enumerates fields (it already does for
initialize_new_entity, derive/src/model/model.rs:35-40), and per-accessor
granularity would leak partial schemas and buys nothing. (b) predicate reads
register at first use because resolution requires canonical property ids;
direct id gets are cache/load-only because locating a known entity id does
not require a predicate binding. REN vs PR #236's "on first trx.create
only": query paths also resolve through registration, because predicates
need ids before any create happens on this node.

**Ordering within the registration execution:** model entities, then
property entities, then memberships, extending #236's two-phase plan. The
executor resolves the whole request under one mutex hold, so this is
internal sequencing, not a wire contract; membership references are plain
EntityId references, not causal parents (5.1), so catalog entities
integrate in any arrival order on other nodes.

**The catalog map (AC3).** Each node maintains an in-memory map, warmed by
subscribing to the three catalog collections (predicate True) once
system-ready, and fed IMMEDIATELY by SchemaRegistered responses (the
response is the fast path; the subscription converges everyone else): by
(minting model, current name) to property entity id, by id to canonical
definition, and by model id to its membership set (the contract). Backend
and value_type are compatibility inputs on a lookup hit, never identity
inputs.
Name-based lookup of models and properties, one of the maintainer's
required paths, is this map; it answers "the property named X in
collection C" through the model's memberships. This is an ordinary
LiveQuery-backed subscription, the same machinery applications use.
Durable nodes have the catalog locally; ephemeral nodes get initial state
plus updates through the standard subscription relay. Registration events
arriving from peers update the map like any changeset. Two normative
obligations from adversarial review, both surviving rev 4:
(a) a `wait_catalog_ready` gate, analogous to wait_system_ready
(core/src/system.rs:97-101): a consumer with no local compiled schema (a
relay, a dynamic binding) DEFERS resolution until the initial catalog
snapshot lands rather than failing UnknownProperty against a cold cache;
(b) `hard_reset` MUST flush the catalog map and every cached schema
binding along with the state it already clears
(core/src/system.rs:208-234), because allocated ids belong to one system
and a node that hard-resets into a different system must re-register
against that system's allocator; a stale cache would leak the old
system's ids into the new one. A durable node reused in place remains
catalog-unready until its replacement system root is ready, then starts one
warm/listener for the current reset generation. A superseded generation MUST
NOT publish readiness or catalog state afterward.
Every catalog effect that can overlap reset is owned by that system epoch:
the durable storage warm, the ephemeral setup and cached local activation,
admitted relay responses/updates, and schema registration. Hard reset first
invalidates their fences and drains admitted work, then deletes storage and
clears the map/bindings. Registration remains closed until a ready system root
rearms it; a forwarded `SchemaRegistered` response retains its admission lease
through both the map fold and exact compiled-schema latch. An ephemeral warm
may not claim the rootless reset-to-join gap. System load/create/join/reset
transitions are serialized; if a reset future is canceled while draining, the
invalidated owner fences remain attached to the reset state so its retry must
finish the same drain before deletion. An incomplete-reset marker also survives
cancellation or deletion failure; load, create, and join must resume that reset
before reading or publishing a system root, including when physical deletion
completed before the canceled future returned.

**Idempotence across restarts and peers.** Ensure-registration re-issues
RegisterSchema whenever the local map lacks a binding; the executor's
upsert finds the existing entities, emits zero events (or
difference-only follow-ups), and returns the same ids. Follow-up
metadata (optional, target_model) re-asserted by a node that missed
prior state merges as LWW.

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

produced by a resolution pass that binds `steps[0]` against the queried
collection's catalog definition (the compiled schema contributes names and
types; ids exist only in the catalog and its registration
responses, 5.2) and leaves the remaining steps as the JSON sub-path (to be typed against the phase-3 schema plan's StructuredKey model
when that lands; section 7). Resolution failure IS the AC5 fail-closed
behavior: predicate building returns an UnknownProperty error naming the
collection and property. This replaces today's three inconsistent
missing-property behaviors (hard evaluation error,
core/src/selection/filter.rs:59-91; reactor unwrap_or(false),
core/src/reactor/subscription_state.rs:391; SQL assume_null,
storage/postgres/src/lib.rs:528-548) with one rule: an UNRESOLVABLE
reference fails at build time; a resolvable reference whose property is
absent on a given entity evaluates as NULL in predicates and policy. Required
defaults belong only to typed View getters under 5.4. It also subsumes the
known assume_null/referenced_columns
first-vs-last-step inconsistency on JSON paths (ankql/src/ast.rs:241-333
keying on path.property() vs path.first(); acknowledged in a sqlite comment,
storage/sqlite/src/engine.rs:462-468), because the resolution pass fixes
which step is the property once, in one place.

**The unregistered-collection rule (rev 4 corollary, revised 2026-07-06
with the REN 2 second ruling and its fail-loud corollary; CONFIRMED by
the maintainer 2026-07-07 after a read-path semantics walkthrough,
recorded on #289).** Rev 4 makes creation impossible without registration.
But a warm catalog REPLICA cannot by itself prove non-registration --
it may simply lag the authority by a subscription hop -- so resolution
never renders a verdict from the replica alone: a compiled model
REGISTERS AT FIRST USE (5.2), and the upsert response is the
authoritative answer (existing rows on the no-op, fresh rows on
allocation), after which resolution proceeds normally. When first-use
registration cannot run -- the principal may not define schema (policy
denial), or no durable peer is reachable -- the reference FAILS LOUD as
UnregisteredCollection on fetch and live query alike (maintainer
ruling, superseding this addendum's earlier defer-empty draft): a
lagging cache cannot prove emptiness, and a subscription that can never
answer truthfully (the earlier draft forwarded a Predicate::False
placeholder) serves no one; callers retry once the schema is registered
or connectivity returns. An unknown property within a REGISTERED
collection, or a reference no compiled schema anticipates, equally
fails closed at build time -- AC5's letter is untouched where it can
catch a real mistake.

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

The current rule is deliberately small; type and identity admission happen
at registration, not while reading:

1. Query and policy property references resolve to a property id before
   evaluation. An unknown reference fails closed with `UnknownProperty`.
2. Generic backend dispatch reads the resolved Id entry first. A present Id
   entry, including a cleared tombstone, is authoritative. Only an absent Id
   may fall back to legacy Name residue.
3. A typed View getter casts the canonical stored value to its compiled type.
   An unrepresentable value returns `PropertyError::NonCastable`. When the
   property is absent, the getter's compiled optionality and Property
   implementation decide `None`, a required default, or `Missing` for types
   such as `entityid` that have no fabricable default.
4. Predicate evaluation and policy inspection canonicalize defensively. An
   absent or uncastable value evaluates as NULL with a warning, never as a
   type error. Data under a different property id is not this field and never
   substitutes for it.

There are no sibling, foreign-data, or backend-specific type gates on the read
path, and `TypeSkew` is deleted. Cross-system data is refused at wire ingress
by model id; an out-of-band copied foreign property id reads as absence.

Optionality remains membership metadata because the same shared property can
be required by one model and optional in another. The Rust View getter obeys
its compiled return type. Predicate evaluation does not fabricate required
defaults and therefore does not need membership optionality; absent is NULL.
Future dynamic typed bindings can consult the membership flag without making
it part of property or membership identity.

The degenerate #175 case (every field empty, so zero operations, so
no creation event and no persisted entity, tests/tests/yrs_backend.rs:300-305)
is fixed by allowing creation events with an empty operation set rather than
skipping event generation; EventId hashes fine over empty operations
(proto/src/data.rs:15-24), and a zero-op genesis is exactly what "an entity
whose every field is default" means. This piece is independent of the catalog.

### 5.5 Wire and state impact (phased identity keying)

The PropertyKey amendment (#289, 2026-07-07) is the current contract. It
replaced the proposal-time `SchemaBinding`, `WireMode`, and bind-at-assembly
design before release. The backend holds no binding or mode; identity is
carried by the key and resolution belongs to catalog-aware callers. Yrs uses
the same PropertyKey contract as every other backend.

AMENDED (model-id envelope amendment, #289, 2026-07-09; implemented on PR
#307 together with the engine column maps under Phase C below): the
data-plane envelope drops collection names entirely. `Event`, `EntityState`,
`EntityDelta`, and `SubscriptionUpdateItem` carry `model: EntityId` -- the
model definition entity's id -- where they carried `collection: CollectionId`
(proto/src/data.rs, proto/src/request.rs, proto/src/update.rs). EventId
already excluded the collection from identity (proto/src/data.rs), so event
ids are unchanged. Collection strings survive only in query/API surfaces and
registration payloads, where the collection is the data (#305). The system
and catalog collections, which have no allocated model entities, use
WELL-KNOWN model ids -- [0u8; 15] plus an ordinal (1 = _ankurah_system,
2 = _ankurah_model, 3 = _ankurah_property, 4 = _ankurah_model_property), a
range no ULID mint can produce (core/src/schema/mod.rs) -- and the section-4
protection guard keys on them. Ingress resolution (Node::resolve_model) maps
a wire model id to its collection via well-knowns then the catalog map; an
unknown model id is REJECTED loud (retryable), never synthesized into a
collection. On a DURABLE node whose startup catalog warm (a storage scan)
has not finished, ingress resolution awaits catalog readiness once and
retries before rejecting (resolve_model_wait) -- a restarted node
receiving traffic immediately must not reject models it allocated before
the restart; this is policy (e)'s readiness participation on the ingress
side. Ephemeral nodes never wait: their definitions arrive inline with
the message, so a miss is already final. Cold-catalog policy, maintainer-ruled as options c + e + d:
(c) once per connection per model, NodeUpdate and NodeResponse attach the
attested catalog entity states (model, memberships, properties) for any
non-well-known model the payload references (`#[serde(default)] schema:
Vec<Attested<EntityState>>`; core/src/node.rs schema_states_for_models /
ingest_schema, tracked per peer in PeerState.announced_models); receivers
policy-validate each definition and ingest them BEFORE processing the body;
definitions never ride inside events or state buffers. (e) Catalog warmth
participates in readiness through the existing ensure_subscribed /
context_async gates. (d) The engines' truncated-id fallback naming (Phase C
amendment below) stays as a loud belt-and-suspenders net that should never
fire. Ships as PROTOCOL_VERSION = 3 (proto/src/peering.rs).

Streaming `SubscriptionUpdateItem`s also carry `source_queries`: the query ids
whose result sets caused the item to be emitted, including steady-state
updates where `predicate_relevance` is empty. A receiver admits the envelope
only while every item has a current source query on the sending peer. For the
catalog's reset-sensitive queries, the receiver holds the generation's
request-fence lease from attached-schema ingestion through delta persistence
and acknowledgement. Hard reset invalidates the old generation, waits for
admitted leases to drain, deletes storage, clears reactor/catalog state, and
only then permits a replacement warm. Queued old responses and updates
therefore fail closed without relying on unsubscribe delivery order.

The delivered storage contract is uniform across property backends. Backends
hold no schema binding or wire mode; every entry is addressed by a tagged
`PropertyKey::{Id, Name}` supplied by the catalog-aware caller.

- LWW diff version 2 and state version 0xA2 each encode one
  `BTreeMap<PropertyKey, ...>`. Legacy diff v1, state 0xA1, and pre-0.9
  buffers still decode, normalizing their string keys to `PropertyKey::Name`.
  Every save emits the current tagged form. A registered user commit resolves
  staged names to `Id` before event generation; system/catalog fields and
  unresolved legacy residue remain `Name`. A present Id entry, including a
  tombstone, is authoritative; only an absent Id may fall back to Name
  residue.
- Yrs names each root from the same `PropertyKey`. New registered fields use
  id-named roots; system/catalog or unresolved fields use name roots. Existing
  CRDT history cannot be re-keyed, so a migrated field whose history still
  exists only under its legacy name continues reading, editing, and observing
  that root. Once an id root exists, it is authoritative.
- Query ASTs carry resolved `Identifier` nodes, and each resolved ORDER BY
  item carries the same stable property id beside its display path. The id
  keeps a relayed/rebuilt sort stable across a catalog rename; the path remains
  the SQL/physical-column hint. Fetch and SubscribeQuery are bincode shapes,
  so this is a hard wire change rather than a local planner detail.

Materialized storage is a separate concern. Every engine maintains a durable
property-id to physical-column map, seeded from the injected catalog resolver
at `set_state` and consulted on reads. PostgreSQL and SQLite persist it in
`ankurah_property_columns`; sled uses `property_columns`; IndexedDB uses a
`property_columns` object store. The first claimant receives the sanitized
display name; a collision appends a widening suffix from the property id.
Renames do not change identity, and physical column assignments remain sticky
until explicit rename/rematerialization work changes them. A selection is
translated into physical column space once at the engine boundary; ORDER BY
retains the property's canonical value_type even when its physical name is a
sticky pre-rename column. Empty scans resolve the model lazily, so an absent
row reports `EntityNotFound` rather than a cold-catalog error. The durable
model-to-table map remains deferred with #304.

These changes ship together behind protocol v3. Presence negotiation refuses
incompatible peers before registering a connection; the same epoch covers
the model-id data envelopes, registration messages, resolved selections, and
backend payload changes. Durable nodes upgrade first. Engine column maps are
node-local, but they consume the catalog contract introduced by that epoch.

### 5.6 Mismatched model code across nodes

- **Same name, different type or backend (the #289 strategy-agreement
  case):** different (backend, value_type) means a different lookup key,
  hence distinct allocated property ids (5.1). The two versions write to
  DIFFERENT properties that happen to share a display name. From Phase A the divergence is both VISIBLE (both entities
  queryable in the catalog, tooling can warn) and structurally
  un-collidable at the data layer, since payloads key by id from the same
  epoch (rev 3; previously these arrived in separate phases). During a retype rollout window, queries resolve
  per-node against each node's own schema (its own id), so the two fleets
  see different subsets for the affected property until the fleet converges;
  5.4's sibling gate keeps reads fail-visible rather than silently defaulted
  through that window. A declared-coercion migration ("retyped_from", where
  new code reads the old property id through a cast) is future
  schema-evolution work this design leaves room for but does not include.
  AMENDED (canonical value_type ruling, #289, 2026-07-10): SUPERSEDED before
  first release -- the fork-identity design above never shipped. A property's
  (backend, value_type) is CANONICAL: fixed at first allocation and never
  changed by registration (the lookup key drops the type pair, 5.1
  amendment). A same-name registration is a COMPATIBILITY check, not a fork
  and not an update: the backend must match, and a drifted value_type is
  admitted only when mutually castable with the canonical one per the
  Value::cast_to relation, else the registration refuses loudly. An admitted
  drifted binary operates entirely through casts: its commits canonicalize
  staged values INTO the backends (writer-side, so engines and indexes
  collate exactly one type per property -- the motivating requirement, since
  cast-at-comparison alone cannot cure a mixed-type stored population; a
  value the canonical type cannot represent fails that writer's commit), its
  getters cast canonical values back to the compiled type (per-value
  failures surface as `PropertyError::NonCastable`), comparison literals
  canonicalize in the resolution pass, and readers defensively canonicalize
  at the read/evaluation boundary (ingest stays schema-blind per the
  catalog-lag rule below, so a legacy or ill-typed payload under a known id
  evaluates as NULL with a warning rather than poisoning the read). A
  struct-level retype is therefore durably INERT: the canonical type does
  not follow the code, and registration logs the drift. Changing a canonical
  type is a deliberate migration operation (#303), never a model-struct
  edit. One live property per (model, name) becomes an allocator invariant,
  and the 5.4 read-time gates are removed outright (see the 5.4 rule-4
  amendment of the same date): registration is the sole admission point. The SchemaRegistered response carries the CANONICAL (backend,
  value_type) so a drifted requester's catalog map holds its cast target;
  explicit-id binding verification (5.9) admits the same castable drift.
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
like any write (core/src/context.rs:129), so agents can gate registration
on the principal (who may define schema) or on the object (which
definitions are writable) -- both are legitimate PolicyAgent styles and
the surface must not privilege one (maintainer, 2026-07-06); (b) a
dedicated registration verb (maintainer direction, 2026-07-06):
`check_schema_registration(node, cdata, plan)`, called by the executor
AFTER its lookup phase and BEFORE any event is emitted, still under the
5.1 mutex. The plan is the executor's own resolution -- which definitions
will be CREATED, which will receive metadata UPDATES (renames, optional
flips, retargets), which already exist as no-ops -- so an agent decides
"may this actually mint a property/model" at the right altitude without
performing its own catalog lookup (the request-level check_request cannot
know existence, and check_event fires per event mid-commit, where
creation-ness must be reverse-engineered). Default implementation
allows, so existing agents are unaffected; refusal fails the whole
registration before anything is emitted; check_event still gates every
emitted event underneath, as defense in depth -- individually, not
transactionally: an agent that allows the plan but denies a constituent
event aborts the remainder and leaves earlier catalog events durable
(maintainer ruling 2026-07-06: registration need not be atomic; the
allocator's storage-checked lookups keep identity convergent across such
partials, and #313 tracks the transactional upgrade). Schema knowledge
follows collection access: the executor requires can_access_collection
for every collection a request names, before any lookup, so the
verb-skipped no-op upsert cannot serve as an existence oracle for
principals that may not read the collection (second 2026-07-06 ruling). The
plan type is core-side and never crosses the wire; (c) catalog
collections are protected (section 4); (d) the
PolicyAgent surface for per-property gates should be designed ONCE together
with #264's commit-time hook and #274's admission seam, keyed by property
entity id, when one of those lands. Adding a use case to that consolidated
design, not a parallel surface, is the coordination contract with the
phase-2 track.

### 5.8 Renames (the stretch AC)

**Decision (rev 4, replacing rev 3's anchors): no anchors. A rename ships
a TRANSIENT migration hint.** Rev 3's anchor apparatus (a permanent anchor
attribute in source, the anchor-reuse guard, the anchored descriptor bit,
rename-back semantics) existed only because derivation keyed identity on
the first field name; it is deleted with derivation.

Under allocation, a rename with NO hint re-registers under the new name:
the lookup (5.1) misses, a fresh property id is allocated, and the old
property remains in the catalog with its data -- an add plus an orphan.
That is exactly today's failure mode, except fail-visible: both entities
sit in the catalog with provenance, so tooling can flag the likely mistake.

The hint makes the rename identity-preserving instead. It declares "the
property formerly known as X on this model IS this field"; the executor,
before lookup-or-create, applies "a property matching the hint exists on
this model -> update its `name` to the field's current name" (an ordinary
provenance-ordered follow-up), after which the normal upsert finds it
under the new name. Semantics, normative regardless of the attribute form:

- TRANSIENT and idempotent: once applied, or when nothing matches, the
  hint no-ops; it is removable from source once every system the code
  deploys to has seen it. This is the opposite of the anchor, which had to
  stay in source forever.
- GUARDED: the hint applies only when the current-name lookup MISSES and
  the hinted lookup HITS. If a property already exists under the current
  name, the hint no-ops and the existing property wins (the old lineage
  stays orphaned and visible); the executor never merges two identities
  and never creates a second live property under one (model, name) key.
- Chained renames update the hint in place (a to b to c ships
  renamed-from-b once b has rolled out). A system that skipped the
  intermediate deploy no-ops the hint and allocates fresh: the same
  visible orphan shape as a hintless rename, reconciled by tooling or a
  manual registration rather than silently.

**The attribute form (maintainer ruling, 2026-07-06):
`#[property(renamed_from = "title")]`, as a convenience, with explicit-id
binding intact.** The hint matches by OLD NAME (same model, same
property identity); the ordinary canonical compatibility check then applies
to the declaring backend/value_type. It is PORTABLE: one binary applies the same rename
on every system it deploys to (dev, staging, prod, self-hosted), each
updating its own allocated id; the attribute is removable after rollout.
Residual risk is name-shaped: if some system carries a semantically
different property under the old name on the same model, the hint renames
it anyway -- the same scope of ambiguity by-name registration already
accepts.

The alternative that was weighed, `#[property(id = "01H...")]` doing
double duty as the rename vehicle, is NOT the rename hint but is
preserved in full as the 5.9 binding mechanism: a literal id is one
system's allocation, so as a portable rename signal it would hard-fail
(fresh system, verify-never-mint) or silently orphan and fresh-allocate
(any other established system) unless per-deployment binding config
supplied each system's id. Under explicit binding, though, a rename
remains incidental -- the name is pure metadata on the bound entity,
updated by the ordinary difference-only follow-up -- and that behavior
keeps working unchanged for sharing and curated deployments.

**The stale-writer window is policy-governed (maintainer ruling,
2026-07-06).** After a rename lands, a node still running pre-rename code
that re-binds and takes a mutating path registers the OLD name afresh: it
carries no hint, the current-name lookup misses, and the request is an
ordinary schema-definition event for the executor's policy gate to judge.
On a permissive system (data freedom) it allocates -- an orphan lineage
beside the renamed one, visible in the catalog with provenance,
reconcilable by tooling; the hint's guard keeps later hinted
registrations from ever hijacking it. A system that does not want stale
writers minting refuses it through the ordinary PolicyAgent gate, in
either style: object-based (no write access to a property definition
meeting that description) or principal-based (this user may not define
    schema). A denied registration prevents the stale write when its old field
    has no compatible canonical binding. An unavailable reassertion may proceed
    only when every compiled field is already bound compatibly; a registered
    user collection never emits a newly unregistered field as Name residue.
    No former-names lookup fallback is added: the fork remains a policy
    decision, not a protocol one.

SQL columns follow a rename via the catalog-driven RENAME COLUMN (5.5),
unchanged. Display-name updates are ordinary provenance-ordered follow-ups
(5.1): concurrent different renames converge as LWW, and each node's
queries keep resolving through its own compiled name to its bound id, so
display-name churn never affects addressing.

Relation to #85's stretch wording ("expressly referencing the property
entity id"): explicit-id binding (5.9) satisfies it verbatim; the rename
hint is the portable form for by-name-registered properties.

### 5.9 Explicit id binding and shared properties

By-name registration (the upsert, 5.1) is the DEFAULT, not the only path. The
macro accepts explicit bindings to known definition entities:

```rust
#[model(id = "AZB64ULID...")]           // bind this struct to a known
struct Signal { ... }                    // model entity

#[property(id = "AZB64ULID...")]        // bind this field to a known,
pub label: String,                       // possibly shared, property entity
```

Semantics: an explicit id bypasses the by-name lookup entirely.
Registration looks the entity up executor-side and applies the same canonical
compatibility rule as by-name registration: backend must match exactly and
value_type must be mutually castable with the stored canonical value_type.
Derived field access carries that literal id beside the local field name, so
getters, mutations, listeners, predicates, and ORDER BY remain bound to the
definition even when the local name differs or the catalog display name is
later renamed. The local name remains only the legacy-residue fallback.
An incompatible binding fails registration; a castable declaration is
admitted and the response returns the canonical pair. Registration ensures the (model,
property) MEMBERSHIP exists, allocating it on miss (5.1); it never
mutates the bound property entity's type pair (backend and value_type are
verify-only; display-name behavior under binding follows the 5.8 ruling).
Binding an id that does not exist in the catalog is an error, not
an implicit create: explicit binding is a REFERENCE to a definition
authored elsewhere (another model's registration, or DDL, 5.10). Two
consequences, stated as design rather than left implicit. Cold start: if
the authoring producer has never registered in this system, the binder's
registration HARD-FAILS; deployments using explicit binding must guarantee
the definition exists first (DDL application or eager registration of the
authoring model is the intended provisioning step). Drift in a binder is
handled by that same compatibility rule: castable drift uses the definition's
canonical type, while a different backend or non-castable value type fails
loudly. The definition itself is immutable after allocation.

The literal id is also the compiled field's runtime address, not registration
metadata that is discarded afterward. Derive-generated Model initialization,
View getters, Mutable accessors, listeners, predicates, and ORDER BY retain the
id even when the Rust field name differs from the catalog definition's current
display name. Reads may fall back to legacy residue under the local name only
when the id entry/root is absent; LWW writes and new CRDT roots use the id and
undergo the same canonical value_type conversion as any other id-keyed write.
A migrated Yrs field whose history already lives under the legacy name keeps
editing that root because CRDT history cannot be re-keyed. Query alias
resolution becomes available only after that exact compiled schema shape has
registered successfully and the catalog holds its live model/property
membership. If two ensured compiled declarations map one local name to
different ids, resolution fails closed as ambiguous. Active-type integrations
receive the complete `(local name, optional explicit id)` address at their
`FromEntity` / `InitializeWith` boundary so a backend cannot silently erase
explicit identity.

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
every id is an allocation by one system's durable node (5.1), whether the
registration was authored by macro or by DDL. A literal id in source
therefore binds the source to one system's catalog. Workable practices:
per-deployment binding (the attribute value sourced from build-time config
or codegen against the target system's catalog); cross-system portability
comes from by-name registration plus rename hints (5.8) rather than from
ids. Tooling should make both easy (print ids per system; emit binding
files from a catalog). The mechanism is required and shipped; choosing a
portability practice is deployment policy, not RFC policy.

### 5.10 The declarative DDL path

The catalog entities are the definitive schema. Rust structs are one
authoring surface for them; this section commits to a second, declarative
one, without implementing it here.

A schema DOCUMENT declares desired catalog state: models (name,
collection), properties (name, backend, value_type), memberships (model,
property, optional), sharing (membership citing a property by name-within-
this-document or by explicit id), renames (rename hints, 5.8). Applying
the document diffs declared state against the live catalog and issues the
SAME RegisterSchema upserts the derive macro issues, through the same
allocating executor (5.1, 5.2): the differ's output is a reviewable
migration plan (create these entities, update these display names, add
these memberships), and application is idempotent because the executor is
an upsert. Nothing in the write path is DDL-specific; DDL is another
producer of registration requests.

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
- No federation-grade catalog trust model (per-system allocation is
  deliberate headroom for it).
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
  supplies what that plan stubbed: property identity is the allocated
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
| Schema evolution | Stable identity under rename via the transient hint (5.8); struct-level retypes reuse that identity through the canonical-type compatibility check (5.1, 5.6); ids on the wire | Phase A (canonical-type migration and SQL rename DDL follow later) |
| SQL column identity | Catalog-bound columns, rename DDL, collision suffixes (5.5) | Phase C |
| Cross-node strategy agreement | One canonical (backend, value_type) per property; registration admits castable drift and refuses incompatible declarations; payloads are id-keyed (5.6) | Phase A |
| Per-field addressing | Property entity id as the stable key for signals, policy, indexes (5.7, 7) | Foundation only; consumers later |
| Introspection | Catalog collections queryable with ordinary predicates (4) | Phase A |

## 9. Phasing (design-level; detailed ladder after sign-off)

- **Phase 0: #294.** Protocol version in the Presence handshake;
  refuse-vs-degrade semantics decided and implemented. Gates Phase A; the
  implementation session does this first.
- **Phase A (one protocol epoch; durable nodes upgrade first):** the three
  catalog collections (model, property, membership), durable-allocated
  ids via the upsert executor with its process-local mutex and the
  SchemaRegistered response (5.1, 5.2), the registration protocol
  operation with policy gate and receiver-side catalog protection (4,
  5.2), the strict never-registered-offline error (5.2), catalog
  subscription and map with response-fed upserts, wait_catalog_ready, and
  hard_reset flushing (5.2), uniform `PropertyKey::{Id, Name}` backend maps
  with id-keyed writes for registered user collections and legacy Name
  fallback on reads (5.5), ankql
  Identifier on the wire with resolution preceding serialization and
  receiver-side validation pass-through pending #274 (5.3), generic
  id-then-legacy-name reads, canonical evaluation, and zero-op creation
  events (5.4), reserved-prefix enforcement, `UnknownProperty` and
  `NonCastable` errors, the transient rename hint (5.8, renamed_from per the
  2026-07-06 ruling), explicit id-binding attributes for models and properties
  including shared-property membership (5.9; the attribute is parsed and
  enforced here, while portable AUTHORING of id values gets ergonomic
  with Phase D's codegen/DDL tooling). Yrs uses the same PropertyKey contract
  and id-named roots for registered properties (5.5).
- **Phase C (engine-local, no wire gate):** sled property_config rekey;
  postgres/sqlite catalog-bound columns with rename DDL and collision
  suffixes; IndexedDB re-materialization on rename; all while keeping the
  canonical-vs-materialization write seams clean per 4a (#304's
  consumer). (AMENDED 2026-07-09: the durable column maps, collision
  suffixes, and sled/IndexedDB keying landed early on PR #307 with the
  engine column-map amendment, 5.5; rename DDL and rename-driven
  re-materialization remain here.)
- **Phase D:** the declarative schema document and differ (#301),
  catalog-to-binding codegen (#302), rename-hint tooling (orphan
  detection, divergence warnings), per-property policy keys when the
  #264/#274 consolidated surface exists, per-field addressing consumers.
  Schema-evolution follow-ons tracked as #303.

(Phase B no longer exists: rev 3 folded the query-AST wire change into
Phase A.)

## 10. Renegotiations of #85, collected

1. AC2 trigger granularity: per model at first use, not per accessor
   (5.2). The allocator lookup key is (model entity, current name). Backend
   and value_type are checked against the canonical definition on a hit;
   they do not participate in identity. Rev 4 made the upsert literal (a
   durable-side lookup-or-create) rather than derivational.
2. AC2 read-path timing: schema-dependent predicate fetch/query/subscribe
   paths register at first use through the idempotent upsert. Direct entity-id
   gets are the implemented exception: they load the entity without minting
   identity; a later edit records the exact schema for commit-time admission.
   Denied or offline registration
   fails loud on paths that require it (5.3); an already-complete schema
   resolves to a no-op plan that emits nothing and skips the policy verb
   (5.7).
3. AC4 vehicle: a new resolved `Identifier` node produced by a resolution
   pass, rather than mutating `PathExpr` in place (5.3); PathExpr remains
   the parse-time form.
4. Stretch AC vehicle: literal entity-id references exist as required
   (`#[property(id = ...)]`, 5.9, also the sharing vehicle); the portable
   companion for by-name-registered properties is the transient
   renamed_from hint (5.8; ruled 2026-07-06). Rev 3's anchor attribute
   is removed.
5. Offline-first registration (a section 2 constraint, not an #85 AC;
   collected here for honesty): revs 1-3 required coordination-free
   registration; rev 4 requires the allocating durable node for FIRST
   registration and makes offline creation into a never-registered
   collection a strict error, maintainer-ruled acceptable 2026-07-06.
   Offline data writes are unaffected only for an exact compiled schema whose
   fields are all already bound compatibly in the local catalog.

Everything else in #85 is adopted as written, including the
`_ankurah_property` collection name (AC1) that PR #236 had moved away from.

## 11. Open questions (for #289 discussion)

1. **Ulid timestamp bits in derived ids** (5.1): SUPERSEDED by rev 4.
   Allocated ids are real ULIDs with genuine timestamp and randomness;
   the question no longer exists.
2. **Registration writes from read-only contexts** (5.2): RESOLVED by the
   implemented path split. Predicate fetch/query/subscribe registers at first
   use through the idempotent upsert; direct entity-id get only loads, and a
   later edit records the exact schema for commit-time admission. An existing
   schema is a no-op plan, while denial/offline fails loud
   only on paths that require registration. See renegotiation 2.
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
   tracked as #302; by-name registration plus rename hints (5.8) are the
   portable form meanwhile.
8. **Membership retirement** (4): tombstone semantics deferred to
   schema-evolution work, tracked as #303 (together with the
   declared-coercion retype escape hatch from 5.6/5.9/6). The collection
   excision, excluded from this project (4a), is tracked as #304.
9. **Rename-hint attribute form** (5.8, rev 4): RESOLVED 2026-07-06 --
   `renamed_from` by old name, as a convenience, with explicit-id
   binding (5.9) intact and load-bearing beside it. Rationale for both
   candidates recorded in 5.8.
10. **Multi-durable allocation** (5.1): single-allocator routing is a
    documented constraint (#309); a real allocator protocol (leases or
    consensus) is future work there.

(Resolved during adversarial review and later amendments, recorded in their
home sections: Yrs uses id-named roots under the uniform PropertyKey contract,
5.5; required defaults and absent-as-NULL behavior are separated by consumer,
5.4; relay validation remains pass-through until #274, 5.3.)

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
