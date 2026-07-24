# Storage Architecture

Status: RATIFIED for PR #307 (maintainer direction, 2026-07-23).

This document defines the semantic contract between Ankurah core and a
storage engine. It deliberately does not prescribe tables, trees, object
stores, physical names, or the representation of engine-private metadata.

## 1. Ontology

### 1.1 Entities and events are model-independent

An entity is identified by `EntityId`. Its canonical accumulated state exists
once in a storage engine, independent of every model through which the entity
is created, edited, fetched, or queried.

An event belongs to an entity and is identified by `EventId`. It also exists
once in a storage engine and has no intrinsic model.

Consequently:

- canonical `EntityState` and `Event` values do not contain a `ModelId`;
- direct state and event reads are addressed only by entity/event identity;
- a storage engine must not duplicate canonical state, head, or events per
  model.

The protocol still carries a model where an operation needs a model projection:
query and mutation requests, subscription items, entity deltas, and schema
envelopes. That context says how an entity is being addressed; it does not
become part of the canonical entity or event, establish membership, or serve
as independent authorization proof.

### 1.2 Models define materializations

A model is a data contract and query surface identified by `ModelId`.
Registered models use their durable catalog `EntityId`; built-in catalog and
system models use `ModelId::System(SystemModel)`.

A model materialization contains projected property values needed to filter,
order, and index entities used through that model. It is derived data:

- canonical state is authoritative;
- a materialization can be rebuilt without changing entity or event identity;
- an entity may be materialized under zero, one, or many models;
- editing through one model must refresh every model already associated with
  the entity.

The canonical catalog entities use the same machinery. Their materializations
are the built-in models `_ankurah_system`, `_ankurah_model`,
`_ankurah_property`, and `_ankurah_model_property`.

### 1.3 Entity-model association

An entity's model memberships are canonical, event-derived state. Genesis
carries an attested `Operation::Membership(Membership::Add(model))`; applying
that operation adds the model to `State.memberships`. A request envelope,
query, or typed view never creates membership implicitly.

The engine chooses how to persist and index that canonical set. SQL engines
use an `_ankurah_entity_model` relation; other engines may use a dedicated
tree/store or embed the set with their canonical entity record. That physical
representation is engine-private indexing/storage machinery, but its contents
must equal the memberships in the prepared canonical state.

A prepared write therefore needs no separate association instruction:

```rust
pub struct PreparedEntityWrite {
    /// The canonical storage head from which `state` was derived.
    ///
    /// A missing canonical record has the empty genesis clock.
    pub expected_head: Clock,
    /// Canonical model-independent state after event replay.
    pub state: Attested<EntityState>,
}
```

Committing a `PreparedEntityWrite` has the following semantic effect:

1. compare its expectation with the canonical entity record;
2. persist its canonical state if the expectation matches;
3. make the engine's durable entity-model index equal the state's membership
   set (this revision rejects removal);
4. refresh every member model's materialization from the canonical state.

Memberships are set-valued and idempotent. The current event protocol admits
exactly one membership add on genesis and rejects membership operations on
later events. The plural representation is intentional: adding a model to an
existing entity is deferred to #412 and will not require a storage-shape
change. Removal remains undefined. An engine must not encode memberships as a
`PropertyId::System(...)`.

## 2. StorageEngine contract

`StorageCollection` is removed. It conflated three different things: the
global entity store, the global event store, and one model materialization.
Core addresses semantic operations directly through `StorageEngine`. Events
are appended before state preparation; prepared entity writes are committed as
one exact-head compare-and-swap batch:

```rust
#[async_trait]
pub trait StorageEngine: Send + Sync {
    type Value;

    fn set_catalog_resolver(&self, resolver: Weak<dyn CatalogResolver>);

    async fn append_events(&self, events: &[Attested<Event>])
        -> Result<Vec<bool>, MutationError>;

    async fn commit_batch(&self, batch: StorageWriteBatch)
        -> Result<CommitBatchOutcome, MutationError>;

    async fn get_state(&self, id: EntityId)
        -> Result<Attested<EntityState>, RetrievalError>;
    async fn get_states(&self, ids: Vec<EntityId>)
        -> Result<Vec<Attested<EntityState>>, RetrievalError>;

    async fn fetch_states(
        &self,
        model: &ModelId,
        selection: &Selection,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError>;

    async fn get_events(&self, ids: Vec<EventId>)
        -> Result<Vec<Attested<Event>>, RetrievalError>;
    async fn dump_entity_events(&self, id: EntityId)
        -> Result<Vec<Attested<Event>>, RetrievalError>;

    async fn list_materializations(&self)
        -> Result<Vec<ModelId>, RetrievalError>;
    async fn delete_all(&self) -> Result<bool, MutationError>;
}
```

Engines may use private bucket, transaction, table, tree, or materialization
handles internally. Their construction arguments and resolver propagation are
implementation details.

### 2.1 Blind event append

Validated and attested events are immutable, content-addressed, and
model-independent. `append_events` inserts them idempotently without comparing
an entity head. An event being present does not make it part of canonical
entity state; only a canonical head does that. A failed state transaction may
therefore leave an unreferenced event, as the existing event-first durability
discipline already permits.

Every event needed to derive a proposed state must be durable before that
state's CAS attempt. Consequently a committed canonical head never references
an event which was rolled back with a failed state attempt.

### 2.2 Atomic prepared-state batch

```rust
pub struct StorageWriteBatch {
    pub entities: Vec<PreparedEntityWrite>,
}

pub enum CommitBatchOutcome {
    Committed(StorageCommitResult),
    Conflict {
        /// Canonical states observed while checking this attempt.
        observed: BTreeMap<EntityId, Option<Attested<EntityState>>>,
    },
}
```

An engine checks every entity expectation before making the batch visible. A
single mismatch rolls back the complete batch. On success, canonical entity
rows, entity-model associations, every affected materialization, and their
secondary indexes commit atomically.

`expected_head` matches only byte-for-byte logical `Clock` equality. A missing
canonical record is compared as the empty genesis clock. A clock is a causal
frontier, not a scalar version; engines must not invent a greater-than
comparison.

The conflict result must give core enough canonical state to true up affected
resident entities. An engine may return the states read during its failed
transaction or core may immediately fetch them after rollback. If that fetch
observes a newer version, the newer fetched head becomes the next expectation.

Physical schema preparation which a backend cannot transact with ordinary
data may happen before the CAS transaction, but it must not expose entity,
association, materialization, or index records from a failed batch.

### 2.3 Core retry and monotonic true-up

The storage engine does not replay events. Core retains one of two logical
write intents:

- an event-backed intent containing the transaction's validated, durable
  events; or
- a state-backed intent containing a validated resident snapshot received
  through a replication shape which may not include a replayable event list.

For each attempt, core:

1. loads the current canonical state of every affected entity;
2. builds a detached candidate:
   - event-backed candidates begin at that exact durable state;
   - state-backed candidates begin at the validated snapshot and causally
     merge the exact durable state, fetching durable tips when the two heads
     diverge;
3. reapplies any supplied events parents-first (duplicates are no-ops);
4. for each newly applied event, re-runs the original event policy check with
   the refreshed before/after states and the canonical membership admitted for
   this revision (exactly one membership; add-to-existing policy is deferred);
5. pairs the candidate with the exact loaded durable head;
6. attempts the complete prepared batch.

On conflict, core discards the candidates and repeats those steps for every
entity in the rolled-back batch. On success, it causally reconciles the
committed candidate into the canonical resident. Candidate preparation never
assigns an observed head directly and never mutates or overwrites a resident
branch. Missing concurrent lineage fails the attempt without writing state;
lineage repair is outside this revision. A per-resident reconciliation guard
may serialize that post-commit in-memory update, but it is not a storage lock
and is not part of cross-node correctness.

The retry loop is bounded and yields between conflicts so competing tasks can
make progress. Exhaustion returns an error and never falls back to an
unconditional or non-monotonic write.

## 3. Catalog resolver

`Node` injects one weak `CatalogResolver` into the engine after constructing
the catalog. The engine decides when and where to use it.

For registered models and properties, a human-named engine must:

1. consult its durable identity-to-physical-name map;
2. consult the resolver only on a durable miss;
3. sanitize and deduplicate the resolver-provided label;
4. persist the assignment before using it.

Renames never move an existing physical structure.

An engine also needs the complete durable property membership of a model to
project only that model's fields. The resolver contract therefore exposes a
model-property enumeration in addition to individual name and type lookups.
An incomplete catalog is an error here, never an empty property list: otherwise
an engine could silently publish an empty materialization while metadata is
still warming.

Built-in system models and system properties are the bootstrap exception:
their identities, logical schemas, and reserved physical names are fixed and
must be usable before the registered catalog is warm.

## 4. Read and write behavior

### 4.1 Identity reads

`get_state`, `get_states`, `get_events`, and `dump_entity_events` read the
global canonical stores. They do not accept a model and do not create an
entity-model association.

Policy and schema interpretation happen above the storage engine. A
model-scoped caller must first prove that the canonical state's membership set
contains the requested model; the request's model context is not proof by
itself.

### 4.2 Model queries

`fetch_states(model, selection)` executes the selection against `model`'s
materialization and hydrates matching canonical entity states from the global
entity store. The returned states remain model-independent.

Querying and receiving a query result do not create membership. Every
materialized match already carries that model in its canonical membership
state.

### 4.3 Writes

`append_events` persists model-independent events exactly once.
Model-specific authorization is complete before the storage call.

`commit_batch` is the only storage operation that changes canonical state or
its engine-private membership index. The engine derives the target model set
from each write's canonical `State.memberships`, refuses a removal in this
revision, and refreshes the complete set. Its successful result reports
canonical changes, newly observed memberships, and every refreshed model so
core can notify each in-memory query surface without knowing the engine's
physical representation.

## 5. Backend shape

The following names describe responsibilities, not mandatory identifiers:

| Logical store | Contents |
|---|---|
| entity store | One canonical state/head/attestation record per `EntityId` |
| event store | One canonical event/attestation record per `EventId` |
| association store | Durable set of `(EntityId, ModelId)` |
| model materialization | Projected values and indexes for one `ModelId` |
| model-name registry | Durable `ModelId` to physical materialization name |
| property-name registry | Durable `(ModelId, PropertyId)` to physical field name |

The implementations in this revision choose the following private layouts:

- Sled uses global entity, event, and entity-association trees plus
  identity-named model-materialization trees.
- PostgreSQL and SQLite use one canonical entity table, one canonical event
  table, a private entity-model relation, and one projected table per model.
- IndexedDB uses separate canonical entity, canonical event, association, and
  materialization object stores. A private discriminator scopes projected
  records within the shared materialization store.

These layouts are not part of the public storage contract.

## 6. Required tests

Every engine must demonstrate:

1. one entity can be associated with two unrelated models;
2. editing it through either model refreshes both materializations;
3. canonical state and events are stored once and contain no singular model;
4. two models and two properties may share the same registered label without
   sharing physical storage;
5. physical names are normalized, deduplicated, and stable across reopen;
6. built-in catalog models operate before registered catalog warm-up;
7. identity reads do not create associations or materializations;
8. deleting/rebuilding a materialization does not lose canonical state,
   events, or association truth;
9. views and mutables for one entity retain the model projection requested by
   each caller while sharing one model-independent canonical `Entity`, and
   reactor query watchers receive only changes for their own model;
10. every prepared batch is all-or-nothing across canonical states,
    associations, materializations, and indexes;
11. one mismatched entity head rolls back the entire batch and reports the
    canonical states needed for retry;
12. replaying durable transaction events over the reported state produces a
    monotonic retry, including a concurrent-head merge;
13. multiple engine instances sharing one PostgreSQL database cannot regress a
    canonical head or leave a materialization behind it;
14. a prepared state containing an additional membership can add that
    association and notify its model without duplicating canonical entity or
    event storage (the core event path for add-to-existing remains #412);
15. a shared SQL database may contain unrelated tables before first open;
    initialization accepts them, physical-name assignment deduplicates around
    them, and `delete_all` leaves them untouched.
