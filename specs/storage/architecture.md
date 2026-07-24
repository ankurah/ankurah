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

The protocol still carries a model where an operation needs a model context:
query and mutation requests, subscription items, entity deltas, policy checks,
schema envelopes, and state writes. That context says how an entity is being
used; it does not become part of the canonical entity or event.

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

An engine must durably remember which models an entity has been used through.
The association is engine-local indexing metadata, not an entity property and
not replicated CRDT state.

Association creation is explicit at the core/engine boundary. A prepared
entity write carries every model usage which the committing transaction must
add:

```rust
pub struct PreparedEntityWrite {
    /// The canonical storage head from which `state` was derived.
    ///
    /// A missing canonical record has the empty genesis clock.
    pub expected_head: Clock,
    /// Canonical model-independent state after event replay.
    pub state: Attested<EntityState>,
    /// Models through which this transaction used the entity.
    pub associate_with: BTreeSet<ModelId>,
}
```

Committing a `PreparedEntityWrite` has the following semantic effect:

1. compare its expectation with the canonical entity record;
2. persist its canonical state if the expectation matches;
3. durably add every `associate_with` model to the entity's association set;
4. load the complete durable model association set for the entity;
5. refresh every associated model materialization from the canonical state.

Associations are set-valued and idempotent. This revision does not define
association removal.

The physical representation is intentionally unspecified. A relational engine
may use an `_ankurah_entity_model` relation; a key/value engine may use a
dedicated tree or embed engine metadata alongside its canonical record. It
must not expose that choice through `StorageEngine`, and it must not encode
the association as `PropertyId::System(...)`.

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

The storage engine does not replay events. Core retains the model usages and
one of two logical write intents:

- an event-backed intent containing the transaction's validated, durable
  events and the model usage under which each event was authorized; or
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
   that event's model context and the refreshed before/after states;
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

Built-in system models and system properties are the bootstrap exception:
their identities, logical schemas, and reserved physical names are fixed and
must be usable before the registered catalog is warm.

## 4. Read and write behavior

### 4.1 Identity reads

`get_state`, `get_states`, `get_events`, and `dump_entity_events` read the
global canonical stores. They do not accept a model and do not create an
entity-model association.

Policy and schema interpretation happen above the storage engine using the
model context of the request that caused the read.

### 4.2 Model queries

`fetch_states(model, selection)` executes the selection against `model`'s
materialization and hydrates matching canonical entity states from the global
entity store. The returned states remain model-independent.

Querying does not by itself create an association: every materialized match is
already associated. Receiving and accepting a state from a remote query does
create the local association when core includes that model in the prepared
entity write.

### 4.3 Writes

`append_events` persists model-independent events exactly once.
Model-specific authorization is complete before the storage call.

`commit_batch` is the only storage operation that forms entity-model
associations or changes canonical state. The engine unions each write's
`associate_with` set with its durable existing associations and refreshes the
complete result, not only the newly supplied models. Its successful result
reports canonical changes, newly formed associations, and every refreshed
model so core can notify each in-memory query surface without knowing the
engine's association representation.

An association-only attempt uses the latest canonical state as both its base
and proposed state. It still locks/checks that head with the rest of the batch,
adds the association, and materializes the current state without manufacturing
an entity change.

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
9. resident handles for one entity retain the model context requested by each
   caller while sharing canonical in-memory state, and reactor query watchers
   receive only changes for their own model.
10. every prepared batch is all-or-nothing across canonical states,
    associations, materializations, and indexes;
11. one mismatched entity head rolls back the entire batch and reports the
    canonical states needed for retry;
12. replaying durable transaction events over the reported state produces a
    monotonic retry, including a concurrent-head merge;
13. multiple engine instances sharing one PostgreSQL database cannot regress a
    canonical head or leave a materialization behind it;
14. same-head writes can add a new model association and notify that model
    without reporting a canonical state change.
15. a shared SQL database may contain unrelated tables before first open;
    initialization accepts them, physical-name assignment deduplicates around
    them, and `delete_all` leaves them untouched.
