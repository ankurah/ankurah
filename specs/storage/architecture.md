# Storage Architecture

Status: Ratified.

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

- canonical state records the membership set, not a single owning model;
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

A staged state therefore needs no separate association instruction:

```rust
transaction.set_state(&expected_head, &state).await?;
```

Committing that state has the following semantic effect:

1. compare its expectation with the canonical entity record;
2. persist its canonical state if the expectation matches;
3. make the engine's durable entity-model index equal the state's membership
   set (this revision rejects removal);
4. refresh every member model's materialization from the canonical state.

Memberships are set-valued and idempotent. Genesis establishes at least one
membership; later events may add memberships. Editing through one model
preserves the others and refreshes all affected materializations. Removal
remains undefined. Memberships are not `PropertyId::System(...)` values.

## 2. StorageEngine contract

`StorageCollection` is removed. It conflated three different things: the
global entity store, the global event store, and one model materialization.
Core addresses semantic operations directly through `StorageEngine`. Transaction
events and prepared entity writes commit together in one exact-head
compare-and-swap batch:

```rust
#[async_trait]
pub trait StorageEngine: Send + Sync {
    type Value;
    type Transaction<'a>: StorageTransaction + 'a where Self: 'a;

    fn transaction(&self) -> Self::Transaction<'_>;

    fn set_catalog_resolver(&self, resolver: Weak<dyn CatalogResolver>);

    async fn append_events(&self, events: &[Attested<Event>])
        -> Result<Vec<bool>, MutationError>;

    async fn get_state(&self, id: EntityId)
        -> Result<Attested<EntityState>, RetrievalError>;
    async fn get_states(&self, ids: Vec<EntityId>)
        -> Result<Vec<Attested<EntityState>>, RetrievalError>;

    async fn fetch_states(
        &self,
        selection: &Selection<Resolved>,
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
entity state; only a canonical head does that. Standalone append is for event
caching and lineage retrieval, not transaction commits.

Events needed by a proposed state must either be durable already or included
in its batch. A failed batch publishes neither its new events nor its states.

### 2.2 Engine-owned staging and atomic commit

```rust
#[async_trait]
pub trait StorageTransaction: Send {
    async fn add_events(&mut self, events: &[Attested<Event>]) -> Result<(), MutationError>;
    async fn set_state(&mut self, expected_head: &Clock, state: &Attested<EntityState>)
        -> Result<(), MutationError>;
    async fn commit(self) -> Result<StorageCommitOutcome, MutationError>;
}

pub enum StorageCommitOutcome {
    Committed(StorageCommitResult),
    Conflict {
        /// Canonical states observed while checking this attempt.
        observed: BTreeMap<EntityId, Option<Attested<EntityState>>>,
    },
}
```

Writes borrow inputs only until the call returns. Each engine chooses whether
to execute them inside its native transaction immediately or retain encoded
values until commit. The current engines prepare labels and physical schema
before opening the native data transaction. Repeated `set_state` calls for an
entity replace its preceding transactional state; buffered engines coalesce
them while preserving the first storage expectation.

An engine checks every entity expectation before making the writes visible. A
single mismatch rolls back the complete batch. On success, canonical entity
rows, events, entity-model associations, every affected materialization, and
their secondary indexes commit atomically. PostgreSQL and SQLite use one SQL
data transaction per Ankurah transaction.

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

The storage engine does not replay events. Local and remote commit paths
validate on resident-backed forks and send each event and resulting state
directly to their storage transaction, preserving event order. The first
attempt uses the resident state without an unconditional storage reload.

On conflict, the commit path discards its forks and repeats event application
and policy checks from the residents. The node serializes storage commit and
resident publication, so a competing commit waits for the winner to publish.
Only a successful storage commit allows this transaction's events to be applied
to residents. State-only replication instead
merges its validated incoming snapshot with current state, persists that merge,
and reconciles the resident after success. Missing concurrent lineage fails
the attempt without writing state; lineage repair is outside this revision.
Peer waits and reactor notification stay outside the node's commit/publication
lock; cross-node conflict detection remains the storage engine's responsibility.

The retry loop is bounded. Exhaustion returns an error and never falls back
to an unconditional or non-monotonic write.

## 3. Catalog resolver

`Node` injects one weak `CatalogResolver` into the engine after constructing
the catalog. The engine decides when and where to use it.

For registered models and properties, a human-named engine must:

1. consult its durable identity-to-physical-name map;
2. consult the resolver only on a durable miss;
3. sanitize and deduplicate the resolver-provided label, or use an ID-derived name if unavailable;
4. persist the assignment before using it.

Catalog label lookup is best-effort: return a cached label immediately, otherwise
wait up to one second for the catalog query and return `None` on timeout. It
does not issue another fetch or register the ID. Engines then assign a name
from an ID prefix using their normal collision handling. Once assigned, the
durable map is authoritative, including after restart; a late label or rename
never moves an existing physical structure or triggers another label lookup.

Materializations project the values present in canonical entity state for each
of its member models. Storage does not enumerate a model's declared properties;
model-scoped name resolution and validation belong to core. SQL column types
come from values on first materialization, and absent properties are cleared
when the projection is replaced. The resolver supplies naming labels only.

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

`StorageTransaction::commit` is the only storage operation that changes canonical state or
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
