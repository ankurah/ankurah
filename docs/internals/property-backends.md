# Property Backends

## Overview

Each entity in ankurah stores its mutable state across one or more **property
backends**. A backend owns a set of named properties and knows how to apply
mutations, resolve concurrency, and serialize its state. The two shipping
backends are:

- **LWW** (Last-Writer-Wins) -- stores scalar values (strings, integers, JSON,
  entity references). Concurrent writes to the same property are [resolved
  deterministically](lww-merge.md): the write with the greater `EventId` wins.
- **Yrs** -- stores collaboratively-editable text using the Yrs CRDT library.
  Concurrent edits merge automatically because Yrs operations are commutative
  and idempotent.

The backend system lives in `core/src/property/backend/`. Value-type wrappers
that present backends to user code live in `core/src/property/value/`.


## The PropertyBackend Trait

Defined in `core/src/property/backend/mod.rs`, `PropertyBackend` is the
interface that every backend must implement:

```rust
pub trait PropertyBackend: Any + Send + Sync + Debug + 'static {
    fn property_backend_name() -> &'static str where Self: Sized;

    // -- Mutation --
    fn to_operations(&self) -> Result<Option<Vec<Operation>>, MutationError>;
    fn apply_operations(&self, operations: &[Operation]) -> Result<(), MutationError>;
    fn apply_operations_with_event(&self, operations: &[Operation], event_id: EventId)
        -> Result<(), MutationError>;
    fn apply_layer(&self, layer: &EventLayer) -> Result<(), MutationError>;

    // -- Serialization --
    fn to_state_buffer(&self) -> Result<Vec<u8>, StateError>;
    fn from_state_buffer(state_buffer: &Vec<u8>) -> Result<Self, RetrievalError>
        where Self: Sized;

    // -- Query --
    fn properties(&self) -> Vec<PropertyName>;
    fn property_value(&self, name: &PropertyName) -> Option<Value>;
    fn property_values(&self) -> BTreeMap<PropertyName, Option<Value>>;

    // -- Lifecycle --
    fn fork(&self) -> Arc<dyn PropertyBackend>;
    fn listen_field(&self, name: &PropertyName, listener: Listener) -> ListenerGuard;
}
```

The methods break down into four groups:

### Mutation methods

`to_operations` collects all changes made since the last call. For LWW, it
transitions `Uncommitted` entries to `Pending` and serializes the changed
property-name-to-value pairs into a single `Operation`. For Yrs, it encodes the
delta between the previous `StateVector` and the current document state using
Yrs v2 encoding. If nothing changed, both backends return `Ok(None)`.

`apply_operations` applies operations without event tracking. This is used when
loading from a state buffer where there is no associated event.

`apply_operations_with_event` applies operations and records which `EventId` set
each property. The default implementation (used by Yrs) delegates to
`apply_operations` and discards the event ID, since CRDTs resolve concurrency
internally. LWW overrides this to store the `EventId` alongside each value,
which is essential for later conflict resolution.

`apply_layer` is the unified concurrency-resolution entry point, described in
detail below and in the [LWW Merge Resolution](lww-merge.md) document.

### Serialization methods

`to_state_buffer` serializes the backend's current state into an opaque byte
vector. For LWW, this is a bincode-encoded `BTreeMap<PropertyName,
CommittedEntry>` where each `CommittedEntry` carries the value and the
`EventId` that wrote it. If any entry lacks an `EventId` (i.e. it is still
`Uncommitted` or `Pending`), serialization fails -- this guards against
persisting incomplete state. For Yrs, the buffer is the document encoded as a
Yrs v2 update from an empty `StateVector`, which captures the complete document
history.

`from_state_buffer` reconstructs a backend from a previously serialized buffer.

### The StateBuffers format

Entity state is persisted as a `State` struct containing a `Clock` (the [head](event-dag.md#core-concepts))
and a `StateBuffers` map (`BTreeMap<String, Vec<u8>>`). Each key is a backend
name (`"lww"`, `"yrs"`), and each value is the byte vector produced by that
backend's `to_state_buffer`. This is defined in `proto/src/data.rs`. See
[Entity State Persistence](entity-lifecycle.md#entity-state-persistence) for the full serialization flow.


## LWW Backend

Defined in `core/src/property/backend/lww.rs`.

### Internal state

LWW stores its properties in `RwLock<BTreeMap<PropertyName, ValueEntry>>`. Each
`ValueEntry` is in one of three states:

| State | Meaning |
|-------|---------|
| `Uncommitted { value }` | User called `set()` on a transaction fork; not yet collected |
| `Pending { value }` | `to_operations()` collected it, awaiting commit |
| `Committed { value, event_id }` | Applied from a committed event; `event_id` records the writer |

Only `Committed` entries have an `EventId`. This invariant is enforced by
`to_state_buffer`, which returns an error if any entry is `Uncommitted` or
`Pending`.

### Conflict resolution via apply_layer

When [concurrent branches are merged](entity-lifecycle.md#apply_event-in-detail), the entity layer calls `apply_layer` on
every backend for each [`EventLayer`](#eventlayer-helpers) in causal order. The LWW implementation
works as follows (see also [LWW Resolution Rules](lww-merge.md#lww-resolution-rules) for the full algorithm):

**Step 1 -- Seed winners from stored state.** The backend reads all current
`Committed` entries and creates a `Candidate` for each one. Each candidate
carries the property value, the `EventId` that wrote it, and two flags:
`from_to_apply` (false for stored values) and `older_than_meet`.

The `older_than_meet` flag is set when the stored value's `EventId` is not
present in the [accumulated DAG](event-dag.md#core-concepts) (`!layer.dag_contains(&event_id)`). This means
the value was written by an event that is strictly older than the [meet point](event-dag.md#core-concepts).
Any layer candidate automatically wins against such a value.

**Step 2 -- Process layer events.** For each event in both `already_applied` and
`to_apply`, the backend deserializes LWW operations and extracts per-property
changes. Each change becomes a candidate. For every property, the new candidate
is compared against the current winner:

- If the current winner is `older_than_meet`, the new candidate wins
  unconditionally.
- Otherwise, `layer.compare(&candidate_id, &current_winner_id)` determines the
  causal relationship:
  - `Descends` -- the candidate is causally newer; it replaces the winner.
  - `Ascends` -- the candidate is causally older; no change.
  - `Concurrent` -- the events are on different branches with no causal
    ordering. The tie is broken by lexicographic `EventId` comparison:
    `candidate.event_id > current.event_id` wins.

**Step 3 -- Apply winners from to_apply.** Only candidates whose
`from_to_apply` flag is true are written to the backend's state as `Committed`
entries. Winners that came from `already_applied` events are already reflected
in the current state, so no mutation is needed.

After writing, the backend notifies field-level signal subscribers for any
changed properties.

### Why lexicographic EventId works

`EventId` is a SHA-256 hash of `(entity_id, operations, parent_clock)`. The
lexicographic ordering of these hashes has no semantic relationship to wall-clock
time or branch depth. However, it provides a **deterministic total order** over
events. All replicas comparing the same pair of concurrent events will pick the
same winner, which is the property required for convergence. See
[Determinism](lww-merge.md#determinism) for a formal argument.


## Yrs Backend

Defined in `core/src/property/backend/yrs.rs`.

### Internal state

Yrs stores a `yrs::Doc` document, a `Mutex<StateVector>` tracking the previous
state (for diff computation in `to_operations`), and per-field broadcast
channels for change notifications.

### apply_layer for CRDTs

The Yrs `apply_layer` implementation is much simpler than LWW:

```rust
fn apply_layer(&self, layer: &EventLayer) -> Result<(), MutationError> {
    for event in &layer.to_apply {
        if let Some(operations) = event.operations.get("yrs") {
            for operation in operations {
                self.apply_update(&operation.diff, &changed_fields)?;
            }
        }
    }
    Ok(())
}
```

It ignores `already_applied` events entirely and applies all operations from
`to_apply` events. This works because:

1. **Commutativity** -- Yrs operations produce the same result regardless of
   application order. There is no need to compare candidates or pick winners.
2. **Idempotency** -- Yrs internally deduplicates operations using its state
   vector. Applying an already-integrated update is a no-op.

The `already_applied` events are ignored because their operations are already
reflected in the Yrs document state. Even if they were applied again, Yrs
would deduplicate them.

### The empty-string/null issue

Yrs does not distinguish between a text field that has never been written and
one that has been set to the empty string. When an entity is created with an
empty-string Yrs property, no CRDT operations are generated, which means no
creation event is produced and the entity may not be persisted. This is tracked
as issue #175 (see also `tests/tests/yrs_backend.rs`, the ignored
`test_sequential_text_operations` test and the [Known Gaps](testing.md#known-gaps-and-ignored-tests)
section of the testing strategy).


## EventLayer Helpers

The `EventLayer` struct is defined in `core/src/event_dag/accumulator.rs`. It
carries the events to process plus a shared reference to the [accumulated DAG](event-dag.md#core-concepts)
structure:

```rust
pub struct EventLayer {
    pub already_applied: Vec<Event>,
    pub to_apply: Vec<Event>,
    dag: Arc<BTreeMap<EventId, Vec<EventId>>>,
}
```

Two helper methods on `EventLayer` are used by the LWW backend:

### dag_contains

```rust
pub fn dag_contains(&self, id: &EventId) -> bool {
    self.dag.contains_key(id)
}
```

Checks whether an event ID is present in the accumulated DAG. The LWW backend
uses this to implement the "older than meet" rule: if a stored value's
`EventId` is not in the DAG, the value was written by an event that predates
the meet point, and any layer candidate wins against it unconditionally. This
avoids the need for a potentially expensive ancestry traversal to an event
outside the DAG.

### compare

```rust
pub fn compare(&self, a: &EventId, b: &EventId) -> CausalRelation {
    if a == b { return CausalRelation::Descends; }
    if is_descendant_dag(&self.dag, a, b) { return CausalRelation::Descends; }
    if is_descendant_dag(&self.dag, b, a) { return CausalRelation::Ascends; }
    CausalRelation::Concurrent
}
```

Determines the causal relationship between two events within the accumulated
DAG. The `is_descendant_dag` helper performs a backward BFS through parent
pointers from the candidate descendant, looking for the candidate ancestor.
Missing entries (events below the meet that were not accumulated) are treated
as dead ends rather than errors, making the method infallible.

Note: when `a == b`, the method returns `Descends`. Semantically an event does
not descend from itself, but in the LWW resolution context this means "same
writer, no replacement needed," which is functionally correct.

The `CausalRelation` enum has three variants (`Descends`, `Ascends`,
`Concurrent`) and is defined in `core/src/event_dag/layers.rs`. See also
[LWW Resolution Rules](lww-merge.md#lww-resolution-rules) for how these relations drive conflict resolution.


## Backend Registration and Late Creation

Backend instances are created on demand. The function `backend_from_string` in
`core/src/property/backend/mod.rs` maps a name string (`"lww"` or `"yrs"`) to
a constructor call, optionally initializing from a state buffer:

```rust
pub fn backend_from_string(name: &str, buffer: Option<&Vec<u8>>)
    -> Result<Arc<dyn PropertyBackend>, RetrievalError>
```

This is currently a hardcoded dispatch (with a TODO to implement a registry).

During layer application in [`Entity::apply_event`](entity-lifecycle.md#apply_event-in-detail) (in `core/src/entity.rs`),
the entity checks whether each `to_apply` event references a backend that does
not yet exist in the entity's backend map. If so, a new empty backend is
created and all previously applied layers are replayed on it before it receives
the current layer:

```rust
for evt in &layer.to_apply {
    for (backend_name, _) in evt.operations.iter() {
        if !state.backends.contains_key(backend_name) {
            let backend = backend_from_string(backend_name, None)?;
            for earlier in &applied_layers {
                backend.apply_layer(earlier)?;
            }
            state.backends.insert(backend_name.clone(), backend);
        }
    }
}
```

This ensures that a backend first encountered mid-merge receives the full
causal history from the meet point forward.

When an entity is first accessed via `Entity::get_backend<P>()`, the same
`backend_from_string` call creates a fresh backend if one does not exist. This
is how the LWW and Yrs backends are lazily initialized on a newly created
entity before any mutations occur.


## Operation Diff Format

### LWW

LWW operations are serialized as an `LWWDiff` struct containing a version byte
(currently `1`) and a bincode-encoded inner payload. The inner payload is a
`BTreeMap<PropertyName, Option<Value>>` mapping each changed property to its new
value. A `None` value represents deletion.

`to_operations` collects all `Uncommitted` entries, transitions them to
`Pending`, and returns a single `Operation` wrapping the serialized diff.
`apply_operations_internal` deserializes the diff and writes each property into
the backend's value map.

### Yrs

Yrs operations contain a single Yrs v2-encoded update diff. `to_operations`
computes the diff between the previous `StateVector` and the current document
state. If the diff equals `Update::EMPTY_V2`, it returns `None`.
`apply_operations` decodes the v2 update and applies it to the Yrs document via
`txn.apply_update`.


## Value Types

The value type wrappers in `core/src/property/value/` present backends to user
code through the derive macro system:

| Type | Backend | Purpose |
|------|---------|---------|
| `LWW<T>` | LWWBackend | Scalar property with last-writer-wins semantics. `T` implements `Property` (String, i64, Json, Ref, etc.) |
| `YrsString<P>` | YrsBackend | Collaboratively-editable text with insert/delete/replace operations |
| `Json` | LWWBackend (via `LWW<Json>`) | Structured JSON data stored as a scalar LWW value |
| `Ref<T>` | LWWBackend (via `LWW<Ref<T>>`) | Typed entity reference storing an `EntityId` |

`LWW<T>` delegates `set` and `get` to `LWWBackend::set` / `LWWBackend::get`.
`YrsString<P>` delegates `insert`, `delete`, `replace`, and `overwrite` to the
corresponding `YrsBackend` methods, which operate on a named Yrs `Text` type
within the document.

Both types enforce write guards: calling `set` or `insert` on a non-writable
entity (i.e. not inside an active [transaction](entity-lifecycle.md#local-creation-transaction-path)) returns
`PropertyError::TransactionClosed`.


## How It All Fits Together

The end-to-end flow for a concurrent merge:

1. A remote event arrives and is [staged](event-dag.md#the-staging-pattern). [`Entity::apply_event`](entity-lifecycle.md#apply_event-in-detail) calls
   [`compare()`](event-dag.md#the-comparison-algorithm) which traverses the DAG and returns `DivergedSince`.
2. The [`EventAccumulator`](event-dag.md#core-concepts) from the comparison is consumed to produce
   [`EventLayers`](event-dag.md#core-concepts) -- an async iterator of `EventLayer` values in topological
   order from the meet.
3. Under the entity's write lock, each layer is applied to every backend via
   `apply_layer`. The LWW backend [resolves per-property winners](lww-merge.md#lww-resolution-rules) using
   [`dag_contains`](#dag_contains) and [`compare`](#compare). The Yrs backend applies CRDT updates.
4. Late-created backends receive replayed earlier layers before the current one.
5. The entity head is updated: meet IDs are removed and the new event ID is
   inserted.
6. Signal subscribers are notified of the change.
