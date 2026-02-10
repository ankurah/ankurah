# Property Backends

## What is a Property Backend?

Every entity in ankurah stores its mutable state in one or more **property
backends**. A backend is the layer responsible for three things: holding named
property values, applying mutations to those values, and deciding what happens
when two replicas change the same property concurrently.

The system ships two backends, each with a different conflict-resolution
strategy:

- **LWW (Last-Writer-Wins)** -- for scalar values (strings, integers, JSON,
  entity references). When two replicas write to the same property at the same
  time, a single winner is [chosen deterministically](lww-merge.md) so that
  every replica converges on the same result.
- **Yrs** -- for collaboratively-editable text, powered by the Yrs CRDT
  library. Concurrent edits are merged automatically; no winner needs to be
  chosen because CRDT operations are commutative and idempotent.

**When to use which:** Use LWW for any property where the value is a discrete
whole -- a name, a count, a JSON blob, a reference to another entity. Use Yrs
when the property is text that multiple users may edit simultaneously and you
want their keystrokes to merge rather than overwrite each other.


## The PropertyBackend Trait

Both backends implement a shared `PropertyBackend` trait (defined in
`core/src/property/backend/mod.rs`). Rather than listing every method, the
trait covers four responsibilities:

1. **Mutation** -- collect local changes into serializable operations, and
   apply incoming operations from other replicas.
2. **Serialization** -- snapshot the backend to a byte buffer and restore it
   later. Entity state is persisted as a `State` containing a `Clock` (the
   [head](event-dag.md#core-concepts)) and a `StateBuffers` map keyed by
   backend name (`"lww"`, `"yrs"`). See
   [Entity State Persistence](entity-lifecycle.md#entity-state-persistence).
3. **Query** -- read the current value of any property.
4. **Lifecycle** -- fork a copy for transaction isolation and subscribe to
   per-field change notifications.


## LWW Backend

### Value lifecycle

An LWW property value moves through three states:

| State | Meaning |
|-------|---------|
| **Uncommitted** | User called `set()` inside a transaction; change has not been collected yet |
| **Pending** | `to_operations()` collected the change; awaiting commit |
| **Committed** | Applied from a committed event; an `EventId` records which event wrote it |

Only **Committed** entries carry an `EventId`. The backend refuses to serialize
its state if any entry is still Uncommitted or Pending -- this prevents
persisting incomplete state.

### Conflict resolution

When [concurrent branches merge](entity-lifecycle.md#apply_event-in-detail),
the entity feeds each backend an `EventLayer` -- a batch of events in causal
order. For each layer the LWW backend must choose a single winning value per
property. The full algorithm is described in
[LWW Resolution Rules](lww-merge.md#lww-resolution-rules); the conceptual
steps are:

1. **Seed winners from stored state.** Every existing Committed value becomes a
   candidate. If its `EventId` is absent from the
   [accumulated DAG](event-dag.md#core-concepts), the value is flagged
   **older than meet**.

2. **Process layer events.** Each event in the layer may write to the same
   property. The new candidate is compared against the current winner:
   - If the current winner is *older than meet*, the new candidate wins
     unconditionally.
   - Otherwise, causal ordering decides: a causally newer event replaces an
     older one.
   - For **concurrent** events (no causal relationship), the tie is broken by
     lexicographic `EventId` comparison.

3. **Apply only new winners.** Winners from events not yet in the entity's
   state are written as Committed entries; winners from already-applied events
   need no mutation. Changed properties notify signal subscribers.

### The "older than meet" rule

When the stored value's `EventId` is not in the accumulated DAG, it was
written by an event that predates the [meet point](event-dag.md#core-concepts).
Any layer candidate is guaranteed to be at least as recent as the meet, so it
wins unconditionally. This avoids expensive ancestry traversals to events
outside the DAG.

### Why lexicographic EventId comparison is safe

`EventId` is a SHA-256 hash of `(entity_id, operations, parent_clock)`. The
ordering of hashes has no relationship to wall-clock time, but it provides a
**deterministic total order**: every replica comparing the same pair of
concurrent events will pick the same winner. That is all convergence requires.
See [Determinism](lww-merge.md#determinism) for the formal argument.


## Yrs Backend

The Yrs backend stores a `yrs::Doc` document internally and tracks a
`StateVector` for computing diffs.

Conflict resolution is far simpler than LWW: the backend applies every
operation from new events and ignores already-applied events entirely. This
works because:

- **Commutativity** -- Yrs operations produce the same result regardless of
  application order. No winner selection is needed.
- **Idempotency** -- Yrs deduplicates operations internally via its state
  vector. Re-applying an update is a no-op.

### Known limitation: empty-string/null ambiguity

Yrs cannot distinguish between a text field that has never been written and one
set to the empty string. An entity created with an empty Yrs property produces
no CRDT operations, which can prevent persistence. This is tracked as issue
\#175; see also [Known Gaps](testing.md#known-gaps-and-ignored-tests).


## Backend Registration

Backend instances are created on demand via `backend_from_string` in
`core/src/property/backend/mod.rs`, which maps a name (`"lww"` or `"yrs"`) to
a constructor.

During [layer application](entity-lifecycle.md#apply_event-in-detail), if an
event references a backend that does not yet exist on the entity, a new empty
backend is created and all earlier layers are replayed on it before the current
layer is applied. This ensures a backend first encountered mid-merge receives
the full causal history from the meet point forward.


## Value Types

User code interacts with backends through value-type wrappers generated by the
derive macro system (defined in `core/src/property/value/`):

| Type | Backend | Purpose |
|------|---------|---------|
| `LWW<T>` | LWW | Scalar property; `T` implements `Property` (String, i64, Json, Ref, etc.) |
| `YrsString<P>` | Yrs | Collaboratively-editable text with insert/delete/replace |
| `Json` | LWW (via `LWW<Json>`) | Structured JSON stored as a scalar |
| `Ref<T>` | LWW (via `LWW<Ref<T>>`) | Typed entity reference storing an `EntityId` |

Both `LWW<T>` and `YrsString<P>` enforce write guards: calling a mutating
method outside an active [transaction](entity-lifecycle.md#local-creation-transaction-path)
returns `PropertyError::TransactionClosed`.


## End-to-End Merge Flow

1. A remote event arrives and is [staged](event-dag.md#the-staging-pattern).
   [`Entity::apply_event`](entity-lifecycle.md#apply_event-in-detail) calls
   [`compare()`](event-dag.md#the-comparison-algorithm), which returns
   `DivergedSince`.
2. The [`EventAccumulator`](event-dag.md#core-concepts) produces
   [`EventLayers`](event-dag.md#core-concepts) -- topologically ordered batches
   of events from the meet point forward.
3. Each layer is applied to every backend: LWW
   [resolves per-property winners](lww-merge.md#lww-resolution-rules); Yrs
   applies CRDT updates.
4. Late-created backends receive replayed earlier layers first.
5. The entity head is updated and signal subscribers are notified.
