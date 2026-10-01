# Entity Lifecycle

> **0.10 preview.** This chapter describes the pending StorageEngine and entity
> changes. The type split below is not the released 0.9 implementation.

## Views, Mutables, and Entities

An entity is one replicated object with an identity, properties, and a set of
model memberships. A model struct describes a particular set of properties;
its generated View and Mutable expose those properties with typed accessors.
Two Views of different models can therefore read the same entity.

| Handle | Holds | Purpose |
|---|---|---|
| `AlbumView` (implements `View`) | `Entity` | Read an entity through Album's typed accessors. |
| `AlbumMut` (implements `Mutable`) | `LocalTrxEntity` | Edit Album's properties inside a transaction. |
| `Entity` | `Resident` or `Proxy` | Read properties and memberships without choosing a model struct. |

These types separate read access from the two ways of producing a change:
local property mutations and application of existing events.

<iframe src="figures/entity-handles.html" title="Entity handles and their permitted operations" style="width: 100%; height: 580px; border: 0;"></iframe>

[Open the diagram at full width](figures/entity-handles.html).

## The Type Boundaries

`Entity::Resident` holds an `Arc<EntityInner>`: the node's committed, in-memory
instance of that entity. Only `WeakEntitySet` can construct it. The set keeps a
weak reference, and `EntityInner` retains its registry. This enforces one live
resident entity per ID within that set, without keeping unused entities alive forever.

`Entity::Proxy` is a readable handle whose target can change. A View obtained
from a Mutable reads the transaction's working state through this proxy. When
the transaction finishes, the proxy follows its outcome. Ordinary Views fetched
from the context read the resident entity directly.

The two transaction types have different capabilities:

| Type | Creation | Editing | Accepts |
|---|---|---|---|
| `LocalTrxEntity` | `Pending`: identity is initially unset | `Mut`: retains and forks the resident entity | Property mutations, later frozen into events |
| `RemoteTrxEntity` | `New`: identity comes from the received genesis | `Mut`: retains and forks the resident entity | Existing events and state snapshots |

Their private `TrxEntityData` holds the common machinery: working state,
retained events, the transaction's alive flag, system epoch, and coordination
with its readable proxy. Each transaction entity owns its own data; sharing the
implementation does not share working state between transactions.

This structure enforces three boundaries:

- A readable `Entity` exposes neither property mutation nor event application.
- A `LocalTrxEntity` exposes mutation; a `RemoteTrxEntity` exposes application.
  Callers cannot switch modes on one transaction entity.
- Pending creations stay outside `WeakEntitySet`. Obtaining an ID does not
  publish a resident entity. Storage must commit before a transaction publishes one.

Detached state used for query evaluation has a separate type,
`TemporaryEntity`. It does not masquerade as a registered resident entity.

## What a Transaction View Reads

While a transaction is open, `mutable.read()` creates a View backed by its
proxy. That View can survive the transaction and follow later updates to the resident entity.

| Outcome | View of an edited entity | View of a new entity |
|---|---|---|
| Transaction open | Reads the working fork, including local edits | Reads the pending creation |
| Commit succeeds | Follows the updated resident entity | Follows the newly published resident entity |
| Rollback or drop | Returns to the current resident entity | Property reads and `mutable.read()` fail with `TransactionClosed` |

For example, an existing resident entity has `points = 1`. A transaction changes it to
`2`, and a View obtained from that Mutable reads `2` while an ordinary View of
the resident entity still reads `1`. After commit both read the resident entity's value.
After rollback the transaction View returns to the resident entity's value, including any changes
committed by other transactions in the meantime.

The proxy changes its target; the transaction entity never changes its kind.
Mutable handles stop accepting writes when their transaction closes. A
rolled-back creation reports an empty head and no memberships.


## Creation

`Transaction::create()` builds a pending `LocalTrxEntity`, initializing its
membership and properties without assigning an ID or registering a resident entity.
The first identity request freezes the mutations so far into a genesis event;
that event determines the entity ID. An explicit `id()` call or construction of
a `Ref` can make that request. If nothing asks for the ID, commit preparation
generates a single genesis containing all mutations. Mutations after an early
ID demand become a later update event.

| Before commit | Generated events |
|---|---|
| Create, edit, commit without demanding an ID | One genesis containing the final values |
| Create, demand ID, edit, commit | The retained genesis plus an update for later mutations |
| Create, demand ID, commit without further edits | The retained genesis alone |

Editing a committed entity forks its [backends](property-backends.md#the-propertybackend-trait),
memberships, and head into a `LocalTrxEntity` retaining the resident entity. New creations have no
upstream entity to fork.

System roots use the same preparation and publication machinery without a
user transaction. They are registered only after their genesis and state persist.


## Local Transaction Commit

Local mutations first become events, then use the same event-application type
as remote writes: `RemoteTrxEntity`. Here, "remote" describes the input mode
(existing events); local commit uses it too. The current implementation builds
a fresh application fork for validation and retries. Reusing the local working
fork is a follow-up tracked in
[#509](https://github.com/ankurah/ankurah/issues/509).

Five phases execute in order:

**1. Generate events.** Each entity's pending property operations (via
[`to_operations()`](property-backends.md#the-propertybackend-trait)) and
membership additions become an event. A creation whose ID was never demanded
gets a single genesis containing all of them. Otherwise they form an update
whose parent is the current head, retained alongside any genesis frozen earlier.
Unchanged edits are skipped, and their views return to the resident entity.

**2. Apply and authorize.** Preserve an original snapshot and a working
`RemoteTrxEntity` per entity. Apply each event and authorize its
original-to-current state transition. Retain the admitted events and write
them, with the resulting state, through one `StorageTransaction`. A rejection
discards the attempt without committing its writes or changing resident entities.

**3. Relay to peers.** Attested events are sent to
[durable peers](node-architecture.md#durable-vs-ephemeral-nodes). The commit
waits for peer confirmation. A storage retry does not relay them again.

**4. Persist.** Events and prepared states are persisted atomically, provided
storage still matches the heads from which the forks were made. A conflict
discards the attempt's application forks and repeats validation from the resident entities.

**5. Publish.** After storage commits, consume each working fork to apply its
retained events to its upstream resident entity. A creation instead transfers its
prepared state into a newly registered resident entity. Redirect transaction views and
emit change notifications.
The node serializes storage commit and WeakEntitySet publication; a losing writer
cannot retry its commit before the winner publishes. Peer waits and reactor
notification happen outside that lock.

An incoming `RemoteTransaction` starts at phase 2 with events already supplied
by the sender and does not relay them back for approval. The enclosing
`Transaction` or `RemoteTransaction` owns persistence;
`RemoteTrxEntity::commit()` performs the in-memory publication after storage
has committed.


## Remote Event Application

Replication updates arrive via `NodeApplier` through two delivery mechanisms (see
[Node Architecture and Replication](node-architecture.md) for the full
protocol):

**Subscription updates** come in two forms:
- *EventOnly* -- the common incremental case.
- *StateAndEvent* -- used for initial subscription delivery and fetch responses.
  The system first tries the fast path: apply the state snapshot directly. If
  that succeeds, done. If the state diverges (concurrent edits exist), it falls
  back to the accompanying events. This two-phase approach ensures events are
  never silently dropped on divergence.

**Delta application** (fetch/query responses) similarly comes as either a
*StateSnapshot* (applied directly) or an *EventBridge* (events connecting the
requester's known head to the responder's).

For every multi-event payload -- *EventOnly*, *StateAndEvent*, and
*EventBridge* alike -- the receiver validates and stages the whole batch, then
topologically sorts it by in-batch parent edges (`event_dag/ordering.rs`) and
applies parents before children. Sender order is not trusted: applying a child
before its staged parent would fast-forward the head past the parent, whose
operations would then be silently dropped as `StrictAscends`.


## How Events Are Applied

The private shared state implementation's `apply_event` is the central
integration point, used by both local commit and remote delivery. Readable
`Entity` handles do not expose it. It works in two stages: guard checks, then a retry loop.

The head clock records the event IDs at the frontier of the entity's applied
history. Comparing those heads through the [event DAG](event-dag.md) determines
whether an incoming event extends, duplicates, or diverges from that history.

### Guard Ordering

Three guards execute before the main logic, handling edge cases around creation
events and empty heads:

1. **Creation event on a non-empty head.** On
   [durable nodes](node-architecture.md#durable-vs-ephemeral-nodes) where
   storage is definitive, `event_stored() == true` identifies a re-delivery --
   no-op, while a not-yet-stored event proves different genesis -- reject as
   `Disjoint`. On ephemeral nodes, fall through to BFS which distinguishes
   re-delivery from different genesis.

2. **Creation event on an empty head.** Acquire the write lock, re-check that
   the head is still empty (TOCTOU protection), apply operations, set the head.

3. **Non-creation event on an empty head.** The entity was never created
   properly. Reject with `InvalidEvent` rather than letting BFS produce a
   spurious `DivergedSince(meet=[])`.

### The Retry Loop

After guards pass, `apply_event` enters a bounded retry loop (up to 5
attempts). Each attempt reads the current head, runs
[`compare()`](event-dag.md#comparing-two-clocks) against the event DAG,
and acts on the [`causal relation`](event-dag.md#key-concepts):

| Relation | Action |
|----------|--------|
| `Equal` | Already integrated -- no-op |
| `StrictDescends` | Direct descendant -- apply operations, advance head |
| `StrictAscends` | Event is older than current state -- no-op |
| `DivergedSince` | True concurrency -- compute [event layers](event-dag.md#key-concepts) from the meet point, merge per-backend via [`apply_layer`](property-backends.md#the-propertybackend-trait), add the layers' membership additions, update head (remove meet ancestors, insert the event id) so it reflects both tips |
| `Disjoint` | Different lineage -- error |
| `BudgetExceeded` | DAG traversal too deep -- error |

Retries happen when the head moves between comparison and mutation (see
[TOCTOU protection](#toctou-protection) below).


## How State Snapshots Are Applied

`apply_state` handles full state snapshots rather than individual events. It
follows the same compare-then-mutate pattern but **cannot merge divergent
state** -- merging requires the per-operation detail that only events carry
(see [LWW Merge Resolution](lww-merge.md)).

| Relation | Result |
|----------|--------|
| `Equal` | `AlreadyApplied` |
| `StrictDescends` | Load the snapshot's backends and memberships, advance the head -- `Applied` |
| `StrictAscends` | `Older` |
| `DivergedSince` | `DivergedRequiresEvents` -- caller must fall back to event-by-event application |
| `Disjoint` / `BudgetExceeded` | Error |

When a new state arrives for an entity that may not exist locally yet,
`WeakEntitySet::with_state` handles the lookup: check the in-memory weak set,
then local storage, then create from the incoming state if neither has it.


## TOCTOU Protection

Because DAG comparison is async (and lock-free), the head can move between
comparison and mutation. The `try_mutate` helper serializes this:

```rust
fn try_mutate(&self, expected_head: &mut Clock, body: F) -> Result<bool, E> {
    let mut state = self.inner.write().unwrap();
    if &state.head != expected_head {
        *expected_head = state.head.clone();
        return Ok(false);  // head moved -- caller should retry
    }
    body(&mut state)?;
    Ok(true)
}
```

If the head moved, the caller's `expected_head` is updated in place and the
retry loop re-runs comparison against the fresh value. Both `apply_event` and
`apply_state` use this pattern. Retries are bounded to 5 attempts.


## Head Clock Evolution

The head clock evolves through three patterns:

**Linear extension** -- the common case. Head is `[A]`, event `B` arrives with
`parent=[A]`, comparison yields `StrictDescends`, head becomes `[B]`.

**Divergence** -- two events `B` and `C` are created concurrently from `A`.
After applying `B` (head=`[B]`), `C` arrives and comparison yields
`DivergedSince{meet=[A]}`. After layer-based merge, head becomes `[B, C]` --
a multi-element clock indicating concurrent tips.

**Merge** -- event `D` arrives with `parent=[B, C]`, matching the current head
exactly. Head collapses back to `[D]`.


## Persistence Ordering

Events become durable together with the canonical state referencing them (see
[The Staging Pattern](event-dag.md#the-staging-pattern) and
[Crash Safety](retrieval.md#crash-safety)). Events, canonical state,
entity-model associations, and every affected model projection commit in one
exact-head storage batch. Event insertion is content-addressed and idempotent.

This gives clean crash recovery semantics:

- A failed or conflicting `StorageTransaction::commit` exposes none of its new events,
  canonical states, associations, projections, or index changes.
- A successful `StorageTransaction::commit` exposes all of those changes together.


## Key Invariants

1. **Atomic head, membership, and backend updates.** All three live under a
   single `RwLock` and are always updated together.

2. **TOCTOU protection on every mutation path.** Compare-then-mutate is
   serialized with bounded retries (5 attempts).

3. **Creation event idempotency.** Re-delivery is detected by the durable fast
   path or by BFS (`StrictAscends`). Neither corrupts state.

4. **Transaction snapshot isolation.** The resident entity is not modified until
   commit phase 5.

5. **Discoverable history; atomic event/state persistence.** During application,
   BFS can read the incoming event, retained fork events, and stored history.
   Events must be durable with the state whose head references them.

6. **StateAndEvent divergence fallback.** When `apply_state` does not apply
   the incoming state (divergence, or the state is older than what the
   receiver has), the applier falls back to event-by-event application.
   Events are never silently dropped on divergence.
