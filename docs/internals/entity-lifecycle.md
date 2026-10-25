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

**5. Publish.** After storage commits, compare the resident's head with the head
captured when its working fork was created. If unchanged, copy the prepared state
under the resident's write lock, preserving its identity and broadcast, without
another event comparison or application. If the resident advanced, reconcile the
retained events through ordinary checked application instead. A creation transfers
its prepared state into a newly registered resident entity. Redirect transaction
views and emit change notifications. `EntityChange` packages the accepted events
without revalidating their history.
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
  For an existing entity, `apply_and_commit_state` adopts the snapshot if it
  extends the resident; otherwise it applies the accompanying event batch.
  Snapshot adoption checks the events against its head without replaying their
  operations. Both paths persist all accompanying events atomically with the
  resulting state before publication. A write conflict repeats the decision
  against a fresh fork, and an already-applied batch produces no change notification.

**State application** (Get responses) applies each state directly. Existing
entities use the same snapshot coordinator with no accompanying events: a
concurrent snapshot leaves the resident unchanged, including on retry.

For every multi-event payload -- *EventOnly* and *StateAndEvent* alike -- the
receiver validates and stages the whole batch, then
topologically sorts it by in-batch parent edges (`event_dag/ordering.rs`) and
applies parents before children. Sender order is not trusted: applying a child
before its staged parent would fast-forward the head past the parent, whose
operations would then be silently dropped as `StrictAscends`.


## How Events Are Applied

Local commits and remote delivery use `RemoteTrxEntity::apply_events` to prepare
and admit events on an isolated fork. `EntityState::apply_events` reconciles
already-admitted batches with a resident whose head moved before publication.
Both use the same preparation and backend layer application. Readable `Entity`
handles do not expose these methods.

The head records the event IDs at the frontier of the entity's applied
history, with a [generation](event-dag.md#generations) for each tip.
Comparing those heads through the [event DAG](event-dag.md) determines
whether an incoming event extends, duplicates, or diverges from that history.
An update can be stored before it is applied to a resident entity, so storage
presence alone does not establish that its operations have been applied.
Comparison returns `Equal` or `StrictAscends` for an already-integrated update,
and application leaves the state unchanged.

### Guard Ordering

Creation and empty-head guards precede application:

1. **Creation event on a non-empty head.** A stored genesis identifies a
   re-delivery and is a no-op. If the genesis is absent and
   [storage is definitive](node-architecture.md#durable-vs-ephemeral-nodes),
   reject it as `Disjoint`. An ephemeral node may have adopted a snapshot
   without storing its genesis, so absence there requires causal comparison
   to distinguish re-delivery from a different genesis.

2. **Creation event on an empty head.** Comparison returns an empty meet.
   Layer construction loads the complete staged creation chain, and locked
   application checks the starting head before applying it.

3. **Update without a creation chain on an empty head.** Reject with `InvalidEvent`
   before comparison. A creation batch must supply the genesis and every parent
   of its subsequent updates in causal order.


### The Retry Loop

For a batch, preparation checks the generation bound in causal order against
an advancing head, then compares the batch's final tips against the starting
head once. All history reads and layer construction finish before application.
A preparation error rejects the whole batch without changing state or running
write-policy checks.

`EventLayers` holds the fully loaded layers and the boundaries for each input
event. Layers belong to the first supplied event that needs their operations.
Each event is applied before its write-policy check, so that check sees its own
resulting state without later events. If an operation or policy check fails,
the caller discards the transaction fork and none of the batch is committed.
Other entity items in a received update continue independently.

Resident publication retries preparation if the head moved, up to five attempts.
The comparison's [causal relation](event-dag.md#key-concepts) determines the layers:

| Relation | Action |
|----------|--------|
| `Equal` | Already integrated -- no-op |
| `StrictDescends` | Use the compared head as the meet; apply layers and replace the superseded tips |
| `StrictAscends` | Event is older than current state -- no-op |
| `DivergedSince` | True concurrency -- compute [event layers](event-dag.md#key-concepts) from the meet point, merge per-backend via [`apply_layer`](property-backends.md#the-propertybackend-trait), add the layers' membership additions, then advance the head through the newly applied events, preserving concurrent tips |
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
retry loop re-runs comparison against the fresh value. Both `apply_events` and
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
   receiver has), the applier falls back to atomic application of the
   accompanying batch. It does not assume that batch contains the snapshot's
   entire history or fetch its head events as a substitute on retry.
