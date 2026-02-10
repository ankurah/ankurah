# Entity Lifecycle

## Mental Model

An entity in ankurah is a **replicated, convergent data object**. Its lifecycle
follows four phases:

```text
 ┌──────────┐     ┌──────────────┐     ┌────────────┐     ┌───────────┐
 │ Creation │────▶│ Local Mutation│────▶│   Commit   │────▶│ Persisted │
 └──────────┘     │ (transaction) │     │ (validate, │     │  (stored  │
                  └──────────────┘     │  relay,     │     │   state)  │
                                       │  persist)   │     └─────┬─────┘
                                       └────────────┘           │
                         ┌──────────────────────────────────────┘
                         ▼
                  ┌──────────────┐
                  │Remote Events │◀─── other nodes
                  │  (apply or   │
                  │   merge)     │
                  └──────────────┘
```

At every stage, two things determine what happens next:

- The **head clock** -- a set of event IDs recording which events have been
  integrated into the entity's current state.
- The **[event DAG](event-dag.md)** -- which determines whether an incoming
  update extends, duplicates, or conflicts with that state.

Head and [backend](property-backends.md) state are bundled under a single lock
so they are always updated atomically.


## Creation

An entity comes into existence through `Transaction::create()`. This does two
things:

1. **Mints a primary entity** with an empty head and empty backends, registered
   in a node-wide weak set (which guarantees at most one live instance per
   entity ID).
2. **Forks a transactional snapshot** by cloning every
   [backend](property-backends.md#the-propertybackend-trait) and the current
   head. The snapshot is `Transacted` -- it holds a back-pointer to its primary
   but is the only copy the user mutates.

This snapshot isolation means the primary entity stays read-only until commit.
User mutations (setting properties) go through the snapshot's backends, which
accumulate pending operations.

> **System root entities** follow a different path: they are created outside a
> transaction, have their properties set directly, and produce a creation event
> that is immediately applied and persisted. This is the only code path where
> a creation event is applied to the same entity that generated it.


## Local Transaction Commit

When a transaction commits, five phases execute in order:

**1. Generate events.** Each entity's backends are asked for pending operations
(via [`to_operations()`](property-backends.md#mutation-methods)). These become
an `Event` whose parent is the snapshot's current head. Entities with no
pending operations are skipped. A validation check ensures creation events can
only come from entities that were actually created through the transaction --
preventing "phantom entities."

**2. Fork-based validation.** For each entity/event pair, a second fork is
created as a validation sandbox. The event is
[staged](event-dag.md#the-staging-pattern), applied to the sandbox, and the
resulting before/after state is passed to the policy agent for attestation.
Attested events are committed to storage.

**3. Update heads.** Heads on the transacted entities are updated to include the
new event ID. This happens *before* relaying to peers -- so if a peer echoes the
event back, the local entity already recognizes it as already-integrated.

**4. Relay to peers.** Attested events are sent to
[durable peers](node-architecture.md#durable-vs-ephemeral-nodes). The commit
waits for peer confirmation.

**5. Persist state.** The event is applied to the upstream primary entity (via
`apply_event`), bringing it up to date. The entity's state is serialized and
persisted to storage. Change notifications are emitted to the reactor.


## Remote Event Application

Remote events arrive via `NodeApplier` through two delivery mechanisms (see
[Node Architecture and Replication](node-architecture.md) for the full
protocol):

**Subscription updates** come in two forms:
- *EventOnly* -- the common incremental case. Events are validated, staged, and
  applied one at a time.
- *StateAndEvent* -- used for initial subscription delivery and fetch responses.
  The system first tries the fast path: apply the state snapshot directly. If
  that succeeds, done. If the state diverges (concurrent edits exist), it falls
  back to applying the accompanying events individually. This two-phase approach
  ensures events are never silently dropped on divergence.

**Delta application** (fetch/query responses) similarly comes as either a
*StateSnapshot* (applied directly) or an *EventBridge* (events staged for
[BFS](event-dag.md#bfs-traversal) discovery, then applied in causal order).


## How Events Are Applied

`apply_event` is the central integration point, used by both local commit and
remote delivery. It works in two stages: guard checks, then a retry loop.

### Guard Ordering

Three guards execute before the main logic, handling edge cases around creation
events and empty heads:

1. **Creation event on a non-empty head.** On
   [durable nodes](node-architecture.md#durable-vs-ephemeral-nodes) where
   storage is definitive, a not-yet-stored event proves different genesis --
   reject as `Disjoint`. On ephemeral nodes, fall through to BFS which
   distinguishes re-delivery from different genesis.

2. **Creation event on an empty head.** Acquire the write lock, re-check that
   the head is still empty (TOCTOU protection), apply operations, set the head.

3. **Non-creation event on an empty head.** The entity was never created
   properly. Reject with `InvalidEvent` rather than letting BFS produce a
   spurious `DivergedSince(meet=[])`.

### The Retry Loop

After guards pass, `apply_event` enters a bounded retry loop (up to 5
attempts). Each attempt reads the current head, runs
[`compare()`](event-dag.md#the-comparison-algorithm) against the event DAG,
and acts on the [`causal relation`](event-dag.md#core-concepts):

| Relation | Action |
|----------|--------|
| `Equal` | Already integrated -- no-op |
| `StrictDescends` | Direct descendant -- apply operations, advance head |
| `StrictAscends` | Event is older than current state -- no-op |
| `DivergedSince` | True concurrency -- compute [event layers](event-dag.md#core-concepts) from the meet point, merge per-backend via [`apply_layer`](property-backends.md#the-propertybackend-trait), update head to reflect both tips |
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
| `StrictDescends` | Replace all backends from snapshot -- `Applied` |
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
    let mut state = self.state.write().unwrap();
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

State persistence follows a strict ordering invariant: **commit events to
storage before persisting state** (see
[The Staging Pattern](event-dag.md#the-staging-pattern) and
[Crash Safety](retrieval.md#crash-safety)).

This gives clean crash recovery semantics:
- Crash after `commit_event` but before `set_state`: recovery loads the old
  state and the event is re-applied on next delivery.
- Crash before `commit_event`: neither event nor updated state is persisted --
  a clean rollback.


## Key Invariants

1. **Atomic head + backend updates.** Both live under a single `RwLock` and are
   always updated together.

2. **TOCTOU protection on every mutation path.** Compare-then-mutate is
   serialized with bounded retries (5 attempts).

3. **Creation event idempotency.** Re-delivery is detected by the durable fast
   path or by BFS (`StrictAscends`). Neither corrupts state.

4. **Transaction snapshot isolation.** The primary entity is not modified until
   commit phase 5.

5. **Staging before comparison; commit before persistence.** Events must be
   staged (discoverable by BFS) before `apply_event` is called. Events must be
   committed to storage before entity state referencing them is persisted.

6. **StateAndEvent divergence fallback.** When `apply_state` returns
   `DivergedRequiresEvents`, the applier falls back to event-by-event
   application. Events are never silently dropped on divergence.
