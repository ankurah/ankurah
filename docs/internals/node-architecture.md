# Node Architecture and Replication Protocol

## What Is a Node?

A **node** is the unit of participation in an Ankurah deployment. It owns a
storage engine, holds live entities in memory, and communicates with peer nodes
to keep entity state converged. Every node can create, mutate, and query
entities -- the difference between node types is *how much history they keep*
and *where they go when history is missing*.

## Durable vs Ephemeral Nodes

Think of **durable nodes** as the archival backbone. A durable node persists
every event it accepts and every entity state snapshot. Because it has the
complete event history for every entity it knows about, it never needs to ask
anyone else for data. When it says "I have not seen this event," that statement
is authoritative.

**Ephemeral nodes** are lightweight participants -- typically client-side
processes. They also write to a local storage engine, but they often receive
entity state *without* the underlying events (via a state snapshot). Their
local storage is therefore incomplete: a missing event might simply mean "I was
told the answer without being shown the work." When an ephemeral node needs an
event it does not have, it transparently fetches it from a durable peer and
caches it locally.

This distinction surfaces in one critical API:
`storage_is_definitive()`. On a durable node this returns `true` -- a
negative lookup is conclusive. On an ephemeral node it returns `false`,
which forces the system to do a deeper [DAG traversal](event-dag.md#bfs-traversal)
instead of taking the shortcut. See the
[creation-event guard](entity-lifecycle.md#guard-ordering) for the main place
this matters.

For details on the event-getter implementations that back this behavior, see
the [Retrieval and Storage Layer](retrieval.md) document.


## The Replication Protocol

Data moves between nodes through two mechanisms: **streaming updates** (pushed)
and **request/response deltas** (pulled). Both carry entity state and/or events,
but they serve different moments in the lifecycle.

### Streaming updates (`UpdateContent`)

Once an ephemeral node has an active subscription with a durable peer, the
durable node pushes changes as they happen. Each update carries the new entity
state plus the events that produced it. On the receiving side the node
validates the state, integrates the events via
[`apply_event`](entity-lifecycle.md#apply_event-in-detail), and persists the
result. If the incoming state diverges from what the receiver already has, the
receiver falls back to per-event application with
[LWW merge resolution](lww-merge.md).

### Request/response deltas (`DeltaContent`)

When a subscription is first established -- or when an ephemeral node runs a
fetch -- the durable node assembles a delta for each entity. The choice of
delta depends on what the requester already knows:

- **State snapshot** -- the requester has never seen the entity, or the gap is
  too complex to bridge. The full state is sent.
- **Event bridge** -- the requester has a known (older) head and the durable
  node can trace a clean forward path through the DAG. Only the missing events
  are sent, avoiding a full state transfer.

The event bridge is especially valuable after brief disconnections: a handful
of events is far cheaper than retransmitting the full state. The bridge is
built by walking backward from the current head through parent pointers until
every frontier member is in the requester's known head, then reversing the
collected events into causal (oldest-first) order.

> **Known limitation:** The backward walk currently has no traversal budget. A
> stale or malicious `known_head` could trigger unbounded event collection.


## Subscription Propagation

Subscriptions are how ephemeral nodes stay synchronized with durable peers.
The flow has three phases: establishment, streaming, and recovery.

**Establishment.** When application code creates a live query on an ephemeral
node, the node registers it as a pending subscription. When a durable peer is
(or becomes) available, the node sends a subscribe request that includes a map
of entities it already knows and their head clocks. The durable peer responds
with deltas -- state snapshots for unknown entities, event bridges for known
ones -- and begins monitoring its local reactor for future changes.

**Streaming.** After establishment, the durable node pushes updates whenever
entities matching the subscribed query change. The ephemeral node applies these
incrementally, as described [above](#streaming-updates-updatecontent).

**Recovery.** If the durable peer disconnects, all subscriptions associated with
it revert to pending and are automatically re-established when a new durable
peer connects. A background retry loop (every 5 seconds) also picks up any
subscriptions that failed with transient errors. Permanent failures (access
denied, server error) are marked as failed and not retried.

Each subscription carries context data used for policy validation of incoming
events and state.


## Commit Paths

### Ephemeral node commits locally

An ephemeral node creates events referencing the entity's current head and
sends them to durable peers via a `CommitTransaction` request. The durable peer
validates, applies, and persists the events, then its reactor distributes the
changes to all other connected peers. The originating ephemeral node may see
its own events echo back as a streaming update; it recognizes them as
re-delivery and no-ops.

### Durable node commits locally

A durable node applies and persists events directly. Its reactor distributes
the resulting updates to connected ephemeral peers. There is no upstream relay
step because the durable node is itself the authority.

### Durable node receives remote commit

When a durable node receives events from a peer, it validates each event
against the [policy agent](entity-lifecycle.md#apply_event-in-detail), applies
it to the entity (forking first for safe validation), persists the result, and
notifies the reactor to propagate the change to other peers. See also the
[entity lifecycle](entity-lifecycle.md#local-transaction-commit) document for
the full commit flow.


## Integration Test Patterns

See the [Testing Strategy](testing.md#durableephemeral-interaction) document
for the full test matrix.

The `durable_ephemeral` tests exercise four core scenarios: ephemeral writes
propagated to a durable node (including DAG forks), durable writes observed by
an ephemeral node, cross-node concurrent writes that must converge, and
late-arriving branches from deep history.

The `multi_ephemeral` tests extend this to topologies with multiple ephemeral
nodes connected to a single durable node, verifying that independent writes,
[same-property conflicts](lww-merge.md#per-property-independence), and
three-way races all converge deterministically across all participants.
