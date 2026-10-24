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
which forces the system to do a deeper [DAG traversal](event-dag.md#generation-guided-traversal)
instead of taking the shortcut. See the
[creation-event guard](entity-lifecycle.md#guard-ordering) for the main place
this matters.

For details on the event-getter implementations that back this behavior, see
the [Event Retrieval and Staging](retrieval.md) document.


## The Replication Protocol

Query results move between nodes over **state streams**, which the
`ankurah_proto::state_sync` module documents in full. Commits travel
separately, as described under [Commit Paths](#commit-paths).

### Comparison rounds

A Fetch or SubscribeQuery carries a `KnownState`: the requester's account of the
query result it already holds, as digests of key ranges or as the head of every
entity in a range. The durable peer compares it with a snapshot of its own result
and sends what differs: questions that narrow the ranges whose digests disagree,
inventories of its heads, and a delta for each entity the requester lacks or holds
at an older head. Once the requester's copy covers every head the round offered, it
reports the round applied, and the peer releases its snapshot.

Each delta (`DeltaContent`) is one of:

- **State snapshot** -- the requester lacks the entity, or no event path connects
  its head to the target. The full state is sent.
- **Event bridge** -- the requester holds an older head, and the peer sends only the
  events that connect it to the target.

The event bridge is especially valuable after brief disconnections: a handful
of events is far cheaper than retransmitting the full state. The bridge is
built by walking backward from the target through parent pointers until every
frontier member is in the requester's head, then reversing the collected events
into causal (oldest-first) order. The walk is bounded, and a delta too large for
one frame travels in chunks.

### Live publications

A SubscribeQuery stream stays open after its first round. The durable peer
publishes each change to the query's membership, in revision order, with the
entity data it concerns. The ephemeral node applies the data via
[`apply_events`](entity-lifecycle.md#how-events-are-applied) and installs the
membership once its copy covers the published heads. If incoming state diverges
from what the receiver already has, the receiver falls back to batch
`apply_events` with [BFS comparison](event-dag.md#comparing-two-clocks). A later
round can compare the whole result again on the same stream.


## Subscription Propagation

Subscriptions are how ephemeral nodes stay synchronized with durable peers.
The flow has three phases: establishment, streaming, and recovery.

**Establishment.** When application code creates a live query on an ephemeral
node, the node registers it as a pending subscription. When a durable peer is
(or becomes) available, the node sends a SubscribeQuery carrying its
`KnownState`, and the durable peer answers with the stream's first
[comparison round](#comparison-rounds).

**Streaming.** After the first round, the durable peer sends the query's
[live publications](#live-publications) on the same stream.

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
against the [policy agent](entity-lifecycle.md#how-events-are-applied), applies
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
