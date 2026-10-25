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

Data moves between nodes through two mechanisms: **streaming updates**, which
a node pushes to a peer on the update channel their connection carries, and
**requests**, each of which gets exactly one response. Entity data travels in
updates: a query's initial data, the data its repairs ask for, and a
subscription's live changes all arrive the same way. The requests that open and
repair a query, and their responses, exchange accounts of the query's result
rather than entity data. `Get` and `GetEvents`, which read entities and events
by ID, still answer with them directly.

### Streaming updates (`UpdateContent`)

Each update carries `SubscriptionUpdateItem`s, one per entity. An item carries
either the events alone (`EventOnly`) or an entity state plus events
(`StateAndEvent`). A subscription's live change carries the new state with the
events that produced it. Data that a comparison or a repair pushes carries
either the events from the head the receiver holds, as `EventOnly`, or the
current state alone, as `StateAndEvent` with no events
([below](#comparing-query-results-knownstate)). On the receiving side the node
validates what it receives, integrates the events via
[`apply_events`](entity-lifecycle.md#how-events-are-applied), and persists the
result. If the incoming state diverges from what the receiver already has,
the receiver falls back to batch `apply_events` with
[BFS comparison](event-dag.md#comparing-two-clocks).

A node admits a peer's updates only while it holds a subscription with that
peer or has a Fetch in flight with it, and applies them under those queries'
credentials. An item can also name the queries it concerns on the sender and
how the entity's membership changed in each (`Initial`, `Add` or `Remove`),
but the receiver does not act on these tags: its own reactor decides what each
applied change means for its queries.

### Comparing query results (`KnownState`)

`Fetch` and `SubscribeQuery` carry the requester's `KnownState`, an account of
its local copy of the query's result: every entity head when the result is
small, otherwise digests of spans of it. The responder compares the account
with a snapshot of its own result, answers with its own `KnownState`, the
target, and pushes as an update each entity the account shows the requester
lacks or holds at another head, tagged `Initial` for a subscription. The
requester's copy is ready when it matches or exceeds the target. The requester
repairs what the pushes do not bring: at once where its account gave only
digests, and whenever its copy stops progressing short of the target.
`CompareSpans` asks for the responder's digests of spans of its result or a
page of its heads, and `SyncEntities` names the entities to push, whose data
again arrives as updates. The
[synchronization guide](../learn/synchronization.md) walks through each step, and its
[reference](../learn/synchronization/reference.html) lists the messages,
readiness rules and limits.

For each entity it pushes, the responder sends one of two forms, depending on
what the requester holds:

- **State** -- the requester holds no head of the entity, or its head cannot
  be bridged. The current state is sent.
- **Event bridge** -- the requester holds an older head, the responder can
  trace a clean forward path through the DAG from it to the current head, and
  the requester's read policy admits every event on that path. Only the
  missing events are sent, even when the state is smaller: a requester adopts
  a state over an older copy only after proving its lineage, which fetches
  those same events one request at a time.

The event bridge is especially valuable after brief disconnections: a handful
of events is far cheaper than retransmitting the full state. The bridge is
built by walking backward from the current head through parent pointers until
every frontier member is in the requester's known head, then sorting the
collected events parents-first.

> **Known limitation:** The backward walk currently has no traversal budget. A
> stale or malicious `known_head` could trigger unbounded event collection.
> Chunked bridge framing, size limits, and resource governance are tracked in
> the phase-2 spec (`specs/concurrency/phase-2.md`).


## Subscription Propagation

Subscriptions are how ephemeral nodes stay synchronized with durable peers.
The flow has three phases: establishment, streaming, and recovery.

**Establishment.** When application code creates a live query on an ephemeral
node, the node registers it as a pending subscription. When a durable peer is
(or becomes) available, the node sends `SubscribeQuery` with a `KnownState` of
its local copy of the query's result. The durable peer installs the query in
its reactor subscription for that peer before it takes the snapshot it
compares, so every change reaches the copy either through the comparison or as
a live update. It then answers with its target and pushes what the copy lacks,
tagged `Initial`. The query is established once its copy matches or exceeds
the target, not when the response arrives; the
[live query walkthrough](../learn/synchronization/livequery.html) follows each
step.

**Streaming.** After establishment, the durable node pushes updates whenever
entities matching the subscribed query change. The ephemeral node applies these
incrementally, as described [above](#streaming-updates-updatecontent).

**Recovery.** If the durable peer disconnects, all subscriptions associated with
it revert to pending and are automatically re-established when a new durable
peer connects. A background retry loop (every 5 seconds) also picks up any
subscriptions that failed with transient errors, including a sync whose copy
stopped progressing. Permanent failures (access denied, server error) are
marked as failed and not retried.

An update lost while the peer stays connected would otherwise go unnoticed
until the entity changes again, so each established query also checks its copy
against the durable peer's result periodically: first 30 seconds after it is
established, then twice as long after each check that agrees, up to every 5
minutes. Because updates still in flight can explain one disagreement, the
query repairs its copy only when two checks in a row disagree, and checks again
30 seconds after a disagreement or a repair. A repair whose data fails to apply
leaves the query established with that entity stale, as a live update that
fails to apply does; its checks back off as if they agreed, and the next one
that disagrees repairs without waiting for a second.

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
