# The Compare-Apply Cycle

Every mutation that reaches an entity -- whether from a local commit or a
remote peer -- travels the same pipeline: the events are made discoverable,
the batch's final tips are compared against the entity's current head once, and
the prepared layers are applied event by event with intervening policy checks. The reference chapters each describe one stage of that pipeline in
isolation. This chapter traces **one concrete divergence through the entire
cycle**, end to end, with every intermediate value shown.

```text
arrive -> stage -> compare -> integrate -> commit -> persist -> notify
```

What `integrate` means is decided by the comparison verdict -- six are
possible (see the [verdict table](entity-lifecycle.md#the-retry-loop)). The
interesting one is `DivergedSince`, true concurrency, so that is the path we
walk. The others appear at the [end](#the-paths-not-taken).


## The Scenario

Two replicas, A and B, share an entity with a short linear history:

```text
    G                genesis: creates the entity, title = "draft"
    |
    P                title = "v1"          (both replicas at head [P])
```

Working offline, each replica commits one event against head `[P]`:

- Replica A commits **X**: `title = "alpha"`. A's head is now `[X]`.
- Replica B commits **Y**: `title = "beta"`. B's head is now `[Y]`.

```text
    G
    |
    P                <-- the meet (neither replica knows this yet)
   / \
  X   Y              concurrent: neither saw the other
```

EventIds are content hashes; in this telling `id(X) > id(Y)`. Now Y arrives
at replica A over a subscription. Everything below is replica A's view.


## Stage 0: Arrival

The [node applier](node-architecture.md#the-replication-protocol) receives a
batch of events. Three things happen before any comparison:

1. **Staging.** Validated events enter the in-memory staging map, making them
   visible to `get_event` -- the
   [union view of staging plus storage](retrieval.md#staging-vs-permanent-storage).
2. **Topological sort.** The batch is sorted parents-first
   (`event_dag/ordering.rs`). Batch preparation rejects a child appearing before
   its in-batch parent; sender order is never trusted.
3. **Guards.** Batch preparation checks admission against A's current head.
   Y is not a creation event, A's head is non-empty, and Y's generation does not
   exceed the head's next generation, so the checks pass.

Only after application succeeds are the events and resulting state
[persisted together](event-dag.md#the-staging-pattern), before publication
updates the resident entity.


## Stage 1: Compare

`apply_events` calls `compare` with the subject clock `[Y]` (just the incoming
event's id) against the comparison clock `[X]` (the current head).

**Quick-check first.** The
[linear-extension fast path](event-dag.md#quick-check-the-linear-extension-fast-path)
tries to follow Y's parent links down to every tip of `[X]`. Y's parent P
is older than X and outside that head, so this shortcut cannot prove descent.
Comparison falls through to the
[two-sided traversal](event-dag.md#generation-guided-traversal).

**Walk newer generations first.** After fetching the compared tips Y and X,
comparison sees their shared parent P. With one meet and every remaining path
crossing it, it can stop without reading the older shared history G.

Result: `DivergedSince { meet: [P], subject_chain: [Y], other_chain: [X] }`.
The accumulated parent links cover both tips down to the meet. Ordinary LWW
resolution needs only stored write ranks for writers older than that boundary.
Multiple unresolved meet nodes and saturated generations retain exhaustive
traversal; see [comparison](event-dag.md#comparing-two-clocks).


## Stage 2: Layers

`EventLayers::prepare` uses the comparison's accumulated history to load the
[application layers](event-dag.md#event-layers). P is marked as processed:

```text
Layer 1: { X, Y }
         X -> already-applied   (in head ancestry)
         Y -> to-apply          (the novel branch)
```

P's operations are already represented in both replicas' stored states and
are not replayed. The shared parent-link map lets each layer inspect causal
relationships throughout the histories from the meet to the compared heads.


## Stage 3: Apply

After all layers are loaded, `apply_events` takes the entity write lock and
[re-checks that the head is still `[X]`](entity-lifecycle.md#toctou-protection).
It then feeds the layer to **every** backend.

**LWW runs a per-property election**
([resolution rules](lww-merge.md#resolution-rules)). For `title`:

| Challenger | Source | vs. incumbent | Outcome |
|------------|--------|---------------|---------|
| *(seed)* | stored state | -- | incumbent = ("alpha", X) |
| X: "alpha" | already-applied | same event | incumbent keeps |
| Y: "beta" | to-apply | equal generation; `id(X) > id(Y)` | incumbent keeps |

The winner is X -- an *already-applied* event. Winners only mutate state when
they come from to-apply events, so **nothing is written and no field signal
fires**. X's stored write rank is sufficient to reject Y even if X's event
is absent from the accumulated DAG.

**Yrs** ([contrast](property-backends.md#yrs-backend)) would simply apply Y's
operations and ignore the rest -- CRDT commutativity needs no election.

**Head update.** After all layers succeed, `apply_layers` advances the head
through each newly applied event. Here it removes Y's parent P (which is not
in the head) and inserts Y:

```text
head: [X]  ->  [X, Y]
```

The punchline of this trace: **the merge changed no property value, yet it
still changed the entity** -- the head grew a second tip, recording that Y's
lineage is now integrated. `apply_events` reports one applied event and the entity-level
signal fires (field-level signals stay quiet). The caller then commits Y to
permanent storage and persists the state with its two-tip head.


## Stage 4: Convergence on the Other Side

Replica B runs the mirror image when X arrives: same comparison, same meet `[P]`,
same single layer -- but partitioned as `already = {Y}`, `to-apply = {X}`.
The election reaches the same winner X, and this time the winner **is**
to-apply: B writes `title = ("alpha", X)`, the field signal fires, and B's
head becomes `[Y] -> [X, Y]`.

Both replicas now hold identical state *and* identical heads, having applied
the branches in opposite orders. The tiebreak (`max` over EventIds) is
commutative and associative, which is the heart of the
[determinism argument](lww-merge.md#determinism).


## Stage 5: Healing the Fork

The two-tip head is not a degenerate state, but it does not persist forever.
The next mutation on either replica -- say A commits Z, `title = "final"` --
uses the full head as its parent clock: `parent(Z) = [X, Y]`.

When Z arrives at B (head `[X, Y]`), the
[quick-check](event-dag.md#quick-check-the-linear-extension-fast-path) fires:
Z's parents are non-empty and all lie inside the comparison clock, and
together they cover it. Verdict: `StrictDescends`, **no BFS, no fetches of X
or Y at all**. The previous head `[X, Y]` becomes the meet, producing one
layer containing Z. The same backend application path runs and the head collapses:

```text
head: [X, Y]  ->  [Z]
```

The fork is closed. One ordinary event, created with no knowledge that it was
"merging" anything, reunifies the lineage simply by naming both tips as its
parents.


## The Paths Not Taken

For completeness, the same machinery handles every other arrival shape:

- **Re-delivery of Y** (head `[X, Y]`): the BFS's comparison side reaches Y --
  a subject head -- in one step. Verdict `StrictAscends`, no-op. Idempotency
  falls out of the comparison; there is no separate dedup table.
- **Equal** -- the incoming clock *is* the head: no-op before any traversal.
- **Disjoint** -- traversal bottoms out at two different genesis events:
  rejected. See the
  [creation guards](entity-lifecycle.md#guard-ordering) for the cheap
  durable-node shortcut.
- **BudgetExceeded** -- the traversal ran out of
  [budget even after escalation](event-dag.md#budget-escalation): surfaced as
  an error carrying both frontiers.
- **Unfetchable meet** -- on
  [ephemeral nodes](node-architecture.md#durable-vs-ephemeral-nodes) the
  traversal may hit an event that exists only on the durable peer; if it sits
  on [both frontiers](event-dag.md#unfetchable-events-on-both-frontiers) it is
  treated as the meet and the cycle proceeds normally.


## Where to Go Deeper

Each stage of this trace has a reference chapter:

| Stage | Chapter |
|-------|---------|
| Staging, storage traits | [Event Retrieval and Staging](retrieval.md) |
| Guards, retry loop, TOCTOU | [Entity Lifecycle](entity-lifecycle.md#how-events-are-applied) |
| BFS, verdicts, meet, layers | [Event DAG Subsystem](event-dag.md) |
| Election rules, determinism | [LWW Merge Resolution](lww-merge.md) |
| Backend contract, Yrs | [Property Backends](property-backends.md) |
| The tests that pin all of this | [Testing Strategy](testing.md) |
