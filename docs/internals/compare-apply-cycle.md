# The Compare-Apply Cycle

Every mutation that reaches an entity -- whether from a local commit or a
remote peer -- travels the same pipeline: the event is made discoverable,
compared against the entity's current head, and integrated according to the
verdict. The reference chapters each describe one stage of that pipeline in
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

1. **Topological sort.** The batch is sorted parents-first
   (`event_dag/ordering.rs`). Neither discovery order nor the sender is
   trusted: applying a child before its staged parent would fast-forward the
   head past the parent, silently dropping the parent's operations as
   `StrictAscends`.
2. **Staging.** Each event is placed in the in-memory staging map, making it
   visible to `get_event` -- the
   [union view of staging plus storage](retrieval.md#staging-vs-permanent-storage).
   The BFS can now discover Y.
3. **Guards.** Y is not a creation event and A's head is non-empty, so the
   [creation guards](entity-lifecycle.md#guard-ordering) pass through to the
   retry loop.

Only after `apply_event` succeeds is the event
[committed to permanent storage, then entity state persisted](event-dag.md#the-staging-pattern)
-- stage before head update, commit before state persist.


## Stage 1: Compare

`apply_event` calls `compare` with the subject clock `[Y]` (just the incoming
event's id) against the comparison clock `[X]` (the current head).

**Quick-check first.** The
[linear-extension fast path](event-dag.md#quick-check-the-linear-extension-fast-path)
asks: do Y's parents all sit inside the comparison clock? Y's parent is `[P]`
and the comparison set is `{X}` -- no. The fast path does not apply; fall
through to the [BFS](event-dag.md#bfs-traversal).

**Backward BFS, step by step.** Both sides walk toward their ancestors
simultaneously:

| Step | Fetched | Subject frontier after | Comparison frontier after | Notes |
|------|---------|------------------------|---------------------------|-------|
| 1 | Y, X | `{P}` | `{P}` | Each side expands its own head |
| 2 | P | `{G}` | `{G}` | P was on **both** frontiers: common -- meet candidate; its parent G gains a common child |
| 3 | G | `{}` | `{}` | G also common (both sides reach it). Frontiers exhausted |

Neither side ever saw the *other side's head* (X is not an ancestor of Y or
vice versa), so neither `StrictDescends` nor `StrictAscends` fired. Both
frontiers are empty: exhaustion. The verdict is computed from the shared
bookkeeping:

- Meet candidates: `{P, G}` (reached from both directions).
- Meet filter -- candidates with **no common children**: P qualifies (its
  children X and Y are one-sided); G does not (its child P is common).
- **Meet = `[P]`.**

Result: `DivergedSince { meet: [P], subject_chain: [Y], other_chain: [X] }`.
Four events were fetched; the [budget](event-dag.md#budget-escalation) spent 4
of 1000. Note that exhaustion walked *below* the meet all the way to G -- the
accumulated DAG now holds the parent pointers of all four events, and that
completeness is what the next stage relies on.


## Stage 2: Layers

The accumulator is consumed into an [`EventLayers`](event-dag.md#event-layers)
iterator, seeded with the meet `[P]` marked as already emitted and the current
head `[X]` for partitioning. One generation of the forward topological sweep:

```text
Layer 1: { G, X, Y }
         G -> already-applied   (below the meet; inert context)
         X -> already-applied   (in head ancestry)
         Y -> to-apply          (the novel branch)
```

Everything lands in a single layer: G because its (empty) parent set is
trivially satisfied, X and Y because their parent P is the meet. This tiny
example exhibits both scope notes from
[Event Layers](event-dag.md#event-layers): a below-meet event rides along as
inert already-applied context, and the divergent region's members (X, Y) are
genuinely concurrent -- depth groups by causal distance, not by which replica
produced the event. P itself is **never emitted**: the meet's own writes are
already baked into stored state on both replicas and take no part in what
follows.


## Stage 3: Apply

`apply_event` collects all layers (the async fetches happen before locking),
takes the entity write lock, and
[re-checks that the head is still `[X]`](entity-lifecycle.md#toctou-protection).
It then feeds the layer to **every** backend.

**LWW runs a per-property election**
([resolution rules](lww-merge.md#resolution-rules)). For `title`:

| Challenger | Source | vs. incumbent | Outcome |
|------------|--------|---------------|---------|
| *(seed)* | stored state | -- | incumbent = ("alpha", X) |
| G: "draft" | already-applied | X descends G | incumbent keeps |
| X: "alpha" | already-applied | same event | incumbent keeps |
| Y: "beta" | to-apply | concurrent; `id(X) > id(Y)` | incumbent keeps |

The winner is X -- an *already-applied* event. Winners only mutate state when
they come from to-apply events, so **nothing is written and no field signal
fires**. This is the already-applied side doing its real job: providing the
context that stops a losing remote write from clobbering the local winner.

**Yrs** ([contrast](property-backends.md#yrs-backend)) would simply apply Y's
operations and ignore the rest -- CRDT commutativity needs no election.

**Head update.** The meet ids are removed from the head (P is not in it) and
Y is inserted:

```text
head: [X]  ->  [X, Y]
```

The punchline of this trace: **the merge changed no property value, yet it
still changed the entity** -- the head grew a second tip, recording that Y's
lineage is now integrated. `apply_event` returns `true` and the entity-level
signal fires (field-level signals stay quiet). The caller then commits Y to
permanent storage and persists the state with its two-tip head.


## Stage 4: Convergence on the Other Side

Replica B runs the mirror image when X arrives: same BFS, same meet `[P]`,
same single layer -- but partitioned as `already = {G, Y}`, `to-apply = {X}`.
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
or Y at all**. Z's operations are applied directly and the head collapses:

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
