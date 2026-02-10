# Testing Strategy: Event DAG and Concurrent Updates

## Overview

The concurrent update system is tested at three levels: **unit tests** in the
`core` crate that exercise the [DAG algorithms](event-dag.md) in isolation, **integration tests**
in the `tests/` crate that run real [nodes](node-architecture.md) with storage and propagation, and
**staging lifecycle tests** in `core/src/retrieval.rs` that verify the
[stage-compare-commit protocol](event-dag.md#the-staging-pattern). Together they cover [causal comparison](event-dag.md#the-comparison-algorithm), [LWW](lww-merge.md) and
[Yrs](property-backends.md#yrs-backend) conflict resolution, determinism, idempotency, multi-node convergence, and
edge cases.


## Test Categories

### Unit tests (`core/src/event_dag/tests.rs`)

These tests construct DAG topologies by hand using a `MockRetriever` and call
[`compare()`](event-dag.md#the-comparison-algorithm) directly, verifying the [`AbstractCausalRelation`](event-dag.md#core-concepts) returned. No node,
transaction, or storage engine is involved.

The file is organised into sections by phase:

| Section | What it covers |
|---------|---------------|
| Basic comparison | Linear, concurrent, incomparable, empty-clock, self-comparison |
| Missing event / busyloop | `EventNotFound` on one frontier vs both frontiers |
| Multi-head (Phase 2A) | Event extending one tip, all tips, three-way head |
| Deep concurrency (Phase 2G) | Symmetric/asymmetric deep diamond, short branch from deep point, late-arrival long branch |
| Chain ordering (Phase 2G) | Forward chain in `StrictDescends`, forward chains in `DivergedSince` |
| Idempotency (Phase 2G) | Redundant delivery of head event (`Equal`), delivery of historical ancestor (`StrictAscends`) |
| LWW layer (Phase 3) | [`apply_layer`](property-backends.md#conflict-resolution-via-apply_layer) with [lexicographic tiebreak](lww-merge.md#lww-resolution-rules), already-applied suppression, multiple properties, three-way concurrency |
| Yrs layer (Phase 3e) | Concurrent inserts, already-applied filtering, order-independent convergence, empty `to_apply` |
| Determinism (Phase 3e) | Two-event, three-event (all permutations), multi-property, sequential-layer |
| Edge cases (Phase 3e) | Empty layer, event with no LWW ops, same event in both lists, many concurrent events, property deletion, event-id tracking |
| Phase 4: stored below meet | Stored event-id not in DAG loses to layer candidate |
| Phase 4: idempotency | Re-delivery of historical and head events |
| Phase 4: duplicate creation | Second creation event rejected as `Disjoint`; re-delivery of same creation is no-op |
| Phase 4: budget escalation | Budget 2 escalates to 8, succeeding where budget 1 (max 4) fails |
| Phase 4: mixed-parent merge | Merge event whose parents span the meet boundary layers correctly |
| Phase 4: eager storage BFS | Eagerly stored events are discoverable by BFS; accumulator produces valid layers |

### Integration tests (`tests/tests/`)

These tests spin up real `Node` instances ([durable and ephemeral](node-architecture.md#durable-vs-ephemeral-nodes)), create
entities via the [transaction API](entity-lifecycle.md#local-creation-transaction-path), commit events, and assert on persisted DAG
structure, property values, and cross-node convergence.

| File | Focus |
|------|-------|
| `concurrent_transactions.rs` | Two concurrent transactions on same entity; many (5) concurrent transactions; long lineage (20 events) before fork |
| `determinism.rs` | Same-property two-event determinism across nodes; deep chain structure; multi-property diamond; three-way concurrent fork |
| `lww_resolution.rs` | Deeper branch wins; sequential writes; [lexicographic tiebreak](lww-merge.md#lww-resolution-rules); [per-property](lww-merge.md#per-property-independence) concurrent writes with merge; order independence across trials |
| `edge_cases.rs` | Single-event entity; rapid (20) concurrent transactions; deep (50-event) lineage fork; multiple fork-merge cycles; empty transaction; [`apply_state`](entity-lifecycle.md#apply_state-in-detail) with diverged/ascended heads; empty clock handling; [state buffer](property-backends.md#the-statebuffers-format) round-trip; backend error handling; multi-head single-tip extension (same-node and cross-node); redelivery idempotency |
| `durable_ephemeral.rs` | [Ephemeral](node-architecture.md#durable-vs-ephemeral-nodes) writes / durable receives; durable writes / ephemeral observes; durable vs ephemeral concurrent write; late-arriving branch from deep history |
| `multi_ephemeral.rs` | Two [ephemeral nodes](node-architecture.md#durable-vs-ephemeral-nodes) with independent writes; two ephemeral same-property conflict; three ephemeral three-way race |

### Staging lifecycle tests (`core/src/retrieval.rs`)

Four tests exercise the [`LocalEventGetter`](retrieval.md#localeventgetter) [staging protocol](event-dag.md#the-staging-pattern):

- **`test_stage_then_get_event`** -- After `stage_event`, `get_event` finds the
  event.
- **`test_stage_does_not_affect_event_stored`** -- `event_stored` returns `false`
  for staged-but-not-committed events, preserving the creation-uniqueness guard.
- **`test_commit_makes_event_stored_true`** -- After `commit_event`,
  `event_stored` returns `true` and `get_event` still works (now from permanent
  storage).
- **`test_storage_is_definitive_reflects_durable_flag`** -- Durable getter
  reports `true`; ephemeral getter reports `false`.


## Test Infrastructure

### `MockRetriever` (unit tests)

`MockRetriever` implements [`GetEvents`](retrieval.md#getevents) with an in-memory `HashMap<EventId, Event>`.
Tests build DAG topologies imperatively:

```rust
let mut retriever = MockRetriever::new();
let ev_a = make_test_event(1, &[]);           // genesis
let id_a = ev_a.id();
retriever.add_event(ev_a);
let ev_b = make_test_event(2, &[id_a.clone()]); // child of A
retriever.add_event(ev_b);
```

`make_test_event(seed, parent_ids)` produces an event with deterministic
content-hashed IDs. The seed differentiates events; parent IDs form the clock.
No storage engine, node, or transaction is needed.

### `TestDag` and assertion macros (integration tests)

`TestDag` (defined in `tests/tests/common.rs`) assigns single-character labels
(A, B, C, ...) to events in the order they are committed. After each
`commit_and_return_events()`, call `dag.enumerate(events)` to register the
labels.

Two macros provide declarative DAG verification:

```rust
// Verify parent relationships
assert_dag!(dag, events, {
    A => [],       // genesis
    B => [A],
    C => [A],
    D => [B, C],   // merge
});

// Verify the entity head
clock_eq!(dag, state.payload.state.head, [D]);
```

Labels are stable and predictable because tests control commit order. When an
assertion fails, the macro reports expected vs actual parents and lists all
registered labels.

Property values are verified separately with standard `assert_eq!` calls, keeping
DAG topology, head state, and value assertions each focused.

### Test models

Integration tests use two model structs:

- **`Album`** -- [Yrs](property-backends.md#yrs-backend)-backed `name` and `year` fields (default `String` backend).
  Used for multi-node, edge-case, and concurrent-transaction tests.
- **`Record`** -- [LWW](property-backends.md#lww-backend)-backed `title` and `artist` fields (annotated with
  `#[active_type(LWW)]`). Used for determinism and [LWW resolution](lww-merge.md) tests.


## Key Scenarios and What They Prove

### Idempotency

Re-delivery of an already-applied event must be a no-op. This was the motivating
bug for the Phase 5 staging rewrite: the old [`compare_unstored_event`](event-dag.md#eliminating-compare_unstored_event) would
corrupt the head when a historical event was re-delivered.

**Unit:** `test_same_event_redundant_delivery` verifies that re-delivering the
head event returns `Equal`. `test_event_in_history_not_at_head` verifies that
re-delivering an ancestor returns `StrictAscends`.
`test_redelivery_of_historical_event_is_noop` (Phase 4) combines both cases.

**Integration:** `test_redelivery_of_ancestor_event_is_noop` in `edge_cases.rs`
builds a linear chain A-B-C, then re-delivers event B via
[`commit_remote_transaction`](node-architecture.md#durable-node-receives-remote-commit). It asserts that the head remains `[C]`, the entity
state is unchanged, and the event count stays at 3.

### Determinism

The same set of events applied in any order must produce the same state. [LWW
resolution](lww-merge.md#determinism) is by depth (deeper wins), with [lexicographic `EventId` tiebreak](lww-merge.md#lww-resolution-rules) at
equal depth.

**Unit:** `determinism_tests` module tests two-event, three-event (all six
permutations), multi-property, and sequential-layer determinism at the
`LWWBackend` level.

**Integration:** `test_two_event_determinism_same_property` creates events on
one node, replays them in reversed order on a second node, and asserts identical
title. `test_multi_property_determinism` verifies that concurrent updates to
different properties both appear. `test_three_way_concurrent_determinism` forks
three transactions from genesis and verifies the winner matches the highest
`EventId`.

### Concurrent merge correctness

**Diamond:** `test_concurrent_transactions_same_entity` (concurrent_transactions.rs)
commits B and C concurrently from A, verifies the DAG structure `A->[B,C]` and
head `[B,C]`, and checks that both property changes are applied.

**Deep diamond:** `test_deep_diamond_asymmetric_branches` (unit) builds an
8-event symmetric diamond (4 events per branch) and asserts
`DivergedSince { meet: [A] }` with correct chain lengths. The integration
counterpart `test_deep_diamond_determinism` builds a 5-event linear chain and
verifies structural integrity.

**Three-way:** `test_multihead_three_way_concurrency` (unit) verifies that event
E extending tip B of a three-head `[B,C,D]` returns `DivergedSince { meet: [B] }`.
`test_three_way_concurrent_determinism` (integration) forks three transactions
from genesis, verifies `assert_dag!` with `[B,C,D]` all parented on `[A]`, and
confirms the lexicographic winner.

**Multiple merge cycles:** `test_multiple_merge_cycles` goes through two
fork-merge cycles (A -> B||C -> D -> E||F -> G) and verifies the full 7-event
DAG structure with head `[G]`.

### Durable/ephemeral interaction

See [Node Architecture](node-architecture.md#durable-vs-ephemeral-nodes) for the
durable/ephemeral distinction and [Replication Protocol](node-architecture.md#the-replication-protocol)
for the message formats.

**Ephemeral writes, durable receives:** `test_ephemeral_writes_durable_receives`
creates an entity on the durable node, makes two concurrent writes on the
ephemeral node, waits for [propagation](node-architecture.md#subscription-propagation), and verifies the durable node has correct
DAG structure and property values.

**Durable vs ephemeral concurrent write:** `test_durable_vs_ephemeral_concurrent_write`
forks from the same head on both nodes, commits concurrently, and asserts that
both converge to identical state.

**Missing events / BFS meet points:** `test_both_frontiers_unfetchable_meet_point`
(unit) creates a diamond A->[B,C] where A is not in the retriever. [BFS](event-dag.md#bfs-traversal) discovers
A on [both frontiers](event-dag.md#unfetchable-events-and-both-frontiers-meet) and correctly returns `DivergedSince { meet: [A] }` without
erroring on the missing event. `test_missing_event_busyloop` verifies that a
missing event on only one frontier returns `EventNotFound` rather than looping.

**Late-arriving branch:** `test_late_arriving_branch` (integration) builds a
20-event linear history, then has the ephemeral node create a concurrent branch.
It verifies no [`BudgetExceeded`](event-dag.md#budget-escalation) and correct merged state.

### Creation event handling

See [Guard Ordering](entity-lifecycle.md#guard-ordering) for how creation events are handled in
`apply_event`.

**Duplicate creation:** `test_second_creation_event_rejected` creates an entity
with one genesis event, then applies a second genesis event with different
content. BFS finds two different roots and returns `Disjoint`, which is
mapped to `MutationError::LineageError(Disjoint)`.

**Re-delivery of same creation:** `test_redelivery_of_same_creation_event_is_noop`
re-delivers the exact same creation event and asserts `Ok(false)` (no-op).

**Disjoint genesis in BFS:** `test_incomparable` (unit) constructs two
independent DAGs sharing no common ancestor. Comparing events across them
returns `Disjoint` with correct `subject_root` and `other_root`.

### [Budget escalation](event-dag.md#budget-escalation)

`test_budget_escalation_succeeds` creates a 6-event chain. With initial budget 2,
the internal 4x [escalation](event-dag.md#budget-escalation) (to 8) succeeds. With initial budget 1 (max 4),
the chain exceeds the escalated budget and returns `BudgetExceeded`.
`test_budget_exceeded` confirms the same with a 20-event chain.


## Known Gaps and Ignored Tests

**TOCTOU retry exhaustion (`#[ignore]`):** `test_toctou_retry_exhaustion_produces_clean_error`
in `phase4_toctou_retry` is ignored. Testing retry exhaustion requires a mock that
modifies the entity head between comparison and the CAS attempt inside
[`try_mutate`](entity-lifecycle.md#the-try_mutate-toctou-protection), which requires interior mutability and precise timing control over
the entity's `RwLock`. The expected behavior is documented in the ignore message:
after `MAX_RETRIES` (5) attempts, `apply_event` returns
`Err(MutationError::TOCTOUAttemptsExhausted)`.

**Yrs empty-string as null (`#[ignore]`):** `test_sequential_text_operations`
in `tests/tests/yrs_backend.rs` is blocked on issue #236 ([Yrs empty-string
treated as null](property-backends.md#the-empty-stringnull-issue)). Creating an entity with empty-string content produces no CRDT
operations and no creation event.

**Yrs concurrency under full transactions:** The Yrs backend is tested for
order-independent convergence at the unit level (`yrs_layer_tests`) but
multi-node Yrs-specific concurrency tests (e.g., concurrent text inserts at
the same position across nodes) do not exist yet.

**Notification correctness:** No tests currently verify that subscribers receive
exactly one notification per event, in causal order, without duplicates.

**[`apply_state`](entity-lifecycle.md#apply_state-in-detail) with `DivergedSince`:** The spec calls for verifying that
`apply_state` returns `Ok(false)` when states have diverged. The integration
test `test_apply_state_diverged_since` creates the diverged topology but
verifies that both concurrent changes are applied rather than testing the
`apply_state` rejection path directly.

**Multi-column ORDER BY (`#[ignore]`):** Three tests in
`tests/tests/sled/multi_column_order_by.rs` are blocked on issue #210
(i64 values sorted lexicographically instead of numerically). These are
unrelated to the event DAG but appear in the same test crate.
