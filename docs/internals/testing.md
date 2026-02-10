# Testing Strategy: Event DAG and Concurrent Updates

## Philosophy

Tests are structured in three levels, each proving something different:

1. **Unit tests** (`core/src/event_dag/tests.rs`) exercise [DAG comparison](event-dag.md#the-comparison-algorithm) and [conflict resolution](lww-merge.md) algorithms in isolation. No node, storage engine, or transaction is involved -- just hand-built topologies and direct function calls. These prove the algorithms are correct.

2. **Integration tests** (`tests/tests/`) spin up real [durable and ephemeral](node-architecture.md#durable-vs-ephemeral-nodes) `Node` instances, create entities via the [transaction API](entity-lifecycle.md#local-creation-transaction-path), commit events, and assert on persisted state and cross-node convergence. These prove the system behaves correctly end-to-end.

3. **Staging lifecycle tests** (`core/src/retrieval.rs`) verify the [stage-compare-commit protocol](event-dag.md#the-staging-pattern) -- that staged events are visible to comparison but do not appear committed until explicitly promoted, and that the durable/ephemeral flag is reported correctly.


## What the Tests Verify

### Idempotency

Re-delivery of an already-applied event must be a no-op. This was the motivating bug for the Phase 5 staging rewrite: the old [`compare_unstored_event`](event-dag.md#eliminating-compare_unstored_event) would corrupt the head when a historical event was re-delivered.

At the unit level, re-delivering the current head returns `Equal` and re-delivering an ancestor returns `StrictAscends` -- both no-ops. Integration tests build a linear chain, re-deliver a middle event via [`commit_remote_transaction`](node-architecture.md#durable-node-receives-remote-commit), and confirm the head, entity state, and event count are unchanged.

### Determinism

The same set of events applied in any order must produce identical state. [LWW resolution](lww-merge.md#determinism) is by depth (deeper branch wins) with [lexicographic `EventId` tiebreak](lww-merge.md#lww-resolution-rules) at equal depth.

Unit tests cover two-event, three-event (all six permutations), multi-property, and sequential-layer determinism at the `LWWBackend` level. Integration tests create events on one node, replay them in reversed order on a second node, and assert identical values. Three-way concurrent forks verify the winner matches the highest `EventId`.

### Concurrent merge correctness

Several topologies are tested at both levels:

- **Diamond merge** -- Two concurrent branches from a common ancestor, verifying DAG structure, multi-head state, and that both property changes are applied.
- **Deep diamond** -- Symmetric and asymmetric branches (up to 8 events) asserting `DivergedSince` with correct meet point and chain lengths.
- **Three-way concurrency** -- Three branches from the same head, verifying the lexicographic winner and `assert_dag!` structure.
- **Multiple merge cycles** -- Repeated fork-merge sequences (e.g., A -> B||C -> D -> E||F -> G) verifying the full DAG with final single head.
- **Missing events / BFS meet** -- Diamonds where the common ancestor is absent from the retriever. [BFS](event-dag.md#bfs-traversal) discovers it on [both frontiers](event-dag.md#unfetchable-events-and-both-frontiers-meet) and returns `DivergedSince` without erroring. A missing event on only one frontier correctly returns `EventNotFound`.

### Durable/ephemeral interaction

See [Node Architecture](node-architecture.md#durable-vs-ephemeral-nodes) for the distinction and [Replication Protocol](node-architecture.md#the-replication-protocol) for message formats.

Scenarios covered: ephemeral node writes while durable node receives via [propagation](node-architecture.md#subscription-propagation); both nodes fork from the same head and commit concurrently, then converge; a late-arriving branch from deep (20-event) history merges without [`BudgetExceeded`](event-dag.md#budget-escalation); multiple ephemeral nodes racing on the same property.

### Creation event handling

See [Guard Ordering](entity-lifecycle.md#guard-ordering) for how creation events are handled.

A second, different genesis event applied to an existing entity produces `Disjoint` (mapped to `MutationError::LineageError`). Re-delivery of the exact same creation event is a no-op. Unit tests additionally verify that two independent DAGs sharing no common ancestor return `Disjoint` with correct root identification.

### Budget escalation

Tests verify that [budget escalation](event-dag.md#budget-escalation) works: an initial budget of 2 internally escalates 4x to 8, which is enough for a 6-event chain. An initial budget of 1 (max 4 after escalation) correctly returns `BudgetExceeded` when the chain is longer.


## Test Infrastructure

**`MockRetriever`** (unit tests) -- Implements [`GetEvents`](retrieval.md#why-three-traits-instead-of-one) with an in-memory `HashMap`. Tests build topologies imperatively with `make_test_event(seed, parent_ids)`, which produces deterministic content-hashed IDs. No storage, node, or transaction needed.

**`TestDag`** (integration tests) -- Defined in `tests/tests/common.rs`. Assigns single-character labels (A, B, C, ...) to events in commit order. Two macros provide declarative verification:

```rust
assert_dag!(dag, events, {
    A => [],       // genesis
    B => [A],
    C => [A],
    D => [B, C],   // merge
});
clock_eq!(dag, state.payload.state.head, [D]);
```

**Test models** -- `Album` uses [Yrs](property-backends.md#yrs-backend)-backed fields for multi-node and edge-case tests. `Record` uses [LWW](property-backends.md#lww-backend)-backed fields for determinism and [resolution](lww-merge.md) tests.


## Known Gaps

**TOCTOU retry exhaustion** (`#[ignore]`) -- Testing retry exhaustion requires a mock that modifies the entity head between comparison and the CAS attempt inside [`try_mutate`](entity-lifecycle.md#the-try_mutate-toctou-protection), which needs interior mutability and precise timing over the entity's `RwLock`. Expected behavior: after `MAX_RETRIES` (5) attempts, `apply_event` returns `Err(MutationError::TOCTOUAttemptsExhausted)`.

**Yrs empty-string as null** (`#[ignore]`) -- Blocked on issue #236 ([empty-string treated as null](property-backends.md#the-empty-stringnull-issue)). Creating an entity with empty-string content produces no CRDT operations and no creation event.

**Yrs multi-node concurrency** -- Order-independent convergence is tested at the unit level, but multi-node Yrs-specific concurrency (e.g., concurrent text inserts at the same position across nodes) is not yet covered.

**Notification correctness** -- No tests verify that subscribers receive exactly one notification per event, in causal order, without duplicates.

**`apply_state` divergence path** -- The integration test creates a diverged topology but verifies that both concurrent changes are applied, rather than testing the [`apply_state`](entity-lifecycle.md#apply_state-in-detail) rejection path (`Ok(false)`) directly.

**Multi-column ORDER BY** (`#[ignore]`) -- Three tests in `tests/tests/sled/multi_column_order_by.rs` are blocked on issue #210 (i64 sorted lexicographically). Unrelated to the event DAG.
