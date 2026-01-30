# Adversarial Testing Suite: Implementation Plan

## Overview

This plan outlines how to build a comprehensive adversarial test suite for Ankurah. The approach combines:

1. **Property-based testing** for invariant verification
2. **Deterministic simulation** for distributed scenarios
3. **Fault injection** for crash consistency
4. **Targeted unit tests** for specific attack vectors

## Phase 1: Foundation & Infrastructure

### 1.1 Test Harness Extensions

Extend the existing `test-helpers` feature (from PR 235) with adversarial primitives:

```rust
// core/src/tests/adversarial.rs

/// Create entity with forged lineage (parent events that don't exist)
pub fn forge_lineage_entity(node: &Node, fake_parents: Clock) -> Entity;

/// Create event with mismatched entity_id
pub fn forge_event_wrong_entity(event: &Event, wrong_id: EntityId) -> Event;

/// Generate pathological DAG structures
pub fn generate_deep_chain(depth: usize) -> Vec<Event>;
pub fn generate_wide_fork(width: usize, parent: Clock) -> Vec<Event>;
pub fn generate_diamond(depth: usize, width: usize) -> Vec<Event>;

/// Simulation clock for deterministic timing
pub struct SimClock { ... }

/// Network partition simulation
pub struct PartitionedNetwork { ... }
```

### 1.2 Property-Based Testing Setup

Add `proptest` for invariant-based testing:

```toml
# Cargo.toml
[dev-dependencies]
proptest = "1.4"
proptest-derive = "0.4"
```

Define generators for core types:

```rust
// Arbitrary implementations for:
// - EntityId, EventId, Clock
// - Operation, OperationSet
// - AnkQL predicates
// - Network message sequences
```

### 1.3 Deterministic Simulation Layer

For distributed scenarios, build on `turmoil` or similar:

```rust
// Deterministic network simulation
pub struct SimulatedCluster {
    nodes: Vec<SimNode>,
    network: SimNetwork,
    clock: SimClock,
    rng: StdRng,  // Seeded for reproducibility
}

impl SimulatedCluster {
    pub fn partition(&mut self, groups: &[&[NodeId]]);
    pub fn heal_partition(&mut self);
    pub fn delay_messages(&mut self, node: NodeId, delay: Duration);
    pub fn drop_messages(&mut self, node: NodeId, rate: f64);
    pub fn advance_time(&mut self, duration: Duration);
}
```

## Phase 2: DAG & Lineage Testing

### 2.1 Pathological Structure Generation

Tests for `lineage.rs` comparison logic:

| Test | Structure | Expected |
|------|-----------|----------|
| `deep_chain_traversal` | 10K linear events | Completes within budget or returns `BudgetExceeded` |
| `wide_fork_comparison` | 100 concurrent heads | Correct `Ordering` for each pair |
| `diamond_merge` | Fork + rejoin | Single head after merge |
| `interleaved_chains` | Two chains with shared ancestors | Correct meet detection |
| `degenerate_single_event` | Entity with one event | All comparisons correct |

### 2.2 Budget Exhaustion

Verify graceful handling when lineage traversal exceeds limits:

```rust
#[test]
fn budget_exceeded_returns_resumable_state() {
    // Create lineage that exceeds default 1000-iteration budget
    // Verify BudgetExceeded contains frontiers for resumption
    // Verify no panic, no infinite loop
}

#[test]
fn budget_exceeded_concurrent_transactions() {
    // Reproduce the concurrent_transactions_long_lineage scenario
    // Verify second commit gets appropriate error, not corruption
}
```

### 2.3 Clock Invariants (Property-Based)

```rust
proptest! {
    #[test]
    fn clock_comparison_is_antisymmetric(a: Clock, b: Clock) {
        let ab = compare(&a, &b);
        let ba = compare(&b, &a);
        // If a < b, then b > a (and vice versa)
        assert!(is_inverse(ab, ba));
    }

    #[test]
    fn clock_comparison_is_transitive(a: Clock, b: Clock, c: Clock) {
        // If a ≤ b and b ≤ c, then a ≤ c
    }
}
```

## Phase 3: CRDT Convergence Testing

### 3.1 Yrs/Text Concurrent Operations

Test that concurrent text edits converge:

```rust
#[test]
fn yrs_concurrent_insert_same_position() {
    // Two nodes insert at position 0 concurrently
    // After sync, both have identical text (order determined by CRDT rules)
}

#[test]
fn yrs_interleaved_typing_simulation() {
    // Simulate two users typing simultaneously
    // Random delays, random positions
    // Final state identical on both nodes
}

#[test]
fn yrs_delete_during_insert() {
    // Node A inserts, Node B deletes same range concurrently
    // Verify convergence without crash
}
```

### 3.2 LWW Conflict Resolution

```rust
#[test]
fn lww_identical_timestamp_tiebreak() {
    // Two writes with identical timestamps
    // Verify deterministic winner (lexicographic EventId)
}

#[test]
fn lww_rapid_updates() {
    // 1000 updates in quick succession from multiple nodes
    // All converge to same final value
}
```

### 3.3 PNCounter Edge Cases

```rust
#[test]
fn pncounter_overflow_handling() {
    // Increment to u64::MAX, then increment again
    // Verify defined behavior (saturate? wrap? error?)
}

#[test]
fn pncounter_negative_total() {
    // More decrements than increments
    // Verify counter can go negative (or defined minimum)
}
```

### 3.4 Convergence Property Tests

```rust
proptest! {
    #[test]
    fn crdt_operations_commute(ops: Vec<Operation>) {
        // Apply operations in different orders
        // Final state must be identical
    }

    #[test]
    fn crdt_idempotent(op: Operation) {
        // Applying same operation twice = applying once
    }
}
```

## Phase 4: Transaction Safety Testing

### 4.1 Phantom Entity Tests (Extending PR 235)

```rust
#[test]
fn phantom_with_valid_looking_lineage() {
    // Create phantom with parent clock pointing to real events
    // Verify rejection (parent clock valid, but entity not created in trx)
}

#[test]
fn phantom_via_deserialized_entity() {
    // Deserialize entity from bytes without going through trx.create()
    // Attempt commit, verify rejection
}
```

### 4.2 TOCTOU Race Conditions

```rust
#[test]
fn entity_modified_between_read_and_commit() {
    // Thread 1: trx.get(id), prepare mutation
    // Thread 2: modify same entity, commit
    // Thread 1: commit
    // Verify: Thread 1 gets conflict error or merges correctly
}

#[test]
fn entity_deleted_during_transaction() {
    // Start transaction, get entity
    // Another transaction deletes it
    // First transaction commits
    // Verify defined behavior
}
```

### 4.3 Transaction Lifecycle Violations

```rust
#[test]
fn use_entity_after_rollback() {
    let trx = node.begin();
    let entity = trx.create::<Album>();
    trx.rollback();

    // Attempt to read/write entity after rollback
    // Verify appropriate error, no crash
}

#[test]
fn commit_same_transaction_twice() {
    let trx = node.begin();
    trx.create::<Album>();
    trx.commit().await?;

    // Second commit attempt
    // Verify error, not double-write
}

#[test]
fn nested_transaction_semantics() {
    // If supported: verify isolation
    // If not supported: verify clear error
}
```

## Phase 5: Storage Consistency Testing

### 5.1 Crash Injection Framework

```rust
pub struct CrashPoints {
    pub before_add_event: bool,
    pub after_add_event_before_set_state: bool,
    pub after_set_state: bool,
    pub during_batch_commit: bool,
}

pub struct CrashableStorage<S: StorageEngine> {
    inner: S,
    crash_points: CrashPoints,
    crash_counter: AtomicU64,
}

impl CrashableStorage {
    pub fn crash_after_n_operations(&self, n: u64);
    pub fn simulate_crash(&self) -> !;  // longjmp or panic
}
```

### 5.2 Crash Recovery Tests

```rust
#[test]
fn crash_after_add_event_before_set_state() {
    // Event persisted but state not updated
    // Verify: recovery reconstructs correct state from events
}

#[test]
fn crash_during_batch_commit() {
    // Multiple events in single commit, crash mid-batch
    // Verify: either all persisted or none (atomicity)
}

#[test]
fn crash_loop_convergence() {
    // Crash at every possible point in a complex operation
    // After each crash, recover and continue
    // Final state must equal non-crashed execution
}
```

### 5.3 Backend Parity Tests

```rust
#[test]
fn operations_identical_across_backends() {
    let operations = generate_operation_sequence();

    let sled_result = execute_on_sled(&operations);
    let sqlite_result = execute_on_sqlite(&operations);
    let postgres_result = execute_on_postgres(&operations);

    assert_eq!(sled_result, sqlite_result);
    assert_eq!(sqlite_result, postgres_result);
}
```

## Phase 6: Network Adversary Testing

### 6.1 Byzantine Peer Simulation

```rust
pub struct ByzantineNode {
    behavior: ByzantineBehavior,
}

pub enum ByzantineBehavior {
    ForgeEvents,           // Send events with wrong signatures
    ReplayOldEvents,       // Re-send already-seen events
    OmitEvents,            // Withhold specific events
    ReorderMessages,       // Send out of causal order
    CorruptPayloads,       // Flip bits in event data
    WrongCollection,       // Events with mismatched collection_id
}
```

### 6.2 Network Partition Tests (Jepsen-style)

```rust
#[test]
fn partition_then_heal_convergence() {
    let cluster = SimulatedCluster::new(5);

    // Write to all nodes
    // Partition into [A, B] and [C, D, E]
    // Write to both partitions
    // Heal partition
    // Verify: all nodes converge to same state
}

#[test]
fn asymmetric_partition() {
    // A can send to B, but B cannot send to A
    // Verify: no deadlock, eventual consistency after heal
}

#[test]
fn rolling_partitions() {
    // Rapidly change partition membership
    // Verify: no lost updates, eventual convergence
}
```

### 6.3 Message Timing Attacks

```rust
#[test]
fn delayed_parent_events() {
    // Receive child event before parent
    // Verify: buffered and applied in correct order
}

#[test]
fn duplicate_message_idempotence() {
    // Send same message 100 times
    // Verify: applied exactly once
}
```

## Phase 7: Subscription Chaos Testing

### 7.1 Rapid State Changes

```rust
#[test]
fn subscribe_unsubscribe_loop() {
    for _ in 0..1000 {
        let sub = query.subscribe();
        // Make some changes
        drop(sub);
    }
    // Verify: no leaked watchers, no missed updates
}

#[test]
fn predicate_change_during_notification() {
    // Change selection while notification is in flight
    // Verify: consistent state, no panic
}
```

### 7.2 High-Cardinality Subscriptions

```rust
#[test]
fn subscription_10k_entities() {
    // Create 10K entities matching predicate
    // Subscribe
    // Verify: all received, memory bounded
}

#[test]
fn subscription_with_rapid_entity_churn() {
    // Entities rapidly entering/leaving result set
    // Verify: correct add/remove notifications
}
```

## Phase 8: Resource Exhaustion Testing

### 8.1 Memory Pressure

```rust
#[test]
#[should_panic(expected = "allocation")]  // Or graceful error
fn large_event_payload() {
    // Attempt to create event with 1GB payload
    // Verify: rejected or OOM is catchable
}

#[test]
fn many_concurrent_transactions() {
    // 10K simultaneous transactions
    // Verify: completes (maybe slowly) or returns resource error
}
```

### 8.2 Lineage Budget Stress

```rust
#[test]
fn deep_history_queries() {
    // Query entity with 100K event history
    // Verify: budget limits prevent runaway
}
```

## Phase 9: Input Validation Testing

### 9.1 Malformed Inputs

```rust
#[test]
fn invalid_entity_id_format() {
    let bad_ids = vec![
        "",
        "not-a-uuid",
        "00000000-0000-0000-0000-000000000000",  // Nil UUID
        &"a".repeat(1000),  // Oversized
    ];

    for id in bad_ids {
        // Attempt to use, verify rejection at boundary
    }
}

#[test]
fn malformed_crdt_operations() {
    // Random bytes as operation payload
    // Verify: rejected, not crash
}
```

### 9.2 AnkQL Injection

```rust
#[test]
fn ankql_injection_attempts() {
    let injections = vec![
        "name = 'foo' OR 1=1",
        "name = 'foo'; DROP TABLE entities",
        "name = '\\x00\\x00\\x00'",
    ];

    for query in injections {
        // Verify: parse error or safe execution
    }
}
```

## CI/CD Integration

### Test Organization

```
tests/
├── adversarial/
│   ├── mod.rs
│   ├── dag_tests.rs
│   ├── crdt_tests.rs
│   ├── transaction_tests.rs
│   ├── storage_tests.rs
│   ├── network_tests.rs
│   ├── subscription_tests.rs
│   ├── resource_tests.rs
│   └── validation_tests.rs
├── property/
│   ├── mod.rs
│   ├── clock_properties.rs
│   ├── crdt_properties.rs
│   └── lineage_properties.rs
└── simulation/
    ├── mod.rs
    ├── cluster.rs
    ├── partition.rs
    └── scenarios.rs
```

### CI Pipeline

```yaml
adversarial-tests:
  runs-on: ubuntu-latest
  steps:
    - name: Quick adversarial tests
      run: cargo test --features test-helpers adversarial -- --test-threads=4

    - name: Property tests (limited iterations for CI)
      run: PROPTEST_CASES=100 cargo test property

    - name: Simulation tests
      run: cargo test simulation --release

nightly-adversarial:
  schedule: "0 3 * * *"  # 3 AM daily
  steps:
    - name: Extended property tests
      run: PROPTEST_CASES=10000 cargo test property

    - name: Long-running simulation
      run: cargo test simulation --release -- --ignored
```

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Tests too slow for CI | Tiered approach: fast subset in PR, full suite nightly |
| Flaky tests | Seeded randomness, deterministic simulation |
| False confidence | Complement with fuzzing, mutation testing |
| Maintenance burden | Tests document themselves; property tests cover many cases |

## Success Criteria

Phase complete when:

1. All categories have representative tests
2. Tests integrated into CI (blocking on PR for critical tests)
3. At least one bug found and fixed by new tests
4. Test documentation explains failure modes covered
