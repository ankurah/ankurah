# Adversarial Testing Suite: Tasks

## Project 1: Foundation & Test Harness

### 1.1 Core Infrastructure

- [ ] Add `proptest` and `proptest-derive` to dev-dependencies
- [ ] Create `tests/adversarial/mod.rs` with feature gate
- [ ] Extend `test-helpers` feature with adversarial primitives in `core/src/tests/adversarial.rs`:
  - `forge_lineage_entity()` - entity with fake parent clock
  - `forge_event_wrong_entity()` - event with mismatched entity_id
  - `CorruptibleStorage` wrapper for crash injection
- [ ] Add `Arbitrary` implementations for core types:
  - `EntityId`, `EventId`, `Clock`
  - `Operation`, `OperationSet`
  - `AnkQL` predicates (via string generation + parse)

### 1.2 Simulation Infrastructure

- [ ] Evaluate `turmoil` vs custom simulation for deterministic network tests
- [ ] Implement `SimulatedCluster` with:
  - Seeded RNG for reproducibility
  - Message delay/drop injection
  - Partition simulation
  - Deterministic time advancement
- [ ] Implement `SimClock` for controlled time progression in tests

---

## Project 2: DAG & Lineage Tests

### 2.1 Pathological Structure Tests

- [ ] `test_deep_chain_10k_events` - Linear chain, verify comparison completes
- [ ] `test_wide_fork_100_heads` - 100 concurrent events from same parent
- [ ] `test_diamond_merge_convergence` - Fork then rejoin, verify single head
- [ ] `test_interleaved_chains` - Two chains with shared ancestors
- [ ] `test_degenerate_single_event` - Entity with exactly one event

### 2.2 Budget Exhaustion Tests

- [ ] `test_budget_exceeded_returns_frontiers` - Verify resumable state
- [ ] `test_budget_exceeded_no_panic` - Deep lineage doesn't crash
- [ ] `test_budget_configurable` - Custom budget limits work
- [ ] `test_incremental_traversal` - Resume from BudgetExceeded frontiers

### 2.3 Clock Property Tests

- [ ] Property: `clock_comparison_antisymmetric`
- [ ] Property: `clock_comparison_transitive`
- [ ] Property: `clock_merge_commutative`
- [ ] Property: `clock_merge_idempotent`

### 2.4 Lineage Comparison Edge Cases

- [ ] `test_compare_empty_clocks` - Both clocks empty
- [ ] `test_compare_one_empty_clock` - One empty, one populated
- [ ] `test_compare_identical_clocks` - Same events in both
- [ ] `test_compare_disjoint_histories` - No shared ancestors

---

## Project 3: CRDT Convergence Tests

### 3.1 YrsString/Text Tests

- [ ] `test_yrs_concurrent_insert_position_zero` - Both insert at start
- [ ] `test_yrs_concurrent_insert_same_offset` - Same character position
- [ ] `test_yrs_interleaved_typing` - Simulated two-user typing session
- [ ] `test_yrs_delete_during_insert` - Concurrent delete and insert
- [ ] `test_yrs_large_document` - 100K character document operations

### 3.2 LWW Tests

- [ ] `test_lww_identical_timestamp` - Same timestamp, verify tiebreak
- [ ] `test_lww_rapid_updates` - 1000 updates in sequence
- [ ] `test_lww_concurrent_from_multiple_nodes` - N nodes write simultaneously

### 3.3 PNCounter Tests

- [ ] `test_pncounter_overflow` - Increment past u64::MAX
- [ ] `test_pncounter_underflow` - Decrement past minimum
- [ ] `test_pncounter_negative_total` - More decrements than increments
- [ ] `test_pncounter_concurrent_inc_dec` - Simultaneous +1 and -1

### 3.4 CRDT Property Tests

- [ ] Property: `crdt_operations_commutative` - Order doesn't matter
- [ ] Property: `crdt_operations_idempotent` - Double-apply = single-apply
- [ ] Property: `crdt_convergence` - All replicas reach same state

---

## Project 4: Transaction Safety Tests

### 4.1 Phantom Entity Tests (Building on PR 235)

- [ ] `test_phantom_with_real_parent_clock` - Valid-looking but invalid
- [ ] `test_phantom_from_deserialized_bytes` - Bypass trx.create()
- [ ] `test_phantom_remote_rejection` - Server rejects phantom from peer
- [ ] `test_phantom_after_entity_delete` - Reference to deleted entity

### 4.2 TOCTOU Race Tests

- [ ] `test_entity_modified_during_transaction` - Concurrent modification
- [ ] `test_entity_deleted_during_transaction` - Entity removed mid-trx
- [ ] `test_head_changed_before_commit` - Verify recomputation or error

### 4.3 Transaction Lifecycle Tests

- [ ] `test_use_after_rollback` - Access entity post-rollback
- [ ] `test_double_commit` - Commit same transaction twice
- [ ] `test_commit_after_rollback` - Commit already-rolled-back
- [ ] `test_entity_escapes_transaction` - Entity used outside trx scope
- [ ] `test_concurrent_transactions_same_entity` - Isolation verification

### 4.4 Creation/Duplication Tests

- [ ] `test_double_create_same_id` - Two creates for same EntityId
- [ ] `test_create_with_existing_id` - Create where entity already exists
- [ ] `test_remote_create_for_existing` - Peer sends create for known entity

---

## Project 5: Storage Consistency Tests

### 5.1 Crash Injection Infrastructure

- [ ] Implement `CrashableStorage<S>` wrapper
- [ ] Define crash points enum
- [ ] Implement `crash_after_n_operations()` method
- [ ] Create recovery test harness

### 5.2 Crash Recovery Tests

- [ ] `test_crash_before_add_event` - Crash before event persisted
- [ ] `test_crash_after_event_before_state` - Event saved, state not
- [ ] `test_crash_during_batch` - Mid-batch failure
- [ ] `test_crash_loop` - Crash at every point, verify convergence

### 5.3 Backend Parity Tests

- [ ] `test_sled_sqlite_parity` - Same ops, same results
- [ ] `test_sqlite_postgres_parity` - Same ops, same results
- [ ] `test_all_backends_identical` - Comprehensive parity check

### 5.4 Corruption Detection Tests

- [ ] `test_detect_corrupted_event` - Hash mismatch detection
- [ ] `test_detect_corrupted_state` - Invalid state data
- [ ] `test_recovery_from_corruption` - Graceful handling

---

## Project 6: Network Adversary Tests

### 6.1 Byzantine Node Simulation

- [ ] Implement `ByzantineNode` with configurable behaviors
- [ ] `test_forged_event_rejected` - Wrong signature
- [ ] `test_replay_attack_idempotent` - Old event re-sent
- [ ] `test_event_wrong_collection` - Mismatched collection_id
- [ ] `test_corrupted_payload_rejected` - Bit-flipped data

### 6.2 Partition Tests

- [ ] `test_simple_partition_heal` - Split, write, heal, converge
- [ ] `test_asymmetric_partition` - One-way communication
- [ ] `test_rolling_partitions` - Rapidly changing membership
- [ ] `test_minority_partition_writes` - Writes in smaller partition

### 6.3 Message Ordering Tests

- [ ] `test_child_before_parent` - Out-of-order delivery
- [ ] `test_duplicate_messages` - Same message N times
- [ ] `test_delayed_messages` - Variable latency
- [ ] `test_reordered_message_batch` - Shuffled message sequence

---

## Project 7: Subscription Chaos Tests

### 7.1 Rapid Toggle Tests

- [ ] `test_subscribe_unsubscribe_1000x` - Rapid sub/unsub loop
- [ ] `test_predicate_change_storm` - Rapidly changing selection
- [ ] `test_drop_during_init` - Unsubscribe during initialization

### 7.2 High-Cardinality Tests

- [ ] `test_subscription_10k_initial` - Large initial result set
- [ ] `test_subscription_high_churn` - Rapid enter/leave result set
- [ ] `test_subscription_memory_bounded` - Verify no unbounded growth

### 7.3 Notification Ordering Tests

- [ ] `test_notification_ordering` - Events delivered in causal order
- [ ] `test_no_duplicate_notifications` - Each change notified once
- [ ] `test_resubscribe_state_consistency` - State after drop + reacquire

---

## Project 8: Resource Exhaustion Tests

### 8.1 Memory Pressure Tests

- [ ] `test_large_event_rejected` - Oversized payload handling
- [ ] `test_many_concurrent_transactions` - 10K simultaneous
- [ ] `test_deep_history_memory` - 100K events per entity

### 8.2 Budget/Limit Tests

- [ ] `test_lineage_budget_enforced` - Traversal limits work
- [ ] `test_query_result_limits` - Large result set handling
- [ ] `test_connection_limits` - Many peer connections

---

## Project 9: Input Validation Tests

### 9.1 ID Validation Tests

- [ ] `test_invalid_entity_id_formats` - Bad UUID strings
- [ ] `test_invalid_event_id_formats` - Bad hash strings
- [ ] `test_nil_uuid_handling` - Zero UUID behavior

### 9.2 Payload Validation Tests

- [ ] `test_oversized_payload` - Exceeds limits
- [ ] `test_malformed_operations` - Invalid CRDT bytes
- [ ] `test_empty_operation_set` - No operations in event

### 9.3 Query Validation Tests

- [ ] `test_ankql_injection_attempts` - SQL-style injection
- [ ] `test_ankql_malformed_syntax` - Parse error handling
- [ ] `test_ankql_unicode_edge_cases` - Unusual characters

---

## Project 10: CI Integration

### 10.1 Test Organization

- [ ] Create `tests/adversarial/` directory structure
- [ ] Create `tests/property/` directory structure
- [ ] Create `tests/simulation/` directory structure
- [ ] Add feature flags for test categories

### 10.2 CI Pipeline

- [ ] Add adversarial test job to CI (fast subset)
- [ ] Add nightly extended test job (full suite)
- [ ] Add property test job with configurable iteration count
- [ ] Document test categories and run instructions

---

## Priority Order

**Immediate (High-value, builds on PR 235):**
1. Project 4 (Transaction Safety) - Direct follow-up to PR 235
2. Project 2 (DAG/Lineage) - Known budget exhaustion issues
3. Project 3 (CRDT Convergence) - Core correctness

**Near-term:**
4. Project 5 (Storage) - Crash consistency
5. Project 6 (Network) - Distributed correctness
6. Project 1 (Infrastructure) - Enable everything else

**Later:**
7. Project 7 (Subscriptions)
8. Project 8 (Resources)
9. Project 9 (Validation)
10. Project 10 (CI)

---

## Notes

- Each task should create at least one test that would have caught a real bug
- Tests should be deterministic (seeded RNG where randomness needed)
- Property tests should document the invariant being verified
- Simulation tests should be reproducible via seed
- Link relevant tests to spec sections they verify
