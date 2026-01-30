# Adversarial Testing Suite: Functional Specification

## Motivation

Ankurah is a distributed, event-sourced reactive database handling collaborative/multiplayer workloads. Its correctness guarantees—causal consistency, CRDT convergence, and data integrity—are only meaningful if they hold under adversarial conditions: malformed inputs, pathological DAG structures, Byzantine peers, network partitions, clock skew, crash recovery, and resource exhaustion.

PR 235 exposed that basic invariants (phantom entity rejection, nonexistent entity handling) were untested until bugs surfaced. A systematic adversarial test suite is needed to:

1. **Proactively discover** latent bugs before users encounter them
2. **Establish confidence** in correctness claims under stress
3. **Prevent regressions** as the codebase evolves
4. **Document** system behavior at boundaries and failure modes

## Scope

This specification covers adversarial testing across all Ankurah components:

| Layer | Attack Surface |
|-------|----------------|
| **DAG/Lineage** | Pathological structures, deep histories, wide forks, budget exhaustion |
| **CRDTs** | Merge semantics, concurrent operations, interleaving, convergence |
| **Transactions** | Phantom entities, TOCTOU races, rollback/commit ordering |
| **Storage** | Crash recovery, partial writes, corruption detection, backend parity |
| **Network/Peers** | Malicious messages, Byzantine behavior, partitions, delays |
| **Subscriptions** | Rapid sub/unsub, predicate churn, notification ordering |
| **Policy/Auth** | Attestation bypass, permission escalation, context manipulation |
| **Resources** | Memory pressure, budget limits, unbounded allocations |

## Goals

### Primary Goals

1. **Invariant Validation**: Verify that documented invariants hold under stress
   - Causal ordering is never violated
   - CRDTs converge to identical state across all replicas
   - Transactions are atomic (all-or-nothing)
   - No data loss or silent corruption

2. **Boundary Behavior**: Test behavior at limits
   - Budget exhaustion returns recoverable errors (not panics)
   - Resource limits are enforced
   - Malformed inputs are rejected with appropriate errors

3. **Byzantine Resistance**: Validate behavior with malicious peers
   - Forged events are rejected
   - Invalid attestations are detected
   - Policy enforcement cannot be bypassed

4. **Crash Consistency**: Verify recovery semantics
   - Storage can recover from crashes at any point
   - No orphaned events or dangling references
   - Head clock always reflects persisted state

### Secondary Goals

1. **Performance Baselines**: Establish acceptable degradation under stress
2. **Regression Detection**: Catch correctness issues before release
3. **Documentation**: Tests serve as executable specification of edge cases

## Testing Categories

### Category 1: DAG Integrity (Jepsen-inspired)

Pathological DAG structures that stress lineage comparison:

- **Deep linear chains**: 10K+ events in sequence
- **Wide forks**: 100+ concurrent heads from same parent
- **Diamond merges**: Converging after divergence
- **Degenerate structures**: Single-event entities, empty operations
- **Budget exhaustion**: Lineages exceeding traversal limits

Success criteria: All comparisons terminate with correct `Ordering`, budget limits respected.

### Category 2: CRDT Semantics

Concurrent operations that stress merge logic:

- **Yrs/Text**: Concurrent inserts at same position, interleaved typing simulation
- **LWW**: Simultaneous writes with identical timestamps
- **PNCounter**: Overflow/underflow, negative totals
- **Cross-backend**: Operations mixing backend types

Success criteria: All replicas converge to identical state; convergence is deterministic.

### Category 3: Transaction Safety

Race conditions and invalid state transitions:

- **Phantom entities**: Commit events for non-created entities (PR 235)
- **Double-create**: Multiple creation events for same EntityId
- **TOCTOU**: Entity modified between read and commit
- **Rollback races**: Use entity after transaction death
- **Concurrent commit**: Same entity in multiple simultaneous transactions

Success criteria: Invalid operations rejected with appropriate errors; no partial commits.

### Category 4: Storage Consistency

Crash recovery and backend parity:

- **Crash injection**: Kill process at every storage operation point
- **Partial writes**: Incomplete event/state pairs
- **Backend parity**: Same operations yield identical results across Sled/SQLite/Postgres/IndexedDB
- **Corruption detection**: Detect and report tampered data

Success criteria: Recovery always succeeds; backends behave identically.

### Category 5: Network Adversary (Byzantine)

Malicious peer behavior:

- **Forged events**: Events with wrong entity_id/collection
- **Invalid attestations**: Signatures that don't verify
- **Replay attacks**: Re-sending old events
- **Out-of-order delivery**: Events arriving before their parents
- **Selective omission**: Peer withholds specific events

Success criteria: Malicious messages rejected; honest nodes unaffected.

### Category 6: Subscription Chaos

Subscription system stress:

- **Rapid toggle**: Subscribe/unsubscribe in tight loop
- **Predicate churn**: Change selection mid-stream
- **High cardinality**: Predicates matching 10K+ entities
- **Notification ordering**: Events during initialization
- **Resubscribe consistency**: State after drop and re-acquire

Success criteria: No missed updates; no duplicate notifications; correct final state.

### Category 7: Resource Exhaustion

System behavior under resource pressure:

- **Memory pressure**: Large operations, many concurrent transactions
- **Lineage budget**: Deep histories exceeding traversal limits
- **Connection limits**: Many simultaneous peer connections
- **Queue depth**: Notification backlog

Success criteria: Graceful degradation; recoverable errors; no OOM panics.

### Category 8: Input Validation

Malformed and boundary inputs:

- **Invalid EntityId/EventId**: Non-conforming formats
- **Oversized payloads**: Events exceeding reasonable limits
- **Malformed operations**: Invalid CRDT operation bytes
- **AnkQL injection**: Malicious query strings
- **Unicode edge cases**: Surrogate pairs, zero-width chars

Success criteria: All invalid inputs rejected at system boundary.

## Non-Goals

- Performance benchmarking (separate effort)
- Fuzz testing of dependencies (Yrs, Sled, etc.)
- UI/client-side testing
- Load testing for capacity planning

## Success Metrics

1. **Coverage**: Every documented invariant has adversarial test
2. **Reproducibility**: All tests deterministic (seeded randomness)
3. **CI Integration**: Tests run on every PR
4. **Regression Rate**: Zero correctness regressions after suite deployed

## Dependencies

- `test-helpers` feature (added in PR 235) for `conjure_evil_phantom()` pattern
- Property-based testing framework (proptest or quickcheck)
- Deterministic simulation for network tests (turmoil or similar)
- Crash injection tooling for storage tests

## References

- [Jepsen](https://jepsen.io/) - Distributed systems testing methodology
- [FoundationDB Testing](https://apple.github.io/foundationdb/testing.html) - Deterministic simulation
- [TigerBeetle Testing](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/DESIGN.md) - VOPR/simulation
- PR 235 - Phantom entity bugs that motivated this effort
