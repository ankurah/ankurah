# Concurrent Updates: Test Design Specification

> **Purpose:** Define comprehensive test scenarios that validate concurrent update handling across the full system - algorithmic correctness, multi-node behavior, determinism guarantees, and proper CRDT semantics.

## Design Philosophy

### Focus: Correctness Verification

These tests exist to **positively verify** that:
1. The event DAG has the correct structure after operations
2. Property values resolve correctly under concurrency
3. All nodes converge to identical state
4. Operations are deterministic regardless of application order

### Human-Readable DAG Verification

Tests should express expected DAG structure clearly. Rather than opaque assertions on raw data, use declarative patterns:

```
Expected DAG after concurrent B and C from A:

      A (genesis)
     / \
    B   C

Expressed as:
  - A: parents=[], is_genesis=true
  - B: parents=[A]
  - C: parents=[A]
  - head=[B, C]
```

The test file should read almost like documentation - someone unfamiliar with the code should understand what DAG topology we expect.

### Synchronization for Deterministic Concurrency

When testing concurrent operations, use explicit synchronization rather than hoping for races:

```
Pattern: Controlled Concurrent Commit

1. T1 = begin(), T2 = begin()           // Both fork from same head
2. T1.edit(X=1), T2.edit(Y=1)           // Both make changes
3. barrier.wait()                        // Ensure both ready
4. spawn(T1.commit()), spawn(T2.commit()) // Commit concurrently
5. join_all()                            // Wait for completion
6. Verify final state
```

This ensures we test true concurrency, not sequential execution that happens to look concurrent.

---

## Current Coverage Analysis

### Well-Covered Areas

**Algorithmic (event_dag/tests.rs):**
- Linear/concurrent history comparisons
- Disjoint/incomparable detection
- Multi-head scenarios (event extends one tip, extends all tips, three-way)
- Forward chain ordering
- LWW resolution (depth precedence, lexicographic tiebreak)
- Idempotency (redundant event delivery)

**Single-Node Transactions (concurrent_transactions.rs):**
- Two concurrent transactions on same entity
- Long lineage before fork

**Inter-Node (inter_node.rs):**
- Basic fetch across nodes
- Subscription propagation

### Coverage Gaps

1. **Multi-node concurrent writes** - No tests where multiple nodes write simultaneously
2. **Determinism verification** - No tests proving same events yield same state regardless of order
3. **Event DAG structure auditing** - Tests verify values but not the actual DAG topology
4. **Yrs CRDT concurrency** - LWW is well-tested, Yrs behavior under concurrency is not
5. **`apply_state` edge cases** - DivergedSince returns Ok(false), untested
6. **State buffer round-trip** - event_id preservation through serialization

---

## Test Scenarios

### Category 1: Single Durable Node - LWW Backend

#### 1.1 Diamond Merge with DAG Verification

```
Scenario:
  Create entity with X=0, Y=0 (genesis event A)
  T1: X=1 (creates event B)
  T2: Y=1 (creates event C)
  Both committed concurrently from same head.

Expected DAG:
      A
     / \
    B   C

Verify:
  - Event count: 3 (A, B, C)
  - A.parents = [] (genesis)
  - B.parents = [A]
  - C.parents = [A]
  - head = [B, C] (2 members)
  - X=1, Y=1 (both changes applied)
```

#### 1.2 Three-Way Concurrent Modification

```
Scenario:
  Genesis A (X=0, Y=0, Z=0)
  T1: X=1, T2: Y=1, T3: Z=1
  All three committed concurrently.

Expected DAG:
        A
      / | \
     B  C  D

Verify:
  - Event count: 4
  - B, C, D all have parents=[A]
  - head = [B, C, D] (3 members)
  - X=1, Y=1, Z=1
```

#### 1.3 Same-Property Conflict - Depth Resolution

```
Scenario:
  A (X=0)
  ├─ B (X=1)
  │  └─ C (X=2)    ← depth 2 from A
  └─ D (X=3)       ← depth 1 from A

  Apply in order: A, B, C (head=[C]), then D arrives.

Expected DAG:
      A
     / \
    B   D
    |
    C

Verify:
  - head = [C, D]
  - X=2 (C wins: depth 2 > depth 1)
  - C's value persists despite D arriving later
```

#### 1.4 Same-Property, Same-Depth - Lexicographic Tiebreak

```
Scenario:
  A (X=0)
  ├─ B (X=1)  ← known event_id
  └─ C (X=2)  ← known event_id > B's id lexicographically

  Both at depth 1 from A.

Verify:
  - Winner is C (lexicographically greater event_id)
  - X=2
  - Result identical regardless of B-then-C or C-then-B application order
```

#### 1.5 Deep Diamond - Per-Property Resolution

```
Scenario:
  Branch 1: A → B(X=1) → C(Y=1) → D
  Branch 2: A → E(X=2) → F(X=3) → G(Y=2) → H

  X: Branch 1 sets at depth 1, Branch 2 last sets at depth 2
  Y: Branch 1 sets at depth 2, Branch 2 sets at depth 3

Expected:
  - X=3 (depth 2 > depth 1, from F)
  - Y=2 (depth 3 > depth 2, from G)
  - Different properties have different "winners"
```

#### 1.6 Merge Event Collapses Multi-Head

```
Scenario:
  A (genesis)
  ├─ B
  └─ C
  Then: D with parents=[B, C]

Expected DAG:
      A
     / \
    B   C
     \ /
      D

Verify:
  - head = [D] (single member - concurrency resolved)
  - D.parents = [B, C]
```

---

### Category 2: Single Durable Node - Yrs Backend

Yrs CRDTs handle concurrency internally. Unlike LWW, there's no "winner" - operations merge.

#### 2.1 Concurrent Text Inserts - Same Position

```
Scenario:
  Text field initially: "hello"
  T1: Insert " world" at position 5
  T2: Insert " there" at position 5
  Both committed concurrently.

Verify:
  - Both insertions present in final text
  - Order determined by Yrs internal logic (deterministic)
  - Possible results: "hello world there" or "hello there world"
  - Same result regardless of commit order
```

#### 2.2 Concurrent Text Inserts - Different Positions

```
Scenario:
  Text: "hello world"
  T1: Insert "X" at position 0
  T2: Insert "Y" at position 11

Verify:
  - Result: "Xhello worldY"
  - No conflict - positions don't overlap
```

#### 2.3 Concurrent Text Deletes - Overlapping Range

```
Scenario:
  Text: "hello world"
  T1: Delete positions 0-5 ("hello")
  T2: Delete positions 6-11 ("world")

Verify:
  - Result: " " (only the space remains)
  - Both deletions applied
```

#### 2.4 Concurrent Map Updates - Same Key

```
Scenario:
  Map with key "status"
  T1: Set status="active"
  T2: Set status="inactive"

Verify:
  - Yrs uses internal LWW for maps
  - Winner determined by Yrs vector clock, not our depth
  - Result is deterministic
```

#### 2.5 Concurrent Map Updates - Different Keys

```
Scenario:
  Empty map
  T1: Set key1="value1"
  T2: Set key2="value2"

Verify:
  - Both keys present
  - No conflict
```

#### 2.6 Yrs Convergence Under Reordering

```
Scenario:
  Build sequence of 10 text operations creating concurrent branches.
  Apply in multiple orders to fresh backends.

Verify:
  - All orderings produce identical final text
  - CRDT convergence property holds
```

---

### Category 3: Durable + Single Ephemeral Node

#### 3.1 Ephemeral Writes, Durable Receives

```
Setup:
  Durable D, Ephemeral E connected

Scenario:
  1. Create entity on D
  2. E subscribes
  3. E: T1(X=1), T2(Y=1) committed concurrently
  4. Verify D's state

Verify on D:
  - Both events persisted
  - DAG structure correct
  - head has 2 members
  - X=1, Y=1
```

#### 3.2 Durable Writes, Ephemeral Observes

```
Setup:
  Durable D, Ephemeral E subscribed to entity

Scenario:
  1. D: T1(X=1), T2(X=2) concurrent
  2. E observes final state

Verify:
  - E's local state matches D's state
  - E received correct change notifications
  - No duplicate notifications
```

#### 3.3 Durable vs Ephemeral Concurrent Write

```
Setup:
  Durable D, Ephemeral E, entity exists

Scenario:
  1. D begins T_d, E begins T_e (both fork same head)
  2. D: X=1, E: Y=1
  3. barrier.wait() - both ready
  4. Both commit concurrently

Verify:
  - Final state: X=1, Y=1
  - head = [event_from_D, event_from_E]
  - Both D and E converge to same state
  - Event DAG on D has correct structure
```

#### 3.4 Late-Arriving Branch from Deep History

```
Scenario:
  1. Build 20-event linear history on D
  2. E connects, receives state at event 20
  3. Event branching from event 10 arrives (was delayed)

Verify:
  - DivergedSince detected (meet at event 10)
  - LWW resolution uses depth from event 10
  - No BudgetExceeded (budget=100 > depth=20)
  - E sees correct merged state
```

---

### Category 4: Multiple Ephemeral Nodes

#### 4.1 Two Ephemeral - Independent Writes

```
Setup:
  Durable D, Ephemeral E1, E2 all connected

Scenario:
  1. Create entity on D
  2. E1 subscribes, E2 subscribes
  3. E1: X=1, E2: Y=1 (concurrent, before seeing each other's write)

Verify:
  - D has both events
  - D: X=1, Y=1
  - E1 converges to X=1, Y=1
  - E2 converges to X=1, Y=1
```

#### 4.2 Two Ephemeral - Same Property Conflict

```
Setup:
  Durable D, Ephemeral E1, E2

Scenario:
  1. Entity with X=0 on D
  2. E1: X=1, E2: X=2 (racing)

Verify:
  - Winner is deterministic (depth then lexicographic)
  - D, E1, E2 all converge to same X value
  - Losing value not visible after convergence
```

#### 4.3 Three Ephemeral - True Three-Way Race

```
Setup:
  Durable D, Ephemeral E1, E2, E3

Scenario:
  1. Entity on D
  2. E1: X=1, E2: Y=1, E3: Z=1 (all concurrent)

Verify:
  - head may have up to 4 members
  - X=1, Y=1, Z=1
  - All four nodes converge to identical state
  - Event DAG has correct structure
```

---

### Category 5: Determinism Verification

These tests apply identical events in different orders and verify identical results.

#### 5.1 Two-Event Determinism

```
Events:
  A: genesis
  B: parent=[A], sets X=1
  C: parent=[A], sets X=2

Test orderings:
  Order 1: Apply A, B, C
  Order 2: Apply A, C, B

Verify:
  - state_order1 == state_order2
  - Same X value (lexicographic tiebreak since same depth)
  - Same head structure
```

#### 5.2 Deep Diamond Determinism

```
Events:
  Branch 1: A → B → C → D → E (E sets X=10)
  Branch 2: A → F → G → H → I (H sets X=20)

Test orderings:
  Order 1: Full branch 1, then full branch 2
  Order 2: Full branch 2, then full branch 1
  Order 3: Interleaved (B, F, C, G, D, H, E, I)

Verify:
  - All orderings produce identical X value
  - Winner determined by depth from A, not application order
```

#### 5.3 Multi-Property Determinism

```
Events:
  Branch 1: A → B(X=1, depth=1) → C(Y=1, depth=2)
  Branch 2: A → D(X=2, depth=1) → E(Y=2, depth=2) → F

Test multiple orderings.

Verify:
  - X winner: lexicographic tiebreak (both depth 1)
  - Y winner: lexicographic tiebreak (both depth 2)
  - Results identical across all orderings
```

#### 5.4 Yrs Determinism

```
Events:
  Text operations creating concurrent edits.

Test multiple orderings.

Verify:
  - CRDT convergence: all orderings produce identical text
```

---

### Category 6: Event DAG Structure Auditing

Tests that verify the actual graph topology, not just property values.

#### 6.1 Linear History Structure

```
After: A → B → C → D

Verify DAG:
  A: parents=[], genesis=true
  B: parents=[A]
  C: parents=[B]
  D: parents=[C]
  head=[D]
  event_count=4
```

#### 6.2 Simple Diamond Structure

```
After concurrent B and C from A:

      A
     / \
    B   C

Verify DAG:
  A: parents=[]
  B: parents=[A]
  C: parents=[A]
  head=[B, C]
  event_count=3
```

#### 6.3 Diamond with Merge Structure

```
After:
      A
     / \
    B   C
     \ /
      D

Verify DAG:
  A: parents=[]
  B: parents=[A]
  C: parents=[A]
  D: parents=[B, C]
  head=[D]
  event_count=4
```

#### 6.4 Complex Multi-Merge Structure

```
After:
        A
       /|\
      B C D
      |X|
      E F    (E merges B+C, F merges C+D)
       \|
        G    (G merges E+F)

Verify DAG:
  A: parents=[]
  B: parents=[A]
  C: parents=[A]
  D: parents=[A]
  E: parents=[B, C]
  F: parents=[C, D]
  G: parents=[E, F]
  head=[G]
  event_count=7
```

#### 6.5 Head Evolution Through Operations

```
Track head after each operation:

  Create A      → head=[A]
  Commit B      → head=[B]
  Commit C||    → head=[B, C]  (concurrent)
  Commit D→C    → head=[B, D]  (D extends C)
  Commit E→B,D  → head=[E]     (E merges B and D)

Verify head at each step matches expected.
```

---

### Category 7: Edge Cases and Error Conditions

#### 7.1 `apply_state` with DivergedSince

```
Scenario:
  Entity at head [B] on node N
  State snapshot arrives with head [C] where B and C diverged from A

Verify:
  - apply_state returns Ok(false)
  - Entity state unchanged (still at B's values)
  - No corruption
  - Warning logged about divergent state
```

#### 7.2 `apply_state` with StrictAscends

```
Scenario:
  Entity at head [C] (descended from B)
  State snapshot arrives with head [B] (older)

Verify:
  - apply_state returns Ok(false)
  - Entity state unchanged
  - No rollback to older state
```

#### 7.3 Empty Clock Handling

```
Scenario:
  Compare empty clock vs empty clock
  Compare empty clock vs non-empty clock

Verify:
  - Graceful handling
  - Returns DivergedSince with empty meet
  - No panics
```

#### 7.4 Single Event Entity

```
Scenario:
  Entity with just genesis event A
  Two concurrent events B, C both parent=[A]

Verify:
  - After B: head=[B]
  - After C (concurrent): head=[B, C]
  - Correct DAG structure
```

#### 7.5 Event with Empty Operations

```
Scenario:
  Event that modifies no properties (empty operation map)

Verify:
  - Event still applied (advances head)
  - No property changes
  - DAG structure correct
```

#### 7.6 State Buffer Round-Trip with Event Tracking

```
Scenario:
  1. Apply event E1 that sets X=1 (event_id tracked)
  2. Serialize state to buffer
  3. Deserialize state from buffer
  4. Apply concurrent event E2 that sets X=2

Verify:
  - After deserialization, event_id tracking preserved
  - Conflict resolution works correctly against E1
  - Depth comparison functional
```

#### 7.7 Rapid Concurrent Transactions

```
Scenario:
  Launch 20 concurrent transactions on same entity.

Verify:
  - All transactions either succeed or fail gracefully
  - No panics
  - Final state consistent
  - TOCTOU retries work (some may exhaust retries)
  - Event DAG is valid (no orphan events, no cycles)
```

#### 7.8 Downcast Failure Handling

```
Scenario:
  (May require test infrastructure to simulate)
  Backend name says "lww" but stored backend type mismatches.

Verify:
  - Graceful error handling
  - No silent corruption
  - Error logged or returned
```

---

### Category 8: Notification Correctness

#### 8.1 One Notification Per Change

```
Scenario:
  Subscribe to entity
  Make concurrent changes B, C

Verify:
  - Exactly two notifications received (one for B, one for C)
  - No duplicates
  - No missed notifications
```

#### 8.2 Causal Notification Order

```
Scenario:
  Sequential changes A→B→C

Verify:
  - Notifications arrive in causal order
  - Never see C notification before B
```

#### 8.3 Multi-Subscriber Consistency

```
Scenario:
  Three subscribers S1, S2, S3 on same entity
  Make changes

Verify:
  - All subscribers see same sequence
  - All converge to same view
```

---

## DAG Verification Patterns

### The `TestDag` + `assert_dag!` Pattern

Tests use a `TestDag` struct to track event labels as they are created, then declarative macros to verify structure:

```rust
// Create TestDag and enumerate events as they're committed
let mut dag = TestDag::new();

// Each commit returns events, which we enumerate to assign labels
let album_id = {
    let trx = context.begin();
    let album = trx.create(&Album { ... }).await?;
    dag.enumerate(trx.commit_and_return_events().await?);  // A = genesis
    album.id()
};

// Concurrent transactions
let trx1 = context.begin();
let trx2 = context.begin();
// ... make changes ...
dag.enumerate(trx1.commit_and_return_events().await?);  // B
dag.enumerate(trx2.commit_and_return_events().await?);  // C

// Fetch persisted events and verify DAG structure
let events = collection.dump_entity_events(entity_id).await?;

// The DAG we expect:
//       A
//      / \
//     B   C

assert_dag!(dag, events, {
    A => [],      // genesis (no parents)
    B => [A],
    C => [A],
});

// Verify head separately
let head = stored_state.payload.state.head;
clock_eq!(dag, head, [B, C]);
```

### How Labels Work

Labels (A, B, C...) are **assigned in creation order** as events are committed:
- Call `dag.enumerate(events)` after each `commit_and_return_events()`
- Labels are assigned A, B, C... in the order events are enumerated
- The `TestDag` maintains a bidirectional map between labels and EventIds
- This provides **stable, predictable labels** that reflect actual creation order

This works because:
1. Tests control commit order, making labels predictable
2. Creation order is unambiguous (unlike topological sort with tie-breaking)
3. Labels match the logical flow of the test code

### Value Assertions

DAG structure, head state, and property values are verified **separately**:

```rust
// 1. Verify DAG structure (parent relationships)
assert_dag!(dag, events, {
    A => [],
    B => [A],
    C => [A],
});

// 2. Verify head state
clock_eq!(dag, head, [B, C]);

// 3. Verify property values
assert_eq!(album.name(), "Updated by B");
assert_eq!(album.year(), "2025");
```

This separation keeps each assertion focused and readable:
- `assert_dag!` verifies parent relationships
- `clock_eq!` verifies the current head/clock state
- Regular `assert_eq!` handles property value verification

### Examples

**Simple Linear History:**
```rust
// A → B → C → D

assert_dag!(dag, events, {
    A => [],
    B => [A],
    C => [B],
    D => [C],
});
clock_eq!(dag, head, [D]);
```

**Diamond (Concurrent then Merge):**
```rust
//       A
//      / \
//     B   C
//      \ /
//       D

assert_dag!(dag, events, {
    A => [],
    B => [A],
    C => [A],
    D => [B, C],
});
clock_eq!(dag, head, [D]);
```

**Multi-Head (Unresolved Concurrency):**
```rust
//       A
//      / \
//     B   C

assert_dag!(dag, events, {
    A => [],
    B => [A],
    C => [A],
});
clock_eq!(dag, head, [B, C]);
```

**Three-Way Concurrency:**
```rust
//        A
//      / | \
//     B  C  D

assert_dag!(dag, events, {
    A => [],
    B => [A],
    C => [A],
    D => [A],
});
clock_eq!(dag, head, [B, C, D]);
```

**Complex Multi-Merge:**
```rust
//         A
//       / | \
//      B  C  D
//      |\ /|
//      E  F     (E merges B+C, F merges C+D)
//       \ |
//        G      (G merges E+F)

assert_dag!(dag, events, {
    A => [],
    B => [A],
    C => [A],
    D => [A],
    E => [B, C],
    F => [C, D],
    G => [E, F],
});
clock_eq!(dag, head, [G]);
```

### Error Messages

When assertions fail, the macro produces readable output:

```
DAG structure mismatch:

D => [B, C] (expected) vs D => [B] (actual)

Registered labels: ['A', 'B', 'C', 'D']
```

### Implementation Notes

The implementation consists of:

1. **`TestDag` struct** (`tests/tests/common.rs`):
   - `new()` - creates empty tracker
   - `enumerate(events: Vec<Event>)` - assigns labels A, B, C... in order
   - `verify(events, expected)` - checks parent relationships
   - `clock_labels(clock)` - converts Clock to sorted label chars

2. **`commit_and_return_events()` method** (`core/src/transaction.rs`):
   - Returns `Vec<Event>` for test tracking
   - Companion to `commit()` which returns `()`

3. **Macros** (`tests/tests/common.rs`):
   - `assert_dag!(dag, events, { ... })` - verify parent relationships
   - `clock_eq!(dag, clock, [...])` - verify head/clock state

---

## Cross-Node Consistency Testing

Multi-node tests should verify consistency at different points in the convergence process:

### Post-Convergence Verification

After all events have propagated and nodes have converged:

```rust
// Wait for propagation
tokio::time::sleep(Duration::from_millis(100)).await;

// All nodes should have identical DAG structure
let events_d = collection_d.dump_entity_events(entity_id).await?;
let events_e1 = collection_e1.dump_entity_events(entity_id).await?;
let events_e2 = collection_e2.dump_entity_events(entity_id).await?;

assert_dag!(dag, events_d, {
    A => [],
    B => [A],
    C => [A],
});
clock_eq!(dag, head_d, [B, C]);

// Same structure on all nodes
assert_eq!(events_d.len(), events_e1.len());
assert_eq!(events_d.len(), events_e2.len());

// Same property values on all nodes
assert_eq!(entity_d.x(), entity_e1.x());
assert_eq!(entity_d.x(), entity_e2.x());
```

### Pre/Partial Convergence Verification

Test state during propagation (before full convergence):

```rust
// E1 writes X=1, E2 writes Y=1 (concurrent)
// Before propagation completes:

// E1 may only see its own event (B)
let events_e1 = collection_e1.dump_entity_events(entity_id).await?;
assert_dag!(dag, events_e1, {
    A => [],
    B => [A],        // E1's event only
});
clock_eq!(dag, head_e1, [B]);  // Single head

// After propagation, E1 sees both:
let events_e1 = collection_e1.dump_entity_events(entity_id).await?;
assert_dag!(dag, events_e1, {
    A => [],
    B => [A],
    C => [A],        // Now sees E2's event
});
clock_eq!(dag, head_e1, [B, C]);  // Multi-head
```

### Convergence Helper

```rust
async fn wait_for_convergence(nodes: &[&Node], entity_id: EntityId) {
    loop {
        let heads: Vec<_> = futures::future::join_all(
            nodes.iter().map(|n| n.get_entity_head(entity_id))
        ).await;

        if heads.windows(2).all(|w| w[0] == w[1]) {
            break; // All nodes have same head
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
```

---

## Unit vs Integration Testing

### Unit Tests (event_dag/tests.rs, lww_resolution_tests)

Test algorithmic correctness in isolation:
- Causal comparison logic (`compare`, `compare_unstored_event`)
- LWW resolution rules (depth precedence, lexicographic tiebreak)
- Chain building and ordering

```rust
#[test]
fn test_lww_deeper_event_wins() {
    let backend = LWWBackend::new();
    // ... direct backend manipulation
    backend.apply_operations_with_resolution(...);
    assert_eq!(backend.get(&"x".into()), expected_value);
}
```

### Integration Tests (tests/tests/*.rs)

Test full system behavior with real nodes, transactions, and propagation:
- Multi-node scenarios
- Transaction lifecycle
- Subscription notifications
- DAG structure verification

```rust
#[tokio::test]
async fn test_durable_ephemeral_concurrent_write() {
    let server = Node::new_durable(...);
    let client = Node::new(...);
    // ... full transaction flow
    assert_dag!(events, entity, { ... });
}
```

### Coverage Strategy

| Aspect | Unit Test | Integration Test |
|--------|-----------|------------------|
| Depth comparison logic | ✓ | |
| Lexicographic tiebreak | ✓ | |
| Chain building | ✓ | |
| Multi-node propagation | | ✓ |
| Transaction commit | | ✓ |
| Subscription notifications | | ✓ |
| DAG structure | ✓ (mock) | ✓ (real) |
| Determinism | ✓ | ✓ |

---

## Synchronization Patterns

### Barrier-Based Concurrent Commit

```rust
use tokio::sync::Barrier;

let barrier = Arc::new(Barrier::new(2));

let t1 = {
    let barrier = barrier.clone();
    tokio::spawn(async move {
        let trx = ctx.begin();
        entity.edit(&trx)?.x().replace(1)?;
        barrier.wait().await;  // Sync point
        trx.commit().await
    })
};

let t2 = {
    let barrier = barrier.clone();
    tokio::spawn(async move {
        let trx = ctx.begin();
        entity.edit(&trx)?.y().replace(1)?;
        barrier.wait().await;  // Sync point
        trx.commit().await
    })
};

let (r1, r2) = tokio::join!(t1, t2);
```

### Ordered Multi-Phase Test

```rust
// Phase 1: Both transactions start and make changes
let trx1 = ctx.begin();
let trx2 = ctx.begin();
entity.edit(&trx1)?.x().replace(1)?;
entity.edit(&trx2)?.y().replace(1)?;

// Phase 2: Concurrent commit
let (r1, r2) = tokio::join!(trx1.commit(), trx2.commit());

// Phase 3: Verification
verify_dag_structure(...);
```

---

## Priority Order for Implementation

1. **Category 5 (Determinism)** - Foundational correctness guarantee
2. **Category 6 (DAG Auditing)** - Verify internal consistency
3. **Category 4 (Multi-Ephemeral)** - Real-world scenario, high bug likelihood
4. **Category 2 (Yrs Backend)** - Currently under-tested
5. **Category 3 (Durable+Ephemeral)** - Common deployment
6. **Category 1 (Single Node LWW)** - Foundation, expand existing
7. **Category 7 (Edge Cases)** - Robustness
8. **Category 8 (Notifications)** - User experience

---

## Success Criteria

A complete test suite should:

1. **Verify DAG structure** after every significant operation
2. **Prove determinism** by testing multiple event orderings
3. **Cover all backends** (LWW and Yrs) under concurrency
4. **Test multi-node** scenarios with controlled synchronization
5. **Document behavior** through readable test names and assertions
6. **Catch regressions** - any change to resolution logic should fail tests
