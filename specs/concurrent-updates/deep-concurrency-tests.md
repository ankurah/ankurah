# Deep Concurrency Test Scenarios

**Purpose:** Comprehensive test scenarios for concurrent update handling with both LWW and Yrs backends.

---

## 1. Multi-Head Extension Tests

### 1.1 Event Extends One Tip of Multi-Head

```
Setup:
    A (genesis)
   / \
  B   C   ← entity head is [B, C]
      |
      D   ← incoming event with parent [C]

Test:
- compare_unstored_event(D, [B, C]) should return DivergedSince
- After apply: head should be [B, D]
- D's operations should be applied
```

**LWW Backend:**
```rust
#[tokio::test]
async fn test_lww_multihead_extend_one_tip() {
    // Create entity with LWW backend at head [B, C]
    // Event D with parent [C] sets property X=1
    // Apply D
    // Verify: head is [B, D], property X is 1
}
```

**Yrs Backend:**
```rust
#[tokio::test]
async fn test_yrs_multihead_extend_one_tip() {
    // Create entity with Yrs backend at head [B, C]
    // Event D with parent [C] modifies text
    // Apply D
    // Verify: head is [B, D], text modification applied
}
```

### 1.2 Event Extends Multiple Tips (Merge)

```
Setup:
    A (genesis)
   / \
  B   C   ← entity head is [B, C]
   \ /
    D   ← incoming event with parent [B, C]

Test:
- compare_unstored_event(D, [B, C]) should return StrictDescends
- After apply: head should be [D]
- D's operations should be applied
```

### 1.3 Event Creates Additional Concurrency

```
Setup:
    A
   / \
  B   C   ← entity head is [B, C]
 / \
D   E   ← event E with parent [B] arrives

Test:
- compare_unstored_event(E, [B, C]) should return DivergedSince
- After apply: head should be [E, C]
```

---

## 2. Deep Diamond Concurrency

### 2.1 Symmetric Deep Diamond (LWW)

```
          A (genesis)
         / \
        B   C
        |   |
        D   E
        |   |
        F   G
        |   |
        H   I

Entity processes: A → B → D → F → H (head [H])
Then receives: A → C → E → G → I (full concurrent branch)

Expected:
- DivergedSince { meet: [A], subject_chain: [C,E,G,I], other_chain: [B,D,F,H] }

LWW Resolution for property X:
- If branch 1 sets X at depth 2 (event D)
- If branch 2 sets X at depth 3 (event G)
- Winner: branch 2 (G) because depth 3 > depth 2
```

```rust
#[tokio::test]
async fn test_lww_deep_diamond_symmetric() {
    // Build both branches
    // Entity at [H]
    // Apply events from branch 2

    // Verify:
    // - Relation is DivergedSince with meet [A]
    // - Forward chains correct
    // - Property X has value from deeper event
}
```

### 2.2 Asymmetric Deep Diamond (LWW)

```
          A (genesis)
         / \
        B   C
        |   |
        D   E
        |
        F
        |
        G
        |
        H

Entity processes: A → B → D → F → G → H (head [H])
Then receives: A → C → E (short concurrent branch)

Expected:
- DivergedSince { meet: [A], subject_chain: [C,E], other_chain: [B,D,F,G,H] }

LWW Resolution:
- Short branch has max depth 2
- Long branch has max depth 5
- Long branch generally wins unless short branch sets properties that long branch never touches
```

```rust
#[tokio::test]
async fn test_lww_deep_diamond_asymmetric() {
    // Short branch sets property X at depth 2
    // Long branch sets property Y at depth 4, never touches X
    // Verify: X has short branch value, Y has long branch value
}
```

### 2.3 Deep Diamond (Yrs)

```rust
#[tokio::test]
async fn test_yrs_deep_diamond() {
    // Same topology as LWW tests
    // Yrs should apply all operations from concurrent branch
    // CRDT handles the merge internally
    // Final state should converge regardless of application order
}
```

---

## 3. Short Branch from Deep Point

### 3.1 Short Branch from Linear History (LWW)

```
A → B → C → D → E → F → G → H (entity head [H])
            |
            X → Y (arrives late, parent [D])

Expected:
- DivergedSince { meet: [D], subject_chain: [X,Y], other_chain: [E,F,G,H] }
- Meet is D, NOT genesis A

LWW Resolution:
- Short branch: max depth 2 from D
- Main branch: max depth 4 from D
- Main branch wins for shared properties
```

```rust
#[tokio::test]
async fn test_lww_short_branch_from_deep_point() {
    // Build linear history A→B→C→D→E→F→G→H
    // Create short branch D→X→Y
    // X sets property P at depth 1
    // F sets property P at depth 2
    // Verify: P has F's value (depth 2 > depth 1)
}

#[tokio::test]
async fn test_lww_short_branch_unique_property() {
    // Same setup
    // Y sets property Q (not touched by main branch)
    // Verify: Q has Y's value (only setter)
}
```

### 3.2 Short Branch (Yrs)

```rust
#[tokio::test]
async fn test_yrs_short_branch_from_deep_point() {
    // Same topology
    // Verify CRDT operations from short branch are applied
    // Text modifications should merge correctly
}
```

---

## 4. Per-Property LWW Resolution

### 4.1 Different Properties, Different Winners

```
Branch 1: A → B(X=1) → C(Y=1) → D
Branch 2: A → E(X=2) → F(X=3) → G(Y=2) → H

Property X:
- Branch 1: set at depth 1 (B)
- Branch 2: last set at depth 2 (F)
- Winner: Branch 2, X=3

Property Y:
- Branch 1: set at depth 2 (C)
- Branch 2: set at depth 3 (G)
- Winner: Branch 2, Y=2

Final state: X=3, Y=2
```

```rust
#[tokio::test]
async fn test_lww_different_properties_different_winners() {
    // Build both branches with property modifications
    // Apply concurrent branch
    // Verify each property has correct winner's value
}
```

### 4.2 Same Depth, Lexicographic Tiebreak

```
Branch 1: A → B(X=1) → C
Branch 2: A → D(X=2) → E

Both set X at depth 1.
Winner: lexicographically greater event ID
```

```rust
#[tokio::test]
async fn test_lww_same_depth_lexicographic_tiebreak() {
    // Create two branches with same depth property modification
    // Verify winner is determined by event ID comparison
    // Test is deterministic - same result regardless of application order
}
```

### 4.3 Multiple Properties, Complex Resolution

```
Branch 1: A → B(X=1, Y=1) → C(Z=1) → D(X=2)
Branch 2: A → E(Y=2) → F(X=3, Z=2) → G

Property X:
- Branch 1: depths 1 and 3, last at depth 3 (D, X=2)
- Branch 2: depth 2 (F, X=3)
- Winner: Branch 1, X=2 (depth 3 > depth 2)

Property Y:
- Branch 1: depth 1 (B, Y=1)
- Branch 2: depth 1 (E, Y=2)
- Winner: lexicographic tiebreak

Property Z:
- Branch 1: depth 2 (C, Z=1)
- Branch 2: depth 2 (F, Z=2)
- Winner: lexicographic tiebreak
```

```rust
#[tokio::test]
async fn test_lww_complex_resolution() {
    // Build complex scenario with multiple properties
    // Verify all properties resolve correctly
}
```

---

## 5. Nested Concurrency (Multi-Head + New Concurrency)

### 5.1 Event Creating Three-Way Concurrency

```
            A
           / \
          B   C   ← current head [B, C]
         /
        D   ← event D with parent [B]
            ← D is concurrent with C

After applying D:
- Head should be [D, C]
```

```rust
#[tokio::test]
async fn test_multihead_additional_concurrency() {
    // Start with head [B, C]
    // Apply D with parent [B]
    // Verify head is [D, C]
}
```

### 5.2 Event Collapsing Multi-Head

```
            A
           / \
          B   C   ← current head [B, C]
           \ /
            D   ← event D with parent [B, C]

After applying D:
- Head should be [D] (single head, concurrency resolved)
```

```rust
#[tokio::test]
async fn test_multihead_collapse() {
    // Start with head [B, C]
    // Apply D with parent [B, C]
    // Verify head is [D]
}
```

---

## 6. Idempotency Tests

### 6.1 Exact Duplicate Delivery

```rust
#[tokio::test]
async fn test_idempotency_exact_duplicate() {
    // Apply event E once
    // Verify head updated
    // Apply event E again (same event)
    // Verify: returns Equal, head unchanged, no double-apply
}
```

### 6.2 Event Already in History

```rust
#[tokio::test]
async fn test_idempotency_event_in_history() {
    // Apply A → B → C, head is [C]
    // Try to apply B again
    // Verify: returns StrictAscends (or appropriate no-op)
    // Head unchanged, no error
}
```

---

## 7. Yrs-Specific Tests

### 7.1 Yrs Text Concurrent Edits

```rust
#[tokio::test]
async fn test_yrs_text_concurrent_edits() {
    // Branch 1: insert "hello" at position 0
    // Branch 2: insert "world" at position 0
    // Apply both
    // Verify: CRDT merge produces consistent result
    // Could be "helloworld" or "worldhello" but deterministic
}
```

### 7.2 Yrs Array Concurrent Modifications

```rust
#[tokio::test]
async fn test_yrs_array_concurrent_push() {
    // Branch 1: push element A
    // Branch 2: push element B
    // Apply both
    // Verify: array contains both elements in deterministic order
}
```

### 7.3 Yrs Map Concurrent Updates

```rust
#[tokio::test]
async fn test_yrs_map_concurrent_updates() {
    // Branch 1: set key "x" to value 1
    // Branch 2: set key "x" to value 2
    // Apply both
    // Verify: Yrs LWW map semantics determine winner
}
```

---

## 8. Edge Cases

### 8.1 Empty Parent (Genesis) Concurrency

```rust
#[tokio::test]
async fn test_concurrent_genesis_events() {
    // Two genesis events (empty parent) for same entity
    // This violates single-root invariant
    // Should return Disjoint
}
```

### 8.2 Very Deep History with Budget

```rust
#[tokio::test]
async fn test_deep_history_budget_exceeded() {
    // Create very deep linear history (depth > budget)
    // Try concurrent event at genesis
    // Verify: BudgetExceeded with correct frontiers
}
```

### 8.3 Single Event Entity

```rust
#[tokio::test]
async fn test_single_event_entity_concurrency() {
    // Entity with just genesis event A
    // Two concurrent events B and C both with parent [A]
    // Apply B, then C
    // Verify: head becomes [B, C]
}
```

---

## Test Implementation Notes

1. **LWW tests** use `LWWBackend` and verify property values
2. **Yrs tests** use `YrsBackend` and verify CRDT state
3. **All tests should be deterministic** - same inputs → same outputs
4. **Tests should verify both:**
   - The causal relation returned
   - The final entity state after apply
5. **Use MockEventStore for comparison tests**
6. **Use real Entity + backends for integration tests**
