# Concurrent Updates: Optimization and Refactoring Plan

## Status

**Current**: Implementation complete and functional. All tests passing.  
**Goal**: Identify and prioritize opportunities for performance, clarity, and maintainability improvements.

---

## Overview

The initial implementation prioritized correctness and getting the semantics right. Now that we have:

- ✅ Working LWW with topological precedence
- ✅ Correct `compare` and `compare_unstored_event` logic
- ✅ Proper `DivergedSince` handling with ForwardView
- ✅ Comprehensive test coverage

We can focus on optimization and refactoring.

---

## 1. Performance Optimizations

### 1.1 ForwardView Construction and Iteration

**Current State**:

- `ForwardView::new` materializes the entire event HashMap
- ReadySets are computed lazily via iterator, but we clone events for each ReadySet
- No memoization of topological sorting results

**Optimization Opportunities**:

1. **Lazy event loading**: Don't fetch all events upfront; stream them as ReadySets are iterated

   - Benefit: Reduced memory footprint for large divergences
   - Challenge: Requires async iterator pattern or callback-based design
   - Priority: **Medium** (worthwhile for large histories)

2. **ReadySet event references**: Use `&Event` instead of cloning into each ReadySet

   - Benefit: Less memory churn, faster iteration
   - Challenge: Lifetime management across ReadySet iterator
   - Priority: **High** (relatively easy win)

3. **Incremental topological sort**: Cache partial ordering for frequently-accessed regions
   - Benefit: Faster ForwardView construction for overlapping divergences
   - Challenge: Cache invalidation strategy
   - Priority: **Low** (complex, marginal benefit)

### 1.2 Comparison Algorithm (`compare.rs`)

**Current State**:

- Allocates `Vec`s for meet candidates, subject/other frontiers
- No memoization of comparison results
- `Comparison` struct holds multiple collections during traversal

**Optimization Opportunities**:

1. **SmallVec for common cases**: Most clocks have 1-3 members

   - Benefit: Avoid heap allocation for small clocks
   - Implementation: `SmallVec<[EventId; 3]>`
   - Priority: **High** (easy, measurable win)

2. **Comparison result cache**: Memoize `compare(a, b)` results

   - Benefit: Repeated comparisons (e.g., in reactor) don't re-traverse
   - Challenge: Cache size/eviction policy, invalidation on new events
   - Priority: **Medium** (depends on access patterns)

3. **Early termination heuristics**: For `StrictAscends`/`StrictDescends`, detect sooner
   - Benefit: Avoid full traversal when relationship is obvious
   - Implementation: Quick checks before full BFS (e.g., head overlap + frontier empty)
   - Priority: **Medium** (moderate complexity)

### 1.3 LWW Transaction Processing

**Current State**:

- `apply_ready_set` clones `base_registers` on first call
- Deserializes operations for every event, every ReadySet
- `pending_winners` may accumulate many entries for sparse updates

**Optimization Opportunities**:

1. **Operation deserialization caching**: Parse operations once, not per ReadySet

   - Benefit: Reduced CPU for deep histories
   - Implementation: Cache parsed ops in `Attested<Event>` or ReadySet
   - Priority: **Medium** (depends on divergence depth)

2. **Incremental register updates**: Don't clone entire `base_registers`

   - Benefit: Reduced allocations
   - Implementation: Use `Arc<BTreeMap>` + copy-on-write for changed keys
   - Priority: **Low** (marginal, adds complexity)

3. **Property-level parallelism**: Resolve independent properties concurrently
   - Benefit: Use multiple cores for wide entities
   - Challenge: Transaction semantics, backend thread-safety
   - Priority: **Low** (niche use case)

---

## 2. Code Organization and Clarity

### 2.1 Module Boundaries

**Current State**:

- `compare.rs` has both `compare` and `compare_unstored_event`
- `entity.rs` has complex `apply_event` with retry logic
- `node_applier.rs` orchestrates delta application

**Refactoring Opportunities**:

1. **Extract `EventApplication` module**: Separate concerns

   - `entity.rs`: State management, backend coordination
   - `event_application.rs`: Apply logic, retry, relation handling
   - Benefit: Clearer separation, easier testing
   - Priority: **Medium**

2. **`Comparison` trait or enum**: Unify `compare` and `compare_unstored_event`

   - Current: Two separate functions with overlapping logic
   - Proposed: Trait for `Comparable` (Clock vs Unstored)
   - Benefit: Reduced duplication, cleaner API
   - Priority: **Low** (works well as-is)

3. **ForwardView builder pattern**: Improve construction ergonomics
   - Current: `ForwardView::new` takes 4 ordered parameters
   - Proposed: Builder with named setters
   - Benefit: Self-documenting, extensible
   - Priority: **Low** (minor QoL)

### 2.2 Error Handling

**Current State**:

- `RetrievalError`, `MutationError`, `StateError` are loosely coordinated
- Some errors wrapped in `anyhow::Error`, losing type info
- Retry logic in `apply_event` uses loop + attempt counter

**Refactoring Opportunities**:

1. **Structured error types**: Introduce `ConcurrentUpdateError` enum

   - Variants: `BudgetExceeded`, `Disjoint`, `BackendFailed`, `StateChanged`
   - Benefit: Typed error handling, better error messages
   - Priority: **High** (improves debugging experience)

2. **Retry policy abstraction**: Extract retry logic from `apply_event`
   - Implementation: `RetryPolicy` trait with backoff strategy
   - Benefit: Testable, configurable retry behavior
   - Priority: **Low** (current approach is simple and works)

### 2.3 Testing Infrastructure

**Current State**:

- Unit tests use helper functions (`event_with_ops`, `test_event_id`)
- Integration tests repeat setup boilerplate
- No property-based testing for comparison algorithm

**Improvements**:

1. **Test fixture library**: Shared helpers for building test DAGs

   - `DagBuilder::new().add_chain(a, b, c).add_fork(a, q).build()`
   - Benefit: Concise test setup, easier to reason about
   - Priority: **Medium** (improves test maintainability)

2. **Property-based testing**: Use `proptest` for comparison invariants

   - Properties: reflexivity, antisymmetry, transitivity (where applicable)
   - Benefit: Catch edge cases, build confidence
   - Priority: **Low** (good for long-term robustness)

3. **Deterministic fuzz testing**: Generate random DAGs, assert convergence
   - Benefit: Stress test under realistic workloads
   - Priority: **Low** (current test coverage is solid)

---

## 3. Algorithmic Improvements

### 3.1 Meet Computation

**Current State**:

- `Comparison::determine_final_ordering` filters `meet_candidates` by `common_child_count == 0`
- No incremental meet updates

**Optimization Opportunities**:

1. **Meet caching**: Store (clock_a, clock_b) → meet in LRU cache

   - Benefit: Amortized O(1) for repeated comparisons
   - Challenge: Cache invalidation on new events
   - Priority: **Medium** (depends on access patterns)

2. **Meet hints**: If previous comparison returned `DivergedSince{meet}`, reuse meet for related comparisons
   - Example: Comparing [a] vs [b], then [a] vs [c] (both descend from same meet)
   - Benefit: Skip meet recomputation
   - Priority: **Low** (niche optimization)

### 3.2 ForwardView Construction

**Current State**:

- Uses Kahn's algorithm for topological sort
- No pruning of irrelevant branches

**Optimization Opportunities**:

1. **Branch pruning**: If a branch doesn't reach any head in the other set, exclude it

   - Benefit: Smaller ForwardView for complex DAGs with multiple roots
   - Challenge: Correct detection of irrelevant branches
   - Priority: **Low** (rare case)

2. **Incremental ForwardView**: Extend existing ForwardView with new events
   - Use case: `BudgetExceeded` → resume with more budget
   - Benefit: Avoid re-traversing already-sorted events
   - Priority: **Medium** (useful for large histories)

---

## 4. API and Usability

### 4.1 Developer Experience

**Current State**:

- `compare` returns `RelationAndChain` with optional `forward_view`
- `ForwardView::iter_ready_sets()` returns opaque iterator
- EventId is a hash, hard to debug

**Improvements**:

1. **Rich debug output**: Add `Display` for `CausalRelation` with context

   - Example: `"DivergedSince { meet: [a], subject: [q], other: [m] } - 5 ReadySets"`
   - Benefit: Easier debugging, better logs
   - Priority: **High** (low effort, high value)

2. **ForwardView introspection**: Expose `len()`, `is_empty()`, `num_ready_sets()`

   - Benefit: Visibility into processing costs
   - Priority: **Low** (nice-to-have)

3. **EventId human-readable suffix**: Include truncated hash in Display
   - Example: `EventId(abc123...)` instead of full 32-byte hash
   - Benefit: Easier to correlate logs with events
   - Priority: **Medium**

### 4.2 Configuration and Tuning

**Current State**:

- Budget is hardcoded (e.g., `100` in tests, varies in production)
- No tunables for retry policy, cache sizes, etc.

**Improvements**:

1. **`ConcurrentUpdateConfig` struct**: Centralize configuration

   - Fields: `traversal_budget`, `max_retries`, `comparison_cache_size`
   - Benefit: Explicit tuning knobs
   - Priority: **Low** (current defaults work)

2. **Adaptive budgeting**: Adjust budget based on recent traversal costs
   - Benefit: Balance latency vs. completeness
   - Priority: **Low** (premature optimization)

---

## 5. Observability and Diagnostics

### 5.1 Metrics and Tracing

**Current State**:

- `tracing::info!` for major events
- No metrics for comparison costs, ReadySet sizes, etc.

**Improvements**:

1. **Structured metrics**: Instrument key paths

   - `comparison_duration`, `forward_view_size`, `ready_set_count`, `lww_conflicts_resolved`
   - Benefit: Production visibility, identify bottlenecks
   - Priority: **High** (essential for production)

2. **Debug visualizations**: Export DAG to Graphviz/Mermaid for failing tests
   - Benefit: Understand complex scenarios
   - Priority: **Low** (developer tool)

### 5.2 Error Context

**Current State**:

- Errors lack context (which entity? which event?)
- Retry failures are logged but not aggregated

**Improvements**:

1. **Error context propagation**: Use `anyhow::Context` consistently

   - Example: `.context(format!("Applying event {} to entity {}", event_id, entity_id))`
   - Benefit: Actionable error messages
   - Priority: **High**

2. **Failure telemetry**: Track retry failures, budget exceeded frequency
   - Benefit: Identify systemic issues (e.g., insufficient budget)
   - Priority: **Medium**

---

## 6. Semantic and Edge Case Refinements

### 6.1 Root Event Handling

**Current State**:

- Special case in `compare_unstored_event`: Empty parent returns `StrictAscends`
- No validation that root events are actually roots (parent = [])

**Potential Issues**:

1. **Malformed root events**: Parent = [] but event has parent IDs in payload

   - Risk: Inconsistent DAG structure
   - Mitigation: Validate root events at ingestion
   - Priority: **Low** (should be enforced elsewhere)

2. **Multiple roots**: What if entity has multiple independent root events?
   - Current: `Disjoint` relation, rejected by policy
   - Question: Should we support multi-root entities?
   - Priority: **Low** (policy decision, not implementation)

### 6.2 Clock Invariants

**Current State**:

- Clocks are assumed to be antichains (no member is ancestor of another)
- No runtime validation

**Potential Issues**:

1. **Malformed clocks**: Clock = [a, b] where b descends from a

   - Risk: Incorrect comparison results
   - Mitigation: Validate/normalize clocks on construction
   - Priority: **Medium** (defensive programming)

2. **Empty clock semantics**: Does Clock = [] represent "no state" or "unknown"?
   - Current: Treated as "no state" (genesis)
   - Question: Should empty clock be invalid?
   - Priority: **Low** (clarify in docs)

---

## 7. Future Enhancements (Post-Optimization)

### 7.1 Backend Extensibility

**Current State**:

- LWW and Yrs backends
- `PropertyTransaction` trait is generic

**Future Work**:

1. **Additional backends**: CRDT Sets, Counters, Lists
2. **Composite backends**: Entity with mixed backend types
3. **Backend versioning**: Migrate backend semantics without data loss

### 7.2 Advanced Concurrency Patterns

**Current State**:

- Simple two-way merge (subject vs other)
- No multi-way merge

**Future Work**:

1. **N-way merge**: Merge multiple concurrent branches in one pass
2. **Partial application**: Apply subset of concurrent events (filtered by predicate)
3. **Speculative execution**: Optimistically apply events, rollback if head changed

---

## Priority Matrix

| Category        | Item                              | Impact | Effort | Priority |
| --------------- | --------------------------------- | ------ | ------ | -------- |
| **Performance** | ReadySet event references         | Medium | Low    | **High** |
|                 | SmallVec for clocks               | Medium | Low    | **High** |
|                 | Operation deserialization caching | Low    | Medium | Medium   |
|                 | Lazy event loading                | Medium | High   | Medium   |
| **Clarity**     | Structured error types            | High   | Medium | **High** |
|                 | Extract EventApplication module   | Medium | High   | Medium   |
|                 | ForwardView builder pattern       | Low    | Low    | Low      |
| **Usability**   | Rich debug output                 | High   | Low    | **High** |
|                 | EventId readable suffix           | Medium | Low    | Medium   |
|                 | Metrics instrumentation           | High   | Medium | **High** |
| **Robustness**  | Clock validation                  | Medium | Medium | Medium   |
|                 | Error context propagation         | High   | Low    | **High** |
|                 | Test fixture library              | Medium | Medium | Medium   |
| **Advanced**    | Comparison result cache           | Medium | High   | Medium   |
|                 | Meet caching                      | Low    | High   | Low      |
|                 | Property-based testing            | Low    | High   | Low      |

---

## Recommended First Steps

1. **Quick Wins** (1-2 days):

   - Add structured error types (`ConcurrentUpdateError`)
   - Implement rich debug output for `CausalRelation`
   - Use `SmallVec` for clock members
   - Add error context propagation

2. **Medium-Term** (1 week):

   - Refactor ReadySet to use event references
   - Add metrics instrumentation
   - Extract `EventApplication` module
   - Create test fixture library

3. **Long-Term** (ongoing):
   - Implement comparison result cache (if profiling shows benefit)
   - Add property-based testing
   - Explore lazy event loading for large histories

---

## Success Criteria

**How do we know we've succeeded?**

1. **Performance**:

   - 50% reduction in allocations for typical concurrent update
   - Sub-millisecond comparison for histories < 1000 events

2. **Clarity**:

   - New contributor can understand apply_event flow in < 30 minutes
   - Error messages are actionable without reading source

3. **Robustness**:

   - Zero panics in production under any DAG structure
   - Graceful degradation when budgets are exceeded

4. **Maintainability**:
   - Adding new backend type requires < 200 LOC
   - Test setup time reduced by 50% with fixture library

---

## Open Questions

1. Should we support multi-root entities, or enforce single-root policy?
2. What should the default traversal budget be? (Currently ad-hoc per call site)
3. Is clock validation the responsibility of the comparison layer or the caller?
4. Should `ForwardView` be a trait to support alternative implementations?
5. Do we need a separate `Rollback` phase for failed transactions, or is drop() sufficient?

---

## References

- Implementation: `core/src/entity.rs`, `core/src/causal_dag/compare.rs`, `core/src/property/backend/lww.rs`
- Specification: `specs/concurrent-updates/spec.md`, `specs/concurrent-updates/lww-semantics.md`
- Tests: `tests/tests/concurrent_transactions.rs`, `core/src/property/backend/lww.rs` (unit tests)
