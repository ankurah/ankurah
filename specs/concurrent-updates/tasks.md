## Concurrent Updates: Task Tracking

### ✅ Completed

1. **Lineage infrastructure**:

   - ✅ `LineageVisitor` trait defined with `on_forward` signature.
   - ✅ `Ordering` renamed to `CausalRelation` with correct variants.
   - ✅ `RelationAndChain` struct; `compare`/`compare_unstored_event` return it.
   - ✅ `build_forward_chain` using Kahn's algorithm (topological sort).
   - ✅ `EventAccumulator` injected via `new_with_accumulator`.

2. **ForwardView and ReadySet streaming**:

   - ✅ `ForwardView<E: TEvent>` type implemented with closed set filtering.
   - ✅ `ReadySet<E>` with `EventRole` tagging (Primary vs Concurrency).
   - ✅ `ReadySetIterator` using Kahn's algorithm for topological layering.
   - ✅ `compute_primary_path()` reverse-BFS from subject_head to meet for Primary tagging.
   - ✅ Unit tests: 7 tests covering linear history, divergence, merges, empty views.
   - ✅ `Comparison` caches `events_by_id` and `parents_by_id` during traversal.
   - ✅ `Comparison::build_forward_view()` filters to closed set (meet → {subject, other}).
   - ✅ `compare()` and `compare_unstored_event()` return `ForwardView` in `RelationAndChain`.

3. **Backend transaction protocol**:

   - ✅ `PropertyTransaction<E: TEvent>` trait defined (generic over Event type).
   - ✅ Methods: `apply_ready_set(&ReadySet<E>)`, `commit(&mut self)`, `rollback(&mut self)`.
   - ✅ `PropertyBackend::begin() -> Box<dyn PropertyTransaction<Event>>` for object safety.
   - ✅ `LWWTransaction` stub with `base_registers` and `pending_winners`.
   - ✅ `YrsTransaction` implemented (applies all ops, commits via backend.apply_operations).
   - ✅ `ValueRegister::last_writer_event_id` field added for causal tiebreaking.

4. **Entity application**:

   - ✅ `Entity::apply_event` handles `Equal` (no-op), `StrictAscends` (no-op).
   - ✅ `Entity::apply_event` handles `StrictDescends` via `apply_forward_chain`.
   - ✅ `Entity::apply_forward_chain` replays events oldest→newest.
   - ✅ `Entity::apply_event` handles `DivergedSince` via ForwardView + backend transactions.
   - ✅ Atomic transaction commit with CAS head check.
   - ✅ Legacy augment fallback (to be removed after LWW implementation).

5. **Server-side EventBridge**:

   - ✅ `Node::collect_event_bridge` uses ForwardView ReadySets (Primary-only).
   - ✅ Integrated in Fetch and SubscribeQuery paths via `generate_entity_delta`.
   - ✅ Smart delta generation: omit if heads equal, else EventBridge, else StateSnapshot.

6. **Client-side application**:

   - ✅ `NodeApplier::apply_deltas` applies bridges and snapshots in parallel batches.
   - ✅ Batch reactor notification via `reactor.notify_change`.
   - ✅ Ephemeral event staging and persistence via `store_used_events`.

7. **Tests**:
   - ✅ `causal_dag::forward_view::tests`: 7 unit tests for ReadySet streaming, Primary/Concurrency tagging.
   - ✅ `causal_dag::tests::test_forward_view_*`: 4 integration tests exercising ForwardView with Comparison.
   - ✅ `lineage::tests::test_forward_chain_*`: Kahn topological sort correctness.
   - ✅ `lineage::tests::test_event_accumulator`: Bridge event collection.
   - ✅ `lineage::tests::test_empty_clocks`: Empty clock classification.

### ⚠️ In Progress / Remaining

#### High Priority (Core Functionality)

1. **Implement LWWTransaction apply_ready_set logic** [IN PROGRESS]:

   - Causal LWW tiebreaking per ReadySet:
     - For each property touched by events in the ReadySet:
       - Get current winner from `base_registers.last_writer_event_id`
       - Collect all candidate events (Primary + Concurrency) that touch this property
       - Tiebreak: lineage precedence first, then lexicographic on EventId
       - Update `pending_winners` if best_id differs from current winner
   - Primary events are for comparison only (DO NOT re-apply their operations)

2. **Implement LWWTransaction commit logic** [NOT STARTED]:

   - Diff `pending_winners` vs `base_registers` to find changed properties
   - Build `events: Vec<EventId>` from unique winner EventIds
   - Create `entity_id_offset` mapping for each changed register
   - Merge `pending_winners` into backend's `ValueRegisters` (update value and last_writer_event_id)
   - Mark registers as committed
   - Serialize changed registers via `to_state_buffer()` with events table

3. **Unit tests: LWWTransaction** [NOT STARTED]:

   - Primary-only ReadySets produce no changes (comparison-only)
   - Single concurrency event wins over existing register
   - Concurrent siblings → lexicographic tiebreak
   - Multiple ReadySets revise winners across layers
   - Lineage precedence: later events beat earlier events
   - Events table construction at commit: offsets stable and deduplicated

4. **Remove reverse-apply hack in NodeApplier** [BLOCKED]:

   - Once LWW is fully implemented and tested, remove `.rev()` in `apply_delta_inner`
   - Verify no regressions in integration tests

5. **Consider removing EventAccumulator** [LOW PRIORITY]:

   - EventBridge now uses ForwardView ReadySets directly
   - EventAccumulator conditional logic during traversal is complex and potentially over-collecting
   - Can be removed if no other use cases remain

#### Medium Priority (Polish & Conversions)

6. **Add proto::CausalRelation conversions** [NOT STARTED]:

   - Implement `From<lineage::CausalRelation<EventId>>` for `proto::CausalRelation`.
   - Implement `TryFrom<proto::CausalRelation>` for `lineage::CausalRelation<EventId>`.

7. **Integration tests for DivergedSince** [NOT STARTED]:

   - End-to-end tests with real Entity and backends
   - Deterministic LWW resolution (lineage + lexicographic tiebreak)
   - No spurious multi-heads for linear histories
   - Verify state convergence across different event orderings

8. **Budget/resumption tests** [NOT STARTED]:

   - `BudgetExceeded` return and resumable frontiers

#### Documentation

14. **Document budgeting and resumption** [NOT STARTED]:

    - Update API docs for `Comparison` with budget behavior.
    - Document resumption pattern for callers.

15. **Update inline docs for renamed types** [PARTIAL]:
    - ✅ `CausalRelation` documented with proto distinction.
    - ⚠️ Update remaining references to "Ordering" in comments.

### Notes

- **ForwardView closed set**: Contains exactly the events between meet → {subject, other}. Built via reverse-walk after meet is known.
- **EventAccumulator**: Now potentially redundant (EventBridge uses ForwardView ReadySets). Consider removal.
- **Comparison caching**: All fetched events stored in `events_by_id` + `parents_by_id` for post-meet ForwardView construction.
- **Object safety**: `PropertyBackend` remains object-safe by returning `Box<dyn PropertyTransaction<Event>>` from `begin()`.
- **Transaction lifetime**: `commit()` and `rollback()` take `&mut self` (not `self`) for trait object compatibility.
- **Temporary hack**: `.rev()` in `NodeApplier` exists to work around legacy issue; remove after LWW implementation verified.
- **Backend contract**: Atomic commit/abort across all backends in the apply phase critical section with CAS head check.
