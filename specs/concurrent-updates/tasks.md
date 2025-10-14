## Concurrent Updates: Task Tracking

### ✅ Completed

1. **Lineage infrastructure**:

   - ✅ `LineageVisitor` trait defined with `on_forward` signature.
   - ✅ `Ordering` renamed to `CausalRelation` with correct variants.
   - ✅ `RelationAndChain` struct; `compare`/`compare_unstored_event` return it.
   - ✅ `build_forward_chain` using Kahn's algorithm (topological sort).
   - ✅ `EventAccumulator` injected via `new_with_accumulator`.

2. **Server-side EventBridge**:

   - ✅ `Node::collect_event_bridge` uses `EventAccumulator` to collect gap events.
   - ✅ Integrated in Fetch and SubscribeQuery paths via `generate_entity_delta`.
   - ✅ Smart delta generation: omit if heads equal, else EventBridge, else StateSnapshot.

3. **Client-side application**:

   - ✅ `NodeApplier::apply_deltas` applies bridges and snapshots in parallel batches.
   - ✅ Batch reactor notification via `reactor.notify_change`.
   - ✅ Ephemeral event staging and persistence via `store_used_events`.

4. **Entity application (partial)**:

   - ✅ `Entity::apply_event` handles `Equal` (no-op), `StrictAscends` (no-op).
   - ✅ `Entity::apply_event` handles `StrictDescends` via `apply_forward_chain`.
   - ✅ `Entity::apply_forward_chain` replays events oldest→newest.

5. **Tests**:
   - ✅ `lineage::tests::test_forward_chain_*`: Kahn topological sort correctness.
   - ✅ `lineage::tests::test_event_accumulator`: Bridge event collection.
   - ✅ `lineage::tests::test_empty_clocks`: Empty clock classification.

### ⚠️ In Progress / Remaining

#### High Priority (Core Functionality)

1. **Wire LineageVisitor into Comparison** [NOT STARTED]:

   - Accept optional `visitor: Option<Box<dyn LineageVisitor>>` in `Comparison::new`.
   - After meet discovery, invoke `visitor.on_forward(meet, forward_chain, descendants, ctx)`.
   - Return visitor result alongside `CausalRelation`.

2. **Implement ForwardView and ReadySet streaming** [NOT STARTED]:

   - Replace `build_forward_chain() -> Vec<Event>` with `ForwardView` generator.
   - `ForwardView` streams `ReadySets` (topological layers from Kahn).
   - Tag each event as `Primary` (on subject path) or `Concurrency` (sibling).
   - Update `RelationAndChain` to hold `ForwardView` handle instead of `Vec<Event>`.

3. **Backend transaction protocol** [NOT STARTED]:

   - Define `PropertyBackend::begin() -> BackendTransaction`.
   - Define `BackendTransaction::apply_ready_set(&ReadySetView)` and `commit()/rollback()`.
   - Implement for LWW: maintain in-memory per-property champions; persist diffs on `commit()`.
   - Implement for Yrs: apply unseen ops; commit/rollback accordingly.

4. **Update Entity::apply_event for DivergedSince** [NOT STARTED]:

   - For `DivergedSince`, construct `ForwardView` from meet to union of heads.
   - Call `backend.begin()` for each backend; stream `ReadySets` via `tx.apply_ready_set`.
   - On success: `tx.commit()` all backends, update heads per pruning logic.
   - On error: `tx.rollback()` all backends.
   - Remove head-augmentation fallback.

5. **Remove reverse-apply hack in NodeApplier** [BLOCKED on #4]:

   - Once `Entity::apply_event` handles all cases correctly, remove `.rev()` in `apply_delta_inner`.

6. **Implement LWWTransaction** [NOT STARTED]:

   - Structure: `base_registers`, `pending_winners` (EventId, Value).
   - Logic: snapshotless per-ReadySet tiebreak using Primary for comparison only; update winners.
   - Commit: diff `pending_winners` vs `base_registers`; build `events` and `entity_id_offset` on-the-fly; serialize only changed registers via `to_state_buffer()`.
   - Rollback: drop pending.

7. **Unit tests: LWWTransaction** [NOT STARTED]:

   - Primary-only ReadySets produce no changes.
   - Single concurrency event wins over existing register.
   - Concurrent siblings tie → lexicographic tiebreak.
   - Multiple ReadySets revise winners across layers.
   - No-meet-snapshot correctness: outcomes match applying from meet with ideal snapshot.
   - Events table construction at commit: offsets stable and deduplicated across changed registers.

8. **Backend: ValueRegister last-writer** [NOT STARTED]:
   - Add `last_writer_event_id: EventId` to `ValueRegister`.
   - Update load/save paths to preserve it via state buffers.
   - Ensure `to_state_buffer()` emits diffs with `events[]` and `entity_id_offset` derived from changed registers.

#### Medium Priority (Polish & Conversions)

8. **Add proto::CausalRelation conversions** [NOT STARTED]:

   - Implement `From<lineage::CausalRelation<EventId>>` for `proto::CausalRelation`.
   - Implement `TryFrom<proto::CausalRelation>` for `lineage::CausalRelation<EventId>`.

9. **TOCTOU protection in Entity::apply_event** [NOT STARTED]:

   - Plan phase (no lock): compute relation, prepare ForwardView.
   - Apply phase (with lock): recheck head unchanged, then apply.
   - On head mismatch: release lock, recompute plan.

#### Testing

10. **Unit tests for ForwardView/ReadySet** [NOT STARTED]:

- ReadySet streaming with concurrent branches.
- Primary vs Concurrency tagging.

11. **Unit tests for backend transactions** [NOT STARTED]:

- LWW commit/rollback behavior.
- Yrs commit/rollback behavior.

12. **Integration tests for DivergedSince** [NOT STARTED]:

- Deterministic LWW resolution (lineage + lexicographic tiebreak).
- No spurious multi-heads for linear histories.

13. **Budget/resumption tests** [NOT STARTED]:

- `BudgetExceeded` return and resumable frontiers.

#### Documentation

14. **Document budgeting and resumption** [NOT STARTED]:

    - Update API docs for `Comparison` with budget behavior.
    - Document resumption pattern for callers.

15. **Update inline docs for renamed types** [PARTIAL]:
    - ✅ `CausalRelation` documented with proto distinction.
    - ⚠️ Update remaining references to "Ordering" in comments.

### Notes

- **EventAccumulator = BridgeCollector**: No separate type needed; injected accumulator is the implementation.
- **Single traversal engine**: `lineage::Comparison` is the authoritative reverse BFS; avoid duplicating traversal logic.
- **Temporary hack**: `.rev()` in `NodeApplier` exists to work around legacy `NotDescends` issue; remove after #4/#5.
- **ForwardView design**: Should be memory-efficient; avoid materializing entire DAG when possible (streaming/iterator-based).
- **Backend contract**: Atomic commit/abort across all backends in the apply phase critical section.
