## Concurrent Updates: Implementation Plan

### Objectives

- Correctness-first: replay gaps for StrictDescends; deterministic LWW for true concurrency; ignore StrictAscends.
- Minimal invasiveness: keep current call sites stable; migrate types gradually.
- Prepare for future lineage attestation and wire-level `proto::CausalRelation`.

### Nomenclature and Type Alignment

- **Single traversal engine**: `lineage::Comparison` remains the authoritative reverse BFS implementation.

  - ✅ `LineageVisitor` trait defined for observing traversal steps and accumulating results.
  - ⚠️ Not yet wired: `Comparison` does not yet accept or invoke visitors.

- **Unified relation types**:

  - ✅ `lineage::Ordering` renamed to `lineage::CausalRelation` (working/in-memory type).
  - ✅ `lineage::RelationAndChain` struct added; `compare`/`compare_unstored_event` return it (contains `CausalRelation` + `forward_chain`).
  - ⚠️ `proto::CausalRelation` conversions not yet implemented.

- **ForwardView and ReadySet**:
  - ✅ Implemented as types in `causal_dag/forward_view.rs`.
  - ✅ `ForwardView<E: TEvent>` contains closed set between meet → {subject, other}.
  - ✅ `ReadySetIterator` streams `ReadySets` using Kahn's algorithm.
  - ✅ Events tagged as `Primary` (subject path) or `Concurrency` via precomputed `primary_path`.
  - ✅ `RelationAndChain` includes `forward_view: Option<ForwardView>` (backward compat: also keeps `forward_chain`).

### Injection Points (What's Implemented)

- **Event caching in Comparison**:

  - ✅ All fetched events cached in `events_by_id` and `parents_by_id` during traversal.
  - ✅ After meet is known, `build_forward_view()` filters to closed set (meet → {subject, other}).
  - ✅ Used by `compare()` to construct `ForwardView` with complete event set for both branches.

- **EventAccumulator** (legacy, now redundant):
  - ✅ Injected via `Comparison::new_with_accumulator`.
  - ⚠️ Still accumulates during traversal but EventBridge now uses ForwardView ReadySets instead.
  - ⚠️ Consider removal after verifying no other use cases.

### Visitors to Implement

- ✅ **ForwardView**: Implemented as a separate type; streams ReadySets via iterator.
- ⚠️ **LineageVisitor invocation**: trait defined but not actively used (ForwardView replaced the need for visitor-based forward application).
- ⚠️ **LWWResolver**: to be implemented inside `LWWTransaction::apply_ready_set` (causal LWW-register with lexicographic tiebreak).
- ⚠️ **Optional Stats**: budget accounting, depths, and frontiers for resumption.

### Implementation Status and Next Steps

**✅ Completed (Phase 1 + ForwardView Implementation)**:

- `LineageVisitor` trait defined with `on_forward` signature.
- `Ordering` renamed to `CausalRelation` with variants: `Equal`, `StrictDescends`, `StrictAscends`, `DivergedSince`, `Disjoint`, `BudgetExceeded`.
- `RelationAndChain` struct added; `compare`/`compare_unstored_event` return it with `forward_view: Option<ForwardView>`.
- `ForwardView<E: TEvent>` and `ReadySet<E>` types implemented with Kahn's algorithm for topological layering.
- `EventRole` tagging: events classified as `Primary` (subject path) or `Concurrency` (concurrent branches).
- `Comparison` caches all fetched events in `events_by_id` and `parents_by_id`.
- `build_forward_view()` filters to closed set (meet → {subject, other}) after meet is known.
- `PropertyTransaction<E: TEvent>` trait with `apply_ready_set()`, `commit(&mut self)`, `rollback(&mut self)`.
- `PropertyBackend::begin()` returns `Box<dyn PropertyTransaction<Event>>` for object safety.
- `LWWTransaction` and `YrsTransaction` stubs implemented.
- `Entity::apply_event` handles `DivergedSince` via ForwardView + backend transactions (with legacy fallback).
- Server-side `EventBridge` generation via `Node::collect_event_bridge` uses ForwardView ReadySets (Primary-only).
- Client-side `NodeApplier::apply_deltas` applies bridges with batch reactor notification.
- 11 unit tests for ForwardView and integration with Comparison.

**⚠️ Remaining Work**:

0. **Invert compare arguments for clarity** [SMALL REFACTOR]:

   - Update `Entity::apply_event` to call `compare_unstored_event(getter, &head, event, budget)` (head is subject/Primary, event is other/incoming).
   - Update `Entity::apply_state` to call `compare(getter, &head, &new_head, budget)` (head is subject/Primary, new_head is other/incoming).
   - This aligns with the semantic model where the entity's current head is the reference branch (Primary/subject).
   - Be sure to switch the match arms between StrictDescends and StrictAscends accordingly.

1. **Wire LineageVisitor into Comparison**:

   - Accept optional visitor in `Comparison::new` / `new_with_accumulator`.
   - Invoke `visitor.on_forward(meet, forward_chain, descendants, ctx)` after meet discovery.
   - Allow visitor to drive forward application planning.

2. **Implement ForwardView and ReadySet streaming**:

   - Replace `build_forward_chain` (which returns `Vec<Event>`) with a `ForwardView` generator.
   - `ForwardView` should stream `ReadySets` (topological layers from Kahn's algorithm).
   - Each `ReadySet` tags events as `Primary` (on subject path) or `Concurrency` (sibling events).
   - Keep memory-efficient; avoid materializing the entire DAG when possible.

3. **Backend transaction protocol**:

   - ✅ `PropertyBackend::begin() -> Box<dyn PropertyTransaction<Event>>` for object safety.
   - ✅ `PropertyTransaction<E: TEvent>` trait with `apply_ready_set(&ReadySet<E>)`, `commit(&mut self)`, `rollback(&mut self)`.
   - ✅ `YrsTransaction` implemented: applies all ops from ReadySets; commits via backend.apply_operations.
   - ⚠️ `LWWTransaction`: stub only; apply_ready_set and commit logic not yet implemented.

   Snapshotless LWW tiebreak (CausalLWWState):

   - Inputs per ReadySet: { PrimaryClock, ConcurrencyEvents[] } and current LWW state (reflects Primary head, not the meet)
   - State structures:
     - `CausalLWWState { events: Vec<EventId>, registers: BTreeMap<PropertyName, ValueRegisterWire> }`
     - `ValueRegisterWire { entity_id_offset: u16, value: Option<Value> }` where `events[entity_id_offset]` is the event that last set the register
   - Per ReadySet procedure (no Primary re-apply; Primary is for comparison only):
     1. Determine Primary event ID for this layer (if any). Do NOT apply it.
     2. For each property mentioned by any event in {Primary ∪ Concurrency} at this layer:
        - Let `current_winner_id` be `events[register.entity_id_offset]` from CausalLWWState (if register exists), else None
        - Compute `best_id` among candidates: { current_winner_id (if exists), any concurrency event IDs touching this property, Primary event ID if it touches this property }
        - Tiebreak order:
          a) Prefer descendants over ancestors relative to the meet (i.e., this layer’s events supersede earlier ones)
          b) Among concurrent candidates in this layer, prefer lexicographically greatest EventId
        - If `best_id` != `current_winner_id`, update register: set `entity_id_offset` to index of `best_id` in `events` (push if new), and set `value` to that event’s value for the property
     3. Repeat for each ReadySet until ForwardView is exhausted
   - Commit:
     - Serialize only changed registers to a diff via `to_state_buffer()`; persist alongside the updated head
   - Notes:
     - Never apply Primary’s operation directly; its presence in the ReadySet is only for comparison against concurrency
     - Because we do not have a snapshot at the meet, `current_winner_id` represents the Primary-branch-applied winner as of the entity’s head; ReadySet processing revises winners as needed moving forward

   LWWTransaction structure (begin/apply/commit lifecycle):

   - Fields:
     - `base_registers: BTreeMap<PropertyName, ValueRegister>` – copy of current backend registers at `begin()` (values reflect Primary head, includes last_writer_event_id)
     - `pending_winners: BTreeMap<PropertyName, (EventId, Option<Value>)>` – proposed winners after ReadySet processing (event that wins and its value)
   - Methods:
     - `apply_ready_set(rs)`: executes the per-ReadySet procedure above, updating `pending_winners` using lineage-first + lexicographic tie; Primary is comparison-only
     - `commit()`:
       - Compute diffs by comparing `pending_winners` against `base_registers` (event_id and value)
       - Build `events: Vec<EventId>` and per-register `entity_id_offset` ON THE FLY from the set of changed winners (no events table during apply)
       - Merge winners into backend’s in-memory `ValueRegister`s (update value and last_writer_event_id) and mark committed
       - `to_state_buffer()` then serializes only changed registers (diff)
     - `rollback()`: drop pending state
   - Backend support required:
     - ValueRegister MUST store `last_writer_event_id: EventId`
     - `to_state_buffer()` MUST serialize diffs: { events[], registers{ name -> { entity_id_offset, value } } } constructed at commit time

4. **Update Entity::apply_event for DivergedSince**:

   - For `DivergedSince`, construct `ForwardView` from meet to union of heads.
   - Call `backend.begin()` for each backend.
   - Iterate `ReadySets`; call `tx.apply_ready_set(rs)` for each backend.
   - On success: call `tx.commit()` for all backends, then `backend.to_state_buffer()` to serialize state, and atomically persist state buffers with updated heads per pruning logic.
   - On error, call `tx.rollback()` for all backends.
   - Remove head-augmentation hack in `Entity::apply_event`.

5. **Remove reverse-apply hack in NodeApplier**:

   - Once `Entity::apply_event` handles forward replay correctly, remove the `.rev()` hack in `NodeApplier::apply_delta_inner`.

6. **Add proto::CausalRelation conversions**:

   - Implement `From<lineage::CausalRelation<EventId>>` for `proto::CausalRelation`.
   - Implement `TryFrom<proto::CausalRelation>` for `lineage::CausalRelation<EventId>`.

7. **Testing**:
   - Unit tests for `ForwardView` ReadySet streaming with concurrent branches.
   - Unit tests for backend transaction commit/abort.
   - Integration tests for `DivergedSince` deterministic resolution.
   - Regression tests: no spurious multi-heads for linear histories.

### API Changes (current state)

- **✅ `lineage::compare` / `compare_unstored_event`**:

  - Return `RelationAndChain { relation, forward_chain, forward_view: Option<ForwardView> }`.
  - `forward_chain` kept for backward compatibility (subject-only path, topologically sorted).
  - `forward_view` contains complete closed set (meet → {subject, other}) with Primary/Concurrency tagging.
  - Built for `StrictDescends` and `DivergedSince`; `None` for other relations.

- **✅ `Entity::apply_event`**:

  - `Equal`: no-op.
  - `StrictDescends`: calls `apply_forward_chain` to replay events oldest→newest.
  - `StrictAscends`: no-op (incoming is older).
  - `DivergedSince`: uses ForwardView + backend transactions for deterministic resolution.
    - Begins transaction for each backend, applies ReadySets, commits atomically with CAS head check.
    - Legacy augment fallback if ForwardView unavailable (to be removed).

- **✅ Backend transaction contract**:

```rust
trait PropertyTransaction<E: TEvent<Id = EventId>>: Send {
  fn apply_ready_set(&mut self, rs: &ReadySet<E>) -> Result<(), MutationError>;
  fn commit(&mut self) -> Result<(), MutationError>;
  fn rollback(&mut self) -> Result<(), MutationError>;
}

trait PropertyBackend {
  fn begin(&self) -> Result<Box<dyn PropertyTransaction<Event>>, MutationError>;
  // ... other methods
}
```

- **✅ `NodeApplier`** (with temporary hack):
  - `apply_deltas` applies `EventBridge` and `StateSnapshot` deltas in parallel batches.
  - ⚠️ Temporary hack: applies events in reverse order (`.rev()`) to work around legacy `NotDescends` issue in `Entity::apply_event`.
  - ⚠️ Next: remove `.rev()` hack once `Entity::apply_event` handles forward replay correctly for all cases.
  - ✅ Persists only actually applied events for ephemeral nodes via `store_used_events`.

### Testing Status

**✅ Completed**:

- `lineage::tests::test_forward_chain_*`: Kahn's algorithm produces correct causal order.
- `lineage::tests::test_event_accumulator`: EventAccumulator collects subject-side events correctly.
- `lineage::tests::test_empty_clocks`: Correct `Equal`/`StrictDescends`/`StrictAscends`/`Disjoint` classification for empty clocks.
- Integration: Server EventBridge generation and client application via `NodeApplier` (with reverse-apply hack).

**⚠️ Needed**:

- Unit: LWWTransaction apply_ready_set tiebreaking logic (lineage + lexicographic).
- Unit: LWWTransaction commit logic (diff, events table, register merge, serialization).
- Unit: LWW tests in `lww.rs` with directly constructed ForwardViews.
- Integration: DivergedSince end-to-end with real backends (verify deterministic convergence).
- Integration: EventBridge applies without reverse hack once LWW is verified.
- Regression: No spurious multi-heads for linear histories.

### Performance and Budgeting

- Staged events are always preferred during traversal.
- Local retrieval is batched with fixed low cost; remote retrieval is costlier and batched.
- On `BudgetExceeded`, return frontiers and resume later; visitors surface these frontiers.

### Backward Compatibility and Rollout

- Provide type aliases to keep older names compiling.
- Gate new behavior behind minimal code paths first (Descends replay), then introduce concurrency resolution.
- Once stable, migrate call sites to use `CausalRelation` naming and remove aliases.

### Atomicity and TOCTOU

- Follow the spec's apply-phase head recheck: compute plan without lock, acquire lock, verify head unchanged, then run backend transactions (`begin_forward` → `on_ready_set` → `finish(Commit|Abort)`), and advance heads atomically.

### Future Considerations

- Lineage attestation (`DeltaContent::StateAndRelation`) will allow skipping traversal in some cases subject to policy trust; this is specified in `specs/lineage-attestation/spec.md` and will be implemented after concurrent-updates.
