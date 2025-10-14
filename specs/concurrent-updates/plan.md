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
  - ⚠️ Terminology defined in spec; not yet implemented as types.
  - Current: `build_forward_chain` produces a topologically sorted `Vec<Event>` (Kahn's algorithm).
  - Needed: Replace `forward_chain` with a `ForwardView` that streams `ReadySets` (topological layers with Primary/Concurrency tagging).

### Injection Points (What's Implemented)

- **EventAccumulator** (Phase 1 bridge collector):
  - ✅ Injected via `Comparison::new_with_accumulator`.
  - ✅ Accumulates subject-side attested events during reverse BFS.
  - ✅ Used by `Node::collect_event_bridge` for server-side `EventBridge` generation.
  - ✅ Integrated in Fetch and SubscribeQuery paths; client applies via `NodeApplier::apply_deltas`.

### Visitors to Implement

- ✅ **EventAccumulator** (bridge collector): collects the connecting chain for server-side EventBridge construction (implemented as injected accumulator).
- ⚠️ **LineageVisitor invocation**: trait defined but not yet wired into `Comparison::step`.
- ⚠️ **ForwardView generation**: need to replace `build_forward_chain` with a ReadySet streaming generator.
- ⚠️ **LWWResolver**: to be implemented inside backend transactions (causal LWW-register with lexicographic tiebreak).
- ⚠️ **Optional Stats**: budget accounting, depths, and frontiers for resumption.

### Implementation Status and Next Steps

**✅ Completed (Phase 1)**:

- `LineageVisitor` trait defined with `on_forward` signature.
- `Ordering` renamed to `CausalRelation` with variants: `Equal`, `StrictDescends`, `StrictAscends`, `DivergedSince`, `Disjoint`, `BudgetExceeded`.
- `RelationAndChain` struct added; `compare`/`compare_unstored_event` return it.
- `build_forward_chain` using Kahn's algorithm produces topologically sorted event list.
- `EventAccumulator` injected into `Comparison` for bridge collection.
- Server-side `EventBridge` generation via `Node::collect_event_bridge` integrated in Fetch and Subscribe paths.
- Client-side `NodeApplier::apply_deltas` applies bridges with batch reactor notification.
- `Entity::apply_event` uses `compare_unstored_event` and `apply_forward_chain` for `StrictDescends` case (partial implementation).

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

   - Define `PropertyBackend::begin() -> BackendTransaction`.
   - Define `BackendTransaction::apply_ready_set(&ReadySetView) -> Result<(), Error>`.
   - Define `BackendTransaction::commit() -> Result<(), Error>` and `rollback() -> Result<(), Error>`.
   - Implement for LWW backend: maintain in-memory per-property champions; on `commit()`, call `to_state_buffer()` to serialize diffs.
   - Implement for Yrs backend: apply unseen ops; on `commit()`, call `to_state_buffer()` to serialize state.

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

  - Return `RelationAndChain { relation: CausalRelation, forward_chain: Vec<Event> }`.
  - `forward_chain` is topologically sorted (Kahn's algorithm) for `StrictDescends` and `DivergedSince`.
  - ⚠️ Next: replace `forward_chain: Vec<Event>` with `ForwardView` handle for ReadySet streaming.

- **✅ `Entity::apply_event` (partial)**:

  - `Equal`: no-op.
  - `StrictDescends`: calls `apply_forward_chain` to replay events oldest→newest.
  - `StrictAscends`: no-op (incoming is older).
  - ⚠️ `DivergedSince`: currently falls back to head augmentation (legacy behavior).
  - ⚠️ Next: for `DivergedSince`, build `ForwardView`, call `begin_forward` → `on_ready_set` → `finish(Commit|Abort)` for each backend.

- **⚠️ Backend transaction contract** (to be implemented):

```rust
trait PropertyBackend {
  type Tx: BackendTransaction;
  fn begin(&self) -> Result<Self::Tx, Error>;
}

trait BackendTransaction {
  fn apply_ready_set(&mut self, rs: &ReadySetView) -> Result<(), Error>;
  fn commit(self) -> Result<(), Error>;
  fn rollback(self) -> Result<(), Error>;
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

- Unit: `StrictDescends` with N>1 gap replays N events oldest→newest in `Entity::apply_event` (no augmentation).
- Unit: `StrictAscends` is ignored (no state change).
- Unit: `DivergedSince` resolved deterministically per backend (LWW + lexicographic tiebreak).
- Unit: `ForwardView` ReadySet streaming with concurrent branches (Primary vs Concurrency tagging).
- Unit: Backend transaction commit/abort behavior.
- Integration: EventBridge applies without reverse hack once `Entity::apply_event` handles forward replay.
- Regression: No spurious multi-heads for linear histories after removing augmentation fallback.

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
