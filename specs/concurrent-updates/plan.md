## Concurrent Updates: Implementation Plan

### Objectives

- Correctness-first: replay gaps for StrictDescends; deterministic LWW for true concurrency; ignore StrictAscends.
- Minimal invasiveness: keep current call sites stable; migrate types gradually.
- Prepare for future lineage attestation and wire-level `proto::CausalRelation`.

### Nomenclature and Type Alignment

- Soft-rename comparison engine:

  - `lineage::Comparison` → `lineage::EventTraversal`.
  - Introduce `LineageVisitor` trait to observe traversal steps and accumulate results.

- Unify relation types:
  - Rename `lineage::Ordering` → `lineage::CausalRelation` (working/in-memory type).
  - Provide `From`/`Into` conversions between `lineage::CausalRelation` and `proto::CausalRelation` (wire).
  - Existing compare APIs continue to return the working `CausalRelation`.

### Visitors to Implement

- BridgeCollector: collects the connecting chain for server-side EventBridge construction.
- ForwardApplyPlanner: unified ascending-phase `on_forward(meet, subject_chain_forward, known_descendants_of_meet, ctx) -> new_frontier` for `Entity::apply_event`.
- Classification: classifies `StrictAscends`, `StrictDescends`, `DivergedSince`, `Equal`, `Disjoint`, `BudgetExceeded`.
- LWWResolver: within `on_forward`, resolves per-backend deterministically (lineage-first; lexicographic id tiebreak; optional Yrs later) and bubbles unsubsumed concurrency into the returned frontier.
- Optional StatsVisitor: budget accounting, depths, and frontiers for resumption.

### Phased Delivery

- Phase 1: Introduce `EventTraversal` + `LineageVisitor` and adapt existing server EventBridge path to use `BridgeCollector`. Keep old names via type alias.
- Phase 2: Update `Entity::apply_event` to use `ReplayPlanner` for `StrictDescends` with gaps. Apply chain oldest→newest under CAS. This removes the need for the current reverse-application hack in `NodeApplier`.
- Phase 3: Replace the `NotDescends` augmentation path:
  - Use `ClassificationVisitor` to split `StrictAscends` (ignore) from `DivergedSince` (true concurrency).
  - Use `MergePlanner`+`LWWResolver` to deterministically update backends; do not augment heads unless a backend explicitly wants multi-head semantics.
- Phase 4: Rename `Ordering` to `CausalRelation` and implement conversions to `proto::CausalRelation`.
- Phase 5: Test suite and instrumentation: focused unit tests + integration tests; surface budget/resume info.

### API Changes (incremental)

- `lineage::compare` / `compare_unstored_event` return `lineage::CausalRelation` and, when a visitor is provided, drive a two-phase flow:

  - Backward traversal to detect the meet and prepare forward chains/frontiers,
  - Ascending invocations of `on_forward(meet, subject_chain_forward, known_descendants_of_meet, ctx) -> new_frontier`.

- `Entity::apply_event`:

  - Use ForwardApplyPlanner and implement `on_forward` to apply subject_chain_forward, prune subsumed known descendants of the meet, and bubble remaining concurrency into the returned frontier.
  - `StrictDescends` yields a single-member frontier; `StrictAscends` is a no-op (no `on_forward`).
  - `DivergedSince` uses the same `on_forward` hook to deterministically resolve values and bubble concurrency; no head-augmentation hacks.

- `NodeApplier`:
  - EventBridge application remains oldest→newest; once Phase 2 lands, this is naturally satisfied without hacks.
  - Persist only actually applied events for ephemeral nodes (`store_used_events`).

### Testing Plan

- Unit: Descends with N>1 replays N steps; head advances correctly; no `NotDescends` encountered.
- Unit: StrictAscends ignored.
- Unit: DivergedSince resolved deterministically per backend (LWW + lexicographic id tiebreak).
- Integration: Server EventBridge applies on client without budget exceed; staged events are used at zero cost.
- Regression: No spurious multi-heads created for linear histories.

### Performance and Budgeting

- Staged events are always preferred during traversal.
- Local retrieval is batched with fixed low cost; remote retrieval is costlier and batched.
- On `BudgetExceeded`, return frontiers and resume later; visitors surface these frontiers.

### Backward Compatibility and Rollout

- Provide type aliases to keep older names compiling.
- Gate new behavior behind minimal code paths first (Descends replay), then introduce concurrency resolution.
- Once stable, migrate call sites to use `CausalRelation` naming and remove aliases.

### Future Considerations

- Lineage attestation (`DeltaContent::StateAndRelation`) will allow skipping traversal in some cases subject to policy trust; this is specified in `specs/lineage-attestation/spec.md` and will be implemented after concurrent-updates.
