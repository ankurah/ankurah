## Tasks

1. Introduce `EventTraversal` and `LineageVisitor` (keep `Comparison` alias short-term?).
2. Implement `BridgeCollector` and switch server EventBridge to use it.
3. Implement `ForwardApplyPlanner` with unified `on_forward(meet, subject_chain_forward, known_descendants_of_meet, ctx)` for `Entity::apply_event`.
4. Split `NotDescends` into `StrictAscends` vs `DivergedSince` using traversal classification.
5. Implement `LWWResolver` inside `on_forward` (lineage-first; lexicographic id tiebreak; optional Yrs later) and bubble unsubsumed concurrency.
6. Rename `Ordering` → `CausalRelation`; add conversions to/from `proto::CausalRelation`.
7. Update tests for replay and concurrency resolution; add budget/resume tests.
8. Remove head-augmentation fallback in non-concurrent cases.
9. Document budgeting and resumption in traversal APIs.
10. Roll out aliases removal when stable.
