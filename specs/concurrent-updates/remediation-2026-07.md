# Remediation Tracker, 2026-07

Execution state for the fixes in `fix-plan-2026-07.md`, addressing the confirmed findings in `verification-review-2026-07.md`. Update this file as items land; each fix commit un-ignores its verification test.

**Convention:** every red verification test is committed with `#[ignore = "Vn: red until remediation ..."]` in `core/src/event_dag/tests.rs`. A remediation item is DONE when its fix is committed, the test's ignore attribute is removed, and the test passes.

## Cluster A: comparison.rs bookkeeping

- [x] **A1 (V1, CRITICAL): per-side visitation and head accounting.**
      Replace `unseen_comparison_heads`/`unseen_subject_heads` counters with idempotent seen-sets; add per-side processed sets consulted before frontier extension and processing.
      Test to un-ignore: `bfs_revisit_bugs::double_decrement_falsely_reports_strict_descends`.
      Additional tests: mirror false-StrictAscends case; diamond-chain budget test (completes within O(events)).
- [x] **A2 (V2, HIGH): origin retirement on propagation to already-common nodes.**
      Retire arriving origins from `outstanding_heads` when propagation reaches a node already in `meet_candidates`; debug-assert against the exhaustion-time recompute.
      Test to un-ignore: `bfs_revisit_bugs::late_origin_propagation_yields_empty_meet`.
      DEVIATION from fix-plan A2: the planned design is insufficient. Origins can stall at a node that is already expanded but NOT common (A1's per-side dedup no longer re-expands it), so neither incremental retirement nor the origin-union recompute ever sees that head satisfied; both agree on the wrong answer and the planned debug-assert would not fire. Implemented the incremental retirement as planned PLUS an exhaustion-time reachability reconciliation over the accumulated DAG (a head is outstanding only if no meet candidate is backward-reachable from it), which replaces the recompute/debug-assert as product code. New pinning test: `bfs_revisit_bugs::origin_stalled_at_processed_node_still_retires_head` (verified to fail with reconciliation disabled and the planned fix in place). Note for the post-merge factorization pass: with reconciliation as ground truth, the incremental origins bookkeeping is a prunable fast path only.
- [x] **A3 (V3, HIGH): quick-check soundness guard.**
      Subset shortcut only when every subject event has a nonempty parent set contained in the comparison set; otherwise fall through to BFS.
      Test to un-ignore: `quick_check_disjoint_verify::test_quick_check_disjoint_extra_root`.
      Additional test: guard-triggered fallthrough returns DivergedSince.
      DEVIATION from fix-plan A3 (and correction to verification-review V3): the quick-check guard alone does not make the V3 test pass, because the BFS itself returns StrictDescends for the same shape. The review's claim that "the full state machine, if reached, answers correctly" is wrong post-trace: the seen-all-comparison-heads completion is cover-containment logic, so B's line reaching A completes it while disjoint root X rides along. Fixed by requiring, at StrictDescends completion, that every original subject head is grounded, meaning its ancestry within the accumulated DAG (including referenced parent ids) intersects the comparison heads' ancestry; ungrounded shapes run to exhaustion and yield DivergedSince { meet: [A] }, exactly the review's expected verdict. The predicate is shared-lineage, NOT reaches-a-comparison-head: the stronger form wrongly diverged the legitimate sibling-tip shape (incoming state head [F, J] over local head [J], F and J concurrent children of the creation event), caught by test_durable_vs_ephemeral_concurrent_write. Honest shapes ground at the same step they previously fired, so no behavior change for linear, diamond, or sibling-tip histories (all suites green). StrictAscends deliberately keeps cover-containment semantics: it drives skip-incoming decisions, where covering is sufficient, and tightening it is not required by any verification test.
      Also decided while in here (fix-plan A tests, hardening item 6): empty-vs-empty clocks now compare Equal (the identical-clocks check runs before the empty-clock check); one-sided empty still yields DivergedSince with an empty meet. `test_empty_clocks` updated accordingly; all three compare() call sites behave sensibly with Equal (apply_event guards empties earlier, apply_state maps it to AlreadyApplied, collect_event_bridge sends nothing).
- [x] **A4 (optional, recommended): property test.**
      Randomized small DAGs, state-machine verdict vs brute-force reachability oracle. Timebox one hour.
      Landed as `comparison_property::randomized_dags_match_reachability_oracle`: 300 xorshift-seeded DAGs (5-10 events, extra genesis roots ~1 in 6) x 4 antichain clock pairs each, no external crates. The oracle encodes the post-A3 semantics (grounded StrictDescends, cover-only StrictAscends) and the meet as the maximal antichain of the common cover. Validated once at 5000 seeds (20k comparisons) before committing at 300.
      HARDENING NOTE (found while specifying the oracle, pre-existing): for a comparison head set mixing shared-lineage and foreign heads, check_result's verdict is traversal-order-dependent: Disjoint when a foreign genesis happens to be recorded as other_root first, else DivergedSince with an empty meet, because the Disjoint sub-branch only compares first-found roots and ignores any_common. The oracle accepts either. Consider tightening Disjoint to require !any_common (a verdict claiming disjointness while common history exists misclassifies a mergeable update as an identity mismatch); fold into the post-merge hardening issue list.

## Cluster B: application ordering

- [ ] **B1 (V4, CRITICAL): receiver-side topological sort of EventBridge application.**
      Kahn's over the staged bridge set in node_applier; cycle detection bails to the existing error path. Fix the false "already in causal order" comment.
      Test to un-ignore: `strict_descends_gap_jump::test_strict_descends_gap_jump_skips_ancestor_ops`.
      Additional test: uneven-diamond bridge `[X,P,H2,H1]` end-to-end via EventBridge delivery.
- [ ] **B2 (hygiene): producer-side sort in `collect_event_bridge`.**
- [ ] **B3 (follow-up issue, do not implement now): StrictDescends gap-replay defense in entity.rs.**
      Blocked on chain trustworthiness (delivered by A1). File issue on merge.

## Cluster C: contained fixes

- [ ] **C1 (V5, MEDIUM): Clock normalization.**
      `normalized()` (sort+dedup) in new/From/TryInto and serde deserialization.
      Tests: unsorted bincode round-trip; codex's `[C,B,E]` head-maintenance scenario.
- [ ] **C2 (V7, LOW): two-phase commit_local_trx.**
      All `check_event` before any `commit_event`.
      Test: two-entity trx, second denied, assert zero durable events.
- [ ] **C3 (V6, MEDIUM-HIGH): per-item error containment in apply_updates.**
      Collect per-item failures, continue batch, notify applied subset; remove phantom empty-head entity on failure. Needs-state recovery: implement if under an hour, else file follow-up issue.
      Test: three-item batch with middle EventOnly-for-unknown-entity; items 1 and 3 apply and notify; no phantom resident.
      Audit: callers of apply_updates for any-failure-fails-batch assumptions.

## Docs and validation gate

- [ ] The 11 doc fixes itemized in `implementation-resume.md` (Doc Fixes Still TODO).
- [ ] event-dag.md: chain reliability caveat; corrected bridge-ordering statement.
- [ ] Update `implementation-resume.md` / `implementation-status.md` headers to reference the July verification and this tracker.
- [ ] Validation: all five verification tests green (zero ignores from this effort); `cargo test` default members; jwt-auth suite; `cargo check -p ankurah-core --features wasm` (isolated); fmt + taplo.
- [ ] Merge prep: PR #201 description refresh (no em dashes); version rides the 0.9.0 chain.

## After merge (tracked elsewhere)

- Factorization review pass over the branch (requested by maintainer; after remediation).
- Hardening list items 1-9 in `verification-review-2026-07.md` not folded in above.
- #252 re-rebase onto post-#201 main (preserve check_read on the get_entity resident path).
