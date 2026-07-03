# Fix Plan, 2026-07: landing concurrent-updates-event-dag

Companion to `verification-review-2026-07.md`. Goal: take PR #201 from "not merge-ready" to merged, fixing the four confirmed bugs and two regressions, without redesigning the algorithm.

**Non-goals:** pre-existing issues tracked elsewhere (#242-#247, diverged-snapshot staleness, remote-commit ordering), offline-writes work (belongs to the #252 follow-up), and any protocol changes (none required; wire compatibility across minors is not guaranteed regardless, and the eventual release rides the 0.9.0 chain).

**Working state:** worktree `~/ak/ankurah-201`, rebased onto 0.8.1 and pushed. Verification red tests live uncommitted in `core/src/event_dag/tests.rs` (backup in stash "verification red-tests"). Each fix commit should include its red test turning green; commit test+fix together, in the red-to-green style.

## Cluster A: Comparison state machine bookkeeping (fixes V1, V2, V3)

All three confirmed comparison bugs are bookkeeping, not algorithm-shape, problems. One focused rework of `core/src/event_dag/comparison.rs`:

### A1. Per-side visitation and head accounting (V1)

- Replace the numeric `unseen_comparison_heads` / `unseen_subject_heads` counters with idempotent sets: `seen_comparison_heads: BTreeSet<EventId>` and `seen_subject_heads: BTreeSet<EventId>`; completion tests compare `len()` against the original head sets. Double-visits become harmless inserts.
- Add per-side processed sets (`subject_processed`, `comparison_processed`) consulted before `frontier.extend` and in `process_event`, so each node is expanded at most once per side. This also:
  - restores budget semantics to per-event rather than per-path (removes the exponential diamond blowup),
  - stops `subject_visited` collecting duplicates, making `chain` contents sane,
  - removes redundant event fetches on diamond DAGs.
- Preserve existing constraints: no `E: Clone`; budget escalation stays internal to `Comparison`; only the LRU cache survives retry.

### A2. Origin propagation retirement (V2)

- On the propagation branch where a parent is reached that is already common (`meet_candidates.insert == false`), still retire the arriving origins from `outstanding_heads`.
- Belt-and-braces alternative if the incremental retirement proves fiddly: at exhaustion, recompute outstanding as `original_heads - union(origins of meet candidates and common nodes)` before the empty-meet gate. Prefer the incremental fix; use the recompute as a debug assertion.

### A3. Quick-check soundness guard (V3)

- The subset shortcut is only sound when every subject-clock event's one-step parents are themselves within the comparison set (that is what makes "subject strictly descends comparison" checkable without traversal). Guard accordingly: if ANY subject event has an empty parent set, or parents outside `comparison_set`, fall through to the full state machine. Keep the shortcut for the common linear-extension case it was built for.

### A tests

- Green the three red tests: `double_decrement_falsely_reports_strict_descends`, `late_origin_propagation_yields_empty_meet`, `test_quick_check_disjoint_extra_root`.
- Add: the mirror false-StrictAscends case; a diamond-chain budget test asserting comparison completes within `O(events)` budget; a quick-check fallthrough test (guard triggers, BFS answers DivergedSince); empty-vs-empty clocks (decide Equal semantics while in here, hardening item 6).
- Strongly recommended: a small property test comparing the state machine's verdict against a brute-force reachability oracle over randomized DAGs (bounded to ~10 events, exhaustive-ish seeds). The bug class here is exactly what property testing catches, and the oracle is 30 lines. Budget one hour; drop if it fights the harness.

## Cluster B: Application ordering (fixes V4)

- **Root fix, receiver-side:** in `node_applier.rs` EventBridge handling, after staging all bridge events, topologically sort them oldest-first (Kahn's algorithm over the staged set using parent edges; ids are content-addressed so cycles are impossible in honest input, but bail to the existing error path on cycle detection rather than looping). Then every application step is a clean parent-first apply and the gap-jump never spans staged-but-unapplied events from a bridge.
- Also sort at the producer (`collect_event_bridge`) since it is cheap and makes wire traffic sane, but the receiver-side sort is the correctness guarantee (receivers must not trust sender ordering).
- Correct the false "already in causal order from the server" comment.
- **Defense-in-depth (follow-up, not this pass):** teach the StrictDescends arm to replay staged-but-unapplied ancestors oldest-first before the incoming event. Blocked on trusting `chain`, which Cluster A's fix makes trustworthy; reconstruct via parent walk if we want it sooner. File as an issue so the arm's fragility (H6 in the February matrix, confirmed contract-fragile) stays visible.

### B tests

- Green the red `strict_descends_gap_jump` test (it should pass once bridge application is ordered; keep the test applying via the node_applier path if feasible so it exercises the sort, otherwise add a second integration-shaped test through EventBridge delivery with the uneven-diamond shape `[X,P,H2,H1]`).

## Cluster C: Contained fixes (V5, V6, V7)

### C1. Clock normalization (V5)

- Private `fn normalized(mut ids: Vec<EventId>) -> Vec<EventId>` (sort + dedup). Apply in `Clock::new`, `From<Vec<EventId>>`, the `Vec<Vec<u8>>` decode, and deserialization via `#[serde(from = "Vec<EventId>")]` (or a manual Deserialize). `from_strings` folds into the same helper.
- Tests: bincode round-trip of an unsorted clock followed by `contains`/`remove` assertions; head-maintenance regression per codex's `[C,B,E]` scenario.

### C2. Two-phase commit loop (V7)

- Split `commit_local_trx`'s fused loop: first loop runs every `check_event` (staging + fork application as today, no storage writes); second loop runs `commit_event` for all attested events. Restores main's failure-atomicity: a later policy denial leaves nothing durable.
- Test: two-entity transaction where the second entity's policy check fails; assert no event rows exist for either entity afterward (needs a PolicyAgent test double that denies selectively; the jwt-auth test patterns show the shape, but use a local core test agent to avoid a dev-dependency cycle).

### C3. EventOnly per-item containment (V6)

- In `node_applier.apply_updates`, stop `?`-propagating per-item errors: collect them, continue the batch, run `notify_change` for the successfully applied subset, and return an aggregate error (or log-and-count; match the codebase's tolerance elsewhere, e.g. the Fetch handler's skip semantics).
- For the specific empty-head-non-creation case: remove the phantom entity from the WeakEntitySet on failure, and request recovery instead of erroring: the natural shape is treating it like the diverged StateAndEvent fallback (fetch state from the peer). If that plumbing is more than an hour, do containment + phantom cleanup now and file the needs-state recovery as a follow-up issue; containment alone removes the batch-poisoning regression.
- Test: batch of three updates where the middle one is an EventOnly for an unknown entity; assert items one and three apply and notify, and no empty-head entity remains resident.

## Sequencing and estimates

1. **A** first (0.5-1 day): everything else's correctness reasoning depends on comparison verdicts being right, and A un-breaks `chain` for B's follow-up.
2. **B** (0.5 day) and **C** (0.5 day) are independent of each other; C can interleave anywhere.
3. Docs pass (0.25 day): the 11 items listed in `implementation-resume.md`, plus the bridge-ordering comment, `chain` caveats, and updating `implementation-resume.md`/`implementation-status.md` to reference the July verification (the "all tests pass / review complete" status is superseded).
4. Validation gate: all verification tests green; `cargo test` (default members) green; jwt-auth suite green; `cargo check -p ankurah-core --features wasm` green (isolated feature check, the 0.8.0 lesson); fmt + taplo.
5. Merge mechanics: squash-or-two-commit shape per maintainer preference (feature + release-chain), PR description updated to reflect the July verification and fixes, no em dashes. Version rides the 0.9.0 chain (with #252) unless released independently.
6. After merge: re-rebase `feature/offline-first` (#252) onto the new main; its plan is tracked separately and its `get_entity` seam must again preserve `check_read`.

Total: roughly two focused days including tests and docs.

## Risk notes

- Cluster A touches the hottest logic in the branch. Mitigations: the red tests pin the exact bugs; the property-test oracle catches regression classes we did not enumerate; the full integration suites (including jwt-auth and the durable/ephemeral concurrency tests) ran green on the rebase and act as the behavioral baseline.
- The quick-check guard (A3) may reduce how often the shortcut fires; if any perf-sensitive path relied on it for deep linear histories, the full BFS still answers correctly and the budget covers linear chains cheaply. No perf regression expected beyond the shortcut's narrowed applicability.
- C3 changes error semantics of `apply_updates`; audit its callers (peer_subscription client relay) for assumptions that any item failure fails the batch.
