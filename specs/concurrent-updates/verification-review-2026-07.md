# Verification Review, 2026-07 (post-rebase onto 0.8.1)

**Branch:** `concurrent-updates-event-dag` @ rebased `b377dc03` lineage
**Status verdict: NOT merge-ready.** Four confirmed bugs (two data-loss criticals) with failing tests, plus two confirmed branch regressions. All are concentrated and fixable; see `fix-plan-2026-07.md`.

This review supersedes the merge-readiness claim in `implementation-resume.md` ("All code complete. All tests pass. Code review matrix COMPLETE"). The February matrix was thorough on LWW-local resolution but under-covered the BFS comparison state machine and cross-event application ordering, which is where every confirmed bug lives.

## Methodology

Independent multi-model verification, blinded to all February artifacts (reviewers were instructed not to read `specs/concurrent-updates/reviews/` or the resume):

1. Two fresh-eyes Claude (Fable 5) reviewers: one on the event_dag algorithm core, one on entity/applier integration and the rebase seams.
2. One independent codex review (OpenAI GPT via codex-cli 0.142.5, read-only sandbox, same blinding).
3. A verification gate: every candidate finding was assigned to an adversarial verifier (Claude Opus 4.8) instructed to refute it, with a decisive unit test where possible. No finding below is reported on reviewer say-so; each has either a test or a quoted-code trace, and each is classified against origin/main to separate branch regressions from pre-existing behavior.

Verification tests currently live uncommitted in `core/src/event_dag/tests.rs` (also preserved in the stash "verification red-tests"). They are red by design; each should be committed together with its fix.

## Confirmed bugs (failing tests exist)

### V1. BFS revisit double-decrement produces false StrictDescends (CRITICAL, data loss)

- **Where:** `core/src/event_dag/comparison.rs` (frontier extension ~347-363, head decrement ~351-353, result checks ~413/423). `Frontier` (frontier.rs) is a bare BTreeSet; there is no per-side visited set, and `step()` only skips ids currently present in a frontier.
- **Mechanism:** a node reachable by two paths is processed twice from the same side; each visit re-runs the `original_comparison.contains(&id)` block, decrementing `unseen_comparison_heads` twice for the same head. The counter reaches zero while another comparison head was never visited, and `check_result` returns `StrictDescends` (mirror case: false `StrictAscends`).
- **Repro:** DAG `S->{A,B}, A->N, B->C, C->N, N->R, D->R`; subject `{S}` vs comparison `{N,D}`. Returns `StrictDescends` (correct: `DivergedSince{meet:[N]}`). Requires `id(N) < id(C)` for in-step ordering; the test brute-forces a seed, deterministic thereafter.
- **Damage:** `entity.rs` ~300-308 replaces the head wholesale, silently discarding concurrent tip `D`. Mirror case silently drops the incoming event.
- **Test:** `bfs_revisit_bugs::double_decrement_falsely_reports_strict_descends` (failing).
- Side effects fixed by the same root cause: budget is burned per path rather than per event (codex: diamond chains with O(n) events and exponential path counts can spuriously exhaust budget on a fully local DAG), and `StrictDescends.chain` / `DivergedSince` chains collect duplicates in non-causal order.

### V2. Late origin propagation yields an empty meet (HIGH)

- **Where:** `comparison.rs` ~303-312 (outstanding-head retirement) and the empty-meet gate ~434.
- **Mechanism:** a head is retired from `outstanding_heads` only at the instant a node first becomes common, using that moment's origin snapshot. If another head's origin arrives at that node later via a longer path, the `meet_candidates.insert == false` branch skips retirement; the head stays outstanding forever and the comparison is forced to `DivergedSince{meet: vec![]}` despite a real meet.
- **Repro (deterministic):** `C1->M, C2->X->M, S->M`; subject `{S}` vs comparison `{C1,C2}` returns meet `[]` (correct `[M]`).
- **Damage:** the merge path calls `into_layers(vec![], head)`, which re-layers the entire accumulated history from genesis, and the head-tip removal loop over the meet is a no-op, risking non-antichain heads.
- **Test:** `bfs_revisit_bugs::late_origin_propagation_yields_empty_meet` (failing).

### V3. Quick-check accepts a disjoint extra genesis root as StrictDescends (HIGH; found by codex)

- **Where:** `comparison.rs` ~83-103, the `comparison_set.is_subset(&all_parents)` shortcut.
- **Mechanism:** a parentless subject event contributes nothing to `all_parents`, so `[B, X]` vs `[A]` with `B.parent=[A]` and `X` an independent genesis passes the subset test and short-circuits before BFS. The full state machine, if reached, answers correctly (`DivergedSince{meet:[A]}`).
- **Damage:** `apply_state`/`apply_event` adopt the subject clock wholesale, importing the disjoint lineage without the merge concurrency requires. This defeats the branch's own "disjoint genesis detected by BFS" invariant precisely because BFS never runs.
- **Test:** `quick_check_disjoint_verify::test_quick_check_disjoint_extra_root` (failing).

### V4. StrictDescends gap-jump silently loses ancestor operations (CRITICAL, data loss)

- **Where:** `entity.rs` ~300-315 (the StrictDescends arm matches `{ .. }`, discarding `chain`, applies only the incoming event's operations, and jumps the head; there is no replay loop, unlike the DivergedSince arm) plus `node.rs` `collect_event_bridge` ~689-711 (backward BFS discovery order reversed is not a topological sort; the "already in causal order" comment at node_applier.rs ~284 is false).
- **Mechanism:** the EventBridge path stages all bridge events up front, then applies them in received order. A child applied before its staged parent compares `StrictDescends` (parents resolved via staging), gap-jumps the head, and applies only its own ops; the parent then returns `StrictAscends` and its operations are never applied to the backends, even though it is unconditionally committed to storage.
- **Repro:** head `{A}`; bridge delivers `[B, X]` where `X.parent=[A]` writes p1 and `B.parent=[X]` writes p2. Result: head `{B}`, p2 present, **p1 = None**, X durably stored. On a durable node the wrong state is persisted via `set_state` and served to peers as canonical.
- **Test:** `strict_descends_gap_jump` module (failing on the p1 assertion).

### V5. Clock sortedness is an unenforced load-bearing invariant (MEDIUM; February H2, confirmed and broadened)

- **Where:** `proto/src/clock.rs`. `contains`/`insert`/`remove` binary-search (~26-45); only `from_strings` sorts. `Clock::new`, `From<Vec<EventId>>`, the `Vec<Vec<u8>>` decode path, and the serde derive all trust input order.
- **Damage:** a peer-supplied unsorted clock silently breaks membership tests; codex adds a concrete downstream: `head.remove(meet ancestor)` can miss, leaving a redundant non-antichain tip.
- Independent cross-model convergence: found by my direct read and by codex separately.

### V6. EventOnly for an unknown entity poisons the whole update batch (MEDIUM-HIGH; branch regression)

- **Where:** `node_applier.rs` ~48 (`?` per item), `entity.rs` ~676-677 (`get_retrieve_or_create` inserts an empty-head resident entity), ~283-285 (new guard errors on non-creation event over empty head).
- **Vs main:** main had no empty-head guard; lineage returned NotDescends and main force-applied ("NotDescends - HACK"). The branch converts that into an error that aborts the remaining batch items, skips `notify_change`, and leaks a phantom empty entity in the WeakEntitySet.
- **Reachability:** a durable peer can legitimately send EventOnly for an entity the ephemeral never materialized (re-push after eviction; missed initial state).

### V7. Multi-entity commit atomicity regression (LOW; confirmed regression, damage proven inert)

- **Where:** `context.rs` commit_local_trx: the branch fuses `check_event` (~127) and `commit_event` (~131) per entity; main ran all checks before persisting anything.
- **Damage bound (verified):** an orphaned durable event is reachable only by explicit-ID `get_events`, never enumerated by state queries, never pointed to by any head, never relayed. Inert garbage. Fix is nearly free, so it is in the plan anyway.

## Refuted or reclassified (the gate working as intended)

- **February H1 (cross-layer LWW stale event_id): REFUTED as unreachable.** The mechanics are real when artificially constructed, but `entity.rs` only advances the head after applying an event's operations, so the stored per-property entry is always already the LWW winner over head-ancestry events touching it. The invariant is now pinned by the passing `lww_layer_tests` (three scenarios, including the artificial one demonstrating why the invariant matters). Optional defense-in-depth: guarded write-back of already-applied winners when they differ from stored; must not fire spurious field broadcasts.
- **Bridge "permanent history hole": REFUTED as a composite.** Both ingredients are real (a conditional-commit drop point exists in the SubscriptionUpdateItem paths; bridge ordering is non-topological) but they never connect: the EventBridge path commits unconditionally, `UpdateContent::EventOnly` is never constructed anywhere (dead receive path), and an ephemeral drop self-heals via `CachedEventGetter` peer refetch. Downgraded to hardening (commit verified events on `Ok(false)`; fix the stale comment).
- **Diverged snapshot "re-saves old state as corruption": CONFIRMED-PREEXISTING, harm bounded.** `DivergedRequiresEvents` collapses to `changed=false` and `save_state` re-attests identical local state (idempotent). The loss is a missed update (temporary staleness); the streaming StateAndEvent path already has the correct event fallback; main's NotDescends behavior was equivalent. Hardening: give the StateSnapshot/fetch paths a needs-events trigger.
- **Remote CommitTransaction ordering (event committed before apply/set_state): CONFIRMED-PREEXISTING.** Main's add_event/set_state ordering had the same window; orphans are content-addressed, idempotent, benign redundancy.
- **EventBridge skips validate_received_event (#244): PREEXISTING-UNCHANGED.** Identical gap at the same location on main. Three independent reviews converged on it, which supports the issue's priority, but it is not a branch blocker.

## Cross-model observations

- Codex and the Claude fresh reviewers independently converged on the missing-visited-set root cause, the Clock invariant, the partial-layer mutation window, and the commit-ordering windows; codex uniquely contributed V3 and the exponential-budget framing of V1.
- The February matrix's misses (V1-V4) share a signature: they require reasoning about revisit order, multi-path DAG shapes, and cross-event sequencing rather than single-function inspection. Future review prompts for this codebase should explicitly demand adversarial DAG construction.
- The rebase seams were independently verified clean: every `check_read` site matches main 1:1, `on_node_ready` is wired in both constructors, and getter collection/entity pairings are consistent.

## Hardening list (non-blocking, file as issues or fold into the fix PR where trivial)

1. Commit verified events on `Ok(false)` in the two conditional SubscriptionUpdateItem loops; correct the "already in causal order" comment.
2. Partial-layer application atomicity: an error mid-layers (including cross-backend: LWW succeeds, Yrs fails) leaves backends mutated under the old head; consider staging backend mutations or at least restoring head consistency on error.
3. Needs-events trigger for diverged StateSnapshot/fetch paths (see reclassified item above).
4. Budget check granularity (checked per step, not per event) and escalation restart cost; reassess after V1's fix removes revisit inflation.
5. Ephemeral creation-event re-delivery on large entities (fresh-review candidate, unverified): the empty-parent fall-through to full-ancestry BFS may exhaust budget; add a cheap `event_stored`/id-equality guard. Verify before or while fixing.
6. Empty-vs-empty clock comparison returns DivergedSince rather than Equal (low).
7. Layer frontier admits below-meet events as `already_applied` (benign for current LWW; violates the layers-start-at-meet contract).
8. Pre-existing tracked: #242, #243, #244, #245, #246, #247.
9. Documentation: the 11 itemized fixes in `implementation-resume.md`, plus event-dag.md caveats on `chain` reliability and the corrected bridge-ordering statement.
