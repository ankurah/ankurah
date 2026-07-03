# Remediation Session Prompt, 2026-07

You are picking up a fully-scoped remediation effort on ankurah PR #201 (`concurrent-updates-event-dag`). This document is the session bootstrap: everything operational you need is here or in the three sibling documents. Read those three before writing any code.

## Read first, in this order

1. `specs/concurrent-updates/remediation-2026-07.md` - the tracker/checklist. Work top to bottom; update checkboxes in the same commit as each fix.
2. `specs/concurrent-updates/fix-plan-2026-07.md` - the designs for each fix cluster, test strategy, sequencing, risk notes.
3. `specs/concurrent-updates/verification-review-2026-07.md` - the verified findings (V1-V7) with mechanisms, repro DAGs, and refuted-claims record. Trust these verdicts; they were adversarially verified with tests, unlike the February review matrix, whose merge-readiness claim is superseded.
4. Optional context: `implementation-resume.md` (February state, the Critical Invariants section still applies, and its "Doc Fixes Still TODO" list is a work item; its status header is stale).

## Environment map

- **Work here:** `/Users/daniel/ak/ankurah-201`, branch `concurrent-updates-event-dag`, remote `origin` = github.com/ankurah/ankurah, PR #201. Base: rebased onto main at 0.8.1 (July 2). Backup branch: `backup/event-dag-prerebase`.
- **Do NOT touch:** `/Users/daniel/ak/ankurah` (primary worktree; holds the unpushed local rebase of `feature/offline-first` / PR #252 plus a stash with a superseded wasm fix) and `/Users/daniel/ak/ankurah-jwt-auth` (on main/release duty). The git stash in ankurah-201 named "verification red-tests" is now redundant (tests were committed in `be3ad1bc`); it may be dropped.
- Toolchain notes: `taplo fmt` after ANY Cargo.toml edit (CI has a toml format check); `cargo fmt --all` before every commit; rustfmt config makes long single-line fn bodies normal here.

## Current state (verified at handoff)

- All suites green EXCEPT four intentionally-red verification tests committed with `#[ignore = "Vn: ..."]` in `core/src/event_dag/tests.rs`:
  - `bfs_revisit_bugs::double_decrement_falsely_reports_strict_descends` (V1, un-ignore with fix A1)
  - `bfs_revisit_bugs::late_origin_propagation_yields_empty_meet` (V2, fix A2)
  - `quick_check_disjoint_verify::test_quick_check_disjoint_extra_root` (V3, fix A3)
  - `strict_descends_gap_jump::test_strict_descends_gap_jump_skips_ancestor_ops` (V4, fix B1)
  - Also present and passing (do not break, not ignored): `lww_layer_tests::test_cross_layer_already_applied_winner_persistence`, which pins the invariant that refuted February's H1.
- Green baselines to preserve: core lib 176 passing (+ the new tests as they un-ignore), `cargo test -p ankurah-tests` 41 suites / 180 tests, `cargo test -p ankurah-jwt-auth` 15 suites / 88 tests.
- **A1 is mid-flight, uncommitted, in `core/src/event_dag/comparison.rs`.** The maintainer started it: struct fields are converted (counters replaced by `seen_comparison_heads`/`seen_subject_heads` sets; `subject_processed`/`comparison_processed` added; constructor updated; the subject-side extension guarded by `subject_processed.insert`). It does NOT compile: two references to the removed `unseen_comparison_heads`/`unseen_subject_heads` remain (run `cargo check -p ankurah-core` to locate; expect them in `check_result` and/or the comparison-side mirror branch). Finish it, do not restart it: convert the mirror branch (`from_subject == false`) symmetrically with `comparison_processed`, make `check_result` compare `seen_*_heads.len()` against the original head-set sizes, keep the doc comments the maintainer wrote, then un-ignore the V1 test and confirm it passes plus the whole `bfs_revisit_bugs` module behavior described in the tracker (add the mirror StrictAscends test and diamond-chain budget test per tracker A1).

## Execution order

Follow the tracker: finish A1 (in flight) -> A2 -> A3 (-> A4 optional, timeboxed) -> B1/B2 -> C1, C2, C3 -> docs -> validation gate -> PR refresh. B3 is explicitly a follow-up issue, not code in this effort. Details, invariants, and risks per item are in fix-plan-2026-07.md; do not re-derive them.

Key invariants that must survive (from implementation-resume.md, still authoritative):
- `stage_event` before head update (memory); `commit_event` before `set_state` (disk).
- `get_event` = staging + storage union; `event_stored` = storage only.
- No `E: Clone`; budget escalation (single 1x -> 4x) stays internal to `Comparison`; only the LRU cache survives retry.
- Never call `into_layers()` on `BudgetExceeded`.

## Test commands

- Run one ignored red test: `cargo test -p ankurah-core --lib <test_name> -- --ignored --exact <full::path::name>` (or temporarily remove the attribute while developing; the fix commit removes it permanently).
- Cluster gate: `cargo test -p ankurah-core --lib` then `cargo test -p ankurah-tests` then `cargo test -p ankurah-jwt-auth`.
- Final validation gate (all required): zero remaining `#[ignore]`s from this effort; the three suites above green; `cargo check -p ankurah-core --features wasm` green IN ISOLATION with `-p` (this exact invocation caught the 0.8.0 publish breakage; workspace builds mask feature-unification gaps); `cargo fmt --all -- --check`; `taplo fmt --check`.

## Conventions (maintainer-set, non-negotiable)

- Red-to-green: each fix commit includes its test's `#[ignore]` removal and names the test in the commit body. Update the tracker checkbox in the same commit.
- Commit messages: descriptive title plus an explanatory body (see `be3ad1bc` and `04b64290` for the house style). Never use em dashes in anything GitHub-visible: commit messages, PR descriptions, issue comments, docs. Scan before posting, e.g. `grep -cP '\x{2014}|\x{2013}' <file>`.
- Push to `origin/concurrent-updates-event-dag` after each cluster lands green. CI runs tests, rustfmt, taplo, and wasm browser tests.
- Comments in code state constraints the code cannot show; do not narrate changes or reference this remediation in code comments (commit messages and the tracker carry that).

## Completion checklist (beyond the tracker's gate)

1. Docs pass: the 11 items in `implementation-resume.md` "Doc Fixes Still TODO"; event-dag.md chain caveat and corrected bridge-ordering statement; update `implementation-resume.md` and `implementation-status.md` headers to point at the July verification and tracker.
2. PR #201 description refresh: reflect the rebase, the July verification, and the remediation (accurate test counts; no em dashes; note the version rides the planned 0.9.0 chain, wire compatibility across minors is not guaranteed).
3. File follow-up issues on merge: B3 (StrictDescends gap-replay defense, now unblocked by A1's trustworthy chains), and the hardening list items 1-7 of verification-review-2026-07.md that were not folded in (items already tracked: #242-#247).
4. Tell the maintainer these post-merge items are queued but NOT part of this effort: the factorization review pass over the branch he requested, and the #252 re-rebase onto post-#201 main (its local rebase in `/Users/daniel/ak/ankurah` will need redoing; when re-resolving `context.rs::get_entity`, `check_read` must be preserved on every read path, including the resident fast path that PR #252 adds).

## If something contradicts this document

The tracker and the code are ground truth for state; this document is ground truth for intent and environment. If a suite that should be green is red for reasons unrelated to your change, stop and report rather than fixing forward. If a fix reveals the design in fix-plan-2026-07.md is wrong, write the discrepancy into the tracker and choose the smallest correct alternative; the verification tests define correctness.
