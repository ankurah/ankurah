# Review Process & Plan Refinement — Resume Document

**Date:** 2026-02-05
**Repository:** ankurah/ankurah, PR #201 (`concurrent-updates-event-dag`)
**Worktree:** `/Users/daniel/ak/ankurah-201` (branch: `concurrent-updates-event-dag`)
**Main repo:** `/Users/daniel/ak/` (contains main repo and worktrees)

---

## What This PR Does

PR #201 replaces the old monolithic `lineage.rs` with a new `event_dag` module implementing:
- Backward BFS comparison algorithm for determining causal relationships between event clocks
- `AbstractCausalRelation` enum: Equal, StrictDescends, StrictAscends, DivergedSince, Disjoint, BudgetExceeded
- Layered concurrent event application model (`compute_layers`, `EventLayer`)
- Per-property LWW conflict resolution with causal comparison and lexicographic tiebreak
- TOCTOU retry loop for optimistic locking in `apply_event`

---

## Phase 1: Compartmentalized Multi-Agent Review (sessions 1–2)

### Methodology
A deliberately compartmentalized 2-phase, 2x4 review matrix designed to maximize independent insight and minimize groupthink.

**Phase 1 seed documents (2 background agents):**
- **1A (PR Digest):** Digest of all PR review comments across 5 rounds by multiple AI agents -> `review-digest-pr-comments.md`
- **1B (Holistic):** Independent code-only review, no PR context -> `review-holistic-code.md`

**Phase 2 matrix (8 independent agents):**

| | Algorithm+Invariants | LWW Register | Spec Conformance | Adversarial |
|---|---|---|---|---|
| **Row A** (PR digest seeded) | A1 | A2 | A3 | A4 |
| **Row B** (holistic seeded) | B1 | B2 | B3 | B4 |

Each agent was compartmentalized (A-row couldn't see holistic review; B-row couldn't see PR comments). Cross-cutting concern (asymmetric depth concurrency) injected into 6 of 8 agents.

**Output:** 8 review files (`review-matrix-{A,B}{1,2,3,4}.md`) + the 2 seed documents = 10 review files total.

### Metareview Synthesis

All 10 reviews were synthesized into `claude-metareview.md`. Key findings:

| # | Finding | Severity | Agreement |
|---|---|---|---|
| P0-1 | **InsufficientCausalInfo hard crash** — stored property event_id below meet not in accumulated events map | Critical | 7/8 |
| P0-2 | **Idempotency violated** — re-delivery of historical events creates spurious multi-head | Critical | 3/8 |
| P1-3 | **O(n^2 children_of** — linear scan per parent lookup in compute_layers | High | 7/8 |
| P1-4 | **Budget=100 too small** — no escalation on BudgetExceeded | High | 6/8 |
| P1-5 | **Entity creation race** — two creation events corrupt entity permanently | Medium | 1/8 |
| P2-7 | **build_forward_chain incorrect** for multi-meet DAGs (currently harmless) | Medium | 5/8 |
| P2-8 | **Backend misses earlier layers** — backend created at Layer N skips 1..N-1 | Medium-High | 1/8 |

Positive: Core BFS algorithm sound (8/8), cross-replica convergence correct (8/8), module decomposition excellent (8/8).

**Verdict:** Do not merge until InsufficientCausalInfo fixed + idempotency pre-check added.

---

## Phase 2: EventAccumulator Plan Review & Revision (session 3)

### Context
Commit `4bb9f34a` introduced an EventAccumulator refactor plan (`event-accumulator-refactor-plan.md` + `event-accumulator-research.md`). The plan proposed replacing `AccumulatingNavigator`, `CausalNavigator`, and `staged_events` with a cleaner `EventAccumulator` / `ComparisonResult` / `EventLayers` architecture.

10 incremental commits followed the plan but implemented **none of the planned refactor**. Instead they made targeted fixes: added `EventLayer.compare()`, `CausalRelation`, `is_descendant`, LWW causal resolution, three-state `ValueEntry`, and `InsufficientCausalInfo` error type. The fundamental P0 bug remained (now an explicit error instead of panic, but still rejects legitimate events).

### Plan review against metareview
We (Claude) reviewed whether the plan would address the metareview findings. Conclusion: **the plan was an architectural refactor, not a bug-fix plan**. It enabled the P0-1 fix but didn't specify the mechanism. It didn't address P0-2, P1-3, P1-4, P1-5, P2-8 at all. This analysis was written to `plan-review-and-recommendations.md`.

### Codex spec-additions comparison
Codex independently produced `spec-additions-codex.md` (8 recommended spec changes). We compared the two documents:

**Key divergence on P0-1 fix:**
- **Claude's approach:** Retriever-backed `is_descendant` fallback (async, architectural)
- **Codex approach:** Semantic rule — if stored event_id absent from accumulated set, it's provably below the meet, so it always loses. Simple, sync, no fetch needed.
- **Resolution:** Codex's approach is correct and simpler. The BFS accumulates everything above the meet; absence proves the event is below it.

**Combined best ideas:**
- Codex section 1 (older-than-meet rule) — adopted as the InsufficientCausalInfo fix
- Both: idempotency pre-check, budget increase, children index
- Codex section 4 (seeded events first-class), section 5 (StrictDescends causal delivery), section 7 (StateApplyResult enum), section 8 (API docs)
- Claude additions: entity creation race guard, backend layer replay, test coverage

### Revised plan
The combined insights were written into the revised `event-accumulator-refactor-plan.md`. Major additions:
- "Behavioral Rules" section (5 rules: older-than-meet, idempotency, causal delivery, creation uniqueness, mixed-parent spanning meet)
- `EventLayer` carries `dag: Arc<BTreeMap<Id, Vec<Id>>>` (parent pointers) instead of full events
- `EventLayers` pre-builds parent->children index, generalized topological-sort frontier seed
- Budget escalation internal to `Comparison` (no `R: Clone` requirement)
- `apply_event` sketch with idempotency guard, creation guard, backend replay
- `StateApplyResult` enum replacing `bool`
- LWW `apply_layer` sketch with `dag_contains()` / `older_than_meet` flag
- Eager event storage model (no seeded events, no store_event)
- Migration phases expanded with correctness fixes and 8 specific test cases
- All 12 design questions resolved (no open questions remain)

---

## Phase 3: Iterative Review & Refinement (session 4 — current)

### Codex nits (round 2) — all applied
4 documentation nits from `plan-review-codex-followup.md`:
1. DAG completeness invariant on `EventLayers::new()` — added precondition doc
2. `compare()` precondition — tightened contract (both IDs must be in DAG)
3. Budget escalation = fresh comparison — clarified in code sketch
4. `compute_ancestry_from_dag` scoping — documented as intentional
5. Rename `current_frontier` → `layer_frontier` — applied

### Mixed-parent spanning meet (correctness fix)
Codex identified that merge events with parents from non-meet branches stall forever. Fix: generalized frontier initialization (topological-sort seed from all ready events, not just children of meet) + parent-readiness check ignores out-of-DAG parents. Added behavioral rule and test case.

### Layer semantics wording
Updated "same causal depth from meet" → "no in-DAG ancestor/descendant relation within a layer" to reflect the generalized frontier.

### `is_descendant_dag` DAG boundary fix
Final review (comprehensive agent) identified that `is_descendant_dag` would error when backward traversal reaches parents below the meet. Fixed: uses `if let Some` / `else { continue }` pattern (dead end, not error). `compare()` now returns `CausalRelation` directly (no `Result`).

### Budget escalation moved inside `Comparison`
Codex recommended moving budget escalation from caller-side (requiring `R: Clone`) to internal to `Comparison` (reset traversal state, retain accumulator cache). Eliminates `R: Clone` from all public APIs. `EphemeralNodeRetriever` (lifetime borrows) now compatible without restructuring.

### Eager event storage
Daniel questioned why `store_event` exists on `Retrieve` when events are already stored on receipt. Analysis + Codex collaboration identified a correctness pitfall with lazy storage: LRU cache eviction could drop unstored event bodies before persistence. Resolution: adopt eager storage model. Removed `seeded` map, `seed()`, `get_and_store_event()`, `store_event` from plan. `EventAccumulator` simplified to DAG tracker + read-through LRU cache.

Key invariant: `Retrieve::get_event` guarantees the returned event is in local storage (either already there, or fetched from peer and stored before returning). Locally created events don't need to be stored before comparison — the BFS walks backward through the event's parents (already stored), never the event itself.

`event_exists` should preferably be a storage-level `has_event(id)` rather than a wrapper over `get_event`, to avoid the fetch-and-store side-effect of `EphemeralNodeRetriever::get_event`.

### PR comment cleanup
Deleted 10 outdated line-level review comments and 17 outdated issue comments. Kept 1 (EventAccumulator plan announcement). 9 review submission objects remain (can't be deleted via API).

---

## Current State of Files

### Review artifacts (read-only, reference)
- `specs/concurrent-updates/review-digest-pr-comments.md` — Phase 1A output
- `specs/concurrent-updates/review-holistic-code.md` — Phase 1B output
- `specs/concurrent-updates/review-matrix-{A,B}{1,2,3,4}.md` — Phase 2 matrix (8 files)
- `specs/concurrent-updates/review-matrix-prompts.md` — Agent prompts used
- `specs/concurrent-updates/claude-metareview.md` — Synthesized metareview
- `specs/concurrent-updates/spec-additions-codex.md` — Codex's independent spec recommendations
- `specs/concurrent-updates/plan-review-and-recommendations.md` — Claude's initial plan review (pre-Codex-merge)
- `specs/concurrent-updates/plan-review-codex-followup.md` — Codex's second-pass review with 4 remaining nits
- `specs/concurrent-updates/codex-review-response.md` — Response to Codex re-review (mixed-parent fix)
- `specs/concurrent-updates/final-plan-review.md` — Comprehensive final review report
- `specs/concurrent-updates/event-accumulator-research.md` — Supporting research

### Active plan (revised, current)
- **`specs/concurrent-updates/event-accumulator-refactor-plan.md`** — The plan incorporating all review feedback across 4 sessions. This is the document to implement from. Codex and Claude have confirmed no remaining correctness issues.

### Spec documents
- `specs/concurrent-updates/spec.md` — Authoritative feature spec (needs updates per plan's "Spec Alignment Notes" section)
- `specs/concurrent-updates/lww-causal-register-fix.md` — LWW fix spec (partially implemented by incremental commits, superseded by the revised plan)

### Key source files
- `core/src/event_dag/comparison.rs` — BFS comparison (613 lines)
- `core/src/event_dag/layers.rs` — Layer computation + EventLayer.compare() (465 lines)
- `core/src/event_dag/navigator.rs` — CausalNavigator + AccumulatingNavigator (149 lines, to be removed)
- `core/src/entity.rs` — apply_event, apply_state (694 lines)
- `core/src/property/backend/lww.rs` — LWW backend with apply_layer (316 lines)
- `core/src/retrieval.rs` — Retrieve trait, EphemeralNodeRetriever
- `core/src/event_dag/mod.rs` — Module exports
- `core/src/event_dag/relation.rs` — AbstractCausalRelation enum
- `core/src/event_dag/frontier.rs` — Frontier management
- `core/src/event_dag/traits.rs` — Abstract types (EventId, TEvent, TClock)
- `core/src/event_dag/tests.rs` — Unit tests (1801 lines)
- `core/src/node_applier.rs` — StateAndEvent handling

---

## What Comes Next

The plan is ready for implementation. No open design questions remain. The migration path has 4 phases:

1. **Phase 1:** Add new types (EventAccumulator, EventLayers, ComparisonResult, StateApplyResult, `get_event` + `event_exists` on Retrieve trait, `lru` crate)
2. **Phase 2:** Modify Comparison internals (embed EventAccumulator, return ComparisonResult, internal budget escalation) + increase budget to 1000
3. **Phase 3:** Update callers + all correctness fixes (idempotency guard, creation race guard, backend replay across layers, LWW older-than-meet rule, StateApplyResult enum)
4. **Phase 4:** Cleanup (remove AccumulatingNavigator, CausalNavigator, staged_events, standalone compute_layers/compute_ancestry) + 8 specific test cases + Retrieve trait scope assessment

The next step is implementation.
