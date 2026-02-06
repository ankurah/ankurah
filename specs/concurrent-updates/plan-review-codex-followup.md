# Plan Re‑Review (Codex Follow‑Up)

**Target:** `specs/concurrent-updates/event-accumulator-refactor-plan.md` (Claude’s updated version)

## Overall Assessment

The revised plan is **internally coherent** and now directly addresses the major correctness gaps previously noted (LWW missing‑ancestor, idempotency, creation race, budget escalation, children index). It is implementable as written and largely elegant.

## What’s Improved / Solid

- **EventLayer now carries DAG only**, keeping `compare()` sync and avoiding full event cloning.
- **Frontier expansion fixed** to use only the current frontier (no O(n²) scan).
- **R: Clone** for retriever is documented and used for budget escalation.
- **Older‑than‑meet rule** is explicitly embedded in LWW and documented.

## Remaining Nits / Clarifications

1. **DAG completeness invariant**
   - `EventLayers` snapshots `accumulator.dag` and does not extend it during iteration.
   - This is fine because the frontier is derived strictly from `dag`, but it should be explicit: *all layer events must already be in `dag` (i.e., all events from heads to meet are accumulated).* Add one sentence noting this invariant.

2. **EventLayer.compare precondition**
   - `is_descendant_dag` errors if an id is missing. The plan relies on LWW checking `dag_contains()` first.
   - Add a short note: *Callers must only call `compare()` when both ids are in the DAG.*

3. **Budget escalation semantics**
   - Retries should be described as a **fresh comparison** with a new accumulator. The current example implies this but doesn’t say it explicitly.

4. **Ancestry computation scope**
   - `compute_ancestry_from_dag` only knows the accumulated DAG. Add a short note that ancestry is scoped to the comparison DAG by design.

## Optional Elegance Tweaks

- Consider a helper on `EventLayer` such as `compare_or_assume_older(stored_id, candidate_id)` to centralize the “older‑than‑meet” rule and reduce misuse in backends.
- Small readability: rename `current_frontier` to `layer_frontier` in `EventLayers::next()`.

## Any Missing Correctness Issues?

No new blockers identified. The plan now covers the previously critical issues (LWW missing‑ancestor failures, staged/seeded visibility, idempotency, creation uniqueness, budget escalation, children index). The only remaining uncertainty is whether backend replay across layers is strictly necessary (plan includes it; keep if backend types can appear first on concurrent branches).
