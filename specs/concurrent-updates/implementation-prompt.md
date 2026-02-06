# EventAccumulator Refactor — Implementation Prompt

**Worktree:** `/Users/daniel/ak/ankurah-201` (branch: `concurrent-updates-event-dag`)

---

## Your Role

You are the supervisor agent. You coordinate implementation across 4 sequential phases, dispatch code work to sub-agents, run validation gates between phases, and preserve your own context for decision-making. You do not write code directly.

## The Plan

`specs/concurrent-updates/event-accumulator-refactor-plan.md` — read this first. It is the authoritative implementation spec. It contains full code sketches, type definitions, behavioral rules, migration phases with checklists, and resolved design questions. Everything you need to implement is in this document. The plan was refined across 4 review sessions by Claude and Codex with no remaining open questions.

Do not read any other spec files unless a specific task requires it (the adversarial validation in Phase 4 will need `specs/concurrent-updates/claude-metareview.md`).

---

## Current Code Starting Point

The plan describes the target. This is what exists today — the delta you're working from.

**`core/src/event_dag/comparison.rs` (613 lines)**
- `compare<N: CausalNavigator>(navigator, subject, comparison, budget) → Result<AbstractCausalRelation>`
- `compare_unstored_event<N: CausalNavigator>(navigator, event, comparison, budget) → Result<AbstractCausalRelation>`
- Private `Comparison<'a, N>` state machine: borrows `navigator: &'a N`, calls `navigator.expand_frontier()` to batch-fetch events
- Assertions system exists but always returns empty (dead code)

**`core/src/event_dag/navigator.rs` (149 lines)** — to be removed entirely
- `CausalNavigator` trait: `expand_frontier(&self, ids, budget) → NavigationStep { events, assertions, consumed_budget }`
- `AccumulatingNavigator<N>`: wraps navigator, intercepts events into `RwLock<BTreeMap>`

**`core/src/event_dag/layers.rs` (465 lines)**
- `compute_layers(events: &BTreeMap, meet, ancestry) → Vec<EventLayer>` — standalone function, takes pre-collected events
- `children_of(events, parent)` — **O(n) linear scan** per call
- `EventLayer` has `events: Arc<BTreeMap<Id, E>>` (full event clones), `.compare()` returns `Result<CausalRelation>`

**`core/src/entity.rs` (694 lines)**
- `apply_event<G: CausalNavigator>` — creates `AccumulatingNavigator` per retry attempt, calls `compare_unstored_event`, extracts events via `acc.into_events()`, passes to `compute_layers`
- `apply_state<G: CausalNavigator>` — returns `bool`

**`core/src/property/backend/lww.rs` (316 lines)**
- `apply_layer` seeds winners from stored `Committed{value, event_id}` values, uses `layer.compare()` for causal resolution
- No handling for stored event_id absent from the DAG (the P0-1 bug)

**`core/src/retrieval.rs`**
- `Retrieve` trait has only `get_state()` — no `get_event`, no `event_exists`
- `EphemeralNodeRetriever` implements `CausalNavigator` (expand_frontier with staged→local→remote fallback) and has `staged_events: Mutex<Option<HashMap>>`

**`core/src/error.rs`** — `MutationError` exists, no `DuplicateCreation` variant
**`core/Cargo.toml`** — no `lru` crate

---

## Phase Execution

The plan defines 4 phases with per-phase checklists. Follow them. What's below is the coordination layer on top — how to dispatch, validate, and sequence.

### Phase 1: Add New Types (additive, non-breaking)

**Parallelization:** The Retrieve trait changes, the accumulator module, and the error enum additions are independent — dispatch up to 3 agents in parallel.

**Gate:** `cargo check && cargo test` — all existing tests must still pass. Grep for existing `compare(`, `apply_event(`, `apply_layer(` call sites and confirm none were modified.

### Phase 2: Comparison Rewrite

**Do not split this across agents.** This is a single coherent change to comparison.rs — the BFS loop restructuring (batch `expand_frontier` → per-event `get_event`), accumulator integration, `ComparisonResult` return type, and budget escalation are all interdependent.

**Key structural change:** The current BFS calls `navigator.expand_frontier(&all_frontier_ids, budget)` which returns multiple events at once. The new code calls `accumulator.get_event(id)` per frontier node. The assertions system is dropped entirely (always returned empty). This requires restructuring `Comparison::step()`.

**Gate:** `cargo check && cargo test` with comparison tests adapted to new signatures. Verify `ComparisonResult.relation` matches old return values for every existing test case.

### Phase 3: Callers + Correctness Fixes

**Parallelization:** entity.rs changes and lww.rs changes are somewhat independent — both consume the new EventLayer API but don't depend on each other.

**Gate:** `cargo check && cargo test`, then launch a **spec alignment agent** that reads the plan's "Behavioral Rules" section and the modified source, and maps each rule to exact file:line. Any rule without a clear implementation is a blocker.

### Phase 4: Cleanup + Tests

**Gate:** `cargo check && cargo test` with all 8 test cases from the plan passing. Grep to confirm no references to removed types. Then launch an **adversarial agent** that reads `specs/concurrent-updates/claude-metareview.md` and the final source files, and verifies each P0/P1/P2 finding is addressed.

---

## Invariants

These are the things most likely to go wrong. Keep them visible.

1. **No `R: Clone`.** Budget escalation is internal to `Comparison` — the retriever stays owned by the accumulator. This is critical for `EphemeralNodeRetriever` which holds lifetime borrows.

2. **Don't alter BFS traversal logic.** The core algorithm (frontier expansion, meet detection, state tracking) was validated correct by 8/8 independent reviewers. You're changing the retrieval plumbing underneath it, not the algorithm itself.

3. **Out-of-DAG parents are dead ends, not errors.** In `EventLayers` frontier computation and `is_descendant_dag`, use `if let Some` / `else { continue }`. Never panic or return error for missing parents — they're below the meet.

4. **Idempotency and creation guards go before the retry loop** in `apply_event`, not inside it.

5. **Never call `into_layers()` on `BudgetExceeded`.** The DAG is incomplete.

6. **Follow the plan's code sketches when stuck.** They were iterated across 4 review sessions and handle edge cases you might not anticipate. The sketches are more specific than typical pseudocode.
