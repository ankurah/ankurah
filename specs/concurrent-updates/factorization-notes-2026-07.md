# Factorization Notes, 2026-07

Improvement candidates collected while executing the July remediation and
writing the ankurah.org concurrency book section. Input to the post-merge
factorization review pass; nothing here blocks the merge of #201. Items
overlapping the verification review's hardening list are marked (H-n).

## Understandability and SOLID

1. **Symmetric-side duplication in `comparison.rs`.** `Comparison` carries
   every structure twice (frontier, processed set, seen-heads set, visited
   list, root), and `process_event`/`check_result` repeat each branch per
   side. A `Side` struct with two instances would halve the branching and
   structurally prevent asymmetric drift, which is exactly V1's bug class
   (head accounting diverged between sides).
2. **Origins machinery is now a fast path only.** With the exhaustion-time
   reachability reconciliation as ground truth (A2), `NodeState.origins`
   (a `Vec<EventId>` cloned on every processed event) and its two retirement
   sites are removable: `outstanding_heads` could be computed once at
   exhaustion by reachability. Deletes a per-event allocation and the entire
   V2-class incremental bookkeeping.
3. **If origins stay, dedupe them.** `origins.extend(...)` accumulates
   duplicates today; a set or dedup-on-extend bounds it.
4. **Quick check duplicates adoption semantics.** The one-step shortcut in
   `compare()` and the grounded StrictDescends completion in `check_result`
   are two expressions of the same soundness rule, and they drifted apart
   once already (V3). Recasting the quick check as a bounded first BFS step
   would leave a single source of truth.
5. **Dead `pn_counter.rs`.** The module is commented out of
   `property/backend/mod.rs`, its `property_backend_name` signature no longer
   matches the trait (`String` vs `&'static str`), and the registry contains
   a commented-out branch for it. Delete or revive; as-is it misleads
   readers.
6. **`ordering.rs` returns `MutationError`.** event_dag is otherwise
   application-agnostic (verdicts and layers out, `GetEvents` in). A small
   ordering error type mapped at the call sites would keep the layering
   clean.
7. **`EventLayers::next` duplicates its readiness predicate.** The
   "all in-dag parents processed" check appears in both frontier seeding and
   frontier advancement; extract it.
8. **Hand-rolled clock normalization in decode paths.** `proto/src/wasm.rs`
   and `proto/src/postgres.rs` re-implement sorted insertion (postgres calls
   `EventId::from_sql` three times per element). Route both through
   `Clock::from` now that construction normalizes (C1).

## Reliability

9. **(H) Disjoint verdict ignores `any_common`.** For a comparison head set
   mixing shared-lineage and foreign heads, `check_result` returns Disjoint
   or `DivergedSince { meet: [] }` depending on which genesis is recorded as
   `other_root` first. Tighten Disjoint to require `!any_common`; a verdict
   claiming disjointness while common history exists misclassifies a
   mergeable update as an identity mismatch. The property-test oracle
   currently accepts either verdict for this shape and can be tightened in
   the same change.
10. **Duplicate apply loops in `node_applier`.** The EventOnly arm and the
    StateAndEvent fallback arm carry near-identical apply/commit/collect
    loops. Extracting a shared helper would also give StateAndEvent the same
    partial-progress containment semantics C3 added to EventOnly.
11. **Duplicate staging-map logic in retrieval getters.**
    `LocalEventGetter` and `CachedEventGetter` each own an identical staging
    map and stage/commit implementation; extract a `StagingMap` component and
    compose.
12. **B3 remains the known reliability follow-up.** The StrictDescends arm
    in `entity.rs` has no gap-replay defense for staged-but-unapplied
    ancestors. A1 made chains duplicate-free, which unblocks building it (or
    reconstruct the replay set via parent walk). Tracked for an issue at
    merge.
13. **Error taxonomy.** `MutationError` mixes typed variants with
    `General(String)` and `InvalidUpdate(&'static str)`; callers cannot
    reliably distinguish policy, lineage, and storage failures.
    `ApplyError::Items` is a good pattern to extend toward typed causes.

## Efficiency

14. **Grounding recheck cost.** Once seen-complete but ungrounded,
    `check_result` recomputes the comparison-heads ancestry every step.
    Budget-bounded, but caching the ancestry and extending it as the dag
    grows (or rechecking only when it grows) removes repeated walks.
15. **DivergedSince collects all layers before applying.** `entity.rs`
    buffers every layer into a Vec and applies under the write lock;
    memory is O(explored history). Streaming layers under the existing
    head-recheck discipline is plausible but needs care with lock scope.
16. **Accumulator memory duplication.** `EventAccumulator` stores parent
    edges in `dag` and full `Event` clones in the LRU; parent vectors exist
    twice for cached events. Measure first; candidates are `Arc<Event>` in
    the cache or deriving dag reads from cached events.
17. **(H4) Budget granularity.** Budget is checked per step rather than per
    event within a step, and escalation restarts spend from zero on retry.
    Reassess now that A1 removed revisit inflation.
18. **`step()` snapshot-and-recheck.** Collecting `all_frontier_ids` then
    re-checking frontier membership per id works but obscures the
    both-frontiers case; draining per frontier deterministically would
    remove the mid-loop skip and clarify processing order.

## Additions from the 2026-07-03 diff-weight audit

Parallel audits over the full branch diff (three primary agents plus
slices) confirmed proto/, the manifests, and the bulk of core/ carry no
churn. Safe trims were applied directly on the PR branch (dead
re-exports, an uncalled accumulator method, duplicated test helper,
subsumed and duplicate tests, debug leftovers; see commit "Trim diff
weight found by the readability audit"). Items needing judgment or
deferred to this pass:

19. **EventLayer visibility leak.** `PropertyBackend::apply_layer` is a
    pub trait method taking `&EventLayer`, but `EventLayer` is
    pub(crate); the compiler warns private_interfaces at the trait and
    both impls. Decide: make EventLayer (and enough of its surface)
    genuinely public so external crates can implement PropertyBackend,
    or narrow the trait's reachability.
20. **ValueEntry::Pending is behaviorally inert.** Constructed in two
    places but value()/event_id() fold it into the same arms as
    Uncommitted/Committed, so it acts as a rename. Confirm whether it
    is scaffolding for offline writes (#252) or collapse the enum.
21. **Notification-loop duplication.** The lock-broadcasts/iterate/send
    loop appears four times across lww.rs and yrs.rs; extract a helper
    taking an iterator of PropertyName.
22. **node_applier validate+stage loop duplicated** verbatim in the
    EventOnly and StateAndEvent arms (the branch deleted the shared
    save_events helper); re-extract, which also unifies partial-progress
    semantics.
23. **transaction.rs commit wrappers** (js/native/uniffi) triplicate a
    two-line body discarding commit_local_trx's Vec<Event>; consider one
    inner fn.
24. **False positive to remember:** the `use std::collections::HashMap`
    inside the assert_dag! macro body is load-bearing (macro_export
    expands at call sites in other files); do not "clean" it.

## Status stamp, 2026-07-04

RFC issues #265 (comparison core), #266 (indexed causality), #267
(backend contract), #268 (ingest pipeline) now carry the strategic
items. Landed directly in PR #201 before merge: items 1 and 2 (Side
type; origins machinery removed, reachability-only accounting), item 9
(Disjoint requires no shared history; oracle pins one verdict), item
19 provisionally (EventLayer pub with crate-internal surface), item 21
(notify_changed_fields helper), item 22 (validate_and_stage helper).
Still open here or in the RFCs: items 4-8, 10-18, 20, 23.
