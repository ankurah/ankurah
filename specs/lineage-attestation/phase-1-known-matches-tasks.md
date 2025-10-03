# Phase 1 Known Matches – Execution Tasks (Concise)

## Context (what’s already decided/implemented)

- Protocol: Fetch/SubscribeQuery include known_matches. Fetch and QuerySubscribed return Vec<EntityDelta> which represent the changes the server thinks we need.
- Server-side: Smart EntityDelta generation (omit if heads equal; else EventBridge; else StateSnapshot).
- Lineage gap: Comparison accumulates subject-side events; bridge built via local retriever; no max size.
- Client-side: NodeApplier consolidates remote update logic. LiveQuery uses version≥1; initialized_version: u32.
- Policy: Always apply remote responses locally; LiveQuery re-fetches from local storage to populate resultsets.

## Immediate Tasks (what to build next)

1. NodeApplier API

- Make apply_delta private; return Option<EntityChange> (entity + empty events for now).
- Add apply_deltas(deltas: Vec<EntityDelta>, ...):
  - Run per-delta applies concurrently.
  - Drain all Ready completions per wake into a batch, then call reactor.notify_change(batch).
  - Log-and-skip errors; do not abort the batch.

2. Client Relay integration

- Use apply_deltas for QuerySubscribed.deltas; then continue to overwrite the resultset.
- Initial subscription setup (EntityLiveQuery::initialize):
  - Call apply_deltas on the deltas from QuerySubscribed.
  - Then perform fetch_entities_from_local for the current selection.
  - Replace the LiveQuery resultset with the fetched entities (no initial ItemChange emission; the resultset is authoritative).
- Selection updates (update_selection_init): after apply_deltas completes, re-fetch from local with new predicate and overwrite the LiveQuery resultset.

3. LiveQuery behavior (unchanged for Phase 1)

- Emission policy:
  - Selection updates via reactor: emit ItemChange::Initial for new matches, ItemChange::Remove for removals.
  - Initial subscription setup: no ItemChange emission; resultset is populated directly and is authoritative.

4. Tests to fix/validate

- server_edits_subscription: initial entity should appear in client resultset post-init.
- test_view_field_subscriptions_with_query_lifecycle: correct Add/Update/Remove sequencing across selection changes.
- lineage budget exceeded current behavior (ensure errors map correctly; storage misses vs budget exceeded).

## Implementation Notes

- QuerySubscribed.deltas are not a full initial set; they represent what the server thinks we need based on known_matches. The deltas will most likely apply to records NOT currently in the query resultset, because they represent the predicate change about to happen (but which has NOT yet happened—the LiveQuery is still using the old predicate during apply_deltas).
- Resultset overwrite (after apply_deltas and fetch_entities_from_local) is the authoritative data source for the subscribing query. This happens post-init and post-selection-update.
- apply_deltas calls reactor.notify_change for drain-ready batches, which notifies ALL local subscriptions (including the one for the query in question). This is correct: the query hasn't changed predicates yet, so updates are faithfully applied to the old predicate. If deltas change the current resultset for that query under the old predicate, that's fine and correct.
- Phase 1 EntityChange.events can remain empty; membership logic does not require event lists.

## Open Discussions (to resolve after core wiring)

- Reactor.add_query correctness: re-screen entities using evaluate_predicate and implement gap filling logic similar to update_query. Changes may have been applied to Resident entities mid-fetch_entities_from_local (which is async), but the Resident Entities themselves are updated. This creates a potential race condition between fetching and entity updates.
- Cancellation: abort background gap-filling operations when superseded by new gap-fill requests or subsequent predicate/selection updates.
- Applied-events provenance: how to accumulate only actually-applied events into EntityChange (state head for StateSnapshot; exact saved events for EventBridge).
- Executor implementation: FuturesUnordered drain pattern - poll all remaining futures to gather ALL Ready futures (not stop at first Pending), then yield that batch. API ergonomics and backpressure considerations.
- Initial broadcast policy: formalize when/if add_query should emit initial updates vs rely on LiveQuery/local overwrite.

## ✅ **Implementation Status: FULLY COMPLETED**

All Phase 1 Known Matches tasks have been successfully implemented.

## ✅ **Quick Checklist - All Completed**

- [x] apply_delta private → Option<EntityChange> with empty events list
- [x] apply_deltas: parallelize, drain ALL Ready futures per wake, batch notify_change
- [x] Client relay: apply_deltas, then fetch_entities_from_local, then overwrite resultset
- [x] Lineage error handling verified (budget exceeded vs storage misses)

## 📋 **Tests Status**

- ✅ server_edits_subscription: Initial entity appears in client resultset post-init
- ✅ test_view_field_subscriptions_with_query_lifecycle: Correct Add/Update/Remove sequencing
- ✅ Lineage budget exceeded behavior: Proper error mapping implemented

## 🚀 **Phase 2 Path Forward**

The "Open Discussions" items identified below are Phase 2 considerations, not Phase 1 scope:

- Reactor.add_query correctness and re-screening logic
- Cancellation of background gap-filling operations
- Applied-events provenance in EntityChange
- Executor implementation details and backpressure
- Initial broadcast policy formalization

## 🎯 **Success Criteria Met**

- ✅ All existing tests pass without modification
- ✅ Network traffic reduced for subscriptions with known entities
- ✅ Event bridges used when delta is small (no size limits as planned)
- ✅ No changes to public API or breaking changes to protocol
- ✅ Clear path forward for Phase 2 (actual lineage attestation)
