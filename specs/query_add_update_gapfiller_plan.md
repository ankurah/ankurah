### Query add/update + gap filling plan (concise)

Goals

- Deterministic, single notification per logical change.
- No redundant server-side Reactor notifications; client derives changes from its own emitting path.
- Gap filling runs predictably and completes before notifying.

Scope (this step)

- Gap filling factorization.
- `add_query` and `add_query_and_notify` behavior.
- `update_query` and `update_query_and_notify` behavior.
- Do NOT change resultset sequencing/serialization yet (see TODO).

Locking discipline

- Use std locks for Reactor’s internal maps; hold briefly and NEVER across await.
- Snapshot per-query inputs under the subscriptions lock, then drop the lock before async work.
- Evaluate predicates and detect removals/additions OUTSIDE the subscriptions lock; only take `resultset.write()` for the brief, final commit.
- Only take `resultset.write()` for the brief, final commit of mutations.
- Clone the `broadcast` under lock (if a notification will be sent), then drop lock and send later.

Single-query gap fill (helper)

- Free helper (module-private), not a Reactor/SubscriptionState method:
  - Inputs: `resultset`, `gap_fetcher`, `collection_id`, `selection`, `last_entity`, `gap_size`.
  - Fetch with `gap_fetcher.fetch_gap(...)` (await), then `resultset.write().add(...)` new entities.
  - Returns `Vec<Entity>` that were actually added.
- When planning a gap fill under lock, compute `gap_size` only when needed:
  - `limit.is_some()` AND `current_len < limit` AND (for update: removals occurred; for add: initial fetch didn’t fill limit).

add_query (state-only)

- Under subscriptions lock:
  - Insert `QueryState` (empty `resultset`, store `gap_fetcher`, `selection`, `version=0`).
  - (Preferred) Immediately invoke `update_query` pipeline (see implementation note below).
  - Clone `broadcast` only if the emitting variant will be used.
- Outside lock:
  - Defer all fetching/evaluation to `update_query` pipeline.

Implementation note: add_query as wrapper over update_query

- For correctness and efficiency, prefer implementing `add_query` as:
  - Register `QueryState` (empty `resultset`, set `selection`, store `gap_fetcher`, `version=0`).
  - Immediately invoke the same mutation pipeline as `update_query` (state-only for server, emitting for LiveQuery):
    - Full refetch for the predicate
    - Initial-case handling when `version==0`: skip watcher removals, no removes expected; only additions
    - Optional gap fill if under limit
    - Single consolidated notify for emitting variant
- Benefits: single fetch, one unified codepath, consistent watcher setup and `did_match` semantics.

add_query_and_notify (emitting)

- Call state-only `add_query` to fully populate `resultset`.
- Build `ReactorUpdateItem { Initial }` for all entities returned.
- Send a single `ReactorUpdate` via cloned `broadcast`.

update_query (state-only)

- Under subscriptions lock:
  - Read old selection; set new `selection`/`order_by`/`limit`; `mark_all_dirty()`.
  - If `version==0` (initial): treat as first activation; skip watcher removals logic.
  - Fetch full set from local storage for NEW predicate.
  - Process includes: add if not present.
  - Process removes: `retain_dirty` with `evaluate_predicate`; track if any removals.
  - If `limit.is_some()` AND `current_len < limit` AND removals occurred → plan gap fill (snapshot inputs).
  - Drop lock.
- Outside lock:
  - If planned, run single-query gap fill helper; commit via `resultset.write()`.
  - Return `Vec<Entity>` of final entities (if needed by caller, e.g., server).
- Note: state-only path does not notify.

update_query_and_notify (emitting)

- Run `update_query` (state-only).
- Build a single consolidated `ReactorUpdate` that includes:
  - `Remove` items computed during `retain_dirty`.
  - `Initial` items for newly added entities.
  - If gap fill occurred, append gap-filled entities to the `Initial` list.
- Send via cloned `broadcast`.

Server path (remote subscription)

- Replace existing server calls with state-only variants:
  - v1: `add_query` (no notify).
  - v>1: `update_query` (no notify).
- Server computes `QuerySubscribed` deltas independently and returns them; no Reactor notifications are emitted server-side for subscribe/update.

Client path (EntityLiveQuery)

- v1: call `add_query_and_notify`.
- v>1: call `update_query_and_notify`.
- LiveQuery no longer fetches entities itself; Reactor performs fetch internally for add/update.

Emitting variants: mapping gap-filled entities to ReactorUpdate

- add_query_and_notify:

  - `add_query` returns the final entity set placed into the resultset (initial fetch + gap-filled tail).
  - Build `ReactorUpdateItem { Initial }` for each of these entities.
  - Send a single consolidated update (no `Remove` items expected since the query was empty initially).

- update_query_and_notify:
  - During the state-only phase, collect `removed_entities` and `newly_added_entities`.
  - If gap fill occurs, append the gap-filled entities to `newly_added_entities`.
  - Build one `ReactorUpdate` with `Remove` for removed and `Initial` for newly added (including gap-filled).
  - Send exactly once after committing all changes to the resultset.

Multi-query gap fill (notify_change)

- Keep multi-query gap fill on `SubscriptionState`:
  - Under lock: collect all gap plans (per-query), clear `gap_dirty`, clone `broadcast`, drop lock.
  - Outside: run per-query fills concurrently; each fill commits via `resultset.write().add(...)`.
  - Send one consolidated `ReactorUpdate` via cloned `broadcast`.
- Prevent redundant work via early `gap_dirty` clearing; per-query serialization is out-of-scope for now.

Race handling

- Eliminating server Reactor notifications removes request/response vs. reactor-update races.
- Emitting variants send exactly one notification after gap fill completes.

## Implementation Status: ✅ COMPLETED

This plan has been successfully implemented as part of the Reactor hot path refactoring.

**Completed components:**

- ✅ Gap filling factorization with per-query helpers
- ✅ State-only (`add_query`/`update_query`) and emitting (`add_query_and_notify`/`update_query_and_notify`) variants
- ✅ Server path uses state-only variants (no redundant notifications)
- ✅ Client path uses emitting variants with single consolidated notifications
- ✅ Proper locking discipline with serialization locks in SubscriptionState
- ✅ Gap filling runs predictably before notifications
- ✅ Race handling for request/response vs reactor-update timing

**Related issues:**

- The resultset sequencing/serialization TODO was completed during implementation (moved gap filling and notification into SubscriptionState with serialization locks)
- Network-level SubscriptionUpdate vs QuerySubscribed timing concerns are tracked in [Issue #147](https://github.com/ankurah/ankurah/issues/147)
