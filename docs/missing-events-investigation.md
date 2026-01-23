Missing Events: Investigation Notes
==================================

Goal
----
Improve telemetry for the error:
`Mutation error: retrieval error: Storage error: Events not found`
so we can pinpoint the exact path and conditions when it happens in the wild,
then evaluate fallback behavior (retries / relaxed apply).

Current understanding / hypotheses
---------------------------------
- The error originates in lineage comparison when required parent events are not
  present in local storage or staged events, and `GetEvents` retrieval fails or
  returns incomplete results.
- Snapshot delivery and event retrieval are separate RPC paths:
  `SubscribeQuery`/`Fetch` returns `StateSnapshot` inline, while lineage compare
  later triggers `GetEvents`. These can fail independently even if “back-to-back.”
- `known_matches` can be empty even if state exists (predicate scan misses it,
  state not indexed, or timing around persistence).
- EventBridge is only sent when `known_matches` are present and
  `collect_event_bridge` succeeds; otherwise the server falls back to snapshot.

Progress to date
---------------
- Added a repro test that forces missing-event lineage failures:
  `tests/tests/rt_missing_events.rs`
  - Uses LocalProcessConnector transforms to strip `known_matches` on
    `SubscribeQuery` and return empty `GetEvents` responses.
  - Reproduces the “Events not found” error deterministically.
- Added error context wrapping:
  - `core/src/error.rs`: `RetrievalError::Context`, `with_context`, `root`.
  - `core/src/entity.rs`: wraps lineage compare errors with
    `Entity::apply_event compare_unstored_event` and
    `Entity::apply_state compare`.
  - `core/src/peer_subscription/client_relay.rs`: retry logic now unwraps
    context via `root()` before deciding retryability.
- LocalProcessConnector now supports per-direction message transforms:
  `connectors/local-process/src/lib.rs`

Test run
--------
`cargo test -p ankurah-tests rt_missing_events_from_state_snapshot`
PASS (warnings only)

Uncommitted changes (at session end)
-----------------------------------
- `connectors/local-process/src/lib.rs`
- `core/src/entity.rs`
- `core/src/error.rs`
- `core/src/peer_subscription/client_relay.rs`
- `tests/tests/rt_missing_events.rs` (new)

Next steps
----------
1) Logging/telemetry improvements:
   - Add structured logs in `core/src/lineage.rs` when
     `Events not found` occurs (include missing IDs and frontier).
   - Add logs in `core/src/retrieval.rs::EphemeralNodeRetriever::retrieve_event`
     showing:
     - whether a durable peer was available,
     - how many events retrieved from staged/local/peer,
     - which IDs are still missing.
   - In `core/src/peer_subscription/client_relay.rs`, log the full
     context chain (from `RetrievalError::Context`) on permanent failures.
2) Use `rt_missing_events_from_state_snapshot` to verify logs show:
   - apply path (`apply_event` vs `apply_state`),
   - missing event IDs,
   - retrieval source failure.
3) Explore fallback behavior:
   - retry (time-based or limited attempts),
   - relaxed mode to accept state without lineage on clients,
   - or cache-only activation when remote subscription fails.
4) Investigate cached-result behavior:
   - On ephemeral nodes, remote subscribe failure prevents `activate`,
     so cached results are not inserted into the reactor.
   - Consider an “activate with cached state even if subscribe fails” option.

Open questions
--------------
- Under what real-world conditions are `known_matches` empty when state exists?
- Are event histories missing on the remote (durable) side, or is retrieval
  failing at the client (no durable peer, request failure, or peer returning
  empty)?
