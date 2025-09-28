## Tasks

1. Proto changes

- Extend `Fetch` and `SubscribeQuery` with `known_matches`.
- Add `HeadRelation`, `EntityHeadRelation` (signed payload), `EntityHeadRelationFragment` (wire).
- Change `QuerySubscribed` to include `initial: Vec<EntityDeltaState>`.
- Change `Fetch` to return `Vec<EntityDeltaState>`.
- Remove `NodeUpdateBody::SubscriptionUpdate.initialized_query`.
- Remove `UpdateContent::StateOnly` from streaming updates.
- Remove `impl From<Attested<EntityState>> for StateFragment`; rely on explicit conversions.

2. Reactor/Node cleanups

- Remove initial `broadcast.send` in `Reactor::add_query` and initial `subscription.notify` emission in `Reactor::update_query`.
- Keep `notify_change` and gap-filling in `SubscriptionState::notify` for ongoing updates.
- Remove `pending_predicate_subs` and oneshot plumbing from `Node` and the matching branch in `UpdateApplier::apply_updates`.

3. Server handlers

- In `SubscriptionHandler::subscribe_query`, compute bridge-first `initial` from `known_matches`; omit equal heads; return `QuerySubscribed { initial }`.
- Fetch handler mirrors bridge-first logic.

4. Client: LiveQuery + SubscriptionRelay

- Pass a clone of `ResultSet` to `SubscriptionRelay.subscribe_query/update_query`.
- Relay flow per connection: prefetch `known_matches` via `node.fetch_entities_from_local`; send subscribe/update; await `QuerySubscribed`;
  apply deltas to local; re-fetch local and `replace_all` on the `ResultSet`.
- After relay completes:
  - `LiveQuery::new`: call `reactor.add_query` with refreshed local entities; wire `QueryGapFetcher` as today.
  - `LiveQuery::update_selection_init`: call `reactor.update_query` and then trigger `QueryGapFetcher` directly.

5. PolicyAgent

- Implement `validate_lineage_attestation(EntityHeadRelationFragment, state.head, entity_id)`.
- Add caps/trust knobs for bridge size and acceptance rules.

6. Tests

- Unit: relation calc, bridge limits, signature/validation glue.
- Integration: subscription init cases (events-only, attestation-only, omitted), fetch analogs; streaming unaffected.

7. Docs

- Update `SUBSCRIPTION.md`; document init-in-response, relay responsibilities, and bridge-first behavior.
