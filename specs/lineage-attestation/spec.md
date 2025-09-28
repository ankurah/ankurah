## Functional Requirement: Server-assisted lineage confirmation and stale-entity correction

### Motivation

Ephemeral peers can go offline, or unsubscribe entities/predicates, and thus may fall behind on updates for entities already in their local storage engine. When they receive a new entity state from Fetch response or SubscriptionUpdate, confirming that it causally descends from their current head generally requires events that they don't have, and thus must be fetched or otherwise prove that the State in question fully descends their stale state. Recursively fetching those events individually can easily exhaust our appetite for round-trip requests. So instead, we propose to give the peer the information needed to either ship the events in question, or Attest that the state in question fully descends their stale state (with trust of that attestation being judged by the requester's `PolicyAgent`). This also allows the server to include states for items which _do not_ match the predicate, so as to freshen the requester's version and thus update said entity to be appropriately non-matching.

### Requirement

- Fetch and subscription requests MUST convey the requester’s current entity membership and heads for the relevant predicate/entities (known_matches).
- The server MUST use this information to assist in lineage confirmation and to correct stale membership on the requester.
- On initial fetch/subscribe, the server SHALL return per-entity one of:
  - For entities not present in known_matches: An attested State snapshot (the server MAY include items that do not currently match to freshen the requester’s state)
  - For entities present in known_matches (regardless of current matching state!): A bounded set of connecting ancestor events (a small bridge) that enables the requester to verify/apply without deep traversal; or
  - For entities present in known_matches but where the bridge exceeds the policy-configured max events: An attested State snapshot plus a cryptographic attestation of the lineage relation (e.g., Descends), allowing acceptance per policy
- For subscription initialization, the server omits entities whose heads are Equal to the requester’s known head. No explicit remove notifications are sent in the init response. After applying the returned deltas, the client MUST re-query local storage to refresh its result set.

### Outcomes

- Requester can efficiently conclude one of: Equal, Descends, NotDescends (with meet), PartiallyDescends (with meet), or Incomparable.
- On Descends, requester can accept state without regression (either via bridge or trusted attestation).
- On non-descending outcomes, requester avoids regression and may reconcile per policy.
- Streaming subscription updates presume continuity and DO NOT include lineage attestations; lineage assistance applies to Fetch and the initialization phase of Subscribe only.
- During initialization, for every entity in `known_matches` (regardless of current matching state): if the head is Equal, the server MAY omit it; otherwise the server MUST return either a small event bridge or an attested state plus relation. After applying deltas, the client re-queries to converge the result set to the predicate. This is necessary to correct membership in cases where the requester's state is stale.

### Policy & Trust

- Acceptance of attested relations is governed by the requester’s `PolicyAgent` and deployment trust model.
- Server assistance is bounded by policy-configured effort/budget.
- Attestations MAY (at the discretion of each PolicyAgent) cryptographically bind the entity identifier and both heads used in the relation; wire encodings MAY avoid duplicating a head already present in the state fragment while remaining verifiable.

### Notes

- This requirement applies to both Fetch and the initialization phase of Subscribe; streaming updates are unchanged.
- Concrete wire changes, data structures, and handler logic are documented in `specs/lineage-attestation/plan.md` and scheduled in `tasks.md`.
