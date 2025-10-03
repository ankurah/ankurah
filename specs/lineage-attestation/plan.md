## Implementation Plan: Lineage Attestation and Small Bridges

### Phase 1: Proto changes (breaking)

- Extend `NodeRequestBody::Fetch` and `SubscribeQuery` with `known_matches`
- Move subscription initialization out of the update stream into the `QuerySubscribed` response.
- Remove `NodeUpdateBody::SubscriptionUpdate.initialized_query`.
- Remove `UpdateContent::StateOnly` from streaming updates.
- Avoid double-encoding of the head in lineage evidence by carrying a wire-minimal form; signatures still bind both heads and ids.

Inline shapes (illustrative):

```rust
// proto/src/request.rs
#[derive(Serialize, Deserialize, Clone)]
pub struct KnownEntity {
    pub entity_id: EntityId,
    pub head: Clock,
}

pub enum NodeRequestBody {
    Fetch {
        collection: CollectionId,
        selection: ast::Selection,
        known_matches: Vec<KnownEntity>,
    },
    SubscribeQuery {
        query_id: QueryId,
        collection: CollectionId,
        selection: ast::Selection,
        version: u32,
        known_matches: Vec<KnownEntity>,
    },
    // ... existing
}

// proto/src/update.rs
#[derive(Serialize, Deserialize, Clone)]
pub enum HeadRelation {
    Equal,
    Descends,
    NotDescends { meet: Vec<EventId> },
    PartiallyDescends { meet: Vec<EventId> },
    Incomparable,
}

// Full signed payload (not serialized directly in messages)
pub struct EntityHeadRelation {
    pub entity_id: EntityId,
    pub from_head: Clock,
    pub to_head: Clock,
    pub relation: HeadRelation,
}

pub struct EntityHeadRelationFragment {
    // Wire-minimal: omit from_head and to_head; verifier reconstructs using known_from_head and state.head
    pub relation: HeadRelation,
    pub attestations: AttestationSet,
}


// Initialization items returned in QuerySubscribed
#[derive(Serialize, Deserialize, Clone)]
pub struct EntityDeltaState {
    pub entity_id: EntityId,
    pub collection: CollectionId,
    pub content: DeltaContent,
}

pub enum DeltaContent {
    // Entity not provided in known_matches; send full state snapshot
    StateSnapshot { state: StateFragment },
    // Known entity with small bridge under cap
    EventBridge { events: Vec<EventFragment> },
    // Known entity where bridge exceeds cap; send state + attested relation
    AttestedState { state: StateFragment, relation: EntityHeadRelationFragment },
}

pub enum NodeResponseBody {
    // ... existing
    QuerySubscribed { query_id: QueryId, initial: Vec<EntityDeltaState> },
    Fetch(Vec<EntityDeltaState>),
}

// Streaming updates (no initialized_query, no StateOnly)
pub enum NodeUpdateBody {
    SubscriptionUpdate { items: Vec<SubscriptionUpdateItem> }, // unchanged except removed initialized_query
}

pub enum UpdateContent {
    // removed StateOnly - because notify_change will ALWAYS have events
    EventOnly(Vec<EventFragment>),
    StateAndEvent(StateFragment, Vec<EventFragment>),
}
```

- Remove `impl From<Attested<EntityState>> for StateFragment`; use explicit conversions (`to_parts`/`from_parts`).

### Phase 2: Server handlers

- Subscribe:

  - Parse `known_matches` into a map keyed by `(EntityId)` (per `query_id`).
  - For each candidate entity:
    - If known and heads equal, omit from `initial` entirely.
    - Else try to build a small bridge under `max_events` (constant for now). If fits, include `events` only (no `state`, no `relation`).
    - Otherwise include attested `state` and `relation` (wire form). Signatures bind `(entity_id, from_head, state.head, relation)`.
  - Always return `QuerySubscribed { query_id, initial }` for both version 0 and selection updates; begin streaming updates (no init marker).

- Fetch:
  - Use the same bridge-first logic per item; return `Fetch(Vec<EntityDeltaState>)`.

### Phase 3: Client handling

- Ephemeral nodes (with `subscription_relay`):

  - LiveQuery constructs the `resultset` but does not call reactor add/update yet.
  - LiveQuery delegates to `SubscriptionRelay` for subscribe and selection update flows, passing `(query_id, collection_id, selection, context_data, version, ResultSet clone)`.
  - The relay computes `known_matches` using `node.fetch_entities_from_local(collection, selection)`.
  - The relay sends `SubscribeQuery { known_matches, version }` and awaits `QuerySubscribed { initial }`.
  - The relay applies `initial` deltas to the local node (bridges or attested state) synchronously.
  - Then it re-queries local storage to refresh the `resultset` (simpler approach; optimized delta update left as TODO).
  - Finally, LiveQuery calls `reactor.add_query` (for new) or `reactor.update_query` (for updates) with the locally re-queried entities.
  - For selection updates, LiveQuery must also trigger `QueryGapFetcher` directly after `reactor.update_query` since reactor no longer emits an init update.

- Durable nodes (no relay):

  - LiveQuery may call `node.request(SubscribeQuery)` directly with locally computed `known_matches`, handle `QuerySubscribed.initial` as above, then call reactor add/update.

- Streaming updates:

  - Process `SubscriptionUpdate` items (EventOnly/StateAndEvent) as today; gap filling for ongoing updates remains in `SubscriptionState::notify`.

- Fetch path:
  - Consume `Fetch(Vec<EntityDeltaState>)` with the same logic as subscription init items.

### Phase 4: PolicyAgent

- Add `validate_lineage_attestation(EntityHeadRelationFragment, state_head, entity_id)`.
- Configure server effort/budget for bridge building (max events).
- Define acceptance behavior for non-descending outcomes.

### Phase 5: Reactor and Node cleanups

- Remove initial broadcasts from `Reactor::add_query` and initial `subscription.notify` emissions from `Reactor::update_query` (no `initialized_query`).
- Keep `notify_change` and `SubscriptionState::notify` gap-filling behavior for ongoing updates.
- Remove `pending_predicate_subs` and the oneshot plumbing from `Node`; remove the `initialized_query` branch in `UpdateApplier::apply_updates`.

### Phase 6: Tests

- Unit: relation calculation, bridge builder limits, signature reconstruction using `state.head`.
- Integration:
  - Subscription init with: small bridge (events-only), attestation-only, omitted when equal.
  - Fetch with same scenarios.
  - Streaming updates unaffected by removal of `initialized_query` and `StateOnly`.

### Phase 7: Docs

- Update `SUBSCRIPTION.md` to describe init-in-response and bridge-first semantics.
- Brief README note linking to this spec.

### Risks & Mitigations

- Large selections: enforce per-response caps and paginate if needed.
- Attestation misuse: signatures bind `(entity_id, from_head, to_head, relation)`; wire carries minimal fields; policy validates against `state.head`.
